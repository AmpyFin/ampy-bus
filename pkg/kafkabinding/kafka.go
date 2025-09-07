package kafkabinding

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	kafka "github.com/segmentio/kafka-go"
)

// Config for Kafka bus.
type Config struct {
	Brokers   []string          // e.g. []{"127.0.0.1:9092"}
	GroupID   string            // consumer group for Subscribe
	DLQPrefix string            // e.g. "ampy.prod.dlq.v1." (prefix + original topic)
	ExtraHdrs map[string]string // optional static headers added on publish
	// (TLS/auth can be added later; using PLAINTEXT for local dev)
}

type Bus struct {
	cfg       Config
	writersMu sync.Mutex
	writers   map[string]*kafka.Writer // per-topic writers
	closed    bool
}

// New returns a Kafka Bus.
func New(cfg Config) *Bus {
	if cfg.DLQPrefix == "" {
		cfg.DLQPrefix = "ampy.prod.dlq.v1."
	}
	return &Bus{
		cfg:     cfg,
		writers: make(map[string]*kafka.Writer),
	}
}

func (b *Bus) Close() error {
	b.writersMu.Lock()
	defer b.writersMu.Unlock()
	if b.closed {
		return nil
	}
	for _, w := range b.writers {
		_ = w.Close()
	}
	b.closed = true
	return nil
}

// EnsureTopic creates the topic if it doesn't exist (idempotent).
func (b *Bus) EnsureTopic(ctx context.Context, topic string, partitions int) error {
	if partitions <= 0 {
		partitions = 3
	}
	// Use a control connection to the first broker.
	conn, err := kafka.DialContext(ctx, "tcp", b.cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	// Check if topic exists; if not, create.
	part, err := conn.ReadPartitions()
	if err == nil {
		for _, p := range part {
			if p.Topic == topic {
				return nil // already exists
			}
		}
	}
	// Create with RF=1 for local dev; adjust in real clusters.
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "topic with this name already exists") {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	return nil
}

func (b *Bus) PublishEnvelope(ctx context.Context, env ampybus.Envelope, extra map[string]string) error {
	w := b.writerForTopic(env.Topic)
	h := env.Headers // copy by value

	// Merge extra headers (static + call-time)
	allExtra := map[string]string{}
	for k, v := range b.cfg.ExtraHdrs {
		allExtra[k] = v
	}
	for k, v := range extra {
		allExtra[k] = v
	}
	kHeaders := toKafkaHeaders(h, allExtra)

	// IMPORTANT: do NOT set Message.Topic when Writer.Topic is already set.
	msg := kafka.Message{
		Key:     []byte(h.PartitionKey),
		Value:   env.Payload,
		Time:    time.Now().UTC(),
		Headers: kHeaders,
		// Topic: (omit)
	}

	return w.WriteMessages(ctx, msg)
}


// Subscribe starts a consuming loop on a single topic using a consumer group.
// For each message, it reconstructs the Ampy envelope and calls handler.
// If handler returns error, the message is sent to DLQ and the original is committed.
func (b *Bus) Subscribe(ctx context.Context, topic string, handler func(context.Context, ampybus.Envelope) error) error {
	if b.cfg.GroupID == "" {
		return errors.New("GroupID required in Config for Subscribe")
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  b.cfg.Brokers,
		GroupID:  b.cfg.GroupID,
		Topic:    topic,
		MaxBytes: 10 << 20, // 10MB
	})
	defer r.Close()

	dlqTopic := b.cfg.DLQPrefix + topic
	if err := b.EnsureTopic(ctx, dlqTopic, 3); err != nil {
        return fmt.Errorf("ensure DLQ topic %q: %w", dlqTopic, err)
    }
	dlqWriter := b.writerForTopic(dlqTopic)
	

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// ctx canceled or reader error
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return fmt.Errorf("read: %w", err)
		}

		// Build envelope from Kafka message
		env := kafkaMsgToEnvelope(topic, &m)
		// Call handler
		if err := handler(ctx, env); err != nil {
			// Route to DLQ with reason
			dlqHdrs := env.Headers
			dlqHdrs.DLQReason = fmt.Sprintf("consume_error: %v", err)
			dlqMsg := kafka.Message{
				Topic:   b.cfg.DLQPrefix + topic,
				Key:     []byte(env.Headers.PartitionKey),
				Value:   env.Payload,
				Headers: toKafkaHeaders(dlqHdrs, nil),
				Time:    time.Now().UTC(),
			}
			if werr := dlqWriter.WriteMessages(ctx, dlqMsg); werr != nil {
				// DLQ failure is fatal for now
				return fmt.Errorf("dlq write failed: %w (orig err: %v)", werr, err)
			}
			fmt.Printf("[kafka] DLQ routed: %s -> %s reason=%s\n", topic, dlqTopic, dlqHdrs.DLQReason)
			// Commit original regardless to avoid tight redelivery loops
			continue
		}
		// Success path: committed implicitly by group reader
	}
}

// -------- internals --------

func (b *Bus) writerForTopic(topic string) *kafka.Writer {
	b.writersMu.Lock()
	defer b.writersMu.Unlock()
	if w, ok := b.writers[topic]; ok {
		return w
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(b.cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // use message Key hashing
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireAll, // strong durability
	}
	b.writers[topic] = w
	return w
}

func toKafkaHeaders(h ampybus.Headers, extras map[string]string) []kafka.Header {
	kv := map[string]string{
		"message_id":       h.MessageID,
		"schema_fqdn":      h.SchemaFQDN,
		"schema_version":   h.SchemaVersion,
		"schema_hash":      h.SchemaHash,
		"content_type":     h.ContentType,
		"content_encoding": h.ContentEncoding,
		"produced_at":      h.ProducedAt.Format(time.RFC3339Nano),
		"producer":         h.Producer,
		"source":           h.Source,
		"run_id":           h.RunID,
		"trace_id":         h.TraceID,
		"span_id":          h.SpanID,
		"partition_key":    h.PartitionKey,
		"dedupe_key":       h.DedupeKey,
		"dlq_reason":       h.DLQReason,
		"blob_ref":         h.BlobRef,
		"blob_hash":        h.BlobHash,
	}
	if h.BlobSize > 0 {
		kv["blob_size"] = fmt.Sprintf("%d", h.BlobSize)
	}
	for k, v := range extras {
		kv[k] = v
	}
	out := make([]kafka.Header, 0, len(kv))
	for k, v := range kv {
		if strings.TrimSpace(v) == "" {
			continue
		}
		out = append(out, kafka.Header{Key: k, Value: []byte(v)})
	}
	return out
}

func kafkaMsgToEnvelope(topic string, m *kafka.Message) ampybus.Envelope {
	// Convert headers to map[string]string (lowercase keys)
	hm := map[string]string{}
	for _, hd := range m.Headers {
		hm[strings.ToLower(hd.Key)] = string(hd.Value)
	}

	// Build Headers
	h := ampybus.Headers{
		MessageID:       hm["message_id"],
		SchemaFQDN:      hm["schema_fqdn"],
		SchemaVersion:   hm["schema_version"],
		SchemaHash:      hm["schema_hash"],
		ContentType:     hm["content_type"],
		ContentEncoding: hm["content_encoding"],
		Producer:        hm["producer"],
		Source:          hm["source"],
		RunID:           hm["run_id"],
		TraceID:         hm["trace_id"],
		SpanID:          hm["span_id"],
		PartitionKey:    hm["partition_key"],
		DedupeKey:       hm["dedupe_key"],
		DLQReason:       hm["dlq_reason"],
		BlobRef:         hm["blob_ref"],
		BlobHash:        hm["blob_hash"],
	}
	if ts := hm["produced_at"]; ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			h.ProducedAt = t.UTC()
		}
	}

	return ampybus.Envelope{
		Topic:   topic,
		Headers: h,
		Payload: m.Value,
	}
}
