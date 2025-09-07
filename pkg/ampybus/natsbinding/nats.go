package natsbinding

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
)

type Bus struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	stream    string
	subjects  []string
	durablePx string
}

func Connect(cfg Config) (*Bus, error) {
	if cfg.URLs == "" {
		cfg.URLs = nats.DefaultURL // nats://127.0.0.1:4222
	}

	opts := []nats.Option{
		nats.Name("ampy-bus"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
	}

	// ---- Auth options (precedence: creds > nkey > user/pass > token)
	if cfg.Auth.UserCredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.Auth.UserCredsFile))
	} else if cfg.Auth.NkeySeedFile != "" {
		// NKey requires a public key; use the helper option with seed file
		nb, err := os.ReadFile(cfg.Auth.NkeySeedFile)
		if err != nil {
			return nil, fmt.Errorf("read nkey seed: %w", err)
		}
		nkeyOpt, err := nats.NkeyOptionFromSeed(string(nb))
		if err != nil {
			return nil, fmt.Errorf("nkey option from seed: %w", err)
		}
		opts = append(opts, nkeyOpt)
	} else if cfg.Auth.Username != "" || cfg.Auth.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.Auth.Username, cfg.Auth.Password))
	} else if cfg.Auth.Token != "" {
		opts = append(opts, nats.Token(cfg.Auth.Token))
	}

	// ---- TLS options
	if cfg.TLS != nil {
		tlsCfg, err := loadTLSConfig(*cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, nats.Secure(tlsCfg))
	}

	nc, err := nats.Connect(cfg.URLs, opts...)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		_ = nc.Drain()
		return nil, err
	}
	if cfg.StreamName == "" {
		cfg.StreamName = "AMPY"
	}
	if len(cfg.Subjects) == 0 {
		cfg.Subjects = []string{"ampy.>"}
	}
	return &Bus{
		nc:        nc,
		js:        js,
		stream:    cfg.StreamName,
		subjects:  cfg.Subjects,
		durablePx: cfg.DurablePrefix,
	}, nil
}

func loadTLSConfig(t TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: t.InsecureSkipVerify} // #nosec G402 (allow via flag)
	if t.CAFile != "" {
		caBytes, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file: %w", err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to parse CA file")
		}
		tlsCfg.RootCAs = cp
	}
	if t.CertFile != "" && t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func (b *Bus) Close() { if b != nil && b.nc != nil { _ = b.nc.Drain() } }

func (b *Bus) EnsureStream() error {
	_, err := b.js.StreamInfo(b.stream)
	if err == nil {
		return nil
	}
	_, err = b.js.AddStream(&nats.StreamConfig{
		Name:     b.stream,
		Subjects: b.subjects,
		Storage:  nats.FileStorage,
		NoAck:    false,
		Replicas: 1,
	})
	return err
}

func SubjectFromTopic(topic string) string { return topic }

// PublishEnvelope publishes an Envelope with optional extraHeaders (for control keys, etc.).
// It auto-populates "schema_hash" if Headers.SchemaHash is empty.
func (b *Bus) PublishEnvelope(ctx context.Context, env ampybus.Envelope, extraHeaders map[string]string) (*nats.PubAck, error) {
	if err := env.Headers.ValidateBasic(); err != nil {
		return nil, err
	}
	msg := &nats.Msg{
		Subject: SubjectFromTopic(env.Topic),
		Data:    env.Payload,
		Header:  nats.Header{},
	}
	// Map envelope headers
	h := env.Headers
	msg.Header.Set("message_id", h.MessageID)
	msg.Header.Set("schema_fqdn", h.SchemaFQDN)
	msg.Header.Set("schema_version", h.SchemaVersion)
	msg.Header.Set("content_type", h.ContentType)
	if h.ContentEncoding != "" {
		msg.Header.Set("content_encoding", h.ContentEncoding)
	}
	msg.Header.Set("produced_at", h.ProducedAt.Format(time.RFC3339Nano))
	msg.Header.Set("producer", h.Producer)
	msg.Header.Set("source", h.Source)
	msg.Header.Set("run_id", h.RunID)
	msg.Header.Set("trace_id", h.TraceID)
	if h.SpanID != "" {
		msg.Header.Set("span_id", h.SpanID)
	}
	msg.Header.Set("partition_key", h.PartitionKey)

	if h.DedupeKey != "" {
		msg.Header.Set("dedupe_key", h.DedupeKey)
	}
	if h.RetryCount > 0 {
		msg.Header.Set("retry_count", fmt.Sprintf("%d", h.RetryCount))
	}
	if h.DLQReason != "" {
		msg.Header.Set("dlq_reason", h.DLQReason)
	}

	// ----- Schema hash (auto) -----
	schemaHash := h.SchemaHash
	if schemaHash == "" {
		schemaHash = ampybus.ExpectedSchemaHash(h.SchemaFQDN)
	}
	msg.Header.Set("schema_hash", schemaHash)

	// Optional blob/pointer headers
	if h.BlobRef != "" {
		msg.Header.Set("blob_ref", h.BlobRef)
	}
	if h.BlobHash != "" {
		msg.Header.Set("blob_hash", h.BlobHash)
	}
	if h.BlobSize > 0 {
		msg.Header.Set("blob_size", fmt.Sprintf("%d", h.BlobSize))
	}

	// W3C traceparent
	msg.Header.Set("traceparent", MakeTraceparent(h.TraceID, h.SpanID, true))

	// Extra headers (control fields)
	for k, v := range extraHeaders {
		if v != "" {
			msg.Header.Set(k, v)
		}
	}

	ackFuture, err := b.js.PublishMsgAsync(msg)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pa := <-ackFuture.Ok():
		return pa, nil
	case err = <-ackFuture.Err():
		return nil, err
	case <-time.After(5 * time.Second):
		return nil, errors.New("publish timeout waiting for ack")
	}
}

type Handler func(ctx context.Context, env ampybus.Envelope) error

// SubscribePull creates a durable pull subscription and fetch loop; call cancel() to stop.
func (b *Bus) SubscribePull(subject, durable string, handler Handler) (cancel func(), err error) {
	if durable == "" {
		durable = b.durablePx + "-default"
	}
	sub, err := b.js.PullSubscribe(subject, durable,
		nats.BindStream(b.stream),
		nats.AckExplicit(),
	)
	if err != nil {
		return nil, err
	}

	stop := make(chan struct{})
	go func() {
		defer sub.Unsubscribe()
		for {
			select {
			case <-stop:
				return
			default:
			}
			msgs, err := sub.Fetch(10, nats.MaxWait(2*time.Second))
			if err != nil && err != nats.ErrTimeout {
				continue
			}
			for _, m := range msgs {
				env, decErr := messageToEnvelope(m)
				if decErr != nil {
					_ = b.routeToDLQ(m, decErr.Error())
					_ = m.Ack()
					continue
				}
				// Schema hash verification (if header present)
				if verr := ampybus.VerifySchemaHash(env.Headers); verr != nil {
					_ = b.routeToDLQ(m, verr.Error())
					_ = m.Ack()
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				hErr := handler(ctx, env)
				cancel()
				if hErr != nil {
					_ = m.Nak()
					continue
				}
				_ = m.Ack()
			}
		}
	}()
	return func() { close(stop) }, nil
}

// Exported helper so other packages (examples) can reuse the mapping.
func MessageToEnvelope(m *nats.Msg) (ampybus.Envelope, error) { return messageToEnvelope(m) }

func messageToEnvelope(m *nats.Msg) (ampybus.Envelope, error) {
	h := ampybus.Headers{}
	get := func(k string) string { return m.Header.Get(k) }

	h.MessageID = get("message_id")
	h.SchemaFQDN = get("schema_fqdn")
	h.SchemaVersion = get("schema_version")
	h.ContentType = get("content_type")
	h.ContentEncoding = get("content_encoding")

	// produced_at parsing (try Nano then non-Nano)
	pt, err := time.Parse(time.RFC3339Nano, get("produced_at"))
	if err != nil {
		pt, err = time.Parse(time.RFC3339, get("produced_at"))
		if err != nil {
			return ampybus.Envelope{}, err
		}
	}
	h.ProducedAt = pt.UTC()

	h.Producer = get("producer")
	h.Source = get("source")
	h.RunID = get("run_id")
	h.TraceID = get("trace_id")
	h.SpanID = get("span_id")
	h.PartitionKey = get("partition_key")

	h.DedupeKey = get("dedupe_key")
	h.DLQReason = get("dlq_reason")
	h.SchemaHash = get("schema_hash")

	env := ampybus.Envelope{
		Topic:   m.Subject,
		Headers: h,
		Payload: m.Data,
	}
	if err := env.Headers.ValidateBasic(); err != nil {
		return ampybus.Envelope{}, err
	}
	return env, nil
}

func (b *Bus) routeToDLQ(m *nats.Msg, reason string) error {
	dlqSubject := "ampy.prod.dlq.v1." + m.Subject
	copyMsg := &nats.Msg{
		Subject: dlqSubject,
		Data:    m.Data,
		Header:  m.Header,
	}
	copyMsg.Header.Set("dlq_reason", reason)
	_, err := b.js.PublishMsg(copyMsg)
	return err
}

// JS exposes the underlying JetStream context for advanced operations.
func (b *Bus) JS() nats.JetStreamContext { return b.js }
