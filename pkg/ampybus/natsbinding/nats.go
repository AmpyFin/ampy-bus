package natsbinding

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/obs"
	"github.com/nats-io/nats.go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ----- Configs (defined in config.go)

// ----- Bus

type Bus struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	cfg    Config
	closed int32
}

type PubAck struct {
	Stream   string
	Sequence uint64
}

func Connect(cfg Config) (*Bus, error) {
	if cfg.URLs == "" {
		cfg.URLs = nats.DefaultURL
	}
	opts := []nats.Option{
		nats.Name("ampy-bus"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
	}

	// TLS
	if cfg.TLS != nil {
		tlsCfg := &tls.Config{InsecureSkipVerify: cfg.TLS.InsecureSkipVerify} //nolint:gosec
		if cfg.TLS.CAFile != "" {
			caPEM, err := os.ReadFile(cfg.TLS.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read ca file: %w", err)
			}
			cp := x509.NewCertPool()
			if !cp.AppendCertsFromPEM(caPEM) {
				return nil, errors.New("append ca")
			}
			tlsCfg.RootCAs = cp
		}
		if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("load client keypair: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, nats.Secure(tlsCfg))
	}

	// Auth (precedence: creds > nkey > user/pass > token)
	if cfg.Auth.UserCredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.Auth.UserCredsFile))
	} else if cfg.Auth.NkeySeedFile != "" {
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

	nc, err := nats.Connect(cfg.URLs, opts...)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &Bus{nc: nc, js: js, cfg: cfg}, nil
}

func (b *Bus) Close() {
	if b == nil {
		return
	}
	if atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		b.nc.Drain()
		b.nc.Close()
	}
}

func (b *Bus) JS() nats.JetStreamContext { return b.js }

func (b *Bus) EnsureStream() error {
	if b.cfg.StreamName == "" {
		b.cfg.StreamName = "AMPY"
	}
	subjects := b.cfg.Subjects
	if len(subjects) == 0 {
		subjects = []string{"ampy.>"}
	}
	_, err := b.js.StreamInfo(b.cfg.StreamName)
	if err == nil {
		return nil
	}
	_, err = b.js.AddStream(&nats.StreamConfig{
		Name:        b.cfg.StreamName,
		Subjects:    subjects,
		NoAck:       false,
		Retention:   nats.LimitsPolicy,
		Storage:     nats.FileStorage,
		Replicas:    1,
		AllowRollup: true,
	})
	return err
}

func (b *Bus) PublishEnvelope(ctx context.Context, env ampybus.Envelope, extra map[string]string) (*PubAck, error) {
	// --- Tracing (producer)
	tr := otel.Tracer("ampybus")
	ctx, span := tr.Start(ctx, "bus.publish", trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.operation", "publish"),
		attribute.String("messaging.destination", env.Topic),
		attribute.String("ampy.schema_fqdn", env.Headers.SchemaFQDN),
	)
	defer span.End()

	// Build headers from envelope
	h := nats.Header{}
	write := func(k, v string) { if strings.TrimSpace(v) != "" { h.Set(k, v) } }
	write("message_id", env.Headers.MessageID)
	write("schema_fqdn", env.Headers.SchemaFQDN)
	write("schema_version", env.Headers.SchemaVersion)
	write("content_type", env.Headers.ContentType)
	write("content_encoding", env.Headers.ContentEncoding)
	write("produced_at", env.Headers.ProducedAt.UTC().Format(time.RFC3339Nano))
	write("producer", env.Headers.Producer)
	write("source", env.Headers.Source)
	write("run_id", env.Headers.RunID)
	write("partition_key", env.Headers.PartitionKey)
	write("dedupe_key", env.Headers.DedupeKey)
	if env.Headers.RetryCount > 0 {
		write("retry_count", fmt.Sprintf("%d", env.Headers.RetryCount))
	}
	write("dlq_reason", env.Headers.DLQReason)
	write("schema_hash", env.Headers.SchemaHash)
	if env.Headers.BlobRef != "" { write("blob_ref", env.Headers.BlobRef) }
	if env.Headers.BlobHash != "" { write("blob_hash", env.Headers.BlobHash) }
	if env.Headers.BlobSize > 0 { write("blob_size", fmt.Sprintf("%d", env.Headers.BlobSize)) }

	// Inject W3C trace headers + legacy trace_id/span_id
	carrier := propagation.HeaderCarrier(h)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	sc := span.SpanContext()
	write("trace_id", sc.TraceID().String())
	write("span_id", sc.SpanID().String())

	for k, v := range extra { write(k, v) }

	// Metrics: produced_total + batch_size_bytes
	obs.IncProduced(env.Topic, env.Headers.Producer)
	obs.ObserveBatchSize(env.Topic, len(env.Payload))

	msg := &nats.Msg{Subject: env.Topic, Header: h, Data: env.Payload}
	pa, err := b.js.PublishMsgAsync(msg)
	if err != nil {
		return nil, err
	}
	select {
	case ack := <-pa.Ok():
		return &PubAck{Stream: ack.Stream, Sequence: ack.Sequence}, nil
	case e := <-pa.Err():
		return nil, e
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *Bus) SubscribePull(subject, durable string,
	handler func(ctx context.Context, env ampybus.Envelope) error,
) (cancel func(), err error) {
	if b.cfg.StreamName == "" {
		b.cfg.StreamName = "AMPY"
	}
	if strings.TrimSpace(durable) == "" {
		durable = "bus-sub"
	}
	_, _ = b.js.AddConsumer(b.cfg.StreamName, &nats.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     nats.AckExplicitPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	sub, err := b.js.PullSubscribe(subject, durable, nats.BindStream(b.cfg.StreamName))
	if err != nil {
		return nil, fmt.Errorf("pull subscribe: %w", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	go func() {
		defer sub.Drain()
		tr := otel.Tracer("ampybus")
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msgs, ferr := sub.Fetch(10, nats.MaxWait(2*time.Second))
			if ferr == nats.ErrTimeout {
				continue
			}
			if ferr != nil {
				continue
			}
			for _, m := range msgs {
				// Extract context from headers
				carrier := propagation.HeaderCarrier(m.Header)
				parent := otel.GetTextMapPropagator().Extract(ctx, carrier)

				ctxSpan, span := tr.Start(parent, "bus.consume", trace.WithSpanKind(trace.SpanKindConsumer))
				span.SetAttributes(
					attribute.String("messaging.system", "nats"),
					attribute.String("messaging.operation", "process"),
					attribute.String("messaging.destination", m.Subject),
				)

				env := toEnvelope(m)

				// Metrics (consume path)
				obs.IncConsumed(env.Topic, durable)
				obs.ObserveBatchSize(env.Topic, len(env.Payload))
				if !env.Headers.ProducedAt.IsZero() {
					obs.ObserveDeliveryLatency(env.Topic, env.Headers.ProducedAt, time.Now().UTC())
				}

				// Invoke user handler
				hctx, cancel := context.WithTimeout(ctxSpan, 10*time.Second)
				err := handler(hctx, env)
				cancel()
				if err != nil {
					obs.IncDecodeFail(env.Topic, firstLine(err.Error()))
					_ = m.Ack()
					span.RecordError(err)
					span.End()
					continue
				}
				_ = m.Ack()
				span.End()
			}
		}
	}()

	return func() { cancelFn() }, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func toEnvelope(m *nats.Msg) ampybus.Envelope {
	var h ampybus.Headers
	get := func(k string) string { if m.Header == nil { return "" }; return m.Header.Get(k) }

	h.MessageID = get("message_id")
	h.SchemaFQDN = get("schema_fqdn")
	h.SchemaVersion = get("schema_version")
	h.ContentType = get("content_type")
	h.ContentEncoding = get("content_encoding")
	h.Producer = get("producer")
	h.Source = get("source")
	h.RunID = get("run_id")
	h.TraceID = get("trace_id")
	h.SpanID = get("span_id")
	h.PartitionKey = get("partition_key")
	h.DedupeKey = get("dedupe_key")
	if rc := get("retry_count"); rc != "" {
		fmt.Sscanf(rc, "%d", &h.RetryCount)
	}
	h.DLQReason = get("dlq_reason")
	h.SchemaHash = get("schema_hash")
	if pa := get("produced_at"); pa != "" {
		if t, err := time.Parse(time.RFC3339Nano, pa); err == nil {
			h.ProducedAt = t.UTC()
		} else if t2, err2 := time.Parse(time.RFC3339, pa); err2 == nil {
			h.ProducedAt = t2.UTC()
		}
	}
	if v := get("blob_ref"); v != "" { h.BlobRef = v }
	if v := get("blob_hash"); v != "" { h.BlobHash = v }
	if v := get("blob_size"); v != "" { fmt.Sscanf(v, "%d", &h.BlobSize) }

	return ampybus.Envelope{
		Topic:   m.Subject,
		Headers: h,
		Payload: bytesClone(m.Data),
	}
}

func bytesClone(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
