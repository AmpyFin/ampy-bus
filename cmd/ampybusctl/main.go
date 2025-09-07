package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/AmpyFin/ampy-bus/pkg/fixture"
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/natsbinding"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/emptypb"
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/obs"
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/validate"



)

const help = `
ampybusctl — AmpyFin bus control CLI

USAGE:
  ampybusctl pub-empty      --topic <ampy.topic> --producer <name> --source <name> --pk <partition_key> [--schema <fqdn>] [--schema-hash <value>] [conn flags]
  ampybusctl sub            --subject <pattern>  [--durable <name>] [conn flags]
  ampybusctl dlq            [--subject ampy.prod.dlq.v1.>] [--durable <name>] [conn flags]
  ampybusctl dlq-inspect    --subject <ampy.prod.dlq.v1.*|...> [--durable <name>] [--max N] [--since RFC3339] [--outdir DIR] [--decode] [conn flags]
  ampybusctl dlq-redrive    --subject <ampy.prod.dlq.v1.*|...> [--durable <name>] [--max N] [--since RFC3339] [conn flags]
  ampybusctl bench-pub      --topic <ampy.topic> --producer <name> --source <name> --pk <partition_key> --count <N> [--schema <fqdn>] [conn flags]
  ampybusctl replay         --env <dev|paper|prod> --domain <bars|ticks|...> --version v1 --subtopic <XNAS.AAPL> \
                            [--subject <ampy.* pattern>] --start <RFC3339Z> --end <RFC3339Z> --reason "<text>" [conn flags]
  ampybusctl validate-fixture --file <path.json> | --dir <dir>
  ampybusctl publish-fixture  --file <path.json> [conn flags]

Connection flags (or set envs):
  --nats <url>               (env: NATS_URL)
  --creds <file>             (env: NATS_CREDS)
  --nkey-seed <file>         (env: NATS_NKEY_SEED)
  --user <user> --pass <pw>  (env: NATS_USER / NATS_PASS)
  --token <token>            (env: NATS_TOKEN)
  --tls-ca <file>            (env: NATS_TLS_CA)
  --tls-cert <file>          (env: NATS_TLS_CERT)
  --tls-key <file>           (env: NATS_TLS_KEY)
  --tls-insecure             (env: NATS_TLS_INSECURE=1)

ENV:
  STREAM      AMPY                  (default)
`

type connOpts struct {
	natsURL                string
	credsFile              string
	nkeySeedFile           string
	user, pass, token      string
	tlsCA, tlsCert, tlsKey string
	tlsInsecure            bool
}

func addConnFlags(fs *flag.FlagSet) *connOpts {
	co := &connOpts{}
	fs.StringVar(&co.natsURL, "nats", getenv("NATS_URL", ""), "NATS URL")
	fs.StringVar(&co.credsFile, "creds", getenv("NATS_CREDS", ""), "NATS .creds file")
	fs.StringVar(&co.nkeySeedFile, "nkey-seed", getenv("NATS_NKEY_SEED", ""), "NKey seed file")
	fs.StringVar(&co.user, "user", getenv("NATS_USER", ""), "username")
	fs.StringVar(&co.pass, "pass", getenv("NATS_PASS", ""), "password")
	fs.StringVar(&co.token, "token", getenv("NATS_TOKEN", ""), "auth token")
	fs.StringVar(&co.tlsCA, "tls-ca", getenv("NATS_TLS_CA", ""), "TLS CA file")
	fs.StringVar(&co.tlsCert, "tls-cert", getenv("NATS_TLS_CERT", ""), "TLS client cert")
	fs.StringVar(&co.tlsKey, "tls-key", getenv("NATS_TLS_KEY", ""), "TLS client key")
	defInsecure := getenv("NATS_TLS_INSECURE", "")
	co.tlsInsecure = defInsecure == "1" || strings.ToLower(defInsecure) == "true"
	fs.BoolVar(&co.tlsInsecure, "tls-insecure", co.tlsInsecure, "skip TLS cert verification (NOT for prod)")
	return co
}

func makeConfig(co *connOpts) natsbinding.Config {
	var tlsCfg *natsbinding.TLSConfig
	if co.tlsCA != "" || (co.tlsCert != "" && co.tlsKey != "") || co.tlsInsecure {
		tlsCfg = &natsbinding.TLSConfig{
			CAFile:             co.tlsCA,
			CertFile:           co.tlsCert,
			KeyFile:            co.tlsKey,
			InsecureSkipVerify: co.tlsInsecure,
		}
	}
	return natsbinding.Config{
		URLs:          co.natsURL,
		StreamName:    getenv("STREAM", "AMPY"),
		Subjects:      []string{"ampy.>"},
		DurablePrefix: "busctl",
		Auth: natsbinding.AuthConfig{
			UserCredsFile: co.credsFile,
			NkeySeedFile:  co.nkeySeedFile,
			Username:      co.user,
			Password:      co.pass,
			Token:         co.token,
		},
		TLS: tlsCfg,
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Print(help)
		os.Exit(1)
	}
	switch os.Args[1] {
	case "pub-empty":
		pubEmpty(os.Args[2:])
	case "sub":
		sub(os.Args[2:])
	case "dlq":
		dlq(os.Args[2:])
	case "dlq-inspect":
		dlqInspect(os.Args[2:])
	case "dlq-redrive":
		dlqRedrive(os.Args[2:])
	case "bench-pub":
		benchPub(os.Args[2:])
	case "replay":
		replay(os.Args[2:])
	case "validate-fixture":
		validateFixtureCmd(os.Args[2:])
	case "publish-fixture":
		publishFixtureCmd(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(help)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n%s", os.Args[1], help)
		os.Exit(1)
	}
}

func connectWithOpts(co *connOpts) (*natsbinding.Bus, error) {
	cfg := makeConfig(co)
	bus, err := natsbinding.Connect(cfg)
	if err != nil {
		return nil, err
	}
	if err := bus.EnsureStream(); err != nil {
		bus.Close()
		return nil, err
	}
	return bus, nil
}

func guessSchemaFromTopic(topic string) string {
	if strings.Contains(topic, ".bars.") {
		return "ampy.bars.v1.BarBatch"
	}
	if strings.Contains(topic, ".ticks.") {
		return "ampy.ticks.v1.TradeTick"
	}
	if strings.Contains(topic, ".orders.") {
		return "ampy.orders.v1.OrderRequest"
	}
	if strings.Contains(topic, ".fills.") {
		return "ampy.fills.v1.Fill"
	}
	if strings.Contains(topic, ".positions.") {
		return "ampy.positions.v1.Position"
	}
	if strings.Contains(topic, ".signals.") {
		return "ampy.signals.v1.Signal"
	}
	if strings.Contains(topic, ".fx.") {
		return "ampy.fx.v1.FxRate"
	}
	if strings.Contains(topic, ".news.") {
		return "ampy.news.v1.NewsItem"
	}
	return "ampy.control.v1.Empty"
}

func pubEmpty(args []string) {
	fs := flag.NewFlagSet("pub-empty", flag.ExitOnError)
	topic := fs.String("topic", "", "ampy topic (e.g., ampy.prod.bars.v1.XNAS.AAPL)")
	producer := fs.String("producer", "", "producer id (service@host)")
	source := fs.String("source", "", "logical source (e.g., yfinance-go)")
	pk := fs.String("pk", "", "partition key")
	schema := fs.String("schema", "", "Schema FQDN (guessed from topic if omitted)")
	schemaHash := fs.String("schema-hash", "", "OVERRIDE schema_hash header (for testing/forcing DLQ)")
	co := addConnFlags(fs)
	
	useOTEL := fs.Bool("otel", false, "enable OpenTelemetry exporter")
	otelEP  := fs.String("otel-endpoint", "", "OTLP/gRPC endpoint (e.g., localhost:4317)")

	fs.Parse(args)

	var shutdown func(context.Context) error = func(context.Context) error { return nil }
	if *useOTEL {
		var err error
		shutdown, err = obs.InitTracer(context.Background(), "ampybusctl-"+fs.Name(), *otelEP)
		if err != nil { fmt.Fprintf(os.Stderr, "otel init: %v\n", err) }
		defer shutdown(context.Background())
	}

	if *topic == "" || *producer == "" || *source == "" || *pk == "" {
		fmt.Println("missing required flags. See --help.")
		fs.PrintDefaults()
		os.Exit(2)
	}
	fqdn := *schema
	if fqdn == "" {
		fqdn = guessSchemaFromTopic(*topic)
	}

	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	h := ampybus.NewHeaders(fqdn, *producer, *source, *pk)
	if *schemaHash != "" {
		h.SchemaHash = *schemaHash
	}

	payload, enc, err := ampybus.EncodeProtobuf(&emptypb.Empty{}, ampybus.DefaultCompressThreshold)
	if err != nil {
		panic(err)
	}
	h.ContentEncoding = enc
	env := ampybus.Envelope{Topic: *topic, Headers: h, Payload: payload}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := bus.PublishEnvelope(ctx, env, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[publish] ack stream=%s seq=%d topic=%s msg_id=%s schema=%s\n", ack.Stream, ack.Sequence, env.Topic, h.MessageID, fqdn)
}

func sub(args []string) {
	fs := flag.NewFlagSet("sub", flag.ExitOnError)
	subject := fs.String("subject", "ampy.>", "subject pattern (e.g., ampy.prod.bars.v1.>)")
	durable := fs.String("durable", "busctl-sub", "durable name")
	metricsAddr := fs.String("metrics", "", "Prometheus /metrics http listen address (e.g., :9102)")
	co := addConnFlags(fs)
	useOTEL := fs.Bool("otel", false, "enable OpenTelemetry exporter")
	otelEP  := fs.String("otel-endpoint", "", "OTLP/gRPC endpoint (e.g., localhost:4317)")
	fs.Parse(args)

	var shutdown func(context.Context) error = func(context.Context) error { return nil }
	if *useOTEL {
		var err error
		shutdown, err = obs.InitTracer(context.Background(), "ampybusctl-"+fs.Name(), *otelEP)
		if err != nil { fmt.Fprintf(os.Stderr, "otel init: %v\n", err) }
		defer shutdown(context.Background())
	}

	if strings.HasSuffix(*subject, ".*") {
		fmt.Println("[hint] NATS '*' matches one token; use '>' to match multiple segments (e.g., ampy.prod.bars.v1.>)")
	}

	if *metricsAddr != "" {
		obs.StartMetricsServer(*metricsAddr)
		fmt.Printf("[metrics] listening on http://127.0.0.1%s/metrics\n", *metricsAddr)
	}

	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	cancel, err := bus.SubscribePull(*subject, *durable, func(ctx context.Context, env ampybus.Envelope) error {
		raw, err := ampybus.DecodePayload(env.Payload, env.Headers.ContentEncoding)
		if err != nil {
			fmt.Printf("[error] decode: %v\n", err)
			return err
		}
		fmt.Printf("[consume] subject=%s msg_id=%s schema=%s size=%d decoded=%d pk=%s\n",
			env.Topic, env.Headers.MessageID, env.Headers.SchemaFQDN, len(env.Payload), len(raw), env.Headers.PartitionKey)
		return nil
	})
	if err != nil { panic(err) }
	defer cancel()

	fmt.Printf("[sub] listening on %s (durable=%s). Ctrl+C to exit.\n", *subject, *durable)
	wait()
}


func dlq(args []string) {
	fs := flag.NewFlagSet("dlq", flag.ExitOnError)
	subject := fs.String("subject", "ampy.prod.dlq.v1.>", "DLQ subject pattern")
	durable := fs.String("durable", "busctl-dlq", "durable name")
	co := addConnFlags(fs)
	fs.Parse(args)

	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	cancel, err := bus.SubscribePull(*subject, *durable, func(ctx context.Context, env ampybus.Envelope) error {
		fmt.Printf("[DLQ] subject=%s msg_id=%s reason=%s producer=%s\n",
			env.Topic, env.Headers.MessageID, env.Headers.DLQReason, env.Headers.Producer)
		return nil
	})
	if err != nil {
		panic(err)
	}
	defer cancel()

	fmt.Printf("[dlq] listening on %s (durable=%s). Ctrl+C to exit.\n", *subject, *durable)
	wait()
}

func dlqInspect(args []string) {
	fs := flag.NewFlagSet("dlq-inspect", flag.ExitOnError)
	subject := fs.String("subject", "ampy.prod.dlq.v1.>", "DLQ subject pattern")
	durable := fs.String("durable", "busctl-dlqinspect", "durable name")
	maxN := fs.Int("max", 50, "max messages to inspect")
	sinceStr := fs.String("since", "", "start time RFC3339 (optional)")
	outdir := fs.String("outdir", "", "dump payloads to directory (optional)")
	doDecode := fs.Bool("decode", false, "decode payload (respect content_encoding) and print size")
	co := addConnFlags(fs)
	fs.Parse(args)

	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	js := bus.JS()
	stream := getenv("STREAM", "AMPY")

	var startTime *time.Time
	if strings.TrimSpace(*sinceStr) != "" {
		t, err := time.Parse(time.RFC3339, *sinceStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad --since: %v\n", err)
			os.Exit(2)
		}
		t = t.UTC()
		startTime = &t
	}

	cc := &nats.ConsumerConfig{
		Durable:       *durable,
		FilterSubject: *subject,
		AckPolicy:     nats.AckExplicitPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
	}
	if startTime != nil {
		cc.DeliverPolicy = nats.DeliverByStartTimePolicy
		cc.OptStartTime = startTime
	} else {
		cc.DeliverPolicy = nats.DeliverAllPolicy
	}
	_, _ = js.AddConsumer(stream, cc)

	sub, err := js.PullSubscribe(*subject, *durable, nats.BindStream(stream))
	if err != nil {
		fmt.Fprintf(os.Stderr, "pull subscribe: %v\n", err)
		os.Exit(1)
	}

	if *outdir != "" {
		if err := os.MkdirAll(*outdir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir outdir: %v\n", err)
			os.Exit(1)
		}
	}

	processed := 0
	for processed < *maxN {
		msgs, err := sub.Fetch(10, nats.MaxWait(2*time.Second))
		if err == nats.ErrTimeout {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "fetch: %v\n", err)
			break
		}
		for _, m := range msgs {
			meta, _ := m.Metadata()
			fmt.Println("------------------------------------------------------------")
			fmt.Printf("[DLQ] subject=%s\n", m.Subject)
			if meta != nil {
				fmt.Printf("      stream_seq=%d consumer_seq=%d time=%s\n", meta.Sequence.Stream, meta.Sequence.Consumer, meta.Timestamp.UTC().Format(time.RFC3339))
			}
			keys := make([]string, 0, len(m.Header))
			for k := range m.Header { keys = append(keys, k) }
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("      %s: %s\n", k, m.Header.Get(k))
			}
			decodedSize := "n/a"
			if *doDecode {
				enc := m.Header.Get("content_encoding")
				raw, err := ampybus.DecodePayload(m.Data, enc)
				if err != nil {
					fmt.Printf("      decode_error: %v\n", err)
				} else {
					decodedSize = fmt.Sprintf("%d", len(raw))
				}
			}
			fmt.Printf("      payload_bytes=%d decoded_bytes=%s\n", len(m.Data), decodedSize)
			if *outdir != "" {
				msgID := m.Header.Get("message_id")
				if msgID == "" && meta != nil {
					msgID = fmt.Sprintf("%d", meta.Sequence.Stream)
				}
				filename := filepath.Join(*outdir, fmt.Sprintf("%s.bin", msgID))
				if err := os.WriteFile(filename, m.Data, 0o644); err != nil {
					fmt.Printf("      write_error: %v\n", err)
				} else {
					fmt.Printf("      wrote %s\n", filename)
				}
			}
			_ = m.Ack()
			processed++
			if processed >= *maxN { break }
		}
	}
	fmt.Printf("[dlq-inspect] processed %d message(s)\n", processed)
}

func dlqRedrive(args []string) {
	fs := flag.NewFlagSet("dlq-redrive", flag.ExitOnError)
	subject := fs.String("subject", "ampy.prod.dlq.v1.>", "DLQ subject pattern")
	durable := fs.String("durable", "busctl-redrive", "durable name")
	maxN := fs.Int("max", 100, "max messages to redrive")
	sinceStr := fs.String("since", "", "start time RFC3339 (optional)")
	co := addConnFlags(fs)
	fs.Parse(args)

	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	js := bus.JS()
	stream := getenv("STREAM", "AMPY")

	var startTime *time.Time
	if strings.TrimSpace(*sinceStr) != "" {
		t, err := time.Parse(time.RFC3339, *sinceStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad --since: %v\n", err)
			os.Exit(2)
		}
		t = t.UTC()
		startTime = &t
	}

	cc := &nats.ConsumerConfig{
		Durable:       *durable,
		FilterSubject: *subject,
		AckPolicy:     nats.AckExplicitPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
	}
	if startTime != nil {
		cc.DeliverPolicy = nats.DeliverByStartTimePolicy
		cc.OptStartTime = startTime
	} else {
		cc.DeliverPolicy = nats.DeliverAllPolicy
	}
	_, _ = js.AddConsumer(stream, cc)

	sub, err := js.PullSubscribe(*subject, *durable, nats.BindStream(stream))
	if err != nil {
		fmt.Fprintf(os.Stderr, "pull subscribe: %v\n", err)
		os.Exit(1)
	}

	const dlqPrefix = "ampy.prod.dlq.v1."
	redriven := 0

	for redriven < *maxN {
		msgs, err := sub.Fetch(10, nats.MaxWait(2*time.Second))
		if err == nats.ErrTimeout {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "fetch: %v\n", err)
			break
		}
		for _, m := range msgs {
			orig := strings.TrimPrefix(m.Subject, dlqPrefix)
			if orig == m.Subject {
				_ = m.Ack()
				continue
			}
			hdr := nats.Header{}
			for k, vals := range m.Header {
				if strings.EqualFold(k, "dlq_reason") { continue }
				for _, v := range vals { hdr.Add(k, v) }
			}
			rc, _ := strconv.Atoi(hdr.Get("retry_count"))
			if rc < 0 { rc = 0 }
			hdr.Set("retry_count", fmt.Sprintf("%d", rc+1))
			out := &nats.Msg{Subject: orig, Header: hdr, Data: m.Data}
			if _, err := js.PublishMsg(out); err != nil {
				fmt.Printf("[redrive] publish error: %v\n", err)
				_ = m.Nak()
				continue
			}
			fmt.Printf("[redrive] %s -> %s msg_id=%s\n", m.Subject, orig, m.Header.Get("message_id"))
			_ = m.Ack()
			redriven++
			if redriven >= *maxN { break }
		}
	}
	fmt.Printf("[dlq-redrive] redriven %d message(s)\n", redriven)
}

func benchPub(args []string) {
	fs := flag.NewFlagSet("bench-pub", flag.ExitOnError)
	topic := fs.String("topic", "", "ampy topic")
	producer := fs.String("producer", "", "producer id")
	source := fs.String("source", "", "source name")
	pk := fs.String("pk", "", "partition key")
	count := fs.Int("count", 1000, "messages to publish")
	schema := fs.String("schema", "", "Schema FQDN (guessed if empty)")
	co := addConnFlags(fs)
	useOTEL := fs.Bool("otel", false, "enable OpenTelemetry exporter")
	otelEP  := fs.String("otel-endpoint", "", "OTLP/gRPC endpoint (e.g., localhost:4317)")
	fs.Parse(args)

	var shutdown func(context.Context) error = func(context.Context) error { return nil }
	if *useOTEL {
		var err error
		shutdown, err = obs.InitTracer(context.Background(), "ampybusctl-"+fs.Name(), *otelEP)
		if err != nil { fmt.Fprintf(os.Stderr, "otel init: %v\n", err) }
		defer shutdown(context.Background())
	}


	if *topic == "" || *producer == "" || *source == "" || *pk == "" {
		fmt.Println("missing required flags.")
		fs.PrintDefaults()
		os.Exit(2)
	}
	fqdn := *schema
	if fqdn == "" {
		fqdn = guessSchemaFromTopic(*topic)
	}

	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	start := time.Now()
	for i := 0; i < *count; i++ {
		h := ampybus.NewHeaders(fqdn, *producer, *source, *pk)
		payload, enc, err := ampybus.EncodeProtobuf(&emptypb.Empty{}, ampybus.DefaultCompressThreshold)
		if err != nil {
			panic(err)
		}
		h.ContentEncoding = enc
		env := ampybus.Envelope{Topic: *topic, Headers: h, Payload: payload}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = bus.PublishEnvelope(ctx, env, nil)
		cancel()
		if err != nil {
			fmt.Printf("[bench] publish error at i=%d: %v\n", i, err)
			break
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("[bench] published %d msgs in %s (%.1f msg/s)\n", *count, elapsed, float64(*count)/elapsed.Seconds())
}

func replay(args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	env := fs.String("env", "prod", "environment: dev|paper|prod")
	domain := fs.String("domain", "", "domain: bars|ticks|news|fx|fundamentals|signals|orders|fills|positions|metrics")
	version := fs.String("version", "v1", "payload major version (e.g., v1)")
	subtopic := fs.String("subtopic", "", "subtopic (e.g., XNAS.AAPL)")
	subject := fs.String("subject", "", "explicit subject/pattern override (e.g., ampy.prod.bars.v1.XNAS.AAPL)")
	startStr := fs.String("start", "", "start RFC3339 (UTC, inclusive)")
	endStr := fs.String("end", "", "end RFC3339 (UTC, exclusive)")
	reason := fs.String("reason", "manual-replay", "reason")
	producer := fs.String("producer", "ampybusctl@host", "producer id")
	useOTEL := fs.Bool("otel", false, "enable OpenTelemetry exporter")
	otelEP  := fs.String("otel-endpoint", "", "OTLP/gRPC endpoint (e.g., localhost:4317)")
	co := addConnFlags(fs)
	fs.Parse(args)
	

	var shutdown func(context.Context) error = func(context.Context) error { return nil }
	if *useOTEL {
		var err error
		shutdown, err = obs.InitTracer(context.Background(), "ampybusctl-"+fs.Name(), *otelEP)
		if err != nil { fmt.Fprintf(os.Stderr, "otel init: %v\n", err) }
		defer shutdown(context.Background())
	}


	if *startStr == "" || *endStr == "" {
		fmt.Println("start and end are required (RFC3339 UTC).")
		fs.PrintDefaults()
		os.Exit(2)
	}
	if *subject == "" && (*domain == "" || *version == "") {
		fmt.Println("Either --subject or (--domain AND --version) must be provided.")
		fs.PrintDefaults()
		os.Exit(2)
	}

	start, err := time.Parse(time.RFC3339, *startStr)
	if err != nil { panic(err) }
	end, err := time.Parse(time.RFC3339, *endStr)
	if err != nil { panic(err) }
	if !start.Before(end) {
		fmt.Println("--start must be < --end")
		os.Exit(2)
	}

	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	envlp, err := ampybus.BuildReplayRequestEnvelope(*env, *domain, *version, *subtopic, *subject, start, end, *reason, *producer)
	if err != nil { panic(err) }

	extra := map[string]string{
		ampybus.CtrlDomainKey:    *domain,
		ampybus.CtrlVersionKey:   *version,
		ampybus.CtrlSubtopicKey:  *subtopic,
		ampybus.CtrlSubjectKey:   *subject,
		ampybus.CtrlStartKey:     start.UTC().Format(time.RFC3339),
		ampybus.CtrlEndKey:       end.UTC().Format(time.RFC3339),
		ampybus.CtrlReasonKey:    *reason,
		ampybus.CtrlRequestIDKey: envlp.Headers.MessageID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := bus.PublishEnvelope(ctx, envlp, extra)
	if err != nil { panic(err) }
	fmt.Printf("[replay] request sent: stream=%s seq=%d topic=%s request_id=%s\n",
		ack.Stream, ack.Sequence, envlp.Topic, envlp.Headers.MessageID)
}



func validateFixtureCmd(args []string) {
	fs := flag.NewFlagSet("validate-fixture", flag.ExitOnError)
	file := fs.String("file", "", "fixture JSON path")
	dir := fs.String("dir", "", "directory of fixtures (recursive)")
	fs.Parse(args)

	if err := validate.ValidateDir(*dir); err != nil {
		fmt.Fprintf(os.Stderr, "validation failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("[validate] all fixtures OK")

	var files []string
	if *file == "" && *dir == "" {
		fmt.Println("provide --file or --dir")
		os.Exit(2)
	}
	if *file != "" {
		files = []string{*file}
	}
	if *dir != "" {
		var err error
		files, err = fixture.WalkDir(*dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "walk dir: %v\n", err)
			os.Exit(1)
		}
		if len(files) == 0 {
			fmt.Printf("no *.json files under %s\n", *dir)
			return
		}
	}

	total := 0
	fail := 0
	for _, p := range files {
		ff, err := fixture.Load(p)
		if err != nil {
			fmt.Printf("FAIL: %s\n  - %v\n", p, err)
			fail++
			total++
			continue
		}
		issues := ff.Validate()
		fixture.PrettyIssues(os.Stdout, p, issues)
		if len(issues) > 0 { fail++ }
		total++
	}
	fmt.Printf("[validate-fixture] %d total, %d fail\n", total, fail)
	if fail > 0 {
		os.Exit(1)
	}
}

func publishFixtureCmd(args []string) {
	fs := flag.NewFlagSet("publish-fixture", flag.ExitOnError)
	file := fs.String("file", "", "fixture JSON path (required)")
	co := addConnFlags(fs)
	fs.Parse(args)
	if *file == "" {
		fmt.Println("provide --file")
		os.Exit(2)
	}
	ff, err := fixture.Load(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load fixture: %v\n", err)
		os.Exit(1)
	}
	env, err := ff.ToEnvelope()
	if err != nil {
		fmt.Fprintf(os.Stderr, "to envelope: %v\n", err)
		os.Exit(1)
	}
	bus, err := connectWithOpts(co)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed: %v\n", err)
		os.Exit(1)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := bus.PublishEnvelope(ctx, env, ff.Extra)
	if err != nil {
		fmt.Fprintf(os.Stderr, "publish: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PUBLISHED: stream=%s seq=%d topic=%s msg_id=%s schema=%s\n",
		ack.Stream, ack.Sequence, env.Topic, env.Headers.MessageID, env.Headers.SchemaFQDN)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func wait() { select {} }
