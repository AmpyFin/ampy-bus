package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/AmpyFin/ampy-bus/pkg/kafkabinding"
	"google.golang.org/protobuf/types/known/emptypb"
)

const help = `
kafkabusctl — AmpyFin bus control for Kafka

USAGE:
  kafkabusctl ensure-topic --brokers host:9092,host2:9092 --topic <ampy.topic> [--partitions 3]
  kafkabusctl pub-empty   --brokers host:9092 --topic <ampy.topic> --producer <name> --source <name> --pk <partition_key> [--schema <fqdn>]
  kafkabusctl sub         --brokers host:9092 --topic <ampy.topic> --group <group-id>

Examples:
  kafkabusctl ensure-topic --brokers 127.0.0.1:9092 --topic ampy.prod.bars.v1.XNAS.AAPL
  kafkabusctl sub --brokers 127.0.0.1:9092 --topic ampy.prod.bars.v1.XNAS.AAPL --group cli-consumer
  kafkabusctl pub-empty --brokers 127.0.0.1:9092 --topic ampy.prod.bars.v1.XNAS.AAPL \
      --producer yfinance-go@ingest-1 --source yfinance-go --pk XNAS.AAPL
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(help)
		os.Exit(1)
	}
	switch os.Args[1] {
	case "ensure-topic":
		ensureTopic(os.Args[2:])
	case "pub-empty":
		pubEmpty(os.Args[2:])
	case "dlq-sub":
		dlqSub(os.Args[2:])	
	case "sub":
		sub(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(help)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n%s", os.Args[1], help)
		os.Exit(1)
	}
}

func parseBrokers(fs *flag.FlagSet) []string {
	bs := fs.String("brokers", "", "comma-separated list of Kafka brokers (host:port)")
	fs.Parse(os.Args[2:])
	if *bs == "" {
		fmt.Fprintln(os.Stderr, "--brokers required")
		os.Exit(2)
	}
	return strings.Split(*bs, ",")
}

func ensureTopic(args []string) {
	fs := flag.NewFlagSet("ensure-topic", flag.ExitOnError)
	brokers := fs.String("brokers", "", "comma-separated list of brokers")
	topic := fs.String("topic", "", "topic to create")
	partitions := fs.Int("partitions", 3, "num partitions")
	fs.Parse(args)
	if *brokers == "" || *topic == "" {
		fs.PrintDefaults()
		os.Exit(2)
	}
	cfg := kafkabinding.Config{Brokers: strings.Split(*brokers, ",")}
	bus := kafkabinding.New(cfg)
	defer bus.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := bus.EnsureTopic(ctx, *topic, *partitions); err != nil {
		fmt.Fprintf(os.Stderr, "ensure topic: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[ensure-topic] ok: %s partitions=%d\n", *topic, *partitions)
}

func pubEmpty(args []string) {
	fs := flag.NewFlagSet("pub-empty", flag.ExitOnError)
	brokers := fs.String("brokers", "", "comma-separated list of brokers")
	topic := fs.String("topic", "", "ampy topic")
	producer := fs.String("producer", "", "producer id")
	source := fs.String("source", "", "source name")
	pk := fs.String("pk", "", "partition key")
	schema := fs.String("schema", "", "schema fqdn (guessed from topic if empty)")
	fs.Parse(args)

	if *brokers == "" || *topic == "" || *producer == "" || *source == "" || *pk == "" {
		fs.PrintDefaults()
		os.Exit(2)
	}
	fqdn := *schema
	if fqdn == "" {
		fqdn = guessSchemaFromTopic(*topic)
	}

	cfg := kafkabinding.Config{Brokers: strings.Split(*brokers, ",")}
	bus := kafkabinding.New(cfg)
	defer bus.Close()

	// ensure topic exists
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := bus.EnsureTopic(ctx, *topic, 3); err != nil {
		fmt.Fprintf(os.Stderr, "ensure topic: %v\n", err)
		os.Exit(1)
	}

	h := ampybus.NewHeaders(fqdn, *producer, *source, *pk)
	payload, enc, err := ampybus.EncodeProtobuf(&emptypb.Empty{}, ampybus.DefaultCompressThreshold)
	if err != nil { panic(err) }
	h.ContentEncoding = enc
	env := ampybus.Envelope{Topic: *topic, Headers: h, Payload: payload}

	if err := bus.PublishEnvelope(ctx, env, nil); err != nil {
		fmt.Fprintf(os.Stderr, "publish: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[publish] topic=%s msg_id=%s schema=%s bytes=%d (decoded=0)\n",
		env.Topic, h.MessageID, fqdn, len(env.Payload))
}

func sub(args []string) {
	fs := flag.NewFlagSet("sub", flag.ExitOnError)
	brokers := fs.String("brokers", "", "comma-separated list of brokers")
	topic := fs.String("topic", "", "ampy topic to consume")
	group := fs.String("group", "cli-consumer", "consumer group id")
	fs.Parse(args)

	if *brokers == "" || *topic == "" {
		fs.PrintDefaults()
		os.Exit(2)
	}
	cfg := kafkabinding.Config{
		Brokers: strings.Split(*brokers, ","),
		GroupID: *group,
	}
	bus := kafkabinding.New(cfg)

	ctx := context.Background()
	fmt.Printf("[sub] brokers=%s topic=%s group=%s\n", *brokers, *topic, *group)

	err := bus.Subscribe(ctx, *topic, func(ctx context.Context, env ampybus.Envelope) error {
		raw, err := ampybus.DecodePayload(env.Payload, env.Headers.ContentEncoding)
		if err != nil {
			fmt.Printf("[error] decode: %v\n", err)
			return err
		}
		fmt.Printf("[consume] topic=%s msg_id=%s schema=%s size=%d decoded=%d pk=%s\n",
			env.Topic, env.Headers.MessageID, env.Headers.SchemaFQDN,
			len(env.Payload), len(raw), env.Headers.PartitionKey)
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe error: %v\n", err)
		os.Exit(1)
	}
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

func dlqSub(args []string) {
    fs := flag.NewFlagSet("dlq-sub", flag.ExitOnError)
    brokers := fs.String("brokers", "", "comma-separated list of brokers")
    orig := fs.String("topic", "", "ORIGINAL ampy topic (e.g., ampy.prod.bars.v1.XNAS.AAPL)")
    group := fs.String("group", "dlq-subscriber", "consumer group id")
    prefix := fs.String("dlq-prefix", "ampy.prod.dlq.v1.", "DLQ prefix")
    fs.Parse(args)

    if *brokers == "" || *orig == "" {
        fs.PrintDefaults()
        os.Exit(2)
    }
    dlqTopic := *prefix + *orig
    cfg := kafkabinding.Config{
        Brokers: strings.Split(*brokers, ","),
        GroupID: *group,
        DLQPrefix: *prefix,
    }
    bus := kafkabinding.New(cfg)

    ctx := context.Background()
    fmt.Printf("[dlq-sub] brokers=%s topic=%s group=%s\n", *brokers, dlqTopic, *group)
    err := bus.Subscribe(ctx, dlqTopic, func(ctx context.Context, env ampybus.Envelope) error {
        // Print headers only; do NOT decode; never return error (avoid DLQ-of-DLQ)
        h := env.Headers
        fmt.Printf("[DLQ] topic=%s msg_id=%s reason=%q schema=%s pk=%s bytes=%d producer=%s\n",
            env.Topic, h.MessageID, h.DLQReason, h.SchemaFQDN, h.PartitionKey, len(env.Payload), h.Producer)
        return nil
    })
    if err != nil {
        fmt.Fprintf(os.Stderr, "dlq-sub error: %v\n", err)
        os.Exit(1)
    }
}
