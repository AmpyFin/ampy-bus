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
)

const help = `
kafkapoison — publish a deliberately bad payload to trigger DLQ

USAGE:
  kafkapoison --brokers host:9092 --topic <ampy.topic> \
    --producer <name> --source <name> --pk <partition_key> [--schema <fqdn>]

Example:
  kafkapoison --brokers 127.0.0.1:9092 \
    --topic ampy.prod.bars.v1.XNAS.AAPL \
    --producer poison@cli --source poison-test --pk XNAS.AAPL
`

func main() {
	fs := flag.NewFlagSet("kafkapoison", flag.ExitOnError)
	brokers := fs.String("brokers", "", "comma-separated list of Kafka brokers (host:port)")
	topic := fs.String("topic", "", "ampy topic (e.g., ampy.prod.bars.v1.XNAS.AAPL)")
	producer := fs.String("producer", "", "producer id (service@host)")
	source := fs.String("source", "", "logical source (e.g., test)")
	pk := fs.String("pk", "", "partition key (e.g., XNAS.AAPL)")
	schema := fs.String("schema", "", "schema FQDN (guessed from topic if omitted)")
	fs.Parse(os.Args[1:])

	if *brokers == "" || *topic == "" || *producer == "" || *source == "" || *pk == "" {
		fmt.Fprint(os.Stderr, help)
		fs.PrintDefaults()
		os.Exit(2)
	}
	fqdn := *schema
	if fqdn == "" {
		fqdn = guessSchemaFromTopic(*topic)
	}

	cfg := kafkabinding.Config{Brokers: strings.Split(*brokers, ",")}
	bus := kafkabinding.New(cfg)

	// Ensure the normal topic exists (DLQ usually auto-creates on Redpanda; ensure manually if your cluster needs it)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := bus.EnsureTopic(ctx, *topic, 3); err != nil {
		fmt.Fprintf(os.Stderr, "ensure topic: %v\n", err)
		os.Exit(1)
	}

	// Build headers; lie about gzip so consumers fail to decode → DLQ
	h := ampybus.NewHeaders(fqdn, *producer, *source, *pk)
	h.ContentType = "application/x-protobuf"
	h.ContentEncoding = "gzip" // <— LIE: payload is not gzipped
	h.SchemaHash = ampybus.ExpectedSchemaHash(fqdn)

	// Intentionally bad payload
	payload := []byte("NOT-GZIP-POISON")

	env := ampybus.Envelope{
		Topic:   *topic,
		Headers: h,
		Payload: payload,
	}
	if err := bus.PublishEnvelope(ctx, env, map[string]string{
		"poison_reason": "gzip_header_with_plain_payload",
	}); err != nil {
		fmt.Fprintf(os.Stderr, "publish: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[poison] sent topic=%s pk=%s msg_id=%s schema=%s bytes=%d (declared enc=gzip)\n",
		env.Topic, h.PartitionKey, h.MessageID, h.SchemaFQDN, len(env.Payload))
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
