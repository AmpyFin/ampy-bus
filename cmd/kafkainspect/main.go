package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const help = `
kafkainspect — simple header/length dump for a Kafka topic (no payload decoding)

USAGE:
  kafkainspect --brokers host:9092 --topic <topic> [--group <id>] [--max N]

Examples:
  kafkainspect --brokers 127.0.0.1:9092 \
    --topic ampy.prod.dlq.v1.ampy.prod.bars.v1.XNAS.AAPL \
    --group dlq-inspector --max 10
`

func main() {
	fs := flag.NewFlagSet("kafkainspect", flag.ExitOnError)
	brokers := fs.String("brokers", "", "comma-separated brokers (host:port)")
	topic := fs.String("topic", "", "topic to read")
	group := fs.String("group", "kafkainspect", "consumer group id")
	max := fs.Int("max", 50, "max messages to print before exiting (<=0 = no limit)")
	fs.Parse(os.Args[1:])

	if *brokers == "" || *topic == "" {
		fmt.Fprint(os.Stderr, help)
		fs.PrintDefaults()
		os.Exit(2)
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(*brokers, ","),
		GroupID:  *group,
        // Note: segmentio/kafka-go binds topic here (no per-message Topic)
		Topic:    *topic,
		MaxBytes: 10 << 20,
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	count := 0
	for {
		if *max > 0 && count >= *max {
			break
		}
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if err == context.DeadlineExceeded || strings.Contains(err.Error(), "context deadline exceeded") {
				fmt.Fprintf(os.Stderr, "timeout: no messages received within 30 seconds\n")
				if count == 0 {
					fmt.Fprintf(os.Stderr, "no messages found in topic %s\n", *topic)
					os.Exit(0) // Exit successfully if no messages found
				}
				break
			}
			fmt.Fprintf(os.Stderr, "read: %v\n", err)
			os.Exit(1)
		}
		hdr := func(k string) string {
			k = strings.ToLower(k)
			for _, h := range m.Headers {
				if strings.ToLower(h.Key) == k {
					return string(h.Value)
				}
			}
			return ""
		}
		fmt.Printf("[inspect] topic=%s ts=%s bytes=%d msg_id=%s schema=%s dlq_reason=%q pk=%s producer=%s\n",
			*topic,
			m.Time.UTC().Format(time.RFC3339Nano),
			len(m.Value),
			hdr("message_id"),
			hdr("schema_fqdn"),
			hdr("dlq_reason"),
			hdr("partition_key"),
			hdr("producer"),
		)
		count++
	}
	fmt.Printf("[inspect] done, printed %d message(s)\n", count)
}


