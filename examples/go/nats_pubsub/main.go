package main

import (
	"context"
	"fmt"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/natsbinding"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	bus, err := natsbinding.Connect(natsbinding.Config{
		URLs:          "",
		StreamName:    "AMPY",
		Subjects:      []string{"ampy.>"},
		DurablePrefix: "bus-demo",
	})
	if err != nil { panic(err) }
	defer bus.Close()

	if err := bus.EnsureStream(); err != nil { panic(err) }

	cancel, err := bus.SubscribePull("ampy.prod.bars.v1.>", "bars-consumer", func(ctx context.Context, env ampybus.Envelope) error {
		raw, err := ampybus.DecodePayload(env.Payload, env.Headers.ContentEncoding)
		if err != nil { return err }
		fmt.Printf("[consume] subject=%s, msg_id=%s, bytes=%d (decoded=%d)\n",
			env.Topic, env.Headers.MessageID, len(env.Payload), len(raw))
		return nil
	})
	if err != nil { panic(err) }
	defer cancel()

	h := ampybus.NewHeaders(
		"ampy.bars.v1.BarBatch",
		"nats-publisher@host",
		"example-source",
		ampybus.PartitionKeyBars("XNAS", "AAPL"),
	)
	payload, enc, err := ampybus.EncodeProtobuf(&emptypb.Empty{}, ampybus.DefaultCompressThreshold)
	if err != nil { panic(err) }
	h.ContentEncoding = enc

	env := ampybus.Envelope{
		Topic:   "ampy.prod.bars.v1.XNAS.AAPL",
		Headers: h,
		Payload: payload,
	}

	ctx, cancelPub := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelPub()
	ack, err := bus.PublishEnvelope(ctx, env, nil)
	if err != nil { panic(err) }
	fmt.Printf("[publish] ack stream=%s seq=%d\n", ack.Stream, ack.Sequence)

	time.Sleep(2 * time.Second)
	fmt.Println("done.")
}
