package main

import (
	"fmt"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	// Build headers
	h := ampybus.NewHeaders(
		"ampy.bars.v1.BarBatch",
		"example-producer@host-1",
		"example-source",
		ampybus.PartitionKeyBars("XNAS", "AAPL"),
	)

	// Serialize a protobuf payload (use real ampy-proto msg later)
	payload, enc, err := ampybus.EncodeProtobuf(&emptypb.Empty{}, ampybus.DefaultCompressThreshold)
	if err != nil {
		panic(err)
	}
	h.ContentEncoding = enc

	// Create envelope
	env := ampybus.Envelope{
		Topic:   "ampy.prod.bars.v1.XNAS.AAPL",
		Headers: h,
		Payload: payload,
	}

	// Validate required headers
	if err := env.Headers.ValidateBasic(); err != nil {
		panic(err)
	}

	// Decode back to raw protobuf bytes
	raw, err := ampybus.DecodePayload(env.Payload, env.Headers.ContentEncoding)
	if err != nil {
		panic(err)
	}

	fmt.Printf("OK: topic=%s, payload=%d bytes (decoded=%d), msg_id=%s\n",
		env.Topic, len(env.Payload), len(raw), env.Headers.MessageID)
}
