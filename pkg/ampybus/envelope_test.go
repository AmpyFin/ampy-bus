package ampybus

import (
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	msg := &emptypb.Empty{}
	payload, enc, err := EncodeProtobuf(msg, 0) // force no gzip
	if err != nil {
		t.Fatal(err)
	}
	h := NewHeaders("ampy.bars.v1.BarBatch", "test@host", "test-source", PartitionKeyBars("XNAS", "AAPL"))
	h.ContentEncoding = enc
	env := Envelope{Topic: "ampy.prod.bars.v1.XNAS.AAPL", Headers: h, Payload: payload}

	if err := env.Headers.ValidateBasic(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	raw, err := DecodePayload(env.Payload, env.Headers.ContentEncoding)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(raw) != len(payload) {
		t.Fatalf("round-trip mismatch: got %d want %d", len(raw), len(payload))
	}
	if len(env.Headers.TraceID) != 32 {
		t.Fatalf("trace_id length: %d", len(env.Headers.TraceID))
	}
}

