package ampybus

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (h Headers) ValidateBasic() error {
	if h.MessageID == "" {
		return errors.New("message_id required")
	}
	if _, err := uuid.Parse(h.MessageID); err != nil {
		return errors.New("message_id must be a valid UUID")
	}
	if h.SchemaFQDN == "" {
		return errors.New("schema_fqdn required")
	}
	if h.ContentType != ContentTypeProtobuf {
		return errors.New("content_type must be application/x-protobuf")
	}
	if h.ProducedAt.IsZero() || h.ProducedAt.Location() != time.UTC {
		return errors.New("produced_at must be set and in UTC")
	}
	if h.Producer == "" {
		return errors.New("producer required")
	}
	if h.Source == "" {
		return errors.New("source required")
	}
	if h.RunID == "" {
		return errors.New("run_id required")
	}
	if strings.TrimSpace(h.TraceID) == "" {
		return errors.New("trace_id required")
	}
	if h.PartitionKey == "" {
		return errors.New("partition_key required")
	}
	if l := len(h.TraceID); l != 32 {
		return errors.New("trace_id must be 16 bytes hex (32 chars)")
	}
	if h.SpanID != "" && len(h.SpanID) != 16 {
		return errors.New("span_id must be 8 bytes hex (16 chars) when present")
	}

	return nil
}
