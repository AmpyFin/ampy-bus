package ampybus

import (
	"time"
)

// Headers is the transport-agnostic envelope header set.
// Keep names aligned with the README (lower_snake_case on the wire).
type Headers struct {
	MessageID       string    `json:"message_id"`
	SchemaFQDN      string    `json:"schema_fqdn"`
	SchemaVersion   string    `json:"schema_version"`
	ContentType     string    `json:"content_type"`
	ContentEncoding string    `json:"content_encoding,omitempty"`
	ProducedAt      time.Time `json:"produced_at"`
	Producer        string    `json:"producer"`
	Source          string    `json:"source"`
	RunID           string    `json:"run_id"`
	TraceID         string    `json:"trace_id"`
	SpanID          string    `json:"span_id,omitempty"`
	PartitionKey    string    `json:"partition_key"`

	// Optional
	DedupeKey   string `json:"dedupe_key,omitempty"`
	RetryCount  int    `json:"retry_count,omitempty"`
	DLQReason   string `json:"dlq_reason,omitempty"`
	SchemaHash  string `json:"schema_hash,omitempty"`
	BlobRef     string `json:"blob_ref,omitempty"`
	BlobHash    string `json:"blob_hash,omitempty"`
	BlobSize    int64  `json:"blob_size,omitempty"`
}

// Envelope wraps a protobuf payload with topic + headers.
type Envelope struct {
	Topic   string  `json:"topic"`
	Headers Headers `json:"headers"`

	// Payload is the serialized protobuf bytes (compressed if ContentEncoding=gzip)
	Payload []byte `json:"payload"`
}

// NewHeaders populates a sane default header set.
func NewHeaders(schemaFQDN, producer, source, partitionKey string) Headers {
	tr, sp := NewTrace()
	return Headers{
		MessageID:     NewUUID(), // UUIDv7 preferred
		SchemaFQDN:    schemaFQDN,
		SchemaVersion: "1.0.0",
		ContentType:   ContentTypeProtobuf,
		ProducedAt:    time.Now().UTC(),
		Producer:      producer,
		Source:        source,
		RunID:         "example_run",
		TraceID:       tr,
		SpanID:        sp,
		PartitionKey:  partitionKey,
	}
}

