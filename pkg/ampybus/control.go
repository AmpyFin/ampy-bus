package ampybus

import (
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
)

// Header keys for control messages (names are stable & lower_snake_case)
const (
	CtrlDomainKey    = "ctrl_domain"    // e.g., "bars", "ticks"
	CtrlVersionKey   = "ctrl_version"   // e.g., "v1"
	CtrlSubtopicKey  = "ctrl_subtopic"  // e.g., "XNAS.AAPL" or wildcard "*"
	CtrlSubjectKey   = "ctrl_subject"   // full subject/pattern override (ampy.prod.bars.v1.XNAS.AAPL)
	CtrlStartKey     = "ctrl_start"     // RFC3339 UTC
	CtrlEndKey       = "ctrl_end"       // RFC3339 UTC
	CtrlReasonKey    = "ctrl_reason"    // free text
	CtrlRequestIDKey = "ctrl_request_id"
)

// BuildReplayRequestEnvelope prepares a control message envelope.
// Either pass (domain, version, subtopic) OR set subjectPattern; subjectPattern wins if non-empty.
func BuildReplayRequestEnvelope(env string, domain string, version string, subtopic string, subjectPattern string,
	start time.Time, end time.Time, reason string, producer string) (Envelope, error) {

	// Topic for control requests
	topic := "ampy." + env + ".control.v1.replay_requests"

	h := NewHeaders("ampy.control.v1.ReplayRequest", producer, "ops-tooling", "replay:"+domain)
	// Populate control headers
	h.RunID = "replay_req_" + time.Now().UTC().Format("20060102T150405Z")
	h.DedupeKey = "" // reuse logic if you want idempotency on requests

	// Required control fields
	// Use both param styles: domain+version+subtopic and/or explicit subject pattern
	// Keep everything in headers to avoid schema dependency.
	// Note: ProducedAt already set.
	extra := map[string]string{
		CtrlDomainKey:   domain,
		CtrlVersionKey:  version,
		CtrlSubtopicKey: subtopic,
		CtrlStartKey:    start.UTC().Format(time.RFC3339),
		CtrlEndKey:      end.UTC().Format(time.RFC3339),
		CtrlReasonKey:   reason,
	}
	if subjectPattern != "" {
		extra[CtrlSubjectKey] = subjectPattern
	}

	// Attach extra headers by multiplexing into SchemaHash/BlobHash spares? No—PublishEnvelope maps all headers already.
	// We'll expose a small helper for adding arbitrary headers if needed. For now we'll set via EnvelopePublisher (see binding).

	// Empty payload for now; all control data in headers
	payload, enc, err := EncodeProtobuf(&emptypb.Empty{}, 0)
	if err != nil {
		return Envelope{}, err
	}
	h.ContentEncoding = enc

	envlp := Envelope{
		Topic:   topic,
		Headers: h,
		Payload: payload,
	}
	// We’ll attach the extra headers in the NATS binding publish path or via a wrapper in CLI (below).
	// Since Envelope has fixed headers, we’ll publish the extra keys by adding them to NATS headers just before send.
	// The CLI command will do that to avoid expanding Envelope struct.
	return envlp, nil
}
