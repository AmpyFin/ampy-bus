package fixture

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/google/uuid"
)

// File format: one of payload_* may be present; empty base64 is allowed.
//   {
//     "topic": "ampy.prod.bars.v1.XNAS.AAPL",
//     "headers": {
//       "schema_fqdn": "ampy.bars.v1.BarBatch",
//       "schema_version": "1.0.0",
//       "content_type": "application/x-protobuf",
//       "produced_at": "2025-09-06T19:31:01Z",
//       "producer": "fixture@cli",
//       "source": "golden-fixture",
//       "run_id": "golden_demo",
//       "trace_id": "4b5b3f2a0f9d4e3db4c8a1f0e3a7c812",
//       "partition_key": "XNAS.AAPL"
//     },
//     "payload_base64": ""
//   }
type File struct {
	Topic         string            `json:"topic"`
	Headers       map[string]string `json:"headers"`
	PayloadBase64 string            `json:"payload_base64,omitempty"`
	PayloadB64    string            `json:"payload_b64,omitempty"`  // alias
	PayloadHex    string            `json:"payload_hex,omitempty"`
	PayloadJSON   map[string]any    `json:"payload_json,omitempty"`
	Note          string            `json:"note,omitempty"`
	Extra         map[string]string `json:"extra_headers,omitempty"`
}

var (
	reqHeaderKeys = []string{
		"schema_fqdn",
		"schema_version",
		"content_type",
		"produced_at",
		"producer",
		"source",
		"partition_key",
	}

	semverRe   = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
	traceIDRe  = regexp.MustCompile(`^[a-fA-F0-9]{16,64}$`)
	uuidishRe  = regexp.MustCompile(`^[a-f0-9A-F-]{8,}$`)
	rMicSym    = regexp.MustCompile(`^[A-Z0-9]{4}\.[A-Z0-9.\-]+$`)                   // XNAS.AAPL / XLON.VOD.L
	rFxPair    = regexp.MustCompile(`^[A-Z]{3}\.[A-Z]{3}$`)                           // USD.JPY
	rAcctOrder = regexp.MustCompile(`^[A-Za-z0-9_\-]+[|][A-Za-z0-9_\-]+$`)            // ACCOUNT|CLIENT_ORDER_ID
	rAcctSym   = regexp.MustCompile(`^[A-Za-z0-9_\-]+[|][A-Z0-9.\-]+[.][A-Z0-9]{4}$`) // ACCOUNT|SYMBOL.MIC

	rfc3339Layouts = []string{time.RFC3339Nano, time.RFC3339}
)

// Load a single fixture JSON from disk (strict: unknown fields rejected).
func Load(path string) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ff File
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ff); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	return &ff, nil
}

// ToEnvelope converts the fixture to an Envelope (fills schema_hash if missing).
func (ff *File) ToEnvelope() (ampybus.Envelope, error) {
	if ff.Topic == "" {
		return ampybus.Envelope{}, fmt.Errorf("missing topic")
	}

	// Normalize header keys to lowercase for tolerant lookup.
	if ff.Headers != nil {
		normalized := map[string]string{}
		for k, v := range ff.Headers {
			normalized[strings.ToLower(k)] = v
		}
		ff.Headers = normalized
	}

	get := func(k string) string { return ff.Headers[strings.ToLower(k)] }

	var h ampybus.Headers
	h.SchemaFQDN = get("schema_fqdn")
	h.SchemaVersion = get("schema_version")
	h.ContentType = get("content_type")
	h.ContentEncoding = get("content_encoding")
	h.Producer = get("producer")
	h.Source = get("source")
	h.RunID = get("run_id")
	h.TraceID = get("trace_id")
	h.SpanID = get("span_id")
	h.PartitionKey = get("partition_key")
	h.DedupeKey = get("dedupe_key")
	if rc := get("retry_count"); rc != "" {
		fmt.Sscanf(rc, "%d", &h.RetryCount)
	}
	h.DLQReason = get("dlq_reason")
	h.SchemaHash = get("schema_hash")
	h.BlobRef = get("blob_ref")
	h.BlobHash = get("blob_hash")
	if bs := get("blob_size"); bs != "" {
		fmt.Sscanf(bs, "%d", &h.BlobSize)
	}

	// produced_at
	pa := get("produced_at")
	var parsed time.Time
	var perr error
	for _, layout := range rfc3339Layouts {
		parsed, perr = time.Parse(layout, pa)
		if perr == nil {
			break
		}
	}
	if perr != nil {
		return ampybus.Envelope{}, fmt.Errorf("bad produced_at: %v", perr)
	}
	h.ProducedAt = parsed.UTC()

	// message_id: accept provided; otherwise create deterministic UUIDv5(topic|produced_at)
	h.MessageID = get("message_id")
	if h.MessageID == "" {
		ns := uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8") // DNS namespace (stable)
		h.MessageID = uuid.NewSHA1(ns, []byte(ff.Topic+"|"+h.ProducedAt.Format(time.RFC3339Nano))).String()
	}

	// Payload selection
	var payload []byte
	var err error
	switch {
	case ff.PayloadBase64 != "":
		payload, err = base64.StdEncoding.DecodeString(ff.PayloadBase64)
	case ff.PayloadB64 != "":
		payload, err = base64.StdEncoding.DecodeString(ff.PayloadB64)
	case ff.PayloadHex != "":
		payload, err = hex.DecodeString(strings.TrimSpace(ff.PayloadHex))
	case ff.PayloadJSON != nil:
		if h.ContentType == "" {
			h.ContentType = "application/json"
		}
		payload, err = json.Marshal(ff.PayloadJSON)
	default:
		payload = []byte{} // 0-byte payload allowed
	}
	if err != nil {
		return ampybus.Envelope{}, err
	}

	// Auto-fill schema_hash if missing (nameonly mode).
	if h.SchemaHash == "" {
		h.SchemaHash = ampybus.ExpectedSchemaHash(h.SchemaFQDN)
	}

	return ampybus.Envelope{
		Topic:   ff.Topic,
		Headers: h,
		Payload: payload,
	}, nil
}

// Validate returns human-readable issues (empty slice == PASS).
func (ff *File) Validate() []string {
	var issues []string

	if strings.TrimSpace(ff.Topic) == "" {
		return append(issues, "missing: topic")
	}
	// Required headers present?
	for _, k := range reqHeaderKeys {
		found := false
		for hk := range ff.Headers {
			if strings.EqualFold(hk, k) {
				found = true
				break
			}
		}
		if !found {
			issues = append(issues, "missing header: "+k)
		}
	}

	// Build envelope once and reuse (parses produced_at, normalizes headers).
	env, err := ff.ToEnvelope()
	if err != nil {
		return append(issues, "invalid fields: "+err.Error())
	}

	h := env.Headers

	// schema_version must be semver
	if sv := strings.TrimSpace(h.SchemaVersion); sv == "" || !semverRe.MatchString(sv) {
		issues = append(issues, "headers.schema_version must be semver (e.g., 1.0.0)")
	}

	// content_type expectations
	if ff.PayloadJSON != nil {
		if h.ContentType != "application/json" {
			issues = append(issues, "payload_json requires headers.content_type=application/json")
		}
	} else {
		if h.ContentType != "application/x-protobuf" {
			issues = append(issues, "headers.content_type should be application/x-protobuf (unless payload_json is used)")
		}
	}

	// content_encoding (allow empty or gzip)
	if ce := strings.ToLower(strings.TrimSpace(h.ContentEncoding)); ce != "" && ce != "gzip" {
		issues = append(issues, "headers.content_encoding must be empty or 'gzip'")
	}

	// Trace/IDs sanity (if present)
	if h.TraceID != "" && !traceIDRe.MatchString(h.TraceID) {
		issues = append(issues, "headers.trace_id should be hex(16-64 chars)")
	}
	if mid := strings.TrimSpace(h.MessageID); mid != "" && !uuidishRe.MatchString(mid) {
		issues = append(issues, "headers.message_id should look like a UUID")
	}

	// Verify schema_hash correctness (nameonly); ToEnvelope auto-fills if missing.
	if err := ampybus.VerifySchemaHash(h); err != nil {
		issues = append(issues, err.Error())
	}

	// Topic layout: ampy.{env}.{domain}.{version}.{subtopic...}
	parts := strings.Split(env.Topic, ".")
	if len(parts) < 5 || parts[0] != "ampy" {
		return append(issues, "topic should look like ampy.{env}.{domain}.{version}.{subtopic}")
	}
	domain := parts[2]
	version := parts[3]
	subtopic := strings.Join(parts[4:], ".")
	_ = version // future proof

	// Domain-specific checks
	switch domain {
	case "bars":
		if !rMicSym.MatchString(subtopic) {
			issues = append(issues, "bars subtopic must be MIC.SYMBOL (e.g., XNAS.AAPL)")
		}
		if h.PartitionKey != subtopic {
			issues = append(issues, fmt.Sprintf("bars partition_key=%q should equal subtopic=%q", h.PartitionKey, subtopic))
		}
		if h.SchemaFQDN != "ampy.bars.v1.BarBatch" {
			issues = append(issues, "bars schema_fqdn must be ampy.bars.v1.BarBatch")
		}

	case "ticks":
		// Minimal shape: trade.SYMBOL or quote.SYMBOL
		if !strings.Contains(subtopic, ".") {
			issues = append(issues, "ticks subtopic should contain a '.' (e.g., trade.MSFT)")
		}

	case "news":
		if subtopic != "raw" && subtopic != "nlp" {
			issues = append(issues, "news subtopic must be 'raw' or 'nlp'")
		}
		if strings.TrimSpace(h.DedupeKey) == "" {
			issues = append(issues, "news requires headers.dedupe_key")
		}
		if h.SchemaFQDN != "ampy.news.v1.NewsItem" {
			issues = append(issues, "news schema_fqdn must be ampy.news.v1.NewsItem")
		}

	case "fx":
		if subtopic != "rates" && !rFxPair.MatchString(subtopic) {
			issues = append(issues, "fx subtopic must be 'rates' or BASE.QUOTE (e.g., USD.JPY)")
		}
		if !rFxPair.MatchString(h.PartitionKey) {
			issues = append(issues, "fx partition_key must be BASE.QUOTE (e.g., USD.JPY)")
		}
		if h.SchemaFQDN != "ampy.fx.v1.FxRate" {
			issues = append(issues, "fx schema_fqdn must be ampy.fx.v1.FxRate")
		}

	case "signals":
		if !strings.Contains(h.PartitionKey, "|") {
			issues = append(issues, "signals partition_key should contain '|' (e.g., model@date|NVDA.XNAS)")
		}
		if h.SchemaFQDN != "ampy.signals.v1.Signal" {
			issues = append(issues, "signals schema_fqdn must be ampy.signals.v1.Signal")
		}

	case "orders":
		if subtopic != "requests" {
			issues = append(issues, "orders subtopic must be 'requests'")
		}
		if strings.TrimSpace(h.DedupeKey) == "" {
			issues = append(issues, "orders require headers.dedupe_key (client_order_id)")
		}
		if h.PartitionKey != h.DedupeKey && h.DedupeKey != "" {
			issues = append(issues, fmt.Sprintf("orders partition_key=%q should equal dedupe_key=%q", h.PartitionKey, h.DedupeKey))
		}
		if h.SchemaFQDN != "ampy.orders.v1.OrderRequest" {
			issues = append(issues, "orders schema_fqdn must be ampy.orders.v1.OrderRequest")
		}

	case "fills":
		if subtopic != "events" {
			issues = append(issues, "fills subtopic must be 'events'")
		}
		if !rAcctOrder.MatchString(h.PartitionKey) {
			issues = append(issues, "fills partition_key must be ACCOUNT|CLIENT_ORDER_ID")
		}
		if h.SchemaFQDN != "ampy.fills.v1.Fill" {
			issues = append(issues, "fills schema_fqdn must be ampy.fills.v1.Fill")
		}

	case "positions":
		if subtopic != "snapshots" {
			issues = append(issues, "positions subtopic must be 'snapshots'")
		}
		if !rAcctSym.MatchString(h.PartitionKey) {
			issues = append(issues, "positions partition_key must be ACCOUNT|SYMBOL.MIC")
		}
		if h.SchemaFQDN != "ampy.positions.v1.Position" {
			issues = append(issues, "positions schema_fqdn must be ampy.positions.v1.Position")
		}

	case "metrics":
		// Best-effort: pk like service|metric_name; subtopic is service.
		if !strings.Contains(h.PartitionKey, "|") {
			issues = append(issues, "metrics partition_key should contain 'service|metric_name'")
		}
		// schema_fqdn not strictly enforced yet.

	default:
		// For unknown domains, be lenient: no pk==subtopic rule.
	}

	return issues
}

// WalkDir returns all *.json under dir (recursive).
func WalkDir(dir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			out = append(out, p)
		}
		return nil
	})
	return out, err
}

// PrettyIssues renders the issues (or PASS).
func PrettyIssues(w io.Writer, path string, issues []string) {
	if len(issues) == 0 {
		fmt.Fprintf(w, "PASS: %s\n", path)
		return
	}
	fmt.Fprintf(w, "FAIL: %s\n", path)
	for _, is := range issues {
		fmt.Fprintf(w, "  - %s\n", is)
	}
}
