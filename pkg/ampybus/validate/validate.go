package validate

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type Fixture struct {
	Topic     string            `json:"topic"`
	Headers   map[string]string `json:"headers"`
	PayloadB64 string           `json:"payload_b64,omitempty"`
}

var (
	rMicSym     = regexp.MustCompile(`^[A-Z0-9]{4}\.[A-Z0-9.\-]+$`)              // XNAS.AAPL / XLON.VOD.L
	rFxPair     = regexp.MustCompile(`^[A-Z]{3}\.[A-Z]{3}$`)                      // USD.JPY
	rAcctOrder  = regexp.MustCompile(`^[A-Za-z0-9_\-]+[|][A-Za-z0-9_\-]+$`)       // ACCOUNT|CLIENT_ORDER_ID
	rAcctSym    = regexp.MustCompile(`^[A-Za-z0-9_\-]+[|][A-Z0-9.\-]+[.][A-Z0-9]{4}$`) // ACCOUNT|AAPL.XNAS
)

func ValidateDir(root string) error {
	var errs []error
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil { errs = append(errs, werr); return nil }
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") { return nil }
		fixt, err := loadFixture(path)
		if err != nil { errs = append(errs, fmt.Errorf("%s: %w", path, err)); return nil }
		if vErr := validateFixture(path, fixt); vErr != nil {
			errs = append(errs, vErr)
		} else {
			fmt.Printf("[ok] %s\n", path)
			count++
		}
		return nil
	})
	if err != nil { return err }
	if len(errs) > 0 {
		fmt.Println("---- failures ----")
		for _, e := range errs { fmt.Println(e) }
		return fmt.Errorf("validation failed: %d error(s)", len(errs))
	}
	if count == 0 {
		return errors.New("no fixtures found")
	}
	return nil
}

func loadFixture(path string) (Fixture, error) {
	var f Fixture
	b, err := os.ReadFile(path)
	if err != nil { return f, err }
	if err := json.Unmarshal(b, &f); err != nil { return f, err }
	return f, nil
}

func validateFixture(path string, f Fixture) error {
	if f.Topic == "" { return fmt.Errorf("%s: missing topic", path) }
	if !strings.HasPrefix(f.Topic, "ampy.") {
		return fmt.Errorf("%s: topic must start with 'ampy.'", path)
	}
	if f.Headers == nil { return fmt.Errorf("%s: headers missing", path) }

	sf := f.Headers["schema_fqdn"]
	if sf == "" { return fmt.Errorf("%s: headers.schema_fqdn required", path) }
	if !strings.HasPrefix(sf, "ampy.") {
		return fmt.Errorf("%s: schema_fqdn must start with 'ampy.'", path)
	}
	pk := f.Headers["partition_key"]
	if pk == "" { return fmt.Errorf("%s: headers.partition_key required", path) }

	// produced_at optional but if present must be RFC3339/Z
	if pa := f.Headers["produced_at"]; pa != "" {
		_, e1 := time.Parse(time.RFC3339Nano, pa)
		_, e2 := time.Parse(time.RFC3339, pa)
		if e1 != nil && e2 != nil {
			return fmt.Errorf("%s: produced_at must be RFC3339 or RFC3339Nano", path)
		}
	}

	// payload_b64 optional but if present must be valid base64
	if f.PayloadB64 != "" {
		if _, err := base64.StdEncoding.DecodeString(f.PayloadB64); err != nil {
			return fmt.Errorf("%s: payload_b64 invalid base64: %v", path, err)
		}
	}

	// domain-specific rules from topic: ampy.{env}.{domain}.{version}.{subtopic...}
	toks := strings.Split(f.Topic, ".")
	if len(toks) < 5 {
		return fmt.Errorf("%s: topic should be ampy.{env}.{domain}.{version}.{subtopic}", path)
	}
	domain := toks[2]
	version := toks[3]
	subtopic := strings.Join(toks[4:], ".")
	if version != "v1" { /* allow future */ }

	switch domain {
	case "bars":
		if !rMicSym.MatchString(subtopic) {
			return fmt.Errorf("%s: bars subtopic must be MIC.SYMBOL (got %q)", path, subtopic)
		}
		if pk != subtopic {
			return fmt.Errorf("%s: bars partition_key must equal subtopic (MIC.SYMBOL)", path)
		}
		if sf != "ampy.bars.v1.BarBatch" {
			return fmt.Errorf("%s: bars schema_fqdn must be ampy.bars.v1.BarBatch", path)
		}
	case "ticks":
		// e.g., trade.MSFT or quote.MSFT — only require a dot
		if !strings.Contains(subtopic, ".") {
			return fmt.Errorf("%s: ticks subtopic must contain a '.' (e.g., trade.MSFT)", path)
		}
	case "news":
		if subtopic != "raw" && subtopic != "nlp" {
			return fmt.Errorf("%s: news subtopic must be 'raw' or 'nlp'", path)
		}
		if f.Headers["dedupe_key"] == "" {
			return fmt.Errorf("%s: news requires headers.dedupe_key", path)
		}
		if sf != "ampy.news.v1.NewsItem" {
			return fmt.Errorf("%s: news schema_fqdn must be ampy.news.v1.NewsItem", path)
		}
	case "fx":
		// either "rates" or "BASE.QUOTE"
		if subtopic != "rates" && !rFxPair.MatchString(subtopic) {
			return fmt.Errorf("%s: fx subtopic must be 'rates' or BASE.QUOTE", path)
		}
		if !rFxPair.MatchString(pk) {
			return fmt.Errorf("%s: fx partition_key must be BASE.QUOTE", path)
		}
		if sf != "ampy.fx.v1.FxRate" {
			return fmt.Errorf("%s: fx schema_fqdn must be ampy.fx.v1.FxRate", path)
		}
	case "signals":
		// recommended: model@date|SYMBOL.MIC
		if !strings.Contains(pk, "|") {
			return fmt.Errorf("%s: signals partition_key should contain '|', got %q", path, pk)
		}
		if sf != "ampy.signals.v1.Signal" {
			return fmt.Errorf("%s: signals schema_fqdn must be ampy.signals.v1.Signal", path)
		}
	case "orders":
		if subtopic != "requests" {
			return fmt.Errorf("%s: orders subtopic must be 'requests'", path)
		}
		if f.Headers["dedupe_key"] == "" {
			return fmt.Errorf("%s: orders require headers.dedupe_key (client_order_id)", path)
		}
		if pk != f.Headers["dedupe_key"] {
			return fmt.Errorf("%s: orders partition_key must equal dedupe_key (client_order_id)", path)
		}
		if sf != "ampy.orders.v1.OrderRequest" {
			return fmt.Errorf("%s: orders schema_fqdn must be ampy.orders.v1.OrderRequest", path)
		}
	case "fills":
		if subtopic != "events" {
			return fmt.Errorf("%s: fills subtopic must be 'events'", path)
		}
		if !rAcctOrder.MatchString(pk) {
			return fmt.Errorf("%s: fills partition_key must match ACCOUNT|CLIENT_ORDER_ID", path)
		}
		if sf != "ampy.fills.v1.Fill" {
			return fmt.Errorf("%s: fills schema_fqdn must be ampy.fills.v1.Fill", path)
		}
	case "positions":
		if subtopic != "snapshots" {
			return fmt.Errorf("%s: positions subtopic must be 'snapshots'", path)
		}
		if !rAcctSym.MatchString(pk) {
			return fmt.Errorf("%s: positions partition_key must match ACCOUNT|SYMBOL.MIC", path)
		}
		if sf != "ampy.positions.v1.Position" {
			return fmt.Errorf("%s: positions schema_fqdn must be ampy.positions.v1.Position", path)
		}
	case "metrics":
		// best-effort only
	default:
		// allow other domains for now
	}

	// optional schema_hash sanity
	if sh := f.Headers["schema_hash"]; sh != "" && !strings.HasPrefix(sh, "nameonly:sha256:") {
		return fmt.Errorf("%s: schema_hash must start with 'nameonly:sha256:'", path)
	}
	return nil
}
