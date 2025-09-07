// pkg/ampybus/schemahash_test.go
package ampybus

import (
	"strings"
	"testing"
)

func TestExpectedSchemaHashFallback(t *testing.T) {
	fqdn := "ampy.bars.v1.BarBatch" // not linked yet; should use name-only fallback
	h := ExpectedSchemaHash(fqdn)
	if !strings.HasPrefix(h, "nameonly:sha256:") {
		t.Fatalf("expected fallback prefix %q, got %q", "nameonly:sha256:", h)
	}
}


func TestVerifySchemaHash(t *testing.T) {
	fqdn := "ampy.bars.v1.BarBatch"
	exp := ExpectedSchemaHash(fqdn)
	h := Headers{SchemaFQDN: fqdn, SchemaHash: exp}
	if err := VerifySchemaHash(h); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}
