package ampybus

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ExpectedSchemaHash returns the canonical schema hash string for a given FQDN.
// If the message descriptor is available in the global registry, it hashes the
// FileDescriptorProto bytes + FQDN ("sha256:<hex>").
// Otherwise, returns a stable fallback "nameonly:sha256:<hex>" based only on FQDN.
func ExpectedSchemaHash(schemaFQDN string) string {
	if h, ok := computeSchemaHashFromRegistry(schemaFQDN); ok {
		return h
	}
	// Fallback: stable name-only hash so producers/consumers still agree
	sum := sha256.Sum256([]byte("fqdn:" + schemaFQDN))
	return "nameonly:sha256:" + hex.EncodeToString(sum[:])
}

// VerifySchemaHash compares headers.SchemaHash (if present) with the expected value.
// Returns nil when empty (not enforced) or when matching; otherwise returns a mismatch error.
func VerifySchemaHash(h Headers) error {
	if h.SchemaHash == "" {
		return nil // not enforced
	}
	exp := ExpectedSchemaHash(h.SchemaFQDN)
	if h.SchemaHash == exp {
		return nil
	}
	return fmt.Errorf("schema_hash_mismatch: expected=%s got=%s fqdn=%s", exp, h.SchemaHash, h.SchemaFQDN)
}

func computeSchemaHashFromRegistry(schemaFQDN string) (string, bool) {
	name := protoreflect.FullName(schemaFQDN)
	md, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return "", false
	}
	fd := md.Descriptor().ParentFile()
	fdp := protodesc.ToFileDescriptorProto(fd)
	b, err := (&proto.MarshalOptions{Deterministic: true}).Marshal(fdp)
	if err != nil {
		return "", false
	}
	h := sha256.New()
	_, _ = h.Write(b)
	_, _ = h.Write([]byte(md.Descriptor().FullName()))
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), true
}
