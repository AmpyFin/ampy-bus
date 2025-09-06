package ampybus

import (
	"crypto/rand"
	"encoding/hex"
)

// NewTrace returns a W3C-sized traceID (16 bytes hex) and spanID (8 bytes hex).
func NewTrace() (traceID string, spanID string) {
	var t [16]byte
	var s [8]byte
	_, _ = rand.Read(t[:])
	_, _ = rand.Read(s[:])
	return hex.EncodeToString(t[:]), hex.EncodeToString(s[:])
}
