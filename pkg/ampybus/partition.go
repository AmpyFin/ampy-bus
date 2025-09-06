package ampybus

import (
	"crypto/rand"
	"fmt"

	"github.com/google/uuid"
)

// Domain-specific partition key helpers

func PartitionKeyBars(mic, symbol string) string         { return fmt.Sprintf("%s.%s", mic, symbol) }
func PartitionKeyTicks(mic, symbol string) string        { return fmt.Sprintf("%s.%s", mic, symbol) } // trade/quote via subtopic
func PartitionKeyNewsID(id string) string                { return id }
func PartitionKeyFX(base, quote string) string           { return fmt.Sprintf("%s.%s", base, quote) }
func PartitionKeySignals(modelID, symbol, mic, horizon string) string {
	return fmt.Sprintf("%s|%s.%s|%s", modelID, symbol, mic, horizon)
}
func PartitionKeyOrders(clientOrderID string) string { return clientOrderID }
func PartitionKeyFills(accountID, clientOrderID string) string {
	return fmt.Sprintf("%s|%s", accountID, clientOrderID)
}
func PartitionKeyPositions(accountID, symbol, mic string) string {
	return fmt.Sprintf("%s|%s.%s", accountID, symbol, mic)
}
func PartitionKeyMetrics(service, metricName string) string {
	return fmt.Sprintf("%s|%s", service, metricName)
}

// NewUUID returns a sortable UUIDv7 if available; falls back to v4.
func NewUUID() string {
	if v7, err := uuid.NewV7(); err == nil {
		return v7.String()
	}
	return uuid.NewString()
}

// NewTraceID returns a 16-byte random hex string (trace id).
func NewTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%x", b[:])
}
