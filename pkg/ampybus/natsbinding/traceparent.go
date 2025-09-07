package natsbinding

import "fmt"

// Build a W3C traceparent string from 16-byte hex traceID and 8-byte hex spanID.
// Sampled=01 by default.
func MakeTraceparent(traceID, spanID string, sampled bool) string {
	flag := "00"
	if sampled {
		flag = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", traceID, spanID, flag)
}
