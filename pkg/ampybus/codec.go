package ampybus

import (
	"bytes"
	"compress/gzip"
	"io"
	"time"

	"google.golang.org/protobuf/proto"
)

// EncodeProtobuf serializes msg; compresses if >= threshold (bytes).
// Returns (payload, contentEncoding).
func EncodeProtobuf(msg proto.Message, compressThreshold int) ([]byte, string, error) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil, "", err
	}
	if compressThreshold > 0 && len(raw) >= compressThreshold {
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		zw.Name = "payload.pb"
		zw.ModTime = time.Now()
		if _, err := zw.Write(raw); err != nil {
			_ = zw.Close()
			return nil, "", err
		}
		if err := zw.Close(); err != nil {
			return nil, "", err
		}
		return buf.Bytes(), EncodingGzip, nil
	}
	return raw, "", nil
}

// DecodePayload reverses encoding; if gzip, it gunzips to raw protobuf bytes.
func DecodePayload(payload []byte, contentEncoding string) ([]byte, error) {
	if contentEncoding != EncodingGzip {
		return payload, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}
