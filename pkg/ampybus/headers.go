package ampybus

const (
	ContentTypeProtobuf    = "application/x-protobuf"
	ContentTypeJSON        = "application/json" // for diagnostics only
	EncodingGzip           = "gzip"
	DefaultCompressThreshold = 128 * 1024 // 128 KiB
)
