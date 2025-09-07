import gzip
from google.protobuf.message import Message

DEFAULT_COMPRESS_THRESHOLD = 128 * 1024  # 128 KiB

def encode_protobuf(msg: Message, compress_threshold: int = DEFAULT_COMPRESS_THRESHOLD):
    raw = msg.SerializeToString()
    if len(raw) >= compress_threshold:
        return gzip.compress(raw), "gzip"
    return raw, ""

def decode_payload(data: bytes, content_encoding: str) -> bytes:
    enc = (content_encoding or "").strip().lower()
    if enc == "" or enc == "identity":
        return data
    if enc == "gzip":
        return gzip.decompress(data)
    # extend here if you add more encodings
    return data
