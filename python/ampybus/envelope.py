from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Tuple
import uuid, gzip, io
import secrets

from google.protobuf.message import Message

CONTENT_TYPE_PROTOBUF = "application/x-protobuf"
ENCODING_GZIP = "gzip"
DEFAULT_COMPRESS_THRESHOLD = 128 * 1024  # 128 KiB

@dataclass
class Headers:
    message_id: str
    schema_fqdn: str
    schema_version: str = "1.0.0"
    content_type: str = CONTENT_TYPE_PROTOBUF
    content_encoding: Optional[str] = None
    produced_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    producer: str = ""
    source: str = ""
    run_id: str = ""
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    span_id: Optional[str] = None
    partition_key: str = ""

    # Optional
    dedupe_key: Optional[str] = None
    retry_count: Optional[int] = None
    dlq_reason: Optional[str] = None
    schema_hash: Optional[str] = None
    blob_ref: Optional[str] = None
    blob_hash: Optional[str] = None
    blob_size: Optional[int] = None

    def validate_basic(self) -> None:
        if not self.message_id:
            raise ValueError("message_id required")
        uuid.UUID(self.message_id)  # raises if invalid
        if not self.schema_fqdn:
            raise ValueError("schema_fqdn required")
        if self.content_type != CONTENT_TYPE_PROTOBUF:
            raise ValueError("content_type must be application/x-protobuf")
        if not self.produced_at.endswith("Z"):
            raise ValueError("produced_at must be UTC RFC3339 w/ 'Z'")
        if not self.producer:
            raise ValueError("producer required")
        if not self.source:
            raise ValueError("source required")
        if not self.run_id:
            raise ValueError("run_id required")
        if not self.trace_id:
            raise ValueError("trace_id required")
        if not self.partition_key:
            raise ValueError("partition_key required")
        if len(self.trace_id) != 32:
            raise ValueError("trace_id must be 16 bytes hex (32 chars)")
        if self.span_id is not None and len(self.span_id) != 16:
            raise ValueError("span_id must be 8 bytes hex (16 chars)")
        # basic RFC3339Z parsing sanity check
        try:
            datetime.fromisoformat(self.produced_at.replace("Z", "+00:00"))
        except Exception as e:
            raise ValueError("produced_at must be RFC3339 UTC") from e


@dataclass
class Envelope:
    topic: str
    headers: Headers
    payload: bytes

def new_headers(schema_fqdn: str, producer: str, source: str, partition_key: str) -> Headers:
    trace_id, span_id = new_trace()
    return Headers(
        message_id=str(uuid.uuid4()),
        schema_fqdn=schema_fqdn,
        producer=producer,
        source=source,
        run_id="example_run",
        partition_key=partition_key,
        trace_id=trace_id,
        span_id=span_id,
        produced_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


def encode_protobuf(msg: Message, compress_threshold: int = DEFAULT_COMPRESS_THRESHOLD) -> Tuple[bytes, Optional[str]]:
    raw = msg.SerializeToString()
    if compress_threshold and len(raw) >= compress_threshold:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(raw)
        return buf.getvalue(), ENCODING_GZIP
    return raw, None

def decode_payload(payload: bytes, content_encoding: Optional[str]) -> bytes:
    if content_encoding == ENCODING_GZIP:
        with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as f:
            return f.read()
    return payload

def new_trace() -> tuple[str, str]:
    # 16-byte (32 hex chars) trace id, 8-byte (16 hex) span id
    return secrets.token_hex(16), secrets.token_hex(8)

# Partition key helpers
def pk_bars(mic: str, symbol: str) -> str: return f"{mic}.{symbol}"
def pk_ticks(mic: str, symbol: str) -> str: return f"{mic}.{symbol}"
def pk_news_id(id_: str) -> str: return id_
def pk_fx(base: str, quote: str) -> str: return f"{base}.{quote}"
def pk_signals(model_id: str, symbol: str, mic: str, horizon: str) -> str:
    return f"{model_id}|{symbol}.{mic}|{horizon}"
def pk_orders(client_order_id: str) -> str: return client_order_id
def pk_fills(account_id: str, client_order_id: str) -> str:
    return f"{account_id}|{client_order_id}"
def pk_positions(account_id: str, symbol: str, mic: str) -> str:
    return f"{account_id}|{symbol}.{mic}"
def pk_metrics(service: str, metric_name: str) -> str:
    return f"{service}|{metric_name}"
