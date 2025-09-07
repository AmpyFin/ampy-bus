from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
import uuid
try:
    from uuid6 import uuid7  # type: ignore
except Exception:
    uuid7 = None  # fallback to uuid4

from .schemahash import expected_schema_hash

@dataclass
class Headers:
    message_id: str
    schema_fqdn: str
    schema_version: str = "1.0.0"
    content_type: str = "application/x-protobuf"
    content_encoding: str = ""
    produced_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    producer: str = ""
    source: str = ""
    run_id: str = ""
    trace_id: str = ""
    span_id: str = ""
    partition_key: str = ""
    dedupe_key: str = ""
    retry_count: int = 0
    dlq_reason: str = ""
    schema_hash: str = ""
    blob_ref: str = ""
    blob_hash: str = ""
    blob_size: int = 0

    def validate_basic(self) -> None:
        if not self.message_id:
            raise ValueError("missing message_id")
        if not self.schema_fqdn:
            raise ValueError("missing schema_fqdn")
        if not self.content_type:
            raise ValueError("missing content_type")
        if not isinstance(self.produced_at, datetime):
            raise ValueError("produced_at must be datetime")
        if not self.producer:
            raise ValueError("missing producer")
        if not self.source:
            raise ValueError("missing source")
        if not self.run_id:
            raise ValueError("missing run_id")
        if not self.partition_key:
            raise ValueError("missing partition_key")

def _uuid_v7() -> str:
    if uuid7 is not None:
        return str(uuid7())
    return str(uuid.uuid4())

def new_headers(schema_fqdn: str, producer: str, source: str, partition_key: str) -> Headers:
    h = Headers(
        message_id=_uuid_v7(),
        schema_fqdn=schema_fqdn,
        producer=producer,
        source=source,
        run_id="run_local",
        trace_id="",  # optional; can be set by caller
        partition_key=partition_key,
    )
    # auto-fill schema_hash (fallback mode)
    h.schema_hash = expected_schema_hash(schema_fqdn)
    return h
