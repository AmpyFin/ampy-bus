from __future__ import annotations
import hashlib
from google.protobuf import symbol_database

_symdb = symbol_database.Default()

def expected_schema_hash(schema_fqdn: str) -> str:
    """
    If schema_fqdn is a linked protobuf type, hash its FileDescriptorProto bytes + fqdn.
    Else return a stable name-only fallback.
    """
    try:
        md = _symdb.pool.FindMessageTypeByName(schema_fqdn)
        # md.file.serialized_pb is the FileDescriptorProto bytes
        file_bytes = md.file.serialized_pb
        h = hashlib.sha256()
        h.update(file_bytes)
        h.update(schema_fqdn.encode("utf-8"))
        return "sha256:" + h.hexdigest()
    except KeyError:
        h = hashlib.sha256()
        h.update(("fqdn:" + schema_fqdn).encode("utf-8"))
        return "nameonly:sha256:" + h.hexdigest()

def verify_schema_hash(schema_fqdn: str, schema_hash: str | None) -> None:
    if not schema_hash:
        return
    exp = expected_schema_hash(schema_fqdn)
    if schema_hash != exp:
        raise ValueError(f"schema_hash_mismatch: expected={exp} got={schema_hash} fqdn={schema_fqdn}")
