from google.protobuf.empty_pb2 import Empty
from ampybus.envelope import (
    Envelope, new_headers, encode_protobuf, decode_payload,
    pk_bars,
)

def main():
    h = new_headers(
        schema_fqdn="ampy.bars.v1.BarBatch",
        producer="example-producer@host-1",
        source="example-source",
        partition_key=pk_bars("XNAS", "AAPL"),
    )
    payload, enc = encode_protobuf(Empty())
    h.content_encoding = enc
    env = Envelope(topic="ampy.prod.bars.v1.XNAS.AAPL", headers=h, payload=payload)

    h.validate_basic()
    raw = decode_payload(env.payload, env.headers.content_encoding)
    print(f"OK: topic={env.topic}, payload={len(env.payload)} bytes, decoded={len(raw)} bytes, msg_id={env.headers.message_id}")

if __name__ == "__main__":
    main()
