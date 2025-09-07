from google.protobuf.empty_pb2 import Empty
from ampybus.envelope import Envelope, new_headers, encode_protobuf, decode_payload, pk_bars

def test_roundtrip():
    h = new_headers("ampy.bars.v1.BarBatch", "tester@host", "tester", pk_bars("XNAS","AAPL"))
    payload, enc = encode_protobuf(Empty(), compress_threshold=0)
    h.content_encoding = enc
    env = Envelope(topic="ampy.prod.bars.v1.XNAS.AAPL", headers=h, payload=payload)
    h.validate_basic()
    raw = decode_payload(env.payload, env.headers.content_encoding)
    assert raw == payload
    assert len(h.trace_id) == 32
    assert h.span_id is None or len(h.span_id) == 16
