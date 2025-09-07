import os

def _hex(nbytes: int) -> str:
    return os.urandom(nbytes).hex()

def make_traceparent(trace_id: str | None, span_id: str | None, sampled: bool = True) -> str:
    """
    W3C traceparent: 00-<32hex traceid>-<16hex spanid>-<flags>
    """
    tid = (trace_id or "").strip().lower()
    sid = (span_id or "").strip().lower()
    if len(tid) != 32:
        tid = _hex(16)
    if len(sid) != 16:
        sid = _hex(8)
    flags = "01" if sampled else "00"
    return f"00-{tid}-{sid}-{flags}"
