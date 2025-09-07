from __future__ import annotations
from datetime import datetime, timezone
from prometheus_client import Counter, Histogram, start_http_server
import threading

# Counters
produced_total = Counter(
    "bus_produced_total", "Messages produced.", ["topic", "producer"]
)
consumed_total = Counter(
    "bus_consumed_total", "Messages consumed.", ["topic", "consumer"]
)
dlq_total = Counter(
    "bus_dlq_total", "Messages routed to DLQ.", ["topic", "reason"]
)
decode_fail_total = Counter(
    "bus_decode_fail_total", "Payload decode/handler failures.", ["topic", "reason"]
)

# Histograms
delivery_latency_ms = Histogram(
    "bus_delivery_latency_ms",
    "Latency (ms) from produced_at to consume.",
    ["topic"],
    buckets=[1, 2, 5, 10, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 2000, 5000],
)
batch_size_bytes = Histogram(
    "bus_batch_size_bytes",
    "Payload size in bytes.",
    ["topic"],
    buckets=[32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072],
)

# One-time HTTP server bootstrap
__server_started = False
__lock = threading.Lock()

def start_metrics_server(addr: str) -> None:
    """
    Start a Prometheus /metrics server.
    Accepts ':9103' or '0.0.0.0:9103' or '127.0.0.1:9103'.
    Safe to call multiple times; it only starts once.
    """
    global __server_started
    with __lock:
        if __server_started:
            return
        a = (addr or "").strip()
        if not a:
            return
        if a.startswith(":"):
            host, port = "0.0.0.0", int(a[1:])
        elif ":" in a:
            host, port = a.split(":", 1)[0], int(a.split(":", 1)[1])
        else:
            # If just a number, treat as :PORT
            host, port = "0.0.0.0", int(a)
        start_http_server(port, addr=host)
        __server_started = True

# Convenience helpers (match Go names)
def inc_produced(topic: str, producer: str) -> None:
    produced_total.labels(topic=topic, producer=producer).inc()

def inc_consumed(topic: str, consumer: str) -> None:
    consumed_total.labels(topic=topic, consumer=consumer).inc()

def observe_batch_size(topic: str, size_bytes: int) -> None:
    if size_bytes is not None and size_bytes >= 0:
        batch_size_bytes.labels(topic=topic).observe(float(size_bytes))

def observe_delivery_latency(topic: str, produced_at: datetime, now: datetime | None = None) -> None:
    if not isinstance(produced_at, datetime):
        return
    if produced_at.tzinfo is None:
        produced_at = produced_at.replace(tzinfo=timezone.utc)
    tznow = now or datetime.now(timezone.utc)
    ms = (tznow - produced_at).total_seconds() * 1000.0
    if ms >= 0:
        delivery_latency_ms.labels(topic=topic).observe(ms)

def inc_dlq(topic: str, reason: str) -> None:
    dlq_total.labels(topic=topic, reason=str(reason)[:80]).inc()

def inc_decode_fail(topic: str, reason: str) -> None:
    decode_fail_total.labels(topic=topic, reason=str(reason)[:80]).inc()
