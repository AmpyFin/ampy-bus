import argparse
import asyncio
import ssl

from google.protobuf.empty_pb2 import Empty
from ampybus.headers import new_headers
from ampybus.codec import encode_protobuf, DEFAULT_COMPRESS_THRESHOLD
from ampybus.nats_bus import NATSBus
from ampybus.metrics import start_metrics_server
from ampybus.otel import init_tracer

def make_ssl(args):
    if not (args.tls_ca or args.tls_cert or args.tls_key):
        return None
    ctx = ssl.create_default_context(cafile=args.tls_ca if args.tls_ca else None)
    if args.tls_cert and args.tls_key:
        ctx.load_cert_chain(args.tls_cert, args.tls_key)
    if args.tls_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default="nats://127.0.0.1:4222")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--producer", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--pk", required=True)
    ap.add_argument("--count", type=int, default=1)
    ap.add_argument("--metrics", default="", help="Prometheus listen address (e.g., :9104)")
    ap.add_argument("--otel", action="store_true")
    ap.add_argument("--otel-endpoint", default="", help="OTLP/gRPC endpoint (e.g., localhost:4317)")
    ap.add_argument("--tls-ca")
    ap.add_argument("--tls-cert")
    ap.add_argument("--tls-key")
    ap.add_argument("--tls-insecure", action="store_true")
    ap.add_argument("--user")
    ap.add_argument("--passw")
    ap.add_argument("--token")
    args = ap.parse_args()

    if args.metrics:
        start_metrics_server(args.metrics)
    if args.otel:
        init_tracer("ampybus-py-pub", args.otel_endpoint or None)

    bus = NATSBus(args.nats)
    ssl_ctx = make_ssl(args)
    await bus.connect(tls=ssl_ctx, user=args.user or "", password=args.passw or "", token=args.token or "")
    await bus.ensure_stream()

    for _ in range(args.count):
        h = new_headers("ampy.bars.v1.BarBatch", args.producer, args.source, args.pk)
        payload, enc = encode_protobuf(Empty(), DEFAULT_COMPRESS_THRESHOLD)
        h.content_encoding = enc
        await bus.publish_envelope(args.topic, h, payload, extra_headers=None)

    await bus.close()
    print(f"Published {args.count}")

if __name__ == "__main__":
    asyncio.run(main())
