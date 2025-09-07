import argparse, asyncio, ssl

from ampybus.headers import new_headers
from ampybus.nats_bus import NATSBus

def make_ssl(args):
    if not (args.tls_ca or args.tls_cert or args.tls_key):
        return None
    ctx = ssl.create_default_context(cafile=args.tls_ca or None)
    if args.tls_cert and args.tls_key:
        ctx.load_cert_chain(args.tls_cert, args.tls_key)
    if args.tls_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nats", default="nats://127.0.0.1:4222")
    ap.add_argument("--topic", default="ampy.prod.bars.v1.XNAS.AAPL")
    ap.add_argument("--producer", default="poison@py")
    ap.add_argument("--source", default="poison-test")
    ap.add_argument("--pk", default="XNAS.AAPL")
    ap.add_argument("--tls-ca"); ap.add_argument("--tls-cert"); ap.add_argument("--tls-key"); ap.add_argument("--tls-insecure", action="store_true")
    ap.add_argument("--user"); ap.add_argument("--passw"); ap.add_argument("--token")
    args = ap.parse_args()

    bus = NATSBus(args.nats)
    ssl_ctx = make_ssl(args)
    await bus.connect(tls=ssl_ctx, user=args.user or "", password=args.passw or "", token=args.token or "")
    await bus.ensure_stream()

    h = new_headers("ampy.bars.v1.BarBatch", args.producer, args.source, args.pk)
    h.content_encoding = "gzip"   # lie: payload isn't gzipped
    payload = b"NOT-GZIP-POISON"

    await bus.publish_envelope(args.topic, h, payload, extra_headers={"dlq_inject_reason": "decode_error_test"})
    await bus.close()
    print("[poison] published invalid gzip payload")

if __name__ == "__main__":
    asyncio.run(main())
