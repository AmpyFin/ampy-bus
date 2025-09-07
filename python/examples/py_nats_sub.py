import argparse
import asyncio
import ssl
import sys
import time

from ampybus.nats_bus import NATSBus
from ampybus.codec import decode_payload
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
    ap.add_argument("--subject", required=True)
    ap.add_argument("--durable", default="py-subscriber")
    ap.add_argument("--metrics", default="", help="Prometheus listen address (e.g., :9103)")
    ap.add_argument("--otel", action="store_true")
    ap.add_argument("--otel-endpoint", default="", help="OTLP/gRPC endpoint (e.g., localhost:4317)")
    ap.add_argument("--exit-after", type=int, default=0, help="Exit after N messages (0=run forever)")
    ap.add_argument("--timeout-sec", type=float, default=0.0, help="Exit non-zero if no message within N seconds (0=disabled)")
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
        init_tracer("ampybus-py-sub", args.otel_endpoint or None)

    bus = NATSBus(args.nats)
    ssl_ctx = make_ssl(args)
    await bus.connect(tls=ssl_ctx, user=args.user or "", password=args.passw or "", token=args.token or "")
    await bus.ensure_stream()

    received = 0
    last = time.time()
    done = asyncio.Event()
    exit_code = 0  # 0=ok, 2=timeout

    async def handler(subject, headers, data: bytes):
        nonlocal received, last
        raw = decode_payload(data, headers.content_encoding)
        print(f"[py-consume] subj={subject} msg_id={headers.message_id} schema={headers.schema_fqdn} bytes={len(data)} decoded={len(raw)} pk={headers.partition_key}", flush=True)
        received += 1
        last = time.time()
        if args.exit_after and received >= args.exit_after:
            done.set()

    async def watchdog():
        nonlocal exit_code
        if args.timeout_sec <= 0:
            return
        while not done.is_set():
            await asyncio.sleep(0.2)
            if (time.time() - last) > args.timeout_sec:
                print(f"[py-consume] timeout after {args.timeout_sec}s with {received} msg(s)", file=sys.stderr, flush=True)
                exit_code = 2
                done.set()
                return

    sub_task = asyncio.create_task(bus.subscribe_pull(args.subject, args.durable, handler))
    wd_task = asyncio.create_task(watchdog())

    await done.wait()

    # graceful shutdown
    for t in (sub_task, wd_task):
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    await bus.close()
    sys.exit(exit_code)

if __name__ == "__main__":
    asyncio.run(main())
