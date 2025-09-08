import argparse
import asyncio
import ssl
import sys
import time
from typing import Optional

from ampybus.nats_bus import NATSBus
from ampybus.codec import decode_payload
from ampybus.metrics import start_metrics_server
from ampybus.otel import init_tracer

# Import NATS client directly for fallback
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NATSTimeoutError

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

async def subscribe_direct_nats(nats_url: str, subject: str, handler, timeout_sec: float = 10.0):
    """Fallback method using direct NATS subscription without JetStream."""
    print(f"[fallback] Using direct NATS subscription to {subject}")
    
    nc = NATS()
    try:
        await nc.connect(servers=[nats_url])
        print(f"[fallback] Connected to NATS at {nats_url}")
        
        received = 0
        
        async def message_handler(msg):
            nonlocal received
            try:
                # Parse headers manually
                headers = msg.headers or {}
                from ampybus.headers import Headers
                
                h = Headers(
                    message_id=headers.get("message_id", ""),
                    schema_fqdn=headers.get("schema_fqdn", ""),
                    schema_version=headers.get("schema_version", "1.0.0"),
                    content_type=headers.get("content_type", "application/x-protobuf"),
                    content_encoding=headers.get("content_encoding", ""),
                    producer=headers.get("producer", ""),
                    source=headers.get("source", ""),
                    run_id=headers.get("run_id", ""),
                    trace_id=headers.get("trace_id", ""),
                    span_id=headers.get("span_id", ""),
                    partition_key=headers.get("partition_key", ""),
                    dedupe_key=headers.get("dedupe_key", ""),
                    retry_count=int(headers.get("retry_count", "0") or "0"),
                    dlq_reason=headers.get("dlq_reason", ""),
                    schema_hash=headers.get("schema_hash", ""),
                    blob_ref=headers.get("blob_ref", ""),
                    blob_hash=headers.get("blob_hash", ""),
                    blob_size=int(headers.get("blob_size", "0") or "0"),
                )
                
                # Parse produced_at timestamp
                pa = headers.get("produced_at", "")
                if pa:
                    try:
                        from datetime import datetime
                        h.produced_at = datetime.fromisoformat(pa.replace("Z", "+00:00"))
                    except Exception:
                        pass
                
                await handler(msg.subject, h, msg.data)
                received += 1
                
            except Exception as e:
                print(f"[fallback] Error processing message: {e}")
        
        # Subscribe to the subject
        sub = await nc.subscribe(subject, cb=message_handler)
        print(f"[fallback] Subscribed to {subject}")
        
        # Wait for timeout
        await asyncio.sleep(timeout_sec)
        
        # Unsubscribe
        await sub.unsubscribe()
        print(f"[fallback] Unsubscribed, received {received} messages")
        
    except Exception as e:
        print(f"[fallback] Error: {e}")
    finally:
        if not nc.is_closed:
            await nc.drain()

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
    ap.add_argument("--fallback", action="store_true", help="Use direct NATS subscription if JetStream fails")
    args = ap.parse_args()

    if args.metrics:
        start_metrics_server(args.metrics)
    if args.otel:
        init_tracer("ampybus-py-sub", args.otel_endpoint or None)

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

    # Try JetStream first, fallback to direct NATS if it fails
    use_fallback = args.fallback
    
    try:
        if not use_fallback:
            print("[jetstream] Attempting JetStream subscription...")
            bus = NATSBus(args.nats)
            ssl_ctx = make_ssl(args)
            await bus.connect(tls=ssl_ctx, user=args.user or "", password=args.passw or "", token=args.token or "")
            await bus.ensure_stream()

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
        else:
            raise Exception("Fallback mode requested")
            
    except Exception as e:
        print(f"[jetstream] JetStream failed: {e}")
        print("[fallback] Switching to direct NATS subscription...")
        
        # Use direct NATS subscription
        wd_task = asyncio.create_task(watchdog())
        fallback_task = asyncio.create_task(
            subscribe_direct_nats(args.nats, args.subject, handler, args.timeout_sec or 10.0)
        )
        
        try:
            await asyncio.gather(fallback_task, wd_task, return_exceptions=True)
        except Exception as fallback_error:
            print(f"[fallback] Fallback also failed: {fallback_error}")
            exit_code = 1

    sys.exit(exit_code)

if __name__ == "__main__":
    asyncio.run(main())
