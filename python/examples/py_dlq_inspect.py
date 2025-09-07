import argparse, asyncio, json, os, ssl
from datetime import datetime, timezone

from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError
from nats.errors import TimeoutError as NATSTimeoutError

from ampybus.codec import decode_payload

STREAM = "AMPY"
DLQ_PREFIX = "ampy.prod.dlq.v1."

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
    ap.add_argument("--subject", default="ampy.prod.dlq.v1.>")
    ap.add_argument("--durable", default="py-dlq-inspect")
    ap.add_argument("--max", type=int, default=50)
    ap.add_argument("--decode", action="store_true")
    ap.add_argument("--outdir", default="./dlq_dump")
    ap.add_argument("--tls-ca"); ap.add_argument("--tls-cert"); ap.add_argument("--tls-key"); ap.add_argument("--tls-insecure", action="store_true")
    ap.add_argument("--user"); ap.add_argument("--passw"); ap.add_argument("--token")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    nc = NATS()
    ssl_ctx = make_ssl(args)
    kwargs = {}
    if ssl_ctx: kwargs["tls"] = ssl_ctx
    if args.user or args.passw: kwargs.update(user=args.user or "", password=args.passw or "")
    if args.token: kwargs["token"] = args.token

    await nc.connect(servers=[args.nats], **kwargs)
    js = nc.jetstream()

    # Ensure stream exists (creates if missing)
    try:
        await js.stream_info(STREAM)
    except NotFoundError:
        cfg = StreamConfig(name=STREAM, subjects=["ampy.>"], retention=RetentionPolicy.Limits, storage=StorageType.FILE, replicas=1)
        await js.add_stream(cfg)

    # Ensure consumer
    cfg = ConsumerConfig(
        durable_name=args.durable,
        filter_subject=args.subject,
        ack_policy="explicit",
        replay_policy="instant",
        deliver_policy="all",
    )
    try:
        await js.add_consumer(STREAM, cfg)
    except Exception:
        pass

    sub = await js.pull_subscribe(subject=args.subject, durable=args.durable)

    processed = 0
    try:
        while processed < args.max:
            try:
                msgs = await sub.fetch(min(10, args.max - processed), timeout=2)
            except NATSTimeoutError:
                break
            for msg in msgs:
                mh = dict(msg.headers or {})
                subj = msg.subject
                enc = mh.get("content_encoding", "")
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                base = f"{ts}_{mh.get('message_id','noid')}"

                bin_path = os.path.join(args.outdir, base + ".bin")
                with open(bin_path, "wb") as f: f.write(msg.data)

                meta = {
                    "dlq_subject": subj,
                    "original_subject": subj[len(DLQ_PREFIX):] if subj.startswith(DLQ_PREFIX) else "",
                    "headers": mh,
                    "payload_file": os.path.abspath(bin_path),
                }

                if args.decode:
                    try:
                        raw = decode_payload(msg.data, enc)
                        raw_path = os.path.join(args.outdir, base + ".raw")
                        with open(raw_path, "wb") as f: f.write(raw)
                        meta["decoded_file"] = os.path.abspath(raw_path)
                        meta["decoded_len"] = len(raw)
                    except Exception as e:
                        meta["decode_error"] = str(e)

                with open(os.path.join(args.outdir, base + ".json"), "w") as f:
                    json.dump(meta, f, indent=2)

                await msg.ack()
                processed += 1
                print(f"[dlq-inspect] saved {base}.json")
                if processed >= args.max:
                    break
    finally:
        await nc.drain()

    print(f"[dlq-inspect] processed {processed} message(s)")

if __name__ == "__main__":
    asyncio.run(main())
