import argparse, asyncio, ssl

from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, StreamConfig, RetentionPolicy, StorageType
from nats.js.errors import NotFoundError
from nats.errors import TimeoutError as NATSTimeoutError

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
    ap.add_argument("--durable", default="py-dlq-redrive")
    ap.add_argument("--max", type=int, default=100)
    ap.add_argument("--tls-ca"); ap.add_argument("--tls-cert"); ap.add_argument("--tls-key"); ap.add_argument("--tls-insecure", action="store_true")
    ap.add_argument("--user"); ap.add_argument("--passw"); ap.add_argument("--token")
    args = ap.parse_args()

    nc = NATS()
    ssl_ctx = make_ssl(args)
    kwargs = {}
    if ssl_ctx: kwargs["tls"] = ssl_ctx
    if args.user or args.passw: kwargs.update(user=args.user or "", password=args.passw or "")
    if args.token: kwargs["token"] = args.token

    await nc.connect(servers=[args.nats], **kwargs)
    js = nc.jetstream()

    try:
        await js.stream_info(STREAM)
    except NotFoundError:
        cfg = StreamConfig(name=STREAM, subjects=["ampy.>"], retention=RetentionPolicy.Limits, storage=StorageType.FILE, replicas=1)
        await js.add_stream(cfg)

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

    redriven = 0
    try:
        while redriven < args.max:
            try:
                msgs = await sub.fetch(min(10, args.max - redriven), timeout=2)
            except NATSTimeoutError:
                break
            for msg in msgs:
                mh = dict(msg.headers or {})
                dlq_subject = msg.subject
                if not dlq_subject.startswith(DLQ_PREFIX):
                    await msg.ack()
                    continue
                target = dlq_subject[len(DLQ_PREFIX):]

                # Remove the DLQ-only header to avoid loops
                mh = {k: v for k, v in mh.items() if k.lower() != "dlq_reason"}
                await js.publish(subject=target, payload=msg.data, headers=mh)
                await msg.ack()

                redriven += 1
                print(f"[dlq-redrive] {dlq_subject} -> {target}")
                if redriven >= args.max:
                    break
    finally:
        await nc.drain()

    print(f"[dlq-redrive] redriven {redriven} message(s)")

if __name__ == "__main__":
    asyncio.run(main())
