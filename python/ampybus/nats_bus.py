import asyncio
import ssl
from datetime import timezone
from typing import Callable, Awaitable, Optional

from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig, RetentionPolicy, StorageType, ConsumerConfig
from nats.js.errors import NotFoundError
from nats.errors import TimeoutError as NATSTimeoutError

from .headers import Headers
from .schemahash import verify_schema_hash
from . import metrics

# OpenTelemetry
from opentelemetry import trace, propagate
from opentelemetry.trace import SpanKind

DLQ_PREFIX = "ampy.prod.dlq.v1."

class NATSBus:
    def __init__(self, urls: str, stream: str = "AMPY"):
        self._urls = urls or "nats://127.0.0.1:4222"
        self._stream = stream
        self._nc: Optional[NATS] = None
        self._js = None

    async def connect(self, tls: Optional[ssl.SSLContext | bool] = None, user: str = "", password: str = "", token: str = ""):
        opts = {}
        if tls is not None:
            opts["tls"] = tls
        if user or password:
            opts["user"] = user
            opts["password"] = password
        if token:
            opts["token"] = token
        self._nc = NATS()
        await self._nc.connect(servers=[self._urls], **opts)
        self._js = self._nc.jetstream()

    async def close(self):
        if self._nc and not self._nc.is_closed:
            await self._nc.drain()

    async def ensure_stream(self, subjects: list[str] | None = None):
        subjects = subjects or ["ampy.>"]
        try:
            await self._js.stream_info(self._stream)
        except NotFoundError:
            cfg = StreamConfig(
                name=self._stream,
                subjects=subjects,
                retention=RetentionPolicy.Limits,
                storage=StorageType.FILE,
                no_ack=False,
                replicas=1,
            )
            await self._js.add_stream(cfg)

    async def publish_envelope(self, topic: str, headers: Headers, payload: bytes, extra_headers: dict[str, str] | None = None):
        headers.validate_basic()

        h = {
            "message_id": headers.message_id,
            "schema_fqdn": headers.schema_fqdn,
            "schema_version": headers.schema_version,
            "content_type": headers.content_type,
        }
        if headers.content_encoding:
            h["content_encoding"] = headers.content_encoding

        # times and ids
        h["produced_at"] = headers.produced_at.astimezone(timezone.utc).isoformat()
        h["producer"] = headers.producer
        h["source"] = headers.source
        h["run_id"] = headers.run_id
        h["partition_key"] = headers.partition_key
        if headers.dedupe_key:  h["dedupe_key"] = headers.dedupe_key
        if headers.retry_count: h["retry_count"] = str(headers.retry_count)
        if headers.dlq_reason:  h["dlq_reason"] = headers.dlq_reason
        if headers.schema_hash: h["schema_hash"] = headers.schema_hash
        if headers.blob_ref:    h["blob_ref"] = headers.blob_ref
        if headers.blob_hash:   h["blob_hash"] = headers.blob_hash
        if headers.blob_size:   h["blob_size"] = str(headers.blob_size)

        if extra_headers:
            for k, v in extra_headers.items():
                if v:
                    h[k] = v

        tracer = trace.get_tracer("ampybus")
        # Start PRODUCER span and inject W3C trace headers
        with tracer.start_as_current_span("bus.publish", kind=SpanKind.PRODUCER) as span:
            span.set_attribute("messaging.system", "nats")
            span.set_attribute("messaging.operation", "publish")
            span.set_attribute("messaging.destination", topic)
            span.set_attribute("ampy.schema_fqdn", headers.schema_fqdn)

            carrier: dict[str, str] = {}
            propagate.inject(carrier)
            if "traceparent" in carrier:
                h["traceparent"] = carrier["traceparent"]
            # legacy ids (optional)
            sc = span.get_span_context()
            h["trace_id"] = f"{sc.trace_id:032x}"
            h["span_id"]  = f"{sc.span_id:016x}"

            ack = await self._js.publish(subject=topic, payload=payload, headers=h)

            # Metrics (producer)
            metrics.inc_produced(topic, headers.producer)
            metrics.observe_batch_size(topic, len(payload))
            return ack

    async def subscribe_pull(
        self,
        subject: str,
        durable: str,
        handler: Callable[[str, Headers, bytes], Awaitable[None]],
        *,
        batch: int = 10,
        timeout_sec: float = 2.0,
        idle_sleep_sec: float = 0.1,
    ):
        cfg = ConsumerConfig(
            durable_name=durable,
            filter_subject=subject,
            ack_policy="explicit",
            replay_policy="instant",
            deliver_policy="all",
        )
        try:
            await self._js.add_consumer(self._stream, cfg)
        except Exception:
            pass

        sub = await self._js.pull_subscribe(subject=subject, durable=durable)

        tracer = trace.get_tracer("ampybus")

        while True:
            try:
                msgs = await sub.fetch(batch, timeout=timeout_sec)
            except NATSTimeoutError:
                await asyncio.sleep(idle_sleep_sec)
                continue

            for msg in msgs:
                try:
                    mh = msg.headers or {}
                    # Extract parent context
                    parent = propagate.extract(dict(mh))

                    h = Headers(
                        message_id=mh.get("message_id", ""),
                        schema_fqdn=mh.get("schema_fqdn", ""),
                        schema_version=mh.get("schema_version", "1.0.0"),
                        content_type=mh.get("content_type", "application/x-protobuf"),
                        content_encoding=mh.get("content_encoding", ""),
                        producer=mh.get("producer", ""),
                        source=mh.get("source", ""),
                        run_id=mh.get("run_id", ""),
                        trace_id=mh.get("trace_id", ""),
                        span_id=mh.get("span_id", ""),
                        partition_key=mh.get("partition_key", ""),
                        dedupe_key=mh.get("dedupe_key", ""),
                        retry_count=int(mh.get("retry_count", "0") or "0"),
                        dlq_reason=mh.get("dlq_reason", ""),
                        schema_hash=mh.get("schema_hash", ""),
                        blob_ref=mh.get("blob_ref", ""),
                        blob_hash=mh.get("blob_hash", ""),
                        blob_size=int(mh.get("blob_size", "0") or "0"),
                    )
                    pa = mh.get("produced_at", "")
                    if pa:
                        try:
                            from datetime import datetime
                            h.produced_at = datetime.fromisoformat(pa.replace("Z", "+00:00"))
                        except Exception:
                            pass

                    h.validate_basic()
                    verify_schema_hash(h.schema_fqdn, h.schema_hash)

                    # Metrics (pre-handler)
                    metrics.inc_consumed(msg.subject, durable)
                    metrics.observe_batch_size(msg.subject, len(msg.data))
                    if h.produced_at:
                        from datetime import datetime
                        metrics.observe_delivery_latency(msg.subject, h.produced_at, datetime.now(timezone.utc))

                    # Start CONSUMER span linked to parent
                    with tracer.start_as_current_span("bus.consume", context=parent, kind=SpanKind.CONSUMER) as span:
                        span.set_attribute("messaging.system", "nats")
                        span.set_attribute("messaging.operation", "process")
                        span.set_attribute("messaging.destination", msg.subject)
                        await handler(msg.subject, h, msg.data)
                        await msg.ack()
                except Exception as e:
                    metrics.inc_decode_fail(msg.subject, str(e))
                    metrics.inc_dlq(msg.subject, str(e))
                    await self._js.publish(
                        subject=DLQ_PREFIX + msg.subject,
                        payload=msg.data,
                        headers={**(msg.headers or {}), "dlq_reason": str(e)},
                    )
                    await msg.ack()
