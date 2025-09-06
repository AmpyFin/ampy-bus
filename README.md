# ampy-bus — Messaging Conventions & Helpers (Open Source)

> **Purpose:** Define **transport‑agnostic, versioned messaging contracts** and operating rules for how AmpyFin services publish, subscribe, route, replay, and observe events built on top of `ampy-proto`. This README is **LLM‑ready**: it specifies *what to build and how it should behave* without prescribing implementation details or repository structure. Include it alongside the AmpyFin background so an LLM can design and implement the system.

---

## 0) TL;DR

- A single, consistent **messaging layer** for all AmpyFin subsystems (ingestion, research/ML, backtesting, execution, monitoring).
- **Transport‑agnostic**: works on NATS or Kafka with the **same** envelopes, topics, headers, and QoS semantics.
- Uses **`ampy-proto`** as the only payload source of truth; the bus defines **envelopes + headers** around those payloads.
- Clear **ordering keys, retries, DLQ, replay/backfill**, and **observability** rules so the system is reproducible and auditable.
- Initial helper libraries: **Go** (primary) and **Python**, with example producers/consumers (no business logic).

---

## 1) Problem Statement

AmpyFin is a modular, self‑learning trading system. Teams naturally want different transports (Kafka vs NATS) and different ingestion sources (Databento C++, yfinance Go, Tiingo, Marketbeat, FX rates, etc.). Without a shared messaging contract, systems drift:
- **Schema drift & bespoke adapters** between services
- **Ambiguous delivery semantics** (ordering, idempotency, retries)
- **Poor replayability** for research and audits
- **Inconsistent metrics/logging** across services

**ampy-bus** solves this by specifying the **contract**, not the broker—so modules can be swapped or scaled independently with **zero message‑shape drift** and **predictable delivery semantics**.

---

## 2) Mission & Success Criteria

### Mission
Provide a **single, consistent messaging layer** for all AmpyFin subsystems such that modules are independently deployable and replayable.

### Success looks like
- Any producer can emit `ampy-proto` payloads with **identical envelopes and headers**; any consumer can parse them without adapters.
- Topics and headers encode **schema identity, lineage, and version**, enabling deterministic replays/audits.
- Clear **QoS tiers** and **ordering keys** by domain (e.g., `(symbol, mic)` for prices, `client_order_id` for orders).
- **Observed latency** and **throughput** meet SLOs across live and replay paths.
- **Backpressure**, **retries**, **DLQ**, and **recovery** behaviors are consistent and testable.

---

## 3) Scope (What `ampy-bus` Covers)

- **Transport‑agnostic contract** for topics, envelopes, headers, keys, ordering, retries, DLQ, replay, and observability.
- **Domain‑specific guidance**: bars, ticks, news, FX, fundamentals, corporate actions, universe, signals, orders, fills, positions, metrics.
- **Performance & SLO targets**, backpressure handling, and capacity planning guidance.
- **Security & compliance** norms for trading workloads (authn/z, TLS, PII policy, auditability).
- **Helper libraries**: Go (NATS/Kafka clients), Python helpers for envelope encode/decode and validation.

**Non‑goals**: No broker‑specific configuration or business logic. No repository layout in this README.

---

## 4) Design Principles

1. **`ampy-proto` is the source of truth** for payloads (e.g., `ampy.bars.v1.BarBatch`). No new payload shapes.
2. **Envelope wraps payload** with headers for lineage, routing, and observability.
3. **Time is UTC**. Distinguish: `event_time` (market/source), `ingest_time` (ingestion), `as_of` (logical processing time).
4. **Stable identity** via `SecurityId` where securities are referenced.
5. **Idempotency by default**: stable `message_id` (UUIDv7) plus domain `dedupe_key` when available.
6. **Compatibility**: additive evolution only within a major; breaking changes bump the payload major version (`v2` topics).
7. **Serialization**: `application/x-protobuf` (primary). Optional diagnostic JSON for human inspection only.
8. **Compression**: if payload > 128 KiB, `content_encoding="gzip"` and compress the bytes.
9. **Size limits**: target < 1 MiB; otherwise use **object‑storage pointer** pattern (§10).

---

## 5) Topic Taxonomy & Namespacing

**Canonical pattern** (slashes shown for readability; use `.` separators in broker subjects when appropriate):

```
ampy.{env}.{domain}.{version}.{subtopic}
```

- `env`: `dev` | `paper` | `prod`
- `domain`: `bars` | `ticks` | `fundamentals` | `news` | `fx` | `corporate_actions` | `universe` | `signals` | `orders` | `fills` | `positions` | `metrics` | `dlq` | `control`
- `version`: `v1`, `v2` (mirrors **payload** major version in `ampy-proto`)
- `subtopic`: domain‑specific segment(s) to enforce locality & ordering, e.g.:
  - `bars`: `{mic}.{symbol}` → `XNAS.AAPL`
  - `ticks`: `trade.{symbol}` or `quote.{symbol}`
  - `news`: `raw` or `nlp`
  - `fx`: `rates` or `{base}.{quote}`
  - `signals`: `{model_id}` (e.g., `hyper@2025-09-05`)
  - `orders`: `requests`
  - `fills`: `events`
  - `positions`: `snapshots`
  - `metrics`: `{service}`

**Examples**
- `ampy.prod.bars.v1.XNAS.AAPL`
- `ampy.paper.orders.v1.requests`
- `ampy.prod.signals.v1.hyper@2025-09-05`

> Consumers may subscribe using broker‑native wildcards/prefixes; producers should publish to concrete subjects.

---

## 6) Envelope & Headers (Contract)

Each published record = **Envelope + Payload** (`ampy-proto` bytes).

### 6.1 Required Headers

| Header | Type | Example | Purpose |
|---|---|---|---|
| `message_id` | UUIDv7 | `018F5E2F-9B1C-76AA-8F7A-3B1D8F3EA0C2` | Global unique id; sortable for time‑ordering; dedupe anchor |
| `schema_fqdn` | string | `ampy.bars.v1.BarBatch` | Exact payload message type (`ampy-proto`) |
| `schema_version` | semver | `1.0.0` | Schema minor/patch for diagnostics; major is in topic |
| `content_type` | string | `application/x-protobuf` | Serialization hint |
| `content_encoding` | string | `gzip` (or omitted) | Compression indicator |
| `produced_at` | RFC3339 UTC | `2025-09-05T19:31:01Z` | When producer created this record |
| `producer` | string | `yfinance-go@ingest-1` | Logical service instance id |
| `source` | string | `yfinance-go` \| `databento-cpp` | Upstream/source system identity |
| `run_id` | string | `live_0912` | Correlates records for a pipeline run/session |
| `trace_id` / `span_id` | W3C traceparent | `00-...` | End‑to‑end tracing |
| `partition_key` | string | `XNAS.AAPL` | Sharding/ordering key (domain‑specific) |

### 6.2 Optional Headers

- `dedupe_key` — domain idempotency key (e.g., `client_order_id`, news `id`)
- `retry_count` — incremented on republish after failure
- `dlq_reason` — set by infrastructure when routing to DLQ
- `schema_hash` — hash of compiled schema for defensive checks
- `blob_ref`, `blob_hash`, `blob_size` — pointer pattern for oversized payloads (§10)

---

## 7) Delivery Semantics, Ordering & Keys (by Domain)

> The helper libraries will implement **transport‑specific bindings** that respect these logical guarantees.

**Defaults**  
- QoS: **at‑least‑once** with **idempotent consumers**
- Ordering: guaranteed **within a partition key**

**Recommended Keys & Guarantees**

| Domain | Partition/Ordering Key | Notes |
|---|---|---|
| `bars` | `(symbol, mic)` → `XNAS.AAPL` | Monotonic by `event_time` within key |
| `ticks` | `(symbol, mic)`; subtopics `trade.`/`quote.` | Extremely high‑rate; separate subtopics |
| `news` | `id` | Dedupe by `id` |
| `fx` | `(base, quote)` | Snapshot semantics; latest wins |
| `fundamentals` | `(symbol, mic, period_end, source)` | Consumers handle restatements |
| `universe` | `universe_id` | Snapshots monotonic in `as_of` |
| `signals` | `(model_id, symbol, mic, horizon)` | Latest prior to `expires_at` wins |
| `orders` | `client_order_id` | Strict causal order submit → amend/cancel |
| `fills` | `(account_id, client_order_id)` | Arrival may be out‑of‑order; accumulate |
| `positions` | `(account_id, symbol, mic)` | Monotonic `as_of` per key |
| `metrics` | `(service, metric_name)` | Counters/gauges semantics |

---

## 8) Error Handling, Retries, Backpressure, DLQ

- **Producer retries**: exponential backoff with jitter; ceilings per QoS class
- **Consumer retries**: bounded attempts; on persistent failure → **DLQ** with original headers + `dlq_reason`
- **Backpressure**: consumers signal lag (transport‑specific) → producers reduce batch size/pause low‑priority topics
- **Poison pills**: decode or contract violations → DLQ + metrics/alerts; never drop silently
- **Idempotency**: consumers dedupe by `message_id` and domain `dedupe_key` (if present)

---

## 9) Large Payloads — Object Storage Pointer Pattern

If payload exceeds thresholds:
1. Publish a **pointer envelope** with `blob_ref` (e.g., `s3://bucket/key?versionId=...`) and metadata (`blob_hash`, `blob_size`).
2. Consumers fetch object out‑of‑band, validate hash, then process.
3. Replays retain blobs for the retention window.

---

## 10) Replay & Backfill

- **Time‑window replay** for time‑series domains (bars/ticks/news/fx): specify `[start, end)` in UTC
- **Key‑scoped replay** for orders/fills/positions: by `(account_id, client_order_id)` or `(account_id, symbol, mic)`
- **Idempotent sinks**: replays must be no‑ops on previously applied effects
- **Checkpointing**: consumers persist high‑watermarks (time or offset) per key/partition
- **Retention**: ≥ 7 days live logs (prod), ≥ 30 days analytical cluster; longer for compliance domains

**Control Topic**  
`ampy.{env}.control.v1.replay_requests` carries `ampy.control.v1.ReplayRequest` payloads.

---

## 11) Observability: Metrics, Logs, Traces

**Standard Metrics (examples)**
- `bus.produced_total{topic,producer}` — counter  
- `bus.consumed_total{topic,consumer}` — counter  
- `bus.delivery_latency_ms{topic}` — histogram (p50/p95/p99)  
- `bus.batch_size_bytes{topic}` — histogram  
- `bus.consumer_lag{topic,consumer}` — gauge  
- `bus.dlq_total{topic,reason}` — counter  
- `bus.retry_total{topic,reason}` — counter  
- `bus.decode_fail_total{topic,reason}` — counter  

**Logging**  
Structured JSON with `message_id`, `trace_id`, `topic`, `producer|consumer`, `result` (ok|retry|dlq), `latency_ms`. **Do not log payloads**.

**Tracing**  
Propagate **W3C traceparent**; spans for publish, route, consume, and downstream handling.

---

## 12) Security & Compliance

- **Encryption in transit**: TLS/mTLS
- **AuthN/Z**: topic‑level ACLs (read/write); producers/consumers authenticate
- **PII policy**: forbidden in bus payloads; orders must not contain customer PII
- **Auditability**: headers + payload hashes enable forensic reconstruction
- **Secrets**: retrieved via `ampy-config` (never hardcode)
- **Tenancy**: `dev` / `paper` / `prod` namespaces

> **API keys / credentials**: None required by `ampy-bus` itself. Broker bindings will need credentials (e.g., NATS auth token or Kafka SASL), and some producers (Marketbeat, Tiingo) may need API keys. We’ll prompt for those during binding setup.

---

## 13) Performance Targets (SLOs)

- **Latency (publish → first delivery)**  
  - Orders/Signals/Fills: **p99 ≤ 50 ms** (same‑AZ)  
  - Bars/Ticks: **p99 ≤ 150 ms**
- **Payload size**: < 1 MiB (typical 32–256 KiB); compress large batches
- **Availability**: ≥ **99.9%** monthly for prod bus plane
- **Recovery**: RPO ≤ **5 min**, RTO ≤ **15 min** (documented procedures)

---

## 14) Domain‑Specific Envelope Examples

> Shape and semantics only. Payload bodies are `ampy-proto` message types.

### 14.1 Bars batch (adjusted, 1‑minute)
```
Envelope:
 topic: "ampy.prod.bars.v1.XNAS.AAPL"
 headers: {
   "message_id": "018f5e2f-9b1c-76aa-8f7a-3b1d8f3ea0c2",
   "schema_fqdn": "ampy.bars.v1.BarBatch",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:31:01Z",
   "producer": "yfinance-go@ingest-1",
   "source": "yfinance-go",
   "run_id": "run_abc123",
   "trace_id": "4b5b3f2a0f9d4e3db4c8a1f0e3a7c812",
   "partition_key": "XNAS.AAPL"
 }
Payload:
 BarBatch (multiple Bar records for 19:30–19:31 window, adjusted=true)
```

### 14.2 Trade tick
```
Envelope:
 topic: "ampy.prod.ticks.v1.trade.MSFT"
 headers: {
   "message_id": "018f5e30-1a3b-7f9e-bccc-1e12a1c3e0d9",
   "schema_fqdn": "ampy.ticks.v1.TradeTick",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:30:12.462Z",
   "producer": "databento-cpp@tick-ingest-3",
   "source": "databento-cpp",
   "run_id": "live_0912",
   "trace_id": "a0c1b2d3e4f5061728394a5b6c7d8e9f",
   "partition_key": "MSFT.XNAS"
 }
Payload:
 TradeTick (event_time=...; price/size; venue=XNAS)
```

### 14.3 News item (dedupe by `id`)
```
Envelope:
 topic: "ampy.prod.news.v1.raw"
 headers: {
   "message_id": "018f5e31-0e1d-7b2a-9f7c-41acef2b9f01",
   "schema_fqdn": "ampy.news.v1.NewsItem",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T13:05:15Z",
   "producer": "marketbeat-go@news-2",
   "source": "marketbeat-go",
   "run_id": "news_live_37",
   "trace_id": "f2b1c7d9c4c34b3a9d0e4f5a9e2d8b11",
   "partition_key": "marketbeat:2025-09-05:amzn-headline-8b12c6",
   "dedupe_key": "marketbeat:2025-09-05:amzn-headline-8b12c6"
 }
Payload:
 NewsItem (headline/body/tickers; published_at=...; sentiment_score_bp=240)
```

### 14.4 FX snapshot
```
Envelope:
 topic: "ampy.prod.fx.v1.rates"
 headers: {
   "message_id": "018f5e31-3c55-76af-9421-fd10ce9bba75",
   "schema_fqdn": "ampy.fx.v1.FxRate",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:30:00Z",
   "producer": "fxrates-go@fx-1",
   "source": "fxrates-go",
   "run_id": "fx_145",
   "trace_id": "2f0a3c6e9b574c5e8b7a6d5c4b3a2f19",
   "partition_key": "USD.JPY"
 }
Payload:
 FxRate (bid/ask/mid; as_of=...)
```

### 14.5 Signal (ALPHA) and OMS order request
```
Envelope:
 topic: "ampy.prod.signals.v1.hyper@2025-09-05"
 headers: {
   "message_id": "018f5e32-7f1a-74d2-9a11-b53f54d8a911",
   "schema_fqdn": "ampy.signals.v1.Signal",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:31:03Z",
   "producer": "ampy-model-server@mdl-1",
   "source": "ampy-model-server",
   "run_id": "live_0912",
   "trace_id": "1c2d3e4f5061728394a5b6c7d8e9fa0b",
   "partition_key": "hyper@2025-09-05|NVDA.XNAS"
 }
Payload:
 Signal (type=ALPHA; score=-0.3450; horizon=5d)
```

```
Envelope:
 topic: "ampy.prod.orders.v1.requests"
 headers: {
   "message_id": "018f5e32-9b2a-7cde-9333-4f1ab2a49e77",
   "schema_fqdn": "ampy.orders.v1.OrderRequest",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:31:05Z",
   "producer": "ampy-oms@oms-2",
   "source": "ampy-oms",
   "run_id": "live_trading_44",
   "trace_id": "9f8e7d6c5b4a39281706f5e4d3c2b1a0",
   "partition_key": "co_20250905_001",
   "dedupe_key": "co_20250905_001"
 }
Payload:
 OrderRequest (account_id=ALPACA-LIVE-01; side=BUY; limit_price=191.9900)
```

### 14.6 Fill and Position snapshots
```
Envelope:
 topic: "ampy.prod.fills.v1.events"
 headers: {
   "message_id": "018f5e33-0a1b-71e3-980f-bcaa4c11902a",
   "schema_fqdn": "ampy.fills.v1.Fill",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:31:06Z",
   "producer": "broker-alpaca@alp-1",
   "source": "broker-alpaca",
   "run_id": "live_trading_44",
   "trace_id": "0a1b2c3d4e5f60718293a4b5c6d7e8f9",
   "partition_key": "ALPACA-LIVE-01|co_20250905_001"
 }
Payload:
 Fill (partial fill; price/quantity; venue=ALPACA)
```

```
Envelope:
 topic: "ampy.prod.positions.v1.snapshots"
 headers: {
   "message_id": "018f5e33-4b7d-72ac-8d24-d0a3e1b4c1e3",
   "schema_fqdn": "ampy.positions.v1.Position",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:35:00Z",
   "producer": "ampy-position-pnl@pnl-1",
   "source": "ampy-position-pnl",
   "run_id": "live_trading_44",
   "trace_id": "1029384756abcdef0123456789abcdef",
   "partition_key": "ALPACA-LIVE-01|AAPL.XNAS"
 }
Payload:
 Position (quantity/avg_price/unrealized/realized pnl; as_of=...)
```

### 14.7 Metrics
```
Envelope:
 topic: "ampy.prod.metrics.v1.ampy-oms"
 headers: {
   "message_id": "018f5e34-3b21-7c1f-b8e2-31b9e7fda4d0",
   "schema_fqdn": "ampy.metrics.v1.Metric",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:31:05Z",
   "producer": "ampy-oms@oms-2",
   "source": "ampy-oms",
   "run_id": "live_trading_44",
   "trace_id": "abcdef0123456789abcdef0123456789",
   "partition_key": "ampy-oms|oms.order_rejects"
 }
Payload:
 Metric (name=oms.order_rejects; labels={broker:alpaca, env:prod, reason:risk_check}; value=1)
```

### 14.8 DLQ example
```
Envelope:
 topic: "ampy.prod.dlq.v1.bars"
 headers: {
   "message_id": "018f5e35-0f42-7a31-9e77-1c2a9b11d0ef",
   "schema_fqdn": "ampy.bars.v1.BarBatch",
   "schema_version": "1.0.0",
   "content_type": "application/x-protobuf",
   "produced_at": "2025-09-05T19:31:02Z",
   "producer": "bus-router@plane-1",
   "source": "ampy-bus",
   "run_id": "bus_20250905",
   "trace_id": "feedfacecafebeef0011223344556677",
   "partition_key": "XNAS.AAPL",
   "dlq_reason": "decode_error: invalid decimal scale"
 }
Payload:
 (original payload bytes preserved; access controlled; include hash)
```

---

## 15) Broker Bindings (Implementation Guidance)

`ampy-bus` defines **logical** contracts. Helper libraries will implement:

### 15.1 NATS (suggested)
- Subject maps to topic (with `.` separators).  
- `partition_key` influences subject tokenization or JetStream stream sharding.  
- Headers carried via NATS message headers.  
- JetStream for durability, ack/replay, and consumer lag metrics.

### 15.2 Kafka (optional/parallel)
- Topic = `ampy.{env}.{domain}.{version}`; `subtopic` mapped to record key or additional topic segments.  
- `partition_key` used as Kafka key to guarantee per‑key order.  
- Headers map to Kafka record headers; consumer groups manage offsets/lag.

> Choose either or both. Contracts remain identical; only the binding differs.

---

## 16) Validation & Testing (What “Good” Looks Like)

- **Golden Envelopes**: ≥ 3 per domain (typical, minimal, edge/large).  
- **Cross‑language round‑trip**: Protobuf (Go/Python/C++) identical.  
- **Ordering tests**: per‑key monotonicity under concurrency.  
- **Idempotency tests**: duplicates by `message_id` and `dedupe_key` are no‑ops.  
- **Replay tests**: time‑window & key‑scoped replays do not double‑apply effects.  
- **Fault injection**: drop/duplicate/reorder/corrupt → DLQ + alerts.  
- **Load tests**: validate SLOs; backpressure signals propagate.

---

## 17) Security & Compliance Testing

- mTLS/TLS enforced; cert rotation validated.  
- ACLs: producers/consumers limited to permitted topics.  
- Audit tabletop: reconstruct a trading session from envelopes (headers + payload hashes).  
- Retention: meets policy for orders/fills compliance.

---

## 18) Acceptance Criteria (Definition of Done for v1)

1. Topic taxonomy, envelope header set, and per‑domain keys/ordering are **finalized and documented**.  
2. Golden envelope examples exist for **every domain** (≥3 each).  
3. SLO & capacity targets are documented and **validated by load tests**.  
4. Replay, DLQ, and backpressure behaviors are **proven** via fault‑injection tests.  
5. Security posture (TLS, ACLs, auditability) verified; **no PII** traverses the bus.  
6. Integration note maps each AmpyFin service to required topics and headers.

---

## 19) End‑to‑End Narrative (Cross‑Domain Flow)

1) **yfinance‑go** publishes **bars.v1** batches for `AAPL@XNAS` with `partition_key="XNAS.AAPL"`; compressed if needed.  
2) **ampy‑features** consumes bars, emits features internally, and **ampy‑model‑server** publishes **signals.v1** (`ALPHA` scores) to `signals/hyper@...`.  
3) **ampy‑ensemble** consumes multiple signals, emits final **ACTION** signals.  
4) **ampy‑oms** converts actions into **orders.v1** on `orders/requests` keyed by `client_order_id`, ensuring strict per‑order causality.  
5) **broker‑alpaca** publishes **fills.v1**, and **ampy‑position‑pnl** updates **positions.v1** snapshots.  
6) All services emit **metrics.v1**; dashboards show latency, lag, retries, and DLQ counts.  
7) If a gap is detected, an operator posts a **ReplayRequest** (control topic); consumers reprocess idempotently.

---

## 20) Integration Notes (per AmpyFin subsystem)

- **Data Ingestion**: Databento C++ (ticks), Tiingo/yfinance Go (bars/fundamentals), Marketbeat Go (news), custom FX‑rates Go client (USD/EUR/JPY/KRW etc.). All publish to bus with the same envelopes/headers.  
- **Research/ML**: feature extraction and model inference consume bars/ticks/news/fundamentals; publish `signals.v1`.  
- **Execution**: OMS consumes signals; publishes `orders.v1` and consumes `fills.v1`; positions calculated and published.  
- **Monitoring**: all services publish `metrics.v1` to a metrics sink; alerts on DLQ spikes/lag/latency.  
- **Compliance**: orders/fills/positions retained per policy; audit derives from headers and payload hashes.

---

## 21) Roadmap (post‑v1)

- **Helper SDKs**: `ampy-bus-go` and `ampy-bus-py` (envelopes, validation, tracing hooks, codecs).  
- **CLI tools**: produce/consume/replay testers; DLQ inspector.  
- **Schema registry hooks**: signature checks and schema hash enforcement.  
- **Reference bindings**: NATS JetStream and Kafka examples.  
- **Benchmarks**: publicly documented latency/throughput across brokers.

---

## 22) FAQ

**Q: Why Protobuf instead of Avro/JSON?**  
Protobuf gives compact, fast, cross‑language serialization and already underpins `ampy-proto`.

**Q: Can we use both NATS and Kafka?**  
Yes. Contracts are transport‑agnostic. Bindings map headers/keys appropriately.

**Q: Where do API keys live?**  
In each binding/producer via `ampy-config` or broker‑native secret stores. Never in code or headers.

**Q: How do we handle currency conversions/news IDs/etc.?**  
Those are **producers** (e.g., FX Go client, Marketbeat Go) that emit domain payloads. The bus contract remains unchanged.

---

## 23) Contributing

- Open an issue describing changes to topics/headers/QoS before sending PRs.  
- Include **golden envelopes** and **tests** for any new domain.  
- Follow semantic versioning for header changes (additive only) and bump payload major in topics for breaking payload changes.

---

## 24) License

**Proposed:** Apache‑2.0 (patent‑grant, enterprise‑friendly). *Confirm before first release.*

---

## 25) Badges / About (GitHub)

**About:**  
“Transport‑agnostic messaging conventions & helpers for AmpyFin. Standard topics, headers, QoS, replay, and observability over NATS or Kafka. Payloads are `ampy-proto`.”

**Topics:** `trading-systems`, `messaging`, `protobuf`, `nats`, `kafka`, `event-driven`, `fintech`, `observability`, `slo`, `open-source`, `ampyfin`

---

## 26) Implementation Checklist (for the LLM)

- [ ] Define header constants and envelope struct (Go & Python).  
- [ ] Encode/decode helpers for Protobuf bytes + gzip.  
- [ ] Validation: required headers present; types; UUIDv7 format; size thresholds.  
- [ ] Key selection helpers per domain.  
- [ ] NATS binding: publish/subscribe, JetStream config hints, tracing propagation.  
- [ ] Kafka binding: topic/key mapping, headers, consumer group offsets.  
- [ ] DLQ router + reasons; replay control publisher.  
- [ ] Metrics/logging/tracing hooks (prometheus + OpenTelemetry).  
- [ ] Golden envelopes + unit tests + fault injection tests.  
- [ ] Load generator & SLO verification scripts.  

---

**This README is the contract.** With it, an LLM (or human) can implement `ampy-bus` end‑to‑end without guessing, while leaving broker choice and internal code structure flexible.
