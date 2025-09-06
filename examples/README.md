# Golden Envelopes (Examples)

These files are **contract fixtures** for `ampy-bus`. They illustrate the **envelope + headers** and provide an **illustrative JSON mapping** of the protobuf payloads. The **canonical payload definitions** live in `ampy-proto`.

## Files
- `envelope.schema.json` — JSON Schema for envelope structure and required headers.
- `*_v1_*.json` — Golden envelope examples per domain (bars, ticks, news, fx, signals, orders, fills, positions, metrics, dlq, control).

## How to Use
1. **Validation**: Use `envelope.schema.json` to validate envelope shape during tests.
2. **Cross-language Round-Trip**: Serialize a real `ampy-proto` payload and ensure you can produce an envelope matching these headers, then consume and decode it in Go/Python/C++ identically.
3. **Fault Injection**: Duplicate `message_id` and/or `dedupe_key`, corrupt payload, or omit required headers to confirm DLQ routing and metrics.
4. **Replay**: Use the `control_v1_replay_request.json` shape to drive a time-window replay and assert idempotency of downstream sinks.

> Notes:
> - `payload_json` is **illustrative** for human inspection; production publishes **protobuf bytes** with `content_type = application/x-protobuf`.
> - Timestamps are **UTC**. Partition keys follow the **domain guidance** in the main project README.
