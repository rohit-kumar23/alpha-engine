# Component Testing Guidelines (Pre-Test Blueprint)

This document defines *what to test* and *how to test* every major component before writing actual test code.

For the `tests/` directory layout, build integration, and commands to run suites, see [`COMPONENT_TESTING_SUITE.md`](COMPONENT_TESTING_SUITE.md).

## Important Reality

Absolute "100% correctness" cannot be proven for a live trading system in all market/network conditions.  
What you can achieve is:

- strong correctness guarantees for deterministic logic
- high confidence via exhaustive edge-case coverage
- controlled behavior under failure and recovery paths

This blueprint is designed for that level of rigor.

## Testing Principles

- Test deterministic logic first (pure functions, state machines, parsers).
- Test safety-critical logic before performance optimizations.
- Test failure paths as deeply as happy paths.
- Keep tests reproducible (seeded randomness, fixed fixtures, fixed clocks where possible).
- Every bug fix must add a regression test.
- No silent behavior changes: define explicit expected outcomes and invariants.

## Test Layers

- **Unit tests:** isolated component behavior with fixtures/mocks.
- **Integration tests:** multiple components wired together, controlled I/O.
- **Replay tests:** deterministic playback of captured market/user-stream data.
- **Fault-injection tests:** network/API/time/queue failures and recovery behavior.
- **Soak tests:** long-running stability and resource behavior.

## Global Invariants (Must Always Hold)

- No invalid OMS state transition.
- No order send when kill switch is engaged.
- No order send if risk check rejects command.
- No undefined behavior when MD stream is stale/out-of-sync.
- Snapshot reseed must restore consistent book state.
- Reconcile healing must be bounded by configured per-tick limits.
- Queue overflow/drop counters must reflect actual drops.
- Deterministic components must produce identical outputs for identical inputs.

## Component-by-Component Test Scope

## 1) `marketdata` (parser, ws client, snapshot client, endpoints, types)

### What to test

- JSON event parsing:
  - valid `bookTicker`, `depth`, `aggTrade` messages
  - malformed/missing fields
  - unknown symbols/event types
- Depth sequence logic prerequisites (`U/u/pu` semantics consumed by book layer).
- Endpoint mode selection (`demo` vs `live`) and defaults.
- Snapshot fetch behavior:
  - success body parsing
  - HTTP/non-200 handling
  - malformed snapshot payload handling

### Edge/failure cases

- Partial JSON frames, empty payloads, oversized payloads.
- Timestamp anomalies (older/newer than expected).
- Unsupported symbol strings.
- Reconnect/idle behavior signal correctness.

### Pass criteria

- Parser rejects malformed input safely.
- Valid fixtures map to exact expected normalized events.
- No crash on malformed payloads.

## 2) `orderbook` (`L2Book`)

### What to test

- Snapshot seed:
  - proper best bid/ask, spread, sizes
  - in-sync transitions
- Incremental depth apply:
  - add/update/remove levels
  - best-price changes
  - depth crossing prevention behavior
- Out-of-sync detection and resync-required signaling.
- Buffered depth replay correctness after reseed.

### Edge/failure cases

- Empty book sides.
- Zero/negative quantities in updates.
- Duplicate price levels.
- Sequence gaps and non-monotonic updates.

### Pass criteria

- Exact expected `BookSnapshot` for deterministic fixtures.
- Correct state transitions on sequence continuity breaks.

## 3) `strategy` (`market_maker`)

### What to test

- Intent emission only when prerequisites are valid.
- Edge threshold gating behavior.
- Inventory limit blocking.
- Quantity sizing and inventory shrink logic.
- Buy/sell decision logic around micro vs mid relation.

### Edge/failure cases

- Zero/invalid bid/ask.
- Extremely high inventory values.
- Boundary values around edge threshold.

### Pass criteria

- Deterministic intent outputs for all fixture snapshots.
- No intent when any precondition fails.

## 4) `ordermgmt` (`OrderManager`, `OmsState`)

### `OrderManager` what to test

- New vs replace vs cancel command generation.
- Opposite-side cancel behavior.
- Stale/adverse cancel triggers.
- Replace threshold logic.
- Reconcile drop behavior (missing remote ids).

### `OmsState` what to test

- Full lifecycle transitions:
  - PendingNew -> Live -> Completed/Rejected
  - PendingReplace/PendingCancel flows
- Idempotency on duplicate ack/reject/cancel paths.
- Invalid transition accounting.
- Pending-timeout healing and reconcile marking behavior.

### Edge/failure cases

- Out-of-order exec reports.
- Late acks after terminal states.
- Duplicate cancel attempts.

### Pass criteria

- State machine ends in expected state for every transition script.
- Invalid transitions counted exactly where expected.

## 5) `riskmgmt` (`PreTradeRisk`)

### What to test

- Quantity, notional, and projected position checks.
- Kill switch behavior.
- Position update on fill and retrieval by symbol.

### Edge/failure cases

- Boundary values exactly at limits.
- Unknown instrument enum values.
- Negative/zero quantities and prices.

### Pass criteria

- Correct `RiskRejectReason` for each violating case.
- No false accepts when any hard limit is violated.

## 6) `execution` (gateway, user stream parser/client)

### Gateway what to test

- Query building for New/Replace/Cancel.
- Signature generation not empty for valid inputs.
- HTTP response classification (`ok`, status, exchange error code).
- Retry and backoff behavior.
- Special idempotent handling cases (`-2011`, duplicate client id scenarios).
- REST weight header extraction and storage.

### User stream parser what to test

- `ORDER_TRADE_UPDATE` mapping to `ExecReport`.
- terminal-state detection from event/status combos.
- client order id extraction from `hft_<id>`.

### Edge/failure cases

- Unknown execution type/status.
- Missing nested `"o"` payload.
- Invalid numeric field parsing.

### Pass criteria

- Parser outputs exact expected `ExecReport` fixtures.
- Gateway logic produces expected command/query/handling behavior.

## 7) `analytics` (`PnLEngine`)

### What to test

- Realized PnL updates for buy/sell fills.
- Average price and inventory tracking.
- Fee accounting.
- Mark-to-market output.

### Edge/failure cases

- Inventory crosses through zero.
- Zero inventory reset behavior.
- Tiny quantities and precision-sensitive values.

### Pass criteria

- Numerically stable outputs within strict tolerance bounds.

## 8) `coreinfra` (SPSC ring, audit logger)

### What to test

- SPSC push/pop order and data integrity.
- Full/empty behavior under pressure.
- Non-blocking behavior assumptions.
- Audit log enqueue/dequeue and bounded drop counter behavior.

### Edge/failure cases

- Producer faster than consumer.
- Shutdown while queue still has entries.
- Very long lines and truncation handling.

### Pass criteria

- No data corruption under stress fixtures.
- Bounded and observable drop behavior only when expected.

## 9) `application` (`main` orchestration)

### What to test

- Startup preflight behavior (env parsing and mode selection).
- Thread startup/affinity failure handling paths.
- Main loop batching behavior (`main_md_batch_max`, exec-report batch).
- Snapshot request/response scheduling and backoff.
- Reconcile thread enable/disable conditions.
- Kill switch toggling behavior path.

### Edge/failure cases

- Missing API key/secret combinations.
- Repeated snapshot failures.
- Reconcile mismatch streak handling.
- REST throttle/cooldown activation.

### Pass criteria

- No crash or undefined state transitions under controlled failure injections.

## Cross-Component Scenario Matrix

Use these scenario scripts in integration/replay tests:

1. Normal market flow with low volatility.
2. Burst market updates causing queue pressure.
3. Depth out-of-sync -> snapshot reseed -> trading resume.
4. User stream delayed while reconcile heals local state.
5. Risk breach while strategy still emits intents.
6. Kill switch toggled during active orders.
7. Gateway throttle errors and cooldown path.
8. Drawdown guard trips and cooldown recovery.

## Data/Fixture Requirements

- Curated JSON fixtures for each Binance event type.
- Synthetic transition scripts for OMS/OrderManager.
- Snapshot + incremental depth fixture pairs for sequence tests.
- Replay datasets:
  - calm regime
  - volatile regime
  - failure-heavy regime

## Acceptance Gate Before "Correctness Complete"

- Unit coverage complete for all deterministic logic.
- Integration scenario matrix passes.
- Replay tests are deterministic (same output on repeated runs).
- Fault-injection suite passes for defined safety behaviors.
- No unresolved critical/severity-1 defects.

## Recommended Execution Order

1. `marketdata` parser tests  
2. `orderbook` sequence/snapshot tests  
3. `ordermgmt` + `riskmgmt` state/guard tests  
4. `execution` parser/gateway behavior tests  
5. `strategy` deterministic intent tests  
6. `analytics` PnL tests  
7. full `application` integration and replay suites  

## What This Document Is Not

- This is not implementation code.
- This is not a proof of correctness.
- This is the blueprint to implement a high-confidence correctness program.
