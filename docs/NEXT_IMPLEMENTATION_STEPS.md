# Next Implementation Steps (Toward Real Money)

This is the practical sequence to move from the current codebase into a real-money capable HFT stack. The focus is risk-first execution, not speed-first shortcuts.

## Reality Check

You already have strong foundations in this repository:

- market data ingestion + book maintenance
- strategy/execution/risk separation
- OMS lifecycle + reconciliation paths
- exchange transport and controls

What is missing for "real money like early major HFTs" is not basic plumbing. The missing pieces are robust alpha research loops, hard operational safety, and measurable production reliability.

## Phase 1: Backtesting and Replay (Must Have Before More Strategy Work)

1. Build deterministic market-data replay from captured raw frames.
2. Add fill simulator with queue-position model and fee/slippage model.
3. Add strategy PnL attribution reports:
   - edge capture
   - adverse selection
   - inventory carry costs
   - cancel/replace efficiency
4. Add parameter sweep runner for per-symbol config search.

**Exit criteria:**
- same inputs always produce same decisions/results
- strategy KPIs stable over multiple non-overlapping date ranges

## Phase 2: Production Risk Layer Hardening

1. Add notional and position limits at multiple scopes:
   - per order
   - per symbol
   - global account
2. Add order-rate and cancel-rate hard guards independent of exchange limits.
3. Add kill-switch automation:
   - heartbeat-loss based
   - drawdown burst based
   - latency-spike based
4. Add startup preflight checks:
   - clock sync tolerance
   - key permissions and mode checks
   - exchange filters and precision sync

**Exit criteria:**
- every guard has deterministic behavior and audit trace
- forced-failure drills complete without undefined states

## Phase 3: Storage, Analytics, and Observability

1. Write fills/orders/events to a queryable store (SQLite/Postgres minimum).
2. Add structured metrics export (Prometheus/OpenTelemetry).
3. Build dashboards for:
   - quote-to-fill latency
   - reject/error rates
   - inventory and PnL trajectory
   - drawdown guard activations
4. Add alert rules (pager/telegram/slack):
   - no fills when expected
   - sudden reject spikes
   - stale market data
   - repeated reconcile mismatches

**Exit criteria:**
- you can explain any loss period in minutes from telemetry
- on-call alerts are actionable, not noisy

## Phase 4: Live Execution Quality Improvements

1. Add post-only and maker-protection logic where applicable.
2. Add queue-position-aware repricing policy (not only threshold-based replace).
3. Add regime-based spread and sizing controls (volatility/liquidity states).
4. Add anti-toxic-flow safeguards:
   - pull-back on adverse fill clusters
   - temporary spread widening under toxic regimes

**Exit criteria:**
- maker ratio and realized spread improve materially vs baseline
- lower adverse selection without killing fill rate

## Phase 5: Research Loop for Real Alpha

1. Build feature pipeline from L2/L3 dynamics:
   - microprice drift
   - short-horizon imbalance persistence
   - queue depletion and refill rates
2. Train and validate short-horizon models offline.
3. Deploy model inference as read-only signal first.
4. Gate live quoting changes behind canary mode and shadow evaluation.

**Exit criteria:**
- statistically robust out-of-sample edge after fees/slippage
- paper/live shadow performance tracks offline expectation

## Phase 6: Multi-Venue Expansion

1. Add second venue adapter (Bybit recommended).
2. Normalize instrument metadata and risk controls across venues.
3. Implement hedge routing and exposure netting.
4. Add cross-venue failure isolation (venue-level circuit breakers).

**Exit criteria:**
- venue failure does not destabilize overall book and exposure
- hedge operations are deterministic and audited

## Immediate Priority Checklist (Do These Next)

1. Secrets hygiene: rotate exchange keys and move secrets out tracked files.
2. Build deterministic replay runner from captured live/demo feeds.
3. Add persistent orders/fills/events database and reporting scripts.
4. Add strategy KPI report with daily and per-symbol decomposition.
5. Add preflight validation command (`--preflight`) before trading mode.

## What "Real Money Ready" Means Here

You should only turn full live mode on when all are true:

- deterministic replay + backtest framework exists
- hard guards and kill automation tested by fault injection
- metrics, logs, and alerts are stable
- ops runbooks exist for incident scenarios
- live exposure starts tiny and scales only with statistically proven behavior
