# Environment Variables Reference

This document explains every environment variable used by `alpha_engine`, including what it means, why it exists, and when to tune it.

## How Values Are Scaled

Many values are stored as integers to avoid floating point work in hot paths.

- `X1E6` means divide by 1,000,000 (`20000` -> `0.02`)
- `X10000` means divide by 10,000 (`10000` -> `1.0`)
- `X1000` means divide by 1,000 (`5000` -> `5.0`)
- `X100` means divide by 100 (`5000` -> `50.00`)
- `PPM` means parts per million (`1_000_000` = 100%)

## Runtime and CPU Scheduling

- `HFT_BUSY_SPIN`
  - **Meaning:** Main loop idle behavior (`1` spin, `0` yield).
  - **Use case:** Use `1` for lowest latency on dedicated cores; use `0` on shared machines to reduce CPU burn.

- `HFT_MLOCKALL`
  - **Meaning:** Lock process memory pages to reduce major page-fault jitter.
  - **Use case:** Enable on production hosts with `cap_ipc_lock`; disable in basic dev environments.

- `HFT_PREFAULT_MB`
  - **Meaning:** Amount of memory pre-touched at startup.
  - **Use case:** Increase if you see runtime minor/major faults under load.

- `HFT_CORE_WS`
  - **Meaning:** CPU core id for websocket market-data thread.
  - **Use case:** Isolate network ingest from strategy thread for better tail latency.

- `HFT_CORE_MAIN`
  - **Meaning:** CPU core id for main hot loop thread.
  - **Use case:** Pin to an isolated core for stable decision latency.

- `HFT_CORE_RECONCILE`
  - **Meaning:** CPU core id for reconcile control thread.
  - **Use case:** Keep control-plane REST checks off hot cores.

- `HFT_CORE_SNAPSHOT`
  - **Meaning:** CPU core id for snapshot/control thread.
  - **Use case:** Separate re-seeding and control tasks from execution logic.

- `HFT_RT_FIFO`
  - **Meaning:** Enable realtime scheduling (`SCHED_FIFO`) when `1`.
  - **Use case:** Use in controlled production hosts after validating system-level permissions and starvation risk.

- `HFT_RT_PRIO_MAIN`
  - **Meaning:** Realtime priority of main thread.
  - **Use case:** Keep highest among strategy/execution critical threads.

- `HFT_RT_PRIO_WS`
  - **Meaning:** Realtime priority of websocket thread.
  - **Use case:** Keep high enough to avoid MD backlog.

- `HFT_RT_PRIO_SNAPSHOT`
  - **Meaning:** Realtime priority of snapshot/control thread.
  - **Use case:** Keep lower than hot path so recovery does not preempt trading logic.

## Strategy Trigger and Readiness Controls

- `HFT_REQUIRE_ALL_SYMBOLS_SYNC`
  - **Meaning:** Require all symbols in-sync before global `TRADING_READY`.
  - **Use case:** Safer start mode to avoid partial market visibility.

- `HFT_ALLOW_PARTIAL_TRADING`
  - **Meaning:** Allow per-symbol trading once each symbol is individually ready.
  - **Use case:** Faster ramp-up in production after restarts; pair with strong per-symbol risk limits.

- `HFT_TRIGGER_MIN_INTERVAL_US`
  - **Meaning:** Minimum interval between strategy trigger attempts per symbol.
  - **Use case:** Prevent over-triggering during micro-burst data periods.

- `HFT_TRIGGER_MIN_MID_BPS_X1000`
  - **Meaning:** Minimum mid-price movement to allow another trigger.
  - **Use case:** Ignore micro-noise when market is stationary.

- `HFT_TRIGGER_MIN_IMB_PPM`
  - **Meaning:** Minimum order-book imbalance change to trigger.
  - **Use case:** Fire strategy only on meaningful queue-pressure changes.

## Strategy Global Parameters

- `HFT_STRAT_ALPHA_X1E6`
  - **Meaning:** Inventory penalty/skew coefficient.
  - **Use case:** Increase if inventory drifts too far; decrease if strategy under-trades.

- `HFT_STRAT_BASE_SPREAD_X10000`
  - **Meaning:** Baseline spread width around skew/fair value.
  - **Use case:** Widen in toxic/volatile regimes; tighten for more queue participation.

- `HFT_STRAT_INV_LIM_X1000`
  - **Meaning:** Inventory hard soft-limit used by strategy sizing/skew.
  - **Use case:** Lower while calibrating a new strategy; raise after proven stability.

- `HFT_STRAT_EDGE_BPS_X1000`
  - **Meaning:** Minimum edge required to emit intent.
  - **Use case:** Raise to reduce low-quality churn and fees.

- `HFT_STRAT_QTY_MIN_X1000`
  - **Meaning:** Minimum quote size.
  - **Use case:** Keep above exchange lot/min-notional practical floor.

- `HFT_STRAT_QTY_MAX_X1000`
  - **Meaning:** Maximum quote size.
  - **Use case:** Cap exposure per signal.

- `HFT_STRAT_QTY_INV_SHRINK_PPM`
  - **Meaning:** Linear size shrink factor as inventory approaches limit.
  - **Use case:** Primary inventory protection in active quoting.

## Strategy Per-Symbol Overrides

- `HFT_STRAT_ALPHA_BTC_X1E6`, `HFT_STRAT_ALPHA_ETH_X1E6`, `HFT_STRAT_ALPHA_SOL_X1E6`
  - **Meaning:** Symbol-specific alpha/skew.
  - **Use case:** Different inventory aggressiveness by liquidity profile.

- `HFT_STRAT_BASE_SPREAD_BTC_X10000`, `HFT_STRAT_BASE_SPREAD_ETH_X10000`, `HFT_STRAT_BASE_SPREAD_SOL_X10000`
  - **Meaning:** Symbol-specific spread baseline.
  - **Use case:** Wider on thinner books (often SOL), tighter on BTC.

- `HFT_STRAT_INV_LIM_BTC_X1000`, `HFT_STRAT_INV_LIM_ETH_X1000`, `HFT_STRAT_INV_LIM_SOL_X1000`
  - **Meaning:** Symbol-specific inventory limit.
  - **Use case:** Align with capital allocation and instrument risk.

- `HFT_STRAT_EDGE_BTC_BPS_X1000`, `HFT_STRAT_EDGE_ETH_BPS_X1000`, `HFT_STRAT_EDGE_SOL_BPS_X1000`
  - **Meaning:** Symbol-specific edge threshold.
  - **Use case:** Tune for each symbol's fee + toxicity profile.

- `HFT_STRAT_QTY_MIN_BTC_X1000`, `HFT_STRAT_QTY_MIN_ETH_X1000`, `HFT_STRAT_QTY_MIN_SOL_X1000`
  - **Meaning:** Symbol-specific minimum size.
  - **Use case:** Respect lot size and practical fill behavior.

- `HFT_STRAT_QTY_MAX_BTC_X1000`, `HFT_STRAT_QTY_MAX_ETH_X1000`, `HFT_STRAT_QTY_MAX_SOL_X1000`
  - **Meaning:** Symbol-specific maximum size.
  - **Use case:** Concentrate risk where microstructure quality is best.

- `HFT_STRAT_QTY_INV_SHRINK_BTC_PPM`, `HFT_STRAT_QTY_INV_SHRINK_ETH_PPM`, `HFT_STRAT_QTY_INV_SHRINK_SOL_PPM`
  - **Meaning:** Symbol-specific inventory shrink factors.
  - **Use case:** More aggressive shrink on symbols with weaker hedge liquidity.

## Execution Policy and Adaptive Cancel Controls

- `HFT_EXEC_REPLACE_BPS_X1000`
  - **Meaning:** Minimum quote movement required to replace existing order.
  - **Use case:** Avoid replace spam and queue-position churn.

- `HFT_EXEC_CANCEL_STALE_MS`
  - **Meaning:** Global stale-order timeout before cancel.
  - **Use case:** Set non-zero when stale resting orders are frequently toxic.

- `HFT_EXEC_CANCEL_STALE_BTC_MS`, `HFT_EXEC_CANCEL_STALE_ETH_MS`, `HFT_EXEC_CANCEL_STALE_SOL_MS`
  - **Meaning:** Per-symbol stale cancel timeout override.
  - **Use case:** Faster stale cancellation on symbols with higher adverse selection.

- `HFT_EXEC_ADVERSE_CANCEL_BTC_BPS_X1000`, `HFT_EXEC_ADVERSE_CANCEL_ETH_BPS_X1000`, `HFT_EXEC_ADVERSE_CANCEL_SOL_BPS_X1000`
  - **Meaning:** Adverse move threshold to cancel resting quote.
  - **Use case:** Reduce being picked off when fair value moves away.

- `HFT_EXEC_ADAPTIVE_CANCEL`
  - **Meaning:** Enable adaptive tuning of stale/adverse thresholds.
  - **Use case:** Useful in changing market regimes without constant manual retuning.

- `HFT_EXEC_ADAPTIVE_TARGET_ADV_PER_SEC`
  - **Meaning:** Target per-second adverse-cancel rate.
  - **Use case:** Defines desired aggressiveness against adverse selection.

- `HFT_EXEC_ADAPTIVE_TARGET_STALE_PER_SEC`
  - **Meaning:** Target per-second stale-cancel rate.
  - **Use case:** Controls how quickly stale quotes are cycled.

- `HFT_EXEC_ADAPTIVE_ADV_STEP_BPS_X1000`
  - **Meaning:** Adjustment step for adverse threshold.
  - **Use case:** Bigger step responds faster; too big can oscillate.

- `HFT_EXEC_ADAPTIVE_STALE_STEP_MS`
  - **Meaning:** Adjustment step for stale timeout.
  - **Use case:** Balance adaptation speed and stability.

- `HFT_EXEC_ADAPTIVE_ADV_MIN_BPS_X1000`, `HFT_EXEC_ADAPTIVE_ADV_MAX_BPS_X1000`
  - **Meaning:** Min/max clamps for adaptive adverse threshold.
  - **Use case:** Keep controller inside safe operating bounds.

- `HFT_EXEC_ADAPTIVE_STALE_MIN_MS`, `HFT_EXEC_ADAPTIVE_STALE_MAX_MS`
  - **Meaning:** Min/max clamps for adaptive stale timeout.
  - **Use case:** Prevent pathological extremes under noisy telemetry.

## Control Plane, Logging, and Recovery

- `HFT_KILL_SWITCH`
  - **Meaning:** Startup kill-switch state (`1` rejects all orders).
  - **Use case:** Emergency or maintenance mode on launch.

- `HFT_SIGUSR1_TOGGLE_KILL`
  - **Meaning:** Enable runtime kill-switch toggle via `SIGUSR1`.
  - **Use case:** Fast operator control without restarting process.

- `HFT_EXEC_AUDIT_LOG`
  - **Meaning:** Path/flag for execution audit log output.
  - **Use case:** Keep immutable action trail for diagnostics and compliance.

- `HFT_MD_HEALTH_LOG`
  - **Meaning:** Path/flag for market-data health event log.
  - **Use case:** Analyze reconnect episodes and feed liveness incidents.

- `HFT_RECONCILE_INTERVAL_SEC`
  - **Meaning:** Periodic remote `openOrders` reconcile interval.
  - **Use case:** Catch missed events or local OMS drift.

- `HFT_RECONCILE_HEAL`
  - **Meaning:** Enable automatic local healing from reconcile snapshot.
  - **Use case:** Keep engine consistent after temporary user-stream issues.

- `HFT_RECONCILE_HEAL_MAX_PER_TICK`
  - **Meaning:** Maximum healed orders per reconcile cycle.
  - **Use case:** Bound control-path load and avoid sudden large state jumps.

- `HFT_OMS_PENDING_TIMEOUT_MS`
  - **Meaning:** Timeout for pending OMS states before correction.
  - **Use case:** Detect stuck pending transitions.

- `HFT_OMS_PENDING_HEAL_MAX_PER_TICK`
  - **Meaning:** Max timed-out pending states corrected per tick.
  - **Use case:** Bounded reconciliation pressure.

- `HFT_SNAPSHOT_RETRY_ATTEMPTS`
  - **Meaning:** Retry attempts for depth snapshot pull.
  - **Use case:** Improve resilience under temporary REST failures.

- `HFT_SNAPSHOT_RETRY_BACKOFF_MS`
  - **Meaning:** Backoff between immediate snapshot retries.
  - **Use case:** Prevent burst retry loops.

- `HFT_SNAPSHOT_RETRY_MAX_BACKOFF_MS`
  - **Meaning:** Maximum per-symbol snapshot retry cooldown.
  - **Use case:** Cap exponential retry growth during prolonged outages.

- `HFT_GATEWAY_RETRY_ATTEMPTS`
  - **Meaning:** Retry attempts for order REST call.
  - **Use case:** Short-lived transport failures without immediate order loss.

- `HFT_GATEWAY_RETRY_BACKOFF_MS`
  - **Meaning:** Backoff between gateway retries.
  - **Use case:** Avoid immediate hammering after transient failures.

- `HFT_BINANCE_RECV_WINDOW_MS`
  - **Meaning:** Binance `recvWindow` for signed requests.
  - **Use case:** Increase slightly if host clock jitter/network delay causes timestamp errors.

- `HFT_TRANSPORT_RETRY_ATTEMPTS`
  - **Meaning:** Low-level transport retry budget.
  - **Use case:** Recover from short connection issues before fail-fast.

- `HFT_TRANSPORT_RETRY_BACKOFF_MS`
  - **Meaning:** Delay between transport retries.
  - **Use case:** Smooth out request bursts during link instability.

- `HFT_TRANSPORT_COOLDOWN_MS`
  - **Meaning:** Cooldown after transport failure sequence.
  - **Use case:** Circuit-break behavior for networking glitches.

- `HFT_Q2S_STALE_DROP_MS`
  - **Meaning:** Queue-to-strategy staleness cutoff.
  - **Use case:** Drop old MD events in bursts to protect signal freshness.

- `HFT_LIFECYCLE_TIMEOUT_MS`
  - **Meaning:** Timeout for lifecycle correction logic.
  - **Use case:** Declare stale order lifecycle as timed out for deterministic cleanup.

- `HFT_LIFECYCLE_TIMEOUT_AUDIT_MAX_PER_TICK`
  - **Meaning:** Max lifecycle timeout audit lines emitted per cycle.
  - **Use case:** Limit logging overhead during incident bursts.

- `HFT_TRANSPORT_CIRCUIT_FAIL_THRESHOLD`
  - **Meaning:** Consecutive transport failures before circuit-open behavior.
  - **Use case:** Stop repeatedly sending during upstream unavailability.

- `HFT_TRANSPORT_CIRCUIT_COOLDOWN_MS`
  - **Meaning:** Circuit-open cooldown duration.
  - **Use case:** Give transport/exchange time to recover before retrying.

- `HFT_WS_IDLE_RECONNECT_MS`
  - **Meaning:** No-message threshold before forced WS reconnect.
  - **Use case:** Recover from silent/dead websocket connections.

- `HFT_WS_IDLE_RECONNECT_COOLDOWN_MS`
  - **Meaning:** Minimum interval between forced reconnects.
  - **Use case:** Prevent reconnect storms.

- `HFT_REST_WEIGHT_SOFT_LIMIT`
  - **Meaning:** Soft cap for Binance request weight before local throttling.
  - **Use case:** Stay below hard rate limits.

- `HFT_REST_THROTTLE_COOLDOWN_MS`
  - **Meaning:** Local cooldown after throttle responses (`-1003`, `-1015`).
  - **Use case:** Reduce repeated throttle penalties.

- `HFT_MAIN_MD_BATCH_MAX`
  - **Meaning:** Max market-data events processed per main loop batch.
  - **Use case:** Bound loop-time variance under extreme feed bursts.

- `HFT_EXEC_REPORT_BATCH_MAX`
  - **Meaning:** Max execution reports handled per batch.
  - **Use case:** Prevent control path starvation.

## Send Pacing and Exchange Interaction

- `HFT_EXEC_MIN_SEND_INTERVAL_US`
  - **Meaning:** Global minimum send spacing per symbol/slot.
  - **Use case:** Baseline anti-spam pacing.

- `HFT_EXEC_MIN_SEND_INTERVAL_BTC_US`, `HFT_EXEC_MIN_SEND_INTERVAL_ETH_US`, `HFT_EXEC_MIN_SEND_INTERVAL_SOL_US`
  - **Meaning:** Per-symbol send pacing overrides.
  - **Use case:** Tighter pacing on noisier symbols.

- `HFT_EXCH_MIN_NOTIONAL_USDT`
  - **Meaning:** Fallback minimum notional when exchange filters unavailable.
  - **Use case:** Safety net to avoid immediate filter rejects.

- `HFT_CANARY_FILL_MODE`
  - **Meaning:** Enable controlled spread-crossing mode for fill path tests.
  - **Use case:** Validate end-to-end order/fill accounting without full production aggressiveness.

- `HFT_CANARY_FILL_CROSS_BPS`
  - **Meaning:** Crossing offset used in canary mode.
  - **Use case:** Tune probability/speed of test fills.

- `HFT_CANARY_ROTATE_SYMBOLS`
  - **Meaning:** Rotate canary fill behavior across symbols.
  - **Use case:** Validate symbol-specific execution paths fairly.

- `HFT_CANARY_ROTATION_WINDOW_MS`
  - **Meaning:** Time window per symbol in canary rotation.
  - **Use case:** Deterministic observation windows during test runs.

## Risk and PnL Guardrails

- `HFT_RISK_MAX_ORDER_QTY_X1000`
  - **Meaning:** Max allowed order quantity.
  - **Use case:** Hard cap against fat-finger/bugged intents.

- `HFT_RISK_MAX_NOTIONAL`
  - **Meaning:** Max order notional in quote currency.
  - **Use case:** Notional risk ceiling independent of qty unit.

- `HFT_RISK_MAX_ABS_POS_X1000`
  - **Meaning:** Max absolute projected position.
  - **Use case:** Prevent accumulating inventory beyond strategy tolerance.

- `HFT_PNL_FEE_BPS_X1000`
  - **Meaning:** Fee assumption for mark-to-market telemetry.
  - **Use case:** More realistic PnL and drawdown calculations.

- `HFT_PNL_DRAWDOWN_GUARD`
  - **Meaning:** Enable drawdown-based symbol pause.
  - **Use case:** Stop trading strategy temporarily after local equity shock.

- `HFT_PNL_MAX_DRAWDOWN_USDT_X100`
  - **Meaning:** Default drawdown threshold per symbol.
  - **Use case:** Account-level baseline loss containment.

- `HFT_PNL_MAX_DRAWDOWN_BTC_USDT_X100`, `HFT_PNL_MAX_DRAWDOWN_ETH_USDT_X100`, `HFT_PNL_MAX_DRAWDOWN_SOL_USDT_X100`
  - **Meaning:** Per-symbol drawdown thresholds.
  - **Use case:** Higher threshold for deeper symbols, lower for thinner ones.

- `HFT_PNL_COOLDOWN_SEC`
  - **Meaning:** Default pause duration after drawdown trip.
  - **Use case:** Allow microstructure regime to settle before re-entry.

- `HFT_PNL_COOLDOWN_BTC_SEC`, `HFT_PNL_COOLDOWN_ETH_SEC`, `HFT_PNL_COOLDOWN_SOL_SEC`
  - **Meaning:** Per-symbol cooldown overrides.
  - **Use case:** Longer cooldown where adverse conditions persist.

## Exchange Credentials and Environment

- `BINANCE_MODE`
  - **Meaning:** `demo` or `live` endpoint set.
  - **Use case:** Keep `demo` for most development and validation; switch to `live` only after strict go-live checklist pass.

- `BINANCE_API_KEY`
  - **Meaning:** Binance API key.
  - **Use case:** Required for signed order endpoints and user stream.

- `BINANCE_API_SECRET`
  - **Meaning:** Binance API secret for HMAC signing.
  - **Use case:** Required for authenticated trading/reconcile requests.

## Test binaries

`test_marketdata` and `test_orderbook` do **not** read environment variables for their own configuration; defaults and command-line flags are documented in [`COMPONENT_TESTING_SUITE.md`](COMPONENT_TESTING_SUITE.md).

## Recommended Profiles

- **Local development:** Disable RT scheduling and memory lock, keep `BINANCE_MODE=demo`, conservative risk caps.
- **Staging soak test:** Enable reconciliation, audit logs, drawdown guard, and canary mode for short windows.
- **Production pre-launch:** Dedicated cores, validated RT permissions, strict risk limits, alerting wired, and full secrets hygiene.
