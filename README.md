## Alpha Engine (Pure C++23 Crypto HFT)

This is a production-focused C++23 HFT build, not a toy bot.

Core principles:

- hot path in memory only
- deterministic behavior and replayability
- zero allocation target in critical callbacks
- strict separation of market data, strategy, execution, risk, and state

### Exchange Path (What to Build First)

Start with single exchange first, then expand to multi-exchange.

Phase A (single exchange):

- venue: Binance USD-M perpetuals
- symbols: BTCUSDT, ETHUSDT, SOLUSDT
- reason: best liquidity, strong API ecosystem, fastest path to robust production operations
- objective: stable market making engine with inventory control and strict risk controls

Phase B (multi exchange):

- add Bybit linear perpetuals second
- add Deribit third if you want options/volatility strategies
- objective: cross-venue pricing, hedge routing, and eventually cross-exchange alpha

Do not start with multi-exchange from day one. It slows execution quality and operational maturity.

### What We Are Building

Runtime path:

- `BinanceWebSocketGateway` -> `FeedParser/Normalizer` -> `SPSC Queue` -> `BookBuilder(L2/L3)` -> `StrategyEngine` -> `ExecutionEngine` -> `ExchangeGateway`
- parallel async path: `DropCopy` -> `OMSState` -> `Persistence` -> `Monitoring`

Runtime component status:

- `BinanceWebSocketGateway`: completed
- `FeedParser/Normalizer`: completed
- `SPSC Queue`: completed
- `BookBuilder(L2/L3)`: completed
- `StrategyEngine`: in progress (baseline signal, trigger gating, per-symbol params, inventory-fed state; production alpha logic still evolving)
- `ExecutionEngine`: in progress (new/replace/cancel with stale/adverse/opp-side cancel policies and rate-limit governor)
- `ExchangeGateway`: in progress (signed HTTPS transport, demo/live mode, retry path, response-code telemetry, `X-MBX-USED-WEIGHT-1M` tracking)
- `DropCopy`: in progress (`ORDER_TRADE_UPDATE` path integrated with terminal state handling)
- `OMSState`: in progress (pending/live/cancel/reject/completed lifecycle with invalid-transition telemetry)
- `RiskEngine`: in progress (pre-trade checks, fill-fed position tracking, startup/runtime kill switch)
- `Persistence`: in progress (append-only execution audit sink via lock-free queue + writer thread)
- `Monitoring`: in progress (stdout telemetry includes per-stage latency and per-symbol cancel reason counters)

System modules:

- `md`: exchange adapters, protocol decode, normalization
- `book`: in-memory order book and queue position
- `strategy`: market making and inventory-aware quoting
- `execution`: order state machine, throttles, retries, cancel/replace
- `risk`: hard limits, soft limits, kill switch
- `oms`: positions, open orders, pnl, reconciliation
- `infra`: lock-free queues, clocks, CPU pinning, telemetry

### Build

```bash
cmake -S . -B build
cmake --build build -j
./build/alpha_engine
```

```bash
make
./alpha_engine
```

Architecture, end-to-end data flow, execution, strategy triggers, and remaining work are summarized in [`docs/ARCHITECTURE.txt`](docs/ARCHITECTURE.txt).

### Runtime Config

Create `.env` from `.env.example` and tune values for your machine:

```bash
cp .env.example .env
```

Supported knobs:

- `HFT_BUSY_SPIN` (`0|1`): idle-loop mode; `1` uses pause-spin, `0` yields scheduler.
- `HFT_CORE_WS`: CPU core id for market-data websocket thread.
- `HFT_CORE_SNAPSHOT`: CPU core id for snapshot/control thread.
- `HFT_CORE_MAIN`: CPU core id for hot consumer/strategy thread.
- `HFT_RT_FIFO` (`0|1`): enable realtime scheduler (`SCHED_FIFO`) for pinned threads.
- `HFT_RT_PRIO_WS`: realtime priority for websocket thread.
- `HFT_RT_PRIO_SNAPSHOT`: realtime priority for snapshot/control thread.
- `HFT_RT_PRIO_MAIN`: realtime priority for hot main thread.
- `HFT_MLOCKALL` (`0|1`): lock process memory to reduce page-fault jitter.
- `HFT_REQUIRE_ALL_SYMBOLS_SYNC` (`0|1`): when `1`, global engine state is `TRADING_READY` only after all symbols are in-sync.
- `HFT_ALLOW_PARTIAL_TRADING` (`0|1`): when `1`, each symbol can trade independently as soon as that symbol is ready and in-sync.
- `HFT_PREFAULT_MB`: MB of memory to pre-touch at startup.
- `HFT_TRIGGER_MIN_INTERVAL_US`: minimum microseconds between strategy trigger attempts per symbol.
- `HFT_TRIGGER_MIN_MID_BPS_X1000`: minimum mid-price move threshold for trigger.
- `HFT_TRIGGER_MIN_IMB_PPM`: minimum imbalance move threshold for trigger.
- `HFT_STRAT_ALPHA_X1E6`: strategy alpha scaled by 1e6 (`20000` -> `0.02`).
- `HFT_STRAT_BASE_SPREAD_X10000`: base spread scaled by 1e4 (`10000` -> `1.0`).
- `HFT_STRAT_INV_LIM_X1000`: inventory limit scaled by 1e3 (`5000` -> `5.0`).
- `HFT_STRAT_ALPHA_{BTC|ETH|SOL}_X1E6`: per-symbol alpha overrides (fallback to `HFT_STRAT_ALPHA_X1E6`).
- `HFT_STRAT_BASE_SPREAD_{BTC|ETH|SOL}_X10000`: per-symbol spread overrides (fallback to `HFT_STRAT_BASE_SPREAD_X10000`).
- `HFT_STRAT_INV_LIM_{BTC|ETH|SOL}_X1000`: per-symbol inventory-limit overrides (fallback to `HFT_STRAT_INV_LIM_X1000`).
- `HFT_STRAT_EDGE_BPS_X1000`: minimum micro-vs-mid edge threshold (in bps x1000) required to emit an order intent.
- `HFT_STRAT_QTY_MIN_X1000` / `HFT_STRAT_QTY_MAX_X1000`: strategy order-size band (scaled by 1e3).
- `HFT_STRAT_QTY_INV_SHRINK_PPM`: linear inventory-risk size shrink factor (`0..1e6`, where `1e6` = full shrink to min at inventory limit).
- `HFT_STRAT_EDGE_{BTC|ETH|SOL}_BPS_X1000`: per-symbol edge threshold overrides (fallback to `HFT_STRAT_EDGE_BPS_X1000`).
- `HFT_STRAT_QTY_MIN_{BTC|ETH|SOL}_X1000` / `HFT_STRAT_QTY_MAX_{BTC|ETH|SOL}_X1000`: per-symbol qty band overrides.
- `HFT_STRAT_QTY_INV_SHRINK_{BTC|ETH|SOL}_PPM`: per-symbol inventory shrink overrides.
- `HFT_EXEC_REPLACE_BPS_X1000`: minimum price delta before emitting replace command.
- `HFT_EXEC_CANCEL_STALE_MS`: stale-order cancel timeout in milliseconds. If an order sits unchanged longer than this (and non-zero), emit `Cancel` instead of another replace/new on that slot.
- `HFT_EXEC_CANCEL_STALE_{BTC|ETH|SOL}_MS`: per-symbol stale cancel timeouts (fallback to `HFT_EXEC_CANCEL_STALE_MS`).
- `HFT_EXEC_ADVERSE_CANCEL_{BTC|ETH|SOL}_BPS_X1000`: per-symbol adverse-move cancel threshold for resting quotes. If market-driven target quote moves against the resting quote by at least this bps*1000 amount, emit cancel first.
- Stats expose cancel reasons per symbol: `can_opp_*`, `can_stale_*`, `can_adv_*` for BTC/ETH/SOL.
- `HFT_EXEC_ADAPTIVE_CANCEL` (`0|1`): enable 1s adaptive tuning of stale/adverse cancel thresholds from observed cancel-reason rates.
- `HFT_EXEC_ADAPTIVE_TARGET_ADV_PER_SEC`: target per-second adverse-cancel count per symbol.
- `HFT_EXEC_ADAPTIVE_TARGET_STALE_PER_SEC`: target per-second stale-cancel count per symbol.
- `HFT_EXEC_ADAPTIVE_ADV_STEP_BPS_X1000`: step size applied when adjusting adverse threshold.
- `HFT_EXEC_ADAPTIVE_STALE_STEP_MS`: step size applied when adjusting stale timeout.
- `HFT_EXEC_ADAPTIVE_ADV_MIN_BPS_X1000` / `HFT_EXEC_ADAPTIVE_ADV_MAX_BPS_X1000`: bounds for adaptive adverse threshold.
- `HFT_EXEC_ADAPTIVE_STALE_MIN_MS` / `HFT_EXEC_ADAPTIVE_STALE_MAX_MS`: bounds for adaptive stale timeout.
- `BINANCE_MODE` (`demo|live`, aliases `testnet|sandbox` / `mainnet|production`): selects Binance USD-M Futures endpoints. Demo uses `https://demo-fapi.binance.com` REST and `wss://fstream.binancefuture.com` streams per [Binance USD-M general info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info). Read once at startup (not on the hot path). If unset, defaults to `demo`. Invalid values exit at startup.
- `HFT_GATEWAY_RETRY_ATTEMPTS`: max attempts for gateway REST send.
- `HFT_GATEWAY_RETRY_BACKOFF_MS`: backoff between retry attempts.
- `HFT_REST_WEIGHT_SOFT_LIMIT`: if current `X-MBX-USED-WEIGHT-1M` reaches/exceeds this value, main loop temporarily gates outbound sends (before gateway call).
- `HFT_REST_THROTTLE_COOLDOWN_MS`: cooldown applied after throttle errors (`-1003`, `-1015`) before allowing new sends again.
- `HFT_EXEC_MIN_SEND_INTERVAL_US`: global min interval between successful sends per symbol slot (microseconds).
- `HFT_EXEC_MIN_SEND_INTERVAL_{BTC|ETH|SOL}_US`: per-symbol min send interval overrides (fallback to global).
- `HFT_EXEC_REPORT_BATCH_MAX`: max exec-report messages processed per loop batch (before and after MD batch) to reduce control-path starvation under MD bursts.
- `HFT_EXCH_MIN_NOTIONAL_USDT`: fallback exchange min notional filter used before send if per-symbol exchange rules are unavailable.
- `HFT_CANARY_FILL_MODE` (`0|1`): if `1`, new orders can be shifted to cross the spread for controlled fill-path testing.
- `HFT_CANARY_FILL_CROSS_BPS`: cross amount in bps used by canary fill mode.
- `HFT_CANARY_ROTATE_SYMBOLS` (`0|1`): when canary mode is enabled, rotate canary crossing deterministically across `BTC -> ETH -> SOL`.
- `HFT_CANARY_ROTATION_WINDOW_MS`: per-symbol window length for deterministic canary rotation.
- `HFT_RISK_MAX_ORDER_QTY_X1000`: max per-order quantity, scaled by 1000.
- `HFT_RISK_MAX_NOTIONAL`: max per-order notional in quote currency.
- `HFT_RISK_MAX_ABS_POS_X1000`: max absolute projected position, scaled by 1000.
- `HFT_PNL_FEE_BPS_X1000`: fee used by runtime PnL telemetry (scaled by 1000 bps; `200` -> `0.2` bps).
- `HFT_PNL_DRAWDOWN_GUARD` (`0|1`): per-symbol MTM drawdown guard; pauses quoting for a cooldown window when drawdown from local peak exceeds threshold.
- `HFT_PNL_MAX_DRAWDOWN_USDT_X100`: default symbol drawdown threshold in USDT (scaled by 100).
- `HFT_PNL_MAX_DRAWDOWN_{BTC|ETH|SOL}_USDT_X100`: per-symbol drawdown overrides.
- `HFT_PNL_COOLDOWN_SEC`: default pause duration after drawdown trip.
- `HFT_PNL_COOLDOWN_{BTC|ETH|SOL}_SEC`: per-symbol cooldown overrides.
- `BINANCE_API_KEY`: Binance API key (demo keys from demo trading; live keys for production).
- `BINANCE_API_SECRET`: HMAC secret used to sign REST requests.
- `HFT_KILL_SWITCH` (`0|1`): `1` arms kill switch at startup; pre-trade risk rejects all orders with `KillSwitchEngaged` (single relaxed atomic load on the hot path).
- `HFT_EXEC_AUDIT_LOG`: execution events (risk reject, gateway fail with HTTP + Binance `code`, successful send, fill/reject drop-copy, reconcile/pnl guards) go through a fixed-size SPSC ring; a dedicated thread line-writes so the hot path only does a non-blocking push. Set to a concrete path like `./exec_audit.log` to keep logs in repo/current directory. `1` uses `/tmp/alpha_exec.log`. Use `0` / `off` / `false` to disable.
- `HFT_MD_HEALTH_LOG`: MD liveness and websocket recovery events (startup, idle reconnect trigger, reconnect complete) to a separate fixed-size SPSC-backed log sink. `1` uses `/tmp/alpha_health.log`. Use `0` / `off` / `false` to disable.
- `HFT_WS_IDLE_RECONNECT_MS`: if no websocket message arrives for this many milliseconds, force websocket reconnect (liveness watchdog).
- `HFT_WS_IDLE_RECONNECT_COOLDOWN_MS`: minimum cooldown between forced idle reconnect attempts.
- `HFT_SIGUSR1_TOGGLE_KILL` (`0|1`): if `1` or unset, `SIGUSR1` toggles the kill-switch atomic at runtime (`kill -USR1 <pid>`). No extra work on the hot path; handler only flips the same flag read by pre-trade risk.
- `HFT_RECONCILE_INTERVAL_SEC`: if greater than zero and `BINANCE_API_SECRET` is set, a **control thread** (not the strategy loop) periodically calls signed `GET /fapi/v1/openOrders`, counts tracked `hft_<id>` client order ids, and publishes the result via atomics. The main thread compares remote count against OMS open-order estimate (`Live + PendingReplace + PendingCancel`) when the sequence counter advances and increments `rec_mismatch` if they differ. Default `0` disables (zero hot-path REST).
- `HFT_RECONCILE_HEAL` (`0|1`): if `1`, main thread performs bounded healing on mismatch by dropping local OrderManager live slots and marking OMS live orders `Completed` when their `hft_<id>` is absent in remote `openOrders`.
- `HFT_RECONCILE_HEAL_MAX_PER_TICK`: max local orders healed per reconcile update (bounds control-path work and state jumps).
- `HFT_OMS_PENDING_TIMEOUT_MS`: timeout for `PendingNew` / `PendingReplace` / `PendingCancel` OMS states before reconcile-driven correction.
- `HFT_OMS_PENDING_HEAL_MAX_PER_TICK`: max timed-out pending orders corrected per reconcile update (bounded control-path repair).
- `HFT_SNAPSHOT_RETRY_ATTEMPTS`: per-request snapshot retry count in control thread before reporting failure.
- `HFT_SNAPSHOT_RETRY_BACKOFF_MS`: backoff between snapshot retries.
- `HFT_SNAPSHOT_RETRY_MAX_BACKOFF_MS`: cap for per-symbol resnapshot scheduling backoff after repeated failures.
- `HFT_CORE_RECONCILE`: CPU core for the reconcile thread (defaults to `HFT_CORE_SNAPSHOT` if unset).

The binary auto-loads `.env` at startup (without overriding already-exported shell env vars).

### Enable/Disable Realtime Scheduling

To allow `HFT_RT_FIFO=1` and `HFT_MLOCKALL=1` without running as root, grant capabilities to the binary:

```bash
make
sudo setcap cap_sys_nice,cap_ipc_lock=eip ./alpha_engine
getcap ./alpha_engine
```

You should see:

```bash
./alpha_engine cap_ipc_lock,cap_sys_nice=eip
```

To disable/remove this permission again:

```bash
sudo setcap -r ./alpha_engine
getcap ./alpha_engine
```

If `getcap` prints nothing, capability is removed.

### Implementation Roadmap

1. Binance market data connector (WebSocket only) with heartbeat and reconnect.
2. Deterministic normalizer and in-memory L2 book per symbol.
3. Binance order gateway (new/cancel/replace) with signed auth and rate-limit controls.
4. Order state machine with ack/reject/partial/fill transitions.
5. Inline risk checks: max position, max notional, max order size, kill switch.
6. First production strategy: passive market making with inventory skew and quoting bands.
7. Drop-copy style reconciliation path and persistent execution logs.
8. Latency instrumentation per stage and alerting on spikes.
9. Multi-symbol scaling on one venue.
10. Add second venue (Bybit), then hedge and cross-venue routing.

### Current Code Status

- C++23 build system with CMake and Makefile
- live Binance USD-M WebSocket client integrated
- combined streams enabled for BTCUSDT, ETHUSDT, SOLUSDT bookTicker, depth@100ms, and aggTrade
- lightweight parser + lock-free SPSC queue ingestion path
- depth sequence integrity checks (`U/u/pu`) with stale-book gating before strategy callbacks
- REST depth snapshot reseed path for out-of-sync recovery with controlled strategy re-enable
- snapshot-seeded depth bridge logic (`U <= lastUpdateId+1 <= u`) with buffered pre-seed depth replay
- snapshot recovery moved to dedicated control thread with lock-free request/result queues to keep hot loop non-blocking
- thread pinning for hot/control roles and runtime latency telemetry (`parse`, `enqueue`, `queue-to-strategy`)
- busy-spin hot loop option via `HFT_BUSY_SPIN=1` and queue-to-strategy rolling p50/p99 metrics
- env-configurable core pinning: `HFT_CORE_WS`, `HFT_CORE_SNAPSHOT`, `HFT_CORE_MAIN`, `HFT_CORE_RECONCILE`
- optional realtime scheduler mode: `HFT_RT_FIFO=1` with per-thread priorities
- trigger gating controls to reduce hot-path overload and tail jitter
- pre-trade risk gate + OMS local transitions + execution gateway (demo or live REST)
- gateway live transport path with signed Binance REST request send
- user data stream listener scaffold for real ACK/FILL/REJECT event path
- source structure split by domain: `src/app`, `src/md`, and dedicated low-level infra headers
- simulator files removed from active codebase
- optional execution audit trail (`HFT_EXEC_AUDIT_LOG`) for post-trade analysis without blocking the hot loop
- gateway returns structured HTTP status + Binance JSON `code` on failures for ops telemetry (`gw_last_http`, `gw_last_ix` in stats)
- gateway reuses a process-wide TLS context (`SSL_CTX`) to reduce per-request REST setup overhead
- periodic signed `openOrders` reconciliation scaffold (`HFT_RECONCILE_INTERVAL_SEC`); stats: `rec_exch_open`, `rec_mismatch`, `rec_http_fail`
- websocket liveness watchdog with idle reconnect (`HFT_WS_IDLE_RECONNECT_MS`, `HFT_WS_IDLE_RECONNECT_COOLDOWN_MS`) and persisted `md_health.log` events
- user-stream `ORDER_TRADE_UPDATE` parses Binance USD-M nested `"o"` object; `TRADE`/`NEW`/`CANCELED`/`EXPIRED`/`REJECTED`; `X` order status drives `terminal` (e.g. `FILLED`) for OMS `Completed`
- OMS: `PendingCancel` / `Completed`, drop-copy-driven lifecycle; REST modify uses `origClientOrderId`, cancel uses signed DELETE body
- last REST response `X-MBX-USED-WEIGHT-1M` mirrored to `rest_mbx_wt_1m` in stats (atomic update on I/O thread only)
- strict lifecycle disposition audit path for timeout/reconcile corrections (`event=lifecycle_disposition` with deterministic `action=*`)
- stats now expose readiness state and per-symbol sync/ready bits: `state`, `sync_*`, `ready_*`, `synced_symbols`
- next milestone: richer production market-making controls (symbol-specific edge/size/skew), plus persistent fills/orders storage and monitoring alerts
