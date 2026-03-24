## Alpha Engine (C++23 Crypto HFT)

`alpha_engine` is a production-oriented single-process HFT engine focused on Binance USD-M perpetuals (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`).

Design goals:

- keep hot-path logic in-memory and deterministic
- isolate domains (`marketdata`, `orderbook`, `execution`, `ordermgmt`, `riskmgmt`, `coreinfra`, `strategy`, `analytics`)
- minimize jitter with explicit threading, affinity, and bounded control-path work
- build for operational safety first, then scale alpha complexity

## Current Scope

- **Venue:** Binance USD-M (`demo` or `live` mode)
- **Runtime model:** One hot main loop + dedicated control/I/O threads
- **Primary strategy shape:** inventory-aware market making baseline
- **State handling:** OMS lifecycle + drop-copy integration + optional reconcile healing

## System Overview

Main runtime path:

`Binance WS` -> `Parser/Normalizer` -> `SPSC queue` -> `L2 book` -> `Strategy` -> `OrderManager` -> `PreTradeRisk` -> `Gateway`

Parallel control paths:

- user stream (`ORDER_TRADE_UPDATE`) -> exec report queue -> OMS updates
- snapshot thread for book reseed and out-of-sync repair
- reconcile thread for periodic remote `openOrders` verification
- async audit and MD-health log writers

For detailed internals, read `docs/ARCHITECTURE.txt`.

## Codebase Layout

```text
include/hft/
  analytics/        # PnL and analytics interfaces
  orderbook/        # L2 book model
  execution/        # Exchange execution adapters and stream decoding
  coreinfra/        # Lock-free queues, async log sink utilities
  marketdata/       # Exchange market-data clients and parser types
  ordermgmt/        # Order lifecycle/state manager and order manager
  riskmgmt/         # Pre-trade risk controls
  strategy/         # Strategy interfaces and models
  types.hpp         # Shared core domain types

src/
  analytics/        # PnL engine implementation
  application/      # process bootstrap, thread wiring, main loop
  orderbook/        # L2 book implementation
  execution/        # execution adapter implementations
  coreinfra/        # infrastructure implementations
  marketdata/       # market-data implementations
  ordermgmt/        # order state and order manager implementation
  riskmgmt/         # risk implementation
  strategy/         # strategy implementations
```

## Build and Run

### CMake

```bash
cmake -S . -B build
cmake --build build -j
./build/alpha_engine
```

### Make

```bash
make
./alpha_engine
```

## Configuration

Create `.env` from template:

```bash
cp .env.example .env
```

The binary auto-loads `.env` at startup without overriding already-exported shell variables.

Full variable explanations are intentionally centralized in:

- `docs/ENVIRONMENT_VARIABLES.md`

This avoids duplication and keeps config docs consistent.

## Realtime Scheduling (Optional)

If you want `HFT_RT_FIFO=1` and `HFT_MLOCKALL=1` without root, grant capabilities:

```bash
make
sudo setcap cap_sys_nice,cap_ipc_lock=eip ./alpha_engine
getcap ./alpha_engine
```

Expected:

```bash
./alpha_engine cap_ipc_lock,cap_sys_nice=eip
```

Remove capabilities:

```bash
sudo setcap -r ./alpha_engine
getcap ./alpha_engine
```

## Documentation Map

- `docs/ARCHITECTURE.txt`: data flow, thread model, execution path, failure handling
- `docs/ENVIRONMENT_VARIABLES.md`: meaning and use-case of every env variable
- `docs/NEXT_IMPLEMENTATION_STEPS.md`: practical roadmap to real-money readiness
- `docs/COMPONENT_TESTING_GUIDELINES.md`: what to test per component (checklists and principles)
- `docs/COMPONENT_TESTING_SUITE.md`: which tests exist and how to run them (CMake/Make/CTest)
- `docs/COMPONENT_OVERVIEW.md`: plain-English what each component does and how they interact

## Current Implementation Status

Implemented and usable:

- market-data websocket ingestion + parser + lock-free queue
- L2 book maintenance with sequence checks and snapshot-based reseed
- trigger-gated strategy interface with per-symbol runtime parameters
- execution command path (`New`/`Replace`/`Cancel`) + pre-trade risk checks
- signed Binance REST gateway (`demo`/`live`) with retry and throttle-aware controls
- user stream parsing and OMS lifecycle transitions
- optional reconcile and bounded healing paths
- asynchronous execution audit and MD health logging

Still evolving:

- production alpha sophistication (features, regime logic, model-driven signals)
- deeper persistence/analytics stack
- richer monitoring/alerting and operations tooling

## Security and Operational Safety

- never commit real exchange credentials
- use least-privilege API keys and IP restrictions
- rotate keys immediately if exposed
- start in `demo` and scale risk slowly after deterministic validation

If this repository has ever contained exposed keys, rotate them now.
