# Component testing suite (layout, build, run)

This document is the **practical companion** to [`COMPONENT_TESTING_GUIDELINES.md`](COMPONENT_TESTING_GUIDELINES.md):

| Document | Purpose |
|----------|---------|
| **Component testing guidelines** | *What* to test per component (checklists, layers, principles). |
| **Component testing suite** (this file) | *Where* tests live in the repo, *how* to build and run them, and *how* to add new executables. |

## Layout

Keep tests close to the domain they exercise. Use **full words** in file and binary names (for example `marketdata`, not `md`).

```text
tests/
  common/                         # Shared helpers (logging harness, later: fixtures, matchers)
    test_log.hpp
  marketdata/                     # marketdata unit/integration tests
    test_binance_parser.cpp
    test_ws_smoke.cpp
  orderbook/                      # orderbook tests
    test_l2_book.cpp
  test_marketdata_orderbook.cpp   # Entry point: offline automated suites
  test_orderbook_live_compare.cpp # Optional: REST seed + WS -> printed book (manual UI check)
```

Guidelines:

- **One subdirectory per top-level domain** under `include/hft/` when the tests target that domain (`marketdata`, `orderbook`, `execution`, …).
- **Shared code** lives in `tests/common/`; avoid copying helpers into every suite.
- **Entry points** are named `test_<area>.cpp` (for example `test_marketdata_orderbook.cpp`) and only orchestrate: parse CLI flags, run suites, aggregate exit status.
- **Suite implementation** files are `test_<unit>.cpp` inside the matching subfolder. Expose functions such as `int run_binance_parser_tests(tests::TestLog&)` and declare them in the runner or a small internal header if needed.
- **Production sources** are linked into test binaries explicitly in CMake/Makefile (there is no separate static library yet). Only list `.cpp` files the suite actually needs to keep link time and coupling low.
- **Fixtures**: prefer embedded string literals for small JSON; for larger captures, add `tests/fixtures/<domain>/` (and load from disk in the test) when needed.

## Logging

Suites use [`tests/common/test_log.hpp`](../tests/common/test_log.hpp): `[PASS]`, `[FAIL]`, `[INFO]`, and optional `[VERBOSE]` lines. A non-zero process exit code means at least one check failed.

## How to run tests

### CMake and CTest (recommended for CI)

From the repository root:

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Run only the marketdata + orderbook suite by name:

```bash
ctest --test-dir build -R marketdata_orderbook --output-on-failure
```

The test executable is built as `build/test_marketdata_orderbook`. You can run it directly:

```bash
./build/test_marketdata_orderbook
./build/test_marketdata_orderbook --verbose
```

### Make

```bash
make test_marketdata_orderbook
```

The binary is written to the repo root: `./test_marketdata_orderbook`.

### Verifying success

- **Exit code `0`**: all checks in that run passed.
- **Exit code non-zero**: one or more `[FAIL]` lines; scroll up for the failing assertion name.
- **`--verbose`**: prints extra `[VERBOSE]` lines (for example the first WebSocket payload in smoke mode).
- **`--ws-smoke`**: runs the optional live WebSocket smoke check (see below). Omit it in CI so runs stay deterministic.

## Optional live WebSocket check

By default, `test_marketdata_orderbook` uses **offline fixtures** only (no network).

To additionally verify TLS + WebSocket connectivity and that at least one combined-stream frame parses:

```bash
./build/test_marketdata_orderbook --ws-smoke
```

Use the same flag with Make (`./test_marketdata_orderbook --ws-smoke`). No environment variables.

## Live terminal book vs Binance UI (`test_orderbook_live_compare`)

This target is a **test/diagnostic binary only**: it is **not** linked into `alpha_engine`, so it does not add branches, locks, or I/O to the production hot path.

- **Build:** `cmake --build build --target test_orderbook_live_compare` (or `make test_orderbook_live_compare`).
- **Run:** `./build/test_orderbook_live_compare` — defaults are **hardcoded** (demo endpoints, BTC/ETH/SOL, 10 levels per side, 500 ms refresh). Pass **`--live`** for live Binance futures endpoints (UI comparison). Optional: **`--clear`**, **`--dump-dir PATH`** (append one JSON line per WS frame to `PATH/websocket_captures.jsonl` for fixtures).
- **Flow:** REST depth snapshot seeds `L2Book`, then the same combined futures WebSocket and `BinanceParser` as production apply `BookTicker` + `DepthUpdate`; the process prints top-of-book and N levels on a timer for side-by-side comparison with the futures UI.

## Adding a new test executable

1. Add sources under `tests/<domain>/` and a runner `tests/test_<something>.cpp` if it is a new program.
2. **CMake** (`CMakeLists.txt`): in the **Tests** section, add a new `add_executable(...)`, `target_include_directories(... PRIVATE include ${CMAKE_SOURCE_DIR})`, link OpenSSL and `pthread`, mirror compiler warnings on the production target, then `add_test(NAME <descriptive_name> COMMAND <target>)`.
3. **Makefile**: in the **Tests** section, add a new block with `TEST_<NAME>_TARGET`, `_SRCS`, `_OBJS`, a rule linking the test binary, and a `.PHONY` target that builds and runs it.
4. **`.gitignore`**: add the new executable name next to `test_marketdata_orderbook` / `test_orderbook_live_compare` if it is produced in the repo root via Make.

Keep production and test targets in separate commented blocks in both build files so it stays obvious what ships to production versus what is for validation.

## What passing tests does (and does not) mean

A green run means **every check that is actually implemented** succeeded for **the inputs used** (offline JSON fixtures, synthetic snapshot + depth sequences, and optionally one live WebSocket frame if you enable smoke).

It does **not** mean:

- every possible Binance message shape or field ordering has been seen
- timing, threading, queue backpressure, or reconnect paths are validated
- the full production wiring in `alpha_engine` (threads, batch sizes, snapshot thread) is identical to the test harness
- prices will always match the UI at the same millisecond (different aggregation, latency, and depth limits)

Treat automated tests as **necessary but not sufficient** for production confidence. See also [`COMPONENT_TESTING_GUIDELINES.md`](COMPONENT_TESTING_GUIDELINES.md) on layers and limits.

## Beyond fixtures: comparing to the Binance UI

**Manual / visual parity** (formatted top-of-book or depth in a terminal vs the futures dashboard at the same symbol) is a separate, valuable check: it catches integration issues, stream subscription mistakes, and human-verifiable “does this look like the exchange?” correctness.

Suggested direction (pick what fits your workflow):

1. **Terminal book vs UI** — use `test_orderbook_live_compare` (standalone; does not touch `alpha_engine` hot paths) or, if you ever need it inside the engine, add a cold-path-only printer behind a flag.
2. **Captured replay** — save raw WS payloads to files, replay through the parser + book in tests, and optionally diff snapshots against a known-good golden output (deterministic CI).
3. **Deeper automation** — REST depth snapshot + diff against book state after N stream events (still subject to race; use tolerances and clear expectations).

Use **`test_marketdata_orderbook --ws-smoke`** as a quick “network + parse one frame” check; it is not a book parity tool.

## Suggested next steps

1. Use **`test_orderbook_live_compare`** for formatted multi-level book output vs the Binance UI; capture odd lines with **`--dump-dir`** for fixtures.
2. Add **fixture files** under `tests/fixtures/marketdata/` for real captured messages and regression tests for edge cases you discover during live runs.
3. Extend tests toward **production paths**: snapshot client + `L2Book::seed_from_snapshot` with REST-shaped JSON if not already covered end-to-end in unit tests.
4. Keep **CI** on offline tests only; run smoke and dashboard checks **locally** or in a controlled environment when you need exchange connectivity.
