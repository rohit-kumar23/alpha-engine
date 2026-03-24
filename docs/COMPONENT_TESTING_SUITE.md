# Component testing suite

This file lists the **tests that exist**, **how to run** them, and **how to verify** results. Deeper “what to test per component” lives in [`COMPONENT_TESTING_GUIDELINES.md`](COMPONENT_TESTING_GUIDELINES.md).

## What is covered

| Binary | What it exercises |
|--------|-------------------|
| `test_marketdata_orderbook` | **Marketdata:** `BinanceParser` on fixtures; optional live WebSocket smoke (`--ws-smoke`). **Orderbook:** `L2Book` with parser-fed events (fixtures). |
| `test_orderbook_live_compare` | **Marketdata + orderbook:** REST depth seed, then live combined futures WS into `L2Book`, printed for manual UI comparison (optional `--live`, `--dump-dir`, etc.). |

**How to verify:** exit code `0` means all checks passed; non-zero means failure. Logs use `[PASS]` / `[FAIL]` / `[INFO]`; use `--verbose` where supported for extra detail.

---

## `test_marketdata_orderbook`

### Run (CMake / CTest)

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Filter by registered test name:

```bash
ctest --test-dir build -R marketdata_orderbook --output-on-failure
```

Run the executable:

```bash
./build/test_marketdata_orderbook
./build/test_marketdata_orderbook --verbose
./build/test_marketdata_orderbook --ws-smoke
```

### Run (Make)

```bash
make test_marketdata_orderbook
./test_marketdata_orderbook
./test_marketdata_orderbook --verbose
./test_marketdata_orderbook --ws-smoke
```

| Flag | Effect |
|------|--------|
| `--verbose` | More log detail (`[VERBOSE]`). |
| `--ws-smoke` | Enables live network WebSocket smoke (omit in CI if you want fully offline runs). |

---

## `test_orderbook_live_compare`

Not registered in CTest. Build and run explicitly when you want a **live** terminal book (defaults are built into the binary; no env vars).

### Build

```bash
cmake --build build --target test_orderbook_live_compare
```

```bash
make test_orderbook_live_compare
```

### Run

```bash
./build/test_orderbook_live_compare
./build/test_orderbook_live_compare --live
./build/test_orderbook_live_compare --help
```

| Flag | Effect |
|------|--------|
| `--live` | Live futures endpoints (default is demo). |
| `--clear` | Clear the terminal before each refresh. |
| `--dump-dir PATH` | Append each raw WS frame as one line to `PATH/websocket_captures.jsonl`. |
