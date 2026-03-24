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

Registered in CTest as **`marketdata_orderbook`**.

### CMake / CTest

Configure, build, and run the full test suite:

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Only this test:

```bash
ctest --test-dir build -R marketdata_orderbook --output-on-failure
```

Run the built binary (from `build/`):

```bash
./build/test_marketdata_orderbook
./build/test_marketdata_orderbook --verbose
./build/test_marketdata_orderbook --ws-smoke
```

### Make

Build the executable only:

```bash
make test_marketdata_orderbook
```

Build (if needed) and run it:

```bash
make run_test_marketdata_orderbook
```

After a Make build, the binary is at the repo root:

```bash
./test_marketdata_orderbook --verbose
./test_marketdata_orderbook --ws-smoke
```

### Flags

| Flag | Effect |
|------|--------|
| `--verbose` | More log detail (`[VERBOSE]`). |
| `--ws-smoke` | Live WebSocket smoke (network); omit for fully offline runs (e.g. CI). |

---

## `test_orderbook_live_compare`

**Not** registered in CTest (build and run explicitly). Defaults are in the binary; no env vars.

### CMake / CTest

Build:

```bash
cmake -S . -B build
cmake --build build -j
cmake --build build --target test_orderbook_live_compare
```

Run the built binary:

```bash
./build/test_orderbook_live_compare
./build/test_orderbook_live_compare --live
./build/test_orderbook_live_compare --help
```

### Make

Build the executable only:

```bash
make test_orderbook_live_compare
```

Build (if needed) and run it:

```bash
make run_test_orderbook_live_compare
```

After a Make build, the binary is at the repo root:

```bash
./test_orderbook_live_compare --live
./test_orderbook_live_compare --help
```

### Flags

| Flag | Effect |
|------|--------|
| `--live` | Live futures endpoints (default is demo). |
| `--clear` | Clear the terminal before each refresh. |
| `--dump-dir PATH` | Append each raw WS frame as one line to `PATH/websocket_captures.jsonl`. |
