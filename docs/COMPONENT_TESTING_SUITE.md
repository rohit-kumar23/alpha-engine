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
  run_marketdata_orderbook.cpp    # Entry point: registers and runs the suites above
```

Guidelines:

- **One subdirectory per top-level domain** under `include/hft/` when the tests target that domain (`marketdata`, `orderbook`, `execution`, …).
- **Shared code** lives in `tests/common/`; avoid copying helpers into every suite.
- **Entry points** are named `run_<area>.cpp` (for example `run_marketdata_orderbook.cpp`) and only orchestrate: parse CLI flags, run suites, aggregate exit status.
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

The test executable is built as `build/run_marketdata_orderbook`. You can run it directly:

```bash
./build/run_marketdata_orderbook
./build/run_marketdata_orderbook --verbose
```

### Make

```bash
make test_marketdata_orderbook
```

Alias:

```bash
make test
```

The binary is written to the repo root: `./run_marketdata_orderbook`.

### Verifying success

- **Exit code `0`**: all checks in that run passed.
- **Exit code non-zero**: one or more `[FAIL]` lines; scroll up for the failing assertion name.
- **`--verbose`**: prints extra `[VERBOSE]` lines (for example the first WebSocket payload in smoke mode).

## Optional live WebSocket check

By default, tests use **offline fixtures** only (no network).

To additionally verify TLS + WebSocket connectivity and that at least one combined-stream frame parses:

```bash
ALPHA_ENGINE_WS_SMOKE=1 ./build/run_marketdata_orderbook
```

Optional overrides (same semantics as the main engine’s stream endpoint):

- `BINANCE_STREAM_WS_HOST`
- `BINANCE_STREAM_WS_PORT`

Leave `ALPHA_ENGINE_WS_SMOKE` unset in CI so runs stay deterministic and do not depend on the exchange.

## Adding a new test executable

1. Add sources under `tests/<domain>/` and a runner `tests/run_<something>.cpp` if it is a new program.
2. **CMake** (`CMakeLists.txt`): in the **Tests** section, add a new `add_executable(...)`, `target_include_directories(... PRIVATE include ${CMAKE_SOURCE_DIR})`, link OpenSSL and `pthread`, mirror compiler warnings on the production target, then `add_test(NAME <descriptive_name> COMMAND <target>)`.
3. **Makefile**: in the **Tests** section, add a new block with `TEST_<NAME>_TARGET`, `_SRCS`, `_OBJS`, a rule linking the test binary, and a `.PHONY` target that builds and runs it.
4. **`.gitignore`**: add the new executable name next to `run_marketdata_orderbook` if it is produced in the repo root via Make.

Keep production and test targets in separate commented blocks in both build files so it stays obvious what ships to production versus what is for validation.
