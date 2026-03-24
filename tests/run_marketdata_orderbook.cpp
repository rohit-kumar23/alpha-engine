#include "tests/common/test_log.hpp"

#include <cstring>
#include <iostream>

namespace tests::marketdata {
int run_binance_parser_tests(tests::TestLog& log);
int run_ws_smoke_test(tests::TestLog& log);
} // namespace tests::marketdata

namespace tests::orderbook {
int run_l2_book_tests(tests::TestLog& log);
} // namespace tests::orderbook

int main(int argc, char** argv) {
    bool verbose = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        }
    }

    int failures = 0;

    {
        tests::TestLog log(verbose);
        log.info("=== marketdata: BinanceParser (fixtures) ===");
        failures += tests::marketdata::run_binance_parser_tests(log);
    }
    {
        tests::TestLog log(verbose);
        log.info("=== orderbook: L2Book (fixtures + parser integration) ===");
        failures += tests::orderbook::run_l2_book_tests(log);
    }
    {
        tests::TestLog log(verbose);
        log.info("=== marketdata: WebSocket smoke (ALPHA_ENGINE_WS_SMOKE=1) ===");
        failures += tests::marketdata::run_ws_smoke_test(log);
    }

    if (failures == 0) {
        std::cerr << "[PASS] run_marketdata_orderbook completed with 0 failures\n";
        return 0;
    }
    std::cerr << "[FAIL] run_marketdata_orderbook: " << failures << " check(s) failed\n";
    return 1;
}
