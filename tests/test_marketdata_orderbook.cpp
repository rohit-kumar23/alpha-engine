#include "tests/common/test_log.hpp"

#include <cstring>
#include <iostream>

namespace tests::marketdata {
int run_binance_parser_tests(tests::TestLog& log);
int run_ws_smoke_test(tests::TestLog& log, bool enable);
} // namespace tests::marketdata

namespace tests::orderbook {
int run_l2_book_tests(tests::TestLog& log);
} // namespace tests::orderbook
namespace tests::strategy {
int run_market_maker_tests(tests::TestLog& log);
} // namespace tests::strategy
namespace tests::ordermgmt {
int run_order_manager_tests(tests::TestLog& log);
} // namespace tests::ordermgmt

int main(int argc, char** argv) {
    bool verbose = false;
    bool ws_smoke = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        } else if (std::strcmp(argv[i], "--ws-smoke") == 0) {
            ws_smoke = true;
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
        log.info("=== marketdata: WebSocket smoke (optional --ws-smoke) ===");
        failures += tests::marketdata::run_ws_smoke_test(log, ws_smoke);
    }
    {
        tests::TestLog log(verbose);
        log.info("=== strategy: market maker (fixtures) ===");
        failures += tests::strategy::run_market_maker_tests(log);
    }
    {
        tests::TestLog log(verbose);
        log.info("=== ordermgmt: order manager (fixtures) ===");
        failures += tests::ordermgmt::run_order_manager_tests(log);
    }

    if (failures == 0) {
        std::cerr << "[PASS] test_marketdata_orderbook completed with 0 failures\n";
        return 0;
    }
    std::cerr << "[FAIL] test_marketdata_orderbook: " << failures << " check(s) failed\n";
    return 1;
}
