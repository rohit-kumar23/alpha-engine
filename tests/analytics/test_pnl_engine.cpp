#include "tests/common/test_log.hpp"

#include <cmath>

#include "hft/analytics/pnl_engine.hpp"

namespace tests::analytics {

namespace {

bool near(double a, double b, double eps = 1e-9) {
    return std::abs(a - b) <= eps;
}

} // namespace

int run_pnl_engine_tests(tests::TestLog& log) {
    hft::PnLEngine pnl(2.0, 1.0); // 2 bps taker, 1 bps maker
    hft::PnLState s;

    pnl.on_fill(hft::Fill{hft::Side::Buy, 100.0, 1.0, 1, false}, s);
    log.record(near(s.inventory, 1.0), "buy opens long inventory");
    log.record(near(s.avg_price, 100.0), "buy sets avg price");

    pnl.on_fill(hft::Fill{hft::Side::Sell, 110.0, 0.4, 2, true}, s);
    log.record(near(s.inventory, 0.6), "sell partially closes long");
    log.record(near(s.realized, 4.0), "realized pnl for partial long close");
    log.record(near(s.avg_price, 100.0), "avg unchanged on partial close");

    pnl.on_fill(hft::Fill{hft::Side::Sell, 90.0, 1.0, 3, false}, s);
    log.record(near(s.inventory, -0.4), "oversell flips to short");
    log.record(near(s.avg_price, 90.0), "avg resets on position flip");
    log.record(near(s.realized, -2.0), "realized pnl includes loss on remaining long close");

    pnl.on_fill(hft::Fill{hft::Side::Buy, 80.0, 0.2, 4, true}, s);
    log.record(near(s.inventory, -0.2), "buy partially closes short");
    log.record(near(s.realized, 0.0), "short cover realizes gain");
    log.record(near(s.avg_price, 90.0), "short avg unchanged on partial cover");

    pnl.on_fill(hft::Fill{hft::Side::Buy, 95.0, 0.3, 5, false}, s);
    log.record(near(s.inventory, 0.1), "buy flips short to long");
    log.record(near(s.avg_price, 95.0), "avg resets to flip price");
    log.record(near(s.realized, -1.0), "realized pnl includes short close loss");

    const double mtm = pnl.mark_to_market(s, 100.0);
    log.record(mtm > -1.0, "mark-to-market includes unrealized and fees");
    const double expected_fees =
        (2.0 / 10000.0) * (100.0 * 1.0) +
        (1.0 / 10000.0) * (110.0 * 0.4) +
        (2.0 / 10000.0) * (90.0 * 1.0) +
        (1.0 / 10000.0) * (80.0 * 0.2) +
        (2.0 / 10000.0) * (95.0 * 0.3);
    log.record(near(s.fees_paid, expected_fees, 1e-12), "maker/taker fees accumulate correctly");

    log.summary("analytics PnLEngine");
    return log.failure_count();
}

} // namespace tests::analytics
