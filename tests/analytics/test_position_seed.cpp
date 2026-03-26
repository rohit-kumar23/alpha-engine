#include "tests/common/test_log.hpp"

#include <array>
#include <cmath>

#include "hft/analytics/position_seed.hpp"

namespace tests::analytics {

namespace {

bool near(double a, double b, double eps = 1e-9) {
    return std::abs(a - b) <= eps;
}

} // namespace

int run_position_seed_tests(tests::TestLog& log) {
    const std::string body =
        R"([
            {"symbol":"ETHUSDT","positionAmt":"-2.500","entryPrice":"2500.10","ignore":"x"},
            {"symbol":"BTCUSDT","positionAmt":"0.100","entryPrice":"60000.00"},
            {"symbol":"SOLUSDT","positionAmt":"0","entryPrice":"0"}
        ])";

    std::array<hft::analytics::PositionSeed, 3> seeds {};
    const std::size_t n = hft::analytics::parse_position_risk_seeds(body, seeds);
    log.record(n == 3, "positionRisk parser finds all supported symbols");
    log.record(seeds[0].present && near(seeds[0].position, 0.1) && near(seeds[0].entry_price, 60000.0), "BTC seed parsed");
    log.record(seeds[1].present && near(seeds[1].position, -2.5) && near(seeds[1].entry_price, 2500.10), "ETH seed parsed");
    log.record(seeds[2].present && near(seeds[2].position, 0.0) && near(seeds[2].entry_price, 0.0), "SOL flat seed parsed");

    std::atomic<bool> kill{false};
    hft::riskmgmt::PreTradeRisk risk(hft::riskmgmt::RiskConfig{
        10.0,
        1'000'000.0,
        100.0,
        &kill,
    });
    std::array<hft::PnLState, 3> pnl {};
    hft::analytics::apply_position_seeds(seeds, risk, pnl);

    using hft::marketdata::Instrument;
    log.record(near(risk.position(Instrument::BtcUsdt), 0.1), "risk position seeded BTC");
    log.record(near(risk.position(Instrument::EthUsdt), -2.5), "risk position seeded ETH");
    log.record(near(risk.position(Instrument::SolUsdt), 0.0), "risk position seeded SOL");
    log.record(near(pnl[0].inventory, 0.1) && near(pnl[0].avg_price, 60000.0), "pnl seeded BTC");
    log.record(near(pnl[1].inventory, -2.5) && near(pnl[1].avg_price, 2500.10), "pnl seeded ETH");
    log.record(near(pnl[2].inventory, 0.0) && near(pnl[2].avg_price, 0.0), "pnl seeded SOL flat");

    log.summary("analytics position seed");
    return log.failure_count();
}

} // namespace tests::analytics
