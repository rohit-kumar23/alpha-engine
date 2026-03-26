#include "tests/common/test_log.hpp"

#include <array>
#include <cmath>
#include <fstream>
#include <sstream>

#include "hft/analytics/position_seed.hpp"

namespace tests::analytics {

namespace {

bool near(double a, double b, double eps = 1e-9) {
    return std::abs(a - b) <= eps;
}

std::string read_fixture(const char* rel_path) {
    std::ifstream in(rel_path);
    if (!in.good()) {
        const std::string alt = std::string("../") + rel_path;
        in = std::ifstream(alt);
    }
    if (!in.good()) {
        return {};
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

} // namespace

int run_position_seed_tests(tests::TestLog& log) {
    const std::string body =
        R"([
            {"symbol":"ETHUSDT","positionAmt":"-2.500","entryPrice":"2500.10","ignore":"x"},
            {"symbol":"ETHUSDT","positionAmt":"bad","entryPrice":"2500.10","ignore":"broken"},
            {"symbol":"BTCUSDT","positionAmt":"0.100","entryPrice":"60000.00"},
            {"symbol":"SOLUSDT","positionAmt":"0","entryPrice":"0"}
        ])";

    std::array<hft::analytics::PositionSeed, 3> seeds {};
    std::size_t invalid = 0;
    const std::size_t n = hft::analytics::parse_position_risk_seeds(body, seeds, &invalid);
    log.record(n == 3, "positionRisk parser finds all supported symbols");
    log.record(invalid == 1, "positionRisk parser counts malformed records");
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

    const std::string demo_fixture = read_fixture("tests/fixtures/position_risk_demo.json");
    const std::string live_fixture = read_fixture("tests/fixtures/position_risk_live.json");
    log.record(!demo_fixture.empty() && !live_fixture.empty(), "positionRisk fixtures load");
    if (!demo_fixture.empty()) {
        std::array<hft::analytics::PositionSeed, 3> demo {};
        std::size_t demo_invalid = 0;
        const std::size_t demo_n = hft::analytics::parse_position_risk_seeds(demo_fixture, demo, &demo_invalid);
        log.record(demo_n == 3 && demo_invalid == 0, "demo fixture parses all symbols cleanly");
    }
    if (!live_fixture.empty()) {
        std::array<hft::analytics::PositionSeed, 3> live {};
        std::size_t live_invalid = 0;
        const std::size_t live_n = hft::analytics::parse_position_risk_seeds(live_fixture, live, &live_invalid);
        log.record(live_n == 3 && live_invalid == 0, "live fixture parses all symbols cleanly");
    }

    log.record(
        !hft::analytics::strict_seed_ok(true, 2, 1, 3),
        "strict seed mode rejects missing/invalid symbol seeds");
    log.record(
        hft::analytics::strict_seed_ok(false, 2, 1, 3),
        "non-strict seed mode tolerates partial startup seeds");

    log.summary("analytics position seed");
    return log.failure_count();
}

} // namespace tests::analytics
