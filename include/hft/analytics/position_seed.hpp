#pragma once

#include <array>
#include <cstddef>
#include <string_view>

#include "hft/analytics/pnl_engine.hpp"
#include "hft/marketdata/binance_types.hpp"
#include "hft/riskmgmt/pre_trade_risk.hpp"

namespace hft::analytics {

struct PositionSeed {
    marketdata::Instrument instrument {marketdata::Instrument::Unknown};
    double position {0.0};
    double entry_price {0.0};
    bool present {false};
};

std::size_t parse_position_risk_seeds(std::string_view body, std::array<PositionSeed, 3>& seeds);
void apply_position_seeds(
    const std::array<PositionSeed, 3>& seeds,
    riskmgmt::PreTradeRisk& risk,
    std::array<PnLState, 3>& pnl_states);

} // namespace hft::analytics
