#include "hft/risk/pre_trade_risk.hpp"

#include <cmath>

namespace hft::risk {

PreTradeRisk::PreTradeRisk(RiskConfig config) : config_(config) {}

RiskRejectReason PreTradeRisk::validate(const execution::OrderCommand& cmd) const {
    if (config_.kill_switch != nullptr &&
        config_.kill_switch->load(std::memory_order_relaxed)) {
        return RiskRejectReason::KillSwitchEngaged;
    }

    if (cmd.qty <= 0.0 || cmd.qty > config_.max_order_qty) {
        return RiskRejectReason::OrderQtyExceeded;
    }

    const double notional = std::abs(cmd.price * cmd.qty);
    if (notional > config_.max_order_notional) {
        return RiskRejectReason::OrderNotionalExceeded;
    }

    const std::size_t i = idx(cmd.instrument);
    const double signed_qty = (cmd.side == Side::Buy) ? cmd.qty : -cmd.qty;
    const double projected = positions_[i] + signed_qty;
    if (std::abs(projected) > config_.max_abs_position) {
        return RiskRejectReason::PositionLimitExceeded;
    }

    return RiskRejectReason::None;
}

void PreTradeRisk::on_fill(md::Instrument instrument, double signed_qty) {
    positions_[idx(instrument)] += signed_qty;
}

double PreTradeRisk::position(md::Instrument instrument) const {
    return positions_[idx(instrument)];
}

std::size_t PreTradeRisk::idx(md::Instrument instrument) const {
    using md::Instrument;
    switch (instrument) {
        case Instrument::BtcUsdt:
            return 0;
        case Instrument::EthUsdt:
            return 1;
        case Instrument::SolUsdt:
            return 2;
        default:
            return 0;
    }
}

} // namespace hft::risk
