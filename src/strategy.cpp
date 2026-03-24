#include "hft/strategy.hpp"

#include <cmath>

namespace hft {

StrategyEngine::StrategyEngine(StrategyParams params) : params_(params) {}

std::optional<OrderIntent> StrategyEngine::on_book_update(const BookSnapshot& snap, StrategyState& state) const {
    if (snap.best_bid <= 0.0 || snap.best_ask <= 0.0 || snap.best_ask <= snap.best_bid) {
        return std::nullopt;
    }

    if (std::abs(state.inventory) >= params_.inventory_limit) {
        return std::nullopt;
    }

    const double mid = 0.5 * (snap.best_bid + snap.best_ask);
    if (mid <= 0.0) {
        return std::nullopt;
    }
    const double micro = (snap.best_ask * snap.bid_qty + snap.best_bid * snap.ask_qty) /
        std::max(1e-9, snap.bid_qty + snap.ask_qty);
    const double edge_bps = std::abs(micro - mid) / mid * 10000.0;
    if (edge_bps < params_.edge_threshold_bps) {
        return std::nullopt;
    }

    const double inv_norm = std::min(1.0, std::abs(state.inventory) / std::max(1e-9, params_.inventory_limit));
    const double qty_scale = std::max(0.0, 1.0 - params_.qty_inventory_shrink * inv_norm);
    const double qty = params_.min_qty + (params_.max_qty - params_.min_qty) * qty_scale;
    if (qty <= 0.0) {
        return std::nullopt;
    }

    const double skew = micro - params_.alpha * state.inventory;
    const double bid_quote = skew - 0.5 * params_.base_spread;
    const double ask_quote = skew + 0.5 * params_.base_spread;

    if (micro > mid) {
        return OrderIntent{Side::Buy, bid_quote, qty};
    }
    return OrderIntent{Side::Sell, ask_quote, qty};
}

} // namespace hft
