#include "hft/analytics/pnl_engine.hpp"

#include <algorithm>
#include <cmath>

namespace hft {

PnLEngine::PnLEngine(double taker_fee_bps, double maker_fee_bps)
    : taker_fee_bps_(taker_fee_bps),
      maker_fee_bps_(maker_fee_bps >= 0.0 ? maker_fee_bps : taker_fee_bps) {}

void PnLEngine::on_fill(const Fill& fill, PnLState& state) const {
    constexpr double kEps = 1e-12;
    if (fill.qty <= 0.0 || fill.price <= 0.0) {
        return;
    }

    const double notional = fill.price * fill.qty;
    const double fee_bps = fill.is_maker ? maker_fee_bps_ : taker_fee_bps_;
    const double fee = (fee_bps / 10000.0) * notional;
    state.fees_paid += fee;

    const double signed_qty = (fill.side == Side::Buy) ? fill.qty : -fill.qty;
    const double inv_before = state.inventory;

    if (std::abs(inv_before) <= kEps || (inv_before > 0.0) == (signed_qty > 0.0)) {
        const double new_inventory = inv_before + signed_qty;
        if (std::abs(inv_before) <= kEps) {
            state.avg_price = fill.price;
            state.inventory = new_inventory;
            return;
        }
        const double open_before = std::abs(inv_before);
        const double open_add = std::abs(signed_qty);
        const double open_after = std::max(kEps, std::abs(new_inventory));
        state.avg_price = ((state.avg_price * open_before) + (fill.price * open_add)) / open_after;
        state.inventory = new_inventory;
        return;
    }

    const double close_qty = std::min(std::abs(inv_before), std::abs(signed_qty));
    if (inv_before > 0.0) {
        state.realized += (fill.price - state.avg_price) * close_qty;
    } else {
        state.realized += (state.avg_price - fill.price) * close_qty;
    }

    const double new_inventory = inv_before + signed_qty;
    if (std::abs(new_inventory) <= kEps) {
        state.inventory = 0.0;
        state.avg_price = 0.0;
        return;
    }

    const bool flipped = (inv_before > 0.0) != (new_inventory > 0.0);
    state.inventory = new_inventory;
    if (flipped) {
        // Remaining portion opened a fresh position in opposite direction.
        state.avg_price = fill.price;
    }
}

double PnLEngine::mark_to_market(const PnLState& state, double mid_price) const {
    return state.realized + (state.inventory * (mid_price - state.avg_price)) - state.fees_paid;
}

} // namespace hft
