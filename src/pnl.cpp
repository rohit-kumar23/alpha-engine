#include "hft/pnl.hpp"

namespace hft {

PnLEngine::PnLEngine(double fee_bps) : fee_bps_(fee_bps) {}

void PnLEngine::on_fill(const Fill& fill, PnLState& state) const {
    const double notional = fill.price * fill.qty;
    const double fee = (fee_bps_ / 10000.0) * notional;
    state.fees_paid += fee;

    if (fill.side == Side::Buy) {
        const double new_inventory = state.inventory + fill.qty;
        state.avg_price = (state.avg_price * state.inventory + fill.price * fill.qty) /
            (new_inventory > 0.0 ? new_inventory : 1.0);
        state.inventory = new_inventory;
    } else {
        const double closed = fill.qty;
        state.realized += (fill.price - state.avg_price) * closed;
        state.inventory -= fill.qty;
        if (state.inventory == 0.0) {
            state.avg_price = 0.0;
        }
    }
}

double PnLEngine::mark_to_market(const PnLState& state, double mid_price) const {
    return state.realized + (state.inventory * (mid_price - state.avg_price)) - state.fees_paid;
}

} // namespace hft
