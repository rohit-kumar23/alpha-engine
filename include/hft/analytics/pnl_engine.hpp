#pragma once

#include "hft/types.hpp"

namespace hft {

struct PnLState {
    double realized {0.0};
    double inventory {0.0};
    double avg_price {0.0};
    double fees_paid {0.0};
};

class PnLEngine {
public:
    explicit PnLEngine(double taker_fee_bps, double maker_fee_bps = -1.0);
    void on_fill(const Fill& fill, PnLState& state) const;
    double mark_to_market(const PnLState& state, double mid_price) const;

private:
    double taker_fee_bps_ {0.0};
    double maker_fee_bps_ {0.0};
};

} // namespace hft
