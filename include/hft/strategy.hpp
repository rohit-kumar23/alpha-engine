#pragma once

#include <optional>

#include "hft/types.hpp"

namespace hft {

struct StrategyParams {
    double alpha {0.02};
    double base_spread {1.0};
    double inventory_limit {5.0};
    double edge_threshold_bps {0.30};
    double min_qty {0.001};
    double max_qty {0.010};
    double qty_inventory_shrink {0.70};
};

struct StrategyState {
    double inventory {0.0};
    double volatility {0.0};
};

class StrategyEngine {
public:
    explicit StrategyEngine(StrategyParams params);
    std::optional<OrderIntent> on_book_update(const BookSnapshot& snap, StrategyState& state) const;

private:
    StrategyParams params_;
};

} // namespace hft
