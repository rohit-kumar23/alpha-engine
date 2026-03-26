#pragma once

#include <array>
#include <optional>

#include "hft/types.hpp"

namespace hft {

struct StrategyParams {
    double k_bps {1.5};
    double max_alpha_bps {3.0};
    double gamma_bps {3.0};
    double c1 {1.5};
    double c2_bps {3.0};
    double min_delta_bps {3.0};
    double max_delta_bps {10.0};
    double sigma_threshold_bps {10.0};
    double fee_bps {2.0};
    double min_profit_buffer_bps {0.5};
    double tick_size {0.01};
    double max_inventory {5.0};
    double base_size {0.010};
    double min_qty {0.001};
    std::uint32_t imbalance_stability_ms {100};
    std::uint32_t update_interval_ms {100};
};

struct StrategyState {
    double inventory {0.0};
    double sigma_bps {0.0};
    double stable_imbalance {0.0};
    double pending_imbalance {0.0};
    std::uint32_t pending_imbalance_updates {0};
    double last_mid {0.0};
    std::array<double, 64> ret_buf {};
    std::uint8_t ret_count {0};
    std::uint8_t ret_cursor {0};
};

class StrategyEngine {
public:
    explicit StrategyEngine(StrategyParams params);
    QuoteIntent on_book_update(const BookSnapshot& snap, StrategyState& state) const;

private:
    StrategyParams params_;
};

} // namespace hft
