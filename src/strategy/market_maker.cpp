#include "hft/strategy/market_maker.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace hft {

namespace {

constexpr double kEps = 1e-12;

double clamp_value(double v, double lo, double hi) {
    return std::min(hi, std::max(lo, v));
}

double floor_to_tick(double px, double tick_size) {
    if (tick_size <= 0.0) {
        return px;
    }
    return std::floor(px / tick_size) * tick_size;
}

double ceil_to_tick(double px, double tick_size) {
    if (tick_size <= 0.0) {
        return px;
    }
    return std::ceil(px / tick_size) * tick_size;
}

} // namespace

StrategyEngine::StrategyEngine(StrategyParams params) : params_(params) {}

QuoteIntent StrategyEngine::on_book_update(const BookSnapshot& snap, StrategyState& state) const {
    QuoteIntent out;
    if (snap.best_bid <= 0.0 || snap.best_ask <= 0.0 || snap.best_ask <= snap.best_bid) {
        return out;
    }

    if (params_.max_inventory <= 0.0) {
        return out;
    }

    const double mid = 0.5 * (snap.best_bid + snap.best_ask);
    if (mid <= 0.0) {
        return out;
    }

    // Streaming volatility estimate on log-mid returns using a fixed-size ring.
    if (state.last_mid > 0.0 && mid > 0.0 && std::abs(mid - state.last_mid) > kEps) {
        const double ret = std::log(mid / state.last_mid);
        state.ret_buf[state.ret_cursor] = ret;
        state.ret_cursor = static_cast<std::uint8_t>((state.ret_cursor + 1) % state.ret_buf.size());
        if (state.ret_count < state.ret_buf.size()) {
            ++state.ret_count;
        }
        if (state.ret_count >= 2) {
            double sum = 0.0;
            double sum_sq = 0.0;
            for (std::uint8_t i = 0; i < state.ret_count; ++i) {
                const double x = state.ret_buf[i];
                sum += x;
                sum_sq += x * x;
            }
            const double n = static_cast<double>(state.ret_count);
            const double var = std::max(0.0, (sum_sq / n) - ((sum / n) * (sum / n)));
            state.sigma_bps = std::sqrt(var) * 10000.0;
        }
    }
    state.last_mid = mid;

    // Imbalance must remain stable for a minimum number of updates.
    const double raw_imb = clamp_value(snap.imbalance, -1.0, 1.0);
    if ((raw_imb >= 0.0 && state.pending_imbalance < 0.0) ||
        (raw_imb < 0.0 && state.pending_imbalance >= 0.0)) {
        state.pending_imbalance = raw_imb;
        state.pending_imbalance_updates = 1;
    } else {
        state.pending_imbalance = raw_imb;
        if (state.pending_imbalance_updates < std::numeric_limits<std::uint32_t>::max()) {
            ++state.pending_imbalance_updates;
        }
    }

    const std::uint32_t stable_updates = std::max<std::uint32_t>(
        1,
        (params_.imbalance_stability_ms + std::max<std::uint32_t>(1, params_.update_interval_ms) - 1) /
            std::max<std::uint32_t>(1, params_.update_interval_ms));
    if (state.pending_imbalance_updates >= stable_updates) {
        state.stable_imbalance = state.pending_imbalance;
    }

    if (params_.sigma_threshold_bps > 0.0 && state.sigma_bps > params_.sigma_threshold_bps) {
        return out;
    }

    const double alpha_bps = clamp_value(
        params_.k_bps * state.stable_imbalance,
        -std::abs(params_.max_alpha_bps),
        std::abs(params_.max_alpha_bps));
    const double alpha_px = mid * alpha_bps / 10000.0;
    const double inv_ratio = state.inventory / std::max(kEps, std::abs(params_.max_inventory));
    const double gamma_px = mid * params_.gamma_bps / 10000.0;
    const double reservation_px = mid + alpha_px - (gamma_px * inv_ratio);

    double delta_bps = params_.c1 * state.sigma_bps + params_.c2_bps;
    delta_bps = clamp_value(delta_bps, params_.min_delta_bps, params_.max_delta_bps);
    const double delta_px = mid * delta_bps / 10000.0;
    if (delta_px <= 0.0) {
        return out;
    }

    const double min_required_delta_bps = std::max(0.0, params_.fee_bps + params_.min_profit_buffer_bps);
    const double min_required_delta_px = mid * min_required_delta_bps / 10000.0;
    if (delta_px < min_required_delta_px) {
        return out;
    }

    const double bid_raw = reservation_px - delta_px;
    const double ask_raw = reservation_px + delta_px;
    double bid_px = floor_to_tick(bid_raw, params_.tick_size);
    double ask_px = ceil_to_tick(ask_raw, params_.tick_size);
    const double tick = params_.tick_size > 0.0 ? params_.tick_size : kEps;
    if (!(bid_px < snap.best_ask)) {
        bid_px = floor_to_tick(snap.best_ask - tick, tick);
    }
    if (!(ask_px > snap.best_bid)) {
        ask_px = ceil_to_tick(snap.best_bid + tick, tick);
    }
    if (ask_px < bid_px + tick) {
        ask_px = bid_px + tick;
    }

    const double qty_scale = std::max(0.0, 1.0 - std::abs(inv_ratio));
    const double qty = std::max(params_.min_qty, params_.base_size * qty_scale);
    if (qty <= 0.0) {
        return out;
    }

    // Two-sided quoting target with inventory hard-side gating.
    if (state.inventory <= params_.max_inventory) {
        out.bid = OrderIntent{Side::Buy, bid_px, qty};
    }
    if (state.inventory >= -params_.max_inventory) {
        out.ask = OrderIntent{Side::Sell, ask_px, qty};
    }
    return out;
}

} // namespace hft
