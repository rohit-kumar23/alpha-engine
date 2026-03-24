#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "hft/md/binance_types.hpp"
#include "hft/types.hpp"

namespace hft::book {

enum class ApplyResult : std::uint8_t {
    Ignored,
    Applied,
    OutOfSync,
};

class L2Book {
public:
    ApplyResult apply(const md::MdEvent& event);
    bool seed_from_snapshot(const md::DepthSnapshot& snapshot);
    BookSnapshot snapshot() const;
    bool is_ready() const;
    bool is_in_sync() const;
    bool resync_required() const;
    bool needs_snapshot_seed() const;
    std::uint64_t last_update_id() const;

private:
    static constexpr std::size_t kMaxBookLevels = 32;

    struct Level {
        double px {};
        double qty {};
    };

    void upsert_bid(double px, double qty);
    void upsert_ask(double px, double qty);
    void erase_bid(double px);
    void erase_ask(double px);

    std::array<Level, kMaxBookLevels> bids_ {};
    std::array<Level, kMaxBookLevels> asks_ {};
    std::uint8_t bid_count_ {};
    std::uint8_t ask_count_ {};
    double best_bid_ {0.0};
    double best_ask_ {0.0};
    double bid_qty_ {0.0};
    double ask_qty_ {0.0};
    std::uint64_t last_final_update_id_ {};
    bool has_depth_sequence_ {false};
    bool snapshot_seeded_ {false};
    bool waiting_bridge_event_ {false};
    std::uint16_t waiting_bridge_skips_ {0};
    bool in_sync_ {false};
    bool resync_required_ {false};
    std::uint8_t warmup_depth_events_ {0};
};

} // namespace hft::book
