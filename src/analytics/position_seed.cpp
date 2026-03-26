#include "hft/analytics/position_seed.hpp"

#include <array>
#include <charconv>
#include <cmath>

namespace hft::analytics {

namespace {

std::size_t instrument_idx(marketdata::Instrument instrument) {
    using marketdata::Instrument;
    switch (instrument) {
        case Instrument::BtcUsdt: return 0;
        case Instrument::EthUsdt: return 1;
        case Instrument::SolUsdt: return 2;
        default: return 0;
    }
}

marketdata::Instrument parse_symbol(std::string_view symbol) {
    if (symbol == "BTCUSDT") return marketdata::Instrument::BtcUsdt;
    if (symbol == "ETHUSDT") return marketdata::Instrument::EthUsdt;
    if (symbol == "SOLUSDT") return marketdata::Instrument::SolUsdt;
    return marketdata::Instrument::Unknown;
}

bool key_matches_at(std::string_view s, std::size_t qpos, std::string_view key) {
    if (qpos + 2 + key.size() >= s.size()) {
        return false;
    }
    if (s[qpos] != '"' || s[qpos + 1 + key.size()] != '"') {
        return false;
    }
    return s.substr(qpos + 1, key.size()) == key;
}

std::string_view extract_quoted_field(std::string_view block, std::string_view key) {
    for (std::size_t i = 0; i < block.size(); ++i) {
        if (block[i] != '"') {
            continue;
        }
        if (!key_matches_at(block, i, key)) {
            continue;
        }
        std::size_t p = i + key.size() + 2;
        while (p < block.size() && (block[p] == ' ' || block[p] == '\t')) {
            ++p;
        }
        if (p >= block.size() || block[p] != ':') {
            continue;
        }
        ++p;
        while (p < block.size() && (block[p] == ' ' || block[p] == '\t')) {
            ++p;
        }
        if (p >= block.size() || block[p] != '"') {
            continue;
        }
        ++p;
        const std::size_t start = p;
        const std::size_t end = block.find('"', start);
        if (end == std::string_view::npos || end <= start) {
            return {};
        }
        return block.substr(start, end - start);
    }
    return {};
}

bool parse_double_sv(std::string_view v, double& out) {
    if (v.empty()) {
        return false;
    }
    const auto* b = v.data();
    const auto* e = v.data() + v.size();
    const auto [ptr, ec] = std::from_chars(b, e, out);
    return ec == std::errc() && ptr == e;
}

} // namespace

std::size_t parse_position_risk_seeds(
    std::string_view body,
    std::array<PositionSeed, 3>& seeds,
    std::size_t* invalid_records) {
    for (auto& seed : seeds) {
        seed = PositionSeed{};
    }
    if (invalid_records != nullptr) {
        *invalid_records = 0;
    }

    std::size_t parsed = 0;
    for (std::size_t pos = 0; pos < body.size(); ++pos) {
        if (body[pos] != '{') {
            continue;
        }
        int depth = 0;
        std::size_t end = pos;
        for (; end < body.size(); ++end) {
            if (body[end] == '{') {
                ++depth;
            } else if (body[end] == '}') {
                --depth;
                if (depth == 0) {
                    break;
                }
            }
        }
        if (end >= body.size()) {
            break;
        }
        const std::string_view block = body.substr(pos, end - pos + 1);
        pos = end;

        const std::string_view symbol_sv = extract_quoted_field(block, "symbol");
        const marketdata::Instrument instrument = parse_symbol(symbol_sv);
        if (instrument == marketdata::Instrument::Unknown) {
            continue;
        }
        const std::string_view pos_sv = extract_quoted_field(block, "positionAmt");
        const std::string_view entry_sv = extract_quoted_field(block, "entryPrice");
        double position = 0.0;
        double entry = 0.0;
        if (!parse_double_sv(pos_sv, position) || !parse_double_sv(entry_sv, entry)) {
            if (invalid_records != nullptr) {
                ++(*invalid_records);
            }
            continue;
        }
        auto& seed = seeds[instrument_idx(instrument)];
        seed.instrument = instrument;
        seed.position = position;
        seed.entry_price = entry;
        if (!seed.present) {
            seed.present = true;
            ++parsed;
        }
    }
    return parsed;
}

void apply_position_seeds(
    const std::array<PositionSeed, 3>& seeds,
    riskmgmt::PreTradeRisk& risk,
    std::array<PnLState, 3>& pnl_states) {
    for (std::size_t i = 0; i < seeds.size(); ++i) {
        if (!seeds[i].present) {
            continue;
        }
        risk.set_position(seeds[i].instrument, seeds[i].position);
        pnl_states[i].inventory = seeds[i].position;
        pnl_states[i].avg_price = (std::abs(seeds[i].position) > 1e-12) ? seeds[i].entry_price : 0.0;
    }
}

bool strict_seed_ok(
    bool require_all_symbols,
    std::size_t parsed_symbols,
    std::size_t invalid_records,
    std::size_t expected_symbols) {
    if (!require_all_symbols) {
        return true;
    }
    return invalid_records == 0 && parsed_symbols >= expected_symbols;
}

} // namespace hft::analytics
