#include "hft/analytics/position_seed.hpp"

#include <array>
#include <cmath>
#include <cstdlib>
#include <string>

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

std::string_view extract_quoted_field(std::string_view block, std::string_view key) {
    const std::string needle = "\"" + std::string(key) + "\":\"";
    const std::size_t pos = block.find(needle);
    if (pos == std::string_view::npos) {
        return {};
    }
    const std::size_t value_start = pos + needle.size();
    const std::size_t value_end = block.find('"', value_start);
    if (value_end == std::string_view::npos || value_end <= value_start) {
        return {};
    }
    return block.substr(value_start, value_end - value_start);
}

bool parse_double_sv(std::string_view v, double& out) {
    if (v.empty()) {
        return false;
    }
    const std::string tmp(v);
    char* parse_end = nullptr;
    const double parsed = std::strtod(tmp.c_str(), &parse_end);
    if (parse_end == tmp.c_str() || *parse_end != '\0') {
        return false;
    }
    out = parsed;
    return true;
}

} // namespace

std::size_t parse_position_risk_seeds(std::string_view body, std::array<PositionSeed, 3>& seeds) {
    for (auto& seed : seeds) {
        seed = PositionSeed{};
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

} // namespace hft::analytics
