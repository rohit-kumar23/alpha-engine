#include "hft/marketdata/binance_parser.hpp"

#include <charconv>
#include <string>

namespace hft::marketdata {

namespace {

std::optional<std::string_view> json_string_value(std::string_view json, std::string_view key) {
    const std::string needle = "\"" + std::string(key) + "\":\"";
    const std::size_t key_pos = json.find(needle);
    if (key_pos == std::string_view::npos) {
        return std::nullopt;
    }
    const std::size_t start = key_pos + needle.size();
    const std::size_t end = json.find('"', start);
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    return json.substr(start, end - start);
}

std::optional<double> json_double_value(std::string_view json, std::string_view key) {
    const auto v = json_string_value(json, key);
    if (!v.has_value()) {
        return std::nullopt;
    }
    double out {};
    const auto [ptr, ec] = std::from_chars(v->data(), v->data() + v->size(), out);
    if (ec != std::errc() || ptr != (v->data() + v->size())) {
        return std::nullopt;
    }
    return out;
}

std::optional<std::uint64_t> json_uint64_value(std::string_view json, std::string_view key) {
    const std::string needle = "\"" + std::string(key) + "\":";
    const std::size_t key_pos = json.find(needle);
    if (key_pos == std::string_view::npos) {
        return std::nullopt;
    }
    const std::size_t start = key_pos + needle.size();
    std::size_t end = start;
    while (end < json.size() && json[end] >= '0' && json[end] <= '9') {
        ++end;
    }
    if (end == start) {
        return std::nullopt;
    }

    std::uint64_t out {};
    const auto* begin = json.data() + start;
    const auto* finish = json.data() + end;
    const auto [ptr, ec] = std::from_chars(begin, finish, out);
    if (ec != std::errc() || ptr != finish) {
        return std::nullopt;
    }
    return out;
}

std::size_t parse_depth_levels(
    std::string_view json,
    std::string_view key,
    std::array<double, kDepthLevelsPerEvent>& px_out,
    std::array<double, kDepthLevelsPerEvent>& qty_out) {
    const std::string needle = "\"" + std::string(key) + "\":[";
    const std::size_t key_pos = json.find(needle);
    if (key_pos == std::string_view::npos) {
        return 0;
    }

    std::size_t pos = key_pos + needle.size();
    std::size_t count = 0;
    while (count < kDepthLevelsPerEvent) {
        const std::size_t px_q1 = json.find('"', pos);
        if (px_q1 == std::string_view::npos) {
            break;
        }
        const std::size_t px_q2 = json.find('"', px_q1 + 1);
        if (px_q2 == std::string_view::npos) {
            break;
        }
        double px {};
        const auto [px_ptr, px_ec] = std::from_chars(json.data() + px_q1 + 1, json.data() + px_q2, px);
        if (px_ec != std::errc() || px_ptr != json.data() + px_q2) {
            break;
        }

        const std::size_t qty_q1 = json.find('"', px_q2 + 1);
        if (qty_q1 == std::string_view::npos) {
            break;
        }
        const std::size_t qty_q2 = json.find('"', qty_q1 + 1);
        if (qty_q2 == std::string_view::npos) {
            break;
        }
        double qty {};
        const auto [qty_ptr, qty_ec] = std::from_chars(json.data() + qty_q1 + 1, json.data() + qty_q2, qty);
        if (qty_ec != std::errc() || qty_ptr != json.data() + qty_q2) {
            break;
        }

        px_out[count] = px;
        qty_out[count] = qty;
        ++count;

        const std::size_t close = json.find(']', qty_q2 + 1);
        if (close == std::string_view::npos) {
            break;
        }
        const std::size_t next_open = json.find('[', close + 1);
        if (next_open == std::string_view::npos) {
            break;
        }
        if (json[next_open - 1] == ']') {
            break;
        }
        pos = next_open;
    }
    return count;
}

} // namespace

std::optional<MdEvent> BinanceParser::parse_combined_message(std::string_view json, std::uint64_t ts_recv_ns) const {
    const auto stream = json_string_value(json, "stream");
    if (!stream.has_value()) {
        return std::nullopt;
    }

    MdEvent event;
    event.ts_recv_ns = ts_recv_ns;
    event.instrument = instrument_from_stream(*stream);
    if (event.instrument == Instrument::Unknown) {
        return std::nullopt;
    }

    if (stream->find("@bookTicker") != std::string_view::npos) {
        event.type = MdEventType::BookTicker;
        event.bid_px = json_double_value(json, "b").value_or(0.0);
        event.ask_px = json_double_value(json, "a").value_or(0.0);
        event.bid_qty = json_double_value(json, "B").value_or(0.0);
        event.ask_qty = json_double_value(json, "A").value_or(0.0);
        event.ts_exchange_ns = json_uint64_value(json, "T").value_or(0) * 1000000ULL;
        return event;
    }

    if (stream->find("@depth@100ms") != std::string_view::npos) {
        event.type = MdEventType::DepthUpdate;
        event.depth_first_update_id = json_uint64_value(json, "U").value_or(0);
        event.depth_final_update_id = json_uint64_value(json, "u").value_or(0);
        event.depth_prev_final_update_id = json_uint64_value(json, "pu").value_or(0);
        event.bid_levels_count = static_cast<std::uint8_t>(
            parse_depth_levels(json, "b", event.bid_px_levels, event.bid_qty_levels));
        event.ask_levels_count = static_cast<std::uint8_t>(
            parse_depth_levels(json, "a", event.ask_px_levels, event.ask_qty_levels));
        event.bid_px = event.bid_levels_count > 0 ? event.bid_px_levels[0] : 0.0;
        event.bid_qty = event.bid_levels_count > 0 ? event.bid_qty_levels[0] : 0.0;
        event.ask_px = event.ask_levels_count > 0 ? event.ask_px_levels[0] : 0.0;
        event.ask_qty = event.ask_levels_count > 0 ? event.ask_qty_levels[0] : 0.0;
        event.ts_exchange_ns = json_uint64_value(json, "E").value_or(0) * 1000000ULL;
        return event;
    }

    if (stream->find("@aggTrade") != std::string_view::npos) {
        event.type = MdEventType::AggTrade;
        event.trade_px = json_double_value(json, "p").value_or(0.0);
        event.trade_qty = json_double_value(json, "q").value_or(0.0);
        event.ts_exchange_ns = json_uint64_value(json, "T").value_or(0) * 1000000ULL;
        return event;
    }

    return std::nullopt;
}

} // namespace hft::marketdata
