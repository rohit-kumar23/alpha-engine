#include "hft/execution/user_stream_parser.hpp"

#include <charconv>
#include <cctype>
#include <string>

namespace hft::execution {

namespace {

std::optional<std::string_view> str_field(std::string_view json, std::string_view key) {
    const std::string needle = "\"" + std::string(key) + "\":\"";
    const auto pos = json.find(needle);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    const auto start = pos + needle.size();
    const auto end = json.find('"', start);
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    return json.substr(start, end - start);
}

std::optional<std::string_view> extract_futures_order_object(std::string_view json) {
    constexpr std::string_view key = "\"o\":";
    std::size_t pos = json.find(key);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    pos += key.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == '\r')) {
        ++pos;
    }
    if (pos >= json.size() || json[pos] != '{') {
        return std::nullopt;
    }
    int depth = 0;
    const std::size_t start = pos;
    for (; pos < json.size(); ++pos) {
        const char ch = json[pos];
        if (ch == '{') {
            ++depth;
        } else if (ch == '}') {
            --depth;
            if (depth == 0) {
                return json.substr(start, pos - start + 1);
            }
        }
    }
    return std::nullopt;
}

std::optional<double> dbl_order_field(std::string_view json, std::string_view key) {
    const std::string needle = "\"" + std::string(key) + "\":";
    const auto pos = json.find(needle);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    std::size_t i = pos + needle.size();
    while (i < json.size() && (json[i] == ' ' || json[i] == '\t')) {
        ++i;
    }
    if (i >= json.size()) {
        return std::nullopt;
    }
    if (json[i] == '"') {
        ++i;
        const auto end = json.find('"', i);
        if (end == std::string_view::npos) {
            return std::nullopt;
        }
        const std::string_view slice(json.data() + i, end - i);
        double out {};
        const auto [ptr, ec] = std::from_chars(slice.data(), slice.data() + slice.size(), out);
        if (ec != std::errc() || ptr != slice.data() + slice.size()) {
            return std::nullopt;
        }
        return out;
    }
    std::size_t end = i;
    while (end < json.size() && (std::isdigit(static_cast<unsigned char>(json[end])) || json[end] == '.' || json[end] == '-' ||
                                  json[end] == 'e' || json[end] == 'E' || json[end] == '+')) {
        ++end;
    }
    if (end == i) {
        return std::nullopt;
    }
    const std::string_view slice(json.data() + i, end - i);
    double out {};
    const auto [ptr, ec] = std::from_chars(slice.data(), slice.data() + slice.size(), out);
    if (ec != std::errc() || ptr != slice.data() + slice.size()) {
        return std::nullopt;
    }
    return out;
}

std::uint64_t parse_client_order_id(std::string_view cid) {
    const std::size_t us = cid.rfind('_');
    const std::size_t start = (us == std::string_view::npos) ? 0 : us + 1;
    if (start >= cid.size()) {
        return 0;
    }
    std::uint64_t out {};
    const auto [ptr, ec] = std::from_chars(cid.data() + start, cid.data() + cid.size(), out);
    if (ec != std::errc() || ptr != cid.data() + cid.size()) {
        return 0;
    }
    return out;
}

bool str_ieq(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        const unsigned char ca = static_cast<unsigned char>(a[i]);
        const unsigned char cb = static_cast<unsigned char>(b[i]);
        const char la = static_cast<char>(std::tolower(ca));
        const char lb = static_cast<char>(std::tolower(cb));
        if (la != lb) {
            return false;
        }
    }
    return true;
}

} // namespace

std::optional<ExecReport> UserStreamParser::parse_order_trade_update(std::string_view json) const {
    const auto event_type = str_field(json, "e");
    if (!event_type.has_value() || *event_type != "ORDER_TRADE_UPDATE") {
        return std::nullopt;
    }

    const std::string_view blob = extract_futures_order_object(json).value_or(json);

    const auto exec_type = str_field(blob, "x");
    if (!exec_type.has_value()) {
        return std::nullopt;
    }

    const auto ord_status = str_field(blob, "X");

    ExecReport report {};
    if (*exec_type == "TRADE") {
        report.type = ExecEventType::Fill;
    } else if (*exec_type == "NEW") {
        report.type = ExecEventType::Ack;
    } else if (*exec_type == "REJECTED") {
        report.type = ExecEventType::Reject;
        report.terminal = true;
    } else if (*exec_type == "CANCELED" || *exec_type == "EXPIRED") {
        report.type = ExecEventType::Canceled;
        report.terminal = true;
    } else if (*exec_type == "AMENDMENT" || *exec_type == "CALCULATED") {
        return std::nullopt;
    } else {
        return std::nullopt;
    }

    if (ord_status.has_value()) {
        if (str_ieq(*ord_status, "FILLED") || str_ieq(*ord_status, "CANCELED") || str_ieq(*ord_status, "EXPIRED")) {
            report.terminal = true;
        }
    }

    const auto cid = str_field(blob, "c");
    report.client_order_id = cid.has_value() ? parse_client_order_id(*cid) : 0;

    const auto side = str_field(blob, "S");
    if (side.has_value() && str_ieq(*side, "SELL")) {
        report.side = Side::Sell;
    } else {
        report.side = Side::Buy;
    }
    report.last_fill_qty = dbl_order_field(blob, "l").value_or(0.0);
    report.last_fill_price = dbl_order_field(blob, "L").value_or(0.0);

    const auto sym = str_field(blob, "s");
    if (sym.has_value()) {
        if (str_ieq(*sym, "BTCUSDT")) {
            report.instrument = marketdata::Instrument::BtcUsdt;
        } else if (str_ieq(*sym, "ETHUSDT")) {
            report.instrument = marketdata::Instrument::EthUsdt;
        } else if (str_ieq(*sym, "SOLUSDT")) {
            report.instrument = marketdata::Instrument::SolUsdt;
        }
    }
    return report;
}

} // namespace hft::execution
