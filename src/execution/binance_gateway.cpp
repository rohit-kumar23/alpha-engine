#include "hft/execution/binance_gateway.hpp"

#include <atomic>
#include <charconv>
#include <cctype>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <chrono>
#include <cmath>
#include <mutex>
#include <thread>

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/hmac.h>
#include <openssl/ssl.h>

#include "hft/marketdata/binance_types.hpp"

namespace hft::execution {

namespace {

const char* side_str(Side side) {
    return side == Side::Buy ? "BUY" : "SELL";
}

const char* cmd_symbol(marketdata::Instrument i) {
    using marketdata::Instrument;
    switch (i) {
        case Instrument::BtcUsdt:
            return "BTCUSDT";
        case Instrument::EthUsdt:
            return "ETHUSDT";
        case Instrument::SolUsdt:
            return "SOLUSDT";
        default:
            return "";
    }
}

std::size_t instrument_idx(marketdata::Instrument i) {
    using marketdata::Instrument;
    switch (i) {
        case Instrument::BtcUsdt:
            return 0;
        case Instrument::EthUsdt:
            return 1;
        case Instrument::SolUsdt:
            return 2;
        default:
            return 0;
    }
}

double fallback_min_notional(marketdata::Instrument i) {
    using marketdata::Instrument;
    switch (i) {
        case Instrument::BtcUsdt:
            return 100.0;
        case Instrument::EthUsdt:
            return 20.0;
        case Instrument::SolUsdt:
            return 5.0;
        default:
            return 5.0;
    }
}

SymbolConstraints fallback_constraints(marketdata::Instrument i) {
    using marketdata::Instrument;
    switch (i) {
        case Instrument::BtcUsdt:
            return SymbolConstraints{0.10, 0.001, fallback_min_notional(i), 1, 3, true};
        case Instrument::EthUsdt:
            return SymbolConstraints{0.01, 0.001, fallback_min_notional(i), 2, 3, true};
        case Instrument::SolUsdt:
            return SymbolConstraints{0.01, 0.01, fallback_min_notional(i), 2, 2, true};
        default:
            return SymbolConstraints{0.01, 0.001, 5.0, 2, 3, true};
    }
}

int decimals_from_step(double step, int fallback_dp) {
    if (step <= 0.0) {
        return fallback_dp;
    }
    int dp = 0;
    double s = step;
    while (dp < 9 && std::fabs(s - std::round(s)) > 1e-9) {
        s *= 10.0;
        ++dp;
    }
    return dp;
}

std::string_view extract_quoted_value(std::string_view block, std::string_view key) {
    const std::size_t p = block.find(key);
    if (p == std::string_view::npos) {
        return {};
    }
    std::size_t q1 = block.find('"', p + key.size());
    if (q1 == std::string_view::npos) {
        return {};
    }
    ++q1;
    const std::size_t q2 = block.find('"', q1);
    if (q2 == std::string_view::npos || q2 <= q1) {
        return {};
    }
    return block.substr(q1, q2 - q1);
}

double parse_quoted_double(std::string_view block, std::string_view key, double fallback) {
    const std::string_view v = extract_quoted_value(block, key);
    if (v.empty()) {
        return fallback;
    }
    double out = fallback;
    const std::string s(v);
    char* end = nullptr;
    out = std::strtod(s.c_str(), &end);
    if (end == s.c_str() || *end != '\0') {
        return fallback;
    }
    return out;
}

int tcp_connect(const std::string& host, const std::string& port) {
    addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &result) != 0) {
        return -1;
    }

    int fd = -1;
    for (addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) {
            continue;
        }
        if (::connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(result);
    return fd;
}

bool ssl_read_all(SSL* ssl, std::string& out) {
    char buf[8192];
    const int n = SSL_read(ssl, buf, sizeof(buf));
    if (n <= 0) {
        return false;
    }
    out.append(buf, static_cast<std::size_t>(n));
    return true;
}

int parse_http_status_line(std::string_view line) {
    const std::size_t http = line.find("HTTP/");
    if (http == std::string_view::npos) {
        return 0;
    }
    std::size_t i = http;
    while (i < line.size() && line[i] != ' ') {
        ++i;
    }
    while (i < line.size() && line[i] == ' ') {
        ++i;
    }
    int code = 0;
    while (i < line.size() && line[i] >= '0' && line[i] <= '9') {
        code = code * 10 + (line[i] - '0');
        ++i;
    }
    return code;
}

int parse_binance_error_code(std::string_view body) {
    constexpr std::string_view key = "\"code\"";
    std::size_t pos = body.find(key);
    if (pos == std::string_view::npos) {
        return 0;
    }
    pos += key.size();
    while (pos < body.size() && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == ':')) {
        ++pos;
    }
    int sign = 1;
    if (pos < body.size() && body[pos] == '-') {
        sign = -1;
        ++pos;
    }
    int code = 0;
    bool any = false;
    while (pos < body.size() && body[pos] >= '0' && body[pos] <= '9') {
        any = true;
        code = code * 10 + (body[pos] - '0');
        ++pos;
    }
    return any ? sign * code : 0;
}

std::int64_t parse_binance_server_time_ms(std::string_view body) {
    constexpr std::string_view key = "\"serverTime\"";
    std::size_t pos = body.find(key);
    if (pos == std::string_view::npos) {
        return 0;
    }
    pos += key.size();
    while (pos < body.size() && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == ':')) {
        ++pos;
    }
    std::int64_t ts = 0;
    const auto [ptr, ec] = std::from_chars(body.data() + pos, body.data() + body.size(), ts);
    if (ec != std::errc() || ptr == body.data() + pos) {
        return 0;
    }
    return ts;
}

void trim_ascii(std::string_view& s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) {
        s.remove_prefix(1);
    }
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r')) {
        s.remove_suffix(1);
    }
}

bool header_name_ieq(std::string_view line, std::string_view name_colon_lower) {
    if (line.size() < name_colon_lower.size()) {
        return false;
    }
    for (std::size_t j = 0; j < name_colon_lower.size(); ++j) {
        char a = line[j];
        char b = name_colon_lower[j];
        if (a >= 'A' && a <= 'Z') {
            a = static_cast<char>(a - 'A' + 'a');
        }
        if (a != b) {
            return false;
        }
    }
    return true;
}

void update_used_weight_1m_from_headers(
    std::string_view response,
    std::size_t headers_begin,
    std::size_t headers_end,
    std::atomic<std::int32_t>* out) {
    if (out == nullptr || headers_end <= headers_begin || headers_begin > response.size()) {
        return;
    }
    const std::size_t n = std::min(headers_end, response.size()) - headers_begin;
    std::string_view block(response.data() + headers_begin, n);
    constexpr std::string_view k = "x-mbx-used-weight-1m:";
    for (std::size_t i = 0; i < block.size();) {
        std::size_t nl = block.find('\n', i);
        if (nl == std::string_view::npos) {
            nl = block.size();
        }
        std::string_view line(block.data() + i, nl - i);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }
        if (header_name_ieq(line, k)) {
            std::size_t colon = line.find(':');
            if (colon != std::string_view::npos) {
                std::string_view val = line.substr(colon + 1);
                trim_ascii(val);
                std::int32_t w {};
                const auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), w);
                if (ec == std::errc() && ptr == val.data() + val.size()) {
                    out->store(w, std::memory_order_relaxed);
                }
            }
            return;
        }
        i = nl + 1;
    }
}

SSL_CTX* shared_ssl_ctx() {
    static std::once_flag init_once;
    static SSL_CTX* ctx = nullptr;
    std::call_once(init_once, [] {
        SSL_library_init();
        SSL_load_error_strings();
        ctx = SSL_CTX_new(TLS_client_method());
        if (ctx != nullptr) {
            SSL_CTX_set_default_verify_paths(ctx);
        }
    });
    return ctx;
}

} // namespace

BinanceGateway::BinanceGateway(GatewayConfig config) : config_(std::move(config)) {
    if (const char* rw = std::getenv("HFT_BINANCE_RECV_WINDOW_MS"); rw != nullptr && rw[0] != '\0') {
        const int parsed = std::atoi(rw);
        if (parsed > 0) {
            recv_window_ms_ = static_cast<std::uint32_t>(parsed);
        }
    }
    constraints_[0] = fallback_constraints(marketdata::Instrument::BtcUsdt);
    constraints_[1] = fallback_constraints(marketdata::Instrument::EthUsdt);
    constraints_[2] = fallback_constraints(marketdata::Instrument::SolUsdt);
    load_exchange_constraints();
}

GatewaySendResult BinanceGateway::send(const ordermgmt::OrderCommand& cmd) {
    GatewaySendResult last {};
    const std::string endpoint = "/fapi/v1/order";

    std::string method = "POST";
    if (cmd.type == ordermgmt::CommandType::Replace) {
        method = "PUT";
    } else if (cmd.type == ordermgmt::CommandType::Cancel) {
        method = "DELETE";
    }

    const std::uint32_t attempts = config_.retry_max_attempts > 0 ? config_.retry_max_attempts : 1;
    for (std::uint32_t attempt = 0; attempt < attempts; ++attempt) {
        const std::string query = build_query(cmd);
        if (query.empty()) {
            return last;
        }
        if (config_.api_secret.empty()) {
            return last;
        }
        const std::string signature = sign_query(query);
        if (signature.empty()) {
            return last;
        }
        const std::string signed_query = query + "&signature=" + signature;
        last = send_https_signed(method, endpoint, signed_query);
        if (!last.ok && cmd.type == ordermgmt::CommandType::Cancel && last.binance_error_code == -2011) {
            // Cancel of already-closed/missing order is idempotent for local state progression.
            last.ok = true;
            last.http_status = 200;
            last.binance_error_code = 0;
            return last;
        }
        if (!last.ok && cmd.type == ordermgmt::CommandType::New &&
            (last.binance_error_code == -4115 || last.binance_error_code == -4116)) {
            // Duplicate/invalid client order id can happen when transport retry replays
            // a request that was already accepted upstream.
            last.ok = true;
            last.http_status = 200;
            last.binance_error_code = 0;
            return last;
        }
        if (last.ok) {
            return last;
        }
        if (last.binance_error_code == -1021 || last.binance_error_code == -5028) {
            sync_server_time_offset();
            continue;
        }
        if (attempt + 1 < attempts && config_.retry_backoff_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.retry_backoff_ms));
        }
    }
    return last;
}

std::string BinanceGateway::build_query(const ordermgmt::OrderCommand& cmd) const {
    const char* symbol = cmd_symbol(cmd.instrument);
    if (symbol[0] == '\0') {
        return {};
    }

    const std::uint64_t ts_ms = static_cast<std::uint64_t>(current_epoch_ms());
    const SymbolConstraints rules = symbol_constraints(cmd.instrument);
    double px = cmd.price;
    double qty = cmd.qty;
    normalize_order_price_qty(rules, cmd.price, cmd.qty, px, qty);
    std::ostringstream oss;

    if (cmd.type == ordermgmt::CommandType::Cancel) {
        oss << "symbol=" << symbol << "&origClientOrderId=hft_" << cmd.client_order_id << "&recvWindow=" << recv_window_ms_
            << "&timestamp=" << ts_ms;
        return oss.str();
    }
    if (cmd.type == ordermgmt::CommandType::Replace) {
        oss << "symbol=" << symbol << "&side=" << side_str(cmd.side) << "&type=LIMIT"
            << "&timeInForce=GTC"
            << "&quantity=" << std::fixed << std::setprecision(rules.qty_dp) << qty
            << "&price=" << std::fixed << std::setprecision(rules.price_dp) << px << "&origClientOrderId=hft_"
            << cmd.client_order_id << "&recvWindow=" << recv_window_ms_
            << "&timestamp=" << ts_ms;
        return oss.str();
    }

    oss << "symbol=" << symbol << "&side=" << side_str(cmd.side) << "&type=LIMIT"
        << "&timeInForce=GTC"
        << "&quantity=" << std::fixed << std::setprecision(rules.qty_dp) << qty
        << "&price=" << std::fixed << std::setprecision(rules.price_dp) << px << "&newClientOrderId=hft_"
        << cmd.client_order_id << "&recvWindow=" << recv_window_ms_
        << "&timestamp=" << ts_ms;
    return oss.str();
}

std::string BinanceGateway::sign_query(const std::string& query) const {
    if (config_.api_secret.empty()) {
        return {};
    }

    unsigned int len = 0;
    unsigned char out[EVP_MAX_MD_SIZE] {};
    HMAC(
        EVP_sha256(),
        config_.api_secret.data(),
        static_cast<int>(config_.api_secret.size()),
        reinterpret_cast<const unsigned char*>(query.data()),
        query.size(),
        out,
        &len);

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < len; ++i) {
        oss << std::setw(2) << static_cast<int>(out[i]);
    }
    return oss.str();
}

GatewayRestResult BinanceGateway::https_request(
    const std::string& method,
    const std::string& path,
    const std::string& entity_body) const {
    GatewayRestResult result {};
    if (config_.api_key.empty() || config_.api_secret.empty()) {
        return result;
    }

    int fd = -1;
    SSL* ssl = nullptr;

    try {
        fd = tcp_connect(config_.rest_host, config_.rest_port);
        if (fd < 0) {
            throw 1;
        }
        SSL_CTX* ctx = shared_ssl_ctx();
        if (ctx == nullptr) {
            throw 1;
        }
        ssl = SSL_new(ctx);
        if (ssl == nullptr) {
            throw 1;
        }
        SSL_set_tlsext_host_name(ssl, config_.rest_host.c_str());
        SSL_set_fd(ssl, fd);
        if (SSL_connect(ssl) <= 0) {
            throw 1;
        }

        std::string req;
        if (method == "GET") {
            req =
                "GET " + path + " HTTP/1.1\r\n" +
                "Host: " + config_.rest_host + "\r\n" +
                "X-MBX-APIKEY: " + config_.api_key + "\r\n" +
                "Connection: close\r\n\r\n";
        } else {
            req =
                method + " " + path + " HTTP/1.1\r\n" +
                "Host: " + config_.rest_host + "\r\n" +
                "X-MBX-APIKEY: " + config_.api_key + "\r\n" +
                "Content-Type: application/x-www-form-urlencoded\r\n" +
                "Connection: close\r\n" +
                "Content-Length: " + std::to_string(entity_body.size()) + "\r\n\r\n" +
                entity_body;
        }

        if (SSL_write(ssl, req.data(), static_cast<int>(req.size())) <= 0) {
            throw 1;
        }

        std::string response;
        response.reserve(16384);
        while (ssl_read_all(ssl, response)) {
        }

        const std::size_t status_end = response.find("\r\n");
        if (status_end == std::string::npos) {
            throw 1;
        }
        const std::string_view status_line(response.data(), status_end);
        result.http_status = parse_http_status_line(status_line);
        result.ok = result.http_status == 200 || result.http_status == 201;

        const std::size_t header_end = response.find("\r\n\r\n");
        if (header_end != std::string::npos) {
            const std::size_t headers_begin = status_end + 2;
            if (headers_begin < header_end) {
                update_used_weight_1m_from_headers(response, headers_begin, header_end, config_.rest_weight_1m);
            }
            const std::size_t body_off = header_end + 4;
            if (body_off < response.size()) {
                result.body.assign(response.data() + body_off, response.size() - body_off);
                if (!result.ok) {
                    result.binance_error_code = parse_binance_error_code(result.body);
                }
            }
        }

        if (ssl != nullptr) {
            SSL_shutdown(ssl);
            SSL_free(ssl);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        return result;
    } catch (...) {
    }

    if (ssl != nullptr) {
        SSL_shutdown(ssl);
        SSL_free(ssl);
    }
    if (fd >= 0) {
        ::close(fd);
    }
    return result;
}

GatewaySendResult BinanceGateway::send_https_signed(
    const std::string& method,
    const std::string& endpoint,
    const std::string& signed_query) const {
    const GatewayRestResult rest = https_request(method, endpoint, signed_query);
    GatewaySendResult result {};
    result.ok = rest.ok;
    result.http_status = rest.http_status;
    result.binance_error_code = rest.binance_error_code;
    return result;
}

GatewayRestResult BinanceGateway::signed_open_orders() const {
    GatewayRestResult last {};
    for (int attempt = 0; attempt < 2; ++attempt) {
        std::string q = "recvWindow=" + std::to_string(recv_window_ms_) + "&timestamp=" + std::to_string(current_epoch_ms());
        const std::string sig = sign_query(q);
        const std::string path = "/fapi/v1/openOrders?" + q + "&signature=" + sig;
        last = https_request("GET", path, "");
        if (last.ok) {
            return last;
        }
        if ((last.binance_error_code == -1021 || last.binance_error_code == -5028) && sync_server_time_offset()) {
            continue;
        }
        return last;
    }
    return last;
}

GatewayRestResult BinanceGateway::signed_position_risk() const {
    GatewayRestResult last {};
    for (int attempt = 0; attempt < 2; ++attempt) {
        std::string q = "recvWindow=" + std::to_string(recv_window_ms_) + "&timestamp=" + std::to_string(current_epoch_ms());
        const std::string sig = sign_query(q);
        const std::string path = "/fapi/v2/positionRisk?" + q + "&signature=" + sig;
        last = https_request("GET", path, "");
        if (last.ok) {
            return last;
        }
        if ((last.binance_error_code == -1021 || last.binance_error_code == -5028) && sync_server_time_offset()) {
            continue;
        }
        return last;
    }
    return last;
}

std::int64_t BinanceGateway::current_epoch_ms() const {
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    return static_cast<std::int64_t>(now_ms) + server_time_offset_ms_.load(std::memory_order_relaxed);
}

bool BinanceGateway::sync_server_time_offset() const {
    const auto local_before_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
    const GatewayRestResult r = https_request("GET", "/fapi/v1/time", "");
    if (!r.ok || r.body.empty()) {
        return false;
    }
    const std::int64_t server_ms = parse_binance_server_time_ms(r.body);
    if (server_ms <= 0) {
        return false;
    }
    server_time_offset_ms_.store(server_ms - static_cast<std::int64_t>(local_before_ms), std::memory_order_relaxed);
    return true;
}

void BinanceGateway::load_exchange_constraints() {
    const GatewayRestResult r = https_request("GET", "/fapi/v1/exchangeInfo", "");
    if (!r.ok || r.body.empty()) {
        return;
    }
    struct SymDef {
        marketdata::Instrument inst;
        const char* symbol;
    };
    constexpr std::array<SymDef, 3> syms {{
        {marketdata::Instrument::BtcUsdt, "BTCUSDT"},
        {marketdata::Instrument::EthUsdt, "ETHUSDT"},
        {marketdata::Instrument::SolUsdt, "SOLUSDT"},
    }};
    for (const auto& s : syms) {
        const std::string needle_compact = std::string("\"symbol\":\"") + s.symbol + "\"";
        const std::string needle_spaced = std::string("\"symbol\": \"") + s.symbol + "\"";
        std::size_t pos = r.body.find(needle_compact);
        if (pos == std::string::npos) {
            pos = r.body.find(needle_spaced);
        }
        if (pos == std::string::npos) {
            continue;
        }
        const std::size_t end = r.body.find("\"symbol\":", pos + 1);
        const std::string_view block(r.body.data() + pos, (end == std::string::npos ? r.body.size() : end) - pos);
        const double tick = parse_quoted_double(block, "\"tickSize\":\"", constraints_[instrument_idx(s.inst)].tick_size);
        const double step = parse_quoted_double(block, "\"stepSize\":\"", constraints_[instrument_idx(s.inst)].qty_step);
        double min_notional = parse_quoted_double(block, "\"notional\":\"", 0.0);
        if (min_notional <= 0.0) {
            min_notional = parse_quoted_double(block, "\"minNotional\":\"", fallback_min_notional(s.inst));
        }
        SymbolConstraints c = constraints_[instrument_idx(s.inst)];
        c.tick_size = tick > 0.0 ? tick : c.tick_size;
        c.qty_step = step > 0.0 ? step : c.qty_step;
        c.min_notional = min_notional > 0.0 ? min_notional : c.min_notional;
        c.price_dp = decimals_from_step(c.tick_size, c.price_dp);
        c.qty_dp = decimals_from_step(c.qty_step, c.qty_dp);
        c.valid = true;
        constraints_[instrument_idx(s.inst)] = c;
    }
}

SymbolConstraints BinanceGateway::symbol_constraints(marketdata::Instrument instrument) const {
    return constraints_[instrument_idx(instrument)];
}

} // namespace hft::execution
