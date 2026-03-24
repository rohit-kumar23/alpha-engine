#include "hft/marketdata/binance_snapshot_client.hpp"

#include <array>
#include <charconv>
#include <optional>
#include <string>

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/ssl.h>

namespace hft::marketdata {

namespace {

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

bool ssl_read_some(SSL* ssl, std::string& out) {
    char buf[8192];
    const int n = SSL_read(ssl, buf, sizeof(buf));
    if (n <= 0) {
        return false;
    }
    out.append(buf, static_cast<std::size_t>(n));
    return true;
}

std::optional<std::uint64_t> json_uint64_value(std::string_view json, std::string_view key) {
    const std::string needle = "\"" + std::string(key) + "\":";
    const auto pos = json.find(needle);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    std::size_t start = pos + needle.size();
    std::size_t end = start;
    while (end < json.size() && json[end] >= '0' && json[end] <= '9') {
        ++end;
    }
    std::uint64_t out {};
    const auto [ptr, ec] = std::from_chars(json.data() + start, json.data() + end, out);
    if (ec != std::errc() || ptr != json.data() + end) {
        return std::nullopt;
    }
    return out;
}

std::size_t parse_levels(
    std::string_view json,
    std::string_view key,
    std::array<double, kDepthLevelsPerEvent>& px_out,
    std::array<double, kDepthLevelsPerEvent>& qty_out) {
    const std::string needle = "\"" + std::string(key) + "\":[";
    const auto key_pos = json.find(needle);
    if (key_pos == std::string_view::npos) {
        return 0;
    }
    std::size_t pos = key_pos + needle.size();
    std::size_t count = 0;
    while (count < kDepthLevelsPerEvent) {
        const auto px_q1 = json.find('"', pos);
        if (px_q1 == std::string_view::npos) {
            break;
        }
        const auto px_q2 = json.find('"', px_q1 + 1);
        if (px_q2 == std::string_view::npos) {
            break;
        }
        double px {};
        const auto [px_ptr, px_ec] = std::from_chars(json.data() + px_q1 + 1, json.data() + px_q2, px);
        if (px_ec != std::errc() || px_ptr != json.data() + px_q2) {
            break;
        }

        const auto qty_q1 = json.find('"', px_q2 + 1);
        if (qty_q1 == std::string_view::npos) {
            break;
        }
        const auto qty_q2 = json.find('"', qty_q1 + 1);
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

        const auto close = json.find(']', qty_q2 + 1);
        if (close == std::string_view::npos) {
            break;
        }
        std::size_t probe = close + 1;
        while (probe < json.size() && (json[probe] == ' ' || json[probe] == '\t' || json[probe] == '\n' ||
                                       json[probe] == '\r' || json[probe] == ',')) {
            ++probe;
        }
        if (probe >= json.size() || json[probe] == ']') {
            break;
        }
        if (json[probe] != '[') {
            break;
        }
        pos = probe;
    }
    return count;
}

} // namespace

BinanceSnapshotClient::BinanceSnapshotClient(std::string rest_host, std::string rest_port)
    : rest_host_(std::move(rest_host)),
      rest_port_(std::move(rest_port)) {}

std::optional<DepthSnapshot> BinanceSnapshotClient::fetch_depth_snapshot(Instrument instrument) const {
    const std::string symbol(instrument_to_symbol(instrument));
    if (symbol.empty()) {
        return std::nullopt;
    }

    SSL_library_init();
    SSL_load_error_strings();

    int fd = -1;
    SSL_CTX* ctx = nullptr;
    SSL* ssl = nullptr;

    try {
        fd = tcp_connect(rest_host_, rest_port_);
        if (fd < 0) {
            throw 1;
        }
        ctx = SSL_CTX_new(TLS_client_method());
        if (ctx == nullptr) {
            throw 1;
        }
        SSL_CTX_set_default_verify_paths(ctx);
        ssl = SSL_new(ctx);
        if (ssl == nullptr) {
            throw 1;
        }
        SSL_set_tlsext_host_name(ssl, rest_host_.c_str());
        SSL_set_fd(ssl, fd);
        if (SSL_connect(ssl) <= 0) {
            throw 1;
        }

        const std::string target = "/fapi/v1/depth?symbol=" + symbol + "&limit=20";
        const std::string req =
            "GET " + target + " HTTP/1.1\r\n"
            "Host: " + rest_host_ + "\r\n"
            "Connection: close\r\n"
            "\r\n";

        if (SSL_write(ssl, req.data(), static_cast<int>(req.size())) <= 0) {
            throw 1;
        }

        std::string response;
        response.reserve(64 * 1024);
        while (ssl_read_some(ssl, response)) {
        }

        const auto header_end = response.find("\r\n\r\n");
        if (header_end == std::string::npos) {
            throw 1;
        }
        const std::string_view body(response.data() + header_end + 4, response.size() - (header_end + 4));

        DepthSnapshot snap;
        snap.instrument = instrument;
        snap.last_update_id = json_uint64_value(body, "lastUpdateId").value_or(0);
        snap.bid_levels_count = static_cast<std::uint8_t>(
            parse_levels(body, "bids", snap.bid_px_levels, snap.bid_qty_levels));
        snap.ask_levels_count = static_cast<std::uint8_t>(
            parse_levels(body, "asks", snap.ask_px_levels, snap.ask_qty_levels));
        if (snap.last_update_id == 0 || snap.bid_levels_count == 0 || snap.ask_levels_count == 0) {
            throw 1;
        }

        if (ssl != nullptr) {
            SSL_shutdown(ssl);
            SSL_free(ssl);
        }
        if (ctx != nullptr) {
            SSL_CTX_free(ctx);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        return snap;
    } catch (...) {
    }

    if (ssl != nullptr) {
        SSL_shutdown(ssl);
        SSL_free(ssl);
    }
    if (ctx != nullptr) {
        SSL_CTX_free(ctx);
    }
    if (fd >= 0) {
        ::close(fd);
    }
    return std::nullopt;
}

} // namespace hft::marketdata
