#include "hft/execution/binance_user_stream.hpp"

#include <chrono>
#include <string>
#include <thread>

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/ssl.h>

namespace hft::execution {

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

std::string http_call(
    const std::string& rest_host,
    const std::string& rest_port,
    const std::string& method,
    const std::string& path,
    const std::string& api_key) {
    SSL_library_init();
    SSL_load_error_strings();
    int fd = -1;
    SSL_CTX* ctx = nullptr;
    SSL* ssl = nullptr;
    try {
        fd = tcp_connect(rest_host, rest_port);
        if (fd < 0) throw 1;
        ctx = SSL_CTX_new(TLS_client_method());
        if (!ctx) throw 1;
        SSL_CTX_set_default_verify_paths(ctx);
        ssl = SSL_new(ctx);
        if (!ssl) throw 1;
        SSL_set_tlsext_host_name(ssl, rest_host.c_str());
        SSL_set_fd(ssl, fd);
        if (SSL_connect(ssl) <= 0) throw 1;
        const std::string req =
            method + " " + path + " HTTP/1.1\r\n"
            "Host: " + rest_host + "\r\n"
            "X-MBX-APIKEY: " + api_key + "\r\n"
            "Connection: close\r\n"
            "Content-Length: 0\r\n\r\n";
        if (SSL_write(ssl, req.data(), static_cast<int>(req.size())) <= 0) throw 1;
        std::string resp;
        while (ssl_read_some(ssl, resp)) {}
        auto body_pos = resp.find("\r\n\r\n");
        if (body_pos == std::string::npos) throw 1;
        std::string body = resp.substr(body_pos + 4);
        SSL_shutdown(ssl); SSL_free(ssl); SSL_CTX_free(ctx); ::close(fd);
        return body;
    } catch (...) {
        if (ssl) { SSL_shutdown(ssl); SSL_free(ssl); }
        if (ctx) SSL_CTX_free(ctx);
        if (fd >= 0) ::close(fd);
    }
    return {};
}

std::string extract_json_string(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\":\"";
    const auto pos = json.find(needle);
    if (pos == std::string::npos) return {};
    const auto start = pos + needle.size();
    const auto end = json.find('"', start);
    if (end == std::string::npos) return {};
    return json.substr(start, end - start);
}

bool ssl_read_exact(SSL* ssl, void* dst, std::size_t len) {
    std::size_t off = 0;
    while (off < len) {
        const int n = SSL_read(ssl, static_cast<char*>(dst) + off, static_cast<int>(len - off));
        if (n <= 0) return false;
        off += static_cast<std::size_t>(n);
    }
    return true;
}

} // namespace

BinanceUserStream::BinanceUserStream(
    std::string api_key,
    std::string rest_host,
    std::string rest_port,
    std::string stream_ws_host,
    std::string stream_ws_port)
    : api_key_(std::move(api_key)),
      rest_host_(std::move(rest_host)),
      rest_port_(std::move(rest_port)),
      stream_ws_host_(std::move(stream_ws_host)),
      stream_ws_port_(std::move(stream_ws_port)) {}

std::string BinanceUserStream::create_listen_key() const {
    const std::string body = http_call(rest_host_, rest_port_, "POST", "/fapi/v1/listenKey", api_key_);
    return extract_json_string(body, "listenKey");
}

bool BinanceUserStream::keepalive_listen_key(const std::string& listen_key) const {
    const std::string body =
        http_call(rest_host_, rest_port_, "PUT", "/fapi/v1/listenKey?listenKey=" + listen_key, api_key_);
    return !body.empty();
}

void BinanceUserStream::run_ws(
    const std::string& listen_key,
    std::atomic<bool>& stop,
    const std::function<void(const std::string&)>& on_msg) const {
    if (listen_key.empty()) return;
    SSL_library_init();
    SSL_load_error_strings();
    int fd = -1;
    SSL_CTX* ctx = nullptr;
    SSL* ssl = nullptr;
    try {
        fd = tcp_connect(stream_ws_host_, stream_ws_port_);
        if (fd < 0) throw 1;
        ctx = SSL_CTX_new(TLS_client_method());
        if (!ctx) throw 1;
        SSL_CTX_set_default_verify_paths(ctx);
        ssl = SSL_new(ctx);
        if (!ssl) throw 1;
        SSL_set_tlsext_host_name(ssl, stream_ws_host_.c_str());
        SSL_set_fd(ssl, fd);
        if (SSL_connect(ssl) <= 0) throw 1;

        const std::string req =
            "GET /ws/" + listen_key + " HTTP/1.1\r\n"
            "Host: " + stream_ws_host_ + "\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: dGVzdF9saXN0ZW5LZXk=\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n";
        if (SSL_write(ssl, req.data(), static_cast<int>(req.size())) <= 0) throw 1;

        std::string resp;
        char c {};
        while (resp.find("\r\n\r\n") == std::string::npos) {
            if (!ssl_read_exact(ssl, &c, 1)) throw 1;
            resp.push_back(c);
            if (resp.size() > 65536) throw 1;
        }
        if (resp.find(" 101 ") == std::string::npos) throw 1;

        while (!stop.load(std::memory_order_relaxed)) {
            std::uint8_t hdr2[2];
            if (!ssl_read_exact(ssl, hdr2, sizeof(hdr2))) break;
            const std::uint8_t opcode = hdr2[0] & 0x0F;
            std::uint64_t payload_len = hdr2[1] & 0x7F;
            if (payload_len == 126) {
                std::uint8_t ext[2];
                if (!ssl_read_exact(ssl, ext, sizeof(ext))) break;
                payload_len = (static_cast<std::uint64_t>(ext[0]) << 8) | ext[1];
            } else if (payload_len == 127) {
                std::uint8_t ext[8];
                if (!ssl_read_exact(ssl, ext, sizeof(ext))) break;
                payload_len = 0;
                for (int i = 0; i < 8; ++i) payload_len = (payload_len << 8) | ext[i];
            }
            std::string payload(payload_len, '\0');
            if (payload_len > 0 && !ssl_read_exact(ssl, payload.data(), payload_len)) break;
            if (opcode == 0x1) on_msg(payload);
            if (opcode == 0x8) break;
        }
    } catch (...) {}

    if (ssl) { SSL_shutdown(ssl); SSL_free(ssl); }
    if (ctx) SSL_CTX_free(ctx);
    if (fd >= 0) ::close(fd);
}

} // namespace hft::execution
