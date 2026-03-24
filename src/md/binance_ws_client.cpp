#include "hft/md/binance_ws_client.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <openssl/ssl.h>

#include "hft/md/binance_types.hpp"

namespace hft::md {

namespace {

std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

std::string build_combined_target() {
    std::string target = "/stream?streams=";
    for (std::size_t i = 0; i < kStreams.size(); ++i) {
        if (i > 0) {
            target += "/";
        }
        target += kStreams[i];
    }
    return target;
}

std::string base64_encode(const unsigned char* data, std::size_t len) {
    static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (std::size_t i = 0; i < len; i += 3) {
        const unsigned int octet_a = data[i];
        const unsigned int octet_b = (i + 1 < len) ? data[i + 1] : 0;
        const unsigned int octet_c = (i + 2 < len) ? data[i + 2] : 0;
        const unsigned int triple = (octet_a << 16) | (octet_b << 8) | octet_c;

        out.push_back(table[(triple >> 18) & 0x3F]);
        out.push_back(table[(triple >> 12) & 0x3F]);
        out.push_back(i + 1 < len ? table[(triple >> 6) & 0x3F] : '=');
        out.push_back(i + 2 < len ? table[triple & 0x3F] : '=');
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
            // Keep SSL_read bounded so callers can stop/restart the WS loop.
            timeval tv {};
            tv.tv_sec = 1;
            tv.tv_usec = 0;
            (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            break;
        }
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(result);
    return fd;
}

bool ssl_read_exact(SSL* ssl, void* dst, std::size_t len) {
    std::size_t off = 0;
    while (off < len) {
        const int n = SSL_read(ssl, static_cast<char*>(dst) + off, static_cast<int>(len - off));
        if (n <= 0) {
            return false;
        }
        off += static_cast<std::size_t>(n);
    }
    return true;
}

bool send_masked_frame(SSL* ssl, std::uint8_t opcode, const std::string& payload) {
    std::array<std::uint8_t, 14> hdr {};
    std::size_t hlen = 0;
    hdr[hlen++] = 0x80 | (opcode & 0x0F);

    const std::size_t len = payload.size();
    if (len <= 125) {
        hdr[hlen++] = 0x80 | static_cast<std::uint8_t>(len);
    } else if (len <= 0xFFFF) {
        hdr[hlen++] = 0x80 | 126;
        hdr[hlen++] = static_cast<std::uint8_t>((len >> 8) & 0xFF);
        hdr[hlen++] = static_cast<std::uint8_t>(len & 0xFF);
    } else {
        hdr[hlen++] = 0x80 | 127;
        for (int i = 7; i >= 0; --i) {
            hdr[hlen++] = static_cast<std::uint8_t>((len >> (i * 8)) & 0xFF);
        }
    }

    std::array<std::uint8_t, 4> mask {};
    std::random_device rd;
    for (auto& b : mask) {
        b = static_cast<std::uint8_t>(rd());
    }
    for (std::uint8_t b : mask) {
        hdr[hlen++] = b;
    }

    if (SSL_write(ssl, hdr.data(), static_cast<int>(hlen)) <= 0) {
        return false;
    }

    std::string masked = payload;
    for (std::size_t i = 0; i < masked.size(); ++i) {
        masked[i] = static_cast<char>(static_cast<std::uint8_t>(masked[i]) ^ mask[i % 4]);
    }
    return SSL_write(ssl, masked.data(), static_cast<int>(masked.size())) > 0;
}

} // namespace

BinanceWsClient::BinanceWsClient(MessageHandler handler, std::string ws_host, std::string ws_port)
    : handler_(std::move(handler)),
      ws_host_(std::move(ws_host)),
      ws_port_(std::move(ws_port)) {}

void BinanceWsClient::run(std::atomic<bool>& stop) {
    const std::string& host = ws_host_;
    const std::string& port = ws_port_;
    const std::string target = build_combined_target();

    SSL_library_init();
    SSL_load_error_strings();

    while (!stop.load(std::memory_order_relaxed)) {
        int fd = -1;
        SSL_CTX* ctx = nullptr;
        SSL* ssl = nullptr;
        try {
            fd = tcp_connect(host, port);
            if (fd < 0) {
                throw std::runtime_error("tcp connect failed");
            }

            ctx = SSL_CTX_new(TLS_client_method());
            if (ctx == nullptr) {
                throw std::runtime_error("ssl ctx create failed");
            }
            SSL_CTX_set_default_verify_paths(ctx);

            ssl = SSL_new(ctx);
            if (ssl == nullptr) {
                throw std::runtime_error("ssl create failed");
            }

            SSL_set_tlsext_host_name(ssl, host.c_str());
            SSL_set_fd(ssl, fd);
            if (SSL_connect(ssl) <= 0) {
                throw std::runtime_error("ssl connect failed");
            }

            unsigned char nonce[16];
            std::random_device rd;
            for (auto& b : nonce) {
                b = static_cast<unsigned char>(rd());
            }
            const std::string ws_key = base64_encode(nonce, sizeof(nonce));

            const std::string request =
                "GET " + target + " HTTP/1.1\r\n"
                "Host: " + host + "\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                "Sec-WebSocket-Key: " + ws_key + "\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "\r\n";
            if (SSL_write(ssl, request.data(), static_cast<int>(request.size())) <= 0) {
                throw std::runtime_error("handshake write failed");
            }

            std::string response;
            response.reserve(2048);
            char c {};
            while (response.find("\r\n\r\n") == std::string::npos) {
                if (!ssl_read_exact(ssl, &c, 1)) {
                    throw std::runtime_error("handshake read failed");
                }
                response.push_back(c);
                if (response.size() > 64 * 1024) {
                    throw std::runtime_error("invalid handshake response");
                }
            }
            if (response.find(" 101 ") == std::string::npos) {
                throw std::runtime_error("websocket upgrade failed");
            }

            while (!stop.load(std::memory_order_relaxed)) {
                std::uint8_t hdr2[2];
                if (!ssl_read_exact(ssl, hdr2, sizeof(hdr2))) {
                    break;
                }
                const std::uint8_t opcode = hdr2[0] & 0x0F;
                const bool masked = (hdr2[1] & 0x80) != 0;
                std::uint64_t payload_len = hdr2[1] & 0x7F;

                if (payload_len == 126) {
                    std::uint8_t ext[2];
                    if (!ssl_read_exact(ssl, ext, sizeof(ext))) {
                        break;
                    }
                    payload_len = (static_cast<std::uint64_t>(ext[0]) << 8) | ext[1];
                } else if (payload_len == 127) {
                    std::uint8_t ext[8];
                    if (!ssl_read_exact(ssl, ext, sizeof(ext))) {
                        break;
                    }
                    payload_len = 0;
                    for (int i = 0; i < 8; ++i) {
                        payload_len = (payload_len << 8) | ext[i];
                    }
                }

                std::uint8_t mask[4] {};
                if (masked) {
                    if (!ssl_read_exact(ssl, mask, sizeof(mask))) {
                        break;
                    }
                }

                std::string payload(payload_len, '\0');
                if (payload_len > 0 && !ssl_read_exact(ssl, payload.data(), payload_len)) {
                    break;
                }
                if (masked) {
                    for (std::uint64_t i = 0; i < payload_len; ++i) {
                        payload[static_cast<std::size_t>(i)] ^= static_cast<char>(mask[i % 4]);
                    }
                }

                if (opcode == 0x8) {
                    break;
                }
                if (opcode == 0x9) {
                    if (!send_masked_frame(ssl, 0xA, payload)) {
                        break;
                    }
                    continue;
                }
                if (opcode != 0x1) {
                    continue;
                }
                handler_(payload, now_ns());
            }
        } catch (const std::exception&) {
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

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

} // namespace hft::md
