#pragma once

#include <atomic>
#include <functional>
#include <string>

namespace hft::execution {

class BinanceUserStream {
public:
    BinanceUserStream(
        std::string api_key,
        std::string rest_host,
        std::string rest_port,
        std::string stream_ws_host,
        std::string stream_ws_port);
    std::string create_listen_key() const;
    bool keepalive_listen_key(const std::string& listen_key) const;
    void run_ws(const std::string& listen_key, std::atomic<bool>& stop, const std::function<void(const std::string&)>& on_msg) const;

private:
    std::string api_key_;
    std::string rest_host_;
    std::string rest_port_;
    std::string stream_ws_host_;
    std::string stream_ws_port_;
};

} // namespace hft::execution
