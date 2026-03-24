#pragma once

#include <atomic>
#include <functional>
#include <string>

namespace hft::marketdata {

class BinanceWsClient {
public:
    using MessageHandler = std::function<void(const std::string&, std::uint64_t)>;

    BinanceWsClient(MessageHandler handler, std::string ws_host, std::string ws_port);
    void run(std::atomic<bool>& stop);

private:
    MessageHandler handler_;
    std::string ws_host_;
    std::string ws_port_;
};

} // namespace hft::marketdata
