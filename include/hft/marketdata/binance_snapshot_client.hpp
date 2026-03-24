#pragma once

#include <optional>
#include <string>

#include "hft/marketdata/binance_types.hpp"

namespace hft::marketdata {

class BinanceSnapshotClient {
public:
    explicit BinanceSnapshotClient(std::string rest_host, std::string rest_port);

    std::optional<DepthSnapshot> fetch_depth_snapshot(Instrument instrument) const;

private:
    std::string rest_host_;
    std::string rest_port_;
};

} // namespace hft::marketdata
