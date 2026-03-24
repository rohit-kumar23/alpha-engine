#pragma once

#include <optional>
#include <string_view>

#include "hft/md/binance_types.hpp"

namespace hft::md {

class BinanceParser {
public:
    std::optional<MdEvent> parse_combined_message(std::string_view json, std::uint64_t ts_recv_ns) const;
};

} // namespace hft::md
