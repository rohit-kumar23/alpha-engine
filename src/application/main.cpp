#include <atomic>
#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <cctype>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <cmath>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "hft/orderbook/l2_book.hpp"
#include "hft/execution/binance_gateway.hpp"
#include "hft/execution/binance_user_stream.hpp"
#include "hft/ordermgmt/order_manager.hpp"
#include "hft/execution/user_stream_parser.hpp"
#include "hft/coreinfra/exec_audit_log.hpp"
#include "hft/coreinfra/spsc_ring.hpp"
#include "hft/marketdata/binance_endpoints.hpp"
#include "hft/marketdata/binance_parser.hpp"
#include "hft/marketdata/binance_snapshot_client.hpp"
#include "hft/marketdata/binance_types.hpp"
#include "hft/marketdata/binance_ws_client.hpp"
#include "hft/ordermgmt/order_state.hpp"
#include "hft/analytics/pnl_engine.hpp"
#include "hft/analytics/position_seed.hpp"
#include "hft/riskmgmt/pre_trade_risk.hpp"
#include "hft/strategy/market_maker.hpp"

namespace {

std::atomic<bool> g_stop {false};
std::atomic<bool> g_kill_switch {false};
std::atomic<std::uint64_t> g_reconcile_seq {0};
std::atomic<std::uint64_t> g_reconcile_remote_open {0};
std::atomic<std::uint64_t> g_reconcile_http_fail {0};
constexpr std::size_t kReconcileMaxIds = 128;
std::array<std::atomic<std::uint64_t>, kReconcileMaxIds> g_reconcile_remote_ids {};
std::atomic<std::uint32_t> g_reconcile_remote_ids_count {0};
std::atomic<std::int32_t> g_rest_weight_1m {0};

void handle_signal(int) {
    g_stop.store(true, std::memory_order_relaxed);
}

void handle_sigusr1(int) {
    const bool cur = g_kill_switch.load(std::memory_order_relaxed);
    g_kill_switch.store(!cur, std::memory_order_relaxed);
}

std::size_t count_json_client_order_ids(std::string_view body) {
    constexpr std::string_view key = "\"clientOrderId\"";
    std::size_t n = 0;
    std::size_t pos = 0;
    while ((pos = body.find(key, pos)) != std::string_view::npos) {
        ++n;
        pos += key.size();
    }
    return n;
}

std::size_t extract_json_client_order_ids(
    std::string_view body,
    std::array<std::uint64_t, kReconcileMaxIds>& out) {
    constexpr std::string_view key = "\"clientOrderId\"";
    constexpr std::string_view prefix = "hft_";
    std::size_t n = 0;
    std::size_t pos = 0;
    while (n < out.size() && (pos = body.find(key, pos)) != std::string_view::npos) {
        pos += key.size();
        while (pos < body.size() && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == ':')) {
            ++pos;
        }
        if (pos >= body.size() || body[pos] != '"') {
            continue;
        }
        ++pos;
        const std::size_t value_start = pos;
        const std::size_t value_end = body.find('"', value_start);
        if (value_end == std::string_view::npos) {
            break;
        }
        std::string_view v = body.substr(value_start, value_end - value_start);
        if (v.size() > prefix.size() && v.substr(0, prefix.size()) == prefix) {
            v.remove_prefix(prefix.size());
            std::uint64_t id = 0;
            bool ok = !v.empty();
            for (char c : v) {
                if (c < '0' || c > '9') {
                    ok = false;
                    break;
                }
                id = id * 10ULL + static_cast<std::uint64_t>(c - '0');
            }
            if (ok && id > 0) {
                out[n++] = id;
            }
        }
        pos = value_end + 1;
    }
    return n;
}

std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

bool pin_current_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
}

std::string trim_copy(const std::string& s) {
    std::size_t start = 0;
    while (start < s.size() && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n')) {
        ++start;
    }
    std::size_t end = s.size();
    while (end > start &&
           (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\r' || s[end - 1] == '\n')) {
        --end;
    }
    return s.substr(start, end - start);
}

void load_env_file_if_present(const char* path) {
    std::ifstream in(path);
    if (!in.good()) {
        return;
    }
    std::string line;
    while (std::getline(in, line)) {
        line = trim_copy(line);
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const std::size_t eq = line.find('=');
        if (eq == std::string::npos || eq == 0) {
            continue;
        }
        std::string key = trim_copy(line.substr(0, eq));
        std::string value = trim_copy(line.substr(eq + 1));
        if (key.empty()) {
            continue;
        }
        if (std::getenv(key.c_str()) == nullptr) {
            setenv(key.c_str(), value.c_str(), 0);
        }
    }
}

int read_env_core_or_default(const char* name, int fallback) {
    const char* v = std::getenv(name);
    if (v == nullptr || v[0] == '\0') {
        return fallback;
    }
    char* end = nullptr;
    const long parsed = std::strtol(v, &end, 10);
    if (end == v || *end != '\0') {
        return fallback;
    }
    if (parsed < 0 || parsed > std::numeric_limits<int>::max()) {
        return fallback;
    }
    return static_cast<int>(parsed);
}

std::string env_value_lowered(const char* v) {
    if (v == nullptr || v[0] == '\0') {
        return {};
    }
    std::string s = trim_copy(v);
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

hft::marketdata::BinanceExecutionMode read_binance_execution_mode() {
    // Single getenv at startup; mode and REST/WS hosts are fixed for the process (no hot-path env access).
    const char* raw = std::getenv("BINANCE_MODE");
    if (raw == nullptr || raw[0] == '\0') {
        return hft::marketdata::BinanceExecutionMode::Demo;
    }
    const std::string m = env_value_lowered(raw);
    if (m == "live" || m == "mainnet" || m == "production" || m == "prod") {
        return hft::marketdata::BinanceExecutionMode::Live;
    }
    if (m == "demo" || m == "testnet" || m == "sandbox") {
        return hft::marketdata::BinanceExecutionMode::Demo;
    }
    std::cerr << "fatal: BINANCE_MODE must be demo or live (got: " << raw << ")\n";
    std::exit(1);
}

const char* binance_mode_label(hft::marketdata::BinanceExecutionMode mode) {
    return mode == hft::marketdata::BinanceExecutionMode::Live ? "live" : "demo";
}

// Typical Linux: /tmp is often tmpfs — faster, lower jitter than HDD for the audit writer thread.
inline constexpr const char* kExecAuditDefaultPath = "/tmp/alpha_exec_audit.log";
inline constexpr const char* kMdHealthDefaultPath = "/tmp/alpha_md_health.log";
inline constexpr const char* kPairAuditDefaultPath = "/tmp/alpha_pair_pnl.log";

const char* resolve_exec_audit_path(const char* raw) {
    if (raw == nullptr || raw[0] == '\0') {
        return nullptr;
    }
    const std::string m = env_value_lowered(raw);
    if (m == "0" || m == "off" || m == "no" || m == "false") {
        return nullptr;
    }
    if (m == "1" || m == "yes" || m == "on" || m == "true") {
        return kExecAuditDefaultPath;
    }
    return raw;
}

const char* resolve_md_health_path(const char* raw) {
    if (raw == nullptr || raw[0] == '\0') {
        return nullptr;
    }
    const std::string m = env_value_lowered(raw);
    if (m == "0" || m == "off" || m == "no" || m == "false") {
        return nullptr;
    }
    if (m == "1" || m == "yes" || m == "on" || m == "true") {
        return kMdHealthDefaultPath;
    }
    return raw;
}

const char* resolve_pair_audit_path(const char* raw) {
    if (raw == nullptr || raw[0] == '\0') {
        return nullptr;
    }
    const std::string m = env_value_lowered(raw);
    if (m == "0" || m == "off" || m == "no" || m == "false") {
        return nullptr;
    }
    if (m == "1" || m == "yes" || m == "on" || m == "true") {
        return kPairAuditDefaultPath;
    }
    return raw;
}

bool exec_audit_line(
    bool enabled,
    hft::coreinfra::ExecAuditLog& log,
    std::uint64_t& drop_count,
    const char* fmt,
    ...) {
    if (!enabled) {
        return false;
    }
    char buf[sizeof(hft::coreinfra::ExecAuditRecord::data)];
    std::va_list ap;
    va_start(ap, fmt);
    const int n = std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (n <= 0 || n >= static_cast<int>(sizeof(buf))) {
        return false;
    }
    if (!log.try_push(buf, static_cast<std::uint16_t>(n))) {
        ++drop_count;
        return false;
    }
    return true;
}

int read_env_int_or_default(const char* name, int fallback) {
    const char* v = std::getenv(name);
    if (v == nullptr || v[0] == '\0') {
        return fallback;
    }
    char* end = nullptr;
    const long parsed = std::strtol(v, &end, 10);
    if (end == v || *end != '\0') {
        return fallback;
    }
    if (parsed < std::numeric_limits<int>::min() || parsed > std::numeric_limits<int>::max()) {
        return fallback;
    }
    return static_cast<int>(parsed);
}

bool read_env_flag(const char* name, bool fallback = false) {
    const char* v = std::getenv(name);
    if (v == nullptr || v[0] == '\0') {
        return fallback;
    }
    const std::string s = env_value_lowered(v);
    if (s == "1" || s == "true" || s == "yes" || s == "on") {
        return true;
    }
    if (s == "0" || s == "false" || s == "no" || s == "off") {
        return false;
    }
    return fallback;
}

double read_env_double_or_default(const char* name, double fallback) {
    const char* v = std::getenv(name);
    if (v == nullptr || v[0] == '\0') {
        return fallback;
    }
    char* end = nullptr;
    const double parsed = std::strtod(v, &end);
    if (end == v || *end != '\0') {
        return fallback;
    }
    return parsed;
}

bool set_realtime_fifo_priority(int priority) {
    sched_param param {};
    param.sched_priority = priority;
    return pthread_setschedparam(pthread_self(), SCHED_FIFO, &param) == 0;
}

bool enable_memory_locking() {
    return mlockall(MCL_CURRENT | MCL_FUTURE) == 0;
}

void prefault_memory_mb(int mb) {
    if (mb <= 0) {
        return;
    }
    static std::vector<char> prefault;
    const std::size_t bytes = static_cast<std::size_t>(mb) * 1024ULL * 1024ULL;
    prefault.resize(bytes);
    constexpr std::size_t page = 4096;
    for (std::size_t i = 0; i < prefault.size(); i += page) {
        prefault[i] = static_cast<char>(i & 0xFF);
    }
}

const char* instrument_name(hft::marketdata::Instrument v) {
    using hft::marketdata::Instrument;
    switch (v) {
        case Instrument::BtcUsdt:
            return "BTCUSDT";
        case Instrument::EthUsdt:
            return "ETHUSDT";
        case Instrument::SolUsdt:
            return "SOLUSDT";
        default:
            return "UNKNOWN";
    }
}

std::size_t instrument_index(hft::marketdata::Instrument v) {
    using hft::marketdata::Instrument;
    switch (v) {
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

struct SnapshotRequest {
    hft::marketdata::Instrument instrument {hft::marketdata::Instrument::Unknown};
};

struct SnapshotResult {
    hft::marketdata::Instrument instrument {hft::marketdata::Instrument::Unknown};
    bool success {false};
    hft::marketdata::DepthSnapshot snapshot {};
};

struct ExecReportMsg {
    hft::execution::ExecReport report {};
    std::uint64_t ts_ns {};
};

struct RemoteOpenOrderRef {
    hft::marketdata::Instrument instrument {hft::marketdata::Instrument::Unknown};
    std::uint64_t client_order_id {};
};

hft::marketdata::Instrument instrument_from_symbol_view(std::string_view symbol) {
    using hft::marketdata::Instrument;
    if (symbol == "BTCUSDT") {
        return Instrument::BtcUsdt;
    }
    if (symbol == "ETHUSDT") {
        return Instrument::EthUsdt;
    }
    if (symbol == "SOLUSDT") {
        return Instrument::SolUsdt;
    }
    return Instrument::Unknown;
}

std::size_t extract_json_open_order_refs(
    std::string_view body,
    std::array<RemoteOpenOrderRef, kReconcileMaxIds>& out) {
    constexpr std::string_view key_client_order_id = "\"clientOrderId\"";
    constexpr std::string_view key_symbol = "\"symbol\"";
    constexpr std::string_view prefix = "hft_";
    std::size_t n = 0;
    std::size_t pos = 0;
    while (n < out.size() &&
           (pos = body.find(key_client_order_id, pos)) != std::string_view::npos) {
        const std::size_t object_start = body.rfind('{', pos);
        if (object_start == std::string_view::npos) {
            pos += key_client_order_id.size();
            continue;
        }
        pos += key_client_order_id.size();
        while (pos < body.size() && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == ':')) {
            ++pos;
        }
        if (pos >= body.size() || body[pos] != '"') {
            continue;
        }
        ++pos;
        const std::size_t value_start = pos;
        const std::size_t value_end = body.find('"', value_start);
        if (value_end == std::string_view::npos) {
            break;
        }
        std::string_view coid = body.substr(value_start, value_end - value_start);
        pos = value_end + 1;
        if (coid.size() <= prefix.size() || coid.substr(0, prefix.size()) != prefix) {
            continue;
        }
        coid.remove_prefix(prefix.size());
        std::uint64_t id = 0;
        bool id_ok = !coid.empty();
        for (char c : coid) {
            if (c < '0' || c > '9') {
                id_ok = false;
                break;
            }
            id = id * 10ULL + static_cast<std::uint64_t>(c - '0');
        }
        if (!id_ok || id == 0) {
            continue;
        }

        const std::size_t object_end = body.find('}', value_end);
        if (object_end == std::string_view::npos || object_end <= object_start) {
            continue;
        }
        std::string_view object = body.substr(object_start, object_end - object_start + 1);
        const std::size_t symbol_key_pos = object.find(key_symbol);
        if (symbol_key_pos == std::string_view::npos) {
            continue;
        }
        std::size_t symbol_pos = symbol_key_pos + key_symbol.size();
        while (symbol_pos < object.size() &&
               (object[symbol_pos] == ' ' || object[symbol_pos] == '\t' || object[symbol_pos] == ':')) {
            ++symbol_pos;
        }
        if (symbol_pos >= object.size() || object[symbol_pos] != '"') {
            continue;
        }
        ++symbol_pos;
        const std::size_t symbol_start = symbol_pos;
        const std::size_t symbol_end = object.find('"', symbol_start);
        if (symbol_end == std::string_view::npos) {
            continue;
        }
        const hft::marketdata::Instrument instrument =
            instrument_from_symbol_view(object.substr(symbol_start, symbol_end - symbol_start));
        if (instrument == hft::marketdata::Instrument::Unknown) {
            continue;
        }
        out[n++] = RemoteOpenOrderRef {instrument, id};
    }
    return n;
}

template <std::size_t Capacity>
struct DepthEventBuffer {
    std::array<hft::marketdata::MdEvent, Capacity> items {};
    std::size_t count {0};
    std::size_t drops {0};

    void push(const hft::marketdata::MdEvent& event) {
        if (count < Capacity) {
            items[count++] = event;
            return;
        }
        for (std::size_t i = 1; i < Capacity; ++i) {
            items[i - 1] = items[i];
        }
        items[Capacity - 1] = event;
        ++drops;
    }

    template <typename Fn>
    void replay(Fn&& fn) {
        for (std::size_t i = 0; i < count; ++i) {
            fn(items[i]);
        }
        count = 0;
    }

    void clear() {
        count = 0;
    }
};

template <std::size_t Capacity>
struct LatencyWindow {
    std::array<std::uint64_t, Capacity> samples {};
    std::size_t count {0};
    std::size_t cursor {0};

    void add(std::uint64_t v) {
        samples[cursor] = v;
        cursor = (cursor + 1) % Capacity;
        if (count < Capacity) {
            ++count;
        }
    }

    std::uint64_t percentile(double p) const {
        if (count == 0) {
            return 0;
        }
        std::array<std::uint64_t, Capacity> copy {};
        std::copy_n(samples.begin(), count, copy.begin());
        std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(count - 1));
        std::nth_element(copy.begin(), copy.begin() + idx, copy.begin() + count);
        return copy[idx];
    }
};

} // namespace

int main() {
    load_env_file_if_present(".env");
    if (const char* ks = std::getenv("HFT_KILL_SWITCH"); ks != nullptr && ks[0] == '1') {
        g_kill_switch.store(true, std::memory_order_relaxed);
    }

    hft::coreinfra::ExecAuditRing exec_audit_ring;
    hft::coreinfra::ExecAuditLog exec_audit;
    const char* const exec_audit_resolved = resolve_exec_audit_path(std::getenv("HFT_EXEC_AUDIT_LOG"));
    const bool exec_audit_on =
        exec_audit_resolved != nullptr && exec_audit_resolved[0] != '\0' &&
        exec_audit.start(exec_audit_resolved, &exec_audit_ring);
    if (exec_audit_resolved != nullptr && exec_audit_resolved[0] != '\0' && !exec_audit_on) {
        std::cerr << "warn=exec_audit_open_failed path=" << exec_audit_resolved << '\n';
    }
    std::uint64_t exec_audit_drops = 0;
    hft::coreinfra::ExecAuditRing md_health_ring;
    hft::coreinfra::ExecAuditLog md_health_log;
    const char* const md_health_resolved = resolve_md_health_path(std::getenv("HFT_MD_HEALTH_LOG"));
    const bool md_health_on =
        md_health_resolved != nullptr && md_health_resolved[0] != '\0' &&
        md_health_log.start(md_health_resolved, &md_health_ring);
    if (md_health_resolved != nullptr && md_health_resolved[0] != '\0' && !md_health_on) {
        std::cerr << "warn=md_health_open_failed path=" << md_health_resolved << '\n';
    }
    std::uint64_t md_health_drops = 0;
    hft::coreinfra::ExecAuditRing pair_audit_ring;
    hft::coreinfra::ExecAuditLog pair_audit_log;
    const char* const pair_audit_env = std::getenv("HFT_PAIR_AUDIT_LOG");
    const char* const pair_audit_resolved = resolve_pair_audit_path(pair_audit_env != nullptr ? pair_audit_env : "1");
    const bool pair_audit_on =
        pair_audit_resolved != nullptr && pair_audit_resolved[0] != '\0' &&
        pair_audit_log.start(pair_audit_resolved, &pair_audit_ring);
    if (pair_audit_resolved != nullptr && pair_audit_resolved[0] != '\0' && !pair_audit_on) {
        std::cerr << "warn=pair_audit_open_failed path=" << pair_audit_resolved << '\n';
    }
    std::uint64_t pair_audit_drops = 0;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    const bool sigusr1_toggle_kill = [] {
        const char* v = std::getenv("HFT_SIGUSR1_TOGGLE_KILL");
        return v == nullptr || v[0] == '1';
    }();
    if (sigusr1_toggle_kill) {
        std::signal(SIGUSR1, handle_sigusr1);
    }

    using hft::coreinfra::SPSCRing;
    using hft::marketdata::BinanceExecutionMode;
    using hft::marketdata::BinanceFuturesEndpoints;
    using hft::marketdata::BinanceParser;
    using hft::marketdata::BinanceSnapshotClient;
    using hft::marketdata::BinanceWsClient;
    using hft::marketdata::futures_endpoints;
    using hft::marketdata::Instrument;
    using hft::marketdata::MdEvent;
    using hft::marketdata::MdEventType;
    using hft::ordermgmt::CommandType;
    using hft::execution::BinanceGateway;
    using hft::execution::BinanceUserStream;
    using hft::execution::GatewayConfig;
    using hft::execution::ExecEventType;
    using hft::ordermgmt::OrderManager;
    using hft::execution::UserStreamParser;
    using hft::riskmgmt::RiskRejectReason;
    using hft::Side;
    using hft::StrategyEngine;
    using hft::StrategyParams;
    using hft::StrategyState;
    const int cpu_count = static_cast<int>(std::thread::hardware_concurrency());
    const bool busy_spin = [] {
        const char* v = std::getenv("HFT_BUSY_SPIN");
        return v != nullptr && v[0] == '1';
    }();
    const bool rt_fifo = [] {
        const char* v = std::getenv("HFT_RT_FIFO");
        return v != nullptr && v[0] == '1';
    }();
    const int ws_core = read_env_core_or_default("HFT_CORE_WS", 1);
    const int snapshot_core = read_env_core_or_default("HFT_CORE_SNAPSHOT", 2);
    const int main_core = read_env_core_or_default("HFT_CORE_MAIN", 3);
    const int main_rt_prio = read_env_int_or_default("HFT_RT_PRIO_MAIN", 90);
    const int ws_rt_prio = read_env_int_or_default("HFT_RT_PRIO_WS", 80);
    const int snapshot_rt_prio = read_env_int_or_default("HFT_RT_PRIO_SNAPSHOT", 20);
    const bool mlock_enable = [] {
        const char* v = std::getenv("HFT_MLOCKALL");
        return v != nullptr && v[0] == '1';
    }();
    const bool require_all_symbols_sync = [] {
        const char* v = std::getenv("HFT_REQUIRE_ALL_SYMBOLS_SYNC");
        return v == nullptr || v[0] == '1';
    }();
    const bool allow_partial_trading = read_env_flag("HFT_ALLOW_PARTIAL_TRADING", true);
    const int prefault_mb = read_env_int_or_default("HFT_PREFAULT_MB", 64);
    const int trigger_min_interval_us = read_env_int_or_default("HFT_TRIGGER_MIN_INTERVAL_US", 5);
    const int trigger_bps_x1000 = read_env_int_or_default("HFT_TRIGGER_MIN_MID_BPS_X1000", 50);
    const int trigger_imbalance_ppm = read_env_int_or_default("HFT_TRIGGER_MIN_IMB_PPM", 10000);
    const int exec_replace_bps_x1000 = read_env_int_or_default("HFT_EXEC_REPLACE_BPS_X1000", 20);
    const int exec_cancel_stale_ms = read_env_int_or_default("HFT_EXEC_CANCEL_STALE_MS", 0);
    const int exec_cancel_stale_btc_ms =
        read_env_int_or_default("HFT_EXEC_CANCEL_STALE_BTC_MS", exec_cancel_stale_ms);
    const int exec_cancel_stale_eth_ms =
        read_env_int_or_default("HFT_EXEC_CANCEL_STALE_ETH_MS", exec_cancel_stale_ms);
    const int exec_cancel_stale_sol_ms =
        read_env_int_or_default("HFT_EXEC_CANCEL_STALE_SOL_MS", exec_cancel_stale_ms);
    const int exec_adverse_cancel_btc_bps_x1000 =
        read_env_int_or_default("HFT_EXEC_ADVERSE_CANCEL_BTC_BPS_X1000", 0);
    const int exec_adverse_cancel_eth_bps_x1000 =
        read_env_int_or_default("HFT_EXEC_ADVERSE_CANCEL_ETH_BPS_X1000", 0);
    const int exec_adverse_cancel_sol_bps_x1000 =
        read_env_int_or_default("HFT_EXEC_ADVERSE_CANCEL_SOL_BPS_X1000", 0);
    const bool exec_adaptive_cancel = [] {
        const char* v = std::getenv("HFT_EXEC_ADAPTIVE_CANCEL");
        return v != nullptr && v[0] == '1';
    }();
    const int exec_adaptive_target_adv_per_sec = read_env_int_or_default("HFT_EXEC_ADAPTIVE_TARGET_ADV_PER_SEC", 8);
    const int exec_adaptive_target_stale_per_sec = read_env_int_or_default("HFT_EXEC_ADAPTIVE_TARGET_STALE_PER_SEC", 6);
    const int exec_adaptive_adv_step_bps_x1000 = read_env_int_or_default("HFT_EXEC_ADAPTIVE_ADV_STEP_BPS_X1000", 20);
    const int exec_adaptive_stale_step_ms = read_env_int_or_default("HFT_EXEC_ADAPTIVE_STALE_STEP_MS", 10);
    const int exec_adaptive_adv_min_bps_x1000 = read_env_int_or_default("HFT_EXEC_ADAPTIVE_ADV_MIN_BPS_X1000", 20);
    const int exec_adaptive_adv_max_bps_x1000 = read_env_int_or_default("HFT_EXEC_ADAPTIVE_ADV_MAX_BPS_X1000", 500);
    const int exec_adaptive_stale_min_ms = read_env_int_or_default("HFT_EXEC_ADAPTIVE_STALE_MIN_MS", 0);
    const int exec_adaptive_stale_max_ms = read_env_int_or_default("HFT_EXEC_ADAPTIVE_STALE_MAX_MS", 2000);
    const double strat_alpha_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_ALPHA_X1E6", 20000)) / 1.0e6;
    const double strat_base_spread_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_BASE_SPREAD_X10000", 10000)) / 1.0e4;
    const double strat_inventory_limit_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_INV_LIM_X1000", 5000)) / 1.0e3;
    const double strat_alpha_btc = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_ALPHA_BTC_X1E6", read_env_int_or_default("HFT_STRAT_ALPHA_X1E6", 20000))) / 1.0e6;
    const double strat_alpha_eth = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_ALPHA_ETH_X1E6", read_env_int_or_default("HFT_STRAT_ALPHA_X1E6", 20000))) / 1.0e6;
    const double strat_alpha_sol = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_ALPHA_SOL_X1E6", read_env_int_or_default("HFT_STRAT_ALPHA_X1E6", 20000))) / 1.0e6;
    const double strat_spread_btc = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_BASE_SPREAD_BTC_X10000", read_env_int_or_default("HFT_STRAT_BASE_SPREAD_X10000", 10000))) / 1.0e4;
    const double strat_spread_eth = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_BASE_SPREAD_ETH_X10000", read_env_int_or_default("HFT_STRAT_BASE_SPREAD_X10000", 10000))) / 1.0e4;
    const double strat_spread_sol = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_BASE_SPREAD_SOL_X10000", read_env_int_or_default("HFT_STRAT_BASE_SPREAD_X10000", 10000))) / 1.0e4;
    const double strat_inv_lim_btc = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_INV_LIM_BTC_X1000", read_env_int_or_default("HFT_STRAT_INV_LIM_X1000", 5000))) / 1.0e3;
    const double strat_inv_lim_eth = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_INV_LIM_ETH_X1000", read_env_int_or_default("HFT_STRAT_INV_LIM_X1000", 5000))) / 1.0e3;
    const double strat_inv_lim_sol = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_INV_LIM_SOL_X1000", read_env_int_or_default("HFT_STRAT_INV_LIM_X1000", 5000))) / 1.0e3;
    const double strat_edge_bps_default = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_EDGE_BPS_X1000", 300)) / 1.0e3;
    const double strat_qty_min_default = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MIN_X1000", 1)) / 1.0e3;
    const double strat_qty_max_default = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MAX_X1000", 10)) / 1.0e3;
    const double strat_qty_shrink_default = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_INV_SHRINK_PPM", 700000)) / 1.0e6;
    const double strat_edge_bps_btc = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_EDGE_BTC_BPS_X1000", read_env_int_or_default("HFT_STRAT_EDGE_BPS_X1000", 300))) / 1.0e3;
    const double strat_edge_bps_eth = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_EDGE_ETH_BPS_X1000", read_env_int_or_default("HFT_STRAT_EDGE_BPS_X1000", 300))) / 1.0e3;
    const double strat_edge_bps_sol = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_EDGE_SOL_BPS_X1000", read_env_int_or_default("HFT_STRAT_EDGE_BPS_X1000", 300))) / 1.0e3;
    const double strat_qty_min_btc = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MIN_BTC_X1000", read_env_int_or_default("HFT_STRAT_QTY_MIN_X1000", 1))) / 1.0e3;
    const double strat_qty_min_eth = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MIN_ETH_X1000", read_env_int_or_default("HFT_STRAT_QTY_MIN_X1000", 1))) / 1.0e3;
    const double strat_qty_min_sol = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MIN_SOL_X1000", read_env_int_or_default("HFT_STRAT_QTY_MIN_X1000", 1))) / 1.0e3;
    const double strat_qty_max_btc = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MAX_BTC_X1000", read_env_int_or_default("HFT_STRAT_QTY_MAX_X1000", 10))) / 1.0e3;
    const double strat_qty_max_eth = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MAX_ETH_X1000", read_env_int_or_default("HFT_STRAT_QTY_MAX_X1000", 10))) / 1.0e3;
    const double strat_qty_max_sol = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_MAX_SOL_X1000", read_env_int_or_default("HFT_STRAT_QTY_MAX_X1000", 10))) / 1.0e3;
    const double strat_qty_shrink_btc = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_INV_SHRINK_BTC_PPM", read_env_int_or_default("HFT_STRAT_QTY_INV_SHRINK_PPM", 700000))) / 1.0e6;
    const double strat_qty_shrink_eth = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_INV_SHRINK_ETH_PPM", read_env_int_or_default("HFT_STRAT_QTY_INV_SHRINK_PPM", 700000))) / 1.0e6;
    const double strat_qty_shrink_sol = static_cast<double>(
        read_env_int_or_default("HFT_STRAT_QTY_INV_SHRINK_SOL_PPM", read_env_int_or_default("HFT_STRAT_QTY_INV_SHRINK_PPM", 700000))) / 1.0e6;
    const double strat_k_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_K_BPS_X1000", 1500)) / 1.0e3;
    const double strat_max_alpha_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_MAX_ALPHA_BPS_X1000", 3000)) / 1.0e3;
    const double strat_gamma_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_GAMMA_BPS_X1000", 3000)) / 1.0e3;
    const double strat_c1_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_C1_X1000", 1500)) / 1.0e3;
    const double strat_c2_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_C2_BPS_X1000", 3000)) / 1.0e3;
    const double strat_min_delta_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_MIN_DELTA_BPS_X1000", 3000)) / 1.0e3;
    const double strat_max_delta_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_MAX_DELTA_BPS_X1000", 10000)) / 1.0e3;
    const double strat_sigma_threshold_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_SIGMA_THRESHOLD_BPS_X1000", 10000)) / 1.0e3;
    const double strat_fee_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_FEE_BPS_X1000", 2000)) / 1.0e3;
    const double strat_min_profit_buffer_bps_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_MIN_PROFIT_BUFFER_BPS_X1000", 500)) / 1.0e3;
    const double strat_tick_size_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_TICK_SIZE_X1000000", 10000)) / 1.0e6;
    const double strat_base_size_default = static_cast<double>(read_env_int_or_default("HFT_STRAT_BASE_SIZE_X1000", 10)) / 1.0e3;
    const int strat_imbalance_stability_ms_default = read_env_int_or_default("HFT_STRAT_IMBALANCE_STABILITY_MS", 100);
    const int strat_update_interval_ms_default = read_env_int_or_default("HFT_STRAT_UPDATE_INTERVAL_MS", 100);
    const std::array<double, 3> strat_k_bps_by_symbol {
        static_cast<double>(read_env_int_or_default("HFT_STRAT_K_BTC_BPS_X1000", static_cast<int>(strat_k_bps_default * 1000.0))) / 1.0e3,
        static_cast<double>(read_env_int_or_default("HFT_STRAT_K_ETH_BPS_X1000", static_cast<int>(strat_k_bps_default * 1000.0))) / 1.0e3,
        static_cast<double>(read_env_int_or_default("HFT_STRAT_K_SOL_BPS_X1000", static_cast<int>(strat_k_bps_default * 1000.0))) / 1.0e3,
    };
    const std::array<double, 3> strat_max_alpha_bps_by_symbol {
        static_cast<double>(read_env_int_or_default("HFT_STRAT_MAX_ALPHA_BTC_BPS_X1000", static_cast<int>(strat_max_alpha_bps_default * 1000.0))) / 1.0e3,
        static_cast<double>(read_env_int_or_default("HFT_STRAT_MAX_ALPHA_ETH_BPS_X1000", static_cast<int>(strat_max_alpha_bps_default * 1000.0))) / 1.0e3,
        static_cast<double>(read_env_int_or_default("HFT_STRAT_MAX_ALPHA_SOL_BPS_X1000", static_cast<int>(strat_max_alpha_bps_default * 1000.0))) / 1.0e3,
    };
    const std::array<double, 3> strat_gamma_bps_by_symbol {
        static_cast<double>(read_env_int_or_default("HFT_STRAT_GAMMA_BTC_BPS_X1000", static_cast<int>(strat_gamma_bps_default * 1000.0))) / 1.0e3,
        static_cast<double>(read_env_int_or_default("HFT_STRAT_GAMMA_ETH_BPS_X1000", static_cast<int>(strat_gamma_bps_default * 1000.0))) / 1.0e3,
        static_cast<double>(read_env_int_or_default("HFT_STRAT_GAMMA_SOL_BPS_X1000", static_cast<int>(strat_gamma_bps_default * 1000.0))) / 1.0e3,
    };
    const double risk_max_order_qty = static_cast<double>(read_env_int_or_default("HFT_RISK_MAX_ORDER_QTY_X1000", 2000)) / 1000.0;
    const double risk_max_notional = static_cast<double>(read_env_int_or_default("HFT_RISK_MAX_NOTIONAL", 100000));
    const double risk_max_abs_pos = static_cast<double>(read_env_int_or_default("HFT_RISK_MAX_ABS_POS_X1000", 10000)) / 1000.0;
    const double pnl_fee_bps = static_cast<double>(read_env_int_or_default("HFT_PNL_FEE_BPS_X1000", 200)) / 1000.0;
    const double pnl_taker_fee_bps = static_cast<double>(
        read_env_int_or_default("HFT_PNL_TAKER_FEE_BPS_X1000", read_env_int_or_default("HFT_PNL_FEE_BPS_X1000", 200))) / 1000.0;
    const double pnl_maker_fee_bps = static_cast<double>(
        read_env_int_or_default("HFT_PNL_MAKER_FEE_BPS_X1000", read_env_int_or_default("HFT_PNL_FEE_BPS_X1000", 200))) / 1000.0;
    const bool pnl_seed_require_all_symbols =
        read_env_int_or_default("HFT_PNL_SEED_REQUIRE_ALL_SYMBOLS", 1) == 1;
    const bool pnl_drawdown_guard = [] {
        const char* v = std::getenv("HFT_PNL_DRAWDOWN_GUARD");
        return v != nullptr && v[0] == '1';
    }();
    const double pnl_max_dd_usdt_default = static_cast<double>(
        read_env_int_or_default("HFT_PNL_MAX_DRAWDOWN_USDT_X100", 5000)) / 100.0;
    const double pnl_max_dd_usdt_btc = static_cast<double>(
        read_env_int_or_default(
            "HFT_PNL_MAX_DRAWDOWN_BTC_USDT_X100",
            read_env_int_or_default("HFT_PNL_MAX_DRAWDOWN_USDT_X100", 5000))) / 100.0;
    const double pnl_max_dd_usdt_eth = static_cast<double>(
        read_env_int_or_default(
            "HFT_PNL_MAX_DRAWDOWN_ETH_USDT_X100",
            read_env_int_or_default("HFT_PNL_MAX_DRAWDOWN_USDT_X100", 5000))) / 100.0;
    const double pnl_max_dd_usdt_sol = static_cast<double>(
        read_env_int_or_default(
            "HFT_PNL_MAX_DRAWDOWN_SOL_USDT_X100",
            read_env_int_or_default("HFT_PNL_MAX_DRAWDOWN_USDT_X100", 5000))) / 100.0;
    const int pnl_cooldown_sec_default = read_env_int_or_default("HFT_PNL_COOLDOWN_SEC", 15);
    const int pnl_cooldown_sec_btc =
        read_env_int_or_default("HFT_PNL_COOLDOWN_BTC_SEC", pnl_cooldown_sec_default);
    const int pnl_cooldown_sec_eth =
        read_env_int_or_default("HFT_PNL_COOLDOWN_ETH_SEC", pnl_cooldown_sec_default);
    const int pnl_cooldown_sec_sol =
        read_env_int_or_default("HFT_PNL_COOLDOWN_SOL_SEC", pnl_cooldown_sec_default);
    const BinanceExecutionMode binance_mode = read_binance_execution_mode();
    const BinanceFuturesEndpoints binance_ep = futures_endpoints(binance_mode);
    const int gateway_retry_attempts = read_env_int_or_default("HFT_GATEWAY_RETRY_ATTEMPTS", 2);
    const int gateway_retry_backoff_ms = read_env_int_or_default("HFT_GATEWAY_RETRY_BACKOFF_MS", 10);
    const int transport_retry_attempts = read_env_int_or_default("HFT_TRANSPORT_RETRY_ATTEMPTS", 2);
    const int transport_retry_backoff_ms = read_env_int_or_default("HFT_TRANSPORT_RETRY_BACKOFF_MS", 8);
    const int transport_cooldown_ms = read_env_int_or_default("HFT_TRANSPORT_COOLDOWN_MS", 100);
    const int q2s_stale_drop_ms = read_env_int_or_default("HFT_Q2S_STALE_DROP_MS", 50);
    const int rest_weight_soft_limit = read_env_int_or_default("HFT_REST_WEIGHT_SOFT_LIMIT", 1100);
    const int rest_throttle_cooldown_ms = read_env_int_or_default("HFT_REST_THROTTLE_COOLDOWN_MS", 200);
    const int tradable_zero_alert_ms = read_env_int_or_default("HFT_TRADABLE_ZERO_ALERT_MS", 3000);
    const int rec_mismatch_alert_step = read_env_int_or_default("HFT_REC_MISMATCH_ALERT_STEP", 5);
    const bool reconcile_heal = [] {
        const char* v = std::getenv("HFT_RECONCILE_HEAL");
        return v != nullptr && v[0] == '1';
    }();
    const int reconcile_heal_max_per_tick = read_env_int_or_default("HFT_RECONCILE_HEAL_MAX_PER_TICK", 4);
    const int exec_min_send_interval_us = read_env_int_or_default("HFT_EXEC_MIN_SEND_INTERVAL_US", 0);
    const int exec_min_send_interval_btc_us =
        read_env_int_or_default("HFT_EXEC_MIN_SEND_INTERVAL_BTC_US", exec_min_send_interval_us);
    const int exec_min_send_interval_eth_us =
        read_env_int_or_default("HFT_EXEC_MIN_SEND_INTERVAL_ETH_US", exec_min_send_interval_us);
    const int exec_min_send_interval_sol_us =
        read_env_int_or_default("HFT_EXEC_MIN_SEND_INTERVAL_SOL_US", exec_min_send_interval_us);
    const int reconcile_interval_sec = read_env_int_or_default("HFT_RECONCILE_INTERVAL_SEC", 0);
    const int oms_pending_timeout_ms = read_env_int_or_default("HFT_OMS_PENDING_TIMEOUT_MS", 3000);
    const int oms_pending_heal_max_per_tick = read_env_int_or_default("HFT_OMS_PENDING_HEAL_MAX_PER_TICK", 2);
    const int reconcile_core = read_env_core_or_default("HFT_CORE_RECONCILE", read_env_core_or_default("HFT_CORE_SNAPSHOT", 2));
    const int snapshot_retry_attempts = read_env_int_or_default("HFT_SNAPSHOT_RETRY_ATTEMPTS", 3);
    const int snapshot_retry_backoff_ms = read_env_int_or_default("HFT_SNAPSHOT_RETRY_BACKOFF_MS", 20);
    const int snapshot_retry_max_backoff_ms = read_env_int_or_default("HFT_SNAPSHOT_RETRY_MAX_BACKOFF_MS", 1000);
    const bool canary_fill_mode = read_env_flag("HFT_CANARY_FILL_MODE", false);
    const double canary_fill_cross_bps = read_env_double_or_default("HFT_CANARY_FILL_CROSS_BPS", 5.0);
    const bool canary_rotate_symbols = read_env_flag("HFT_CANARY_ROTATE_SYMBOLS", false);
    const int canary_rotation_window_ms = read_env_int_or_default("HFT_CANARY_ROTATION_WINDOW_MS", 3000);
    const double exch_min_notional_usdt = read_env_double_or_default("HFT_EXCH_MIN_NOTIONAL_USDT", 5.0);
    const int lifecycle_timeout_ms = read_env_int_or_default("HFT_LIFECYCLE_TIMEOUT_MS", 10000);
    const int main_md_batch_max = read_env_int_or_default("HFT_MAIN_MD_BATCH_MAX", 256);
    const int exec_report_batch_max = read_env_int_or_default("HFT_EXEC_REPORT_BATCH_MAX", 512);
    const int lifecycle_timeout_audit_max_per_tick =
        read_env_int_or_default("HFT_LIFECYCLE_TIMEOUT_AUDIT_MAX_PER_TICK", 4);
    const int transport_circuit_fail_threshold =
        read_env_int_or_default("HFT_TRANSPORT_CIRCUIT_FAIL_THRESHOLD", 3);
    const int transport_circuit_cooldown_ms =
        read_env_int_or_default("HFT_TRANSPORT_CIRCUIT_COOLDOWN_MS", 1500);
    const int ws_idle_reconnect_ms = read_env_int_or_default("HFT_WS_IDLE_RECONNECT_MS", 1500);
    const int ws_idle_reconnect_cooldown_ms =
        read_env_int_or_default("HFT_WS_IDLE_RECONNECT_COOLDOWN_MS", 5000);

    const char* binance_api_key = std::getenv("BINANCE_API_KEY");
    const char* binance_api_secret = std::getenv("BINANCE_API_SECRET");
    const bool binance_creds_present =
        binance_api_key != nullptr && binance_api_key[0] != '\0' &&
        binance_api_secret != nullptr && binance_api_secret[0] != '\0';
    if (!binance_creds_present) {
        std::cerr << "fatal=missing_binance_credentials\n";
        return 1;
    }

    std::cout << "startup cpu_count=" << cpu_count
              << " busy_spin=" << (busy_spin ? 1 : 0)
              << " rt_fifo=" << (rt_fifo ? 1 : 0)
              << " core_ws=" << ws_core
              << " core_snapshot=" << snapshot_core
              << " core_main=" << main_core
              << " prio_ws=" << ws_rt_prio
              << " prio_snapshot=" << snapshot_rt_prio
              << " prio_main=" << main_rt_prio
              << " mlockall=" << (mlock_enable ? 1 : 0)
              << " require_all_sync=" << (require_all_symbols_sync ? 1 : 0)
              << " allow_partial_trading=" << (allow_partial_trading ? 1 : 0)
              << " prefault_mb=" << prefault_mb
              << " trig_min_us=" << trigger_min_interval_us
              << " trig_mid_bps_x1000=" << trigger_bps_x1000
              << " trig_imb_ppm=" << trigger_imbalance_ppm
              << " exec_replace_bps_x1000=" << exec_replace_bps_x1000
              << " exec_cancel_stale_ms=" << exec_cancel_stale_ms
              << " exec_cancel_stale_btc_ms=" << exec_cancel_stale_btc_ms
              << " exec_cancel_stale_eth_ms=" << exec_cancel_stale_eth_ms
              << " exec_cancel_stale_sol_ms=" << exec_cancel_stale_sol_ms
              << " exec_adv_cancel_btc_bps_x1000=" << exec_adverse_cancel_btc_bps_x1000
              << " exec_adv_cancel_eth_bps_x1000=" << exec_adverse_cancel_eth_bps_x1000
              << " exec_adv_cancel_sol_bps_x1000=" << exec_adverse_cancel_sol_bps_x1000
              << " exec_adaptive_cancel=" << (exec_adaptive_cancel ? 1 : 0)
              << " exec_adapt_tgt_adv_ps=" << exec_adaptive_target_adv_per_sec
              << " exec_adapt_tgt_stale_ps=" << exec_adaptive_target_stale_per_sec
              << " strat_alpha=" << strat_alpha_default
              << " strat_spread=" << strat_base_spread_default
              << " strat_inv_lim=" << strat_inventory_limit_default
              << " strat_edge_bps=" << strat_edge_bps_default
              << " strat_qty_min=" << strat_qty_min_default
              << " strat_qty_max=" << strat_qty_max_default
              << " strat_qty_inv_shrink=" << strat_qty_shrink_default
              << " strat_edge_btc_bps=" << strat_edge_bps_btc
              << " strat_edge_eth_bps=" << strat_edge_bps_eth
              << " strat_edge_sol_bps=" << strat_edge_bps_sol
              << " strat_qty_min_btc=" << strat_qty_min_btc
              << " strat_qty_min_eth=" << strat_qty_min_eth
              << " strat_qty_min_sol=" << strat_qty_min_sol
              << " strat_qty_max_btc=" << strat_qty_max_btc
              << " strat_qty_max_eth=" << strat_qty_max_eth
              << " strat_qty_max_sol=" << strat_qty_max_sol
              << " strat_qty_shrink_btc=" << strat_qty_shrink_btc
              << " strat_qty_shrink_eth=" << strat_qty_shrink_eth
              << " strat_qty_shrink_sol=" << strat_qty_shrink_sol
              << " strat_alpha_btc=" << strat_alpha_btc
              << " strat_alpha_eth=" << strat_alpha_eth
              << " strat_alpha_sol=" << strat_alpha_sol
              << " strat_spread_btc=" << strat_spread_btc
              << " strat_spread_eth=" << strat_spread_eth
              << " strat_spread_sol=" << strat_spread_sol
              << " strat_inv_lim_btc=" << strat_inv_lim_btc
              << " strat_inv_lim_eth=" << strat_inv_lim_eth
              << " strat_inv_lim_sol=" << strat_inv_lim_sol
              << " risk_max_qty=" << risk_max_order_qty
              << " risk_max_notional=" << risk_max_notional
              << " risk_max_abs_pos=" << risk_max_abs_pos
              << " pnl_fee_bps=" << pnl_fee_bps
              << " pnl_taker_fee_bps=" << pnl_taker_fee_bps
              << " pnl_maker_fee_bps=" << pnl_maker_fee_bps
              << " pnl_seed_require_all=" << (pnl_seed_require_all_symbols ? 1 : 0)
              << " pnl_dd_guard=" << (pnl_drawdown_guard ? 1 : 0)
              << " pnl_dd_usdt=" << pnl_max_dd_usdt_default
              << " pnl_dd_btc=" << pnl_max_dd_usdt_btc
              << " pnl_dd_eth=" << pnl_max_dd_usdt_eth
              << " pnl_dd_sol=" << pnl_max_dd_usdt_sol
              << " pnl_cd_sec=" << pnl_cooldown_sec_default
              << " binance_mode=" << binance_mode_label(binance_mode)
              << " gw_retry_attempts=" << gateway_retry_attempts
              << " gw_retry_backoff_ms=" << gateway_retry_backoff_ms
              << " transport_retry_attempts=" << transport_retry_attempts
              << " transport_retry_backoff_ms=" << transport_retry_backoff_ms
              << " transport_cooldown_ms=" << transport_cooldown_ms
              << " rest_wt_soft_limit=" << rest_weight_soft_limit
              << " rest_cooldown_ms=" << rest_throttle_cooldown_ms
              << " q2s_stale_drop_ms=" << q2s_stale_drop_ms
              << " reconcile_heal=" << (reconcile_heal ? 1 : 0)
              << " reconcile_heal_max=" << reconcile_heal_max_per_tick
              << " exec_min_send_us=" << exec_min_send_interval_us
              << " exec_min_send_btc_us=" << exec_min_send_interval_btc_us
              << " exec_min_send_eth_us=" << exec_min_send_interval_eth_us
              << " exec_min_send_sol_us=" << exec_min_send_interval_sol_us
              << " kill_switch=" << (g_kill_switch.load(std::memory_order_relaxed) ? 1 : 0)
              << " exec_audit=" << (exec_audit_on ? 1 : 0)
              << " exec_audit_path=" << (exec_audit_on ? exec_audit_resolved : "-")
              << " pair_audit=" << (pair_audit_on ? 1 : 0)
              << " pair_audit_path=" << (pair_audit_on ? pair_audit_resolved : "-")
              << " md_health_log=" << (md_health_on ? 1 : 0)
              << " md_health_path=" << (md_health_on ? md_health_resolved : "-")
              << " sigusr1_kill_toggle=" << (sigusr1_toggle_kill ? 1 : 0)
              << " reconcile_sec=" << reconcile_interval_sec
              << " oms_pending_timeout_ms=" << oms_pending_timeout_ms
              << " oms_pending_heal_max=" << oms_pending_heal_max_per_tick
              << " core_reconcile=" << reconcile_core
              << " snap_retry_attempts=" << snapshot_retry_attempts
              << " snap_retry_backoff_ms=" << snapshot_retry_backoff_ms
              << " snap_retry_max_backoff_ms=" << snapshot_retry_max_backoff_ms
              << " canary_fill_mode=" << (canary_fill_mode ? 1 : 0)
              << " canary_fill_cross_bps=" << canary_fill_cross_bps
              << " canary_rotate_symbols=" << (canary_rotate_symbols ? 1 : 0)
              << " canary_rotation_window_ms=" << canary_rotation_window_ms
              << " lifecycle_timeout_ms=" << lifecycle_timeout_ms
              << " main_md_batch_max=" << main_md_batch_max
              << " exec_report_batch_max=" << exec_report_batch_max
              << " lifecycle_timeout_audit_max_per_tick=" << lifecycle_timeout_audit_max_per_tick
              << " transport_circuit_fail_threshold=" << transport_circuit_fail_threshold
              << " transport_circuit_cooldown_ms=" << transport_circuit_cooldown_ms
              << " ws_idle_reconnect_ms=" << ws_idle_reconnect_ms
              << " ws_idle_reconnect_cooldown_ms=" << ws_idle_reconnect_cooldown_ms
              << " exch_min_notional_usdt=" << exch_min_notional_usdt
              << " rest_host=" << binance_ep.rest_host
              << " stream_host=" << binance_ep.stream_ws_host
              << '\n';
    exec_audit_line(
        md_health_on,
        md_health_log,
        md_health_drops,
        "ts_ns=%llu event=md_health_start ws_idle_reconnect_ms=%d ws_idle_reconnect_cooldown_ms=%d",
        static_cast<unsigned long long>(now_ns()),
        ws_idle_reconnect_ms,
        ws_idle_reconnect_cooldown_ms);

    if (mlock_enable && !enable_memory_locking()) {
        std::cerr << "warn=mlockall_failed\n";
    }
    prefault_memory_mb(prefault_mb);

    constexpr std::size_t kQueueSize = 1 << 17;
    static SPSCRing<MdEvent, kQueueSize> queue;
    constexpr std::size_t kSnapshotQueueSize = 256;
    static SPSCRing<SnapshotRequest, kSnapshotQueueSize> snapshot_req_queue;
    static SPSCRing<SnapshotResult, kSnapshotQueueSize> snapshot_res_queue;
    constexpr std::size_t kExecReportQueueSize = 2048;
    static SPSCRing<ExecReportMsg, kExecReportQueueSize> exec_report_queue;
    BinanceParser parser;
    const std::array<StrategyEngine, 3> strategy_engines {
        StrategyEngine(StrategyParams{
            strat_k_bps_by_symbol[0], strat_max_alpha_bps_by_symbol[0], strat_gamma_bps_by_symbol[0],
            strat_c1_default, strat_c2_bps_default, strat_min_delta_bps_default, strat_max_delta_bps_default,
            strat_sigma_threshold_bps_default, strat_fee_bps_default, strat_min_profit_buffer_bps_default,
            strat_tick_size_default, strat_inv_lim_btc, strat_base_size_default, strat_qty_min_btc,
            static_cast<std::uint32_t>(strat_imbalance_stability_ms_default > 0 ? strat_imbalance_stability_ms_default : 100),
            static_cast<std::uint32_t>(strat_update_interval_ms_default > 0 ? strat_update_interval_ms_default : 100)}),
        StrategyEngine(StrategyParams{
            strat_k_bps_by_symbol[1], strat_max_alpha_bps_by_symbol[1], strat_gamma_bps_by_symbol[1],
            strat_c1_default, strat_c2_bps_default, strat_min_delta_bps_default, strat_max_delta_bps_default,
            strat_sigma_threshold_bps_default, strat_fee_bps_default, strat_min_profit_buffer_bps_default,
            strat_tick_size_default, strat_inv_lim_eth, strat_base_size_default, strat_qty_min_eth,
            static_cast<std::uint32_t>(strat_imbalance_stability_ms_default > 0 ? strat_imbalance_stability_ms_default : 100),
            static_cast<std::uint32_t>(strat_update_interval_ms_default > 0 ? strat_update_interval_ms_default : 100)}),
        StrategyEngine(StrategyParams{
            strat_k_bps_by_symbol[2], strat_max_alpha_bps_by_symbol[2], strat_gamma_bps_by_symbol[2],
            strat_c1_default, strat_c2_bps_default, strat_min_delta_bps_default, strat_max_delta_bps_default,
            strat_sigma_threshold_bps_default, strat_fee_bps_default, strat_min_profit_buffer_bps_default,
            strat_tick_size_default, strat_inv_lim_sol, strat_base_size_default, strat_qty_min_sol,
            static_cast<std::uint32_t>(strat_imbalance_stability_ms_default > 0 ? strat_imbalance_stability_ms_default : 100),
            static_cast<std::uint32_t>(strat_update_interval_ms_default > 0 ? strat_update_interval_ms_default : 100)}),
    };
    OrderManager order_manager(
        static_cast<std::uint32_t>(exec_replace_bps_x1000 > 0 ? exec_replace_bps_x1000 : 20),
        static_cast<std::uint32_t>(exec_cancel_stale_ms > 0 ? exec_cancel_stale_ms : 0),
        {
            static_cast<std::uint32_t>(exec_cancel_stale_btc_ms > 0 ? exec_cancel_stale_btc_ms : 0),
            static_cast<std::uint32_t>(exec_cancel_stale_eth_ms > 0 ? exec_cancel_stale_eth_ms : 0),
            static_cast<std::uint32_t>(exec_cancel_stale_sol_ms > 0 ? exec_cancel_stale_sol_ms : 0),
        },
        {
            static_cast<std::uint32_t>(exec_adverse_cancel_btc_bps_x1000 > 0 ? exec_adverse_cancel_btc_bps_x1000 : 0),
            static_cast<std::uint32_t>(exec_adverse_cancel_eth_bps_x1000 > 0 ? exec_adverse_cancel_eth_bps_x1000 : 0),
            static_cast<std::uint32_t>(exec_adverse_cancel_sol_bps_x1000 > 0 ? exec_adverse_cancel_sol_bps_x1000 : 0),
        });
    hft::riskmgmt::PreTradeRisk risk(hft::riskmgmt::RiskConfig{
        risk_max_order_qty,
        risk_max_notional,
        risk_max_abs_pos,
        &g_kill_switch,
    });
    hft::PnLEngine pnl_engine(pnl_taker_fee_bps, pnl_maker_fee_bps);
    hft::ordermgmt::OmsState oms;
    BinanceGateway gateway(GatewayConfig{
        binance_ep.rest_host,
        binance_ep.rest_port,
        binance_api_key,
        binance_api_secret,
        static_cast<std::uint32_t>(gateway_retry_attempts > 0 ? gateway_retry_attempts : 1),
        static_cast<std::uint32_t>(gateway_retry_backoff_ms >= 0 ? gateway_retry_backoff_ms : 0),
        &g_rest_weight_1m,
    });
    const auto preflight_open_orders = gateway.signed_open_orders();
    const auto preflight_position_risk = gateway.signed_position_risk();
    const std::array<double, 3> exch_min_notional_by_symbol {
        gateway.symbol_constraints(Instrument::BtcUsdt).min_notional > 0.0
            ? gateway.symbol_constraints(Instrument::BtcUsdt).min_notional
            : exch_min_notional_usdt,
        gateway.symbol_constraints(Instrument::EthUsdt).min_notional > 0.0
            ? gateway.symbol_constraints(Instrument::EthUsdt).min_notional
            : exch_min_notional_usdt,
        gateway.symbol_constraints(Instrument::SolUsdt).min_notional > 0.0
            ? gateway.symbol_constraints(Instrument::SolUsdt).min_notional
            : exch_min_notional_usdt,
    };
    std::cout << "preflight kind=open_orders ok=" << (preflight_open_orders.ok ? 1 : 0)
              << " http=" << preflight_open_orders.http_status
              << " ix=" << preflight_open_orders.binance_error_code
              << " open=" << (preflight_open_orders.ok
                  ? count_json_client_order_ids(preflight_open_orders.body)
                  : 0)
              << " min_notional_btc=" << exch_min_notional_by_symbol[0]
              << " min_notional_eth=" << exch_min_notional_by_symbol[1]
              << " min_notional_sol=" << exch_min_notional_by_symbol[2]
              << '\n';
    std::cout << "preflight kind=position_risk ok=" << (preflight_position_risk.ok ? 1 : 0)
              << " http=" << preflight_position_risk.http_status
              << " ix=" << preflight_position_risk.binance_error_code
              << '\n';

    if (preflight_open_orders.ok) {
        std::array<RemoteOpenOrderRef, kReconcileMaxIds> startup_open_refs {};
        const std::size_t startup_open_count =
            extract_json_open_order_refs(preflight_open_orders.body, startup_open_refs);
        for (std::size_t i = 0; i < startup_open_count; ++i) {
            const auto& r = startup_open_refs[i];
            const auto cancel_res = gateway.send(hft::ordermgmt::OrderCommand {
                hft::ordermgmt::CommandType::Cancel,
                r.instrument,
                r.client_order_id,
                hft::Side::Buy,
                0.0,
                0.0,
                now_ns()});
            std::cout << "preflight_cleanup kind=cancel_remote_open sym=" << instrument_name(r.instrument)
                      << " coid=" << r.client_order_id
                      << " ok=" << (cancel_res.ok ? 1 : 0)
                      << " http=" << cancel_res.http_status
                      << " ix=" << cancel_res.binance_error_code
                      << '\n';
        }
    }

    std::array<hft::orderbook::L2Book, 3> books;
    std::array<DepthEventBuffer<1024>, 3> pending_depth;
    std::array<StrategyState, 3> strategy_states;
    std::array<hft::PnLState, 3> pnl_states;
    std::array<double, 3> mid_px_by_symbol {0.0, 0.0, 0.0};
    std::array<double, 3> pnl_peak_by_symbol {
        std::numeric_limits<double>::lowest(),
        std::numeric_limits<double>::lowest(),
        std::numeric_limits<double>::lowest(),
    };
    std::array<std::uint64_t, 3> pnl_pause_until_ns {0, 0, 0};
    const std::array<double, 3> pnl_max_dd_by_symbol {
        pnl_max_dd_usdt_btc,
        pnl_max_dd_usdt_eth,
        pnl_max_dd_usdt_sol,
    };
    const std::array<std::uint64_t, 3> pnl_cooldown_ns_by_symbol {
        static_cast<std::uint64_t>(pnl_cooldown_sec_btc > 0 ? pnl_cooldown_sec_btc : 0) * 1000000000ULL,
        static_cast<std::uint64_t>(pnl_cooldown_sec_eth > 0 ? pnl_cooldown_sec_eth : 0) * 1000000000ULL,
        static_cast<std::uint64_t>(pnl_cooldown_sec_sol > 0 ? pnl_cooldown_sec_sol : 0) * 1000000000ULL,
    };
    std::size_t position_seed_parsed_count = 0;
    std::size_t position_seed_missing_count = 3;
    std::size_t position_seed_invalid_count = 0;
    if (preflight_position_risk.ok) {
        std::array<hft::analytics::PositionSeed, 3> seeds {};
        std::size_t invalid_seed_records = 0;
        const std::size_t seeded = hft::analytics::parse_position_risk_seeds(
            preflight_position_risk.body,
            seeds,
            &invalid_seed_records);
        const std::size_t missing = seeded < seeds.size() ? (seeds.size() - seeded) : 0;
        position_seed_parsed_count = seeded;
        position_seed_missing_count = missing;
        position_seed_invalid_count = invalid_seed_records;
        std::cout << "preflight kind=position_seed_summary parsed=" << seeded
                  << " missing=" << missing
                  << " invalid=" << invalid_seed_records
                  << " require_all=" << (pnl_seed_require_all_symbols ? 1 : 0)
                  << '\n';
        if (!hft::analytics::strict_seed_ok(
                pnl_seed_require_all_symbols,
                seeded,
                invalid_seed_records,
                seeds.size())) {
            std::cerr << "fatal: position seed strict mode requires all symbols valid"
                      << " parsed=" << seeded
                      << " missing=" << missing
                      << " invalid=" << invalid_seed_records
                      << '\n';
            std::exit(1);
        }
        hft::analytics::apply_position_seeds(seeds, risk, pnl_states);
        if (seeded > 0) {
            constexpr std::array<const char*, 3> kSymbols {"BTCUSDT", "ETHUSDT", "SOLUSDT"};
            for (std::size_t i = 0; i < seeds.size(); ++i) {
                if (!seeds[i].present) {
                    continue;
                }
                const double pos = seeds[i].position;
                const double entry = pnl_states[i].avg_price;
                pnl_peak_by_symbol[i] = pnl_engine.mark_to_market(
                    pnl_states[i],
                    entry > 0.0 ? entry : 1.0);
                std::cout << "preflight kind=position_seed sym=" << kSymbols[i]
                          << " pos=" << pos
                          << " entry=" << pnl_states[i].avg_price
                          << '\n';
            }
        }
    }
    std::array<std::atomic<bool>, 3> snapshot_pending {
        std::atomic<bool>{false},
        std::atomic<bool>{false},
        std::atomic<bool>{false},
    };
    constexpr std::array<Instrument, 3> instruments {
        Instrument::BtcUsdt,
        Instrument::EthUsdt,
        Instrument::SolUsdt,
    };
    struct LifecycleEntry {
        bool active {false};
        bool timeout_reported {false};
        std::uint64_t client_order_id {0};
        Instrument instrument {Instrument::Unknown};
        std::uint64_t ts_sent_ns {0};
    };
    constexpr std::size_t kLifecycleCap = 4096;
    std::array<LifecycleEntry, kLifecycleCap> lifecycle {};
    struct PairTrack {
        bool active {false};
        bool emitted {false};
        std::uint64_t pair_id {0};
        Instrument instrument {Instrument::Unknown};
        std::uint64_t ts_intent_ns {0};
        bool intent_bid {false};
        bool intent_ask {false};
        double intent_bid_px {0.0};
        double intent_bid_qty {0.0};
        double intent_ask_px {0.0};
        double intent_ask_qty {0.0};
        std::uint64_t open_coids {0};
        std::uint64_t cmd_sent {0};
        std::uint64_t fills {0};
        std::uint64_t cancels {0};
        std::uint64_t rejects {0};
        std::uint64_t ts_last_event_ns {0};
        std::uint64_t bid_last_coid {0};
        std::uint64_t ask_last_coid {0};
        double bid_last_limit_px {0.0};
        double ask_last_limit_px {0.0};
        double bid_fill_qty {0.0};
        double ask_fill_qty {0.0};
        double bid_fill_notional {0.0};
        double ask_fill_notional {0.0};
        bool bid_terminal {false};
        bool ask_terminal {false};
        /// Last terminal disposition for this leg (set once): R=reject C=cancel F=filled-done H=heal
        char bid_term_tag {'\0'};
        char ask_term_tag {'\0'};
        double buy_qty {0.0};
        double buy_notional {0.0};
        double sell_qty {0.0};
        double sell_notional {0.0};
    };
    struct CoidPairRef {
        bool active {false};
        std::uint64_t client_order_id {0};
        std::uint64_t pair_id {0};
        Side side {Side::Buy};
    };
    constexpr std::size_t kPairTrackCap = 2048;
    constexpr std::size_t kPairCoidCap = 4096;
    std::array<PairTrack, kPairTrackCap> pair_tracks {};
    std::array<CoidPairRef, kPairCoidCap> pair_by_coid {};
    std::array<std::uint64_t, 3> current_pair_id_by_symbol {0, 0, 0};
    struct LastIntentQuote {
        bool valid {false};
        bool has_bid {false};
        bool has_ask {false};
        double bid_px {0.0};
        double bid_qty {0.0};
        double ask_px {0.0};
        double ask_qty {0.0};
    };
    std::array<LastIntentQuote, 3> last_intent_quote_by_symbol {};
    std::uint64_t pair_seq = 0;
    std::uint64_t pair_track_overflow = 0;
    std::uint64_t pair_coid_overflow = 0;
    std::uint64_t pair_close_logged = 0;
    std::uint64_t pair_intent_logged = 0;

    std::atomic<std::uint64_t> rx_count {0};
    std::atomic<std::uint64_t> drop_count {0};
    std::atomic<std::uint64_t> parse_reject_count {0};
    std::atomic<std::uint64_t> parse_ns_sum {0};
    std::atomic<std::uint64_t> parse_ns_max {0};
    std::atomic<std::uint64_t> enqueue_ns_sum {0};
    std::atomic<std::uint64_t> enqueue_ns_max {0};
    std::atomic<std::uint64_t> ws_last_msg_ns {0};

    BinanceWsClient ws_client(
        [&](const std::string& msg, std::uint64_t recv_ns) {
        const std::uint64_t parse_start_ns = now_ns();
        auto event = parser.parse_combined_message(msg, recv_ns);
        const std::uint64_t parse_end_ns = now_ns();
        const std::uint64_t parse_ns = parse_end_ns > recv_ns ? (parse_end_ns - recv_ns) : (parse_end_ns - parse_start_ns);
        parse_ns_sum.fetch_add(parse_ns, std::memory_order_relaxed);
        auto cur_parse_max = parse_ns_max.load(std::memory_order_relaxed);
        while (parse_ns > cur_parse_max &&
               !parse_ns_max.compare_exchange_weak(cur_parse_max, parse_ns, std::memory_order_relaxed)) {
        }
        if (!event.has_value()) {
            parse_reject_count.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        ws_last_msg_ns.store(recv_ns, std::memory_order_relaxed);
        event->ts_parse_ns = parse_end_ns;
        rx_count.fetch_add(1, std::memory_order_relaxed);
        const std::uint64_t push_start_ns = now_ns();
        event->ts_enqueued_ns = push_start_ns;
        if (!queue.push(*event)) {
            drop_count.fetch_add(1, std::memory_order_relaxed);
        }
        const std::uint64_t push_end_ns = now_ns();
        const std::uint64_t enqueue_ns = push_end_ns - push_start_ns;
        enqueue_ns_sum.fetch_add(enqueue_ns, std::memory_order_relaxed);
        auto cur_enqueue_max = enqueue_ns_max.load(std::memory_order_relaxed);
        while (enqueue_ns > cur_enqueue_max &&
               !enqueue_ns_max.compare_exchange_weak(cur_enqueue_max, enqueue_ns, std::memory_order_relaxed)) {
        }
    },
        binance_ep.stream_ws_host,
        binance_ep.stream_ws_port);

    std::thread snapshot_thread([&] {
        const bool ok = pin_current_thread_to_core(snapshot_core);
        if (!ok) {
            std::cerr << "warn=affinity_failed thread=snapshot core=" << snapshot_core << '\n';
        }
        if (rt_fifo && !set_realtime_fifo_priority(snapshot_rt_prio)) {
            std::cerr << "warn=rt_failed thread=snapshot prio=" << snapshot_rt_prio << '\n';
        }
        BinanceSnapshotClient snapshot_client(binance_ep.rest_host, binance_ep.rest_port);
        SnapshotRequest req;
        while (!g_stop.load(std::memory_order_relaxed)) {
            if (!snapshot_req_queue.pop(req)) {
                std::this_thread::sleep_for(std::chrono::microseconds(200));
                continue;
            }

            SnapshotResult res;
            res.instrument = req.instrument;
            const int attempts = snapshot_retry_attempts > 0 ? snapshot_retry_attempts : 1;
            for (int attempt = 0; attempt < attempts && !g_stop.load(std::memory_order_relaxed); ++attempt) {
                const auto snap = snapshot_client.fetch_depth_snapshot(req.instrument);
                if (snap.has_value()) {
                    res.success = true;
                    res.snapshot = *snap;
                    break;
                }
                if (attempt + 1 < attempts && snapshot_retry_backoff_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(snapshot_retry_backoff_ms));
                }
            }

            while (!g_stop.load(std::memory_order_relaxed) && !snapshot_res_queue.push(res)) {
                std::this_thread::yield();
            }
        }
    });
    std::atomic<bool> ws_stop {false};
    auto launch_ws_thread = [&]() {
        return std::thread([&] {
            const bool ok = pin_current_thread_to_core(ws_core);
            if (!ok) {
                std::cerr << "warn=affinity_failed thread=ws core=" << ws_core << '\n';
            }
            if (rt_fifo && !set_realtime_fifo_priority(ws_rt_prio)) {
                std::cerr << "warn=rt_failed thread=ws prio=" << ws_rt_prio << '\n';
            }
            ws_client.run(ws_stop);
        });
    };
    std::thread ws_thread = launch_ws_thread();
    std::thread user_stream_thread([&] {
        const bool ok = pin_current_thread_to_core(snapshot_core);
        if (!ok) {
            std::cerr << "warn=affinity_failed thread=user_stream core=" << snapshot_core << '\n';
        }
        if (rt_fifo && !set_realtime_fifo_priority(snapshot_rt_prio)) {
            std::cerr << "warn=rt_failed thread=user_stream prio=" << snapshot_rt_prio << '\n';
        }

        const char* api_key = std::getenv("BINANCE_API_KEY");
        if (api_key == nullptr || api_key[0] == '\0') {
            while (!g_stop.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
            }
            return;
        }

        BinanceUserStream us(
            api_key,
            binance_ep.rest_host,
            binance_ep.rest_port,
            binance_ep.stream_ws_host,
            binance_ep.stream_ws_port);
        UserStreamParser parser;
        auto last_keepalive = std::chrono::steady_clock::now();
        std::string listen_key = us.create_listen_key();
        while (!g_stop.load(std::memory_order_relaxed)) {
            if (listen_key.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                listen_key = us.create_listen_key();
                continue;
            }
            us.run_ws(listen_key, g_stop, [&](const std::string& msg) {
                const auto report = parser.parse_order_trade_update(msg);
                if (!report.has_value()) {
                    return;
                }
                ExecReportMsg m;
                m.report = *report;
                m.ts_ns = now_ns();
                while (!g_stop.load(std::memory_order_relaxed) && !exec_report_queue.push(m)) {
                    std::this_thread::yield();
                }
            });
            const auto now = std::chrono::steady_clock::now();
            if (now - last_keepalive >= std::chrono::minutes(20)) {
                us.keepalive_listen_key(listen_key);
                last_keepalive = now;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    });

    const char* api_secret_for_reconcile = std::getenv("BINANCE_API_SECRET");
    const bool reconcile_run = reconcile_interval_sec > 0 && api_secret_for_reconcile != nullptr &&
        api_secret_for_reconcile[0] != '\0';
    std::thread reconcile_thread;
    if (reconcile_run) {
        reconcile_thread = std::thread([&gateway, reconcile_interval_sec, reconcile_core, rt_fifo, snapshot_rt_prio] {
            const bool ok = pin_current_thread_to_core(reconcile_core);
            if (!ok) {
                std::cerr << "warn=affinity_failed thread=reconcile core=" << reconcile_core << '\n';
            }
            if (rt_fifo && !set_realtime_fifo_priority(snapshot_rt_prio)) {
                std::cerr << "warn=rt_failed thread=reconcile prio=" << snapshot_rt_prio << '\n';
            }
            while (!g_stop.load(std::memory_order_relaxed)) {
                const auto r = gateway.signed_open_orders();
                if (!r.ok) {
                    g_reconcile_http_fail.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::array<std::uint64_t, kReconcileMaxIds> ids {};
                    const std::size_t ids_n = extract_json_client_order_ids(r.body, ids);
                    for (std::size_t i = 0; i < ids_n; ++i) {
                        g_reconcile_remote_ids[i].store(ids[i], std::memory_order_relaxed);
                    }
                    g_reconcile_remote_ids_count.store(static_cast<std::uint32_t>(ids_n), std::memory_order_release);
                    g_reconcile_remote_open.store(static_cast<std::uint64_t>(ids_n), std::memory_order_release);
                    g_reconcile_seq.fetch_add(1, std::memory_order_release);
                }
                for (int s = 0; s < reconcile_interval_sec && !g_stop.load(std::memory_order_relaxed); ++s) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }
        });
    }

    if (!pin_current_thread_to_core(main_core)) {
        std::cerr << "warn=affinity_failed thread=main core=" << main_core << '\n';
    }
    if (rt_fifo && !set_realtime_fifo_priority(main_rt_prio)) {
        std::cerr << "warn=rt_failed thread=main prio=" << main_rt_prio << '\n';
    }

    MdEvent event;
    auto last_stats = std::chrono::steady_clock::now();
    std::uint64_t consumed = 0;
    std::uint64_t book_ticker = 0;
    std::uint64_t depth_update = 0;
    std::uint64_t agg_trade = 0;
    std::uint64_t strategy_signals = 0;
    std::uint64_t exec_cmd_new = 0;
    std::uint64_t exec_cmd_replace = 0;
    std::uint64_t exec_cmd_cancel = 0;
    std::uint64_t exec_cmd_sent = 0;
    std::uint64_t exec_cmd_rejected_risk = 0;
    std::uint64_t exec_risk_kill = 0;
    std::uint64_t exec_cmd_gateway_fail = 0;
    int gw_fail_last_http = 0;
    int gw_fail_last_ix = 0;
    std::uint64_t exec_cmd_rate_limited = 0;
    std::uint64_t exec_cmd_paced = 0;
    std::uint64_t rest_cooldown_until_ns = 0;
    std::array<std::uint64_t, 3> last_send_ns_by_symbol {0, 0, 0};
    std::array<std::uint64_t, 3> transport_circuit_until_ns {0, 0, 0};
    std::array<std::uint32_t, 3> transport_fail_streak_by_symbol {0, 0, 0};
    const std::array<std::uint64_t, 3> min_send_ns_by_symbol {
        static_cast<std::uint64_t>(exec_min_send_interval_btc_us > 0 ? exec_min_send_interval_btc_us : 0) * 1000ULL,
        static_cast<std::uint64_t>(exec_min_send_interval_eth_us > 0 ? exec_min_send_interval_eth_us : 0) * 1000ULL,
        static_cast<std::uint64_t>(exec_min_send_interval_sol_us > 0 ? exec_min_send_interval_sol_us : 0) * 1000ULL,
    };
    std::uint64_t exec_reports = 0;
    std::uint64_t exec_fills = 0;
    std::uint64_t exec_acks = 0;
    std::uint64_t exec_rejects = 0;
    std::uint64_t exec_cancels = 0;
    std::uint64_t intent_generated = 0;
    std::uint64_t om_cmd_new = 0;
    std::uint64_t om_cmd_replace = 0;
    std::uint64_t om_cmd_cancel = 0;
    std::uint64_t risk_pass = 0;
    std::uint64_t gw_attempt = 0;
    std::uint64_t gw_ok = 0;
    std::uint64_t gw_fail_http = 0;
    std::uint64_t gw_fail_transport = 0;
    std::uint64_t gw_transport_retry = 0;
    std::uint64_t gw_transport_drop_after_retries = 0;
    std::uint64_t transport_cooldown_until_ns = 0;
    std::uint64_t transport_cooldown_active = 0;
    std::uint64_t transport_circuit_open = 0;
    std::uint64_t transport_circuit_blocked = 0;
    std::uint64_t ws_reconnects = 0;
    std::uint64_t ws_idle_reconnects = 0;
    std::uint64_t ws_last_msg_age_ms = 0;
    std::uint64_t ws_idle_reconnect_cooldown_until_ns = 0;
    std::uint64_t exec_exch_filter_reject = 0;
    std::uint64_t depth_out_of_sync = 0;
    std::uint64_t stale_skips = 0;
    std::uint64_t snapshot_applied = 0;
    std::uint64_t snapshot_failed = 0;
    std::uint64_t snapshot_req_drop = 0;
    std::uint64_t depth_buffered = 0;
    std::uint64_t depth_buffer_replayed = 0;
    std::uint64_t depth_buffer_drop = 0;
    // Cached sync/ready state to avoid scanning all books per event.
    std::array<std::uint8_t, 3> book_ready_cache {0, 0, 0};
    std::array<std::uint8_t, 3> book_sync_cache {0, 0, 0};
    std::uint8_t ready_symbols_cache = 0;
    std::uint8_t synced_symbols_cache = 0;
    std::uint64_t canary_rot_window_btc = 0;
    std::uint64_t canary_rot_window_eth = 0;
    std::uint64_t canary_rot_window_sol = 0;
    const std::uint64_t canary_rotation_window_ns = canary_rotation_window_ms > 0
        ? static_cast<std::uint64_t>(canary_rotation_window_ms) * 1000000ULL
        : 0ULL;
    const std::uint64_t canary_rotation_start_ns = now_ns();
    std::uint64_t queue_to_strategy_ns_sum = 0;
    std::uint64_t queue_to_strategy_ns_max = 0;
    std::uint64_t queue_to_strategy_count = 0;
    std::uint64_t q2s_stale_drop = 0;
    std::uint64_t lifecycle_overflow = 0;
    std::uint64_t lifecycle_timeout_audit_emitted = 0;
    std::array<std::uint64_t, 3> lifecycle_unresolved_by_symbol {0, 0, 0};
    std::array<std::uint64_t, 3> lifecycle_oldest_ms_by_symbol {0, 0, 0};
    std::array<std::uint64_t, 3> lifecycle_timeout_by_symbol {0, 0, 0};
    std::uint64_t trigger_gated = 0;
    std::uint64_t readiness_gated = 0;
    std::uint64_t pnl_dd_paused = 0;
    std::array<std::uint64_t, 3> pnl_dd_trips {0, 0, 0};
    std::array<std::uint64_t, 3> last_trigger_ns {0, 0, 0};
    std::array<double, 3> last_trigger_mid {0.0, 0.0, 0.0};
    std::array<double, 3> last_trigger_imb {0.0, 0.0, 0.0};
    LatencyWindow<8192> q2s_window;
    auto last_resync_scan = std::chrono::steady_clock::now();
    std::uint64_t last_reconcile_seq = 0;
    std::uint64_t reconcile_mismatch = 0;
    std::uint64_t reconcile_healed_oms = 0;
    std::uint64_t reconcile_healed_om = 0;
    std::uint64_t reconcile_healed_pending = 0;
    std::uint64_t reconcile_healed_pending_drop_om = 0;
    std::uint64_t reconcile_mismatch_streak = 0;
    std::uint64_t reconcile_mismatch_last_alert = 0;
    std::size_t reconcile_last_remote = 0;
    std::size_t reconcile_last_local = 0;
    std::uint64_t throttle_quote_suppressed = 0;
    std::array<std::uint64_t, 3> tradable_zero_since_ns {0, 0, 0};
    std::array<std::uint64_t, 3> tradable_zero_alerts {0, 0, 0};
    std::array<std::uint64_t, 3> tradable_zero_last_alert_ns {0, 0, 0};
    std::array<std::uint32_t, 3> adaptive_stale_ms {
        static_cast<std::uint32_t>(exec_cancel_stale_btc_ms > 0 ? exec_cancel_stale_btc_ms : 0),
        static_cast<std::uint32_t>(exec_cancel_stale_eth_ms > 0 ? exec_cancel_stale_eth_ms : 0),
        static_cast<std::uint32_t>(exec_cancel_stale_sol_ms > 0 ? exec_cancel_stale_sol_ms : 0),
    };
    std::array<std::uint32_t, 3> adaptive_adv_bps_x1000 {
        static_cast<std::uint32_t>(exec_adverse_cancel_btc_bps_x1000 > 0 ? exec_adverse_cancel_btc_bps_x1000 : 0),
        static_cast<std::uint32_t>(exec_adverse_cancel_eth_bps_x1000 > 0 ? exec_adverse_cancel_eth_bps_x1000 : 0),
        static_cast<std::uint32_t>(exec_adverse_cancel_sol_bps_x1000 > 0 ? exec_adverse_cancel_sol_bps_x1000 : 0),
    };
    std::array<std::uint64_t, 3> prev_cancel_stale {0, 0, 0};
    std::array<std::uint64_t, 3> prev_cancel_adv {0, 0, 0};
    std::array<std::uint64_t, 3> adaptive_updates {0, 0, 0};
    std::uint64_t prev_exec_cmd_sent = 0;
    Instrument last_consumed_instrument = Instrument::Unknown;
    std::array<std::uint8_t, 3> snapshot_fail_streak {0, 0, 0};
    std::array<std::uint64_t, 3> next_snapshot_req_ns {0, 0, 0};

    for (const auto instrument : instruments) {
        const std::size_t idx = instrument_index(instrument);
        snapshot_pending[idx].store(true, std::memory_order_relaxed);
        if (!snapshot_req_queue.push(SnapshotRequest{instrument})) {
            snapshot_pending[idx].store(false, std::memory_order_relaxed);
            ++snapshot_req_drop;
        }
    }

    auto store_last_intent_quote = [&](std::size_t sym_ix, const hft::QuoteIntent& quote) {
        LastIntentQuote& s = last_intent_quote_by_symbol[sym_ix];
        s.valid = true;
        s.has_bid = quote.bid.has_value();
        s.has_ask = quote.ask.has_value();
        if (s.has_bid) {
            s.bid_px = quote.bid->price;
            s.bid_qty = quote.bid->qty;
        }
        if (s.has_ask) {
            s.ask_px = quote.ask->price;
            s.ask_qty = quote.ask->qty;
        }
    };
    auto quote_matches_last_intent = [&](std::size_t sym_ix, const hft::QuoteIntent& q) -> bool {
        const LastIntentQuote& s = last_intent_quote_by_symbol[sym_ix];
        if (!s.valid) {
            return false;
        }
        if (s.has_bid != q.bid.has_value() || s.has_ask != q.ask.has_value()) {
            return false;
        }
        if (s.has_bid) {
            if (std::abs(q.bid->price - s.bid_px) > 1e-8) {
                return false;
            }
            if (std::abs(q.bid->qty - s.bid_qty) > 1e-12) {
                return false;
            }
        }
        if (s.has_ask) {
            if (std::abs(q.ask->price - s.ask_px) > 1e-8) {
                return false;
            }
            if (std::abs(q.ask->qty - s.ask_qty) > 1e-12) {
                return false;
            }
        }
        return true;
    };
    auto find_pair_slot = [&](std::uint64_t pair_id) -> PairTrack* {
        if (pair_id == 0) {
            return nullptr;
        }
        const std::size_t base = static_cast<std::size_t>(pair_id % kPairTrackCap);
        for (std::size_t probe = 0; probe < 8; ++probe) {
            PairTrack& s = pair_tracks[(base + probe) % kPairTrackCap];
            if (s.active && s.pair_id == pair_id) {
                return &s;
            }
        }
        return nullptr;
    };
    auto emit_pair_close_audit = [&](std::uint64_t intent_id,
        Instrument instrument,
        std::uint64_t ts_ns,
        const char* reason,
        double matched_qty,
        double pair_pnl,
        std::uint64_t open_coids,
        std::uint64_t sb_coid,
        std::uint64_t sa_coid,
        char bid_tag,
        char ask_tag,
        double bid_avg_fill,
        double bid_fill_qty,
        double ask_avg_fill,
        double ask_fill_qty) -> bool {
        if (!pair_audit_on) {
            return false;
        }
        const bool ok1 = exec_audit_line(
            pair_audit_on,
            pair_audit_log,
            pair_audit_drops,
            "ts_ns=%llu event=pair_close reason=%s intent=%llu sym=%s m=%.6f pnl=%.6f oc=%llu\n",
            static_cast<unsigned long long>(ts_ns),
            reason,
            static_cast<unsigned long long>(intent_id),
            instrument_name(instrument),
            matched_qty,
            pair_pnl,
            static_cast<unsigned long long>(open_coids));
        const bool ok2 = exec_audit_line(
            pair_audit_on,
            pair_audit_log,
            pair_audit_drops,
            "ts_ns=%llu event=pair_close_detail intent=%llu sym=%s sb=%llu sa=%llu b=%c@%.2f/%.6f a=%c@%.2f/%.6f\n",
            static_cast<unsigned long long>(ts_ns),
            static_cast<unsigned long long>(intent_id),
            instrument_name(instrument),
            static_cast<unsigned long long>(sb_coid),
            static_cast<unsigned long long>(sa_coid),
            static_cast<int>(static_cast<unsigned char>(bid_tag)),
            bid_avg_fill,
            bid_fill_qty,
            static_cast<int>(static_cast<unsigned char>(ask_tag)),
            ask_avg_fill,
            ask_fill_qty);
        if (!ok1 || !ok2) {
            return false;
        }
        ++pair_close_logged;
        return true;
    };
    auto emit_pair_record = [&](PairTrack& p, std::uint64_t ts_ns, const char* reason) {
        if (!pair_audit_on || !p.active || p.emitted) {
            return;
        }
        const double buy_avg_px = p.buy_qty > 0.0 ? (p.buy_notional / p.buy_qty) : 0.0;
        const double sell_avg_px = p.sell_qty > 0.0 ? (p.sell_notional / p.sell_qty) : 0.0;
        const double matched_qty = std::min(p.buy_qty, p.sell_qty);
        const double pair_pnl = matched_qty > 0.0 ? ((sell_avg_px - buy_avg_px) * matched_qty) : 0.0;
        const double bid_avg_fill = p.bid_fill_qty > 0.0 ? (p.bid_fill_notional / p.bid_fill_qty) : 0.0;
        const double ask_avg_fill = p.ask_fill_qty > 0.0 ? (p.ask_fill_notional / p.ask_fill_qty) : 0.0;
        const char bid_c = !p.intent_bid ? '-' : (p.bid_term_tag != '\0' ? p.bid_term_tag : '?');
        const char ask_c = !p.intent_ask ? '-' : (p.ask_term_tag != '\0' ? p.ask_term_tag : '?');
        const bool ok = emit_pair_close_audit(
            p.pair_id,
            p.instrument,
            ts_ns,
            reason,
            matched_qty,
            pair_pnl,
            p.open_coids,
            p.bid_last_coid,
            p.ask_last_coid,
            bid_c,
            ask_c,
            bid_avg_fill,
            p.bid_fill_qty,
            ask_avg_fill,
            p.ask_fill_qty);
        if (!ok) {
            return;
        }
        p.emitted = true;
    };
    auto try_finalize_pair = [&](PairTrack& p, std::uint64_t ts_ns, const char* reason) {
        if (!p.active || p.emitted) {
            return;
        }
        const bool bid_done = !p.intent_bid || p.bid_terminal;
        const bool ask_done = !p.intent_ask || p.ask_terminal;
        if (p.open_coids == 0 && bid_done && ask_done) {
            emit_pair_record(p, ts_ns, reason);
        }
    };
    auto upsert_coid_pair = [&](std::uint64_t coid, std::uint64_t pair_id, Side side) -> CoidPairRef* {
        if (coid == 0 || pair_id == 0) {
            return nullptr;
        }
        const std::size_t base = static_cast<std::size_t>(coid % kPairCoidCap);
        for (std::size_t probe = 0; probe < 8; ++probe) {
            CoidPairRef& ref = pair_by_coid[(base + probe) % kPairCoidCap];
            if (!ref.active || ref.client_order_id == coid) {
                ref.active = true;
                ref.client_order_id = coid;
                ref.pair_id = pair_id;
                ref.side = side;
                return &ref;
            }
        }
        ++pair_coid_overflow;
        return nullptr;
    };
    auto find_coid_pair = [&](std::uint64_t coid) -> CoidPairRef* {
        if (coid == 0) {
            return nullptr;
        }
        const std::size_t base = static_cast<std::size_t>(coid % kPairCoidCap);
        for (std::size_t probe = 0; probe < 8; ++probe) {
            CoidPairRef& ref = pair_by_coid[(base + probe) % kPairCoidCap];
            if (ref.active && ref.client_order_id == coid) {
                return &ref;
            }
        }
        return nullptr;
    };
    auto supersede_open_pairs_for_symbol = [&](Instrument instrument, std::uint64_t ts_ns) {
        for (PairTrack& p : pair_tracks) {
            if (!p.active || p.emitted || p.instrument != instrument) {
                continue;
            }
            emit_pair_record(p, ts_ns, "superseded");
        }
    };
    auto begin_pair_from_intent = [&](Instrument instrument, const hft::QuoteIntent& quote, std::uint64_t ts_ns) {
        if (!quote.bid.has_value() && !quote.ask.has_value()) {
            return;
        }
        supersede_open_pairs_for_symbol(instrument, ts_ns);
        const std::uint64_t pair_id = ++pair_seq;
        const std::size_t base = static_cast<std::size_t>(pair_id % kPairTrackCap);
        PairTrack* slot = nullptr;
        for (std::size_t probe = 0; probe < 8; ++probe) {
            PairTrack& s = pair_tracks[(base + probe) % kPairTrackCap];
            // Do not reuse a slot while client orders may still resolve to this pair_id.
            if (!s.active || s.open_coids == 0) {
                slot = &s;
                break;
            }
        }
        if (slot == nullptr) {
            ++pair_track_overflow;
            return;
        }
        *slot = PairTrack {};
        slot->active = true;
        slot->pair_id = pair_id;
        slot->instrument = instrument;
        slot->ts_intent_ns = ts_ns;
        slot->ts_last_event_ns = ts_ns;
        slot->intent_bid = quote.bid.has_value();
        slot->intent_ask = quote.ask.has_value();
        if (quote.bid.has_value()) {
            slot->intent_bid_px = quote.bid->price;
            slot->intent_bid_qty = quote.bid->qty;
        }
        if (quote.ask.has_value()) {
            slot->intent_ask_px = quote.ask->price;
            slot->intent_ask_qty = quote.ask->qty;
        }
        current_pair_id_by_symbol[instrument_index(instrument)] = pair_id;
        if (pair_audit_on) {
            const bool logged = exec_audit_line(
                pair_audit_on,
                pair_audit_log,
                pair_audit_drops,
                "ts_ns=%llu event=pair_intent intent=%llu sym=%s ib=%u@%.2f/%.6f ia=%u@%.2f/%.6f\n",
                static_cast<unsigned long long>(ts_ns),
                static_cast<unsigned long long>(pair_id),
                instrument_name(instrument),
                slot->intent_bid ? 1U : 0U,
                slot->intent_bid_px,
                slot->intent_bid_qty,
                slot->intent_ask ? 1U : 0U,
                slot->intent_ask_px,
                slot->intent_ask_qty);
            if (logged) {
                ++pair_intent_logged;
            }
        }
        store_last_intent_quote(instrument_index(instrument), quote);
    };
    auto mark_pair_local_terminal = [&](std::uint64_t coid, std::uint64_t ts_ns, const char* reason, char tag = 'H') {
        CoidPairRef* ref = find_coid_pair(coid);
        if (ref == nullptr) {
            return;
        }
        PairTrack* p = find_pair_slot(ref->pair_id);
        if (p == nullptr) {
            ref->active = false;
            return;
        }
        p->ts_last_event_ns = ts_ns;
        if (ref->active) {
            ref->active = false;
            if (p->open_coids > 0) {
                --p->open_coids;
            }
        }
        if (ref->side == Side::Buy) {
            p->bid_terminal = true;
            if (p->bid_term_tag == '\0') {
                p->bid_term_tag = tag;
            }
        } else {
            p->ask_terminal = true;
            if (p->ask_term_tag == '\0') {
                p->ask_term_tag = tag;
            }
        }
        try_finalize_pair(*p, ts_ns, reason);
    };

    while (!g_stop.load(std::memory_order_relaxed)) {
        bool had_work = false;

        const std::uint64_t rseq = g_reconcile_seq.load(std::memory_order_acquire);
        if (rseq != last_reconcile_seq) {
            last_reconcile_seq = rseq;
            const std::uint32_t remote_ids_n_u32 =
                g_reconcile_remote_ids_count.load(std::memory_order_acquire);
            const std::size_t remote_ids_n = std::min<std::size_t>(remote_ids_n_u32, kReconcileMaxIds);
            const std::size_t remote = remote_ids_n;
            std::array<std::uint64_t, kReconcileMaxIds> remote_ids {};
            for (std::size_t i = 0; i < remote_ids_n; ++i) {
                remote_ids[i] = g_reconcile_remote_ids[i].load(std::memory_order_relaxed);
            }
            if (oms_pending_timeout_ms > 0 && oms_pending_heal_max_per_tick > 0) {
                const std::uint64_t heal_ts = now_ns();
                const std::size_t max_pending_heal = static_cast<std::size_t>(oms_pending_heal_max_per_tick);
                std::array<std::uint64_t, 32> terminalized_ids {};
                const std::size_t healed_pending = oms.reconcile_heal_stuck_pending(
                    remote_ids.data(),
                    remote_ids_n,
                    static_cast<std::uint64_t>(oms_pending_timeout_ms) * 1000000ULL,
                    max_pending_heal,
                    heal_ts,
                    terminalized_ids.data(),
                    terminalized_ids.size());
                if (healed_pending > 0) {
                    reconcile_healed_pending += static_cast<std::uint64_t>(healed_pending);
                    std::size_t dropped_om = 0;
                    const std::size_t terminalized_n = std::min<std::size_t>(healed_pending, terminalized_ids.size());
                    for (std::size_t i = 0; i < terminalized_n; ++i) {
                        if (terminalized_ids[i] == 0) {
                            continue;
                        }
                        if (order_manager.reconcile_drop_client_order_id(terminalized_ids[i])) {
                            ++dropped_om;
                        }
                        const std::size_t li =
                            static_cast<std::size_t>(terminalized_ids[i] % kLifecycleCap);
                        auto& e = lifecycle[li];
                        if (e.active && e.client_order_id == terminalized_ids[i]) {
                            exec_audit_line(
                                exec_audit_on,
                                exec_audit,
                                exec_audit_drops,
                                "ts_ns=%llu event=lifecycle_disposition sym=%s coid=%llu action=reconcile_pending_terminalize",
                                static_cast<unsigned long long>(heal_ts),
                                instrument_name(e.instrument),
                                static_cast<unsigned long long>(e.client_order_id));
                            e.active = false;
                            e.timeout_reported = false;
                        }
                        mark_pair_local_terminal(terminalized_ids[i], heal_ts, "reconcile_pending");
                    }
                    reconcile_healed_pending_drop_om += static_cast<std::uint64_t>(dropped_om);
                    exec_audit_line(
                        exec_audit_on,
                        exec_audit,
                        exec_audit_drops,
                        "ts_ns=%llu event=reconcile_pending_heal healed=%llu om_drop=%llu timeout_ms=%d",
                        static_cast<unsigned long long>(heal_ts),
                        static_cast<unsigned long long>(healed_pending),
                        static_cast<unsigned long long>(dropped_om),
                        oms_pending_timeout_ms);
                }
            }
            const std::size_t local_open_est = oms.reconcile_open_orders_estimate();
            if (remote != local_open_est) {
                ++reconcile_mismatch;
                if (remote == reconcile_last_remote && local_open_est == reconcile_last_local) {
                    ++reconcile_mismatch_streak;
                } else {
                    reconcile_mismatch_streak = 1;
                }
                reconcile_last_remote = remote;
                reconcile_last_local = local_open_est;
                if (rec_mismatch_alert_step > 0 &&
                    reconcile_mismatch >= reconcile_mismatch_last_alert + static_cast<std::uint64_t>(rec_mismatch_alert_step)) {
                    reconcile_mismatch_last_alert = reconcile_mismatch;
                    exec_audit_line(
                        md_health_on,
                        md_health_log,
                        md_health_drops,
                        "ts_ns=%llu event=alert_reconcile_mismatch mismatch=%llu streak=%llu remote=%llu local=%llu",
                        static_cast<unsigned long long>(now_ns()),
                        static_cast<unsigned long long>(reconcile_mismatch),
                        static_cast<unsigned long long>(reconcile_mismatch_streak),
                        static_cast<unsigned long long>(remote),
                        static_cast<unsigned long long>(local_open_est));
                }
                if (reconcile_heal && reconcile_heal_max_per_tick > 0 && reconcile_mismatch_streak >= 2) {
                    const std::size_t max_heal = static_cast<std::size_t>(reconcile_heal_max_per_tick);
                    const std::uint64_t heal_ts = now_ns();
                    const std::size_t healed_om = order_manager.reconcile_drop_missing_live(
                        remote_ids.data(), remote_ids_n, max_heal);
                    const std::size_t healed_oms = oms.reconcile_mark_missing_completed(
                        remote_ids.data(), remote_ids_n, max_heal, heal_ts);
                    std::size_t healed_lifecycle = 0;
                    auto in_remote = [&](std::uint64_t id) {
                        for (std::size_t i = 0; i < remote_ids_n; ++i) {
                            if (remote_ids[i] == id) {
                                return true;
                            }
                        }
                        return false;
                    };
                    for (auto& e : lifecycle) {
                        if (healed_lifecycle >= max_heal) {
                            break;
                        }
                        if (!e.active || e.client_order_id == 0 || in_remote(e.client_order_id)) {
                            continue;
                        }
                        exec_audit_line(
                            exec_audit_on,
                            exec_audit,
                            exec_audit_drops,
                            "ts_ns=%llu event=lifecycle_disposition sym=%s coid=%llu action=reconcile_missing_terminalize",
                            static_cast<unsigned long long>(heal_ts),
                            instrument_name(e.instrument),
                            static_cast<unsigned long long>(e.client_order_id));
                        mark_pair_local_terminal(e.client_order_id, heal_ts, "reconcile_missing");
                        e.active = false;
                        e.timeout_reported = false;
                        ++healed_lifecycle;
                    }
                    reconcile_healed_om += static_cast<std::uint64_t>(healed_om);
                    reconcile_healed_oms += static_cast<std::uint64_t>(healed_oms);
                    if (healed_om > 0 || healed_oms > 0 || healed_lifecycle > 0) {
                        exec_audit_line(
                            exec_audit_on,
                            exec_audit,
                            exec_audit_drops,
                            "ts_ns=%llu event=reconcile_heal remote=%llu local=%llu om=%llu oms=%llu lc=%llu",
                            static_cast<unsigned long long>(heal_ts),
                            static_cast<unsigned long long>(remote),
                            static_cast<unsigned long long>(local_open_est),
                            static_cast<unsigned long long>(healed_om),
                            static_cast<unsigned long long>(healed_oms),
                            static_cast<unsigned long long>(healed_lifecycle));
                    }
                }
            } else {
                reconcile_mismatch_streak = 0;
                reconcile_last_remote = remote;
                reconcile_last_local = local_open_est;
            }
        }

        SnapshotResult snapshot_res;
        while (snapshot_res_queue.pop(snapshot_res)) {
            had_work = true;
            const std::size_t idx = instrument_index(snapshot_res.instrument);
            snapshot_pending[idx].store(false, std::memory_order_relaxed);
            if (snapshot_res.success && books[idx].seed_from_snapshot(snapshot_res.snapshot)) {
                ++snapshot_applied;
                snapshot_fail_streak[idx] = 0;
                next_snapshot_req_ns[idx] = 0;
                // Update cached ready/sync state (snapshot seed often flips both).
                const std::uint8_t now_ready = books[idx].is_ready() ? 1U : 0U;
                const std::uint8_t now_sync = books[idx].is_in_sync() ? 1U : 0U;
                if (now_ready != book_ready_cache[idx]) {
                    ready_symbols_cache = static_cast<std::uint8_t>(
                        ready_symbols_cache + (now_ready ? 1U : 0U) - (book_ready_cache[idx] ? 1U : 0U));
                    book_ready_cache[idx] = now_ready;
                }
                if (now_sync != book_sync_cache[idx]) {
                    synced_symbols_cache = static_cast<std::uint8_t>(
                        synced_symbols_cache + (now_sync ? 1U : 0U) - (book_sync_cache[idx] ? 1U : 0U));
                    book_sync_cache[idx] = now_sync;
                }
                pending_depth[idx].replay([&](const MdEvent& e) {
                    ++depth_buffer_replayed;
                    const auto replay_result = books[idx].apply(e);
                    if (replay_result == hft::orderbook::ApplyResult::OutOfSync) {
                        ++depth_out_of_sync;
                    }
                });
            } else {
                ++snapshot_failed;
                if (snapshot_fail_streak[idx] < 16) {
                    ++snapshot_fail_streak[idx];
                }
                const std::uint32_t shift = snapshot_fail_streak[idx] > 6 ? 6U : snapshot_fail_streak[idx];
                const std::uint64_t base_ms =
                    static_cast<std::uint64_t>(snapshot_retry_backoff_ms > 0 ? snapshot_retry_backoff_ms : 20);
                std::uint64_t backoff_ms = base_ms << shift;
                const std::uint64_t max_ms =
                    static_cast<std::uint64_t>(snapshot_retry_max_backoff_ms > 0 ? snapshot_retry_max_backoff_ms : 1000);
                if (backoff_ms > max_ms) {
                    backoff_ms = max_ms;
                }
                next_snapshot_req_ns[idx] = now_ns() + backoff_ms * 1000000ULL;
                pending_depth[idx].clear();
            }
        }

        const std::size_t exec_batch_max =
            static_cast<std::size_t>(exec_report_batch_max > 0 ? exec_report_batch_max : 512);
        ExecReportMsg exec_msg;
        auto process_exec_report = [&](const ExecReportMsg& msg) {
            CoidPairRef* pair_ref = find_coid_pair(msg.report.client_order_id);
            PairTrack* pair_slot = (pair_ref != nullptr) ? find_pair_slot(pair_ref->pair_id) : nullptr;
            ++exec_reports;
            if (msg.report.type == ExecEventType::Ack) {
                ++exec_acks;
            } else if (msg.report.type == ExecEventType::Reject) {
                ++exec_rejects;
            } else if (msg.report.type == ExecEventType::Canceled) {
                ++exec_cancels;
            }
            oms.on_exec_report(msg.report, msg.ts_ns);
            order_manager.on_exec_report(msg.report);
            if (msg.report.client_order_id > 0) {
                const std::size_t li = static_cast<std::size_t>(msg.report.client_order_id % kLifecycleCap);
                auto& e = lifecycle[li];
                const bool terminal = msg.report.type == ExecEventType::Reject ||
                    msg.report.type == ExecEventType::Canceled || msg.report.terminal;
                if (terminal && e.active && e.client_order_id == msg.report.client_order_id) {
                    e.active = false;
                    e.timeout_reported = false;
                }
            }
            if (msg.report.type == ExecEventType::Fill && msg.report.last_fill_qty > 0.0) {
                ++exec_fills;
                const double signed_qty = msg.report.side == Side::Buy
                    ? msg.report.last_fill_qty
                    : -msg.report.last_fill_qty;
                risk.on_fill(msg.report.instrument, signed_qty);
                hft::Fill fill {};
                fill.side = msg.report.side;
                fill.price = msg.report.last_fill_price;
                fill.qty = msg.report.last_fill_qty;
                fill.ts_ns = msg.ts_ns;
                fill.is_maker = msg.report.is_maker;
                pnl_engine.on_fill(fill, pnl_states[instrument_index(msg.report.instrument)]);
                if (pair_slot != nullptr) {
                    ++pair_slot->fills;
                    pair_slot->ts_last_event_ns = msg.ts_ns;
                    if (msg.report.side == Side::Buy) {
                        pair_slot->buy_qty += msg.report.last_fill_qty;
                        pair_slot->buy_notional += msg.report.last_fill_qty * msg.report.last_fill_price;
                        pair_slot->bid_fill_qty += msg.report.last_fill_qty;
                        pair_slot->bid_fill_notional += msg.report.last_fill_qty * msg.report.last_fill_price;
                    } else {
                        pair_slot->sell_qty += msg.report.last_fill_qty;
                        pair_slot->sell_notional += msg.report.last_fill_qty * msg.report.last_fill_price;
                        pair_slot->ask_fill_qty += msg.report.last_fill_qty;
                        pair_slot->ask_fill_notional += msg.report.last_fill_qty * msg.report.last_fill_price;
                    }
                }
            }
            const bool pair_terminal = msg.report.type == ExecEventType::Reject ||
                msg.report.type == ExecEventType::Canceled || msg.report.terminal;
            if (pair_slot != nullptr && pair_terminal) {
                pair_slot->ts_last_event_ns = msg.ts_ns;
                if (msg.report.type == ExecEventType::Reject) {
                    ++pair_slot->rejects;
                }
                if (pair_ref != nullptr && pair_ref->active) {
                    pair_ref->active = false;
                    if (pair_slot->open_coids > 0) {
                        --pair_slot->open_coids;
                    }
                }
                if (pair_ref != nullptr) {
                    char tag = 'F';
                    if (msg.report.type == ExecEventType::Reject) {
                        tag = 'R';
                    } else if (msg.report.type == ExecEventType::Canceled) {
                        tag = 'C';
                    }
                    if (pair_ref->side == Side::Buy) {
                        pair_slot->bid_terminal = true;
                        if (pair_slot->bid_term_tag == '\0') {
                            pair_slot->bid_term_tag = tag;
                        }
                    } else {
                        pair_slot->ask_terminal = true;
                        if (pair_slot->ask_term_tag == '\0') {
                            pair_slot->ask_term_tag = tag;
                        }
                    }
                }
                try_finalize_pair(*pair_slot, msg.ts_ns, "terminal");
            }
            const bool audit_drop_copy = msg.report.type == ExecEventType::Fill ||
                msg.report.type == ExecEventType::Reject || msg.report.type == ExecEventType::Canceled ||
                msg.report.terminal;
            if (audit_drop_copy) {
                exec_audit_line(
                    exec_audit_on,
                    exec_audit,
                    exec_audit_drops,
                    "ts_ns=%llu event=drop_copy kind=%u sym=%s coid=%llu term=%u",
                    static_cast<unsigned long long>(msg.ts_ns),
                    static_cast<unsigned>(msg.report.type),
                    instrument_name(msg.report.instrument),
                    static_cast<unsigned long long>(msg.report.client_order_id),
                    msg.report.terminal ? 1U : 0U);
            }
        };
        for (std::size_t exec_n = 0; exec_n < exec_batch_max && exec_report_queue.pop(exec_msg); ++exec_n) {
            had_work = true;
            process_exec_report(exec_msg);
        }

        const std::size_t md_batch_max =
            static_cast<std::size_t>(main_md_batch_max > 0 ? main_md_batch_max : 256);
        for (std::size_t md_n = 0; md_n < md_batch_max && queue.pop(event); ++md_n) {
            had_work = true;
            ++consumed;
            last_consumed_instrument = event.instrument;
            const std::size_t idx = instrument_index(event.instrument);
            if (event.type == MdEventType::DepthUpdate && books[idx].needs_snapshot_seed()) {
                pending_depth[idx].push(event);
                ++depth_buffered;
                depth_buffer_drop += pending_depth[idx].drops;
                pending_depth[idx].drops = 0;
            }
            const std::uint8_t was_ready = book_ready_cache[idx];
            const std::uint8_t was_sync = book_sync_cache[idx];
            const auto apply_result = books[idx].apply(event);
            if (apply_result == hft::orderbook::ApplyResult::OutOfSync) {
                ++depth_out_of_sync;
                pending_depth[idx].clear();
            }
            const std::uint8_t now_ready = books[idx].is_ready() ? 1U : 0U;
            const std::uint8_t now_sync = books[idx].is_in_sync() ? 1U : 0U;
            if (now_ready != was_ready) {
                ready_symbols_cache = static_cast<std::uint8_t>(
                    ready_symbols_cache + (now_ready ? 1U : 0U) - (was_ready ? 1U : 0U));
                book_ready_cache[idx] = now_ready;
            }
            if (now_sync != was_sync) {
                synced_symbols_cache = static_cast<std::uint8_t>(
                    synced_symbols_cache + (now_sync ? 1U : 0U) - (was_sync ? 1U : 0U));
                book_sync_cache[idx] = now_sync;
            }

            const bool all_symbols_synced = (synced_symbols_cache == static_cast<std::uint8_t>(instruments.size()));
            const bool global_trading_ready = !require_all_symbols_sync || all_symbols_synced;
            const bool symbol_tradable = (book_ready_cache[idx] != 0) && (book_sync_cache[idx] != 0) &&
                (global_trading_ready || allow_partial_trading);

            if (symbol_tradable &&
                (event.type == MdEventType::BookTicker || event.type == MdEventType::DepthUpdate)) {
                const auto snap = books[idx].snapshot();
                const double mid = 0.5 * (snap.best_bid + snap.best_ask);
                mid_px_by_symbol[idx] = mid;
                const std::uint64_t now_trigger_ns = now_ns();
                bool pass_gate = false;
                if (last_trigger_ns[idx] == 0) {
                    pass_gate = true;
                } else {
                    const std::uint64_t dt_ns = now_trigger_ns - last_trigger_ns[idx];
                    const bool interval_ok = dt_ns >= static_cast<std::uint64_t>(trigger_min_interval_us) * 1000ULL;
                    const double mid_prev = last_trigger_mid[idx];
                    const double mid_abs_diff = std::abs(mid - mid_prev);
                    const double mid_bps_x1000 = mid_prev > 0.0 ? (mid_abs_diff / mid_prev) * 1.0e7 : 0.0;
                    const bool mid_ok = mid_bps_x1000 >= static_cast<double>(trigger_bps_x1000);
                    const double imb_abs_diff = std::abs(snap.imbalance - last_trigger_imb[idx]);
                    const bool imb_ok = (imb_abs_diff * 1.0e6) >= static_cast<double>(trigger_imbalance_ppm);
                    pass_gate = interval_ok || mid_ok || imb_ok;
                }

                if (pass_gate) {
                    last_trigger_ns[idx] = now_trigger_ns;
                    last_trigger_mid[idx] = mid;
                    last_trigger_imb[idx] = snap.imbalance;
                    if (snap.best_ask <= snap.best_bid || snap.best_bid <= 0.0 || snap.best_ask <= 0.0) {
                        ++stale_skips;
                        continue;
                    }

                    if (pnl_drawdown_guard) {
                        const double pnl_now = mid > 0.0
                            ? pnl_engine.mark_to_market(pnl_states[idx], mid)
                            : (pnl_states[idx].realized - pnl_states[idx].fees_paid);
                        if (pnl_now > pnl_peak_by_symbol[idx]) {
                            pnl_peak_by_symbol[idx] = pnl_now;
                        }
                        if (pnl_pause_until_ns[idx] > now_trigger_ns) {
                            ++pnl_dd_paused;
                            continue;
                        }
                        const double dd_limit = pnl_max_dd_by_symbol[idx];
                        if (dd_limit > 0.0 && (pnl_peak_by_symbol[idx] - pnl_now) >= dd_limit) {
                            const double peak_before_trip = pnl_peak_by_symbol[idx];
                            ++pnl_dd_trips[idx];
                            const std::uint64_t cool_ns = pnl_cooldown_ns_by_symbol[idx];
                            pnl_pause_until_ns[idx] =
                                cool_ns > 0 ? (now_trigger_ns + cool_ns) : now_trigger_ns;
                            pnl_peak_by_symbol[idx] = pnl_now;
                            exec_audit_line(
                                exec_audit_on,
                                exec_audit,
                                exec_audit_drops,
                                "ts_ns=%llu event=pnl_dd_guard sym=%s pnl=%.6f peak=%.6f dd_lim=%.6f",
                                static_cast<unsigned long long>(now_trigger_ns),
                                instrument_name(event.instrument),
                                pnl_now,
                                peak_before_trip,
                                dd_limit);
                            ++pnl_dd_paused;
                            continue;
                        }
                    }

                    strategy_states[idx].inventory = risk.position(event.instrument);
                    auto quote = strategy_engines[idx].on_book_update(snap, strategy_states[idx]);
                    // Avoid repeated exchange filter rejects by suppressing legs
                    // that cannot satisfy symbol min-notional.
                    const double min_notional_symbol = exch_min_notional_by_symbol[idx];
                    auto suppress_below_min_notional = [&](std::optional<hft::OrderIntent>& leg) {
                        if (!leg.has_value()) {
                            return;
                        }
                        const double notional = std::abs(leg->price * leg->qty);
                        if (notional > 0.0 && notional < min_notional_symbol) {
                            leg.reset();
                        }
                    };
                    suppress_below_min_notional(quote.bid);
                    suppress_below_min_notional(quote.ask);
                    const std::uint64_t strategy_ts = now_ns();
                    if (event.ts_enqueued_ns > 0 && strategy_ts > event.ts_enqueued_ns) {
                        const std::uint64_t q2s_ns = strategy_ts - event.ts_enqueued_ns;
                        if (q2s_stale_drop_ms > 0 &&
                            q2s_ns > static_cast<std::uint64_t>(q2s_stale_drop_ms) * 1000000ULL) {
                            ++q2s_stale_drop;
                            continue;
                        }
                        queue_to_strategy_ns_sum += q2s_ns;
                        ++queue_to_strategy_count;
                        q2s_window.add(q2s_ns);
                        if (q2s_ns > queue_to_strategy_ns_max) {
                            queue_to_strategy_ns_max = q2s_ns;
                        }
                    }
                    if (quote.bid.has_value() || quote.ask.has_value() ||
                        order_manager.active_orders(event.instrument) > 0) {
                        const std::uint64_t now_gate_ns = now_ns();
                        const bool rest_cooldown_active =
                            rest_cooldown_until_ns > now_gate_ns && rest_throttle_cooldown_ms > 0;
                        const bool rest_soft_limit_active =
                            rest_weight_soft_limit > 0 &&
                            g_rest_weight_1m.load(std::memory_order_relaxed) >= rest_weight_soft_limit;
                        const bool transport_cooldown_on =
                            transport_cooldown_until_ns > now_gate_ns && transport_cooldown_ms > 0;
                        const bool transport_circuit_on = transport_circuit_until_ns[idx] > now_gate_ns;
                        if (rest_cooldown_active || rest_soft_limit_active || transport_cooldown_on || transport_circuit_on) {
                            ++throttle_quote_suppressed;
                            continue;
                        }
                        ++strategy_signals;
                        if (quote.bid.has_value() || quote.ask.has_value()) {
                            ++intent_generated;
                            const std::size_t sym_ix_q = instrument_index(event.instrument);
                            if (pair_audit_on && quote_matches_last_intent(sym_ix_q, quote)) {
                                const std::uint64_t intent_id = current_pair_id_by_symbol[sym_ix_q];
                                if (intent_id != 0) {
                                    // "Unchanged" does not create a new PairTrack; keep `pair_intent`
                                    // reserved for real pair starts only.
                                    emit_pair_close_audit(
                                        intent_id,
                                        event.instrument,
                                        strategy_ts,
                                        "unchanged",
                                        0.0,
                                        0.0,
                                        0ULL,
                                        0ULL,
                                        0ULL,
                                        'U',
                                        'U',
                                        0.0,
                                        0.0,
                                        0.0,
                                        0.0);
                                    store_last_intent_quote(sym_ix_q, quote);
                                } else {
                                    begin_pair_from_intent(event.instrument, quote, strategy_ts);
                                }
                            } else {
                                begin_pair_from_intent(event.instrument, quote, strategy_ts);
                            }
                        }
                        const auto cmd = order_manager.on_quote(event.instrument, quote, strategy_ts);
                        if (cmd.has_value()) {
                            if (cmd->type == CommandType::New) {
                                ++om_cmd_new;
                            } else if (cmd->type == CommandType::Replace) {
                                ++om_cmd_replace;
                            } else if (cmd->type == CommandType::Cancel) {
                                ++om_cmd_cancel;
                            }
                            const auto risk_result = risk.validate(*cmd);
                            if (risk_result != RiskRejectReason::None) {
                                ++exec_cmd_rejected_risk;
                                if (risk_result == RiskRejectReason::KillSwitchEngaged) {
                                    ++exec_risk_kill;
                                }
                                exec_audit_line(
                                    exec_audit_on,
                                    exec_audit,
                                    exec_audit_drops,
                                    "ts_ns=%llu event=risk_reject sym=%s coid=%llu reason=%u",
                                    static_cast<unsigned long long>(strategy_ts),
                                    instrument_name(event.instrument),
                                    static_cast<unsigned long long>(cmd->client_order_id),
                                    static_cast<unsigned>(risk_result));
                                order_manager.on_command_rejected(*cmd);
                                oms.on_command_rejected(*cmd);
                                continue;
                            }
                            ++risk_pass;
                            auto send_cmd = *cmd;
                            if (canary_fill_mode && send_cmd.type == CommandType::New && send_cmd.price > 0.0) {
                                bool apply_canary_to_symbol = true;
                                if (canary_rotate_symbols && canary_rotation_window_ns > 0) {
                                    const std::uint64_t now_canary_ns = now_ns();
                                    const std::uint64_t elapsed_ns = now_canary_ns > canary_rotation_start_ns
                                        ? (now_canary_ns - canary_rotation_start_ns)
                                        : 0ULL;
                                    const std::size_t active_slot = static_cast<std::size_t>(
                                        (elapsed_ns / canary_rotation_window_ns) % instruments.size());
                                    apply_canary_to_symbol = (idx == active_slot);
                                    if (active_slot == 0) {
                                        ++canary_rot_window_btc;
                                    } else if (active_slot == 1) {
                                        ++canary_rot_window_eth;
                                    } else {
                                        ++canary_rot_window_sol;
                                    }
                                }
                                if (apply_canary_to_symbol) {
                                    const double cross = canary_fill_cross_bps / 10000.0;
                                    if (cross > 0.0) {
                                        send_cmd.price = send_cmd.side == Side::Buy
                                            ? send_cmd.price * (1.0 + cross)
                                            : send_cmd.price * (1.0 - cross);
                                    }
                                }
                            }
                            const double send_notional = std::abs(send_cmd.price * send_cmd.qty);
                            const double min_notional_symbol = exch_min_notional_by_symbol[idx];
                            if (send_notional > 0.0 && send_notional < min_notional_symbol) {
                                ++exec_exch_filter_reject;
                                order_manager.on_command_rejected(*cmd);
                                oms.on_command_rejected(*cmd);
                                exec_audit_line(
                                    exec_audit_on,
                                    exec_audit,
                                    exec_audit_drops,
                                    "ts_ns=%llu event=exch_filter_reject sym=%s coid=%llu notional=%.6f min_notional=%.6f",
                                    static_cast<unsigned long long>(strategy_ts),
                                    instrument_name(event.instrument),
                                    static_cast<unsigned long long>(cmd->client_order_id),
                                    send_notional,
                                    min_notional_symbol);
                                continue;
                            }
                            const std::uint64_t now_send_ns = now_ns();
                            const std::uint64_t min_send_ns = min_send_ns_by_symbol[idx];
                            const bool paced_active =
                                min_send_ns > 0 && last_send_ns_by_symbol[idx] > 0 &&
                                now_send_ns - last_send_ns_by_symbol[idx] < min_send_ns;
                            const bool cooldown_active =
                                rest_cooldown_until_ns > now_send_ns && rest_throttle_cooldown_ms > 0;
                            const bool over_soft_limit =
                                rest_weight_soft_limit > 0 &&
                                g_rest_weight_1m.load(std::memory_order_relaxed) >= rest_weight_soft_limit;
                            if (paced_active || cooldown_active || over_soft_limit) {
                                if (paced_active) {
                                    ++exec_cmd_paced;
                                }
                                ++exec_cmd_rate_limited;
                                order_manager.on_command_rejected(*cmd);
                                oms.on_command_rejected(*cmd);
                                exec_audit_line(
                                    exec_audit_on,
                                    exec_audit,
                                    exec_audit_drops,
                                    "ts_ns=%llu event=rate_limit_gate sym=%s coid=%llu wt=%d cool=%u",
                                    static_cast<unsigned long long>(strategy_ts),
                                    instrument_name(event.instrument),
                                    static_cast<unsigned long long>(cmd->client_order_id),
                                    g_rest_weight_1m.load(std::memory_order_relaxed),
                                    cooldown_active ? 1U : 0U);
                                continue;
                            }
                            const bool transport_cooldown_on =
                                transport_cooldown_until_ns > now_send_ns && transport_cooldown_ms > 0;
                            if (transport_cooldown_on) {
                                ++transport_cooldown_active;
                                ++exec_cmd_rate_limited;
                                order_manager.on_command_rejected(*cmd);
                                oms.on_command_rejected(*cmd);
                                continue;
                            }
                            if (transport_circuit_until_ns[idx] > now_send_ns) {
                                ++transport_circuit_blocked;
                                ++exec_cmd_rate_limited;
                                order_manager.on_command_rejected(*cmd);
                                oms.on_command_rejected(*cmd);
                                continue;
                            }
                            oms.on_command_sent(*cmd);
                            ++gw_attempt;
                            hft::execution::GatewaySendResult gr {};
                            const std::uint32_t tx_attempts =
                                static_cast<std::uint32_t>(transport_retry_attempts > 0 ? transport_retry_attempts : 1);
                            for (std::uint32_t tx = 0; tx < tx_attempts; ++tx) {
                                gr = gateway.send(send_cmd);
                                if (gr.ok || gr.http_status > 0) {
                                    break;
                                }
                                if (tx + 1 < tx_attempts) {
                                    ++gw_transport_retry;
                                    if (transport_retry_backoff_ms > 0) {
                                        std::this_thread::sleep_for(
                                            std::chrono::milliseconds(transport_retry_backoff_ms));
                                    }
                                } else {
                                    ++gw_transport_drop_after_retries;
                                    if (transport_cooldown_ms > 0) {
                                        transport_cooldown_until_ns =
                                            now_send_ns + static_cast<std::uint64_t>(transport_cooldown_ms) * 1000000ULL;
                                    }
                                }
                            }
                            if (!gr.ok) {
                                ++exec_cmd_gateway_fail;
                                gw_fail_last_http = gr.http_status;
                                gw_fail_last_ix = gr.binance_error_code;
                                const bool uncertain_transport = (gr.http_status <= 0);
                                if (gr.http_status > 0) {
                                    ++gw_fail_http;
                                } else {
                                    ++gw_fail_transport;
                                }
                                if ((gr.binance_error_code == -1003 || gr.binance_error_code == -1015) &&
                                    rest_throttle_cooldown_ms > 0) {
                                    rest_cooldown_until_ns =
                                        now_send_ns + static_cast<std::uint64_t>(rest_throttle_cooldown_ms) * 1000000ULL;
                                }
                                if (uncertain_transport) {
                                    auto& fail_streak = transport_fail_streak_by_symbol[idx];
                                    if (fail_streak < 1000000U) {
                                        ++fail_streak;
                                    }
                                    if (transport_circuit_fail_threshold > 0 &&
                                        fail_streak >= static_cast<std::uint32_t>(transport_circuit_fail_threshold)) {
                                        fail_streak = 0;
                                        if (transport_circuit_cooldown_ms > 0) {
                                            transport_circuit_until_ns[idx] =
                                                now_send_ns + static_cast<std::uint64_t>(transport_circuit_cooldown_ms) * 1000000ULL;
                                        }
                                        ++transport_circuit_open;
                                    }
                                } else {
                                    transport_fail_streak_by_symbol[idx] = 0;
                                }
                                exec_audit_line(
                                    exec_audit_on,
                                    exec_audit,
                                    exec_audit_drops,
                                    "ts_ns=%llu event=gw_fail sym=%s coid=%llu http=%d ix=%d type=%u",
                                    static_cast<unsigned long long>(strategy_ts),
                                    instrument_name(event.instrument),
                                    static_cast<unsigned long long>(cmd->client_order_id),
                                    gr.http_status,
                                    gr.binance_error_code,
                                    static_cast<unsigned>(cmd->type));
                                if (!uncertain_transport) {
                                    order_manager.on_command_rejected(*cmd);
                                    oms.on_command_rejected(*cmd);
                                }
                                continue;
                            }
                            transport_fail_streak_by_symbol[idx] = 0;
                            ++gw_ok;
                            oms.on_command_acked(*cmd);
                            if (cmd->client_order_id > 0) {
                                const std::size_t li = static_cast<std::size_t>(cmd->client_order_id % kLifecycleCap);
                                auto& e = lifecycle[li];
                                if (e.active && e.client_order_id != cmd->client_order_id) {
                                    ++lifecycle_overflow;
                                }
                                e.active = true;
                                e.timeout_reported = false;
                                e.client_order_id = cmd->client_order_id;
                                e.instrument = cmd->instrument;
                                e.ts_sent_ns = now_send_ns;
                            }
                            {
                                const std::uint64_t pair_id = current_pair_id_by_symbol[idx];
                                if (PairTrack* p = find_pair_slot(pair_id); p != nullptr) {
                                    p->ts_last_event_ns = now_send_ns;
                                    ++p->cmd_sent;
                                    if (cmd->type == CommandType::Cancel) {
                                        ++p->cancels;
                                    }
                                    if (cmd->side == Side::Buy) {
                                        p->bid_last_coid = cmd->client_order_id;
                                        p->bid_last_limit_px = cmd->price;
                                    } else {
                                        p->ask_last_coid = cmd->client_order_id;
                                        p->ask_last_limit_px = cmd->price;
                                    }
                                    const bool had_ref = find_coid_pair(cmd->client_order_id) != nullptr;
                                    CoidPairRef* ref = upsert_coid_pair(cmd->client_order_id, pair_id, cmd->side);
                                    if (ref != nullptr && !had_ref &&
                                        (cmd->type == CommandType::New || cmd->type == CommandType::Replace)) {
                                        ++p->open_coids;
                                    }
                                }
                            }
                            last_send_ns_by_symbol[idx] = now_send_ns;
                            exec_audit_line(
                                exec_audit_on,
                                exec_audit,
                                exec_audit_drops,
                                "ts_ns=%llu event=sent sym=%s coid=%llu type=%u px=%.2f qty=%.6f",
                                static_cast<unsigned long long>(strategy_ts),
                                instrument_name(event.instrument),
                                static_cast<unsigned long long>(cmd->client_order_id),
                                static_cast<unsigned>(cmd->type),
                                cmd->price,
                                cmd->qty);
                            ++exec_cmd_sent;
                            if (cmd->type == CommandType::New) {
                                ++exec_cmd_new;
                            } else if (cmd->type == CommandType::Replace) {
                                ++exec_cmd_replace;
                            } else if (cmd->type == CommandType::Cancel) {
                                ++exec_cmd_cancel;
                            }
                        }
                    }
                } else {
                    ++trigger_gated;
                }
            } else if (event.type == MdEventType::BookTicker || event.type == MdEventType::DepthUpdate) {
                ++stale_skips;
                if (!global_trading_ready) {
                    ++readiness_gated;
                }
            }

            switch (event.type) {
                case MdEventType::BookTicker:
                    ++book_ticker;
                    break;
                case MdEventType::DepthUpdate:
                    ++depth_update;
                    break;
                case MdEventType::AggTrade:
                    ++agg_trade;
                    break;
            }
        }
        for (std::size_t exec_n = 0; exec_n < exec_batch_max && exec_report_queue.pop(exec_msg); ++exec_n) {
            had_work = true;
            process_exec_report(exec_msg);
        }

        const auto now = std::chrono::steady_clock::now();
        const std::uint64_t now_scan_ns = now_ns();
        const std::uint64_t last_md_ns = ws_last_msg_ns.load(std::memory_order_relaxed);
        ws_last_msg_age_ms = (last_md_ns > 0 && now_scan_ns > last_md_ns)
            ? ((now_scan_ns - last_md_ns) / 1000000ULL)
            : 0ULL;
        if (ws_idle_reconnect_ms > 0 && last_md_ns > 0 &&
            ws_last_msg_age_ms > static_cast<std::uint64_t>(ws_idle_reconnect_ms) &&
            now_scan_ns >= ws_idle_reconnect_cooldown_until_ns) {
            exec_audit_line(
                md_health_on,
                md_health_log,
                md_health_drops,
                "ts_ns=%llu event=ws_idle_reconnect age_ms=%llu threshold_ms=%d",
                static_cast<unsigned long long>(now_scan_ns),
                static_cast<unsigned long long>(ws_last_msg_age_ms),
                ws_idle_reconnect_ms);
            ws_stop.store(true, std::memory_order_relaxed);
            if (ws_thread.joinable()) {
                ws_thread.join();
            }
            ws_stop.store(false, std::memory_order_relaxed);
            ws_thread = launch_ws_thread();
            ++ws_reconnects;
            ++ws_idle_reconnects;
            exec_audit_line(
                md_health_on,
                md_health_log,
                md_health_drops,
                "ts_ns=%llu event=ws_reconnected reason=idle reconnects=%llu",
                static_cast<unsigned long long>(now_ns()),
                static_cast<unsigned long long>(ws_reconnects));
            ws_last_msg_ns.store(now_ns(), std::memory_order_relaxed);
            if (ws_idle_reconnect_cooldown_ms > 0) {
                ws_idle_reconnect_cooldown_until_ns =
                    now_scan_ns + static_cast<std::uint64_t>(ws_idle_reconnect_cooldown_ms) * 1000000ULL;
            }
        }
        if (now - last_resync_scan >= std::chrono::milliseconds(200)) {
            for (const auto instrument : instruments) {
                const std::size_t idx = instrument_index(instrument);
                auto& book = books[idx];
                if (!book.needs_snapshot_seed()) {
                    continue;
                }
                if (snapshot_pending[idx].load(std::memory_order_relaxed)) {
                    continue;
                }
                if (next_snapshot_req_ns[idx] > now_scan_ns) {
                    continue;
                }
                snapshot_pending[idx].store(true, std::memory_order_relaxed);
                if (!snapshot_req_queue.push(SnapshotRequest{instrument})) {
                    snapshot_pending[idx].store(false, std::memory_order_relaxed);
                    ++snapshot_req_drop;
                }
            }
            last_resync_scan = now;
        }

        if (now - last_stats >= std::chrono::seconds(1)) {
            lifecycle_unresolved_by_symbol = {0, 0, 0};
            lifecycle_oldest_ms_by_symbol = {0, 0, 0};
            lifecycle_timeout_by_symbol = {0, 0, 0};
            const std::uint64_t now_lc_ns = now_ns();
            const std::uint64_t timeout_ns =
                lifecycle_timeout_ms > 0 ? static_cast<std::uint64_t>(lifecycle_timeout_ms) * 1000000ULL : 0ULL;
            const std::uint32_t remote_ids_n_u32 = g_reconcile_remote_ids_count.load(std::memory_order_acquire);
            const std::size_t remote_ids_n = std::min<std::size_t>(remote_ids_n_u32, kReconcileMaxIds);
            std::array<std::uint64_t, kReconcileMaxIds> remote_ids {};
            for (std::size_t i = 0; i < remote_ids_n; ++i) {
                remote_ids[i] = g_reconcile_remote_ids[i].load(std::memory_order_relaxed);
            }
            auto exists_remote = [&](std::uint64_t coid) {
                for (std::size_t i = 0; i < remote_ids_n; ++i) {
                    if (remote_ids[i] == coid) {
                        return true;
                    }
                }
                return false;
            };
            std::size_t timeout_audit_budget = lifecycle_timeout_audit_max_per_tick > 0
                ? static_cast<std::size_t>(lifecycle_timeout_audit_max_per_tick)
                : std::numeric_limits<std::size_t>::max();
            for (auto& e : lifecycle) {
                if (!e.active || e.client_order_id == 0 || e.ts_sent_ns == 0) {
                    continue;
                }
                const std::size_t sidx = instrument_index(e.instrument);
                ++lifecycle_unresolved_by_symbol[sidx];
                if (now_lc_ns > e.ts_sent_ns) {
                    const std::uint64_t age_ns = now_lc_ns - e.ts_sent_ns;
                    const std::uint64_t age_ms = age_ns / 1000000ULL;
                    if (age_ms > lifecycle_oldest_ms_by_symbol[sidx]) {
                        lifecycle_oldest_ms_by_symbol[sidx] = age_ms;
                    }
                    if (timeout_ns > 0 && age_ns > timeout_ns) {
                        ++lifecycle_timeout_by_symbol[sidx];
                        if (!e.timeout_reported) {
                            e.timeout_reported = true;
                            if (timeout_audit_budget > 0) {
                                --timeout_audit_budget;
                                exec_audit_line(
                                    exec_audit_on,
                                    exec_audit,
                                    exec_audit_drops,
                                    "ts_ns=%llu event=lifecycle_timeout sym=%s coid=%llu age_ms=%llu",
                                    static_cast<unsigned long long>(now_lc_ns),
                                    instrument_name(e.instrument),
                                    static_cast<unsigned long long>(e.client_order_id),
                                    static_cast<unsigned long long>(age_ms));
                                ++lifecycle_timeout_audit_emitted;
                            }
                        }
                        if (exists_remote(e.client_order_id)) {
                            const bool rebound = oms.mark_live_by_client_order_id(e.client_order_id, now_lc_ns);
                            exec_audit_line(
                                exec_audit_on,
                                exec_audit,
                                exec_audit_drops,
                                "ts_ns=%llu event=lifecycle_disposition sym=%s coid=%llu action=timeout_rebind_remote oms_live=%u",
                                static_cast<unsigned long long>(now_lc_ns),
                                instrument_name(e.instrument),
                                static_cast<unsigned long long>(e.client_order_id),
                                rebound ? 1U : 0U);
                            if (rebound) {
                                e.ts_sent_ns = now_lc_ns;
                                e.timeout_reported = false;
                            } else {
                                // If we cannot rebind locally but the remote still reports open,
                                // keep the lifecycle entry "timed out" so we keep auditing it.
                                // (OMS resurrection is expected to make rebound succeed.)
                            }
                        } else {
                            const bool om_drop = order_manager.reconcile_drop_client_order_id(e.client_order_id);
                            const bool oms_drop = oms.mark_completed_by_client_order_id(e.client_order_id, now_lc_ns);
                            exec_audit_line(
                                exec_audit_on,
                                exec_audit,
                                exec_audit_drops,
                                "ts_ns=%llu event=lifecycle_disposition sym=%s coid=%llu action=timeout_terminalize_local om_drop=%u oms_drop=%u",
                                static_cast<unsigned long long>(now_lc_ns),
                                instrument_name(e.instrument),
                                static_cast<unsigned long long>(e.client_order_id),
                                om_drop ? 1U : 0U,
                                oms_drop ? 1U : 0U);
                            mark_pair_local_terminal(e.client_order_id, now_lc_ns, "timeout_local");
                            e.active = false;
                            e.timeout_reported = false;
                        }
                    }
                }
            }
            std::uint64_t resync_required_symbols = 0;
            for (const auto& book : books) {
                if (book.resync_required()) {
                    ++resync_required_symbols;
                }
            }
            std::uint64_t synced_symbols = 0;
            for (const auto& book : books) {
                if (book.is_in_sync()) {
                    ++synced_symbols;
                }
            }
            const bool sym_sync_btc = books[0].is_in_sync();
            const bool sym_sync_eth = books[1].is_in_sync();
            const bool sym_sync_sol = books[2].is_in_sync();
            const bool sym_ready_btc = books[0].is_ready();
            const bool sym_ready_eth = books[1].is_ready();
            const bool sym_ready_sol = books[2].is_ready();
            const bool all_symbols_synced_now = (synced_symbols == instruments.size());
            const bool trading_ready_now = !require_all_symbols_sync || all_symbols_synced_now;
            const bool tradable_btc = sym_ready_btc && sym_sync_btc && (trading_ready_now || allow_partial_trading);
            const bool tradable_eth = sym_ready_eth && sym_sync_eth && (trading_ready_now || allow_partial_trading);
            const bool tradable_sol = sym_ready_sol && sym_sync_sol && (trading_ready_now || allow_partial_trading);
            const char* run_state = "SYNCING";
            if (trading_ready_now) {
                run_state = "TRADING_READY";
            } else if (consumed == 0 && snapshot_applied == 0) {
                run_state = "BOOTSTRAP";
            }
            if (trading_ready_now && tradable_zero_alert_ms > 0) {
                const std::uint64_t now_alert_ns = now_ns();
                const std::array<bool, 3> tradable_flags {tradable_btc, tradable_eth, tradable_sol};
                for (std::size_t i = 0; i < tradable_flags.size(); ++i) {
                    if (tradable_flags[i]) {
                        tradable_zero_since_ns[i] = 0;
                        continue;
                    }
                    if (tradable_zero_since_ns[i] == 0) {
                        tradable_zero_since_ns[i] = now_alert_ns;
                    }
                    const std::uint64_t zero_dur_ns = now_alert_ns - tradable_zero_since_ns[i];
                    const std::uint64_t alert_ns = static_cast<std::uint64_t>(tradable_zero_alert_ms) * 1000000ULL;
                    if (zero_dur_ns >= alert_ns &&
                        (tradable_zero_last_alert_ns[i] == 0 || now_alert_ns - tradable_zero_last_alert_ns[i] >= alert_ns)) {
                        tradable_zero_last_alert_ns[i] = now_alert_ns;
                        ++tradable_zero_alerts[i];
                        exec_audit_line(
                            md_health_on,
                            md_health_log,
                            md_health_drops,
                            "ts_ns=%llu event=alert_tradable_zero sym=%s zero_ms=%llu state=%s ready=%u sync=%u",
                            static_cast<unsigned long long>(now_alert_ns),
                            instrument_name(instruments[i]),
                            static_cast<unsigned long long>(zero_dur_ns / 1000000ULL),
                            run_state,
                            static_cast<unsigned>(i == 0 ? sym_ready_btc : (i == 1 ? sym_ready_eth : sym_ready_sol)),
                            static_cast<unsigned>(i == 0 ? sym_sync_btc : (i == 1 ? sym_sync_eth : sym_sync_sol)));
                    }
                }
            } else {
                tradable_zero_since_ns = {0, 0, 0};
            }
            const auto cancel_opp = order_manager.cancel_opposite_counts();
            const auto cancel_stale = order_manager.cancel_stale_counts();
            const auto cancel_adv = order_manager.cancel_adverse_counts();
            const std::uint64_t sent_delta = exec_cmd_sent - prev_exec_cmd_sent;
            prev_exec_cmd_sent = exec_cmd_sent;
            if (exec_adaptive_cancel && sent_delta > 0) {
                auto clamp_u32 = [](std::int64_t v, std::int64_t lo, std::int64_t hi) -> std::uint32_t {
                    if (v < lo) v = lo;
                    if (v > hi) v = hi;
                    return static_cast<std::uint32_t>(v);
                };
                bool changed = false;
                for (std::size_t i = 0; i < 3; ++i) {
                    const std::uint64_t d_adv = cancel_adv[i] - prev_cancel_adv[i];
                    const std::uint64_t d_stale = cancel_stale[i] - prev_cancel_stale[i];
                    prev_cancel_adv[i] = cancel_adv[i];
                    prev_cancel_stale[i] = cancel_stale[i];

                    std::uint32_t next_adv = adaptive_adv_bps_x1000[i];
                    std::uint32_t next_stale = adaptive_stale_ms[i];

                    if (d_adv > static_cast<std::uint64_t>(exec_adaptive_target_adv_per_sec)) {
                        next_adv = clamp_u32(
                            static_cast<std::int64_t>(next_adv) + exec_adaptive_adv_step_bps_x1000,
                            exec_adaptive_adv_min_bps_x1000,
                            exec_adaptive_adv_max_bps_x1000);
                    } else if (d_adv + 1 < static_cast<std::uint64_t>(exec_adaptive_target_adv_per_sec) && next_adv > 0) {
                        next_adv = clamp_u32(
                            static_cast<std::int64_t>(next_adv) - exec_adaptive_adv_step_bps_x1000,
                            exec_adaptive_adv_min_bps_x1000,
                            exec_adaptive_adv_max_bps_x1000);
                    }

                    if (d_stale > static_cast<std::uint64_t>(exec_adaptive_target_stale_per_sec)) {
                        next_stale = clamp_u32(
                            static_cast<std::int64_t>(next_stale) + exec_adaptive_stale_step_ms,
                            exec_adaptive_stale_min_ms,
                            exec_adaptive_stale_max_ms);
                    } else if (d_stale + 1 < static_cast<std::uint64_t>(exec_adaptive_target_stale_per_sec)) {
                        next_stale = clamp_u32(
                            static_cast<std::int64_t>(next_stale) - exec_adaptive_stale_step_ms,
                            exec_adaptive_stale_min_ms,
                            exec_adaptive_stale_max_ms);
                    }

                    if (next_adv != adaptive_adv_bps_x1000[i] || next_stale != adaptive_stale_ms[i]) {
                        adaptive_adv_bps_x1000[i] = next_adv;
                        adaptive_stale_ms[i] = next_stale;
                        ++adaptive_updates[i];
                        changed = true;
                    }
                }
                if (changed) {
                    order_manager.update_cancel_policies(adaptive_stale_ms, adaptive_adv_bps_x1000);
                }
            }
            const double pnl_mtm_btc = mid_px_by_symbol[0] > 0.0
                ? pnl_engine.mark_to_market(pnl_states[0], mid_px_by_symbol[0])
                : pnl_states[0].realized - pnl_states[0].fees_paid;
            const double pnl_mtm_eth = mid_px_by_symbol[1] > 0.0
                ? pnl_engine.mark_to_market(pnl_states[1], mid_px_by_symbol[1])
                : pnl_states[1].realized - pnl_states[1].fees_paid;
            const double pnl_mtm_sol = mid_px_by_symbol[2] > 0.0
                ? pnl_engine.mark_to_market(pnl_states[2], mid_px_by_symbol[2])
                : pnl_states[2].realized - pnl_states[2].fees_paid;
            const double pnl_mtm_total = pnl_mtm_btc + pnl_mtm_eth + pnl_mtm_sol;
            std::cout << "rx=" << rx_count.load(std::memory_order_relaxed)
                      << " consumed=" << consumed
                      << " dropped=" << drop_count.load(std::memory_order_relaxed)
                      << " parse_reject=" << parse_reject_count.load(std::memory_order_relaxed)
                      << " bookTicker=" << book_ticker
                      << " depth=" << depth_update
                      << " aggTrade=" << agg_trade
                      << " signals=" << strategy_signals
                      << " intent=" << intent_generated
                      << " om_new=" << om_cmd_new
                      << " om_replace=" << om_cmd_replace
                      << " om_cancel=" << om_cmd_cancel
                      << " exec_new=" << exec_cmd_new
                      << " exec_replace=" << exec_cmd_replace
                      << " exec_cancel=" << exec_cmd_cancel
                      << " exec_sent=" << exec_cmd_sent
                      << " risk_pass=" << risk_pass
                      << " exec_risk_reject=" << exec_cmd_rejected_risk
                      << " exec_risk_kill=" << exec_risk_kill
                      << " exec_paced=" << exec_cmd_paced
                      << " exec_rate_limited=" << exec_cmd_rate_limited
                      << " throttle_q_suppressed=" << throttle_quote_suppressed
                      << " exec_gateway_fail=" << exec_cmd_gateway_fail
                      << " exec_exch_filter_reject=" << exec_exch_filter_reject
                      << " gw_attempt=" << gw_attempt
                      << " gw_ok=" << gw_ok
                      << " gw_fail_http=" << gw_fail_http
                      << " gw_fail_transport=" << gw_fail_transport
                      << " gw_transport_retry=" << gw_transport_retry
                      << " gw_transport_drop_after_retries=" << gw_transport_drop_after_retries
                      << " transport_cooldown_active=" << transport_cooldown_active
                      << " transport_circuit_open=" << transport_circuit_open
                      << " transport_circuit_blocked=" << transport_circuit_blocked
                      << " ws_last_msg_age_ms=" << ws_last_msg_age_ms
                      << " ws_reconnects=" << ws_reconnects
                      << " ws_idle_reconnects=" << ws_idle_reconnects
                      << " md_health_drop=" << md_health_drops
                      << " gw_last_http=" << gw_fail_last_http
                      << " gw_last_ix=" << gw_fail_last_ix
                      << " exec_audit_drop=" << exec_audit_drops
                      << " pair_audit_drop=" << pair_audit_drops
                      << " pair_close_logged=" << pair_close_logged
                      << " pair_intent_logged=" << pair_intent_logged
                      << " pair_slot_overflow=" << pair_track_overflow
                      << " pair_coid_overflow=" << pair_coid_overflow
                      << " rec_exch_open=" << g_reconcile_remote_open.load(std::memory_order_relaxed)
                      << " rec_mismatch=" << reconcile_mismatch
                      << " rec_heal_om=" << reconcile_healed_om
                      << " rec_heal_oms=" << reconcile_healed_oms
                      << " rec_heal_pending=" << reconcile_healed_pending
                      << " rec_heal_pending_om_drop=" << reconcile_healed_pending_drop_om
                      << " rec_http_fail=" << g_reconcile_http_fail.load(std::memory_order_relaxed)
                      << " rest_mbx_wt_1m=" << g_rest_weight_1m.load(std::memory_order_relaxed)
                      << " exec_reports=" << exec_reports
                      << " exec_ack=" << exec_acks
                      << " exec_reject=" << exec_rejects
                      << " exec_cancel_report=" << exec_cancels
                      << " exec_fills=" << exec_fills
                      << " pnl_mtm_total=" << pnl_mtm_total
                      << " pnl_mtm_btc=" << pnl_mtm_btc
                      << " pnl_mtm_eth=" << pnl_mtm_eth
                      << " pnl_mtm_sol=" << pnl_mtm_sol
                      << " pnl_inv_btc=" << pnl_states[0].inventory
                      << " pnl_inv_eth=" << pnl_states[1].inventory
                      << " pnl_inv_sol=" << pnl_states[2].inventory
                      << " pnl_dd_pause=" << pnl_dd_paused
                      << " pnl_dd_trip_btc=" << pnl_dd_trips[0]
                      << " pnl_dd_trip_eth=" << pnl_dd_trips[1]
                      << " pnl_dd_trip_sol=" << pnl_dd_trips[2]
                      << " exec_active=" << order_manager.active_orders()
                      << " can_opp_btc=" << cancel_opp[0]
                      << " can_opp_eth=" << cancel_opp[1]
                      << " can_opp_sol=" << cancel_opp[2]
                      << " can_stale_btc=" << cancel_stale[0]
                      << " can_stale_eth=" << cancel_stale[1]
                      << " can_stale_sol=" << cancel_stale[2]
                      << " can_adv_btc=" << cancel_adv[0]
                      << " can_adv_eth=" << cancel_adv[1]
                      << " can_adv_sol=" << cancel_adv[2]
                      << " adapt_stale_btc=" << adaptive_stale_ms[0]
                      << " adapt_stale_eth=" << adaptive_stale_ms[1]
                      << " adapt_stale_sol=" << adaptive_stale_ms[2]
                      << " adapt_adv_btc=" << adaptive_adv_bps_x1000[0]
                      << " adapt_adv_eth=" << adaptive_adv_bps_x1000[1]
                      << " adapt_adv_sol=" << adaptive_adv_bps_x1000[2]
                      << " adapt_upd_btc=" << adaptive_updates[0]
                      << " adapt_upd_eth=" << adaptive_updates[1]
                      << " adapt_upd_sol=" << adaptive_updates[2]
                      << " oms_live=" << oms.live_orders()
                      << " oms_invalid=" << oms.invalid_transitions()
                      << " depth_oos=" << depth_out_of_sync
                      << " stale_skips=" << stale_skips
                      << " snap_ok=" << snapshot_applied
                      << " snap_fail=" << snapshot_failed
                      << " snap_req_drop=" << snapshot_req_drop
                      << " depth_buf=" << depth_buffered
                      << " depth_replay=" << depth_buffer_replayed
                      << " depth_buf_drop=" << depth_buffer_drop
                      << " parse_ns_avg="
                      << (rx_count.load(std::memory_order_relaxed) > 0
                              ? parse_ns_sum.load(std::memory_order_relaxed) / rx_count.load(std::memory_order_relaxed)
                              : 0)
                      << " parse_ns_max=" << parse_ns_max.load(std::memory_order_relaxed)
                      << " enq_ns_avg="
                      << (rx_count.load(std::memory_order_relaxed) > 0
                              ? enqueue_ns_sum.load(std::memory_order_relaxed) / rx_count.load(std::memory_order_relaxed)
                              : 0)
                      << " enq_ns_max=" << enqueue_ns_max.load(std::memory_order_relaxed)
                      << " q2s_ns_avg="
                      << (queue_to_strategy_count > 0 ? queue_to_strategy_ns_sum / queue_to_strategy_count : 0)
                      << " q2s_ns_p50=" << q2s_window.percentile(0.50)
                      << " q2s_ns_p99=" << q2s_window.percentile(0.99)
                      << " q2s_ns_max=" << queue_to_strategy_ns_max
                      << " q2s_stale_drop=" << q2s_stale_drop
                      << " lc_unres_btc=" << lifecycle_unresolved_by_symbol[0]
                      << " lc_unres_eth=" << lifecycle_unresolved_by_symbol[1]
                      << " lc_unres_sol=" << lifecycle_unresolved_by_symbol[2]
                      << " lc_oldest_btc_ms=" << lifecycle_oldest_ms_by_symbol[0]
                      << " lc_oldest_eth_ms=" << lifecycle_oldest_ms_by_symbol[1]
                      << " lc_oldest_sol_ms=" << lifecycle_oldest_ms_by_symbol[2]
                      << " lc_timeout_btc=" << lifecycle_timeout_by_symbol[0]
                      << " lc_timeout_eth=" << lifecycle_timeout_by_symbol[1]
                      << " lc_timeout_sol=" << lifecycle_timeout_by_symbol[2]
                      << " lc_timeout_audit=" << lifecycle_timeout_audit_emitted
                      << " lc_overflow=" << lifecycle_overflow
                      << " trigger_gated=" << trigger_gated
                      << " readiness_gated=" << readiness_gated
                      << " state=" << run_state
                      << " synced_symbols=" << synced_symbols
                      << " sync_btc=" << (sym_sync_btc ? 1 : 0)
                      << " sync_eth=" << (sym_sync_eth ? 1 : 0)
                      << " sync_sol=" << (sym_sync_sol ? 1 : 0)
                      << " ready_btc=" << (sym_ready_btc ? 1 : 0)
                      << " ready_eth=" << (sym_ready_eth ? 1 : 0)
                      << " ready_sol=" << (sym_ready_sol ? 1 : 0)
                      << " tradable_btc=" << (tradable_btc ? 1 : 0)
                      << " tradable_eth=" << (tradable_eth ? 1 : 0)
                      << " tradable_sol=" << (tradable_sol ? 1 : 0)
                      << " alert_tradable_zero_btc=" << tradable_zero_alerts[0]
                      << " alert_tradable_zero_eth=" << tradable_zero_alerts[1]
                      << " alert_tradable_zero_sol=" << tradable_zero_alerts[2]
                      << " seed_parsed_count=" << position_seed_parsed_count
                      << " seed_missing_count=" << position_seed_missing_count
                      << " seed_invalid_count=" << position_seed_invalid_count
                      << " canary_rot_btc=" << canary_rot_window_btc
                      << " canary_rot_eth=" << canary_rot_window_eth
                      << " canary_rot_sol=" << canary_rot_window_sol
                      << " resync_flags=" << resync_required_symbols
                      << " last=" << instrument_name(last_consumed_instrument)
                      << '\n';
            exec_audit_line(
                md_health_on,
                md_health_log,
                md_health_drops,
                "ts_ns=%llu event=md_health_tick state=%s wt=%d q2s_p99=%llu rec_mismatch=%llu oms_invalid=%llu tradable=%u/%u/%u rate_limited=%llu throttle_supp=%llu",
                static_cast<unsigned long long>(now_ns()),
                run_state,
                g_rest_weight_1m.load(std::memory_order_relaxed),
                static_cast<unsigned long long>(q2s_window.percentile(0.99)),
                static_cast<unsigned long long>(reconcile_mismatch),
                static_cast<unsigned long long>(oms.invalid_transitions()),
                tradable_btc ? 1U : 0U,
                tradable_eth ? 1U : 0U,
                tradable_sol ? 1U : 0U,
                static_cast<unsigned long long>(exec_cmd_rate_limited),
                static_cast<unsigned long long>(throttle_quote_suppressed));
            last_stats = now;
        }

        if (!had_work) {
            if (busy_spin) {
#if defined(__x86_64__) || defined(__i386__)
                for (int i = 0; i < 64; ++i) {
                    __builtin_ia32_pause();
                }
#else
                std::this_thread::yield();
#endif
            } else {
                std::this_thread::yield();
            }
        }
    }

    const std::uint64_t shutdown_ts_ns = now_ns();
    for (auto& p : pair_tracks) {
        if (!p.active || p.emitted) {
            continue;
        }
        emit_pair_record(p, shutdown_ts_ns, "shutdown");
    }
    pair_audit_log.shutdown_join();
    exec_audit.shutdown_join();
    md_health_log.shutdown_join();
    ws_stop.store(true, std::memory_order_relaxed);
    ws_thread.join();
    user_stream_thread.join();
    snapshot_thread.join();
    if (reconcile_thread.joinable()) {
        reconcile_thread.join();
    }
    return 0;
}
