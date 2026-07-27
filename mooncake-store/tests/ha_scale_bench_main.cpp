// =============================================================================
// HA scale benchmark: standalone client against a REAL separate mooncake_master
// PROCESS, driven by ha_scale_bench.sh. Extends ha_recovery_live_main.cpp with
// timing + scale + recovery-window latency/failure statistics.
//
// Measures three metrics:
//   [1] rebuild latency : t_recovered - t_first_read_fail (and since restart)
//   [2] scale           : same run repeated over the --nkeys sweep (via .sh)
//   [3] recovery-window : per-poll sampled success-rate + Get latency during
//                         the outage->recovery window, vs steady-state Get lat
//
// Key design choices (why, so a reviewer can trust the numbers):
//   - Fill with BatchPut (--batch keys per RPC): filling millions of keys one
//     Put at a time is dominated by per-RPC overhead, not the thing we measure.
//   - Completion is judged by a UNIFORM SAMPLE of --probe keys, not by Getting
//     all N. Getting millions of keys each poll would itself take seconds and
//     pollute the latency measurement. Sampling every key-space region detects
//     partial rebuild while staying cheap.
//   - steady_clock everywhere; recovery poll interval is --poll_ms (default 20)
//     so rebuild-latency resolution is +/-poll_ms, not the old 500ms.
//   - Emits a machine-readable JSON line (JSON_RESULT={...}) the .sh collects.
// =============================================================================
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "tcp", "transfer protocol");
DEFINE_string(master, "127.0.0.1:50055", "master rpc ip:port");
DEFINE_string(metadata, "", "http metadata server url (empty => P2PHANDSHAKE)");
DEFINE_string(local, "127.0.0.1:19110", "local hostname ip:port");
DEFINE_int64(nkeys, 50, "number of keys to put");
DEFINE_int32(vsize, 100, "value size in bytes per key");
DEFINE_int32(batch, 2000, "keys per BatchPut RPC while filling");
DEFINE_int32(probe, 1000,
             "number of uniformly-sampled keys used as the "
             "rebuild-completion probe (<=nkeys)");
DEFINE_int32(poll_ms, 20, "recovery poll interval in ms");
DEFINE_int32(max_recovery_sec, 600, "give up waiting for recovery after this");
DEFINE_int64(seg_mb, 0, "segment size in MB (0 => auto from nkeys*vsize)");
DEFINE_int64(alloc_mb, 0, "local buffer size in MB (0 => auto)");

using namespace mooncake;
using Clock = std::chrono::steady_clock;

static double ms_since(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// Build the value payload for key i deterministically (so Get can verify).
static std::string MakeValue(int64_t i, int vsize) {
    std::string v = "v" + std::to_string(i) + "_";
    if (static_cast<int>(v.size()) >= vsize) {
        v.resize(vsize);
    } else {
        v.append(vsize - v.size(), static_cast<char>('a' + (i % 26)));
    }
    return v;
}

static std::string MakeKey(int64_t i) {
    return "scale_key_" + std::to_string(i);
}

// Get one key and byte-verify against expected. Returns {ok, latency_ms}.
static std::pair<bool, double> GetVerify(std::shared_ptr<Client>& c,
                                         SimpleAllocator& a,
                                         const std::string& k,
                                         const std::string& exp) {
    void* buf = a.allocate(exp.size());
    std::vector<Slice> s{Slice{buf, exp.size()}};
    auto t0 = Clock::now();
    auto r = c->Get(k, s);
    auto t1 = Clock::now();
    bool ok = r.has_value() && s[0].size == exp.size() &&
              std::memcmp(s[0].ptr, exp.data(), exp.size()) == 0;
    a.deallocate(buf, exp.size());
    return {ok, ms_since(t0, t1)};
}

// Probe the sampled key set once. Returns {num_ok, avg_latency_ms_over_ok}.
static std::pair<int, double> ProbeOnce(std::shared_ptr<Client>& c,
                                        SimpleAllocator& a,
                                        const std::vector<int64_t>& idx,
                                        int vsize) {
    int ok = 0;
    double sum = 0;
    for (int64_t i : idx) {
        auto [good, lat] = GetVerify(c, a, MakeKey(i), MakeValue(i, vsize));
        if (good) {
            ++ok;
            sum += lat;
        }
    }
    return {ok, ok ? sum / ok : 0.0};
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    const int64_t N = FLAGS_nkeys;
    const int vsize = FLAGS_vsize;
    const int probe_n = std::min<int64_t>(FLAGS_probe, N);

    // Auto-size segment/buffer. Per-object footprint in the segment is far
    // larger than the value payload: object metadata + allocator min-unit /
    // alignment / fragmentation. Measured ~1073 B/object at vsize=100, so we
    // budget max(vsize+1200, 2*vsize) bytes per key plus a floor and headroom.
    int64_t per_obj = std::max<int64_t>(vsize + 1200, vsize * 2);
    int64_t data_mb = (N * per_obj) / (1024 * 1024) + 1;
    int64_t seg_mb =
        FLAGS_seg_mb ? FLAGS_seg_mb : std::max<int64_t>(128, data_mb * 3 / 2);
    int64_t alloc_mb = FLAGS_alloc_mb ? FLAGS_alloc_mb : 64;
    const size_t kSeg = static_cast<size_t>(seg_mb) * 1024 * 1024;
    const size_t kAlloc = static_cast<size_t>(alloc_mb) * 1024 * 1024;

    LOG(INFO) << "CONFIG nkeys=" << N << " vsize=" << vsize
              << " batch=" << FLAGS_batch << " probe=" << probe_n
              << " poll_ms=" << FLAGS_poll_ms << " seg_mb=" << seg_mb
              << " alloc_mb=" << alloc_mb;

    const std::string meta =
        FLAGS_metadata.empty() ? "P2PHANDSHAKE" : FLAGS_metadata;
    auto co = Client::Create(FLAGS_local, meta, FLAGS_protocol, std::nullopt,
                             FLAGS_master);
    if (!co.has_value()) {
        LOG(ERROR) << "RESULT=FAIL reason=client_create_failed";
        return 2;
    }
    auto client = co.value();

    auto alloc = std::make_unique<SimpleAllocator>(kAlloc);
    auto reg = client->RegisterLocalMemory(alloc->getBase(), kAlloc, "cpu:0",
                                           false, false);
    if (!reg.has_value()) {
        LOG(ERROR) << "RESULT=FAIL reason=register_local_memory_failed";
        return 2;
    }
    void* seg = allocate_buffer_allocator_memory(kSeg);
    if (!seg) {
        LOG(ERROR) << "RESULT=FAIL reason=segment_alloc_failed seg_mb="
                   << seg_mb;
        return 2;
    }
    auto mnt = client->MountSegment(seg, kSeg, FLAGS_protocol);
    if (!mnt.has_value()) {
        LOG(ERROR) << "RESULT=FAIL reason=mount_failed";
        return 2;
    }

    // --- fill N keys via BatchPut ---
    // A dedicated fill buffer, reused per batch (separate from the Get path).
    auto fill_alloc = std::make_unique<SimpleAllocator>(std::max<size_t>(
        kAlloc, static_cast<size_t>(FLAGS_batch) * vsize + (1 << 20)));
    auto t_fill0 = Clock::now();
    int64_t filled = 0;
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    for (int64_t base = 0; base < N; base += FLAGS_batch) {
        int64_t cnt = std::min<int64_t>(FLAGS_batch, N - base);
        std::vector<ObjectKey> keys;
        std::vector<std::vector<Slice>> slices;
        std::vector<std::string> vals;
        keys.reserve(cnt);
        slices.reserve(cnt);
        vals.reserve(cnt);
        for (int64_t j = 0; j < cnt; ++j) {
            int64_t i = base + j;
            keys.push_back(MakeKey(i));
            vals.push_back(MakeValue(i, vsize));
        }
        for (int64_t j = 0; j < cnt; ++j) {
            void* b = fill_alloc->allocate(vals[j].size());
            std::memcpy(b, vals[j].data(), vals[j].size());
            slices.push_back({Slice{b, vals[j].size()}});
        }
        auto rs = client->BatchPut(keys, slices, cfg);
        for (auto& s : slices) fill_alloc->deallocate(s[0].ptr, s[0].size);
        for (size_t j = 0; j < rs.size(); ++j) {
            if (!rs[j].has_value()) {
                LOG(ERROR) << "RESULT=FAIL reason=batchput_failed key="
                           << keys[j] << " err=" << toString(rs[j].error());
                return 2;
            }
        }
        filled += cnt;
        if (base / FLAGS_batch % 50 == 0)
            LOG(INFO) << "FILL progress " << filled << "/" << N;
    }
    double fill_ms = ms_since(t_fill0, Clock::now());
    LOG(INFO) << "FILL done " << filled << "/" << N << " in " << fill_ms
              << " ms (" << (filled / (fill_ms / 1000.0)) << " keys/s)";

    // --- build the uniform probe sample ---
    std::vector<int64_t> probe_idx;
    probe_idx.reserve(probe_n);
    for (int p = 0; p < probe_n; ++p) {
        // even spacing across [0, N)
        probe_idx.push_back(
            static_cast<int64_t>((static_cast<double>(p) + 0.5) * N / probe_n));
    }

    // --- baseline: probe must be fully readable, and record steady latency ---
    auto [base_ok, steady_lat] = ProbeOnce(client, *alloc, probe_idx, vsize);
    LOG(INFO) << "BASELINE probe_ok=" << base_ok << "/" << probe_n
              << " steady_get_lat_ms=" << steady_lat;
    if (base_ok != probe_n) {
        LOG(ERROR) << "RESULT=FAIL reason=baseline_incomplete";
        return 2;
    }

    LOG(INFO) << "READY_FOR_KILL";
    fflush(stderr);

    // --- recovery window: poll the probe, capture timing + per-poll stats ---
    auto t_ready = Clock::now();
    bool saw_down = false;
    Clock::time_point t_first_fail, t_recovered;
    int min_ok = probe_n;       // worst observed availability
    double outage_lat_sum = 0;  // avg latency of successful Gets during outage
    int outage_lat_polls = 0;
    int polls = 0;

    int max_polls = (FLAGS_max_recovery_sec * 1000) / FLAGS_poll_ms;
    for (int attempt = 0; attempt < max_polls; ++attempt) {
        auto [ok, lat] = ProbeOnce(client, *alloc, probe_idx, vsize);
        ++polls;
        if (ok < probe_n) {
            if (!saw_down) {
                saw_down = true;
                t_first_fail = Clock::now();
            }
            min_ok = std::min(min_ok, ok);
            if (ok > 0) {
                outage_lat_sum += lat;
                ++outage_lat_polls;
            }
        }
        if (saw_down && ok == probe_n) {
            t_recovered = Clock::now();
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_poll_ms));
    }

    if (!saw_down || t_recovered.time_since_epoch().count() == 0) {
        LOG(ERROR) << "RESULT=FAIL reason=not_recovered saw_down=" << saw_down
                   << " polls=" << polls;
        return 1;
    }

    double rebuild_ms = ms_since(t_first_fail, t_recovered);
    double since_ready_ms = ms_since(t_ready, t_recovered);
    double outage_lat =
        outage_lat_polls ? outage_lat_sum / outage_lat_polls : 0.0;
    double min_avail_pct = 100.0 * min_ok / probe_n;

    LOG(INFO) << "RESULT=PASS recovered=" << probe_n << "/" << probe_n
              << " zero-recompute";
    // Machine-readable line for the shell to scrape.
    LOG(INFO) << "JSON_RESULT={"
              << "\"nkeys\":" << N << ",\"vsize\":" << vsize
              << ",\"probe\":" << probe_n << ",\"fill_ms\":" << fill_ms
              << ",\"fill_keys_per_s\":" << (filled / (fill_ms / 1000.0))
              << ",\"rebuild_ms\":" << rebuild_ms
              << ",\"recover_since_ready_ms\":" << since_ready_ms
              << ",\"steady_get_lat_ms\":" << steady_lat
              << ",\"outage_get_lat_ms\":" << outage_lat
              << ",\"min_avail_pct\":" << min_avail_pct
              << ",\"poll_ms\":" << FLAGS_poll_ms << "}";
    fflush(stderr);

    client->UnmountSegment(seg, kSeg);
    return 0;
}
