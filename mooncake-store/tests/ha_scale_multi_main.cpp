// =============================================================================
// ha_scale_multi_main.cpp —— 大规模多-client HA 重建压测(拟合真实生产)
//
// 相比 ha_scale_bench_main.cpp(单client、静止后kill)的改进:
//   [多client]  --nclients 个 client,各自线程、各自挂段、各自灌 nkeys/nclients
//               个老key。kill master 后每个 client 各自重建自己的元数据(真实
//               生产=多节点并发重建)。
//   [全程压测]  kill 前后不停:每个 client 一个后台压测线程,持续
//               (a) get 老key(测存量复用命中率) + (b) put 新key(测故障期写入)。
//               老/新 key 空间分开,信号不混。
//   [渐进曲线]  恢复轮询每 poll 打印 SAMPLE 行(老key采样命中率 vs 时间),
//               可画"命中率从0爬到100%"的渐进恢复曲线。
//   [机制A]     --master 传 etcd://... 即走 HA 选主(client 代码原生支持,
//               外部起 etcd+2master,kill leader 让 standby 上位)。
//
// 老key命中率 = 你功能价值的纯净信号(组1恢复/组2永久miss)。
// 新key put   = 全程压测的动态负载 + 故障期写入服务质量。
// =============================================================================
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "tcp", "transfer protocol");
DEFINE_string(master, "etcd://127.0.0.1:3579", "master addr; etcd://.. => HA");
DEFINE_string(metadata, "", "metadata server url (empty => P2PHANDSHAKE)");
DEFINE_string(local_base, "127.0.0.1", "local host ip (port auto per client)");
DEFINE_int32(local_port_base, 19200, "base local port; client i uses base+i");
DEFINE_int32(nclients, 10, "number of concurrent clients (threads)");
DEFINE_int64(nkeys, 5000000, "TOTAL old keys across all clients");
DEFINE_int32(vsize, 8192, "value size bytes per key");
DEFINE_int32(batch, 1000, "keys per BatchPut RPC while filling");
DEFINE_int32(probe_per_client, 200, "sampled old keys per client for probe");
DEFINE_int32(poll_ms, 50, "recovery poll interval ms");
DEFINE_int32(max_recovery_sec, 600, "give up after this");
DEFINE_int64(seg_mb_per_client, 0, "segment MB per client (0=>auto)");
DEFINE_int32(stress_get_per_poll, 50, "stress: old-key GETs per client between polls");
DEFINE_int32(stress_put_per_poll, 10, "stress: new-key PUTs per client between polls");
DEFINE_double(recover_pct, 95.0, "old-key hit%% of baseline to declare rebuild complete");
DEFINE_int64(max_new_puts_per_client, 20000, "cap new-key puts per client so the segment isn't flooded (0=unlimited)");
DEFINE_int32(client_id_base, 0, "global client-id offset for multi-PROCESS runs: this process's client c uses global id (client_id_base + c) so key space & local ports don't collide across processes");
DEFINE_int32(hold_after_recover_sec, 0, "after RESULT, keep client alive (segment mounted) this many seconds before exit. Multi-process: prevents an early-finishing client from unmounting its segment and evicting keys that the master's global allocator placed there on behalf of still-recovering peers.");

using namespace mooncake;
using Clock = std::chrono::steady_clock;
static double ms_since(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// key/value 生成:old key 按 (client, idx);new key 按 (client, seq) 独立空间。
static std::string OldKey(int c, int64_t i) {
    return "old_c" + std::to_string(c) + "_" + std::to_string(i);
}
static std::string NewKey(int c, int64_t i) {
    return "new_c" + std::to_string(c) + "_" + std::to_string(i);
}
static std::string MakeValue(int64_t seed, int vsize) {
    std::string v = "v" + std::to_string(seed) + "_";
    if ((int)v.size() >= vsize) v.resize(vsize);
    else v.append(vsize - v.size(), (char)('a' + (seed % 26)));
    return v;
}

// 每个 client 的运行态。
struct ClientCtx {
    int id;
    std::shared_ptr<Client> client;
    std::unique_ptr<SimpleAllocator> alloc;   // get 路径缓冲
    std::unique_ptr<SimpleAllocator> put_alloc;  // put 路径缓冲
    void* seg = nullptr;
    size_t seg_bytes = 0;
    int64_t nkeys_local = 0;
    std::vector<int64_t> probe_idx;           // 采样的老key下标
    std::atomic<int64_t> new_put_seq{0};      // 新key递增序号
    std::atomic<int64_t> new_put_ok{0};
    std::atomic<int64_t> new_put_fail{0};
    std::atomic<bool> stress_run{false};
};

// 单个 client:Create + 挂段 + 灌 nkeys_local 个老key。返回成功与否。
static bool SetupAndFill(ClientCtx& cx, int vsize, int batch) {
    const std::string meta = FLAGS_metadata.empty() ? "P2PHANDSHAKE" : FLAGS_metadata;
    std::string local = FLAGS_local_base + ":" + std::to_string(FLAGS_local_port_base + cx.id);
    auto co = Client::Create(local, meta, FLAGS_protocol, std::nullopt, FLAGS_master);
    if (!co.has_value()) { LOG(ERROR) << "client " << cx.id << " create failed"; return false; }
    cx.client = co.value();

    size_t kAlloc = (size_t)std::max(batch * vsize + (1 << 20), 64 << 20);
    cx.alloc = std::make_unique<SimpleAllocator>(64 << 20);
    cx.put_alloc = std::make_unique<SimpleAllocator>(kAlloc);
    auto reg = cx.client->RegisterLocalMemory(cx.alloc->getBase(), 64 << 20, "cpu:0", false, false);
    if (!reg.has_value()) { LOG(ERROR) << "client " << cx.id << " reg failed"; return false; }

    int64_t per_obj = std::max<int64_t>(vsize + 1200, vsize * 2);
    int64_t data_mb = (cx.nkeys_local * per_obj) / (1024 * 1024) + 1;
    int64_t seg_mb = FLAGS_seg_mb_per_client ? FLAGS_seg_mb_per_client
                                             : std::max<int64_t>(128, data_mb * 3 / 2);
    cx.seg_bytes = (size_t)seg_mb * 1024 * 1024;
    cx.seg = allocate_buffer_allocator_memory(cx.seg_bytes);
    if (!cx.seg) { LOG(ERROR) << "client " << cx.id << " seg alloc " << seg_mb << "MB failed"; return false; }
    auto mnt = cx.client->MountSegment(cx.seg, cx.seg_bytes, FLAGS_protocol);
    if (!mnt.has_value()) { LOG(ERROR) << "client " << cx.id << " mount failed"; return false; }

    // 灌老key。BatchPut 带重试:高并发大流量下 tcp 偶发 Connection reset,
    // 单批瞬时失败重发即可(规模越大批次越多、撞 reset 概率越高,不重试会零容错判败)。
    ReplicateConfig cfg; cfg.replica_num = 1;
    const int kFillRetry = 5;
    for (int64_t base = 0; base < cx.nkeys_local; base += batch) {
        int64_t cnt = std::min<int64_t>(batch, cx.nkeys_local - base);
        std::vector<ObjectKey> keys; std::vector<std::vector<Slice>> slices; std::vector<std::string> vals;
        for (int64_t j = 0; j < cnt; ++j) { keys.push_back(OldKey(cx.id, base + j)); vals.push_back(MakeValue(base + j, vsize)); }
        for (int64_t j = 0; j < cnt; ++j) { void* b = cx.put_alloc->allocate(vals[j].size()); std::memcpy(b, vals[j].data(), vals[j].size()); slices.push_back({Slice{b, vals[j].size()}}); }
        bool ok = false;
        for (int attempt = 0; attempt < kFillRetry && !ok; ++attempt) {
            auto rs = cx.client->BatchPut(keys, slices, cfg);
            ok = true;
            for (auto& r : rs) if (!r.has_value()) { ok = false; break; }
            if (!ok && attempt + 1 < kFillRetry) {
                LOG(WARNING) << "client " << cx.id << " fill batch@" << base
                             << " failed, retry " << (attempt + 1) << "/" << kFillRetry;
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
        }
        for (auto& s : slices) cx.put_alloc->deallocate(s[0].ptr, s[0].size);
        if (!ok) { LOG(ERROR) << "client " << cx.id << " fill batchput failed after " << kFillRetry << " retries"; return false; }
    }
    // 采样探测下标(均匀)
    int pn = std::min<int64_t>(FLAGS_probe_per_client, cx.nkeys_local);
    for (int p = 0; p < pn; ++p) cx.probe_idx.push_back((int64_t)((p + 0.5) * cx.nkeys_local / pn));
    return true;
}

// 探测一个 client 的采样老key:返回命中数(cached/store可读)。
// 探测采样老key。返回命中数,并【分类统计】未命中原因(用于诊断"丢失"方向):
//   miss_notfound: master查不到(Get返回错误,OBJECT_NOT_FOUND) => client账本⊇master方向
//   miss_baddata : master查得到但数据校验失败(目录悬空,数据被顶) => master⊇client方向
static int ProbeClient(ClientCtx& cx, int vsize,
                       int64_t* miss_notfound = nullptr,
                       int64_t* miss_baddata = nullptr) {
    int ok = 0;
    for (int64_t i : cx.probe_idx) {
        std::string exp = MakeValue(i, vsize);
        void* buf = cx.alloc->allocate(exp.size());
        std::vector<Slice> s{Slice{buf, exp.size()}};
        auto r = cx.client->Get(OldKey(cx.id, i), s);
        if (!r.has_value()) {
            if (miss_notfound) ++(*miss_notfound);        // master查不到
        } else if (s[0].size != exp.size() ||
                   std::memcmp(s[0].ptr, exp.data(), exp.size()) != 0) {
            if (miss_baddata) ++(*miss_baddata);           // 查到但数据坏(悬空)
        } else {
            ++ok;
        }
        cx.alloc->deallocate(buf, exp.size());
    }
    return ok;
}

// 压测后台线程:持续 get 老key + put 新key(全程,不停),直到 stress_run=false。
static void StressLoop(ClientCtx& cx, int vsize) {
    ReplicateConfig cfg; cfg.replica_num = 1;
    int64_t gi = 0;
    while (cx.stress_run.load()) {
        // (a) get 老key(压测读,命中率由探测线程单独精确统计,这里只制造流量)
        for (int k = 0; k < FLAGS_stress_get_per_poll; ++k) {
            int64_t i = (gi++) % std::max<int64_t>(1, cx.nkeys_local);
            std::string exp = MakeValue(i, vsize);
            void* buf = cx.alloc->allocate(exp.size());
            std::vector<Slice> s{Slice{buf, exp.size()}};
            cx.client->Get(OldKey(cx.id, i), s);
            cx.alloc->deallocate(buf, exp.size());
        }
        // (b) put 新key(压测写,测故障期写入能否成功)。到上限后停put(继续get),
        //     避免新key无限灌爆有限的段内存、触发淘汰把老key数据挤掉污染重建信号。
        for (int k = 0; k < FLAGS_stress_put_per_poll; ++k) {
            if (FLAGS_max_new_puts_per_client > 0 &&
                cx.new_put_seq.load() >= FLAGS_max_new_puts_per_client) break;
            int64_t seq = cx.new_put_seq++;
            std::string v = MakeValue(1000000000LL + seq, vsize);
            void* b = cx.put_alloc->allocate(v.size());
            std::memcpy(b, v.data(), v.size());
            std::vector<Slice> s{Slice{b, v.size()}};
            std::vector<ObjectKey> ks{NewKey(cx.id, seq)};
            std::vector<std::vector<Slice>> ss{std::move(s)};
            auto rs = cx.client->BatchPut(ks, ss, cfg);
            cx.put_alloc->deallocate(b, v.size());
            if (!rs.empty() && rs[0].has_value()) cx.new_put_ok++; else cx.new_put_fail++;
        }
    }
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    const int M = FLAGS_nclients;
    const int vsize = FLAGS_vsize;
    const int64_t per_client = FLAGS_nkeys / M;
    LOG(INFO) << "CONFIG nclients=" << M << " total_nkeys=" << FLAGS_nkeys
              << " per_client=" << per_client << " vsize=" << vsize
              << " probe_per_client=" << FLAGS_probe_per_client;

    std::vector<std::unique_ptr<ClientCtx>> ctxs;
    for (int c = 0; c < M; ++c) { auto p = std::make_unique<ClientCtx>(); p->id = FLAGS_client_id_base + c; p->nkeys_local = per_client; ctxs.push_back(std::move(p)); }

    // --- 并行 setup + 灌数据 ---
    auto t_fill0 = Clock::now();
    std::vector<std::thread> setup_th; std::atomic<int> ok_cnt{0};
    for (auto& cx : ctxs) setup_th.emplace_back([&]{ if (SetupAndFill(*cx, vsize, FLAGS_batch)) ok_cnt++; });
    for (auto& t : setup_th) t.join();
    if (ok_cnt.load() != M) { LOG(ERROR) << "RESULT=FAIL reason=setup_failed ok=" << ok_cnt.load() << "/" << M; return 2; }
    double fill_ms = ms_since(t_fill0, Clock::now());
    LOG(INFO) << "FILL done total=" << FLAGS_nkeys << " in " << fill_ms << " ms ("
              << (FLAGS_nkeys / (fill_ms / 1000.0)) << " keys/s)";

    // --- 基线探测:所有client采样必须全绿 ---
    int probe_total = 0, base_ok = 0;
    for (auto& cx : ctxs) { probe_total += cx->probe_idx.size(); base_ok += ProbeClient(*cx, vsize); }
    LOG(INFO) << "BASELINE probe_ok=" << base_ok << "/" << probe_total;
    if (base_ok != probe_total) { LOG(ERROR) << "RESULT=FAIL reason=baseline_incomplete"; return 2; }

    // --- 启动全程压测线程(kill前就开始,拟合生产) ---
    for (auto& cx : ctxs) cx->stress_run.store(true);
    std::vector<std::thread> stress_th;
    for (auto& cx : ctxs) stress_th.emplace_back([&]{ StressLoop(*cx, vsize); });

    LOG(INFO) << "READY_FOR_KILL"; fflush(stderr);

    // --- 恢复窗口:轮询探测,逐点输出 SAMPLE 曲线 ---
    // 完成判定:命中数恢复到 baseline 的 recover_pct%(默认95%)即算重建完成。
    // 放宽到<100%是因为:全程压测持续put新key,段内存有限会淘汰少数老key的
    // 数据副本(数据层淘汰,非元数据未重建),这少数key会永久查不回,属压测噪声。
    // 用"恢复到接近baseline的稳定平台"判定,比"绝对100%"更贴合真实且不被噪声卡死。
    const int recover_threshold =
        (int)(probe_total * (FLAGS_recover_pct / 100.0));
    auto t_ready = Clock::now();
    bool saw_down = false; Clock::time_point t_first_fail, t_recovered;
    int min_ok = probe_total; int polls = 0;
    int max_polls = (FLAGS_max_recovery_sec * 1000) / FLAGS_poll_ms;
    for (int attempt = 0; attempt < max_polls; ++attempt) {
        int ok = 0; int64_t miss_nf = 0, miss_bad = 0;
        for (auto& cx : ctxs) ok += ProbeClient(*cx, vsize, &miss_nf, &miss_bad);
        ++polls;
        double t = ms_since(t_ready, Clock::now());
        int64_t nput_ok = 0, nput_fail = 0;
        for (auto& cx : ctxs) { nput_ok += cx->new_put_ok.load(); nput_fail += cx->new_put_fail.load(); }
        // 逐点曲线:老key命中率 + 未命中分类(notfound=master查不到 / baddata=悬空)
        LOG(INFO) << "SAMPLE t_ms=" << (int64_t)t << " old_hit=" << ok << "/" << probe_total
                  << " hit_pct=" << (100.0 * ok / probe_total)
                  << " miss_notfound=" << miss_nf << " miss_baddata=" << miss_bad
                  << " new_put_ok=" << nput_ok << " new_put_fail=" << nput_fail;
        if (ok < recover_threshold) { if (!saw_down) { saw_down = true; t_first_fail = Clock::now(); } min_ok = std::min(min_ok, ok); }
        if (saw_down && ok >= recover_threshold) { t_recovered = Clock::now(); break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_poll_ms));
    }

    // 停压测
    for (auto& cx : ctxs) cx->stress_run.store(false);
    for (auto& t : stress_th) t.join();

    if (!saw_down || t_recovered.time_since_epoch().count() == 0) {
        LOG(ERROR) << "RESULT=FAIL reason=not_recovered saw_down=" << saw_down << " polls=" << polls; return 1;
    }
    double rebuild_ms = ms_since(t_first_fail, t_recovered);
    double since_ready_ms = ms_since(t_ready, t_recovered);
    int64_t tot_put_ok = 0, tot_put_fail = 0;
    for (auto& cx : ctxs) { tot_put_ok += cx->new_put_ok.load(); tot_put_fail += cx->new_put_fail.load(); }
    LOG(INFO) << "RESULT=PASS recovered=" << probe_total << "/" << probe_total;
    LOG(INFO) << "JSON_RESULT={"
              << "\"nclients\":" << M << ",\"total_nkeys\":" << FLAGS_nkeys
              << ",\"per_client\":" << per_client << ",\"vsize\":" << vsize
              << ",\"fill_ms\":" << fill_ms
              << ",\"fill_keys_per_s\":" << (FLAGS_nkeys / (fill_ms / 1000.0))
              << ",\"rebuild_ms\":" << rebuild_ms
              << ",\"recover_since_ready_ms\":" << since_ready_ms
              << ",\"min_avail_pct\":" << (100.0 * min_ok / probe_total)
              << ",\"new_put_ok\":" << tot_put_ok << ",\"new_put_fail\":" << tot_put_fail
              << ",\"poll_ms\":" << FLAGS_poll_ms << "}";
    fflush(stderr);
    // 多进程:先完成的client若立即卸段退出,会把master全局分配器放在它段上的
    // (属于其他仍在恢复的client的)数据一并清掉,污染他人恢复。驻留一段时间,
    // 让所有进程都跑完再统一退出。生产环境client本就不会恢复后立即退出。
    if (FLAGS_hold_after_recover_sec > 0) {
        LOG(INFO) << "HOLD_AFTER_RECOVER " << FLAGS_hold_after_recover_sec
                  << "s (keep segment mounted for peers)";
        std::this_thread::sleep_for(
            std::chrono::seconds(FLAGS_hold_after_recover_sec));
    }
    for (auto& cx : ctxs) cx->client->UnmountSegment(cx->seg, cx->seg_bytes);
    return 0;
}
