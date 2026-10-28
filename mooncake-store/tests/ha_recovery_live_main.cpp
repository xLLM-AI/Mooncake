// Live HA recovery test: standalone client that connects to a REAL, separate
// mooncake_master PROCESS (not InProcMaster). Driven by an external shell
// script (ha_live_test.sh):
//   1. mount a segment, Put N keys, verify baseline
//   2. print "READY_FOR_KILL", then poll Get until reads fail (master killed)
//      and then succeed again (master restarted + metadata rebuilt)
//   3. print RESULT=PASS/FAIL
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <memory>
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
DEFINE_int32(nkeys, 50, "number of keys to put");

using namespace mooncake;

static constexpr size_t kSeg = 128ull * 1024 * 1024;
static constexpr size_t kAlloc = 64ull * 1024 * 1024;

static tl::expected<void, ErrorCode> PutStr(std::shared_ptr<Client>& c,
                                            SimpleAllocator& a,
                                            const std::string& k,
                                            const std::string& v) {
    void* buf = a.allocate(v.size());
    std::memcpy(buf, v.data(), v.size());
    std::vector<Slice> s{Slice{buf, v.size()}};
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto r = c->Put(k, s, cfg);
    a.deallocate(buf, v.size());
    return r;
}

static bool GetVerify(std::shared_ptr<Client>& c, SimpleAllocator& a,
                      const std::string& k, const std::string& exp) {
    void* buf = a.allocate(exp.size());
    std::vector<Slice> s{Slice{buf, exp.size()}};
    auto r = c->Get(k, s);
    bool ok = r.has_value() && s[0].size == exp.size() &&
              std::memcmp(s[0].ptr, exp.data(), exp.size()) == 0;
    a.deallocate(buf, exp.size());
    return ok;
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    std::vector<std::string> keys, vals;
    for (int i = 0; i < FLAGS_nkeys; ++i) {
        keys.push_back("live_key_" + std::to_string(i));
        vals.push_back("live_value_payload_" + std::to_string(i) +
                       std::string(200, 'x'));
    }

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
    auto mnt = client->MountSegment(seg, kSeg, FLAGS_protocol);
    if (!mnt.has_value()) {
        LOG(ERROR) << "RESULT=FAIL reason=mount_failed";
        return 2;
    }

    for (int i = 0; i < FLAGS_nkeys; ++i) {
        auto r = PutStr(client, *alloc, keys[i], vals[i]);
        if (!r.has_value()) {
            LOG(ERROR) << "RESULT=FAIL reason=put_failed key=" << keys[i];
            return 2;
        }
    }
    int base_ok = 0;
    for (int i = 0; i < FLAGS_nkeys; ++i)
        if (GetVerify(client, *alloc, keys[i], vals[i])) ++base_ok;
    LOG(INFO) << "BASELINE ok=" << base_ok << "/" << FLAGS_nkeys;
    if (base_ok != FLAGS_nkeys) {
        LOG(ERROR) << "RESULT=FAIL reason=baseline_incomplete";
        return 2;
    }

    LOG(INFO) << "READY_FOR_KILL";
    fflush(stderr);

    bool saw_down = false;
    int final_ok = -1;
    for (int attempt = 0; attempt < 240; ++attempt) {
        int ok = 0;
        for (int i = 0; i < FLAGS_nkeys; ++i)
            if (GetVerify(client, *alloc, keys[i], vals[i])) ++ok;
        if (ok < FLAGS_nkeys) saw_down = true;
        if (saw_down && ok == FLAGS_nkeys) {
            final_ok = ok;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    if (final_ok == FLAGS_nkeys) {
        LOG(INFO) << "RESULT=PASS recovered=" << final_ok << "/" << FLAGS_nkeys
                  << " zero-recompute-after-master-kill-restart";
        return 0;
    }
    LOG(ERROR) << "RESULT=FAIL reason=not_recovered saw_down=" << saw_down;
    return 1;
}
