// Unit tests for the client-driven metadata rebuild HA feature. After the
// master process crashes and restarts empty, each client resends its locally
// tracked key-to-location table so the master rebuilds its metadata with zero
// recomputation.
//
// Tests:
//   RebuildObjectMetadataAfterMasterRestart: a single client rebuilds its
//     object metadata after a master restart and reads data back with no
//     recompute.
//   RebuiltMetadataPointsToRealData: rebuilt metadata must point to the real,
//     correct data, not to some other key's bytes.
//   LazyDelete_RemovedButNotReused_MayRevive: a removed key may revive after
//     rebuild if its space was not reused, and its data is still correct.
//   CrossClientRebuildViaNotify: client A stores onto client B's segment, so B
//     records the replica via notify and rebuilds it after a master restart.
//   NotifyRetryBackstopRedeliversDroppedNotify: a failed notify is parked and
//     retried by a background thread until the owner receives it.
//   RemovedKeySpaceReuseNoStaleMapping: once a removed key's space is reused,
//     the old key must not revive and point at the new key's address.
//   MultiReplicaMergedOnRebuild: with replica_num=2, the master merges the two
//     replicas reported by different owners back into two on rebuild.

#include <gtest/gtest.h>
#include <future>
#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "types.h"
#include "utils.h"  // allocate_buffer_allocator_memory, SimpleAllocator
#include "test_server_helpers.h"  // InProcMaster, InProcMasterConfigBuilder
#include "default_config.h"
#include "tenant_quota_policy_store.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");

namespace mooncake {
struct ClientTestAccess {
    static auto Move(Client& client, const std::string& key,
                     const std::string& source, const std::string& target) {
        return client.Move(key, source, target);
    }
    static auto Move(Client& client, const std::string& key,
                     const std::string& tenant_id, const std::string& source,
                     const std::string& target) {
        return client.Move(key, tenant_id, source, target);
    }
};
namespace testing {

namespace {

class ScopedRebuildSetting {
   public:
    ScopedRebuildSetting() {
        if (const char* value = std::getenv("MC_ENABLE_HA_REBUILD"))
            previous_ = value;
    }
    ~ScopedRebuildSetting() {
        if (previous_)
            ::setenv("MC_ENABLE_HA_REBUILD", previous_->c_str(), 1);
        else
            ::unsetenv("MC_ENABLE_HA_REBUILD");
    }

   private:
    std::optional<std::string> previous_;
};

TEST(ClientRebuildSwitchTest, DisabledUnlessExplicitlyEnabled) {
    ScopedRebuildSetting setting;
    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    for (const std::string value : {"", "0", "1"}) {
        if (value.empty())
            ::unsetenv("MC_ENABLE_HA_REBUILD");
        else
            ::setenv("MC_ENABLE_HA_REBUILD", value.c_str(), 1);
        auto client = Client::Create(
            "127.0.0.1:" + std::to_string(getFreeTcpPort()), "P2PHANDSHAKE",
            FLAGS_protocol, std::nullopt, master.master_address());
        ASSERT_TRUE(client);
        EXPECT_EQ((*client)->IsHaRebuildEnabled(), value == "1");
        EXPECT_TRUE((*client)->LocalReplicaSnapshotForTest().empty());
    }
}

// Wait until all keys can be read back, not just a single probe key.
// If RebuildMetadata pushes keys one by one instead of atomically, a probe
// key may arrive while keys[N-1] has not, so asserting immediately would
// wrongly report a live key as lost. Requiring every key to Get successfully
// works for both incremental and atomic implementations. Returns false if
// some key is still not rebuilt when the timeout is reached.
bool WaitForAllKeysRebuilt(std::shared_ptr<Client>& client,
                           SimpleAllocator& allocator,
                           const std::vector<std::string>& keys,
                           const std::vector<std::string>& values,
                           int max_attempts = 40, int interval_ms = 500) {
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        bool all_ok = true;
        for (size_t i = 0; i < keys.size(); ++i) {
            void* buf = allocator.allocate(values[i].size());
            std::vector<Slice> slices{Slice{buf, values[i].size()}};
            auto res = client->Get(keys[i], slices);
            const bool matches =
                res && slices.front().size == values[i].size() &&
                std::memcmp(buf, values[i].data(), values[i].size()) == 0;
            allocator.deallocate(buf, values[i].size());
            if (!matches) {
                all_ok = false;
                break;
            }
        }
        if (all_ok) {
            LOG(INFO) << "All " << keys.size() << " keys rebuilt after "
                      << attempt << " polls";
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
    return false;
}

tl::expected<void, ErrorCode> PutString(std::shared_ptr<Client>& client,
                                        SimpleAllocator& allocator,
                                        const std::string& key,
                                        const std::string& value) {
    void* buf = allocator.allocate(value.size());
    std::memcpy(buf, value.data(), value.size());
    std::vector<Slice> slices{Slice{buf, value.size()}};
    ReplicateConfig config;
    config.replica_num = 1;
    auto res = client->Put(key, slices, config);
    allocator.deallocate(buf, value.size());
    return res;
}

// Get the value and compare it byte for byte against the expected content.
bool GetAndVerify(std::shared_ptr<Client>& client, SimpleAllocator& allocator,
                  const std::string& key, const std::string& expected) {
    void* buf = allocator.allocate(expected.size());
    std::vector<Slice> slices{Slice{buf, expected.size()}};
    auto res = client->Get(key, slices);
    bool ok = res.has_value() && slices[0].size == expected.size() &&
              std::memcmp(slices[0].ptr, expected.data(), expected.size()) == 0;
    allocator.deallocate(buf, expected.size());
    return ok;
}

}  // namespace

class ClientMetadataRebuildTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // In-process non-HA master (auto-selected port).
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        master_address_ = master_.master_address();

        // Create the client.
        local_hostname_ = "127.0.0.1:19100";
        auto client_opt =
            Client::Create(local_hostname_, "P2PHANDSHAKE", FLAGS_protocol,
                           std::nullopt, master_address_);
        ASSERT_TRUE(client_opt.has_value()) << "Failed to create client";
        client_ = client_opt.value();

        // Data buffer allocator, registered as local memory. Put must have the
        // buffer registered before it can transfer from it.
        allocator_ = std::make_unique<SimpleAllocator>(kAllocSize);
        auto reg = client_->RegisterLocalMemory(
            allocator_->getBase(), kAllocSize, "cpu:0", false, false);
        ASSERT_TRUE(reg.has_value()) << "RegisterLocalMemory failed";

        // Mount a segment where data will land, using the three-argument
        // overload that takes a protocol.
        seg_ptr_ = allocate_buffer_allocator_memory(kSegmentSize);
        ASSERT_NE(seg_ptr_, nullptr);
        auto mount =
            client_->MountSegment(seg_ptr_, kSegmentSize, FLAGS_protocol);
        ASSERT_TRUE(mount.has_value()) << toString(mount.error());
    }

    void TearDown() override {
        if (client_ && seg_ptr_) {
            client_->UnmountSegment(seg_ptr_, kSegmentSize);
        }
        master_.Stop();
    }

    // Simulate a master failure by bringing up an empty new master on the same
    // port, which is what lets the client reconnect back to it.
    void RestartMasterEmpty() {
        master_.Stop();
        std::this_thread::sleep_for(
            std::chrono::seconds(3));  // let heartbeats fail
        ASSERT_TRUE(master_.Start(
            InProcMasterConfigBuilder()
                .set_rpc_port(master_.rpc_port())
                .set_http_metrics_port(master_.http_metrics_port())
                .build()));
    }

    static constexpr size_t kSegmentSize = 128 * 1024 * 1024;  // 128MB segment
    static constexpr size_t kAllocSize = 64 * 1024 * 1024;     // 64MB buffer

    InProcMaster master_;
    std::string master_address_;
    std::string local_hostname_;
    std::shared_ptr<Client> client_;
    void* seg_ptr_ = nullptr;
    std::unique_ptr<SimpleAllocator> allocator_;
};

TEST_F(ClientMetadataRebuildTest, UpsertCreationAndReplacementSurviveRecovery) {
    const std::vector<std::string> keys{"upsert-created", "upsert-inplace",
                                        "upsert-resized"};
    const std::vector<std::string> expected{"created", "new-value",
                                            std::string(4096, 'R')};
    ASSERT_TRUE(PutString(client_, *allocator_, keys[1], "old-value"));
    ASSERT_TRUE(PutString(client_, *allocator_, keys[2], "old"));
    for (size_t index = 0; index < keys.size(); ++index) {
        void* buffer = allocator_->allocate(expected[index].size());
        std::memcpy(buffer, expected[index].data(), expected[index].size());
        std::vector<Slice> slices{{buffer, expected[index].size()}};
        auto result = client_->Upsert(keys[index], slices, ReplicateConfig{});
        allocator_->deallocate(buffer, expected[index].size());
        ASSERT_TRUE(result) << toString(result.error());
    }
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, expected));
    for (size_t index = 0; index < keys.size(); ++index)
        EXPECT_TRUE(
            GetAndVerify(client_, *allocator_, keys[index], expected[index]));
}

TEST_F(ClientMetadataRebuildTest, UpsertReplaysAuthoritativePinsAndGroup) {
    for (const auto& key : {"pinned-inplace", "pinned-resized"}) {
        const std::string original = "original";
        void* buffer = allocator_->allocate(original.size());
        std::memcpy(buffer, original.data(), original.size());
        std::vector<Slice> slices{Slice{buffer, original.size()}};
        ReplicateConfig pinned;
        pinned.with_hard_pin = true;
        pinned.with_soft_pin = true;
        pinned.group_ids = std::vector<std::string>{"pin-group"};
        ASSERT_TRUE(client_->Put(key, slices, pinned));
        allocator_->deallocate(buffer, original.size());
        const std::string replacement = std::string(key) == "pinned-inplace"
                                            ? "replaced"
                                            : std::string(1024, 'R');
        buffer = allocator_->allocate(replacement.size());
        std::memcpy(buffer, replacement.data(), replacement.size());
        slices = {Slice{buffer, replacement.size()}};
        ASSERT_TRUE(client_->Upsert(key, slices, ReplicateConfig{}));
        allocator_->deallocate(buffer, replacement.size());
    }
    const auto snapshot = client_->LocalReplicaSnapshotForTest();
    ASSERT_EQ(snapshot.size(), 2);
    for (const auto& entry : snapshot) {
        EXPECT_TRUE(entry.with_hard_pin);
        EXPECT_EQ(entry.group_id, "pin-group");
        EXPECT_EQ(entry.with_soft_pin, entry.key == "pinned-resized");
    }
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_,
                                      {"pinned-inplace", "pinned-resized"},
                                      {"replaced", std::string(1024, 'R')}));
    for (const auto& key : {"pinned-inplace", "pinned-resized"}) {
        const auto removed = client_->Remove(key);
        EXPECT_FALSE(removed);
    }
}

TEST_F(ClientMetadataRebuildTest,
       BatchUpsertCreationAndReplacementSurviveRecovery) {
    const std::vector<std::string> keys{"batch-created", "batch-inplace",
                                        "batch-resized"};
    const std::vector<std::string> expected{"created", "new-value",
                                            std::string(4096, 'B')};
    ASSERT_TRUE(PutString(client_, *allocator_, keys[1], "old-value"));
    ASSERT_TRUE(PutString(client_, *allocator_, keys[2], "old"));
    std::vector<std::vector<Slice>> batches;
    for (const auto& value : expected) {
        void* buffer = allocator_->allocate(value.size());
        std::memcpy(buffer, value.data(), value.size());
        batches.push_back({{buffer, value.size()}});
    }
    auto results = client_->BatchUpsert(keys, batches, ReplicateConfig{});
    for (size_t index = 0; index < batches.size(); ++index)
        allocator_->deallocate(batches[index].front().ptr,
                               expected[index].size());
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) ASSERT_TRUE(result);
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, expected));
    for (size_t index = 0; index < keys.size(); ++index)
        EXPECT_TRUE(
            GetAndVerify(client_, *allocator_, keys[index], expected[index]));
}

// ---------------------------------------------------------------------------
// Test 1 (core): after a master restart the client rebuilds object metadata
// and the data reads back with no recomputation.
// ---------------------------------------------------------------------------
TEST_F(ClientMetadataRebuildTest, RebuildObjectMetadataAfterMasterRestart) {
    const int kNumKeys = 50;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("rebuild_key_" + std::to_string(i));
        values.push_back("rebuild_value_" + std::to_string(i));
    }
    for (int i = 0; i < kNumKeys; ++i) {
        auto r = PutString(client_, *allocator_, keys[i], values[i]);
        ASSERT_TRUE(r.has_value())
            << "Put failed " << keys[i] << ": " << toString(r.error());
    }
    // Baseline: everything reads back before the restart.
    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_TRUE(GetAndVerify(client_, *allocator_, keys[i], values[i]))
            << "Baseline Get failed " << keys[i];

    RestartMasterEmpty();

    // Wait for all keys to rebuild, not just one probe, to avoid a false
    // failure under an incremental implementation.
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, values))
        << "Metadata not fully rebuilt in time: new master still returns "
           "NOT_FOUND, or the resend/rebuild path is not working";

    // Core assertion: after rebuild every key reads back with matching content
    // (no recomputation).
    for (int i = 0; i < kNumKeys; ++i)
        EXPECT_TRUE(GetAndVerify(client_, *allocator_, keys[i], values[i]))
            << "After rebuild, Get/verify failed " << keys[i];
}

// ---------------------------------------------------------------------------
// Test 2 (guard against fake recovery): rebuilt metadata must point to the
// real and correct data, never to another key's bytes.
// ---------------------------------------------------------------------------
TEST_F(ClientMetadataRebuildTest, RebuiltMetadataPointsToRealData) {
    std::vector<std::pair<std::string, std::string>> kv = {
        {"distinct_A", std::string(1024, 'A')},
        {"distinct_B", std::string(2048, 'B')},
        {"distinct_C", std::string(512, 'C')},
        {"distinct_D", std::string(4096, 'D')},
    };
    std::vector<std::string> keys, values;
    for (auto& [k, v] : kv) {
        keys.push_back(k);
        values.push_back(v);
    }

    for (auto& [k, v] : kv) {
        auto r = PutString(client_, *allocator_, k, v);
        ASSERT_TRUE(r.has_value()) << "Put failed " << k;
    }
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, values))
        << "Metadata not fully rebuilt in time";
    for (auto& [k, v] : kv)
        EXPECT_TRUE(GetAndVerify(client_, *allocator_, k, v))
            << "Rebuilt metadata for '" << k
            << "' points to wrong/corrupt data";
}

// ---------------------------------------------------------------------------
// Test 3 (lazy-delete semantics): a key that was Removed but whose space was
// not reused may revive after rebuild, and the revived data is still correct
// because Remove does not wipe memory. This is the expected lazy-delete
// behavior, not a bug.
// ---------------------------------------------------------------------------
// NOTE on the semantic change: an earlier eager-delete version asserted that a
// removed key does not revive. It is now lazy-delete: Remove leaves the client
// local table untouched, so a removed-but-not-reused key revives and still
// points to the correct old data. This test checks that live keys work
// normally and that removed-but-not-reused keys revive with intact data, which
// is acceptable. The "old key does not revive after reuse" property is covered
// by test 5, which is the correctness that must be guaranteed.
TEST_F(ClientMetadataRebuildTest, LazyDelete_RemovedButNotReused_MayRevive) {
    const int kNumKeys = 20;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("mix_key_" + std::to_string(i));
        values.push_back("mix_value_" + std::to_string(i));
        auto r = PutString(client_, *allocator_, keys.back(), values.back());
        ASSERT_TRUE(r.has_value()) << "Put failed " << keys.back();
    }
    // Remove half of the keys (even indices), with force=true to bypass the
    // lease. No new Put happens after the removes, so the space is not reused.
    for (int i = 0; i < kNumKeys; i += 2) {
        auto r = client_->Remove(keys[i], /*force=*/true);
        ASSERT_TRUE(r.has_value())
            << "Remove failed " << keys[i] << ": " << toString(r.error());
    }
    RestartMasterEmpty();

    // Lazy-delete: every key (including the removed ones) may rebuild, so wait
    // for all of them.
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, values))
        << "Rebuild did not complete (under lazy-delete a removed-but-not-"
           "reused key should also be able to revive)";

    // Assertion: every key, removed or not, reads back with correct content,
    // which is the expected lazy-delete outcome. A revived removed key is
    // acceptable; what matters is that the data is intact (Remove does not
    // wipe memory).
    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_TRUE(GetAndVerify(client_, *allocator_, keys[i], values[i]))
            << (i % 2 == 0 ? "removed-not-reused key data should be correct "
                             "after revive: "
                           : "live key data should be correct: ")
            << keys[i];
    }
}

// ---------------------------------------------------------------------------
// Test 4 (cross-client / notify path, the core multi-client scenario):
// Client A mounts no segment and client B does, so A's Put data necessarily
// lands on B's segment because it is the only segment in the global pool.
// Those keys are recorded on B via an A-to-B notify, and after a master
// restart B (where the data physically lives) resends them for rebuild. This
// is the test that best represents a real multi-client scenario and where the
// core value of the feature lies.
//
// Reliable way to force data onto B's segment (following the two-client
// pattern in client_integration_test.cpp, where segment_provider_client_
// mounts and test_client_ only RegisterLocalMemory):
//   clientB mounts the only allocatable segment.
//   Client A only RegisterLocalMemory for its local read/write buffer and does
//     not MountSegment.
// At PutStart the global pool has only B's segment, so data lands on B
// deterministically and the test is not flaky.
//
// NOTE: this depends on notify send/receive being implemented. Without it the
// data is on B, A's local table has none of these keys, and B was not
// notified, so after the restart nobody resends them and the test fails. That
// failure is exactly the evidence that notify is being exercised.
// NOTE: uses its own fixture that brings up A and B, rather than reusing the
// single-client ClientMetadataRebuildTest.
class ClientCrossNotifyTest : public ::testing::Test {
   protected:
    virtual InProcMasterConfig MasterConfig() const {
        return InProcMasterConfigBuilder().build();
    }

    void SetUp() override {
        ASSERT_TRUE(master_.Start(MasterConfig()));
        master_address_ = master_.master_address();

        // clientB is the segment owner and mounts the only allocatable
        // segment.
        auto b = Client::Create("127.0.0.1:19201", "P2PHANDSHAKE",
                                FLAGS_protocol, std::nullopt, master_address_);
        ASSERT_TRUE(b.has_value());
        clientB_ = b.value();
        segB_ = allocate_buffer_allocator_memory(kSeg);
        ASSERT_NE(segB_, nullptr);
        ASSERT_TRUE(
            clientB_->MountSegment(segB_, kSeg, FLAGS_protocol).has_value());

        // Client A is the writer and only registers a local read/write buffer;
        // it does not mount a segment.
        auto a = Client::Create("127.0.0.1:19202", "P2PHANDSHAKE",
                                FLAGS_protocol, std::nullopt, master_address_);
        ASSERT_TRUE(a.has_value());
        clientA_ = a.value();
        allocA_ = std::make_unique<SimpleAllocator>(kAlloc);
        ASSERT_TRUE(clientA_
                        ->RegisterLocalMemory(allocA_->getBase(), kAlloc,
                                              "cpu:0", false, false)
                        .has_value());
        // B also needs a local read/write buffer, used when it Gets to verify.
        allocB_ = std::make_unique<SimpleAllocator>(kAlloc);
        ASSERT_TRUE(clientB_
                        ->RegisterLocalMemory(allocB_->getBase(), kAlloc,
                                              "cpu:0", false, false)
                        .has_value());
    }

    void TearDown() override {
        if (clientB_ && segB_) clientB_->UnmountSegment(segB_, kSeg);
        master_.Stop();
    }

    void RestartMasterEmpty() {
        master_.Stop();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        auto config = MasterConfig();
        config.rpc_port = master_.rpc_port();
        config.http_metrics_port = master_.http_metrics_port();
        ASSERT_TRUE(master_.Start(config));
    }

    static constexpr size_t kSeg = 128 * 1024 * 1024;
    static constexpr size_t kAlloc = 64 * 1024 * 1024;
    InProcMaster master_;
    std::string master_address_;
    std::shared_ptr<Client> clientA_, clientB_;
    void* segB_ = nullptr;
    std::unique_ptr<SimpleAllocator> allocA_, allocB_;
};

TEST_F(ClientCrossNotifyTest, BatchAckTimeoutOnlyFailsKeysOnStalledOwner) {
    ASSERT_TRUE(PutString(clientA_, *allocA_, "remote-existing", "old-remote"));
    void* local_segment = allocate_buffer_allocator_memory(kSeg);
    ASSERT_NE(local_segment, nullptr);
    ASSERT_TRUE(clientA_->MountSegment(local_segment, kSeg, FLAGS_protocol));
    void* buffer = allocA_->allocate(128);
    std::memset(buffer, 'L', 128);
    std::vector<Slice> slices{Slice{buffer, 128}};
    ReplicateConfig local;
    local.preferred_segments = {"127.0.0.1:19202"};
    ASSERT_TRUE(clientA_->Put("local-existing", slices, local));
    clientA_->SetRebuildNotifyTimeoutForTest(std::chrono::milliseconds(300));
    clientB_->PauseRebuildNotifiesForTest(true);
    std::vector<std::vector<Slice>> values{slices, slices, slices};
    ReplicateConfig remote;
    remote.preferred_segments = {"127.0.0.1:19201"};
    const auto results = clientA_->BatchPut(
        {"remote-existing", "local-existing", "remote-new"}, values, remote);
    ASSERT_EQ(results.size(), 3);
    EXPECT_FALSE(results[0]);
    if (!results[0]) {
        EXPECT_EQ(results[0].error(), ErrorCode::RPC_TIMEOUT);
    }
    EXPECT_TRUE(results[1]);
    EXPECT_FALSE(results[2]);
    if (!results[2]) {
        EXPECT_EQ(results[2].error(), ErrorCode::RPC_TIMEOUT);
    }
    EXPECT_EQ(clientA_->PendingNotifyEntryCountForTest(), 2);
    EXPECT_EQ(clientA_->PendingNotifyBucketCountForTest(), 1);
    clientB_->PauseRebuildNotifiesForTest(false);
    for (int attempt = 0;
         attempt < 100 && clientA_->PendingNotifyBucketCountForTest() != 0;
         ++attempt)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(clientA_->PendingNotifyBucketCountForTest(), 0);
    EXPECT_TRUE(
        GetAndVerify(clientA_, *allocA_, "remote-existing", "old-remote"));
    EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, "local-existing",
                             std::string(128, 'L')));
    EXPECT_TRUE(
        GetAndVerify(clientA_, *allocA_, "remote-new", std::string(128, 'L')));
    allocA_->deallocate(buffer, 128);
    EXPECT_TRUE(clientA_->UnmountSegment(local_segment, kSeg));
}

class ClientBatchNotifyTest : public ClientCrossNotifyTest,
                              public ::testing::WithParamInterface<bool> {};

TEST_P(ClientBatchNotifyTest, EnqueuesAllKeysBeforeWaitingForOwner) {
    const bool upsert = GetParam();
    std::vector<std::string> keys;
    std::vector<std::vector<Slice>> values;
    for (size_t index = 0; index < 16; ++index) {
        keys.push_back("batch-notify-" + std::to_string(index));
        void* buffer = allocA_->allocate(128);
        std::memset(buffer, static_cast<int>(index), 128);
        values.push_back({Slice{buffer, 128}});
    }
    clientA_->SetRebuildNotifyTimeoutForTest(std::chrono::seconds(3));
    clientB_->PauseRebuildNotifiesForTest(true);
    auto write = std::async(std::launch::async, [&] {
        return upsert ? clientA_->BatchUpsert(keys, values, ReplicateConfig{})
                      : clientA_->BatchPut(keys, values, ReplicateConfig{});
    });
    for (int attempt = 0;
         attempt < 100 &&
         clientA_->PendingNotifyEntryCountForTest() < keys.size();
         ++attempt)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(clientA_->PendingNotifyEntryCountForTest(), keys.size());
    EXPECT_EQ(clientA_->PendingNotifyBucketCountForTest(), 1);
    EXPECT_EQ(write.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    clientB_->PauseRebuildNotifiesForTest(false);
    const auto results = write.get();
    ASSERT_EQ(results.size(), keys.size());
    for (size_t index = 0; index < keys.size(); ++index) {
        EXPECT_TRUE(results[index]);
        EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, keys[index],
                                 std::string(128, static_cast<char>(index))));
        allocA_->deallocate(values[index].front().ptr, 128);
    }
    EXPECT_EQ(clientA_->PendingNotifyBucketCountForTest(), 0);
    EXPECT_EQ(clientB_->LocalReplicaSnapshotForTest().size(), keys.size());
}

TEST_P(ClientBatchNotifyTest, StalledOwnerUsesOneDeadlineForWholeBatch) {
    const bool upsert = GetParam();
    std::vector<std::string> keys;
    std::vector<std::vector<Slice>> values;
    void* buffer = allocA_->allocate(128);
    std::memset(buffer, 'V', 128);
    for (size_t index = 0; index < 16; ++index) {
        keys.push_back("timeout-notify-" + std::to_string(index));
        values.push_back({Slice{buffer, 128}});
    }
    clientA_->SetRebuildNotifyTimeoutForTest(std::chrono::milliseconds(300));
    clientB_->PauseRebuildNotifiesForTest(true);
    const auto start = std::chrono::steady_clock::now();
    const auto results =
        upsert ? clientA_->BatchUpsert(keys, values, ReplicateConfig{})
               : clientA_->BatchPut(keys, values, ReplicateConfig{});
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::milliseconds(1800));
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::RPC_TIMEOUT);
    }
    EXPECT_EQ(clientA_->PendingNotifyEntryCountForTest(), keys.size());
    EXPECT_EQ(clientA_->PendingNotifyBucketCountForTest(), 1);
    clientB_->PauseRebuildNotifiesForTest(false);
    for (int attempt = 0;
         attempt < 100 && clientA_->PendingNotifyBucketCountForTest() != 0;
         ++attempt)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(clientA_->PendingNotifyBucketCountForTest(), 0);
    for (const auto& key : keys)
        EXPECT_TRUE(
            GetAndVerify(clientA_, *allocA_, key, std::string(128, 'V')));
    allocA_->deallocate(buffer, 128);
}

INSTANTIATE_TEST_SUITE_P(PutAndUpsert, ClientBatchNotifyTest,
                         ::testing::Bool());

class ClientUpsertHotCacheRebuildTest
    : public ClientCrossNotifyTest,
      public ::testing::WithParamInterface<bool> {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MC_STORE_LOCAL_HOT_CACHE_SIZE"))
            previous_size_ = value;
        if (const char* value = std::getenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE"))
            previous_block_size_ = value;
        ::setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", "1048576", 1);
        ::setenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4096", 1);
        ClientCrossNotifyTest::SetUp();
    }
    void TearDown() override {
        ClientCrossNotifyTest::TearDown();
        if (previous_size_)
            ::setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", previous_size_->c_str(),
                     1);
        else
            ::unsetenv("MC_STORE_LOCAL_HOT_CACHE_SIZE");
        if (previous_block_size_)
            ::setenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE",
                     previous_block_size_->c_str(), 1);
        else
            ::unsetenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE");
    }
    std::optional<std::string> previous_size_;
    std::optional<std::string> previous_block_size_;
};

TEST_P(ClientUpsertHotCacheRebuildTest,
       CommitInvalidatesStaleFillBeforeAckFailure) {
    const std::string key = "upsert-ack-failure";
    ASSERT_TRUE(PutString(clientA_, *allocA_, key, "old-value"));
    auto cache = clientA_->GetHotCache();
    ASSERT_NE(cache, nullptr);
    HotCachePutToken stale_token;
    clientA_->SetBeforeUpsertEndForTest([&] {
        stale_token = cache->AcquirePutToken(key);
        auto* block = cache->GetFreeBlock();
        ASSERT_NE(block, nullptr);
        block->key_ = key;
        block->size = 9;
        std::memcpy(block->addr, "old-value", 9);
        ASSERT_TRUE(cache->PutHotKey(block, stale_token));
    });
    clientA_->SetRebuildNotifyTimeoutForTest(std::chrono::milliseconds(300));
    clientB_->PauseRebuildNotifiesForTest(true);
    void* buffer = allocA_->allocate(9);
    std::memcpy(buffer, "new-value", 9);
    std::vector<Slice> slices{Slice{buffer, 9}};
    std::vector<std::vector<Slice>> batches{slices};
    const auto result =
        GetParam()
            ? clientA_->BatchUpsert({key}, batches, ReplicateConfig{}).front()
            : clientA_->Upsert(key, slices, ReplicateConfig{});
    clientA_->SetBeforeUpsertEndForTest({});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::RPC_TIMEOUT);
    EXPECT_FALSE(cache->HasHotKey(key));
    EXPECT_FALSE(cache->IsPutTokenValid(key, stale_token));
    auto* delayed = cache->GetFreeBlock();
    ASSERT_NE(delayed, nullptr);
    delayed->key_ = key;
    delayed->size = 9;
    std::memcpy(delayed->addr, "old-value", 9);
    EXPECT_FALSE(cache->PutHotKey(delayed, stale_token));
    clientB_->PauseRebuildNotifiesForTest(false);
    EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, key, "new-value"));
    allocA_->deallocate(buffer, 9);
}

INSTANTIATE_TEST_SUITE_P(SingleAndBatch, ClientUpsertHotCacheRebuildTest,
                         ::testing::Bool());

TEST_F(ClientCrossNotifyTest, AllMoveEntrypointsRejectWithMasterGateDisabled) {
    const std::string key = "guarded-move";
    ASSERT_TRUE(PutString(clientA_, *allocA_, key, "source-value"));
    auto ordinary = ClientTestAccess::Move(*clientB_, key, "127.0.0.1:19201",
                                           "127.0.0.1:19202");
    ASSERT_FALSE(ordinary);
    EXPECT_EQ(ordinary.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    auto tenant = ClientTestAccess::Move(*clientB_, key, "default",
                                         "127.0.0.1:19201", "127.0.0.1:19202");
    ASSERT_FALSE(tenant);
    EXPECT_EQ(tenant.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    void* target = allocate_buffer_allocator_memory(kSeg);
    ASSERT_NE(target, nullptr);
    ASSERT_TRUE(clientA_->MountSegment(target, kSeg, FLAGS_protocol));
    const auto task = clientA_->CreateMoveTask(
        key, "default", "127.0.0.1:19201", "127.0.0.1:19202");
    ASSERT_TRUE(task);
    bool failed = false;
    for (int attempt = 0; attempt < 100; ++attempt) {
        const auto status = clientA_->QueryTask(*task);
        if (status && is_finished_status(status->status)) {
            EXPECT_EQ(status->status, TaskStatus::FAILED);
            EXPECT_NE(status->message.find(
                          toString(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE)),
                      std::string::npos);
            failed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(failed);
    const auto query = clientA_->Query(key);
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1);
    EXPECT_EQ(query->replicas.front()
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              clientB_->GetTransportEndpoint());
    EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, key, "source-value"));
    EXPECT_TRUE(clientA_->UnmountSegment(target, kSeg));
}

TEST_F(ClientCrossNotifyTest,
       DelayedOwnershipIsAppliedAfterOwnersFirstSnapshot) {
    clientB_->PauseRebuildNotifiesForTest(true);
    const auto before = clientB_->RebuildSnapshotCountForTest();
    auto put = std::async(std::launch::async, [&] {
        return PutString(clientA_, *allocA_, "late-notify", "committed-value");
    });
    for (int attempt = 0;
         attempt < 100 && clientA_->PendingNotifyBucketCountForTest() == 0;
         ++attempt)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_GT(clientA_->PendingNotifyBucketCountForTest(), 0);
    EXPECT_EQ(put.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    EXPECT_TRUE(clientB_->LocalReplicaSnapshotForTest().empty());
    RestartMasterEmpty();
    for (int attempt = 0;
         attempt < 100 && clientB_->RebuildSnapshotCountForTest() == before;
         ++attempt)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_GT(clientB_->RebuildSnapshotCountForTest(), before);
    clientB_->PauseRebuildNotifiesForTest(false);
    EXPECT_TRUE(put.get());
    ASSERT_TRUE(WaitForAllKeysRebuilt(clientA_, *allocA_, {"late-notify"},
                                      {"committed-value"}));
    EXPECT_TRUE(
        GetAndVerify(clientB_, *allocB_, "late-notify", "committed-value"));
    EXPECT_EQ(clientA_->PendingNotifyBucketCountForTest(), 0);
}

class ClientTenantNotifyTest : public ClientCrossNotifyTest {
   protected:
    void SetUp() override {
        policy_path_ = (std::filesystem::temp_directory_path() /
                        ("mooncake-rebuild-tenants-" +
                         UuidToString(generate_uuid()) + ".yaml"))
                           .string();
        TenantQuotaPolicySnapshot policy;
        policy.tenant_quotas = {
            {"default", kSeg}, {"tenant-a", kSeg}, {"tenant-b", kSeg}};
        std::ofstream output(policy_path_);
        output << FormatTenantQuotaPolicyYaml(policy);
        output.close();
        ClientCrossNotifyTest::SetUp();
    }

    void TearDown() override {
        ClientCrossNotifyTest::TearDown();
        std::filesystem::remove(policy_path_);
    }

    InProcMasterConfig MasterConfig() const override {
        auto config = ClientCrossNotifyTest::MasterConfig();
        config.enable_multi_tenants = true;
        config.tenant_quota_connector_uri = policy_path_;
        return config;
    }

    std::string policy_path_;
};

TEST_F(ClientTenantNotifyTest, SameKeyFromTwoTenantsSurvivesOnSharedOwner) {
    std::vector<std::shared_ptr<Client>> writers;
    const std::vector<std::string> tenants{"tenant-a", "tenant-b"};
    const std::vector<std::string> values{std::string(1024, 'A'),
                                          std::string(1024, 'B')};
    for (size_t index = 0; index < tenants.size(); ++index) {
        auto writer =
            Client::Create("127.0.0.1:" + std::to_string(getFreeTcpPort()),
                           "P2PHANDSHAKE", FLAGS_protocol, std::nullopt,
                           master_address_, nullptr, {}, tenants[index]);
        ASSERT_TRUE(writer);
        writers.push_back(*writer);
        ASSERT_TRUE(writers.back()->RegisterLocalMemory(
            allocA_->getBase(), kAlloc, "cpu:0", false, false));
        ASSERT_TRUE(
            PutString(writers.back(), *allocA_, "shared-key", values[index]));
    }
    for (int attempt = 0; attempt < 100; ++attempt) {
        if (clientB_->LocalReplicaSnapshotForTest().size() == 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_EQ(clientB_->LocalReplicaSnapshotForTest().size(), 2);
    RestartMasterEmpty();
    for (size_t index = 0; index < writers.size(); ++index) {
        ASSERT_TRUE(WaitForAllKeysRebuilt(writers[index], *allocA_,
                                          {"shared-key"}, {values[index]}));
        EXPECT_TRUE(GetAndVerify(writers[index], *allocA_, "shared-key",
                                 values[index]));
    }
}

TEST_F(ClientCrossNotifyTest, ReorderedRetryCannotResurrectReusedAddress) {
    const std::string value_a(4096, 'A');
    const std::string value_b(4096, 'B');
    ASSERT_TRUE(PutString(clientA_, *allocA_, "parked-A", value_a));
    auto original = clientA_->Query("parked-A");
    ASSERT_TRUE(original);
    const auto descriptor = original->replicas.front();
    ASSERT_TRUE(clientA_->Remove("parked-A", true));
    ASSERT_TRUE(PutString(clientA_, *allocA_, "live-B", value_b));
    auto replacement = clientA_->Query("live-B");
    ASSERT_TRUE(replacement);
    ASSERT_EQ(
        descriptor.get_memory_descriptor().buffer_descriptor.buffer_address_,
        replacement->replicas.front()
            .get_memory_descriptor()
            .buffer_descriptor.buffer_address_);
    bool received = false;
    for (int attempt = 0; attempt < 100; ++attempt) {
        const auto snapshot = clientB_->LocalReplicaSnapshotForTest();
        if (snapshot.size() == 1 && snapshot.front().key == "live-B") {
            received = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(received);
    const auto& endpoint = descriptor.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_;
    clientA_->ParkNotifyForTest(endpoint, "parked-A", descriptor,
                                value_a.size(), ObjectDataType::UNKNOWN, "",
                                "default");
    for (int attempt = 0; attempt < 100; ++attempt) {
        if (clientA_->PendingNotifyBucketCountForTest() == 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_EQ(clientA_->PendingNotifyBucketCountForTest(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto snapshot = clientB_->LocalReplicaSnapshotForTest();
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_EQ(snapshot.front().key, "live-B");
    RestartMasterEmpty();
    ASSERT_TRUE(
        WaitForAllKeysRebuilt(clientA_, *allocA_, {"live-B"}, {value_b}));
    EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, "live-B", value_b));
    auto stale = clientA_->Query("parked-A");
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(ClientCrossNotifyTest, CrossClientRebuildViaNotify) {
    const int kNumKeys = 30;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("cross_key_" + std::to_string(i));
        values.push_back("cross_val_" + std::to_string(i));
    }
    // 1. A Put (data must land on B's segment; the A-to-B notify makes B
    //    record it).
    for (int i = 0; i < kNumKeys; ++i) {
        auto r = PutString(clientA_, *allocA_, keys[i], values[i]);
        ASSERT_TRUE(r.has_value())
            << "A Put failed " << keys[i] << ": " << toString(r.error());
    }
    // (Optional strong assertion) Confirm the data really landed on B's segment
    // by comparing the replica endpoint from Query against B (see
    // client_integration_test.cpp:419-423). This can be enabled if Client
    // exposes GetTransportEndpoint:
    // { auto q = clientA_->Query(keys[0]); ASSERT_TRUE(q.has_value());
    //   EXPECT_EQ(q.value().replicas[0].get_memory_descriptor()
    //             .buffer_descriptor.transport_endpoint_,
    //             clientB_->GetTransportEndpoint()); }

    // 2. Baseline: A can read back (data is on B's segment, so Get asks the
    //    master for the location and then reads over the transport engine).
    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_TRUE(GetAndVerify(clientA_, *allocA_, keys[i], values[i]))
            << "baseline A Get " << keys[i];

    // 3. Master crashes and restarts empty.
    RestartMasterEmpty();

    // 4. Wait for rebuild. The key point: data is on B's segment and A's local
    //    table has none of these keys, so only B (which recorded them via
    //    notify) can resend them. If notify is not working this times out.
    ASSERT_TRUE(WaitForAllKeysRebuilt(clientA_, *allocA_, keys, values))
        << "Cross-client metadata not rebuilt in time: notify recording or B's "
           "resend path is not working";

    // 5. Core assertion: after rebuild both A and B can read with correct
    //    content.
    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, keys[i], values[i]))
            << "After rebuild, A Get/verify failed " << keys[i];
        EXPECT_TRUE(GetAndVerify(clientB_, *allocB_, keys[i], values[i]))
            << "After rebuild, B Get/verify failed " << keys[i];
    }
}

// ---------------------------------------------------------------------------
// Test 7 (notify reliability backstop): a failed notify must not be dropped
// silently. A failed notify is parked in a pending queue and retried by a
// background thread until the peer receives it. Without this backstop, one
// lost notify makes the owner miss a replica, and that redundancy is silently
// lost when the master rebuilds.
// ---------------------------------------------------------------------------
TEST_F(ClientCrossNotifyTest, NotifyRetryBackstopRedeliversDroppedNotify) {
    // 1. A Puts a carrier key normally (lands on B's segment) and gets its
    //    real Descriptor pointing at B's segment.
    const std::string carrier = "carrier_key";
    const std::string carrier_val = std::string(4096, 'C');
    ASSERT_TRUE(PutString(clientA_, *allocA_, carrier, carrier_val).has_value())
        << "carrier Put failed";
    auto q = clientA_->Query(carrier);
    ASSERT_TRUE(q.has_value() && !q.value().replicas.empty())
        << "carrier Query failed";
    const Replica::Descriptor& carrier_desc = q.value().replicas.front();
    const std::string owner_ep = carrier_desc.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_;

    clientB_->PauseRebuildNotifiesForTest(true);
    clientA_->ParkNotifyForTest(owner_ep, carrier, carrier_desc,
                                carrier_val.size(), ObjectDataType::UNKNOWN, "",
                                "default");

    // 1. It really was parked.
    EXPECT_GE(clientA_->PendingNotifyBucketCountForTest(), 1u)
        << "A failed notify should be parked in the pending queue (without the "
           "backstop it would not be parked)";
    clientB_->PauseRebuildNotifiesForTest(false);

    // 2. Wait for the background thread to redeliver, draining pending to zero.
    bool drained = false;
    for (int i = 0; i < 40 && !drained; ++i) {
        if (clientA_->PendingNotifyBucketCountForTest() == 0) {
            drained = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    EXPECT_TRUE(drained)
        << "pending notify did not drain in time via redelivery: the "
           "background retry backstop is not working";

    // Give B's receive thread a moment to record the redelivered notify into
    // its local table.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 3. Master crashes and restarts empty.
    RestartMasterEmpty();

    bool rebuilt = false;
    for (int i = 0; i < 40 && !rebuilt; ++i) {
        auto qq = clientA_->Query(carrier);
        if (qq.has_value() && !qq.value().replicas.empty()) {
            rebuilt = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    EXPECT_TRUE(rebuilt)
        << "the committed key was not rebuilt after acknowledged redelivery";
    EXPECT_TRUE(GetAndVerify(clientB_, *allocB_, carrier, carrier_val));
}

// ---------------------------------------------------------------------------
// Test 5 (address-reuse overwrite, the core correctness guarantee of
// lazy-delete): once a removed key's space is reused by a new key, the removed
// key must not revive on rebuild and point at the address now owned by the new
// key, which would be silent data corruption. Under lazy-delete this is the
// property that MUST be guaranteed. The "not-reused may revive" case from test
// 3 is acceptable, but "old key still present after reuse" is never
// acceptable.
// ---------------------------------------------------------------------------
// Principle: key_A is removed (lazy-delete leaves the client local table
// untouched), so its space in the segment returns to the allocator freelist
// and a later Put reuses the same address. Recording must overwrite by
// address: the new key must clear the old key_A entry in the local table that
// points at the same (segment, address). RecordLocalReplica does this via
// EraseByAddressLocked. This case is a single client Putting onto its own
// segment, so it takes RecordLocalReplica's self-overwrite path and sends no
// notify. In the cross-client case (A writes to B's segment) the UPSERT notify
// triggers the same RecordLocalReplica overwrite on B. Without the
// address-overwrite, resend after restart would report key_A pointing at the
// old address, which now holds the new key's data, causing silent corruption.
// This test forces reuse and verifies that key_A does not revive (its old
// entry was overwritten) and that the new key data is entirely correct.
TEST_F(ClientMetadataRebuildTest, RemovedKeySpaceReuseNoStaleMapping) {
    const std::string kA = "reuse_victim_A";
    const std::string vA = std::string(4096, 'X');  // reused by same-size key

    // 1. Put key_A and confirm it reads back (it occupies some address in the
    //    segment).
    ASSERT_TRUE(PutString(client_, *allocator_, kA, vA).has_value())
        << "Put key_A failed";
    ASSERT_TRUE(GetAndVerify(client_, *allocator_, kA, vA)) << "baseline key_A";

    // 2. force-remove key_A (lazy-delete: the client local table is untouched;
    //    the space returns to the allocator freelist).
    ASSERT_TRUE(client_->Remove(kA, /*force=*/true).has_value())
        << "Remove key_A failed";

    // 3. Put a batch of same-size new keys to force the allocator to reuse the
    //    address key_A just freed. The UPSERT recording on reuse must overwrite
    //    key_A's old local-table entry by address.
    const int kNumNew = 64;
    std::vector<std::string> new_keys, new_values;
    for (int i = 0; i < kNumNew; ++i) {
        new_keys.push_back("reuse_new_" + std::to_string(i));
        new_values.push_back(
            std::string(4096, static_cast<char>('a' + i % 26)));
        ASSERT_TRUE(PutString(client_, *allocator_, new_keys[i], new_values[i])
                        .has_value())
            << "Put new key failed " << new_keys[i];
    }

    // 4. Master crashes and restarts empty, then wait for all new keys to
    //    rebuild.
    RestartMasterEmpty();
    ASSERT_TRUE(
        WaitForAllKeysRebuilt(client_, *allocator_, new_keys, new_values))
        << "New keys not fully rebuilt in time";

    // 5a. Core assertion 1: the removed key_A must not revive.
    {
        void* buf = allocator_->allocate(vA.size());
        std::vector<Slice> slices{Slice{buf, vA.size()}};
        auto res = client_->Get(kA, slices);
        allocator_->deallocate(buf, vA.size());
        EXPECT_FALSE(res.has_value())
            << "removed key_A revived (bug in the delete cleanup logic): if it "
               "still points at the address now reused by a new key, that is "
               "silent data corruption";
    }

    // 5b. Core assertion 2: all new key data must be entirely correct (not
    //     polluted by key_A's stale mapping).
    for (int i = 0; i < kNumNew; ++i)
        EXPECT_TRUE(
            GetAndVerify(client_, *allocator_, new_keys[i], new_values[i]))
            << "New key data polluted or lost: " << new_keys[i];
}

// ---------------------------------------------------------------------------
// Test 6 (multi-replica merge, replica_num=2): the two replicas of one key
// land on different segments (different clients) and are resent by their
// respective owners. On rebuild the master must merge them back into "this key
// has 2 replicas" rather than keeping only one (RebuildMetadata takes the
// merge branch for an already-present key instead of continue-skipping it).
// ---------------------------------------------------------------------------
// NOTE: this is the only test for redundant multi-replica recovery; all other
// tests use replica_num=1 and never reach the merge branch.
// Preconditions: both clients MountSegment so there are two different segments
// for replica_num=2 to spread across, and owner recording, per-owner resend,
// and master merge are all implemented.
// Fixture: both clients mount a segment, unlike test 4 where A mounts none.
class ClientMultiReplicaTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        master_address_ = master_.master_address();
        for (int i = 0; i < 2; ++i) {
            auto c = Client::Create("127.0.0.1:1930" + std::to_string(i + 1),
                                    "P2PHANDSHAKE", FLAGS_protocol,
                                    std::nullopt, master_address_);
            ASSERT_TRUE(c.has_value());
            clients_[i] = c.value();
            seg_[i] = allocate_buffer_allocator_memory(kSeg);
            ASSERT_NE(seg_[i], nullptr);
            ASSERT_TRUE(clients_[i]
                            ->MountSegment(seg_[i], kSeg, FLAGS_protocol)
                            .has_value());
            alloc_[i] = std::make_unique<SimpleAllocator>(kAlloc);
            ASSERT_TRUE(clients_[i]
                            ->RegisterLocalMemory(alloc_[i]->getBase(), kAlloc,
                                                  "cpu:0", false, false)
                            .has_value());
        }
    }
    void TearDown() override {
        for (int i = 0; i < 2; ++i)
            if (clients_[i] && seg_[i])
                clients_[i]->UnmountSegment(seg_[i], kSeg);
        master_.Stop();
    }
    void RestartMasterEmpty() {
        master_.Stop();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        ASSERT_TRUE(master_.Start(
            InProcMasterConfigBuilder()
                .set_rpc_port(master_.rpc_port())
                .set_http_metrics_port(master_.http_metrics_port())
                .build()));
    }
    // Return the replica count the master records for this key, taken from
    // replicas.size() via Query.
    int ReplicaCount(const std::string& key) {
        auto q = clients_[0]->Query(key);
        return q.has_value() ? static_cast<int>(q.value().replicas.size()) : -1;
    }
    static constexpr size_t kSeg = 128 * 1024 * 1024;
    static constexpr size_t kAlloc = 64 * 1024 * 1024;
    InProcMaster master_;
    std::string master_address_;
    std::shared_ptr<Client> clients_[2];
    void* seg_[2] = {nullptr, nullptr};
    std::unique_ptr<SimpleAllocator> alloc_[2];
};

TEST_F(ClientMultiReplicaTest, MultiReplicaMergedOnRebuild) {
    const int kNumKeys = 20;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("dual_key_" + std::to_string(i));
        values.push_back("dual_val_" + std::to_string(i));
    }
    // 1. Put with replica_num=2: two replicas per key, spread across the two
    //    clients' segments.
    for (int i = 0; i < kNumKeys; ++i) {
        void* buf = alloc_[0]->allocate(values[i].size());
        std::memcpy(buf, values[i].data(), values[i].size());
        std::vector<Slice> slices{Slice{buf, values[i].size()}};
        ReplicateConfig cfg;
        cfg.replica_num = 2;  // key point: 2 replicas
        auto r = clients_[0]->Put(keys[i], slices, cfg);
        alloc_[0]->deallocate(buf, values[i].size());
        ASSERT_TRUE(r.has_value()) << "Put(replica_num=2) failed " << keys[i]
                                   << ": " << toString(r.error());
    }

    // 2. Baseline: each key should have 2 replicas before the restart.
    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_EQ(ReplicaCount(keys[i]), 2)
            << "baseline: key should have 2 replicas " << keys[i];

    // 3. Master crashes and restarts empty.
    RestartMasterEmpty();

    // 4. Wait for rebuild: the two owners each report their own replica and the
    //    master merges them. A replica count of 2 marks completion.
    bool merged = false;
    for (int attempt = 0; attempt < 40 && !merged; ++attempt) {
        merged = true;
        for (int i = 0; i < kNumKeys; ++i)
            if (ReplicaCount(keys[i]) != 2) {
                merged = false;
                break;
            }
        if (!merged)
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // 5. KEY ASSERTION: after rebuild every key is restored to 2 replicas
    //    (merge succeeded, redundancy preserved). If the master used continue
    //    to skip (old code), this would be 1 and the test fails.
    for (int i = 0; i < kNumKeys; ++i)
        EXPECT_EQ(ReplicaCount(keys[i]), 2)
            << "key should have 2 replicas after rebuild (multi-replica "
               "merge): "
            << keys[i]
            << " -- a count of 1 means the master did not merge and lost the "
               "second replica (redundancy lost)";

    // 6. Data is still readable and correct.
    for (int i = 0; i < kNumKeys; ++i) {
        void* buf = alloc_[0]->allocate(values[i].size());
        std::vector<Slice> slices{Slice{buf, values[i].size()}};
        auto res = clients_[0]->Get(keys[i], slices);
        bool ok =
            res.has_value() &&
            std::memcmp(slices[0].ptr, values[i].data(), values[i].size()) == 0;
        alloc_[0]->deallocate(buf, values[i].size());
        EXPECT_TRUE(ok) << "data should be correct after rebuild " << keys[i];
    }
}

}  // namespace testing
}  // namespace mooncake
