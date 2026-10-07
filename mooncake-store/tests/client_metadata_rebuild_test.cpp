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
#include "utils.h"  // allocate_buffer_allocator_memory, SimpleAllocator
#include "test_server_helpers.h"  // InProcMaster, InProcMasterConfigBuilder
#include "default_config.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");

namespace mooncake {
namespace testing {

namespace {

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
            allocator.deallocate(buf, values[i].size());
            if (!res.has_value()) {
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
// clientA mounts no segment and clientB does, so A's Put data necessarily
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
//   clientA only RegisterLocalMemory for its local read/write buffer and does
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
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
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

        // clientA is the writer and only registers a local read/write buffer;
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
        ASSERT_TRUE(master_.Start(
            InProcMasterConfigBuilder()
                .set_rpc_port(master_.rpc_port())
                .set_http_metrics_port(master_.http_metrics_port())
                .build()));
    }

    static constexpr size_t kSeg = 128 * 1024 * 1024;
    static constexpr size_t kAlloc = 64 * 1024 * 1024;
    InProcMaster master_;
    std::string master_address_;
    std::shared_ptr<Client> clientA_, clientB_;
    void* segB_ = nullptr;
    std::unique_ptr<SimpleAllocator> allocA_, allocB_;
};

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
// Approach (deterministic and reproducible): A first Puts a key normally so
// the data lands on B's segment, and uses Query to get the real Descriptor
// pointing at B's segment. Then it uses ParkNotifyForTest to park a notify for
// a new key headed to B, simulating "this notify originally failed to send".
// Then:
//   1. Assert the pending bucket count is 1 (it really was parked).
//   2. Wait for the background RebuildNotifyLoop's FlushPendingNotifies to
//      redeliver it, so pending drains to zero.
//   3. Restart the master and assert that this new key, which was recorded
//      only thanks to redelivery, can also be resent and rebuilt by B.
// If the backstop is missing (a failed send is just dropped), pending never
// drains and the new key never rebuilds, so the test fails.
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

    // 2. Simulate "the notify to B originally failed" by parking a notify for
    //    a new key into pending. Reuse the carrier's Descriptor as this new
    //    key's replica location; the focus is the redelivery path, not address
    //    authenticity, and the new key goes through B's RecordLocalReplica so B
    //    can later resend it.
    const std::string dropped = "dropped_notify_key";
    clientA_->ParkNotifyForTest(owner_ep, dropped, carrier_desc,
                                carrier_val.size(), ObjectDataType::UNKNOWN, "",
                                "default");

    // 1. It really was parked.
    EXPECT_GE(clientA_->PendingNotifyBucketCountForTest(), 1u)
        << "A failed notify should be parked in the pending queue (without the "
           "backstop it would not be parked)";

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

    // 3. Assert the dropped key, recorded only thanks to redelivery, can be
    //    resent and rebuilt by B (the master knows it).
    bool rebuilt = false;
    for (int i = 0; i < 40 && !rebuilt; ++i) {
        auto qq = clientA_->Query(dropped);
        if (qq.has_value() && !qq.value().replicas.empty()) {
            rebuilt = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    EXPECT_TRUE(rebuilt)
        << "the key for the redelivered notify was not rebuilt: without the "
           "retry backstop this notify would be lost, the owner would miss the "
           "record, and the replica would be silently lost on rebuild";
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
