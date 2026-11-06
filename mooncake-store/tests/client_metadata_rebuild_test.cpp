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
#include "common/client_buffer_allocation.h"
#include "common/network.h"  // allocate_buffer_allocator_memory, SimpleAllocator
#include "test_server_helpers.h"  // InProcMaster, InProcMasterConfigBuilder
#include "master_config.h"
#include "tenant/quota_policy_store.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");

namespace mooncake {
namespace {
template <typename Descriptor>
auto& BufferDescriptor(Descriptor& replica) {
    return replica.get_memory_descriptor().buffer_descriptor;
}
}  // namespace
struct ClientTestAccess {
    static auto Snapshot(Client& client) {
        std::lock_guard lock(client.local_replica_table_mutex_);
        return client.local_replica_table_.Snapshot();
    }
    static size_t PendingEntries(Client& client) {
        std::lock_guard lock(client.pending_notifies_mutex_);
        return client.pending_notify_entries_;
    }
    static size_t PendingBuckets(Client& client) {
        std::lock_guard lock(client.pending_notifies_mutex_);
        return client.pending_notifies_.size();
    }
    static auto SnapshotCount(Client& client) {
        return client.rebuild_snapshot_count_.load();
    }
    static void Pause(Client& client, bool paused) {
        client.rebuild_notifies_paused_.store(paused);
    }
    static void Timeout(Client& client, std::chrono::milliseconds timeout) {
        client.rebuild_notify_timeout_ms_.store(timeout.count());
    }
    static void BeforeUpsert(Client& client, std::function<void()> callback) {
        client.before_upsert_end_for_test_ = std::move(callback);
    }

    static auto Park(Client& client, const KeyReplicaEntry& entry) {
        return client.ParkPendingNotify(
            BufferDescriptor(entry.replicas.front()).transport_endpoint_,
            {entry});
    }
    template <typename Ticket>
    static bool Wait(Client& client, const Ticket& ticket) {
        return client.WaitForPendingNotifies({ticket});
    }
    template <typename Ticket>
    static void Expire(Client& client, const Ticket& ticket) {
        std::lock_guard<std::mutex> lock(client.pending_notifies_mutex_);
        ticket->created_at =
            std::chrono::steady_clock::now() - Client::kNotifyTtl;
    }
    template <typename Ticket>
    static size_t Attempts(Client& client, const Ticket& ticket) {
        std::lock_guard<std::mutex> lock(client.pending_notifies_mutex_);
        return ticket->attempts;
    }
    static void Flush(Client& client) { client.FlushPendingNotifies(); }
    static auto Resend(Client& client, ViewVersionId view) {
        return client.ResendLocalReplicaTable(view);
    }
    static auto WaitOwned(Client& client, ViewVersionId view) {
        return client.WaitForOwnedReplays(view);
    }
    static void Record(Client& client, const KeyReplicaEntry& entry) {
        std::lock_guard<std::mutex> lock(client.local_replica_table_mutex_);
        for (const auto& replica : entry.replicas)
            client.local_replica_table_.Record(entry, replica);
    }
    static auto View(Client& client) {
        return client.master_client_.Ping().value().view_version_id;
    }
    static void SetOwnedPending(Client& client, uint64_t id, bool failed) {
        std::lock_guard<std::mutex> lock(client.local_replica_table_mutex_);
        client.owned_replay_pending_[id] = failed;
        client.owned_replay_cv_.notify_all();
    }
    static void ClearOwnedPending(Client& client) {
        std::lock_guard<std::mutex> lock(client.local_replica_table_mutex_);
        client.owned_replay_pending_.clear();
        client.owned_replay_cv_.notify_all();
    }
    static auto RegisteredBuffers(Client& client) {
        return client.transfer_engine_->getMetadata()
            ->getSegmentDescByID(LOCAL_SEGMENT_ID)
            ->buffers;
    }
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

class ScopedEnvironment {
   public:
    explicit ScopedEnvironment(const char* name, const char* value = nullptr)
        : name_(name) {
        if (const char* old = std::getenv(name)) previous_ = old;
        Set(value);
    }
    ~ScopedEnvironment() { Set(previous_ ? previous_->c_str() : nullptr); }
    ScopedEnvironment(const ScopedEnvironment&) = delete;

   private:
    void Set(const char* value) {
        if (value)
            ::setenv(name_, value, 1);
        else
            ::unsetenv(name_);
    }
    const char* name_;
    std::optional<std::string> previous_;
};

TEST(ClientRebuildSwitchTest, DisabledUnlessExplicitlyEnabled) {
    ScopedEnvironment setting("MC_ENABLE_HA_REBUILD");
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
        EXPECT_TRUE(ClientTestAccess::Snapshot(**client).empty());
    }
}

struct TestValues {
    SimpleAllocator& allocator;
    std::vector<std::vector<Slice>> slices;
    TestValues(SimpleAllocator& allocator,
               const std::vector<std::string>& values)
        : allocator(allocator) {
        for (const auto& value : values) {
            void* data = allocator.allocate(value.size());
            if (!data) throw std::bad_alloc();
            std::memcpy(data, value.data(), value.size());
            slices.push_back({{data, value.size()}});
        }
    }
    TestValues(const TestValues&) = delete;
    ~TestValues() {
        for (const auto& value : slices)
            allocator.deallocate(value.front().ptr, value.front().size);
    }
};

std::vector<tl::expected<void, ErrorCode>> WriteStrings(
    std::shared_ptr<Client>& client, SimpleAllocator& allocator,
    const std::vector<std::string>& keys,
    const std::vector<std::string>& values, bool upsert, bool batch,
    const ReplicateConfig& config = {}) {
    TestValues buffers(allocator, values);
    if (batch)
        return upsert ? client->BatchUpsert(keys, buffers.slices, config)
                      : client->BatchPut(keys, buffers.slices, config);
    std::vector<tl::expected<void, ErrorCode>> results;
    for (size_t i = 0; i < keys.size(); ++i)
        results.push_back(
            upsert ? client->Upsert(keys[i], buffers.slices[i], config)
                   : client->Put(keys[i], buffers.slices[i], config));
    return results;
}

tl::expected<void, ErrorCode> PutString(std::shared_ptr<Client>& client,
                                        SimpleAllocator& allocator,
                                        const std::string& key,
                                        const std::string& value,
                                        const ReplicateConfig& config = {},
                                        bool upsert = false) {
    return WriteStrings(client, allocator, {key}, {value}, upsert, false,
                        config)
        .front();
}

bool GetAndVerify(std::shared_ptr<Client>& client, SimpleAllocator& allocator,
                  const std::string& key, const std::string& expected) {
    void* buf = allocator.allocate(expected.size());
    if (!buf) return false;
    std::vector<Slice> slices{Slice{buf, expected.size()}};
    auto res = client->Get(key, slices);
    bool ok = res.has_value() && slices[0].size == expected.size() &&
              std::memcmp(slices[0].ptr, expected.data(), expected.size()) == 0;
    allocator.deallocate(buf, expected.size());
    return ok;
}

template <typename Predicate>
bool WaitUntil(
    Predicate ready, int attempts = 100,
    std::chrono::milliseconds interval = std::chrono::milliseconds(50)) {
    while (attempts-- > 0) {
        if (ready()) return true;
        std::this_thread::sleep_for(interval);
    }
    return false;
}

bool WaitForAllKeysRebuilt(std::shared_ptr<Client>& client,
                           SimpleAllocator& allocator,
                           const std::vector<std::string>& keys,
                           const std::vector<std::string>& values) {
    return WaitUntil(
        [&] {
            for (size_t i = 0; i < keys.size(); ++i)
                if (!GetAndVerify(client, allocator, keys[i], values[i]))
                    return false;
            return true;
        },
        40, std::chrono::milliseconds(500));
}

}  // namespace

class RebuildClientFixture : public ::testing::Test {
   protected:
    virtual InProcMasterConfig MasterConfig() const {
        return InProcMasterConfigBuilder().build();
    }
    bool StartMaster() {
        if (!master_.Start(MasterConfig())) return false;
        master_address_ = master_.master_address();
        return true;
    }
    void TearDown() override { master_.Stop(); }
    void RestartMasterEmpty() {
        master_.Stop();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        auto config = MasterConfig();
        config.rpc_port = master_.rpc_port();
        config.http_metrics_port = master_.http_metrics_port();
        ASSERT_TRUE(master_.Start(config));
    }
    ::testing::AssertionResult CreateClient(
        std::shared_ptr<Client>& client,
        std::unique_ptr<SimpleAllocator>& allocator,
        const std::string& hostname, void** segment = nullptr) {
        auto created = Client::Create(hostname, "P2PHANDSHAKE", FLAGS_protocol,
                                      std::nullopt, master_address_);
        if (!created) return ::testing::AssertionFailure() << "Client::Create";
        client = *created;
        allocator = std::make_unique<SimpleAllocator>(kAlloc);
        if (!client->RegisterLocalMemory(allocator->getBase(), kAlloc, "cpu:0",
                                         false, false))
            return ::testing::AssertionFailure() << "RegisterLocalMemory";
        if (segment) {
            *segment = allocate_buffer_allocator_memory(kSeg);
            if (!*segment ||
                !client->MountSegment(*segment, kSeg, FLAGS_protocol))
                return ::testing::AssertionFailure() << "MountSegment";
        }
        return ::testing::AssertionSuccess();
    }
    static constexpr size_t kSeg = 128 * 1024 * 1024;
    // Each of the five payload-size classes needs a separate 16 MiB slab.
    static constexpr size_t kAlloc = 128 * 1024 * 1024;
    InProcMaster master_;
    std::string master_address_;
};

class ClientMetadataRebuildTest : public RebuildClientFixture {
   protected:
    void SetUp() override {
        ASSERT_TRUE(StartMaster());
        ASSERT_TRUE(
            CreateClient(client_, allocator_, "127.0.0.1:19100", &seg_ptr_));
    }
    void TearDown() override {
        if (client_ && seg_ptr_) client_->UnmountSegment(seg_ptr_, kSeg);
        RebuildClientFixture::TearDown();
    }
    std::shared_ptr<Client> client_;
    std::unique_ptr<SimpleAllocator> allocator_;
    void* seg_ptr_{nullptr};
};

class ClientUpsertRebuildTest : public ClientMetadataRebuildTest,
                                public ::testing::WithParamInterface<bool> {};

TEST_P(ClientUpsertRebuildTest, CreationAndReplacementSurviveRecovery) {
    const std::vector<std::string> keys{"created", "inplace", "resized"};
    const std::vector<std::string> expected{"created", "new-value",
                                            std::string(4096, 'R')};
    ASSERT_TRUE(PutString(client_, *allocator_, keys[1], "old-value"));
    ASSERT_TRUE(PutString(client_, *allocator_, keys[2], "old"));
    const auto results =
        WriteStrings(client_, *allocator_, keys, expected, true, GetParam());
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) ASSERT_TRUE(result);
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, expected));
}

INSTANTIATE_TEST_SUITE_P(SingleAndBatch, ClientUpsertRebuildTest,
                         ::testing::Bool());

TEST_F(ClientMetadataRebuildTest, UpsertReplaysAuthoritativePinsAndGroup) {
    for (const auto& key : {"pinned-inplace", "pinned-resized"}) {
        ReplicateConfig pinned;
        pinned.with_hard_pin = true;
        pinned.soft_pin_action = SoftPinAction::ENABLE;
        pinned.group_ids = std::vector<std::string>{"pin-group"};
        ASSERT_TRUE(PutString(client_, *allocator_, key, "original", pinned));
        const std::string replacement = std::string(key) == "pinned-inplace"
                                            ? "replaced"
                                            : std::string(1024, 'R');
        ASSERT_TRUE(
            PutString(client_, *allocator_, key, replacement, {}, true));
    }
    const auto snapshot = ClientTestAccess::Snapshot(*client_);
    ASSERT_EQ(snapshot.size(), 2);
    for (const auto& entry : snapshot) {
        EXPECT_TRUE(entry.with_hard_pin);
        EXPECT_EQ(entry.group_id, "pin-group");
        EXPECT_TRUE(entry.with_soft_pin);
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

// B owns the sole segment; A only registers its transfer buffers.
class ClientCrossNotifyTest : public RebuildClientFixture {
   protected:
    void SetUp() override {
        ASSERT_TRUE(StartMaster());
        ASSERT_TRUE(CreateClient(clientB_, allocB_, "127.0.0.1:19201", &segB_));
        ASSERT_TRUE(CreateClient(clientA_, allocA_, "127.0.0.1:19202"));
    }
    void TearDown() override {
        if (clientB_ && segB_) clientB_->UnmountSegment(segB_, kSeg);
        RebuildClientFixture::TearDown();
    }
    std::shared_ptr<Client> clientA_, clientB_;
    std::unique_ptr<SimpleAllocator> allocA_, allocB_;
    void* segB_{nullptr};
};

enum class ReadScenario { SELF, REMOTE, LAZY_DELETE };
class ClientReadRecoveryTest
    : public ClientCrossNotifyTest,
      public ::testing::WithParamInterface<ReadScenario> {};

TEST_P(ClientReadRecoveryTest, ByteExactReadsAndRegistrationsSurviveRestart) {
    const bool remote = GetParam() == ReadScenario::REMOTE;
    auto writer = remote ? clientA_ : clientB_;
    auto& allocator = remote ? *allocA_ : *allocB_;
    std::vector<std::string> keys, values;
    for (int i = 0; i < 50; ++i) {
        keys.push_back("rebuild-" + std::to_string(i));
        values.push_back("value-" + std::to_string(i));
    }
    for (size_t size : {512, 1024, 2048, 4096}) {
        keys.push_back("extent-" + std::to_string(size));
        values.push_back(
            std::string(size, static_cast<char>('A' + values.size() % 26)));
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(PutString(writer, allocator, keys[i], values[i]));
        ASSERT_TRUE(GetAndVerify(writer, allocator, keys[i], values[i]));
    }
    if (GetParam() == ReadScenario::LAZY_DELETE) {
        for (size_t i = 0; i < keys.size(); i += 2) {
            ASSERT_TRUE(writer->Remove(keys[i], true));
        }
    }
    ASSERT_EQ(ClientTestAccess::Snapshot(*clientB_).size(), keys.size());
    if (remote) {
        EXPECT_TRUE(ClientTestAccess::Snapshot(*writer).empty());
    }
    const auto registrations = ClientTestAccess::RegisteredBuffers(*writer);
    const auto owner_registrations =
        ClientTestAccess::RegisteredBuffers(*clientB_);
    ASSERT_FALSE(registrations.empty());
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(writer, allocator, keys, values));
    EXPECT_EQ(ClientTestAccess::RegisteredBuffers(*writer), registrations);
    EXPECT_EQ(ClientTestAccess::RegisteredBuffers(*clientB_),
              owner_registrations);
    if (remote) {
        for (size_t i = 0; i < keys.size(); ++i) {
            EXPECT_TRUE(GetAndVerify(clientB_, *allocB_, keys[i], values[i]));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(OwnerAndLazyDelete, ClientReadRecoveryTest,
                         ::testing::Values(ReadScenario::SELF,
                                           ReadScenario::REMOTE,
                                           ReadScenario::LAZY_DELETE));

TEST_F(ClientCrossNotifyTest, ExistingPutDoesNotWaitForOfflineOwnersAck) {
    ASSERT_TRUE(PutString(clientA_, *allocA_, "idempotent", "original"));
    ClientTestAccess::Pause(*clientB_, true);
    ClientTestAccess::Timeout(*clientA_, std::chrono::seconds(3));
    const auto begin = std::chrono::steady_clock::now();
    EXPECT_TRUE(PutString(clientA_, *allocA_, "idempotent", "replacement"));
    EXPECT_LT(std::chrono::steady_clock::now() - begin,
              std::chrono::seconds(1));
    EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, "idempotent", "original"));
    EXPECT_EQ(ClientTestAccess::PendingEntries(*clientA_), 1);
    ClientTestAccess::Pause(*clientB_, false);
}

TEST_F(ClientCrossNotifyTest, NotifyExpiryAndCapacityNeverCountAsAck) {
    ASSERT_TRUE(PutString(clientA_, *allocA_, "seed", "value"));
    const auto entry = ClientTestAccess::Snapshot(*clientB_).front();
    ClientTestAccess::Pause(*clientB_, true);
    ClientTestAccess::Timeout(*clientA_, std::chrono::milliseconds(20));
    auto ticket = ClientTestAccess::Park(*clientA_, entry);
    EXPECT_EQ(ticket, ClientTestAccess::Park(*clientA_, entry));
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 1);
    ClientTestAccess::Flush(*clientA_);
    const auto attempts = ClientTestAccess::Attempts(*clientA_, ticket);
    for (int i = 0; i < 10; ++i) ClientTestAccess::Flush(*clientA_);
    EXPECT_EQ(ClientTestAccess::Attempts(*clientA_, ticket), attempts);
    ClientTestAccess::Expire(*clientA_, ticket);
    ClientTestAccess::Flush(*clientA_);
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 0);
    EXPECT_FALSE(ClientTestAccess::Wait(*clientA_, ticket));

    ticket = ClientTestAccess::Park(*clientA_, entry);
    for (int i = 0; i < 1024; ++i) {
        auto unique = entry;
        unique.key = "queued-" + std::to_string(i);
        ClientTestAccess::Park(*clientA_, unique);
    }
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 1024);
    EXPECT_LE(ClientTestAccess::PendingEntries(*clientA_), 65536);
    EXPECT_FALSE(ClientTestAccess::Wait(*clientA_, ticket));
}

TEST_F(ClientCrossNotifyTest, OrphanedRemoteQueueDoesNotBlockAnyReplayView) {
    ASSERT_TRUE(PutString(clientA_, *allocA_, "orphan", "value"));
    auto entry = ClientTestAccess::Snapshot(*clientB_).front();
    BufferDescriptor(entry.replicas.front()).transport_endpoint_ =
        "127.0.0.1:1";
    auto ticket = ClientTestAccess::Park(*clientA_, entry);
    ClientTestAccess::Timeout(*clientA_, std::chrono::milliseconds(50));
    for (const ViewVersionId view : {17, 18}) {
        const auto begin = std::chrono::steady_clock::now();
        ASSERT_TRUE(ClientTestAccess::Resend(*clientA_, view));
        EXPECT_TRUE(ClientTestAccess::WaitOwned(*clientA_, view));
        EXPECT_LT(std::chrono::steady_clock::now() - begin,
                  std::chrono::milliseconds(500));
    }
    EXPECT_FALSE(ClientTestAccess::Wait(*clientA_, ticket));
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 1);
}

TEST_F(ClientCrossNotifyTest, OwnedReplayBarrierDoesNotWaitForRemoteAck) {
    const auto view = ClientTestAccess::View(*clientA_);
    ASSERT_TRUE(ClientTestAccess::Resend(*clientA_, view));
    ClientTestAccess::Timeout(*clientA_, std::chrono::seconds(2));
    ClientTestAccess::SetOwnedPending(*clientA_, 100, false);
    auto wait = std::async(std::launch::async, [&] {
        return ClientTestAccess::WaitOwned(*clientA_, view);
    });
    EXPECT_EQ(wait.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    ClientTestAccess::ClearOwnedPending(*clientA_);
    EXPECT_TRUE(wait.get());
    ClientTestAccess::SetOwnedPending(*clientA_, 101, true);
    auto failed = ClientTestAccess::WaitOwned(*clientA_, view);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::RPC_TIMEOUT);
    EXPECT_FALSE(ClientTestAccess::WaitOwned(*clientA_, view + 1));
    ClientTestAccess::ClearOwnedPending(*clientA_);
}

TEST_F(ClientCrossNotifyTest, InvalidSnapshotRowDoesNotPoisonValidReplay) {
    ASSERT_TRUE(PutString(clientA_, *allocA_, "valid", "payload"));
    auto poison = ClientTestAccess::Snapshot(*clientB_).front();
    poison.key = "poison";
    poison.size += 1;  // inconsistent committed size vs descriptor size
    auto& buffer = BufferDescriptor(poison.replicas.front());
    buffer.buffer_address_ += buffer.allocationExtent();
    ClientTestAccess::Record(*clientB_, poison);
    ASSERT_EQ(ClientTestAccess::Snapshot(*clientB_).size(), 2);
    const auto view = ClientTestAccess::View(*clientB_);
    EXPECT_TRUE(ClientTestAccess::Resend(*clientB_, view));
    EXPECT_TRUE(ClientTestAccess::WaitOwned(*clientB_, view));
    const auto snapshot = ClientTestAccess::Snapshot(*clientB_);
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_EQ(snapshot.front().key, "valid");
    EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, "valid", "payload"));
}

TEST_F(ClientCrossNotifyTest, LargeSnapshotDoesNotHoldTableLockAcrossRpc) {
    ASSERT_TRUE(PutString(clientA_, *allocA_, "seed", "value"));
    auto seed = ClientTestAccess::Snapshot(*clientB_).front();
    const auto original = BufferDescriptor(seed.replicas.front());
    for (uint64_t i = 1; i <= 8192; ++i) {
        auto entry = seed;
        entry.key = "large-" + std::to_string(i);
        entry.version.second += i;
        entry.replicas.front().id += i;
        BufferDescriptor(entry.replicas.front()).buffer_address_ =
            original.buffer_address_ + i * original.allocationExtent();
        ClientTestAccess::Record(*clientB_, entry);
    }
    ASSERT_EQ(ClientTestAccess::Snapshot(*clientB_).size(), 8193);
    const auto view = ClientTestAccess::View(*clientB_);
    auto replay = std::async(std::launch::async, [&] {
        return ClientTestAccess::Resend(*clientB_, view);
    });
    size_t checks = 0;
    while (replay.wait_for(std::chrono::milliseconds(0)) !=
           std::future_status::ready) {
        const auto begin = std::chrono::steady_clock::now();
        EXPECT_EQ(ClientTestAccess::Snapshot(*clientB_).size(), 8193);
        EXPECT_LT(std::chrono::steady_clock::now() - begin,
                  std::chrono::milliseconds(250));
        ++checks;
    }
    EXPECT_GT(checks, 0);
    EXPECT_TRUE(replay.get());
    EXPECT_TRUE(ClientTestAccess::WaitOwned(*clientB_, view));
}

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
    ClientTestAccess::Timeout(*clientA_, std::chrono::milliseconds(300));
    ClientTestAccess::Pause(*clientB_, true);
    std::vector<std::vector<Slice>> values{slices, slices, slices};
    ReplicateConfig remote;
    remote.preferred_segments = {"127.0.0.1:19201"};
    const auto results = clientA_->BatchPut(
        {"remote-existing", "local-existing", "remote-new"}, values, remote);
    ASSERT_EQ(results.size(), 3);
    EXPECT_TRUE(results[0]);  // existing object stays idempotently successful
    EXPECT_TRUE(results[1]);
    EXPECT_FALSE(results[2]);
    if (!results[2]) {
        EXPECT_EQ(results[2].error(), ErrorCode::RPC_TIMEOUT);
    }
    EXPECT_EQ(ClientTestAccess::PendingEntries(*clientA_), 2);
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 2);
    ClientTestAccess::Pause(*clientB_, false);
    EXPECT_TRUE(WaitUntil(
        [&] { return ClientTestAccess::PendingBuckets(*clientA_) == 0; }, 100,
        std::chrono::milliseconds(20)));
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
    std::vector<std::string> keys, expected;
    for (size_t index = 0; index < 16; ++index) {
        keys.push_back("batch-notify-" + std::to_string(index));
        expected.push_back(std::string(128, static_cast<char>(index)));
    }
    ClientTestAccess::Timeout(*clientA_, std::chrono::seconds(3));
    ClientTestAccess::Pause(*clientB_, true);
    auto write = std::async(std::launch::async, [&] {
        return WriteStrings(clientA_, *allocA_, keys, expected, GetParam(),
                            true);
    });
    EXPECT_TRUE(WaitUntil(
        [&] {
            return ClientTestAccess::PendingEntries(*clientA_) == keys.size();
        },
        100, std::chrono::milliseconds(10)));
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 1);
    EXPECT_EQ(write.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    ClientTestAccess::Pause(*clientB_, false);
    const auto results = write.get();
    ASSERT_EQ(results.size(), keys.size());
    for (size_t index = 0; index < keys.size(); ++index) {
        EXPECT_TRUE(results[index]);
        EXPECT_TRUE(
            GetAndVerify(clientA_, *allocA_, keys[index], expected[index]));
    }
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 0);
    EXPECT_EQ(ClientTestAccess::Snapshot(*clientB_).size(), keys.size());
}

TEST_P(ClientBatchNotifyTest, StalledOwnerUsesOneDeadlineForWholeBatch) {
    std::vector<std::string> keys;
    const std::vector<std::string> values(16, std::string(128, 'V'));
    for (size_t index = 0; index < values.size(); ++index)
        keys.push_back("timeout-notify-" + std::to_string(index));
    ClientTestAccess::Timeout(*clientA_, std::chrono::milliseconds(300));
    ClientTestAccess::Pause(*clientB_, true);
    const auto start = std::chrono::steady_clock::now();
    const auto results =
        WriteStrings(clientA_, *allocA_, keys, values, GetParam(), true);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::milliseconds(1800));
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::RPC_TIMEOUT);
    }
    EXPECT_EQ(ClientTestAccess::PendingEntries(*clientA_), keys.size());
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 1);
    ClientTestAccess::Pause(*clientB_, false);
    EXPECT_TRUE(WaitUntil(
        [&] { return ClientTestAccess::PendingBuckets(*clientA_) == 0; }, 100,
        std::chrono::milliseconds(20)));
    EXPECT_EQ(ClientTestAccess::Snapshot(*clientB_).size(), keys.size());
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(clientA_, *allocA_, keys, values));
}

INSTANTIATE_TEST_SUITE_P(PutAndUpsert, ClientBatchNotifyTest,
                         ::testing::Bool());

// RAII sets flags before SetUp and restores them after TearDown.
class ClientUpsertHotCacheRebuildTest
    : public ClientCrossNotifyTest,
      public ::testing::WithParamInterface<bool> {
    ScopedEnvironment size_{"MC_STORE_LOCAL_HOT_CACHE_SIZE", "1048576"};
    ScopedEnvironment block_{"MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4096"};
};

TEST_P(ClientUpsertHotCacheRebuildTest,
       CommitInvalidatesStaleFillBeforeAckFailure) {
    const std::string key = "upsert-ack-failure";
    ASSERT_TRUE(PutString(clientA_, *allocA_, key, "old-value"));
    auto cache = clientA_->GetHotCache();
    ASSERT_NE(cache, nullptr);
    HotCachePutToken stale_token;
    ClientTestAccess::BeforeUpsert(*clientA_, [&] {
        stale_token = cache->AcquirePutToken(key);
        auto* block = cache->GetFreeBlock();
        ASSERT_NE(block, nullptr);
        block->key_ = key;
        block->size = 9;
        std::memcpy(block->addr, "old-value", 9);
        ASSERT_TRUE(cache->PutHotKey(block, stale_token));
    });
    ClientTestAccess::Timeout(*clientA_, std::chrono::milliseconds(300));
    ClientTestAccess::Pause(*clientB_, true);
    void* buffer = allocA_->allocate(9);
    std::memcpy(buffer, "new-value", 9);
    std::vector<Slice> slices{Slice{buffer, 9}};
    std::vector<std::vector<Slice>> batches{slices};
    const auto result =
        GetParam()
            ? clientA_->BatchUpsert({key}, batches, ReplicateConfig{}).front()
            : clientA_->Upsert(key, slices, ReplicateConfig{});
    ClientTestAccess::BeforeUpsert(*clientA_, {});
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
    ClientTestAccess::Pause(*clientB_, false);
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
    EXPECT_EQ(BufferDescriptor(query->replicas.front()).transport_endpoint_,
              clientB_->GetTransportEndpoint());
    EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, key, "source-value"));
    EXPECT_TRUE(clientA_->UnmountSegment(target, kSeg));
}

TEST_F(ClientCrossNotifyTest,
       DelayedOwnershipIsAppliedAfterOwnersFirstSnapshot) {
    ClientTestAccess::Pause(*clientB_, true);
    const auto before = ClientTestAccess::SnapshotCount(*clientB_);
    auto put = std::async(std::launch::async, [&] {
        return PutString(clientA_, *allocA_, "late-notify", "committed-value");
    });
    EXPECT_TRUE(WaitUntil(
        [&] { return ClientTestAccess::PendingBuckets(*clientA_) > 0; }, 100,
        std::chrono::milliseconds(10)));
    EXPECT_EQ(put.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    EXPECT_TRUE(ClientTestAccess::Snapshot(*clientB_).empty());
    RestartMasterEmpty();
    EXPECT_TRUE(WaitUntil(
        [&] { return ClientTestAccess::SnapshotCount(*clientB_) > before; }));
    ClientTestAccess::Pause(*clientB_, false);
    EXPECT_TRUE(put.get());
    ASSERT_TRUE(WaitForAllKeysRebuilt(clientA_, *allocA_, {"late-notify"},
                                      {"committed-value"}));
    EXPECT_TRUE(
        GetAndVerify(clientB_, *allocB_, "late-notify", "committed-value"));
    EXPECT_EQ(ClientTestAccess::PendingBuckets(*clientA_), 0);
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
    ASSERT_TRUE(WaitUntil(
        [&] { return ClientTestAccess::Snapshot(*clientB_).size() == 2; }));
    RestartMasterEmpty();
    for (size_t index = 0; index < writers.size(); ++index) {
        ASSERT_TRUE(WaitForAllKeysRebuilt(writers[index], *allocA_,
                                          {"shared-key"}, {values[index]}));
    }
}

TEST_F(ClientCrossNotifyTest, ReorderedRetryCannotResurrectReusedAddress) {
    const std::string value_a(4096, 'A');
    const std::string value_b(4096, 'B');
    ASSERT_TRUE(PutString(clientA_, *allocA_, "parked-A", value_a));
    const auto parked = ClientTestAccess::Snapshot(*clientB_).front();
    auto original = clientA_->Query("parked-A");
    ASSERT_TRUE(original);
    const auto descriptor = original->replicas.front();
    ASSERT_TRUE(clientA_->Remove("parked-A", true));
    ASSERT_TRUE(PutString(clientA_, *allocA_, "live-B", value_b));
    auto replacement = clientA_->Query("live-B");
    ASSERT_TRUE(replacement);
    ASSERT_EQ(
        descriptor.get_memory_descriptor().buffer_descriptor.buffer_address_,
        BufferDescriptor(replacement->replicas.front()).buffer_address_);
    ASSERT_TRUE(WaitUntil([&] {
        const auto snapshot = ClientTestAccess::Snapshot(*clientB_);
        return snapshot.size() == 1 && snapshot.front().key == "live-B";
    }));
    ClientTestAccess::Park(*clientA_, parked);
    ASSERT_TRUE(WaitUntil(
        [&] { return ClientTestAccess::PendingBuckets(*clientA_) == 0; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto snapshot = ClientTestAccess::Snapshot(*clientB_);
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

TEST_F(ClientMetadataRebuildTest, RemovedKeySpaceReuseNoStaleMapping) {
    const std::string kA = "reuse_victim_A";
    const std::string vA = std::string(4096, 'X');  // reused by same-size key

    ASSERT_TRUE(PutString(client_, *allocator_, kA, vA).has_value())
        << "Put key_A failed";
    ASSERT_TRUE(GetAndVerify(client_, *allocator_, kA, vA)) << "baseline key_A";

    ASSERT_TRUE(client_->Remove(kA, /*force=*/true).has_value())
        << "Remove key_A failed";

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

    RestartMasterEmpty();
    ASSERT_TRUE(
        WaitForAllKeysRebuilt(client_, *allocator_, new_keys, new_values))
        << "New keys not fully rebuilt in time";

    {
        void* buf = allocator_->allocate(vA.size());
        std::vector<Slice> slices{Slice{buf, vA.size()}};
        auto res = client_->Get(kA, slices);
        allocator_->deallocate(buf, vA.size());
        EXPECT_FALSE(res.has_value()) << kA;
    }
}

class ClientMultiReplicaTest : public RebuildClientFixture {
   protected:
    void SetUp() override {
        ASSERT_TRUE(StartMaster());
        for (int i = 0; i < 2; ++i)
            ASSERT_TRUE(CreateClient(clients_[i], alloc_[i],
                                     "127.0.0.1:1930" + std::to_string(i + 1),
                                     &seg_[i]));
    }
    void TearDown() override {
        for (int i = 0; i < 2; ++i)
            if (clients_[i] && seg_[i])
                clients_[i]->UnmountSegment(seg_[i], kSeg);
        RebuildClientFixture::TearDown();
    }
    int ReplicaCount(const std::string& key) {
        auto result = clients_[0]->Query(key);
        return result ? static_cast<int>(result->replicas.size()) : -1;
    }
    std::shared_ptr<Client> clients_[2];
    std::unique_ptr<SimpleAllocator> alloc_[2];
    void* seg_[2]{nullptr, nullptr};
};

TEST_F(ClientMultiReplicaTest, MultiReplicaMergedOnRebuild) {
    const int kNumKeys = 20;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("dual_key_" + std::to_string(i));
        values.push_back("dual_val_" + std::to_string(i));
    }
    ReplicateConfig config;
    config.replica_num = 2;
    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_TRUE(
            PutString(clients_[0], *alloc_[0], keys[i], values[i], config));

    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_EQ(ReplicaCount(keys[i]), 2)
            << "baseline: key should have 2 replicas " << keys[i];

    RestartMasterEmpty();

    ASSERT_TRUE(WaitUntil(
        [&] {
            for (const auto& key : keys)
                if (ReplicaCount(key) != 2) return false;
            return true;
        },
        40, std::chrono::milliseconds(500)));

    for (int i = 0; i < kNumKeys; ++i)
        EXPECT_TRUE(GetAndVerify(clients_[0], *alloc_[0], keys[i], values[i]));
}

}  // namespace testing
}  // namespace mooncake
