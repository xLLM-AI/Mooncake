#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>

#include "master_service.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "rebuild_retry.h"
#include "rebuild_replica_table.h"

namespace mooncake {
namespace {

MasterServiceConfig ReplayConfig(ViewVersionId view) {
    return MasterServiceConfig::builder()
        .set_memory_allocator(BufferAllocatorType::OFFSET)
        .set_default_kv_lease_ttl(60000)
        .set_view_version(view)
        .build();
}

Segment MakeSegment(const std::string& name, uintptr_t base, size_t size) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment.te_endpoint = name;
    segment.base = base;
    segment.size = size;
    return segment;
}

KeyReplicaEntry MakeEntry(const Segment& segment, const std::string& key,
                          uint64_t size, uintptr_t address, ReplicaID id,
                          RebuildVersion version) {
    KeyReplicaEntry entry;
    entry.key = key;
    entry.size = size;
    entry.version = version;
    Replica::Descriptor replica;
    replica.id = id;
    replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor memory;
    memory.buffer_descriptor = {
        size, address, "tcp", segment.te_endpoint, version.first, size};
    replica.descriptor_variant = memory;
    entry.replicas = {replica};
    return entry;
}

TEST(MasterRebuildReplayTest, SnapshotRemountPreservesNewAllocationEpoch) {
    std::vector<char> bytes(4096, 'O');
    const auto segment =
        MakeSegment("snapshot-owner", reinterpret_cast<uintptr_t>(bytes.data()),
                    bytes.size());
    const auto owner = generate_uuid();
    const auto old =
        MakeEntry(segment, "old-key", 128, segment.base, 99, {42, 99});
    RebuildReplicaTable table;
    ASSERT_TRUE(table.Record(old, old.replicas.front()));
    StandbyObjectEntry object;
    object.key = old.key;
    object.metadata.client_id = owner;
    object.metadata.size = old.size;
    object.metadata.replicas = old.replicas;
    object.metadata.rebuild_version = old.version;
    auto decoded = DecodeBatchOpLogSnapshotObjectChunk(
        EncodeBatchOpLogSnapshotObjectChunk(0, {object}), 0, 1);
    ASSERT_TRUE(decoded);
    StandbySegmentInfo saved;
    saved.segment_name = segment.name;
    saved.transport_endpoint = segment.te_endpoint;
    saved.capacity = segment.size;
    saved.is_memory_segment = true;
    MasterService restored(ReplayConfig(43));
    ASSERT_TRUE(
        restored.RestoreFromStandbySnapshot(decoded->objects, 0, {saved}));
    ASSERT_TRUE(restored.ReMountSegment({segment}, owner));
    auto prior = restored.GetCommittedRebuildEntry(old.key, "default");
    ASSERT_TRUE(prior);
    EXPECT_EQ(prior->version, old.version);
    ASSERT_TRUE(restored.Remove(old.key, TenantId::Default(), true));
    auto allocated = restored.PutStart(owner, "new-key", TenantId::Default(),
                                       old.size, ReplicateConfig{});
    ASSERT_TRUE(allocated);
    const auto& buffer =
        allocated->front().get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(buffer.buffer_address_, segment.base);
    EXPECT_EQ(buffer.allocationEpoch(), 43);
    std::fill(bytes.begin(), bytes.begin() + old.size, 'N');
    ASSERT_TRUE(restored.PutEnd(owner, "new-key", TenantId::Default(),
                                ReplicaType::MEMORY));
    auto latest = restored.GetCommittedRebuildEntry("new-key", "default");
    ASSERT_TRUE(latest);
    ASSERT_TRUE(table.Record(*latest, latest->replicas.front()));
    const auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_EQ(snapshot.front().key, "new-key");
    MasterService restarted(ReplayConfig(44));
    ASSERT_TRUE(restarted.ReMountSegment({segment}, owner));
    ASSERT_TRUE(restarted.RebuildMetadata(snapshot, owner));
    EXPECT_FALSE(restarted.GetReplicaList(old.key, TenantId::Default()));
    auto result = restarted.GetReplicaList("new-key", TenantId::Default());
    ASSERT_TRUE(result);
    const auto address = result->replicas.front()
                             .get_memory_descriptor()
                             .buffer_descriptor.buffer_address_;
    EXPECT_EQ(std::memcmp(reinterpret_cast<const void*>(address),
                          std::string(old.size, 'N').data(), old.size),
              0);
}

TEST(MasterRebuildReplayTest, NewerWriteSupersedesOtherOwnersStaleSnapshot) {
    const auto config = ReplayConfig(43);
    MasterService recovered(config);
    const UUID client_id = generate_uuid();
    auto segment = MakeSegment("version-owner", 4096, 1024);
    ASSERT_TRUE(recovered.ReMountSegment({segment}, client_id));
    auto previous = MakeEntry(segment, "key", 128, 4096, 1, {42, 1});
    auto latest = MakeEntry(segment, "key", 256, 4352, 2, {42, 2});
    ASSERT_TRUE(recovered.RebuildMetadata({previous}, client_id));
    ASSERT_TRUE(recovered.RebuildMetadata({latest}, client_id));
    ASSERT_TRUE(recovered.RebuildMetadata({previous}, client_id));
    ASSERT_TRUE(recovered.RebuildMetadata({latest}, client_id));
    const auto result = recovered.GetReplicaList("key", TenantId::Default());
    ASSERT_TRUE(result);
    ASSERT_EQ(result->replicas.size(), 1);
    const auto descriptor =
        result->replicas.front().get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(descriptor.size_, 256);
    EXPECT_EQ(descriptor.buffer_address_, 4352);
}

TEST(MasterRebuildReplayTest, RecreatedObjectsChangeGroupsInEitherReplayOrder) {
    for (const bool newest_first : {false, true}) {
        for (const std::string incoming_group : {"new-group", ""}) {
            SCOPED_TRACE(incoming_group +
                         (newest_first ? " newest" : " oldest"));
            const auto config = ReplayConfig(43);
            MasterService recovered(config);
            const UUID owner = generate_uuid();
            auto segment = MakeSegment("group-owner", 4096, 4096);
            ASSERT_TRUE(recovered.ReMountSegment({segment}, owner));
            auto old_entry =
                MakeEntry(segment, "recreated-key", 128, 4096, 1, {42, 1});
            old_entry.group_id = "old-group";
            auto latest = old_entry;
            latest.group_id = incoming_group;
            latest.version = {42, 2};
            latest.replicas.front().id = 2;
            std::get<MemoryDescriptor>(
                latest.replicas.front().descriptor_variant)
                .buffer_descriptor.buffer_address_ = 4352;
            auto unrelated = old_entry;
            unrelated.key = "unrelated-key";
            unrelated.group_id.clear();
            unrelated.replicas.front().id = 3;
            std::get<MemoryDescriptor>(
                unrelated.replicas.front().descriptor_variant)
                .buffer_descriptor.buffer_address_ = 4608;
            ASSERT_TRUE(recovered.RebuildMetadata(
                {newest_first ? latest : old_entry}, owner));
            ASSERT_TRUE(recovered.RebuildMetadata(
                {newest_first ? old_entry : latest, unrelated}, owner));
            ASSERT_TRUE(recovered.RebuildMetadata({old_entry, latest}, owner));
            const auto committed =
                recovered.GetCommittedRebuildEntry(latest.key, "default");
            ASSERT_TRUE(committed);
            EXPECT_EQ(committed->group_id, incoming_group);
            EXPECT_EQ(committed->version, latest.version);
            ASSERT_EQ(committed->replicas.size(), 1);
            EXPECT_EQ(committed->replicas.front()
                          .get_memory_descriptor()
                          .buffer_descriptor.buffer_address_,
                      4352);
            EXPECT_TRUE(
                recovered.GetReplicaList(unrelated.key, TenantId::Default()));
            ASSERT_TRUE(
                recovered.Remove(latest.key, TenantId::Default(), true));
            EXPECT_FALSE(
                recovered.GetReplicaList(latest.key, TenantId::Default()));
        }
    }
}

TEST(MasterRebuildReplayTest, FullSegmentSurvivesLostCompletionAndLaterWrites) {
    std::vector<unsigned char> bytes(4096);
    const UUID client_id = generate_uuid();
    auto segment = MakeSegment(
        "replay-owner", reinterpret_cast<uint64_t>(bytes.data()), bytes.size());
    const auto config = MasterServiceConfig::builder()
                            .set_memory_allocator(BufferAllocatorType::OFFSET)
                            .set_default_kv_lease_ttl(60000)
                            .set_eviction_high_watermark_ratio(1.0)
                            .set_view_version(42)
                            .build();
    std::vector<KeyReplicaEntry> snapshot;
    {
        MasterService original(config);
        ASSERT_TRUE(original.MountSegment(segment, client_id));
        for (uint64_t index = 0; index < 4; ++index) {
            KeyReplicaEntry entry;
            entry.key = "key-" + std::to_string(index);
            entry.size = 1024;
            auto allocated =
                original.PutStart(client_id, entry.key, TenantId::Default(),
                                  entry.size, ReplicateConfig{});
            ASSERT_TRUE(allocated);
            entry.replicas = *allocated;
            const auto& descriptor = entry.replicas.front()
                                         .get_memory_descriptor()
                                         .buffer_descriptor;
            std::memset(reinterpret_cast<void*>(descriptor.buffer_address_),
                        index + 1, entry.size);
            ASSERT_TRUE(original.PutEnd(client_id, ObjectMeta{entry.key},
                                        TenantId::Default(),
                                        ReplicaType::MEMORY));
            snapshot.push_back(entry);
        }
    }
    std::reverse(snapshot.begin(), snapshot.end());
    MasterService recovered(config);
    int completions = 0;
    int replays = 0;
    RebuildRecoveryOps operations;
    operations.remount = [&] {
        return recovered.ReMountSegment({segment}, client_id);
    };
    operations.publish_segment_descriptor = [] {
        return tl::expected<void, ErrorCode>{};
    };
    operations.publish_rpc_metadata = operations.publish_segment_descriptor;
    operations.resend_metadata = [&] {
        ++replays;
        return recovered.RebuildMetadata(snapshot, client_id);
    };
    operations.signal_complete = [&]() -> tl::expected<void, ErrorCode> {
        if (++completions == 1) return tl::make_unexpected(ErrorCode::RPC_FAIL);
        return {};
    };
    ASSERT_TRUE(RunRebuildRecovery(operations, 2, [](int) { return true; }));
    EXPECT_EQ(replays, 2);
    EXPECT_EQ(completions, 2);
    auto usage = recovered.QuerySegments(segment.name);
    ASSERT_TRUE(usage);
    EXPECT_EQ(usage->first, bytes.size());
    ASSERT_TRUE(recovered.Remove("key-0", TenantId::Default(), true));
    auto replacement = recovered.PutStart(
        client_id, "replacement", TenantId::Default(), 1024, ReplicateConfig{});
    ASSERT_TRUE(replacement);
    std::memset(reinterpret_cast<void*>(replacement->front()
                                            .get_memory_descriptor()
                                            .buffer_descriptor.buffer_address_),
                0xff, 1024);
    ASSERT_TRUE(recovered.PutEnd(client_id, ObjectMeta{"replacement"},
                                 TenantId::Default(), ReplicaType::MEMORY));
    for (uint64_t index = 1; index < 4; ++index) {
        auto result = recovered.GetReplicaList("key-" + std::to_string(index),
                                               TenantId::Default());
        ASSERT_TRUE(result);
        ASSERT_EQ(result->replicas.size(), 1);
        const auto address = result->replicas.front()
                                 .get_memory_descriptor()
                                 .buffer_descriptor.buffer_address_;
        const auto* data = reinterpret_cast<const unsigned char*>(address);
        EXPECT_TRUE(std::all_of(
            data, data + 1024,
            [index](unsigned char value) { return value == index + 1; }));
    }
}

TEST(MasterRebuildReplayTest, SharedWriteTokenAdvancesNextReplicaId) {
    const auto config = ReplayConfig(0);
    MasterService recovered(config);
    const UUID client_id = generate_uuid();
    std::vector<unsigned char> bytes(4096);
    auto segment =
        MakeSegment("shared-token-owner",
                    reinterpret_cast<uint64_t>(bytes.data()), bytes.size());
    ASSERT_TRUE(recovered.ReMountSegment({segment}, client_id));

    auto entry =
        MakeEntry(segment, "recovered-key", 128, segment.base, 7, {0, 100000});

    ASSERT_TRUE(recovered.RebuildMetadata({entry}, client_id));
    auto replacement = recovered.PutStart(
        client_id, "new-key", TenantId::Default(), 128, ReplicateConfig{});
    ASSERT_TRUE(replacement);
    ASSERT_EQ(replacement->size(), 1);
    EXPECT_GT(replacement->front().id, entry.version.second);
}

TEST(MasterRebuildReplayTest,
     ConcurrentOwnersAndDuplicateBatchesAreIdempotent) {
    MasterService recovered(
        MasterServiceConfig::builder().set_view_version(43).build());
    const UUID owners[] = {generate_uuid(), generate_uuid()};
    KeyReplicaEntry entries[2];
    for (int i = 0; i < 2; ++i) {
        auto segment = MakeSegment("concurrent-owner-" + std::to_string(i),
                                   4096 + i * 8192, 4096);
        ASSERT_TRUE(recovered.ReMountSegment({segment}, owners[i]));
        entries[i] = MakeEntry(segment, "concurrent-key", 128, segment.base,
                               i + 1, {42, 10});
    }
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 8; ++worker) {
        workers.emplace_back([&, worker] {
            const int owner = worker % 2;
            for (int repeat = 0; repeat < 32; ++repeat)
                if (!recovered.RebuildMetadata({entries[owner]}, owners[owner]))
                    ++failures;
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(failures.load(), 0);
    auto result =
        recovered.GetReplicaList("concurrent-key", TenantId::Default());
    ASSERT_TRUE(result);
    EXPECT_EQ(result->replicas.size(), 2);
}

TEST(MasterRebuildReplayTest, MissingOwnerCanRemountAndRetryTheSameBatch) {
    MasterService recovered(
        MasterServiceConfig::builder().set_view_version(43).build());
    const UUID owner = generate_uuid();
    auto segment = MakeSegment("retry-owner", 4096, 4096);
    auto entry = MakeEntry(segment, "retry-key", 128, segment.base, 1, {42, 1});
    EXPECT_FALSE(recovered.RebuildMetadata({entry}, owner));
    ASSERT_TRUE(recovered.ReMountSegment({segment}, owner));
    ASSERT_TRUE(recovered.RebuildMetadata({entry}, owner));
    ASSERT_TRUE(recovered.RebuildMetadata({entry}, owner));
    auto result = recovered.GetReplicaList(entry.key, TenantId::Default());
    ASSERT_TRUE(result);
    EXPECT_EQ(result->replicas.size(), 1);
}

TEST(MasterRebuildReplayTest,
     CurrentSnapshotAndOpLogPreserveRebuildTokenAndExtent) {
    const RebuildVersion version{42, 99};
    Replica::Descriptor replica;
    replica.id = 99;
    replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor memory;
    memory.buffer_descriptor = {128, 4096, "tcp", "snapshot-owner", 42, 256};
    replica.descriptor_variant = memory;
    StandbyObjectEntry entry;
    entry.key = "snapshot-key";
    entry.metadata.size = 128;
    entry.metadata.replicas = {replica};
    entry.metadata.rebuild_version = version;
    auto encoded = EncodeBatchOpLogSnapshotObjectChunk(0, {entry});
    auto decoded = DecodeBatchOpLogSnapshotObjectChunk(encoded, 0, 1);
    ASSERT_TRUE(decoded);
    const auto& metadata = decoded->objects.front().metadata;
    EXPECT_EQ(metadata.rebuild_version.value_or(RebuildVersion{}), version);
    const auto& buffer =
        metadata.replicas.front().get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(buffer.allocationEpoch(), 42);
    EXPECT_EQ(buffer.allocationExtent(), 256);

    MetadataPayload payload;
    payload.size = 128;
    payload.replicas = {replica};
    payload.rebuild_version = version;
    MetadataPayload restored;
    EXPECT_EQ(
        struct_pack::deserialize_to(restored, struct_pack::serialize(payload)),
        struct_pack::errc::ok);
    EXPECT_EQ(
        restored.ToStandbyMetadata().rebuild_version.value_or(RebuildVersion{}),
        version);
}

}  // namespace
}  // namespace mooncake
