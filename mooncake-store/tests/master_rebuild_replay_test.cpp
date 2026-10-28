#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <vector>

#include "master_service.h"
#include "rebuild_retry.h"
#include "utils.h"

namespace mooncake {
namespace {

TEST(MasterRebuildReplayTest, NewerWriteSupersedesOtherOwnersStaleSnapshot) {
    const auto config = MasterServiceConfig::builder()
                            .set_view_version(43)
                            .set_default_kv_lease_ttl(60000)
                            .build();
    MasterService recovered(config);
    const UUID client_id = generate_uuid();
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "version-owner";
    segment.te_endpoint = segment.name;
    segment.base = 4096;
    segment.size = 1024;
    ASSERT_TRUE(recovered.ReMountSegment({segment}, client_id));
    KeyReplicaEntry previous;
    previous.key = "key";
    previous.size = 128;
    previous.version = {42, 1};
    Replica::Descriptor old_replica;
    old_replica.id = 1;
    old_replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor old_memory;
    old_memory.buffer_descriptor = {128, 4096, "tcp", segment.te_endpoint,
                                    42,  128};
    old_replica.descriptor_variant = old_memory;
    previous.replicas = {old_replica};
    auto latest = previous;
    latest.size = 256;
    latest.version = {42, 2};
    latest.replicas.front().id = 2;
    auto& latest_memory =
        std::get<MemoryDescriptor>(latest.replicas.front().descriptor_variant);
    latest_memory.buffer_descriptor = {256, 4352, "tcp", segment.te_endpoint,
                                       42,  256};
    ASSERT_TRUE(recovered.RebuildMetadata({previous}, client_id));
    ASSERT_TRUE(recovered.RebuildMetadata({latest}, client_id));
    ASSERT_TRUE(recovered.RebuildMetadata({previous}, client_id));
    ASSERT_TRUE(recovered.RebuildMetadata({latest}, client_id));
    const auto result = recovered.GetReplicaList("key", "default");
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
            const auto config = MasterServiceConfig::builder()
                                    .set_default_kv_lease_ttl(60000)
                                    .set_view_version(43)
                                    .build();
            MasterService recovered(config);
            const UUID owner = generate_uuid();
            Segment segment;
            segment.id = generate_uuid();
            segment.name = "group-owner";
            segment.te_endpoint = segment.name;
            segment.base = 4096;
            segment.size = 4096;
            ASSERT_TRUE(recovered.ReMountSegment({segment}, owner));
            KeyReplicaEntry old_entry;
            old_entry.key = "recreated-key";
            old_entry.group_id = "old-group";
            old_entry.size = 128;
            old_entry.version = {42, 1};
            Replica::Descriptor replica;
            replica.id = 1;
            replica.status = ReplicaStatus::COMPLETE;
            MemoryDescriptor memory;
            memory.buffer_descriptor = {128, 4096, "tcp", segment.te_endpoint,
                                        42,  128};
            replica.descriptor_variant = memory;
            old_entry.replicas = {replica};
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
            EXPECT_TRUE(recovered.GetReplicaList(unrelated.key, "default"));
            ASSERT_TRUE(recovered.Remove(latest.key, "default", true));
            EXPECT_FALSE(recovered.GetReplicaList(latest.key, "default"));
        }
    }
}

TEST(MasterRebuildReplayTest, FullSegmentSurvivesLostCompletionAndLaterWrites) {
    std::vector<unsigned char> bytes(4096);
    const UUID client_id = generate_uuid();
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "replay-owner";
    segment.te_endpoint = segment.name;
    segment.base = reinterpret_cast<uint64_t>(bytes.data());
    segment.size = bytes.size();
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
            auto allocated = original.PutStart(client_id, entry.key, "default",
                                               entry.size, ReplicateConfig{});
            ASSERT_TRUE(allocated);
            entry.replicas = *allocated;
            const auto& descriptor = entry.replicas.front()
                                         .get_memory_descriptor()
                                         .buffer_descriptor;
            std::memset(reinterpret_cast<void*>(descriptor.buffer_address_),
                        index + 1, entry.size);
            ASSERT_TRUE(original.PutEnd(client_id, entry.key, "default",
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
    ASSERT_TRUE(recovered.Remove("key-0", "default", true));
    auto replacement = recovered.PutStart(client_id, "replacement", "default",
                                          1024, ReplicateConfig{});
    ASSERT_TRUE(replacement);
    std::memset(reinterpret_cast<void*>(replacement->front()
                                            .get_memory_descriptor()
                                            .buffer_descriptor.buffer_address_),
                0xff, 1024);
    ASSERT_TRUE(recovered.PutEnd(client_id, "replacement", "default",
                                 ReplicaType::MEMORY));
    for (uint64_t index = 1; index < 4; ++index) {
        auto result =
            recovered.GetReplicaList("key-" + std::to_string(index), "default");
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
    const auto config = MasterServiceConfig::builder()
                            .set_memory_allocator(BufferAllocatorType::OFFSET)
                            .set_view_version(0)
                            .set_default_kv_lease_ttl(60000)
                            .build();
    MasterService recovered(config);
    const UUID client_id = generate_uuid();
    std::vector<unsigned char> bytes(4096);
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "shared-token-owner";
    segment.te_endpoint = segment.name;
    segment.base = reinterpret_cast<uint64_t>(bytes.data());
    segment.size = bytes.size();
    ASSERT_TRUE(recovered.ReMountSegment({segment}, client_id));

    KeyReplicaEntry entry;
    entry.key = "recovered-key";
    entry.size = 128;
    entry.version = {0, 100000};
    Replica::Descriptor descriptor;
    descriptor.id = 7;
    descriptor.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor memory;
    memory.buffer_descriptor = {128, segment.base, "tcp", segment.te_endpoint,
                                0, 128};
    descriptor.descriptor_variant = memory;
    entry.replicas = {descriptor};

    ASSERT_TRUE(recovered.RebuildMetadata({entry}, client_id));
    auto replacement = recovered.PutStart(client_id, "new-key", "default",
                                          128, ReplicateConfig{});
    ASSERT_TRUE(replacement);
    ASSERT_EQ(replacement->size(), 1);
    EXPECT_GT(replacement->front().id, entry.version.second);
}

}  // namespace
}  // namespace mooncake
