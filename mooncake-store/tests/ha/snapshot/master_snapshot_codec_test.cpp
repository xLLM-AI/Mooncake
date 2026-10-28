#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include <msgpack.hpp>

#include "ha/snapshot/master_snapshot_codec.h"
#include "master_config.h"
#include "master_service.h"
#include "segment.h"
#include "task_manager.h"
#include "utils/zstd_util.h"

namespace mooncake::ha {

class MasterSnapshotCodecTest : public ::testing::Test {
   protected:
    void SetUp() override { master_service_ = MakeMasterService(); }

    void TearDown() override { master_service_.reset(); }

    static std::unique_ptr<MasterService> MakeMasterService() {
        MasterServiceConfig config;
        config.default_kv_lease_ttl = 10000;
        config.eviction_ratio = 0.1;
        return std::make_unique<MasterService>(config);
    }

    // The fixture is befriended by MasterService, so private state access is
    // funneled through this helper (friendship is not inherited by the
    // TEST_F-generated subclasses).
    static MasterSnapshotStateView MakeStateView(MasterService& service) {
        return MasterSnapshotStateView(service, service.segment_manager_,
                                       service.nof_segment_manager_,
                                       service.task_manager_);
    }

    static void ExpectGroupPlacement(MasterService& service,
                                     const std::string& key,
                                     const std::string& group) {
        const size_t shard_index =
            service.getMetadataShardIndex("default", key);
        EXPECT_EQ(shard_index, service.getShardIndex(group));
        EXPECT_NE(shard_index, service.getShardIndex(key));
        MasterService::MetadataAccessorRW accessor(&service, {"default", key});
        ASSERT_TRUE(accessor.Exists());
        EXPECT_EQ(accessor.GetTenantState().group_members.at(group).count(key),
                  1);
    }

    static void ExpireLease(MasterService& service, const std::string& key) {
        MasterService::MetadataAccessorRW accessor(&service, {"default", key});
        accessor.Get().lease_timeout =
            std::chrono::system_clock::now() - std::chrono::seconds(1);
    }

    static void EvictAllEligible(MasterService& service) {
        service.BatchEvict(1.0, 1.0);
    }

    std::unique_ptr<MasterService> master_service_;
};

TEST_F(MasterSnapshotCodecTest, EncodeManifestPreservesSnapshotId) {
    std::vector<uint8_t> bytes = MasterSnapshotCodec::EncodeManifest(
        MasterSnapshotCodec::kSerializerType,
        MasterSnapshotCodec::kSerializerVersion, "snapshot-000042");
    std::string manifest(bytes.begin(), bytes.end());
    EXPECT_EQ(manifest, "messagepack|1.0.0|snapshot-000042");
}

TEST_F(MasterSnapshotCodecTest,
       ClientRebuildRestoresGroupRoutingAndSiblingLeases) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "group-rebuild-owner";
    segment.te_endpoint = segment.name;
    segment.base = 0x300000000;
    segment.size = 4096;
    const auto client_id = generate_uuid();
    ASSERT_TRUE(master_service_->MountSegment(segment, client_id));
    const std::string group = "restored-group";
    std::vector<KeyReplicaEntry> entries;
    for (const auto& key : {"group-first", "group-second"}) {
        ReplicateConfig config;
        config.group_ids = std::vector<std::string>{group};
        ASSERT_TRUE(
            master_service_->PutStart(client_id, key, "default", 128, config));
        ASSERT_TRUE(master_service_->PutEnd(client_id, key, "default",
                                            ReplicaType::MEMORY));
        auto entry = master_service_->GetCommittedRebuildEntry(key, "default");
        ASSERT_TRUE(entry);
        entries.emplace_back(std::move(*entry));
    }
    auto recovered = MakeMasterService();
    ASSERT_TRUE(recovered->ReMountSegment({segment}, client_id));
    ASSERT_TRUE(recovered->RebuildMetadata(entries, client_id));
    for (const auto& entry : entries) {
        ExpectGroupPlacement(*recovered, entry.key, group);
        ExpireLease(*recovered, entry.key);
    }
    ASSERT_TRUE(recovered->GetReplicaList(entries.front().key, "default"));
    EXPECT_FALSE(recovered->Remove(entries.back().key, "default", false));
    MasterSnapshotCodec codec;
    auto state = MakeStateView(*recovered);
    auto encoded = codec.Encode(state);
    ASSERT_TRUE(encoded);
    auto restored = MakeMasterService();
    ASSERT_TRUE(codec.Decode(restored.get(), *encoded));
    for (const auto& entry : entries) {
        ExpectGroupPlacement(*restored, entry.key, group);
        ASSERT_TRUE(restored->GetReplicaList(entry.key, "default"));
    }
}

TEST_F(MasterSnapshotCodecTest,
       ClientRebuildPreservesPinProtectionAgainstEviction) {
    auto config = MasterServiceConfig::builder()
                      .set_default_kv_lease_ttl(10000)
                      .set_default_kv_soft_pin_ttl(60000)
                      .set_allow_evict_soft_pinned_objects(false)
                      .set_eviction_high_watermark_ratio(1.0)
                      .build();
    MasterService recovered(config);
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "pin-rebuild-owner";
    segment.te_endpoint = segment.name;
    segment.base = 0x300000000;
    segment.size = 4096;
    const auto client_id = generate_uuid();
    ASSERT_TRUE(master_service_->MountSegment(segment, client_id));
    std::vector<KeyReplicaEntry> entries;
    for (const auto& key : {"hard", "soft", "ordinary"}) {
        ReplicateConfig pin_config;
        pin_config.with_hard_pin = std::string(key) == "hard";
        pin_config.with_soft_pin = std::string(key) == "soft";
        ASSERT_TRUE(master_service_->PutStart(client_id, key, "default", 128,
                                              pin_config));
        ASSERT_TRUE(master_service_->PutEnd(client_id, key, "default",
                                            ReplicaType::MEMORY));
        auto entry = master_service_->GetCommittedRebuildEntry(key, "default");
        ASSERT_TRUE(entry);
        entries.emplace_back(std::move(*entry));
    }
    ASSERT_TRUE(recovered.ReMountSegment({segment}, client_id));
    ASSERT_TRUE(recovered.RebuildMetadata(entries, client_id));
    for (const auto& entry : entries) ExpireLease(recovered, entry.key);
    EvictAllEligible(recovered);
    EXPECT_TRUE(recovered.GetReplicaListForAdmin("hard", "default"));
    EXPECT_TRUE(recovered.GetReplicaListForAdmin("soft", "default"));
    EXPECT_FALSE(recovered.GetReplicaListForAdmin("ordinary", "default"));
    const auto hard = recovered.GetCommittedRebuildEntry("hard", "default");
    const auto soft = recovered.GetCommittedRebuildEntry("soft", "default");
    ASSERT_TRUE(hard);
    ASSERT_TRUE(soft);
    EXPECT_TRUE(hard->with_hard_pin);
    EXPECT_TRUE(soft->with_soft_pin);
    EXPECT_TRUE(recovered.Remove("hard", "default", true));
}

TEST_F(MasterSnapshotCodecTest, EncodeDecodeRoundTrip) {
    MasterSnapshotCodec codec;

    MasterSnapshotStateView state_view = MakeStateView(*master_service_);

    auto encode_result = codec.Encode(state_view);
    ASSERT_TRUE(encode_result.has_value())
        << "Encode failed: " << encode_result.error().message;

    const MasterSnapshotPayloads& payloads = encode_result.value();

    // All three payload buffers must be produced.
    EXPECT_FALSE(payloads.metadata.empty());
    EXPECT_FALSE(payloads.segments.empty());
    EXPECT_FALSE(payloads.task_manager.empty());

    // Decode into a fresh service.
    auto target_service = MakeMasterService();
    auto decode_result = codec.Decode(target_service.get(), payloads);
    ASSERT_TRUE(decode_result.has_value())
        << "Decode failed: " << decode_result.error().message;
}

TEST_F(MasterSnapshotCodecTest, EncodeDecodeRoundTripWithMemoryReplica) {
    // Mount a segment and store an object backed by a MEMORY replica. On
    // decode, the segment/allocator must be restored before the metadata,
    // otherwise deserializing the replica fails with SEGMENT_NOT_FOUND because
    // GetMountedSegment() cannot find its backing segment.
    constexpr size_t kSegmentBase = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    const std::string kKey = "memory_replica_key";
    const std::string kTenant = "default";

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "codec_test_segment";
    segment.base = kSegmentBase;
    segment.size = kSegmentSize;
    segment.te_endpoint = segment.name;

    UUID client_id = generate_uuid();
    auto mount_result = master_service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    auto put_start = master_service_->PutStart(
        client_id, kKey, kTenant,
        /*slice_length=*/1024, ReplicateConfig{.replica_num = 1});
    ASSERT_TRUE(put_start.has_value())
        << "PutStart failed: " << static_cast<int>(put_start.error());
    auto put_end =
        master_service_->PutEnd(client_id, kKey, kTenant, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value())
        << "PutEnd failed: " << static_cast<int>(put_end.error());

    MasterSnapshotCodec codec;
    MasterSnapshotStateView state_view = MakeStateView(*master_service_);

    auto encode_result = codec.Encode(state_view);
    ASSERT_TRUE(encode_result.has_value())
        << "Encode failed: " << encode_result.error().message;
    EXPECT_FALSE(encode_result.value().segments.empty());

    // Decode into a fresh service. This exercises the segments-before-metadata
    // restore order.
    auto target_service = MakeMasterService();
    auto decode_result =
        codec.Decode(target_service.get(), encode_result.value());
    ASSERT_TRUE(decode_result.has_value())
        << "Decode failed: " << decode_result.error().message;

    // The MEMORY replica must be fully restored and queryable.
    auto get_result = target_service->GetReplicaList(kKey, kTenant);
    ASSERT_TRUE(get_result.has_value())
        << "GetReplicaList failed: " << static_cast<int>(get_result.error());
    EXPECT_EQ(get_result.value().replicas.size(), 1u);
}

TEST_F(MasterSnapshotCodecTest,
       RebuildWriteVersionSurvivesSnapshotAndNewEpoch) {
    master_service_ = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_view_version(42).build());
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "rebuild-snapshot-owner";
    segment.te_endpoint = segment.name;
    segment.base = 0x300000000;
    segment.size = 16 * 1024 * 1024;
    const UUID owner = generate_uuid();
    ASSERT_TRUE(master_service_->ReMountSegment({segment}, owner));
    KeyReplicaEntry entry;
    entry.key = "rebuild-key";
    entry.size = 1024;
    entry.version = {42, 100};
    Replica::Descriptor replica;
    replica.id = 10;
    replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor memory;
    memory.buffer_descriptor = {1024, segment.base, "tcp", segment.te_endpoint,
                                42,   1024};
    replica.descriptor_variant = memory;
    entry.replicas = {replica};
    ASSERT_TRUE(master_service_->RebuildMetadata({entry}, owner));
    MasterSnapshotCodec codec;
    auto state = MakeStateView(*master_service_);
    const auto encoded = codec.Encode(state);
    ASSERT_TRUE(encoded) << encoded.error().message;
    auto target = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_view_version(43).build());
    ASSERT_TRUE(codec.Decode(target.get(), *encoded));
    auto before = target->GetReplicaList(entry.key, "default");
    ASSERT_TRUE(before);
    EXPECT_EQ(before->replicas.front().id, 10);
    EXPECT_EQ(before->replicas.front()
                  .get_memory_descriptor()
                  .buffer_descriptor.allocation_epoch_,
              42);
    auto stale = entry;
    stale.version = {42, 99};
    stale.replicas.front().id = 99;
    std::get<MemoryDescriptor>(stale.replicas.front().descriptor_variant)
        .buffer_descriptor.buffer_address_ += 1024;
    ASSERT_TRUE(target->RebuildMetadata({stale}, owner));
    auto unchanged = target->GetReplicaList(entry.key, "default");
    ASSERT_TRUE(unchanged);
    EXPECT_EQ(unchanged->replicas.front().id, 10);
    auto latest = stale;
    latest.version = {42, 101};
    latest.replicas.front().id = 101;
    ASSERT_TRUE(target->RebuildMetadata({latest}, owner));
    auto after = target->GetReplicaList(entry.key, "default");
    ASSERT_TRUE(after);
    EXPECT_EQ(after->replicas.front().id, 101);
    EXPECT_EQ(after->replicas.front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              segment.base + 1024);
    auto upsert = target->UpsertStart(owner, entry.key, "default", 1024,
                                      ReplicateConfig{});
    ASSERT_TRUE(upsert);
    EXPECT_GT(upsert->front().id, 101);
    EXPECT_EQ(upsert->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.allocation_epoch_,
              43);
    ASSERT_TRUE(
        target->UpsertEnd(owner, entry.key, "default", ReplicaType::MEMORY));
    ASSERT_TRUE(target->RebuildMetadata({latest}, owner));
    auto committed = target->GetReplicaList(entry.key, "default");
    ASSERT_TRUE(committed);
    EXPECT_EQ(committed->replicas.front().id, upsert->front().id);
}

TEST_F(MasterSnapshotCodecTest, DecodeWithCorruptPayloadFails) {
    MasterSnapshotCodec codec;

    MasterSnapshotPayloads corrupt;
    corrupt.metadata = std::vector<uint8_t>{1, 2, 3};
    corrupt.segments = std::vector<uint8_t>{4, 5, 6};
    corrupt.task_manager = std::vector<uint8_t>{7, 8, 9};

    auto decode_result = codec.Decode(master_service_.get(), corrupt);
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::DESERIALIZE_FAIL);
}

TEST_F(MasterSnapshotCodecTest, DecodeWithNullService) {
    MasterSnapshotCodec codec;

    MasterSnapshotPayloads payloads;
    auto decode_result = codec.Decode(nullptr, payloads);
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::INVALID_PARAMS);
}

// Regression test: a structurally valid MessagePack task-manager payload whose
// task id field has the wrong type used to throw msgpack::type_error out of
// TaskManagerSerializer::Deserialize() (the arr[0].as<std::string>() call sits
// outside the field-conversion try block). Since RestoreState() no longer
// wraps each candidate in a try/catch, an escaping exception here would abort
// restore and prevent fallback to an older healthy snapshot. Decode() must
// convert it into a SerializationError instead of throwing.
TEST_F(MasterSnapshotCodecTest, DecodeWithInvalidTaskFieldTypeReturnsError) {
    MasterSnapshotCodec codec;

    // Start from a valid encoded snapshot so the segments and metadata payloads
    // decode cleanly; we only want to corrupt the task-manager payload.
    MasterSnapshotStateView state_view = MakeStateView(*master_service_);
    auto encode_result = codec.Encode(state_view);
    ASSERT_TRUE(encode_result.has_value())
        << "Encode failed: " << encode_result.error().message;
    MasterSnapshotPayloads payloads = std::move(encode_result.value());

    // Build a structurally valid MessagePack task-manager payload: an outer
    // array of one task, the task itself a valid array with the expected field
    // count, but the id field (index 0, expected string) is an integer. This
    // unpacks cleanly and only fails at the arr[0].as<std::string>() step,
    // which used to throw msgpack::type_error out of Deserialize().
    constexpr size_t kTaskSerializedFields = 8;  // must match the serializer
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);
    packer.pack_array(1);  // one task
    packer.pack_array(kTaskSerializedFields);
    packer.pack(static_cast<int32_t>(12345));  // id: wrong type (int, not str)
    packer.pack(static_cast<int32_t>(0));      // type
    packer.pack(static_cast<int32_t>(0));      // status
    packer.pack(std::string("payload"));       // payload
    packer.pack(static_cast<int64_t>(0));      // created_at
    packer.pack(static_cast<int64_t>(0));      // last_updated_at
    packer.pack(std::string("message"));       // message
    packer.pack(std::string("assigned"));      // assigned_client

    payloads.task_manager = zstd_compress(
        reinterpret_cast<const uint8_t*>(sbuf.data()), sbuf.size(), 3);

    // Decode a fresh service. It must not throw; it must report a serialization
    // error so RestoreState() can fall back to another candidate snapshot.
    auto target_service = MakeMasterService();
    tl::expected<void, SerializationError> decode_result;
    ASSERT_NO_THROW(
        { decode_result = codec.Decode(target_service.get(), payloads); });
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::DESERIALIZE_FAIL);
}

}  // namespace mooncake::ha
