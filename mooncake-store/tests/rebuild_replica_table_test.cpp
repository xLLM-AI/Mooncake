#include <gtest/gtest.h>

#include "rebuild_replica_table.h"

namespace mooncake {
namespace {

// Pre-NACK wire layout. Do not add compatible fields to this test schema.
enum class LegacyNotifyOp : uint8_t { UPSERT = 0, ACK = 1 };
struct LegacyNotify {
    std::string sender_client_id;
    LegacyNotifyOp op{LegacyNotifyOp::UPSERT};
    std::vector<KeyReplicaEntry> entries;
    std::string reply_endpoint;
    uint64_t request_id{0};
};
YLT_REFL(LegacyNotify, sender_client_id, op, entries, reply_endpoint,
         request_id);

KeyReplicaEntry MakeEntry(const std::string& tenant, const std::string& key,
                          uint64_t address, uint64_t size, uint64_t id,
                          int64_t epoch = 42) {
    Replica::Descriptor replica;
    replica.id = id;
    replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor memory;
    memory.buffer_descriptor = {size, address, "tcp", "owner", epoch, size};
    replica.descriptor_variant = memory;
    KeyReplicaEntry entry;
    entry.key = key;
    entry.tenant_id = tenant;
    entry.size = size;
    entry.version = {epoch, id};
    entry.replicas = {replica};
    return entry;
}

TEST(RebuildNotifyTest, NackExtensionPreservesLegacyWireLayout) {
    auto entry = MakeEntry("default", "key", 1024, 128, 1);
    entry.with_soft_pin = entry.with_hard_pin = true;
    LegacyNotify legacy{"writer", LegacyNotifyOp::UPSERT, {entry}, "owner", 7};
    RebuildNotify current;
    ASSERT_EQ(
        struct_pack::deserialize_to(current, struct_pack::serialize(legacy)),
        struct_pack::errc::ok);
    EXPECT_EQ(current.op, RebuildNotifyOp::UPSERT);
    ASSERT_EQ(current.entries.size(), 1);
    EXPECT_EQ(current.entries.front().key, "key");
    RebuildReplicaTable table;
    ASSERT_TRUE(
        table.Record(current.entries.front(), entry.replicas.front()).value());
    const auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_TRUE(snapshot.front().with_soft_pin);
    EXPECT_TRUE(snapshot.front().with_hard_pin);
    EXPECT_FALSE(current.error);
    EXPECT_EQ(current.request_id, 7);
    current.entries.clear();
    current.op = RebuildNotifyOp::ACK;
    ASSERT_EQ(
        struct_pack::deserialize_to(legacy, struct_pack::serialize(current)),
        struct_pack::errc::ok);
    EXPECT_EQ(legacy.op, LegacyNotifyOp::ACK);
    current.op = RebuildNotifyOp::NACK;
    current.error = ErrorCode::REBUILD_FENCE_EXHAUSTED;
    const auto bytes = struct_pack::serialize(current);
    RebuildNotify nack;
    ASSERT_EQ(struct_pack::deserialize_to(nack, bytes), struct_pack::errc::ok);
    EXPECT_EQ(nack.op, RebuildNotifyOp::NACK);
    EXPECT_EQ(nack.error.value(), ErrorCode::REBUILD_FENCE_EXHAUSTED);
    ASSERT_EQ(struct_pack::deserialize_to(legacy, bytes),
              struct_pack::errc::ok);
    EXPECT_NE(legacy.op, LegacyNotifyOp::ACK);
    EXPECT_NE(legacy.op, LegacyNotifyOp::UPSERT);
    EXPECT_EQ(legacy.sender_client_id, "writer");
    EXPECT_EQ(legacy.reply_endpoint, "owner");
    EXPECT_EQ(legacy.request_id, 7);
}

TEST(RebuildReplicaTableTest, SameKeyInDifferentTenantsSurvives) {
    RebuildReplicaTable table;
    auto first = MakeEntry("first", "key", 1024, 128, 1);
    auto second = MakeEntry("second", "key", 2048, 128, 2);
    ASSERT_TRUE(table.Record(first, first.replicas.front()).value());
    ASSERT_TRUE(table.Record(second, second.replicas.front()).value());
    const auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 2);
    EXPECT_EQ(snapshot[0].tenant_id, "first");
    EXPECT_EQ(snapshot[1].tenant_id, "second");
}

TEST(RebuildReplicaTableTest, ParkedNotificationCannotReplaceNewOccupant) {
    RebuildReplicaTable table;
    auto parked = MakeEntry("default", "A", 1024, 128, 1);
    auto current = MakeEntry("default", "B", 1024, 128, 2);
    ASSERT_TRUE(table.Record(parked, parked.replicas.front()).value());
    ASSERT_TRUE(table.Record(current, current.replicas.front()).value());
    EXPECT_FALSE(table.Record(parked, parked.replicas.front()).value());
    const auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_EQ(snapshot.front().key, "B");
}

TEST(RebuildReplicaTableTest, OverlappingReuseRetainsVersionOfUnusedSuffix) {
    RebuildReplicaTable table;
    auto old_large = MakeEntry("default", "A", 1024, 256, 1);
    auto new_large = MakeEntry("default", "B", 1024, 256, 2);
    auto newest_small = MakeEntry("default", "C", 1024, 128, 3);
    auto stale_suffix = MakeEntry("default", "D", 1152, 128, 1);
    ASSERT_TRUE(table.Record(old_large, old_large.replicas.front()).value());
    ASSERT_TRUE(table.Record(new_large, new_large.replicas.front()).value());
    ASSERT_TRUE(
        table.Record(newest_small, newest_small.replicas.front()).value());
    EXPECT_FALSE(
        table.Record(stale_suffix, stale_suffix.replicas.front()).value());
    const auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_EQ(snapshot.front().key, "C");
}

TEST(RebuildReplicaTableTest, NewMasterEpochOrdersCounterReset) {
    RebuildReplicaTable table;
    auto before = MakeEntry("default", "A", 1024, 128, 1000, 42);
    auto after = MakeEntry("default", "B", 1024, 128, 1, 43);
    ASSERT_TRUE(table.Record(before, before.replicas.front()).value());
    ASSERT_TRUE(table.Record(after, after.replicas.front()).value());
    EXPECT_FALSE(table.Record(before, before.replicas.front()).value());
}

TEST(RebuildReplicaTableTest, RelocationRejectsOldKeyAtAnotherAddress) {
    RebuildReplicaTable table;
    auto before = MakeEntry("default", "key", 1024, 128, 1);
    auto after = MakeEntry("default", "key", 2048, 128, 2);
    ASSERT_TRUE(table.Record(after, after.replicas.front()).value());
    EXPECT_FALSE(table.Record(before, before.replicas.front()).value());
    ASSERT_EQ(table.Snapshot().size(), 1);
}

TEST(RebuildReplicaTableTest, MultipleReplicasOnSameOwnerRemainIndependent) {
    RebuildReplicaTable table;
    auto first = MakeEntry("default", "key", 1024, 128, 1);
    auto second = MakeEntry("default", "key", 2048, 128, 2);
    first.version = second.version;
    ASSERT_TRUE(table.Record(second, second.replicas.front()).value());
    ASSERT_TRUE(table.Record(first, first.replicas.front()).value());
    ASSERT_EQ(table.Snapshot().size(), 2);
    auto replacement = MakeEntry("default", "key", 3072, 128, 3);
    ASSERT_TRUE(
        table.Record(replacement, replacement.replicas.front()).value());
    EXPECT_FALSE(table.Record(first, first.replicas.front()).value());
    ASSERT_EQ(table.Snapshot().size(), 1);
}

TEST(RebuildReplicaTableTest,
     QuarantineCannotEraseConcurrentReplacementOrFences) {
    RebuildReplicaTable table;
    auto rejected = MakeEntry("default", "key", 1024, 128, 1);
    auto replacement = MakeEntry("default", "key", 1024, 128, 2);
    ASSERT_TRUE(table.Record(rejected, rejected.replicas.front()).value());
    ASSERT_TRUE(
        table.Record(replacement, replacement.replicas.front()).value());
    table.ForgetSnapshotEntry(rejected);
    ASSERT_EQ(table.Snapshot().size(), 1);
    EXPECT_EQ(table.Snapshot().front().version, replacement.version);
    table.ForgetSnapshotEntry(replacement);
    EXPECT_TRUE(table.Snapshot().empty());
    EXPECT_FALSE(table.Record(rejected, rejected.replicas.front()).value());
}

TEST(RebuildReplicaTableTest, RetiredRegionClipsFencesButKeepsOtherKeyHistory) {
    RebuildReplicaTable table;
    auto large = MakeEntry("default", "key", 1024, 384, 100);
    auto stale_other = MakeEntry("default", "key", 2048, 128, 1);
    ASSERT_TRUE(table.Record(large, large.replicas.front()).value());
    table.ForgetRegion(1152, 128);
    // Preserve prefix/suffix version fences, remove only the retired middle.
    for (auto address : {1024, 1280}) {
        auto stale = MakeEntry("default", "stale", address, 128, 1);
        EXPECT_FALSE(table.Record(stale, stale.replicas.front()).value());
    }
    auto fresh_region = MakeEntry("default", "remounted", 1152, 128, 1);
    EXPECT_TRUE(
        table.Record(fresh_region, fresh_region.replicas.front()).value());
    table.ForgetRegion(1024, 384);
    EXPECT_TRUE(table.Snapshot().empty());
    // Even if every observed address retired, an older key at an as-yet
    // unseen address in another live segment must remain fenced.
    EXPECT_EQ(table.GetFenceUsage().ranges, 0);
    EXPECT_EQ(table.GetFenceUsage().keys, 2);
    // No live object is NOT proof that other owned segments have retired.
    EXPECT_FALSE(
        table.Record(stale_other, stale_other.replicas.front()).value());
    table.ForgetRegion(1024, 384, true);
    EXPECT_TRUE(
        table.Record(stale_other, stale_other.replicas.front()).value());
}

TEST(RebuildReplicaTableTest, FenceBudgetsNeverDiscardSafetyHistory) {
    RebuildReplicaTable table(2, 64);
    auto first = MakeEntry("default", "A", 1024, 128, 1);
    auto second = MakeEntry("default", "B", 1024, 128, 2);
    auto rejected = MakeEntry("default", "C", 1024, 128, 3);
    ASSERT_TRUE(table.Record(first, first.replicas.front()).value());
    ASSERT_TRUE(table.Record(second, second.replicas.front()).value());
    const auto full = table.Record(rejected, rejected.replicas.front());
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error(), ErrorCode::REBUILD_FENCE_EXHAUSTED);
    EXPECT_EQ(table.Snapshot().front().key, "B");
    EXPECT_FALSE(table.Record(first, first.replicas.front()).value());
    // Known identities still update at capacity; no need to evict their fences.
    second.version.second = second.replicas.front().id = 4;
    EXPECT_TRUE(table.Record(second, second.replicas.front()).value());
    table.ForgetRegion(1024, 128, true);
    EXPECT_TRUE(table.Record(rejected, rejected.replicas.front()).value());

    RebuildReplicaTable ranges(2);
    auto outer = MakeEntry("default", "large", 1024, 384, 1);
    ASSERT_TRUE(ranges.Record(outer, outer.replicas.front()).value());
    auto middle = MakeEntry("default", "middle", 1152, 128, 2);
    EXPECT_EQ(ranges.Record(middle, middle.replicas.front()).error(),
              ErrorCode::REBUILD_FENCE_EXHAUSTED);  // would create three ranges
    EXPECT_EQ(ranges.Snapshot().front().key, "large");
    RebuildReplicaTable bytes(2, 8);
    EXPECT_EQ(bytes.Record(outer, outer.replicas.front()).error(),
              ErrorCode::REBUILD_FENCE_EXHAUSTED);
}

}  // namespace
}  // namespace mooncake
