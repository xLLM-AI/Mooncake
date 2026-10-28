#include <gtest/gtest.h>

#include "rebuild_replica_table.h"

namespace mooncake {
namespace {

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

TEST(RebuildReplicaTableTest, SameKeyInDifferentTenantsSurvives) {
    RebuildReplicaTable table;
    auto first = MakeEntry("first", "key", 1024, 128, 1);
    auto second = MakeEntry("second", "key", 2048, 128, 2);
    ASSERT_TRUE(table.Record(first, first.replicas.front()));
    ASSERT_TRUE(table.Record(second, second.replicas.front()));
    const auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 2);
    EXPECT_EQ(snapshot[0].tenant_id, "first");
    EXPECT_EQ(snapshot[1].tenant_id, "second");
}

TEST(RebuildReplicaTableTest, PinAttributesSurviveNotifyAndSnapshot) {
    auto entry = MakeEntry("default", "pinned", 1024, 128, 1);
    entry.with_hard_pin = true;
    entry.with_soft_pin = true;
    RebuildNotify notify;
    notify.entries = {entry};
    const auto bytes = struct_pack::serialize(notify);
    RebuildNotify decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(decoded.entries.size(), 1);
    RebuildReplicaTable table;
    ASSERT_TRUE(table.Record(decoded.entries.front(), entry.replicas.front()));
    auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_TRUE(snapshot.front().with_soft_pin);
    EXPECT_TRUE(snapshot.front().with_hard_pin);
}

TEST(RebuildReplicaTableTest, ParkedNotificationCannotReplaceNewOccupant) {
    RebuildReplicaTable table;
    auto parked = MakeEntry("default", "A", 1024, 128, 1);
    auto current = MakeEntry("default", "B", 1024, 128, 2);
    ASSERT_TRUE(table.Record(parked, parked.replicas.front()));
    ASSERT_TRUE(table.Record(current, current.replicas.front()));
    EXPECT_FALSE(table.Record(parked, parked.replicas.front()));
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
    ASSERT_TRUE(table.Record(old_large, old_large.replicas.front()));
    ASSERT_TRUE(table.Record(new_large, new_large.replicas.front()));
    ASSERT_TRUE(table.Record(newest_small, newest_small.replicas.front()));
    EXPECT_FALSE(table.Record(stale_suffix, stale_suffix.replicas.front()));
    const auto snapshot = table.Snapshot();
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_EQ(snapshot.front().key, "C");
}

TEST(RebuildReplicaTableTest, NewMasterEpochOrdersCounterReset) {
    RebuildReplicaTable table;
    auto before = MakeEntry("default", "A", 1024, 128, 1000, 42);
    auto after = MakeEntry("default", "B", 1024, 128, 1, 43);
    ASSERT_TRUE(table.Record(before, before.replicas.front()));
    ASSERT_TRUE(table.Record(after, after.replicas.front()));
    EXPECT_FALSE(table.Record(before, before.replicas.front()));
}

TEST(RebuildReplicaTableTest, RelocationRejectsOldKeyAtAnotherAddress) {
    RebuildReplicaTable table;
    auto before = MakeEntry("default", "key", 1024, 128, 1);
    auto after = MakeEntry("default", "key", 2048, 128, 2);
    ASSERT_TRUE(table.Record(after, after.replicas.front()));
    EXPECT_FALSE(table.Record(before, before.replicas.front()));
    ASSERT_EQ(table.Snapshot().size(), 1);
}

TEST(RebuildReplicaTableTest, MultipleReplicasOnSameOwnerRemainIndependent) {
    RebuildReplicaTable table;
    auto first = MakeEntry("default", "key", 1024, 128, 1);
    auto second = MakeEntry("default", "key", 2048, 128, 2);
    first.version = second.version;
    ASSERT_TRUE(table.Record(second, second.replicas.front()));
    ASSERT_TRUE(table.Record(first, first.replicas.front()));
    ASSERT_EQ(table.Snapshot().size(), 2);
    auto replacement = MakeEntry("default", "key", 3072, 128, 3);
    ASSERT_TRUE(table.Record(replacement, replacement.replicas.front()));
    EXPECT_FALSE(table.Record(first, first.replicas.front()));
    ASSERT_EQ(table.Snapshot().size(), 1);
}

}  // namespace
}  // namespace mooncake
