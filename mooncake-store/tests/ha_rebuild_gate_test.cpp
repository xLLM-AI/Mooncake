#include <gtest/gtest.h>

#include "rpc_service.h"

namespace mooncake {
namespace {

WrappedMasterServiceConfig MakeConfig(bool initially_serving,
                                      ViewVersionId view_version = 42) {
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 100;
    config.enable_metric_reporting = false;
    config.initially_serving = initially_serving;
    config.view_version = view_version;
    return config;
}

template <typename T>
void ExpectUnavailable(
    const tl::expected<T, ErrorCode>& result,
    ErrorCode error = ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), error);
}

template <typename T>
void ExpectUnavailable(const std::vector<tl::expected<T, ErrorCode>>& results) {
    ASSERT_FALSE(results.empty());
    for (const auto& result : results) ExpectUnavailable(result);
}

TEST(HaRebuildGateTest, ConstructorHonorsInitialState) {
    WrappedMasterService closed(MakeConfig(false));
    EXPECT_FALSE(closed.IsServing());
    EXPECT_EQ(closed.GetServingState(), StoreServingState::REBUILDING);

    WrappedMasterService open(MakeConfig(true));
    EXPECT_TRUE(open.IsServing());
    EXPECT_EQ(open.GetServingState(), StoreServingState::SERVING);
}

TEST(HaRebuildGateTest, RebuildModeRejectsReplicaMigrationBeforeAllocation) {
    auto config = MakeConfig(true);
    config.enable_ha_rebuild = true;
    WrappedMasterService service(config);
    const auto client = generate_uuid();
    const auto mode = ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    ExpectUnavailable(
        service.CopyStart(client, "key", "default", "source", {"target"}),
        mode);
    ExpectUnavailable(
        service.MoveStart(client, "key", "default", "source", "target"), mode);
    ExpectUnavailable(
        service.DynamicReplicaCopyStart(client, "key", "default", "source",
                                        {"target"}, UUID{2, 3}, 1),
        mode);
    WrappedMasterService ordinary(MakeConfig(true));
    ASSERT_TRUE(ordinary.ReMountSegment({}, client));
    ExpectUnavailable(
        ordinary.CopyStart(client, "key", "default", "source", {"target"}),
        ErrorCode::SEGMENT_NOT_FOUND);
    ExpectUnavailable(
        ordinary.MoveStart(client, "key", "default", "source", "target"),
        ErrorCode::SEGMENT_NOT_FOUND);
}

TEST(HaRebuildGateTest, ExactRosterAndEpochControlOpening) {
    WrappedMasterService service(MakeConfig(false));
    const UUID first{1, 1};
    const UUID second{2, 2};
    const UUID outsider{3, 3};

    auto stale = service.SignalRebuildComplete(first, 41);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_VERSION);
    ASSERT_TRUE(service.SignalRebuildComplete(first, 42));
    service.LockRebuildExpectedClients({first, second});
    EXPECT_FALSE(service.IsServing());

    ASSERT_TRUE(service.SignalRebuildComplete(first, 42));
    ASSERT_TRUE(service.SignalRebuildComplete(outsider, 42));
    EXPECT_FALSE(service.IsServing());

    ASSERT_TRUE(service.SignalRebuildComplete(second, 42));
    EXPECT_TRUE(service.IsServing());
    EXPECT_EQ(service.GetServingState(), StoreServingState::SERVING);
}

TEST(HaRebuildGateTest, EmptyRosterOpensAndTimeoutIsDegraded) {
    WrappedMasterService empty(MakeConfig(false));
    empty.LockRebuildExpectedClients({});
    EXPECT_EQ(empty.GetServingState(), StoreServingState::SERVING);

    WrappedMasterService timed_out(MakeConfig(false));
    timed_out.LockRebuildExpectedClients({UUID{1, 1}});
    timed_out.ForceServingAfterTimeout();
    EXPECT_TRUE(timed_out.IsServing());
    EXPECT_EQ(timed_out.GetServingState(), StoreServingState::DEGRADED);
    ASSERT_TRUE(timed_out.SignalRebuildComplete(UUID{1, 1}, 42));
    EXPECT_EQ(timed_out.GetServingState(), StoreServingState::SERVING);

    empty.SetServing(false);
    EXPECT_EQ(empty.GetServingState(), StoreServingState::REBUILDING);
}

TEST(HaRebuildGateTest, ConcurrentDuplicateDoneOpensOnlyAfterExactRoster) {
    WrappedMasterService service(MakeConfig(false));
    const UUID first{1, 1};
    const UUID second{2, 2};
    service.LockRebuildExpectedClients({first, second});

    std::vector<std::thread> duplicates;
    for (int i = 0; i < 16; ++i) {
        duplicates.emplace_back(
            [&] { EXPECT_TRUE(service.SignalRebuildComplete(first, 42)); });
    }
    for (auto& thread : duplicates) thread.join();
    EXPECT_EQ(service.GetServingState(), StoreServingState::REBUILDING);

    ASSERT_TRUE(service.SignalRebuildComplete(second, 42));
    EXPECT_EQ(service.GetServingState(), StoreServingState::SERVING);
}

TEST(HaRebuildGateTest, TimeoutAndFinalDoneConvergeSafely) {
    for (int iteration = 0; iteration < 5; ++iteration) {
        WrappedMasterService service(MakeConfig(false));
        const UUID client{1, 1};
        service.LockRebuildExpectedClients({client});
        std::thread timeout([&] { service.ForceServingAfterTimeout(); });
        std::thread done(
            [&] { EXPECT_TRUE(service.SignalRebuildComplete(client, 42)); });
        timeout.join();
        done.join();
        EXPECT_EQ(service.GetServingState(), StoreServingState::SERVING);
    }
}

TEST(HaRebuildGateTest, RebuildingRejectsBusinessAndAllowsRecoveryRpc) {
    WrappedMasterService service(MakeConfig(false));
    const UUID client{1, 1};
    ReplicateConfig config;
    config.replica_num = 1;

    ExpectUnavailable(service.ExistKey("key"));
    ExpectUnavailable(service.BatchExistKey({"key"}));
    ExpectUnavailable(service.GetReplicaListByRegex(".*"));
    ExpectUnavailable(service.GetReplicaList("key"));
    ExpectUnavailable(service.BatchGetReplicaList({"key"}));
    ExpectUnavailable(
        service.BatchGetCommittedRebuildEntries({"key", "other"}, "default"));
    ExpectUnavailable(service.PutStart(client, "key", 8, config));
    ExpectUnavailable(service.PutEnd(client, ObjectMeta{"key"}));
    ExpectUnavailable(service.PutRevoke(client, "key"));
    ExpectUnavailable(service.BatchPutStart(client, {"key"}, {8}, config));
    ExpectUnavailable(service.BatchPutEnd(client, {ObjectMeta{"key"}}));
    ExpectUnavailable(service.BatchPutRevoke(client, {"key"}));
    ExpectUnavailable(service.UpsertStart(client, "key", 8, config));
    ExpectUnavailable(service.UpsertEnd(client, ObjectMeta{"key"}));
    ExpectUnavailable(service.UpsertRevoke(client, "key"));
    ExpectUnavailable(service.BatchUpsertStart(client, {"key"}, {8}, config));
    ExpectUnavailable(service.BatchUpsertEnd(client, {ObjectMeta{"key"}}));
    ExpectUnavailable(service.BatchUpsertRevoke(client, {"key"}));
    ExpectUnavailable(service.Remove("key"));
    ExpectUnavailable(service.RemoveByRegex(".*"));
    ExpectUnavailable(service.RemoveAll());
    ExpectUnavailable(service.BatchRemove({"key"}));
    Segment segment;
    segment.id = UUID{4, 4};
    segment.name = "blocked_mount";
    segment.size = 4096;
    segment.base = 4096;
    segment.te_endpoint = "127.0.0.1:1";
    ExpectUnavailable(service.MountSegment(segment, client));
    NoFSegment nof_segment;
    ExpectUnavailable(service.MountNoFSegment(nof_segment, client));
    ExpectUnavailable(service.MountLocalDiskSegment(client, true));
    ExpectUnavailable(service.UnmountLocalDiskSegment(client));
    ExpectUnavailable(service.CreateCopyTask("key", "default", {}));
    ExpectUnavailable(service.CreateMoveTask("key", "default", "a", "b"));
    ExpectUnavailable(service.CopyStart(client, "key", "default", "a", {"b"}));
    ExpectUnavailable(service.CopyEnd(client, "key", "default"));
    ExpectUnavailable(service.CopyRevoke(client, "key", "default"));
    ExpectUnavailable(service.MoveStart(client, "key", "default", "a", "b"));
    ExpectUnavailable(service.MoveEnd(client, "key", "default"));
    ExpectUnavailable(service.MoveRevoke(client, "key", "default"));
    ExpectUnavailable(service.PromotionObjectHeartbeat(client));
    ExpectUnavailable(service.OffloadObjectHeartbeat(client, true));

    // Read-only configuration is intentionally available before mounting.
    ASSERT_TRUE(service.GetFsdir());
    ASSERT_TRUE(service.GetStorageConfig());
    ASSERT_TRUE(service.ServiceReady());
    ASSERT_TRUE(service.Ping(client));
    ASSERT_TRUE(service.ReMountSegment({}, client));
    auto nof_remount = service.ReMountNoFSegment({}, client);
    EXPECT_NE(nof_remount.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ExpectUnavailable(service.RebuildMetadata({}, client, 41),
                      ErrorCode::INVALID_VERSION);
    ASSERT_TRUE(service.RebuildMetadata({}, client, 42));
    ASSERT_TRUE(service.SignalRebuildComplete(client, 42));
}

}  // namespace
}  // namespace mooncake
