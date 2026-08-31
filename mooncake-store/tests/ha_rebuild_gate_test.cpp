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
void ExpectUnavailable(const tl::expected<T, ErrorCode>& result) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
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

TEST(HaRebuildGateTest, ExactRosterAndEpochControlOpening) {
    WrappedMasterService service(MakeConfig(false));
    const UUID first{1, 1};
    const UUID second{2, 2};
    const UUID outsider{3, 3};

    auto stale = service.SignalRebuildComplete(first, 41);
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_VERSION);
    ASSERT_TRUE(service.SignalRebuildComplete(first, 42).has_value());
    service.LockRebuildExpectedClients({first, second});
    EXPECT_FALSE(service.IsServing());

    ASSERT_TRUE(service.SignalRebuildComplete(first, 42).has_value());
    ASSERT_TRUE(service.SignalRebuildComplete(outsider, 42).has_value());
    EXPECT_FALSE(service.IsServing());

    ASSERT_TRUE(service.SignalRebuildComplete(second, 42).has_value());
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
    ASSERT_TRUE(timed_out.SignalRebuildComplete(UUID{1, 1}, 42).has_value());
    EXPECT_EQ(timed_out.GetServingState(), StoreServingState::SERVING);
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
    ExpectUnavailable(service.PutStart(client, "key", 8, config));
    ExpectUnavailable(service.PutEnd(client, "key"));
    ExpectUnavailable(service.PutRevoke(client, "key"));
    ExpectUnavailable(service.BatchPutStart(client, {"key"}, {8}, config));
    ExpectUnavailable(service.BatchPutEnd(client, {"key"}));
    ExpectUnavailable(service.BatchPutRevoke(client, {"key"}));
    ExpectUnavailable(service.UpsertStart(client, "key", 8, config));
    ExpectUnavailable(service.UpsertEnd(client, "key"));
    ExpectUnavailable(service.UpsertRevoke(client, "key"));
    ExpectUnavailable(service.BatchUpsertStart(client, {"key"}, {8}, config));
    ExpectUnavailable(service.BatchUpsertEnd(client, {"key"}));
    ExpectUnavailable(service.BatchUpsertRevoke(client, {"key"}));
    ExpectUnavailable(service.Remove("key"));
    ExpectUnavailable(service.RemoveByRegex(".*"));
    ExpectUnavailable(service.RemoveAll());
    ExpectUnavailable(service.BatchRemove({"key"}));
    ExpectUnavailable(service.CreateCopyTask("key", "default", {}));
    ExpectUnavailable(service.CreateMoveTask("key", "default", "a", "b"));
    ExpectUnavailable(service.CopyEnd(client, "key", "default"));
    ExpectUnavailable(service.CopyRevoke(client, "key", "default"));
    ExpectUnavailable(service.MoveEnd(client, "key", "default"));
    ExpectUnavailable(service.MoveRevoke(client, "key", "default"));
    ExpectUnavailable(service.PromotionObjectHeartbeat(client));
    ExpectUnavailable(service.OffloadObjectHeartbeat(client, true));

    ASSERT_TRUE(service.Ping(client).has_value());
    ASSERT_TRUE(service.ReMountSegment({}, client).has_value());
    auto nof_remount = service.ReMountNoFSegment({}, client);
    EXPECT_NE(nof_remount.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_TRUE(service.RebuildMetadata({}, client, 42).has_value());
    ASSERT_TRUE(service.SignalRebuildCompleteRpc(client, 42).has_value());
}

TEST(HaRebuildGateTest, StaleMetadataBatchIsRejected) {
    WrappedMasterService service(MakeConfig(false));
    auto result = service.RebuildMetadata({}, UUID{1, 1}, 41);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_VERSION);
}

}  // namespace
}  // namespace mooncake
