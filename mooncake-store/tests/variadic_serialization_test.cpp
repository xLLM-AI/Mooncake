// Mooncake's Release flags intentionally keep assertions enabled. Force the
// NDEBUG path that disables YLT type literals in production RPC builds.
#ifndef NDEBUG
#define NDEBUG 1
#endif

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "common/network.h"
#include "rpc_types.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

TEST(VariadicSerializationTest, PreservesAllCompatibleFields) {
    using RpcRequest =
        std::tuple<UUID, std::string, uint64_t, ReplicateConfig, std::string>;
    const auto round_trip = [](const ReplicateConfig& config) {
        auto wire = struct_pack::serialize(UUID{1, 2}, std::string("key"),
                                           uint64_t{4096}, config,
                                           std::string("default"));
        return struct_pack::deserialize<RpcRequest>(wire);
    };

    ReplicateConfig config;
    config.session_ids = std::vector<std::string>{"session-a"};
    config.preferred_hca_names = std::vector<std::string>{"mlx5_0"};
    config.per_key_preferred_hca_names =
        std::vector<std::vector<std::string>>{{"mlx5_1"}};
    config.residency_affinity_ids = std::vector<std::string>{"affinity-a"};
    auto decoded = round_trip(config);
    ASSERT_TRUE(decoded);
    const auto& actual = std::get<3>(*decoded);
    EXPECT_EQ(actual.session_ids, config.session_ids);
    EXPECT_EQ(actual.preferred_hca_names, config.preferred_hca_names);
    EXPECT_EQ(actual.per_key_preferred_hca_names,
              config.per_key_preferred_hca_names);
    EXPECT_EQ(actual.residency_affinity_ids, config.residency_affinity_ids);

    ReplicateConfig empty;
    empty.residency_affinity_ids = std::vector<std::string>{};
    auto empty_decoded = round_trip(empty);
    ASSERT_TRUE(empty_decoded);
    const auto& empty_actual = std::get<3>(*empty_decoded);
    ASSERT_TRUE(empty_actual.residency_affinity_ids);
    EXPECT_TRUE(empty_actual.residency_affinity_ids->empty());
}

namespace {
class VariadicRpcService {
   public:
    // Config is a later argument, as in the production PutStart RPC. Echo
    // independently checks request encoding rather than serializing one tuple.
    std::tuple<UUID, ObjectMeta, std::string, uint64_t, ReplicateConfig> Echo(
        UUID client, ObjectMeta object, std::string tenant, uint64_t bytes,
        ReplicateConfig config) {
        return {client, std::move(object), std::move(tenant), bytes,
                std::move(config)};
    }
};
}  // namespace

TEST(VariadicSerializationTest, RealRpcPreservesLaterFieldsWithNdebug) {
    using async_simple::coro::syncAwait;
    const auto ports = getFreeTcpPorts(1);
    ASSERT_EQ(ports.size(), 1u);
    VariadicRpcService service;
    coro_rpc::coro_rpc_server server(1, ports.front(), "127.0.0.1");
    server.register_handler<&VariadicRpcService::Echo>(&service);
    auto started = server.async_start();
    ASSERT_FALSE(started.hasResult());
    coro_rpc::coro_rpc_client client;
    ASSERT_FALSE(
        syncAwait(client.connect("127.0.0.1", std::to_string(ports.front()))));
    for (int affinity = 0; affinity < 3; ++affinity) {
        SCOPED_TRACE(affinity);
        ReplicateConfig config;
        config.session_ids = std::vector<std::string>{"session-a"};
        config.preferred_hca_names = std::vector<std::string>{"mlx5_0"};
        config.per_key_preferred_hca_names =
            std::vector<std::vector<std::string>>{{"mlx5_1"}};
        if (affinity == 1)
            config.residency_affinity_ids = std::vector<std::string>{};
        if (affinity == 2)
            config.residency_affinity_ids =
                std::vector<std::string>{"affinity-a"};
        auto reply = syncAwait(client.call<&VariadicRpcService::Echo>(
            UUID{1, 2}, ObjectMeta{"key", uint64_t{0x123456789abcdef}},
            std::string("tenant-a"), uint64_t{4096}, config));
        ASSERT_TRUE(reply.has_value()) << reply.error().msg;
        const auto& [id, object, tenant, bytes, actual] = reply.value();
        EXPECT_EQ(id, (UUID{1, 2}));
        EXPECT_EQ(object.key, "key");
        EXPECT_EQ(object.object_checksum, uint64_t{0x123456789abcdef});
        EXPECT_EQ(tenant, "tenant-a");
        EXPECT_EQ(bytes, 4096u);
        EXPECT_EQ(actual.session_ids, config.session_ids);
        EXPECT_EQ(actual.preferred_hca_names, config.preferred_hca_names);
        EXPECT_EQ(actual.per_key_preferred_hca_names,
                  config.per_key_preferred_hca_names);
        ASSERT_EQ(bool(actual.residency_affinity_ids),
                  bool(config.residency_affinity_ids));
        if (config.residency_affinity_ids) {
            EXPECT_EQ(actual.residency_affinity_ids.value(),
                      config.residency_affinity_ids.value());
        }
    }
}

}  // namespace mooncake
