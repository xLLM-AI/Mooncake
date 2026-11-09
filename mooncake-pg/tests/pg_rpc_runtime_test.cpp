#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "control_plane/rpc_runtime.h"

namespace mooncake {
namespace {
class RpcTestService {
   public:
    int Echo(int value) { return value; }
};

TEST(PgRpcRuntimeTest, CachedClientHandlesCallsFromMultipleOwnerThreads) {
    RpcTestService service;
    RpcServer server;
    server.registerHandler<&RpcTestService::Echo>(&service);
    ASSERT_TRUE(server.start());
    const auto addr = server.getListenAddr("127.0.0.1");
    RpcClient client(std::chrono::seconds(5));
    constexpr int kThreads = 4;
    constexpr int kPerThread = 32;
    std::atomic<int> completed{0};
    std::promise<void> all_done;
    auto done = all_done.get_future();
    std::vector<std::thread> callers;
    for (int t = 0; t < kThreads; ++t) {
        callers.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                const int value = t * kPerThread + i;
                client.callAsync<&RpcTestService::Echo>(
                    addr, value, [&, value](PGResult<int> result) {
                        EXPECT_TRUE(result.has_value());
                        if (result) EXPECT_EQ(result.value(), value);
                        if (completed.fetch_add(1) + 1 == kThreads * kPerThread)
                            all_done.set_value();
                    });
            }
        });
    }
    for (auto& caller : callers) caller.join();
    EXPECT_EQ(done.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    // Owner-side shutdown, never from the callback: cached sockets close on
    // their executor only after every spawned coroutine has been drained.
    client.shutdown();
    EXPECT_EQ(completed.load(), kThreads * kPerThread);
}

TEST(PgRpcRuntimeTest, ShutdownDrainsRunningCallbackBeforeOwnerDestruction) {
    RpcTestService service;
    RpcServer server;
    server.registerHandler<&RpcTestService::Echo>(&service);
    ASSERT_TRUE(server.start());
    RpcClient client(std::chrono::seconds(5));
    std::promise<void> callback_entered, release_callback, shutdown_entered;
    auto entered = callback_entered.get_future();
    auto release = release_callback.get_future().share();
    auto shutting_down = shutdown_entered.get_future();
    std::atomic<bool> callback_finished{false};
    client.callAsync<&RpcTestService::Echo>(server.getListenAddr("127.0.0.1"),
                                            42, [&](PGResult<int> result) {
                                                EXPECT_TRUE(result.has_value());
                                                callback_entered.set_value();
                                                release.wait();
                                                callback_finished.store(true);
                                            });
    ASSERT_EQ(entered.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    auto stopped = std::async(std::launch::async, [&] {
        shutdown_entered.set_value();
        client.shutdown();
    });
    shutting_down.wait();
    // Bounded deadline assertion on a controlled callback, not a timing sleep.
    EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    release_callback.set_value();
    EXPECT_EQ(stopped.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    EXPECT_TRUE(callback_finished.load());
    client.shutdown();  // idempotent owner cleanup
}
}  // namespace
}  // namespace mooncake
