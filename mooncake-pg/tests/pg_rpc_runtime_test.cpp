#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <new>
#include <thread>
#include <vector>

#include "control_plane/rpc_runtime.h"

namespace {
thread_local int allocations_until_failure = 0;
}  // namespace

// Fault injection stays in the test executable, on the launching thread.
[[gnu::noinline]] void* operator new(size_t size) {
    if (allocations_until_failure > 0 && --allocations_until_failure == 0)
        throw std::bad_alloc();
    if (void* ptr = std::malloc(size ? size : 1)) return ptr;
    throw std::bad_alloc();
}
[[gnu::noinline]] void operator delete(void* ptr) noexcept { std::free(ptr); }
[[gnu::noinline]] void operator delete(void* ptr, size_t) noexcept {
    std::free(ptr);
}

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

TEST(PgRpcRuntimeTest, LaunchFailureDoesNotLeakShutdownAdmission) {
    RpcTestService service;
    RpcServer server;
    server.registerHandler<&RpcTestService::Echo>(&service);
    ASSERT_TRUE(server.start());
    const auto addr = server.getListenAddr("127.0.0.1");
    // Executor initialization is not the operation whose admission is tested.
    (void)coro_io::get_global_executor();
    int launch_failures = 0;
    // Exercise allocation points without depending on a coroutine-frame size
    // or on the exact number of allocations in one standard-library version.
    for (int nth = 1; nth <= 8; ++nth) {
        SCOPED_TRACE(nth);
        RpcClient client(std::chrono::milliseconds(100));
        allocations_until_failure = nth;
        try {
            client.callAsync<&RpcTestService::Echo>(addr, 42,
                                                    [](PGResult<int>) {});
        } catch (const std::exception&) {
            ++launch_failures;
        }
        allocations_until_failure = 0;
        // A leaked pending-task count hangs here (bounded by CTest TIMEOUT).
        client.shutdown();
    }
    EXPECT_GT(launch_failures, 0);
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
    const auto callback_status = entered.wait_for(std::chrono::seconds(10));
    if (callback_status != std::future_status::ready) {
        release_callback.set_value();
        client.shutdown();
    }
    ASSERT_EQ(callback_status, std::future_status::ready);
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
