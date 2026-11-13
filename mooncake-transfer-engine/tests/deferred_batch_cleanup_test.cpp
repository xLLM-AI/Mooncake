// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "multi_transport.h"
#include "transport/transport.h"

namespace mooncake {
class MultiTransportBatchTestPeer {
   public:
    static Status free(MultiTransport& transport, Transport::BatchID id,
                       const std::function<void()>& hook) {
        return transport.freeBatchID(id, hook);
    }

    static bool pending(MultiTransport& transport, Transport::BatchID id) {
        std::lock_guard<std::mutex> lock(transport.deferred_cleanup_mutex_);
        return transport.deferred_cleanup_batches_.count(id) != 0;
    }
};

namespace {
using Peer = MultiTransportBatchTestPeer;
using Clock = std::chrono::steady_clock;

enum class QueryError { None, Context, InvalidArgument, NotImplemented };

class CounterTransport : public Transport {
   public:
    std::atomic<QueryError> error{QueryError::None};
    std::atomic<int> error_queries{0};
    bool counter_completion = false;
    bool supportsBatchCompletionCounter() const override {
        return counter_completion;
    }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID id, size_t index,
                             TransferStatus& status) override {
        const auto result = error.load();
        if (result != QueryError::None) {
            error_queries.fetch_add(1);
            switch (result) {
                case QueryError::Context:
                    return Status::Context("injected query failure");
                case QueryError::InvalidArgument:
                    return Status::InvalidArgument("injected query failure");
                case QueryError::NotImplemented:
                    return Status::NotImplemented("injected query failure");
                case QueryError::None:
                    break;
            }
        }
        auto& task = toBatchDesc(id).task_list[index];
        const auto success =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
        const auto failed =
            __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
        status.transferred_bytes = success;
        status.s = TransferStatusEnum::WAITING;
        if (success + failed == task.slice_count) {
            status.s = failed ? TransferStatusEnum::FAILED
                              : TransferStatusEnum::COMPLETED;
            __atomic_store_n(&task.is_finished, true, __ATOMIC_RELEASE);
        }
        return Status::OK();
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "counter-cleanup-test"; }
};

struct CleanupCounts {
    std::atomic<int> deleted{0};
    std::atomic<int> slices{0};
    std::atomic<bool> hook_before_slice{true};
};

struct Batch {
    Transport::BatchID id;
    Transport::BatchDesc* desc;
    std::vector<Transport::Slice*> slices;
};

Batch make_batch(MultiTransport& transport, CounterTransport& backend,
                 CleanupCounts& counts, size_t task_count = 1) {
    const auto id = transport.allocateBatchID(task_count);
    auto& desc = Transport::toBatchDesc(id);
    Batch batch{id, &desc, {}};
    batch.slices.reserve(task_count);
    for (size_t i = 0; i < task_count; ++i) {
        auto& task = desc.task_list.emplace_back();
        task.batch_id = id;
        task.transport_ = &backend;
        task.slice_count = 1;
        auto* slice = new Transport::Slice{};
        slice->task = &task;
        slice->length = 1;
        slice->status = Transport::Slice::PENDING;
        slice->ts = 0;
        slice->source_addr = &counts;
        slice->cleanup_callback = [](Transport::Slice* released) {
            auto& counts = *static_cast<CleanupCounts*>(released->source_addr);
            if (counts.deleted.load() != 1) counts.hook_before_slice = false;
            counts.slices.fetch_add(1);
        };
        task.slice_list.push_back(slice);
        batch.slices.push_back(slice);
    }
    return batch;
}

bool wait_until(const std::function<bool()>& predicate) {
    const auto deadline = Clock::now() + std::chrono::seconds(3);
    while (!predicate() && Clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return predicate();
}

void expect_reclaimed(MultiTransport& transport, const Batch& batch,
                      CleanupCounts& counts) {
    ASSERT_TRUE(
        wait_until([&] { return !Peer::pending(transport, batch.id); }));
    EXPECT_EQ(counts.deleted.load(), 1);
    EXPECT_EQ(counts.slices.load(), batch.slices.size());
    EXPECT_TRUE(counts.hook_before_slice.load());
}

TEST(DeferredBatchCleanupTest, InitialQueryErrorDoesNotTransferOwnership) {
    std::string name = "initial-query-error";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    backend.error = QueryError::Context;
    EXPECT_TRUE(
        Peer::free(transport, batch.id, [&] { ++counts.deleted; }).IsContext());
    EXPECT_FALSE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    batch.slices.front()->markFailed();
    backend.error = QueryError::None;
    EXPECT_TRUE(
        Peer::free(transport, batch.id, [&] { ++counts.deleted; }).ok());
    EXPECT_EQ(counts.deleted.load(), 1);
    EXPECT_EQ(counts.slices.load(), 1);
}

TEST(DeferredBatchCleanupTest, TransientQueryErrorRetainsOwnerAndRecovers) {
    std::string name = "transient-query-error";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = QueryError::Context;
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    backend.error = QueryError::None;
    batch.slices.front()->markSuccess();
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest, PersistentQueryErrorDrainsWithoutQueryRecovery) {
    std::string name = "persistent-query-error";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = QueryError::Context;
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_GE(backend.error_queries.load(), 2);
    EXPECT_LE(backend.error_queries.load(), 5);
    batch.slices.front()->markFailed();
    expect_reclaimed(transport, batch, counts);
}

class PermanentCleanupErrorTest : public ::testing::TestWithParam<QueryError> {
};

TEST_P(PermanentCleanupErrorTest, StopsPollingButRetainsCompletionOwner) {
    std::string name = "permanent-query-error";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = GetParam();
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    EXPECT_EQ(backend.error_queries.load(), 1);
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    batch.slices.front()->markFailed();
    expect_reclaimed(transport, batch, counts);
}

INSTANTIATE_TEST_SUITE_P(QueryErrors, PermanentCleanupErrorTest,
                         ::testing::Values(QueryError::InvalidArgument,
                                           QueryError::NotImplemented));

TEST(DeferredBatchCleanupTest, WaitsForEveryTaskAfterQueryError) {
    std::string name = "partial-completion";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts, 2);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = QueryError::InvalidArgument;
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    batch.slices.front()->markFailed();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    batch.slices.back()->markSuccess();
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest, PublishedCompletionDoesNotBypassCallbackPin) {
    std::string name = "completion-pin";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = QueryError::Context;
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    // Model an in-flight callback's internal pin, not a caller reusing its
    // invalid public handle. The pin keeps this saved descriptor alive.
    batch.desc->active_completion_callbacks.fetch_add(1);
    batch.slices.front()->markFailed();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    EXPECT_EQ(counts.slices.load(), 0);
    batch.desc->active_completion_callbacks.fetch_sub(1);
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest, ShutdownReclaimsAlreadyPublishedErrorBatch) {
    std::string name = "shutdown-error-batch";
    CounterTransport backend;
    CleanupCounts counts;
    auto transport = std::make_unique<MultiTransport>(nullptr, name);
    auto batch = make_batch(*transport, backend, counts);
    ASSERT_TRUE(Peer::free(*transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = QueryError::InvalidArgument;
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    batch.slices.front()->markFailed();
    transport.reset();
    EXPECT_EQ(counts.deleted.load(), 1);
    EXPECT_EQ(counts.slices.load(), 1);
    EXPECT_TRUE(counts.hook_before_slice.load());
}

TEST(DeferredBatchCleanupTest, RetirementKeepsHandleUntilAllTasksAndPinsDrain) {
    std::string name = "retirement-failed-sibling";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts, 2);
    bool retired = true;
    batch.slices.front()->markFailed();
    EXPECT_TRUE(
        transport.getBatchRetirementStatus(batch.id, retired).IsContext());
    EXPECT_FALSE(retired);
    EXPECT_EQ(counts.deleted.load(), 0);
    EXPECT_FALSE(Peer::pending(transport, batch.id));
    batch.desc->active_completion_callbacks.fetch_add(1);
    batch.slices.back()->markSuccess();
    EXPECT_TRUE(
        transport.getBatchRetirementStatus(batch.id, retired).IsContext());
    EXPECT_FALSE(retired);
    batch.desc->active_completion_callbacks.fetch_sub(1);
    EXPECT_TRUE(
        transport.getBatchRetirementStatus(batch.id, retired).IsContext());
    EXPECT_TRUE(retired);
    EXPECT_EQ(counts.deleted.load(), 0);
    EXPECT_TRUE(
        Peer::free(transport, batch.id, [&] { ++counts.deleted; }).ok());
    EXPECT_EQ(counts.deleted.load(), 1);
}

TEST(DeferredBatchCleanupTest, RetirementRequiresSealedCounterPublication) {
    std::string name = "retirement-submission-seal";
    CounterTransport backend;
    backend.counter_completion = true;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    auto& task = batch.desc->task_list.front();
    Transport::Slice::unsealTaskSubmission(&task);
    batch.slices.front()->markSuccess();
    Transport::TransferStatus terminal{};
    EXPECT_TRUE(backend.getTransferStatus(batch.id, 0, terminal).ok());
    bool retired = true;
    EXPECT_TRUE(transport.getBatchRetirementStatus(batch.id, retired).ok());
    EXPECT_FALSE(retired);
    // A prior terminal getter cannot make is_finished bypass the seal.
    EXPECT_TRUE(transport.getBatchRetirementStatus(batch.id, retired).ok());
    EXPECT_FALSE(retired);
    Transport::Slice::sealTaskSubmission(&task);
    EXPECT_TRUE(transport.getBatchRetirementStatus(batch.id, retired).ok());
    EXPECT_TRUE(retired);
    EXPECT_TRUE(
        Peer::free(transport, batch.id, [&] { ++counts.deleted; }).ok());
}

TEST(DeferredBatchCleanupTest, RetirementQueryFailureDoesNotInvalidateHandle) {
    std::string name = "retirement-query-error";
    CounterTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    backend.error = QueryError::Context;
    // A legacy finished flag does not make a failed API query a retirement
    // proof; only a successful terminal query or publication can do that.
    __atomic_store_n(&batch.desc->task_list.front().is_finished, true,
                     __ATOMIC_RELEASE);
    bool retired = true;
    EXPECT_TRUE(
        transport.getBatchRetirementStatus(batch.id, retired).IsContext());
    EXPECT_FALSE(retired);
    EXPECT_FALSE(Peer::pending(transport, batch.id));
    batch.slices.front()->markFailed();
    EXPECT_TRUE(
        transport.getBatchRetirementStatus(batch.id, retired).IsContext());
    EXPECT_TRUE(retired);
    EXPECT_EQ(counts.deleted.load(), 0);
    EXPECT_TRUE(
        Peer::free(transport, batch.id, [&] { ++counts.deleted; }).ok());
    EXPECT_EQ(counts.deleted.load(), 1);
}

}  // namespace
}  // namespace mooncake
