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
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "multi_transport.h"
#include "transport/transport.h"

namespace mooncake {
class MultiTransportBatchTestPeer final {
   public:
    static Status free(MultiTransport& transport, Transport::BatchID id,
                       const std::function<void()>& hook) {
        return transport.freeBatchID(id, hook);
    }

    static void forceBacklogWarning(MultiTransport& transport) {
        std::lock_guard<std::mutex> lock(transport.deferred_cleanup_mutex_);
        for (auto& [id, entry] : transport.deferred_cleanup_batches_) {
            entry.enqueued_at =
                std::chrono::steady_clock::now() - std::chrono::seconds(61);
        }
        transport.next_deferred_cleanup_warning_ = {};
        transport.deferred_cleanup_cv_.notify_one();
    }

    static void expire(MultiTransport& transport) {
        std::lock_guard<std::mutex> lock(transport.deferred_cleanup_mutex_);
        for (auto& [id, entry] : transport.deferred_cleanup_batches_) {
            entry.enqueued_at = std::chrono::steady_clock::now() -
                                transport.deferred_cleanup_timeout_;
        }
        transport.deferred_cleanup_cv_.notify_one();
    }
    static int64_t timeoutSeconds(MultiTransport& transport) {
        return transport.deferred_cleanup_timeout_.count();
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

class ControlledTransport final : public Transport {
   public:
    std::atomic<QueryError> error{QueryError::None};
    std::atomic<int> error_queries{0};
    std::atomic<int> queries{0};
    // A poll-driven backend publishes its completion only when queried.
    std::atomic<bool> complete_on_query{false};
    bool publish_finished = true;
    bool counter_completion = true;

    bool supportsBatchCompletionCounter() const override {
        return counter_completion;
    }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID id, size_t index,
                             TransferStatus& status) override {
        queries.fetch_add(1);
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
        if (complete_on_query.load() &&
            !__atomic_load_n(&task.completion_published, __ATOMIC_ACQUIRE)) {
            task.slice_list.front()->markSuccess();
        }
        const auto success =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
        const auto failed =
            __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
        status.transferred_bytes = success;
        status.s = TransferStatusEnum::WAITING;
        if (success + failed == task.slice_count) {
            status.s = failed ? TransferStatusEnum::FAILED
                              : TransferStatusEnum::COMPLETED;
            if (publish_finished) {
                __atomic_store_n(&task.is_finished, true, __ATOMIC_RELEASE);
            }
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

Batch make_batch(MultiTransport& transport, ControlledTransport& backend,
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
            if (counts.deleted.load() != 1) {
                counts.hook_before_slice = false;
            }
            counts.slices.fetch_add(1);
        };
        task.slice_list.push_back(slice);
        batch.slices.push_back(slice);
    }
    return batch;
}

bool wait_until(const std::function<bool()>& predicate) {
    const auto deadline = Clock::now() + std::chrono::seconds(3);
    while (!predicate() && Clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
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

TEST(DeferredBatchCleanupTest, InitialQueryErrorTransfersCleanupOwnership) {
    std::string name = "initial-query-error";
    ControlledTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    backend.error = QueryError::Context;
    EXPECT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    batch.slices.front()->markFailed();
    backend.error = QueryError::None;
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest, ExpiryRevokesBatchBeforeLateCallback) {
    std::string name = "forced-cleanup";
    ControlledTransport backend;
    backend.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts, 2);
    for (auto* slice : batch.slices) {
        slice->callback_batch_owner = batch.desc->cleanup_owner;
        slice->work_references = std::make_shared<std::atomic<uint32_t>>(1);
    }
    auto work_owner = Transport::retainSlices(batch.slices);
    backend.error = QueryError::Context;
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    Peer::expire(transport);
    ASSERT_TRUE(
        wait_until([&] { return !Peer::pending(transport, batch.id); }));
    EXPECT_EQ(counts.deleted.load(), 1);
    EXPECT_EQ(counts.slices.load(), 0);  // Live jobs still own their slices.
    EXPECT_EQ(transport.getDeferredCleanupStats().forced_cleanup_batches, 1);
    batch.slices[0]->markSuccess();
    batch.slices[1]->markFailed();  // No dereference of the revoked task.
    work_owner.reset();
    EXPECT_EQ(counts.slices.load(), 2);
}

TEST(DeferredBatchCleanupTest, ConfigurableExpiryUsesSeconds) {
    const char* previous = std::getenv("MC_DEFER_CLEANUP_TIMEOUT");
    const std::string saved = previous ? previous : "";
    const bool had_previous = previous != nullptr;
    std::string name = "timeout-config";
    setenv("MC_DEFER_CLEANUP_TIMEOUT", "7", 1);
    {
        MultiTransport transport(nullptr, name);
        EXPECT_EQ(Peer::timeoutSeconds(transport), 7);
    }
    setenv("MC_DEFER_CLEANUP_TIMEOUT", "0", 1);
    {
        MultiTransport transport(nullptr, name);
        EXPECT_EQ(Peer::timeoutSeconds(transport), 120);
    }
    if (had_previous)
        setenv("MC_DEFER_CLEANUP_TIMEOUT", saved.c_str(), 1);
    else
        unsetenv("MC_DEFER_CLEANUP_TIMEOUT");
}

TEST(DeferredBatchCleanupTest, TransientQueryErrorRetainsOwnerAndRecovers) {
    std::string name = "transient-query-error";
    ControlledTransport backend;
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
    ControlledTransport backend;
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
    ControlledTransport backend;
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
    ControlledTransport backend;
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

TEST(DeferredBatchCleanupTest, CompletedTransferCanDeferCleanupWithoutFailure) {
    std::string name = "completed-cleanup-deferred";
    ControlledTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    // Model a callback that published success but is still accessing state.
    batch.desc->active_completion_callbacks.fetch_add(1);
    batch.slices.front()->markSuccess();
    Transport::TransferStatus status{};
    ASSERT_TRUE(transport.getBatchTransferStatus(batch.id, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_FALSE(batch.desc->has_failure.load());
    EXPECT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    // Use only the callback's pinned descriptor after the public ID expires.
    batch.desc->active_completion_callbacks.fetch_sub(1);
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest, PublishedCompletionDoesNotBypassCallbackPin) {
    std::string name = "completion-pin";
    ControlledTransport backend;
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
    ControlledTransport backend;
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

class BacklogLogSink final : public google::LogSink {
   public:
    BacklogLogSink() { google::AddLogSink(this); }
    ~BacklogLogSink() override { google::RemoveLogSink(this); }
    void send(google::LogSeverity, const char*, const char*, int,
              const std::tm*, const char* message, size_t length) override {
        if (std::string_view(message, length)
                .find("Deferred cleanup backlog:") != std::string_view::npos) {
            warnings.fetch_add(1);
        }
    }
    std::atomic<int> warnings{0};
};

TEST(DeferredBatchCleanupTest, RetainedBacklogHasLiveStatsAndPeriodicWarnings) {
    BacklogLogSink logs;
    std::string name = "retained-observability";
    ControlledTransport backend;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = QueryError::Context;
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    Peer::forceBacklogWarning(transport);
    ASSERT_TRUE(wait_until([&] { return logs.warnings.load() >= 1; }));
    auto stats = transport.getDeferredCleanupStats();
    EXPECT_EQ(stats.pending_batches, 1);
    EXPECT_EQ(stats.query_error_batches, 1);
    EXPECT_GE(stats.oldest_age_ms, 60000);
    EXPECT_EQ(counts.deleted.load(), 0);
    Peer::forceBacklogWarning(transport);
    ASSERT_TRUE(wait_until([&] { return logs.warnings.load() >= 2; }));
    batch.slices.front()->markFailed();
    expect_reclaimed(transport, batch, counts);
    stats = transport.getDeferredCleanupStats();
    EXPECT_EQ(stats.pending_batches, 0);
    EXPECT_EQ(stats.query_error_batches, 0);
    EXPECT_EQ(stats.oldest_age_ms, 0);
}

TEST_P(PermanentCleanupErrorTest, DoesNotStarvePollingSibling) {
    std::string name = "permanent-error-polling-sibling";
    ControlledTransport failing;
    ControlledTransport polling;
    polling.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, failing, counts, 2);
    batch.desc->task_list.back().transport_ = &polling;
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    failing.error = GetParam();
    ASSERT_TRUE(wait_until([&] { return failing.error_queries.load() > 0; }));
    batch.slices.front()->markFailed();
    polling.complete_on_query = true;
    expect_reclaimed(transport, batch, counts);
    EXPECT_GT(polling.queries.load(), 0);
}

TEST(DeferredBatchCleanupTest,
     TransientErrorBackoffDoesNotStarvePollingSibling) {
    std::string name = "transient-error-polling-sibling";
    ControlledTransport failing;
    ControlledTransport polling;
    polling.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, failing, counts, 2);
    batch.desc->task_list.back().transport_ = &polling;
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    failing.error = QueryError::Context;
    ASSERT_TRUE(wait_until([&] { return failing.error_queries.load() > 0; }));
    // Keep the first task in error/backoff while the sibling needs polling.
    polling.complete_on_query = true;
    ASSERT_TRUE(wait_until([&] {
        return __atomic_load_n(&batch.desc->task_list.back().is_finished,
                               __ATOMIC_ACQUIRE);
    }));
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    EXPECT_EQ(transport.getDeferredCleanupStats().query_error_batches, 1);
    batch.slices.front()->markFailed();
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest, QueryErrorsAreTrackedForEveryTask) {
    std::string name = "independent-task-query-errors";
    ControlledTransport first;
    ControlledTransport second;
    second.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, first, counts, 2);
    batch.desc->task_list.back().transport_ = &second;
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    first.error = QueryError::Context;
    second.error = QueryError::Context;
    ASSERT_TRUE(wait_until([&] {
        return first.error_queries.load() > 0 &&
               second.error_queries.load() > 0;
    }));
    EXPECT_EQ(transport.getDeferredCleanupStats().query_error_batches, 1);
    batch.slices.front()->markFailed();
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    // A successful query clears only this task's error and drives completion.
    second.error = QueryError::None;
    second.complete_on_query = true;
    expect_reclaimed(transport, batch, counts);
    EXPECT_EQ(transport.getDeferredCleanupStats().query_error_batches, 0);
}

TEST(DeferredBatchCleanupTest,
     TerminalQueryIsRememberedUntilCallbackPinDrains) {
    std::string name = "terminal-query-with-pending-pin";
    ControlledTransport backend;
    backend.publish_finished = false;
    backend.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    auto& task = batch.desc->task_list.front();
    task.success_slice_count = 1;
    batch.desc->active_completion_callbacks.fetch_add(1);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    const int queries = backend.queries.load();
    backend.error = QueryError::InvalidArgument;
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(backend.queries.load(), queries);
    EXPECT_EQ(backend.error_queries.load(), 0);
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    batch.desc->active_completion_callbacks.fetch_sub(1);
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest, PollingSiblingStillWaitsForCallbackPin) {
    std::string name = "polling-sibling-with-pending-pin";
    ControlledTransport failing;
    ControlledTransport polling;
    polling.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, failing, counts, 2);
    batch.desc->task_list.back().transport_ = &polling;
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    failing.error = QueryError::NotImplemented;
    ASSERT_TRUE(wait_until([&] { return failing.error_queries.load() > 0; }));
    batch.desc->active_completion_callbacks.fetch_add(1);
    batch.slices.front()->markFailed();
    polling.complete_on_query = true;
    ASSERT_TRUE(wait_until([&] {
        return __atomic_load_n(&batch.desc->task_list.back().is_finished,
                               __ATOMIC_ACQUIRE);
    }));
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    EXPECT_EQ(counts.slices.load(), 0);
    batch.desc->active_completion_callbacks.fetch_sub(1);
    expect_reclaimed(transport, batch, counts);
}

TEST_P(PermanentCleanupErrorTest, NonCounterPublicationNeedsQueryRecovery) {
    std::string name = "noncounter-publication-after-query-error";
    ControlledTransport backend;
    backend.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = GetParam();
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    // Model a legacy backend publishing failure before its worker exits.
    // This publication and is_finished are not a counter-capability promise.
    batch.slices.front()->markFailed();
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() >= 2; }));
    EXPECT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    EXPECT_EQ(counts.slices.load(), 0);
    backend.error = QueryError::None;
    expect_reclaimed(transport, batch, counts);
}

TEST(DeferredBatchCleanupTest,
     QueryRecoveryWaitingDoesNotReviveLegacyFinished) {
    std::string name = "query-recovery-with-legacy-finished";
    ControlledTransport backend;
    backend.counter_completion = false;
    CleanupCounts counts;
    MultiTransport transport(nullptr, name);
    auto batch = make_batch(transport, backend, counts);
    ASSERT_TRUE(Peer::free(transport, batch.id, [&] {
                    ++counts.deleted;
                }).IsBatchCleanupDeferred());
    backend.error = QueryError::Context;
    ASSERT_TRUE(wait_until([&] { return backend.error_queries.load() > 0; }));
    __atomic_store_n(&batch.desc->task_list.front().is_finished, true,
                     __ATOMIC_RELEASE);
    backend.error = QueryError::None;
    ASSERT_TRUE(wait_until([&] {
        return transport.getDeferredCleanupStats().query_error_batches == 0;
    }));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    ASSERT_TRUE(Peer::pending(transport, batch.id));
    EXPECT_EQ(counts.deleted.load(), 0);
    batch.slices.front()->markSuccess();
    expect_reclaimed(transport, batch, counts);
}

}  // namespace
}  // namespace mooncake
