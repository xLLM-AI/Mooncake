#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

#include "rebuild_retry.h"

namespace mooncake {
namespace {

struct ScriptedRecovery {
    std::array<int, 5> failures{};
    std::array<int, 5> calls{};
    std::vector<int> order;

    tl::expected<void, ErrorCode> RunStep(int step) {
        ++calls[step];
        order.push_back(step);
        if (failures[step]-- > 0) {
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        return {};
    }

    RebuildRecoveryOps Ops() {
        return {
            [&] { return RunStep(0); }, [&] { return RunStep(1); },
            [&] { return RunStep(2); }, [&] { return RunStep(3); },
            [&] { return RunStep(4); },
        };
    }
};

TEST(RebuildRetryTest, ExecutesAllStepsInOrderOnceOnSuccess) {
    ScriptedRecovery scripted;
    auto result = RunRebuildRecovery(scripted.Ops(), 3,
                                     [](int) { return true; });
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(scripted.order, (std::vector<int>{0, 1, 2, 3, 4}));
}

class RebuildFailureStepTest : public testing::TestWithParam<int> {};

TEST_P(RebuildFailureStepTest, NeverSignalsCompleteAfterExhaustedStepFailure) {
    ScriptedRecovery scripted;
    scripted.failures[GetParam()] = 3;
    int waits = 0;
    auto result = RunRebuildRecovery(scripted.Ops(), 3,
                                     [&](int) { ++waits; return true; });
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(scripted.calls[GetParam()], 3);
    EXPECT_EQ(scripted.calls[4], GetParam() == 4 ? 3 : 0);
    EXPECT_EQ(waits, 2);
    for (int step = GetParam() + 1; step < 5; ++step) {
        if (step != 4) {
            EXPECT_EQ(scripted.calls[step], 0);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(AllRecoverySteps, RebuildFailureStepTest,
                         testing::Values(0, 1, 2, 3, 4));

TEST(RebuildRetryTest, TransientBatchFailureRestartsWholeTransaction) {
    ScriptedRecovery scripted;
    scripted.failures[3] = 1;
    auto result = RunRebuildRecovery(scripted.Ops(), 3,
                                     [](int) { return true; });
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(scripted.calls, (std::array<int, 5>{2, 2, 2, 2, 1}));
}

TEST(RebuildRetryTest, StaleEpochStopsWithoutRetryOrDone) {
    RebuildRecoveryOps ops;
    int metadata_calls = 0;
    int done_calls = 0;
    ops.remount = [] { return tl::expected<void, ErrorCode>{}; };
    ops.publish_segment_descriptor = [] {
        return tl::expected<void, ErrorCode>{};
    };
    ops.publish_rpc_metadata = [] { return tl::expected<void, ErrorCode>{}; };
    ops.resend_metadata = [&]() -> tl::expected<void, ErrorCode> {
        ++metadata_calls;
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    };
    ops.signal_complete = [&]() -> tl::expected<void, ErrorCode> {
        ++done_calls;
        return {};
    };

    auto result = RunRebuildRecovery(ops, 3, [](int) { return true; });
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_VERSION);
    EXPECT_EQ(metadata_calls, 1);
    EXPECT_EQ(done_calls, 0);
}

TEST(RebuildRetryTest, ShutdownInterruptsRetries) {
    ScriptedRecovery scripted;
    scripted.failures[0] = 3;
    int waits = 0;
    auto result = RunRebuildRecovery(scripted.Ops(), 3,
                                     [&](int) { ++waits; return false; });
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(scripted.calls[0], 1);
    EXPECT_EQ(waits, 1);
}

}  // namespace
}  // namespace mooncake
