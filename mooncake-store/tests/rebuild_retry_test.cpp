#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "rebuild_retry.h"

namespace mooncake {
namespace {

struct ScriptedRecovery {
    std::array<int, 5> failures{};
    std::array<int, 5> calls{};
    std::vector<int> order;
    ErrorCode failure_code{ErrorCode::RPC_FAIL};

    tl::expected<void, ErrorCode> RunStep(int step) {
        ++calls[step];
        order.push_back(step);
        if (failures[step]-- > 0) {
            return tl::make_unexpected(failure_code);
        }
        return {};
    }

    RebuildRecoveryOps Ops() {
        return {
            [&] { return RunStep(0); }, [&] { return RunStep(1); },
            [&] { return RunStep(2); }, [&] { return RunStep(3); },
            [&] { return RunStep(4); }, nullptr,
        };
    }
};

TEST(RebuildRetryTest, ExecutesAllStepsInOrderOnceOnSuccess) {
    ScriptedRecovery scripted;
    auto result =
        RunRebuildRecovery(scripted.Ops(), 3, [](int) { return true; });
    ASSERT_TRUE(result);
    EXPECT_EQ(scripted.order, (std::vector<int>{0, 1, 2, 3, 4}));
}

class RebuildFailureStepTest : public testing::TestWithParam<int> {};

TEST_P(RebuildFailureStepTest, NeverSignalsCompleteAfterExhaustedStepFailure) {
    ScriptedRecovery scripted;
    scripted.failures[GetParam()] = 3;
    int waits = 0;
    auto result = RunRebuildRecovery(scripted.Ops(), 3, [&](int) {
        ++waits;
        return true;
    });
    ASSERT_FALSE(result);
    EXPECT_EQ(scripted.calls[GetParam()], 3);
    EXPECT_EQ(scripted.calls[4], GetParam() == 4 ? 3 : 0);
    EXPECT_EQ(waits, 2);
    for (int step = GetParam() + 1; step < 4; ++step)
        EXPECT_EQ(scripted.calls[step], 0);
}

INSTANTIATE_TEST_SUITE_P(AllRecoverySteps, RebuildFailureStepTest,
                         testing::Values(0, 1, 2, 3, 4));

TEST(RebuildRetryTest, TransientBatchFailureRestartsWholeTransaction) {
    ScriptedRecovery scripted;
    scripted.failures[3] = 1;
    auto result =
        RunRebuildRecovery(scripted.Ops(), 3, [](int) { return true; });
    ASSERT_TRUE(result);
    EXPECT_EQ(scripted.calls, (std::array<int, 5>{2, 2, 2, 2, 1}));
}

TEST(RebuildRetryTest, StaleEpochStopsWithoutRetryOrDone) {
    ScriptedRecovery scripted;
    scripted.failures[3] = 1;
    scripted.failure_code = ErrorCode::INVALID_VERSION;
    auto result =
        RunRebuildRecovery(scripted.Ops(), 3, [](int) { return true; });
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_VERSION);
    EXPECT_EQ(scripted.calls[3], 1);
    EXPECT_EQ(scripted.calls[4], 0);
}

TEST(RebuildRetryTest, StaleViewRefreshesAndRetriesWholeTransaction) {
    ScriptedRecovery scripted;
    scripted.failures[3] = 1;
    scripted.failure_code = ErrorCode::INVALID_VERSION;
    auto ops = scripted.Ops();
    int refresh_calls = 0;
    ops.refresh_view = [&] {
        ++refresh_calls;
        return true;
    };
    ASSERT_TRUE(RunRebuildRecovery(ops, 3, [](int) { return true; }));
    EXPECT_EQ(refresh_calls, 1);
    EXPECT_EQ(scripted.calls, (std::array<int, 5>{2, 2, 2, 2, 1}));
}

TEST(RebuildRetryTest, ShutdownInterruptsRetries) {
    ScriptedRecovery scripted;
    scripted.failures[0] = 3;
    int waits = 0;
    auto result = RunRebuildRecovery(scripted.Ops(), 3, [&](int) {
        ++waits;
        return false;
    });
    ASSERT_FALSE(result);
    EXPECT_EQ(scripted.calls[0], 1);
    EXPECT_EQ(waits, 1);
}

}  // namespace
}  // namespace mooncake
