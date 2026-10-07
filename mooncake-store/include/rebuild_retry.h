#pragma once

#include <functional>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct RebuildRecoveryOps {
    std::function<tl::expected<void, ErrorCode>()> remount;
    std::function<tl::expected<void, ErrorCode>()> publish_segment_descriptor;
    std::function<tl::expected<void, ErrorCode>()> publish_rpc_metadata;
    std::function<tl::expected<void, ErrorCode>()> resend_metadata;
    std::function<tl::expected<void, ErrorCode>()> signal_complete;
};

tl::expected<void, ErrorCode> RunRebuildRecovery(
    const RebuildRecoveryOps& ops, int max_attempts,
    const std::function<bool(int)>& wait_before_retry);

}  // namespace mooncake
