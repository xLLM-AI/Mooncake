#include "rebuild_retry.h"

#include <initializer_list>

namespace mooncake {

tl::expected<void, ErrorCode> RunRebuildRecovery(
    const RebuildRecoveryOps& ops, int max_attempts,
    const std::function<bool(int)>& wait_before_retry) {
    ErrorCode last_error = ErrorCode::INTERNAL_ERROR;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        tl::expected<void, ErrorCode> result;
        for (const auto* step : {&ops.remount, &ops.publish_segment_descriptor,
                                 &ops.publish_rpc_metadata,
                                 &ops.resend_metadata, &ops.signal_complete}) {
            result = (*step)();
            if (!result) break;
        }
        if (result) return {};
        last_error = result.error();
        if (last_error == ErrorCode::INVALID_VERSION &&
            (!ops.refresh_view || !ops.refresh_view())) {
            return tl::make_unexpected(last_error);
        }
        if (attempt + 1 == max_attempts || !wait_before_retry(attempt)) {
            return tl::make_unexpected(last_error);
        }
    }
    return tl::make_unexpected(last_error);
}
}  // namespace mooncake
