#include "rebuild_retry.h"

namespace mooncake {

tl::expected<void, ErrorCode> RunRebuildRecovery(
    const RebuildRecoveryOps& ops, int max_attempts,
    const std::function<bool(int)>& wait_before_retry) {
    ErrorCode last_error = ErrorCode::INTERNAL_ERROR;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        auto remount = ops.remount();
        if (!remount) {
            last_error = remount.error();
        } else {
            auto descriptor = ops.publish_segment_descriptor();
            if (!descriptor) {
                last_error = descriptor.error();
            } else {
                auto rpc_metadata = ops.publish_rpc_metadata();
                if (!rpc_metadata) {
                    last_error = rpc_metadata.error();
                } else {
                    auto metadata = ops.resend_metadata();
                    if (!metadata) {
                        last_error = metadata.error();
                    } else {
                        auto complete = ops.signal_complete();
                        if (complete) return {};
                        last_error = complete.error();
                    }
                }
            }
        }
        if (last_error == ErrorCode::INVALID_VERSION ||
            attempt + 1 == max_attempts || !wait_before_retry(attempt)) {
            return tl::make_unexpected(last_error);
        }
    }
    return tl::make_unexpected(last_error);
}
}  // namespace mooncake
