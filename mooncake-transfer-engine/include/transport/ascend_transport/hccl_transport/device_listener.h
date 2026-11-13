// Copyright 2025 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
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

#pragma once

#include <cstdint>
#include <memory>
#include <utility>

#include "hccl/hccl_types.h"

namespace mooncake::detail {

// HCCL supports Listen on port 0 and GetLocalPort after a successful Listen.
// Bind the NIC first, then use its assigned port for the VNIC. Only publish
// the pair once both listeners are ready. The factory is also the SDK seam
// used by hardware-free regression tests.
template <typename Socket, typename Factory>
HcclResult listenOnAvailableDevicePort(Factory createSocket,
                                       std::shared_ptr<Socket>& nicListener,
                                       std::shared_ptr<Socket>& vnicListener,
                                       uint64_t& devicePort) {
    constexpr int kMaxAttempts = 32;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        auto nic = createSocket(/*vnic=*/false, /*port=*/0);
        if (!nic) return HCCL_E_MEMORY;
        auto result = nic->Init();
        if (result != HCCL_SUCCESS) return result;
        result = nic->Listen();
        // In HCCL's RA listen adapter, an already-bound port is mapped to
        // HCCL_E_UNAVAIL. Do not retry parameter, allocation or device errors.
        if (result == HCCL_E_UNAVAIL) continue;
        if (result != HCCL_SUCCESS) return result;

        const uint32_t port = nic->GetLocalPort();
        if (port == 0 || port > UINT16_MAX) return HCCL_E_PARA;
        auto vnic = createSocket(/*vnic=*/true, port);
        if (!vnic) return HCCL_E_MEMORY;
        result = vnic->Init();
        if (result != HCCL_SUCCESS) return result;
        result = vnic->Listen();
        if (result == HCCL_E_UNAVAIL) {
            // DeInit stops a listener; Close alone only closes connections.
            // Refuse another attempt if rollback itself fails.
            const auto vnicCleanup = vnic->DeInit();
            const auto nicCleanup = nic->DeInit();
            if (vnicCleanup != HCCL_SUCCESS) return vnicCleanup;
            if (nicCleanup != HCCL_SUCCESS) return nicCleanup;
            continue;
        }
        if (result != HCCL_SUCCESS) return result;
        if (vnic->GetLocalPort() != port) return HCCL_E_PARA;

        nicListener = std::move(nic);
        vnicListener = std::move(vnic);
        devicePort = port;
        return HCCL_SUCCESS;
    }
    return HCCL_E_UNAVAIL;
}

}  // namespace mooncake::detail
