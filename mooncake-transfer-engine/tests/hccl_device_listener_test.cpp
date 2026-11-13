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

#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <memory>
#include <thread>

#include "transport/ascend_transport/hccl_transport/device_listener.h"

namespace mooncake {
namespace {

struct SocketState {
    HcclResult initResult = HCCL_SUCCESS;
    HcclResult listenResult = HCCL_SUCCESS;
    int vnicConflicts = 0;
    int creations = 0;
    int listeners = 0;
};

// Two loopback IPs stand in for the separate NIC and VNIC endpoints. Binding
// and listening are real OS operations; only SDK errors are controlled.
class TestSocket {
   public:
    TestSocket(SocketState& state, bool vnic, uint32_t port)
        : state_(state), vnic_(vnic), port_(port) {
        ++state_.creations;
    }
    ~TestSocket() { DeInit(); }

    HcclResult Init() {
        if (state_.initResult != HCCL_SUCCESS) return state_.initResult;
        fd_ = socket(AF_INET, SOCK_STREAM, 0);
        return fd_ < 0 ? HCCL_E_SYSCALL : HCCL_SUCCESS;
    }

    HcclResult Listen() {
        if (state_.listenResult != HCCL_SUCCESS) return state_.listenResult;
        if (vnic_ && state_.vnicConflicts > 0) {
            --state_.vnicConflicts;
            return HCCL_E_UNAVAIL;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(vnic_ ? 0x7f000002 : 0x7f000001);
        address.sin_port = htons(port_);
        if (bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
            0) {
            return errno == EADDRINUSE ? HCCL_E_UNAVAIL : HCCL_E_SYSCALL;
        }
        if (listen(fd_, 1) != 0) return HCCL_E_SYSCALL;
        socklen_t size = sizeof(address);
        if (getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &size) != 0)
            return HCCL_E_SYSCALL;
        port_ = ntohs(address.sin_port);
        listening_ = true;
        ++state_.listeners;
        return HCCL_SUCCESS;
    }

    HcclResult DeInit() {
        if (fd_ >= 0) close(fd_);
        fd_ = -1;
        if (listening_) --state_.listeners;
        listening_ = false;
        return HCCL_SUCCESS;
    }

    uint32_t GetLocalPort() const { return port_; }

   private:
    SocketState& state_;
    bool vnic_;
    uint32_t port_;
    int fd_ = -1;
    bool listening_ = false;
};

HcclResult createListeners(SocketState& state, std::shared_ptr<TestSocket>& nic,
                           std::shared_ptr<TestSocket>& vnic, uint64_t& port) {
    return detail::listenOnAvailableDevicePort<TestSocket>(
        [&state](bool isVnic, uint32_t candidate) {
            return std::make_shared<TestSocket>(state, isVnic, candidate);
        },
        nic, vnic, port);
}

TEST(HcclDeviceListenerTest, OccupiedDefaultPortAllowsConcurrentListenerPairs) {
    SocketState blockerState;
    TestSocket nicBlocker(blockerState, false, 16666);
    TestSocket vnicBlocker(blockerState, true, 16666);
    ASSERT_EQ(nicBlocker.Init(), HCCL_SUCCESS);
    ASSERT_EQ(vnicBlocker.Init(), HCCL_SUCCESS);
    const auto nicBlocked = nicBlocker.Listen();
    const auto vnicBlocked = vnicBlocker.Listen();
    ASSERT_TRUE(nicBlocked == HCCL_SUCCESS || nicBlocked == HCCL_E_UNAVAIL);
    ASSERT_TRUE(vnicBlocked == HCCL_SUCCESS || vnicBlocked == HCCL_E_UNAVAIL);

    constexpr int kWorkers = 8;
    std::array<SocketState, kWorkers> states;
    std::array<std::shared_ptr<TestSocket>, kWorkers> nics;
    std::array<std::shared_ptr<TestSocket>, kWorkers> vnics;
    std::array<uint64_t, kWorkers> ports{};
    std::array<HcclResult, kWorkers> results;
    std::array<std::thread, kWorkers> workers;
    for (int i = 0; i < kWorkers; ++i) {
        workers[i] = std::thread([&, i] {
            results[i] =
                createListeners(states[i], nics[i], vnics[i], ports[i]);
        });
    }
    for (auto& worker : workers) worker.join();
    for (int i = 0; i < kWorkers; ++i) {
        ASSERT_EQ(results[i], HCCL_SUCCESS);
        EXPECT_NE(ports[i], 16666);
        EXPECT_GT(ports[i], 0);
        EXPECT_EQ(nics[i]->GetLocalPort(), ports[i]);
        EXPECT_EQ(vnics[i]->GetLocalPort(), ports[i]);
        for (int j = 0; j < i; ++j) EXPECT_NE(ports[i], ports[j]);
    }
}

TEST(HcclDeviceListenerTest, RollsBackNicWhenVnicPortIsBusy) {
    SocketState state;
    state.vnicConflicts = 1;
    std::shared_ptr<TestSocket> nic, vnic;
    uint64_t port = 0;
    ASSERT_EQ(createListeners(state, nic, vnic, port), HCCL_SUCCESS);
    EXPECT_EQ(state.creations, 4);
    EXPECT_EQ(state.listeners, 2);
    nic.reset();
    vnic.reset();
    EXPECT_EQ(state.listeners, 0);
}

TEST(HcclDeviceListenerTest, BoundsPortRetriesAndPreservesOtherErrors) {
    SocketState state;
    state.vnicConflicts = 32;
    std::shared_ptr<TestSocket> nic, vnic;
    uint64_t port = 0;
    EXPECT_EQ(createListeners(state, nic, vnic, port), HCCL_E_UNAVAIL);
    EXPECT_EQ(state.creations, 64);
    EXPECT_EQ(state.listeners, 0);
    EXPECT_EQ(port, 0);
    EXPECT_EQ(nic, nullptr);
    EXPECT_EQ(vnic, nullptr);

    for (auto error :
         {HCCL_E_MEMORY, HCCL_E_PARA, HCCL_E_SYSCALL, HCCL_E_RUNTIME}) {
        SocketState failedState;
        failedState.initResult = error;
        EXPECT_EQ(createListeners(failedState, nic, vnic, port), error);
        EXPECT_EQ(failedState.creations, 1);
        EXPECT_EQ(failedState.listeners, 0);
        failedState.initResult = HCCL_SUCCESS;
        failedState.listenResult = error;
        EXPECT_EQ(createListeners(failedState, nic, vnic, port), error);
        EXPECT_EQ(failedState.creations, 2);
        EXPECT_EQ(failedState.listeners, 0);
        EXPECT_EQ(port, 0);
    }
}

}  // namespace
}  // namespace mooncake
