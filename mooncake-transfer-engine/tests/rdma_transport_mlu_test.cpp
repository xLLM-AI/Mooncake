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

#include <glog/logging.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <numa.h>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "common.h"
#include "memory_location.h"
#include "transfer_engine.h"
#include "transport/transport.h"

#if !defined(USE_MLU) || !__has_include(<cnrt.h>)
#error "rdma_transport_mlu_test requires USE_MLU and Neuware headers"
#endif

#include <cnrt.h>

using namespace mooncake;

namespace {

struct TestOptions {
    std::string local_server_name = mooncake::getHostname();
    std::string metadata_server = "127.0.0.1:2379";
    std::string mode = "initiator";
    std::string device_name = "mlx5_0";
    std::string nic_priority_matrix;
    std::string segment_id = "127.0.0.1";
    std::string expect_remote_location;
    bool use_mlu = true;
    bool use_wildcard_location = false;
    int mlu_id = 0;
    size_t buffer_size = 64ull << 20;
    size_t data_length = 4ull << 20;
};

TestOptions g_options;

bool parseBool(const std::string &value) {
    return value == "1" || value == "true" || value == "TRUE" ||
           value == "on" || value == "yes";
}

void parseArgs(int argc, char **argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (!arg.starts_with("--")) continue;
        auto pos = arg.find('=');
        std::string key =
            pos == std::string::npos ? arg.substr(2) : arg.substr(2, pos - 2);
        std::string value =
            pos == std::string::npos ? "" : arg.substr(pos + 1);

        if (key == "local_server_name") {
            g_options.local_server_name = value;
        } else if (key == "metadata_server") {
            g_options.metadata_server = value;
        } else if (key == "mode") {
            g_options.mode = value;
        } else if (key == "device_name") {
            g_options.device_name = value;
        } else if (key == "nic_priority_matrix") {
            g_options.nic_priority_matrix = value;
        } else if (key == "segment_id") {
            g_options.segment_id = value;
        } else if (key == "expect_remote_location") {
            g_options.expect_remote_location = value;
        } else if (key == "use_mlu") {
            g_options.use_mlu = parseBool(value);
        } else if (key == "use_wildcard_location") {
            g_options.use_wildcard_location = parseBool(value);
        } else if (key == "mlu_id") {
            g_options.mlu_id = std::stoi(value);
        } else if (key == "buffer_size") {
            g_options.buffer_size = std::stoull(value);
        } else if (key == "data_length") {
            g_options.data_length = std::stoull(value);
        }
    }
}

void checkCnrtError(cnrtRet_t result, const char *message) {
    if (result != cnrtSuccess) {
        LOG(ERROR) << message << ", cnrt error=" << result;
        std::exit(EXIT_FAILURE);
    }
}

void configureLocalDevice() {
    if (!g_options.use_mlu) return;
    checkCnrtError(cnrtSetDevice(g_options.mlu_id), "Failed to set MLU device");
}

std::string expectedLocalLocation() {
    if (!g_options.use_mlu) return "cpu:0";
    return "mlu:" + std::to_string(g_options.mlu_id);
}

std::string requestedLocalLocation() {
    return g_options.use_wildcard_location ? kWildcardLocation
                                           : expectedLocalLocation();
}

void logRegisteredLocation(void *addr) {
    auto entries = getMemoryLocation(addr, std::min(g_options.data_length, size_t(4096)));
    if (entries.empty()) {
        LOG(WARNING) << "getMemoryLocation returned empty result";
        return;
    }
    LOG(INFO) << "Detected local memory location: " << entries[0].location;
}

void *allocateMemoryPool(size_t size, int socket_id, bool from_mlu) {
    if (from_mlu) {
        void *d_buf = nullptr;
        configureLocalDevice();
        checkCnrtError(cnrtMalloc(&d_buf, size),
                       "Failed to allocate MLU device memory");
        return d_buf;
    }
    return numa_alloc_onnode(size, socket_id);
}

void freeMemoryPool(void *addr, size_t size, bool from_mlu) {
    if (from_mlu) {
        configureLocalDevice();
        checkCnrtError(cnrtFree(addr), "Failed to free MLU device memory");
        return;
    }
    numa_free(addr, size);
}

void fillPattern(char *buf, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        buf[i] = 'a' + lrand48() % 26;
    }
}

void copyHostToRegistered(void *registered_addr, const void *host_addr,
                          size_t size, bool to_mlu) {
    if (to_mlu) {
        configureLocalDevice();
        checkCnrtError(cnrtMemcpy(registered_addr, const_cast<void *>(host_addr),
                                  size, cnrtMemcpyHostToDev),
                       "Failed to copy host buffer to MLU");
        return;
    }
    memcpy(registered_addr, host_addr, size);
}

void copyRegisteredToHost(void *host_addr, const void *registered_addr,
                          size_t size, bool from_mlu) {
    if (from_mlu) {
        configureLocalDevice();
        checkCnrtError(cnrtMemcpy(host_addr, const_cast<void *>(registered_addr),
                                  size, cnrtMemcpyDevToHost),
                       "Failed to copy MLU buffer to host");
        return;
    }
    memcpy(host_addr, registered_addr, size);
}

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::string loadNicPriorityMatrix() {
    if (!g_options.nic_priority_matrix.empty()) {
        std::ifstream file(g_options.nic_priority_matrix);
        if (file.is_open()) {
            std::string content((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
            file.close();
            return content;
        }
    }

    auto device_names = formatDeviceNames(g_options.device_name);
    std::string matrix = "{\"cpu:0\": [[" + device_names + "], []]";
    matrix += ", \"cpu:1\": [[" + device_names + "], []]";
    if (g_options.use_mlu) {
        matrix += ", \"mlu:" + std::to_string(g_options.mlu_id) + "\": [[" +
                  device_names + "], []]";
    }
    matrix += "}";
    return matrix;
}

}  // namespace

int initiatorWorker(TransferEngine *engine, SegmentID segment_id, void *addr,
                    bool use_mlu) {
    bindToSocket(0);
    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    LOG_ASSERT(segment_desc);
    LOG_ASSERT(!segment_desc->buffers.empty());
    LOG(INFO) << "Remote segment protocol: " << segment_desc->protocol;
    LOG(INFO) << "Remote buffer location: " << segment_desc->buffers[0].name;
    if (!g_options.expect_remote_location.empty()) {
        LOG_ASSERT(segment_desc->buffers[0].name ==
                   g_options.expect_remote_location);
    }
    uint64_t remote_base = segment_desc->buffers[0].addr;
    const size_t kDataLength = g_options.data_length;
    auto write_shadow = std::make_unique<char[]>(kDataLength);
    auto read_shadow = std::make_unique<char[]>(kDataLength);

    fillPattern(write_shadow.get(), kDataLength);
    copyHostToRegistered(addr, write_shadow.get(), kDataLength, use_mlu);

    {
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = reinterpret_cast<uint8_t *>(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        Status s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        TransferStatus status;
        bool completed = false;
        while (!completed) {
            s = engine->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                completed = true;
            } else if (status.s == TransferStatusEnum::FAILED) {
                LOG(FATAL) << "WRITE transfer failed";
            }
        }
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    {
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = reinterpret_cast<uint8_t *>(addr) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        Status s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        TransferStatus status;
        bool completed = false;
        while (!completed) {
            s = engine->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                completed = true;
            } else if (status.s == TransferStatusEnum::FAILED) {
                LOG(FATAL) << "READ transfer failed";
            }
        }
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    copyRegisteredToHost(read_shadow.get(),
                         reinterpret_cast<uint8_t *>(addr) + kDataLength,
                         kDataLength, use_mlu);

    int ret = memcmp(write_shadow.get(), read_shadow.get(), kDataLength);
    LOG(INFO) << "MLU RDMA compare: " << (ret == 0 ? "OK" : "FAILED");
    return ret == 0 ? 0 : -1;
}

int initiator() {
    LOG_ASSERT(g_options.buffer_size >= g_options.data_length * 2);
    configureLocalDevice();
    auto engine = std::make_unique<TransferEngine>(false);

    auto hostname_port = parseHostNameWithPort(g_options.local_server_name);
    engine->init(g_options.metadata_server, g_options.local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    auto nic_priority_matrix = loadNicPriorityMatrix();
    void *args[2] = {const_cast<char *>(nic_priority_matrix.c_str()), nullptr};
    auto *xport = engine->installTransport("rdma", args);
    LOG_ASSERT(xport);
    LOG(INFO) << "Local topology: " << engine->getLocalTopology()->toString();

    void *addr =
        allocateMemoryPool(g_options.buffer_size, 0, g_options.use_mlu);
    std::string location = requestedLocalLocation();
    LOG(INFO) << "Registering local memory with location: " << location;
    int rc = engine->registerLocalMemory(addr, g_options.buffer_size, location);
    LOG_ASSERT(!rc);
    logRegisteredLocation(addr);

    auto segment_id = engine->openSegment(g_options.segment_id.c_str());
    std::thread worker(initiatorWorker, engine.get(), segment_id, addr,
                       g_options.use_mlu);
    worker.join();
    engine->unregisterLocalMemory(addr);
    freeMemoryPool(addr, g_options.buffer_size, g_options.use_mlu);
    return 0;
}

int target() {
    LOG_ASSERT(g_options.buffer_size >= g_options.data_length * 2);
    configureLocalDevice();
    auto engine = std::make_unique<TransferEngine>(false);

    auto hostname_port = parseHostNameWithPort(g_options.local_server_name);
    engine->init(g_options.metadata_server, g_options.local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    auto nic_priority_matrix = loadNicPriorityMatrix();
    void *args[2] = {const_cast<char *>(nic_priority_matrix.c_str()), nullptr};
    auto *xport = engine->installTransport("rdma", args);
    LOG_ASSERT(xport);
    LOG(INFO) << "Local topology: " << engine->getLocalTopology()->toString();

    void *addr =
        allocateMemoryPool(g_options.buffer_size, 0, g_options.use_mlu);
    std::string location = requestedLocalLocation();
    LOG(INFO) << "Registering local memory with location: " << location;
    int rc = engine->registerLocalMemory(addr, g_options.buffer_size, location);
    LOG_ASSERT(!rc);
    logRegisteredLocation(addr);

    while (true) sleep(1);

    return 0;
}

int main(int argc, char **argv) {
    parseArgs(argc, argv);

    if (g_options.mode == "initiator") return initiator();
    if (g_options.mode == "target") return target();

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    return EXIT_FAILURE;
}
