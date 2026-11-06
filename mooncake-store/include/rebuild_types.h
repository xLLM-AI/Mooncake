// HA metadata rebuild: shared types for client<->master metadata rebuild.
#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

using RebuildVersion = std::pair<int64_t, uint64_t>;

// A locally owned allocation and its object metadata.
struct LocalReplicaMeta {
    Replica::Descriptor replica;
    uint64_t size{0};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string group_id;
    std::string tenant_id{"default"};
    RebuildVersion version{};
    bool with_soft_pin{false};
    bool with_hard_pin{false};
};

// One key's rebuild entry: key + its replica location(s). Descriptor is already
// serializable (YLT_REFL at replica.h:477), so it travels over RPC/notify
// as-is.
struct KeyReplicaEntry {
    std::string key;
    std::string tenant_id{"default"};
    uint64_t size{0};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string group_id;
    std::vector<Replica::Descriptor> replicas;
    RebuildVersion version{};
    bool with_soft_pin{false};
    bool with_hard_pin{false};
    KeyReplicaEntry() = default;
};
YLT_REFL(KeyReplicaEntry, key, tenant_id, size, data_type, group_id, replicas,
         version, with_soft_pin, with_hard_pin);

enum class RebuildNotifyOp : uint8_t { UPSERT = 0, ACK = 1 };

struct RebuildNotify {
    std::string sender_client_id;
    RebuildNotifyOp op{RebuildNotifyOp::UPSERT};
    std::vector<KeyReplicaEntry> entries;
    std::string reply_endpoint;
    uint64_t request_id{0};
    RebuildNotify() = default;
};
YLT_REFL(RebuildNotify, sender_client_id, op, entries, reply_endpoint,
         request_id);

}  // namespace mooncake
