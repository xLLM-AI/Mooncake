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

// One committed object and its replica locations, shared by the owner table,
// metadata RPCs and notifications.
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
    struct_pack::compatible<std::optional<uint64_t>, 5> object_checksum;
    struct_pack::compatible<std::string, 5> session_id;
    struct_pack::compatible<std::string, 5> residency_affinity_id;
    // Which master incarnation confirmed this record (distinct from the
    // allocation/write version of an object restored from an older snapshot).
    struct_pack::compatible<int64_t, 5> observed_epoch;
    KeyReplicaEntry() = default;
};
YLT_REFL(KeyReplicaEntry, key, tenant_id, size, data_type, group_id, replicas,
         version, with_soft_pin, with_hard_pin, object_checksum, session_id,
         residency_affinity_id, observed_epoch);

// All replicas of one write share the same token, including legacy entries
// with an omitted explicit version. Keep the derivation identical at both ends.
inline RebuildVersion GetRebuildVersion(const KeyReplicaEntry& entry) {
    if (entry.version != RebuildVersion{}) return entry.version;
    RebuildVersion version{};
    for (const auto& replica : entry.replicas) {
        if (!replica.is_memory_replica()) continue;
        version = std::max(
            version, RebuildVersion{replica.get_memory_descriptor()
                                        .buffer_descriptor.allocationEpoch(),
                                    replica.id});
    }
    return version;
}

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
