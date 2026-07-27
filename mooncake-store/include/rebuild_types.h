// HA metadata rebuild: shared types for client<->master metadata rebuild.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

// Client-side local table value: the single replica physically located in this
// client's own segment, plus the metadata master needs to rebuild the object.
// Single replica (not a vector): different replicas of one key are forced onto
// different segments, so from one client's view a key has at most one replica
// in its own segment.
struct LocalReplicaMeta {
    Replica::Descriptor replica;
    uint64_t size{0};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string group_id;
    std::string tenant_id{"default"};
};

// One key's rebuild entry: key + its replica location(s). Descriptor is already
// serializable (YLT_REFL at replica.h:477), so it travels over RPC/notify as-is.
struct KeyReplicaEntry {
    std::string key;
    std::string tenant_id{"default"};
    uint64_t size{0};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string group_id;
    std::vector<Replica::Descriptor> replicas;
    KeyReplicaEntry() = default;
};
YLT_REFL(KeyReplicaEntry, key, tenant_id, size, data_type, group_id, replicas);

// Notify payload: one notify may carry multiple keys (multi-key compatible);
// with single-key it just holds one entry. Under lazy-delete only UPSERT is
// used (a reuse write tells the owner to overwrite the stale key at that addr);
// REMOVE is reserved but never sent.
enum class RebuildNotifyOp : uint8_t { UPSERT = 0, REMOVE = 1 /*reserved*/ };

struct RebuildNotify {
    std::string sender_client_id;
    RebuildNotifyOp op{RebuildNotifyOp::UPSERT};
    std::vector<KeyReplicaEntry> entries;
    RebuildNotify() = default;
};
YLT_REFL(RebuildNotify, sender_client_id, op, entries);

}  // namespace mooncake
