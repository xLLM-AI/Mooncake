#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <utility>

#include "rebuild_types.h"

namespace mooncake {

class RebuildReplicaTable {
   public:
    bool Record(const KeyReplicaEntry& entry,
                const Replica::Descriptor& replica) {
        if (!replica.is_memory_replica()) return false;
        const auto& buffer = replica.get_memory_descriptor().buffer_descriptor;
        const uint64_t address = buffer.buffer_address_;
        const uint64_t extent = buffer.allocation_size_;
        if (extent < buffer.size_ || extent == 0 ||
            extent > std::numeric_limits<uint64_t>::max() - address)
            return false;
        const uint64_t end = address + extent;
        const Identity identity{
            entry.tenant_id.empty() ? "default" : entry.tenant_id, entry.key};
        const RebuildVersion version =
            entry.version == RebuildVersion{}
                ? RebuildVersion{buffer.allocation_epoch_, replica.id}
                : entry.version;
        auto key_version = key_versions_.find(identity);
        if (key_version != key_versions_.end() && key_version->second > version)
            return false;
        auto first = ranges_.upper_bound(address);
        if (first != ranges_.begin() && std::prev(first)->second.end > address)
            --first;
        for (auto current = first;
             current != ranges_.end() && current->first < end; ++current) {
            if (current->second.version > version) return false;
        }
        if (key_version != key_versions_.end() &&
            key_version->second < version) {
            auto previous = objects_.find(identity);
            if (previous != objects_.end()) {
                for (const auto& [old_address, metadata] : previous->second)
                    addresses_.erase(old_address);
                objects_.erase(previous);
            }
        }
        auto occupant = addresses_.lower_bound(
            first == ranges_.end() ? address : std::min(address, first->first));
        while (occupant != addresses_.end() && occupant->first < end) {
            const auto old_address = occupant->first;
            auto old_object = objects_.find(occupant->second);
            if (old_object != objects_.end()) {
                old_object->second.erase(old_address);
                if (old_object->second.empty()) objects_.erase(old_object);
            }
            occupant = addresses_.erase(occupant);
        }
        while (first != ranges_.end() && first->first < end) {
            const auto start = first->first;
            const auto range = first->second;
            first = ranges_.erase(first);
            if (start < address)
                ranges_.emplace(start, Range{address, range.version});
            if (range.end > end)
                ranges_.emplace(end, Range{range.end, range.version});
        }
        ranges_[address] = Range{end, version};
        key_versions_[identity] = version;
        objects_[identity][address] = LocalReplicaMeta{replica,
                                                       entry.size,
                                                       entry.data_type,
                                                       entry.group_id,
                                                       identity.first,
                                                       version,
                                                       entry.with_soft_pin,
                                                       entry.with_hard_pin};
        addresses_[address] = identity;
        return true;
    }

    std::vector<KeyReplicaEntry> Snapshot() const {
        std::vector<KeyReplicaEntry> result;
        result.reserve(addresses_.size());
        for (const auto& [address, identity] : addresses_) {
            const auto& metadata = objects_.at(identity).at(address);
            KeyReplicaEntry entry;
            entry.key = identity.second;
            entry.tenant_id = identity.first;
            entry.size = metadata.size;
            entry.data_type = metadata.data_type;
            entry.group_id = metadata.group_id;
            entry.replicas = {metadata.replica};
            entry.version = metadata.version;
            entry.with_soft_pin = metadata.with_soft_pin;
            entry.with_hard_pin = metadata.with_hard_pin;
            result.emplace_back(std::move(entry));
        }
        return result;
    }

   private:
    using Identity = std::pair<std::string, std::string>;
    struct Range {
        uint64_t end;
        RebuildVersion version;
    };
    std::map<Identity, std::map<uint64_t, LocalReplicaMeta>> objects_;
    std::map<uint64_t, Identity> addresses_;
    std::map<uint64_t, Range> ranges_;
    std::map<Identity, RebuildVersion> key_versions_;
};

}  // namespace mooncake
