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
        const uint64_t extent = buffer.allocationExtent();
        if (extent < buffer.size_ || extent == 0 ||
            extent > std::numeric_limits<uint64_t>::max() - address)
            return false;
        const uint64_t end = address + extent;
        const Identity identity{
            entry.tenant_id.empty() ? "default" : entry.tenant_id, entry.key};
        const RebuildVersion version =
            entry.version == RebuildVersion{}
                ? RebuildVersion{buffer.allocationEpoch(), replica.id}
                : entry.version;
        auto key_version = key_versions_.find(identity);
        if (key_version != key_versions_.end() && key_version->second > version)
            return false;
        auto previous = objects_.find(identity);
        if (previous != objects_.end()) {
            auto found = previous->second.find(address);
            if (found != previous->second.end() &&
                found->second.version == version &&
                found->second.observed_epoch.value_or(version.first) >
                    entry.observed_epoch.value_or(version.first))
                return false;
        }
        auto first = ranges_.upper_bound(address);
        if (first != ranges_.begin() && std::prev(first)->second.end > address)
            --first;
        for (auto current = first;
             current != ranges_.end() && current->first < end; ++current) {
            if (current->second.version > version) return false;
            // An equal-version tombstone is not a live allocation. Do not
            // resurrect a quarantined/retired copy on a delayed exact retry.
            if (current->second.version == version &&
                !addresses_.contains(address))
                return false;
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
        auto stored = entry;
        stored.tenant_id = identity.first;
        stored.replicas = {replica};
        stored.version = version;
        objects_[identity][address] = std::move(stored);
        addresses_[address] = identity;
        return true;
    }

    std::vector<KeyReplicaEntry> Snapshot() const {
        std::vector<KeyReplicaEntry> result;
        result.reserve(addresses_.size());
        for (const auto& [address, identity] : addresses_) {
            result.emplace_back(objects_.at(identity).at(address));
        }
        return result;
    }

    // Remove only the rejected snapshot version, never a concurrent
    // replacement. Keep address/key version fences so delayed UPSERTs cannot
    // resurrect it.
    void ForgetSnapshotEntry(const KeyReplicaEntry& entry) {
        const Identity identity{
            entry.tenant_id.empty() ? "default" : entry.tenant_id, entry.key};
        auto object = objects_.find(identity);
        if (object == objects_.end()) return;
        for (const auto& replica : entry.replicas) {
            if (!replica.is_memory_replica()) continue;
            const auto address = replica.get_memory_descriptor()
                                     .buffer_descriptor.buffer_address_;
            auto current = object->second.find(address);
            if (current == object->second.end() ||
                current->second.version != entry.version ||
                current->second.replicas.front().id != replica.id)
                continue;
            object->second.erase(current);
            addresses_.erase(address);
        }
        if (object->second.empty()) objects_.erase(object);
    }

    // Ownership and visibility are separate: an old allocation may already
    // have been imported into the current master (e.g. by a standby snapshot).
    bool NeedsReplay(const KeyReplicaEntry& entry,
                     const Replica::Descriptor& replica, int64_t epoch) const {
        auto object = objects_.find(
            {entry.tenant_id.empty() ? "default" : entry.tenant_id, entry.key});
        if (object == objects_.end()) return false;
        const auto address =
            replica.get_memory_descriptor().buffer_descriptor.buffer_address_;
        auto found = object->second.find(address);
        const auto version =
            entry.version == RebuildVersion{}
                ? RebuildVersion{replica.get_memory_descriptor()
                                     .buffer_descriptor.allocationEpoch(),
                                 replica.id}
                : entry.version;
        return found != object->second.end() &&
               found->second.version == version &&
               found->second.replicas.front().id == replica.id &&
               found->second.observed_epoch.value_or(
                   found->second.version.first) < epoch;
    }

    void MarkReplayed(const KeyReplicaEntry& entry, int64_t epoch) {
        auto object = objects_.find(
            {entry.tenant_id.empty() ? "default" : entry.tenant_id, entry.key});
        if (object == objects_.end()) return;
        for (const auto& replica : entry.replicas) {
            const auto address = replica.get_memory_descriptor()
                                     .buffer_descriptor.buffer_address_;
            auto found = object->second.find(address);
            if (found != object->second.end() &&
                found->second.version == entry.version &&
                found->second.replicas.front().id == replica.id)
                found->second.observed_epoch =
                    std::max(epoch, found->second.observed_epoch.value_or(
                                        found->second.version.first));
        }
    }

    void ForgetRegion(uint64_t base, uint64_t size) {
        auto address = addresses_.lower_bound(base);
        while (address != addresses_.end() && address->first - base < size) {
            auto object = objects_.find(address->second);
            object->second.erase(address->first);
            if (object->second.empty()) objects_.erase(object);
            address = addresses_.erase(address);
        }
    }

   private:
    using Identity = std::pair<std::string, std::string>;
    struct Range {
        uint64_t end;
        RebuildVersion version;
    };
    std::map<Identity, std::map<uint64_t, KeyReplicaEntry>> objects_;
    std::map<uint64_t, Identity> addresses_;
    std::map<uint64_t, Range> ranges_;
    std::map<Identity, RebuildVersion> key_versions_;
};

}  // namespace mooncake
