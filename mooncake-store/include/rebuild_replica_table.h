#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <utility>
#include <ylt/util/tl/expected.hpp>

#include "rebuild_types.h"

namespace mooncake {

class RebuildReplicaTable {
   public:
    explicit RebuildReplicaTable(size_t fence_limit = 1 << 20,
                                 size_t key_byte_limit = 128 * 1024 * 1024)
        : fence_limit_(fence_limit), key_byte_limit_(key_byte_limit) {}

    struct FenceUsage {
        size_t keys, bytes, ranges, key_limit, byte_limit, range_limit;
        bool near_limit() const {
            const auto near = [](size_t used, size_t limit) {
                return limit && used >= limit - limit / 5;
            };
            return near(keys, key_limit) || near(bytes, byte_limit) ||
                   near(ranges, range_limit);
        }
    };
    FenceUsage GetFenceUsage() const {
        return {key_versions_.size(), key_version_bytes_, ranges_.size(),
                fence_limit_,         key_byte_limit_,    fence_limit_};
    }

    // Stale records are harmless no-ops; exhausted tracking must NOT be ACKed.
    tl::expected<bool, ErrorCode> Record(const KeyReplicaEntry& entry,
                                         const Replica::Descriptor& replica) {
        if (!replica.is_memory_replica()) return false;
        const auto& buffer = RebuildBuffer(replica);
        const uint64_t address = buffer.buffer_address_;
        const uint64_t extent = buffer.allocationExtent();
        if (extent < buffer.size_ || extent == 0 ||
            extent > std::numeric_limits<uint64_t>::max() - address)
            return false;
        const uint64_t end = address + extent;
        const auto identity = IdentityFor(entry);
        const auto version = GetRebuildVersion(entry);
        auto key_version = key_versions_.find(identity);
        if (key_version != key_versions_.end() && key_version->second > version)
            return false;
        if (const auto* found =
                FindReplica(addresses_, identity, replica, version, false);
            found && found->observed_epoch.value_or(version.first) >
                         entry.observed_epoch.value_or(version.first))
            return false;
        auto previous = key_addresses_.find(identity);
        auto first = ranges_.upper_bound(address);
        if (first != ranges_.begin() && std::prev(first)->second.end > address)
            --first;
        size_t range_count = ranges_.size() + 1;
        for (auto current = first;
             current != ranges_.end() && current->first < end; ++current) {
            if (current->second.version > version) return false;
            // An equal-version tombstone is not a live allocation. Do not
            // resurrect a quarantined/retired copy on a delayed exact retry.
            if (current->second.version == version &&
                !addresses_.contains(address))
                return false;
            --range_count;
            range_count += current->first < address;
            range_count += current->second.end > end;
        }
        const auto key_bytes = identity.first.size() + identity.second.size();
        if (range_count > fence_limit_ ||
            (key_version == key_versions_.end() &&
             (key_versions_.size() >= fence_limit_ ||
              key_bytes > key_byte_limit_ - key_version_bytes_)))
            return tl::make_unexpected(ErrorCode::REBUILD_FENCE_EXHAUSTED);
        if (key_version != key_versions_.end() &&
            key_version->second < version && previous != key_addresses_.end()) {
            for (const auto old_address : previous->second)
                addresses_.erase(old_address);
            key_addresses_.erase(previous);
        }
        auto occupant = addresses_.lower_bound(
            first == ranges_.end() ? address : std::min(address, first->first));
        while (occupant != addresses_.end() && occupant->first < end)
            occupant = EraseOwnership(occupant);
        TrimRanges(first, address, end);
        ranges_[address] = Range{end, version};
        if (key_version == key_versions_.end()) key_version_bytes_ += key_bytes;
        key_versions_[identity] = version;
        auto stored = entry;
        stored.tenant_id = identity.first;
        stored.replicas = {replica};
        stored.version = version;
        key_addresses_[identity].insert(address);
        addresses_[address] = std::move(stored);
        return true;
    }

    std::vector<KeyReplicaEntry> Snapshot() const {
        std::vector<KeyReplicaEntry> result;
        result.reserve(addresses_.size());
        for (const auto& [address, entry] : addresses_)
            result.emplace_back(entry);
        return result;
    }

    // Remove only the rejected snapshot version, never a concurrent
    // replacement. Keep address/key version fences so delayed UPSERTs cannot
    // resurrect it.
    void ForgetSnapshotEntry(const KeyReplicaEntry& entry) {
        const auto identity = IdentityFor(entry);
        for (const auto& replica : entry.replicas) {
            if (!replica.is_memory_replica()) continue;
            if (FindReplica(addresses_, identity, replica, entry.version))
                EraseOwnership(
                    addresses_.find(RebuildBuffer(replica).buffer_address_));
        }
    }

    // Ownership and visibility are separate: an old allocation may already
    // have been imported into the current master (e.g. by a standby snapshot).
    bool NeedsReplay(const KeyReplicaEntry& entry,
                     const Replica::Descriptor& replica, int64_t epoch) const {
        const auto* found = FindReplica(addresses_, IdentityFor(entry), replica,
                                        GetRebuildVersion(entry));
        return found &&
               found->observed_epoch.value_or(found->version.first) < epoch;
    }

    void MarkReplayed(const KeyReplicaEntry& entry, int64_t epoch) {
        const auto identity = IdentityFor(entry);
        const auto version = GetRebuildVersion(entry);
        for (const auto& replica : entry.replicas) {
            if (auto* found =
                    FindReplica(addresses_, identity, replica, version))
                found->observed_epoch = std::max(
                    epoch,
                    found->observed_epoch.value_or(found->version.first));
        }
    }

    // Only a retired segment's address fences can be removed independently.
    // Key fences can cover delayed records on OTHER live segments, including
    // addresses never observed here. Clear those only after the last segment
    // retires, not merely when key_addresses_ becomes empty.
    void ForgetRegion(uint64_t base, uint64_t size,
                      bool all_segments_retired = false) {
        if (all_segments_retired) {
            key_addresses_.clear();
            addresses_.clear();
            ranges_.clear();
            key_versions_.clear();
            key_version_bytes_ = 0;
            return;
        }
        if (size == 0 || size > std::numeric_limits<uint64_t>::max() - base)
            return;
        const auto end = base + size;
        auto address = addresses_.lower_bound(base);
        while (address != addresses_.end() && address->first < end)
            address = EraseOwnership(address);
        auto range = ranges_.upper_bound(base);
        if (range != ranges_.begin() && std::prev(range)->second.end > base)
            --range;
        TrimRanges(range, base, end);
    }

   private:
    using Identity = std::pair<std::string, std::string>;
    struct Range {
        uint64_t end;
        RebuildVersion version;
    };
    using AddressIndex = std::map<uint64_t, KeyReplicaEntry>;
    using RangeIndex = std::map<uint64_t, Range>;
    static Identity IdentityFor(const KeyReplicaEntry& entry) {
        return {entry.tenant_id.empty() ? "default" : entry.tenant_id,
                entry.key};
    }
    // Coalescing compares write tokens; confirmation/quarantine also match IDs.
    static auto FindReplica(auto& entries, const Identity& identity,
                            const Replica::Descriptor& replica,
                            RebuildVersion version, bool match_id = true)
        -> decltype(&entries.begin()->second) {
        auto found = entries.find(RebuildBuffer(replica).buffer_address_);
        return found != entries.end() &&
                       found->second.tenant_id == identity.first &&
                       found->second.key == identity.second &&
                       found->second.version == version &&
                       (!match_id ||
                        found->second.replicas.front().id == replica.id)
                   ? &found->second
                   : nullptr;
    }
    AddressIndex::iterator EraseOwnership(AddressIndex::iterator address) {
        auto object = key_addresses_.find(IdentityFor(address->second));
        if (object != key_addresses_.end()) {
            object->second.erase(address->first);
            if (object->second.empty()) key_addresses_.erase(object);
        }
        return addresses_.erase(address);
    }
    void TrimRanges(RangeIndex::iterator range, uint64_t base, uint64_t end) {
        while (range != ranges_.end() && range->first < end) {
            const auto start = range->first;
            const auto saved = range->second;
            range = ranges_.erase(range);
            if (start < base)
                ranges_.emplace(start, Range{base, saved.version});
            if (saved.end > end)
                ranges_.emplace(end, Range{saved.end, saved.version});
        }
    }
    // Live entries live only in addresses_; the key index stores addresses, not
    // copies.
    std::map<Identity, std::set<uint64_t>> key_addresses_;
    AddressIndex addresses_;
    RangeIndex ranges_;
    std::map<Identity, RebuildVersion> key_versions_;
    size_t fence_limit_;
    size_t key_byte_limit_;
    size_t key_version_bytes_{0};
};

}  // namespace mooncake
