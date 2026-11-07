#pragma once

#include <array>
#include <tuple>
#include <glog/logging.h>
#include "client_service.h"

namespace mooncake {

// Shared only by test executables. Reuse the pre-existing baseline peer
// instead of adding production friends or making private Client state public.
class ClientTransportInitTestPeer {
   public:
    static std::shared_ptr<Client> Make() {
        auto client = std::shared_ptr<Client>(
            new Client("127.0.0.1:0", P2PHANDSHAKE, "rdma"));
        client->transfer_engine_ = std::make_shared<TransferEngine>(false);
        return client;
    }
    static ErrorCode Init(Client& client, const std::string& protocol,
                          const std::optional<std::string>& devices) {
        return client.InitTransferEngine("127.0.0.1:0", P2PHANDSHAKE, protocol,
                                         devices);
    }
    static TransferEngine& Engine(Client& client) {
        return *client.transfer_engine_;
    }
    static std::string Id(const Client& client) {
        return UuidToString(client.client_id_);
    }
    static auto Snapshot(Client& client) {
        std::lock_guard lock(client.local_replica_table_mutex_);
        return client.local_replica_table_.Snapshot();
    }
    static size_t PendingEntries(Client& client) {
        std::lock_guard lock(client.pending_notifies_mutex_);
        return client.pending_notify_entries_;
    }
    static size_t PendingBuckets(Client& client) {
        std::lock_guard lock(client.pending_notifies_mutex_);
        return client.pending_notifies_.size();
    }
    static auto SnapshotCount(Client& client) {
        return client.rebuild_snapshot_count_.load();
    }
    static void Pause(Client& client, bool paused) {
        client.rebuild_notifies_paused_.store(paused);
    }
    static void Timeout(Client& client, std::chrono::milliseconds timeout) {
        client.rebuild_notify_timeout_ms_.store(timeout.count());
    }
    static void BeforeUpsert(Client& client, std::function<void()> callback) {
        client.before_upsert_end_for_test_ = std::move(callback);
    }

    static auto Park(Client& client,
                     const std::vector<KeyReplicaEntry>& entries) {
        return client.ParkPendingNotify(
            entries.front()
                .replicas.front()
                .get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_,
            entries);
    }
    static auto Park(Client& client, const KeyReplicaEntry& entry) {
        return Park(client, std::vector<KeyReplicaEntry>{entry});
    }
    static auto FenceMetrics(Client& client) {
        auto& m = *client.metrics_;
        return std::array<uint64_t, 4>{
            static_cast<uint64_t>(m.rebuild_fence_keys.value()),
            static_cast<uint64_t>(m.rebuild_fence_bytes.value()),
            static_cast<uint64_t>(m.rebuild_fence_ranges.value()),
            static_cast<uint64_t>(m.rebuild_fence_exhausted.value())};
    }
    static std::string MetricsText(Client& client) {
        std::string text;
        client.metrics_->serialize(text);
        return text;
    }
    static bool FenceWarning(Client& client) {
        std::lock_guard lock(client.local_replica_table_mutex_);
        return client.rebuild_fence_warned_;
    }
    static auto CapacityDrops(Client& client) {
        return client.metrics_->rebuild_notify_capacity_drops.value();
    }
    static auto Apply(Client& client,
                      const std::vector<KeyReplicaEntry>& entries) {
        return client.ApplyOwnedRebuildEntries(entries);
    }
    static auto Apply(Client& client, const KeyReplicaEntry& entry) {
        return Apply(client, std::vector<KeyReplicaEntry>{entry});
    }
    static tl::expected<void, ErrorCode> Reconnect(Client& client,
                                                   const std::string& address) {
        auto connected = client.ConnectMasterEndpoint(address);
        if (connected != ErrorCode::OK) return tl::make_unexpected(connected);
        std::vector<Segment> segments;
        {
            std::lock_guard lock(client.mounted_segments_mutex_);
            for (const auto& [id, segment] : client.mounted_segments_)
                segments.push_back(segment);
        }
        return client.master_client_.ReMountSegment(segments);
    }
    static auto ReplayAttempts(Client& client) {
        std::lock_guard lock(client.local_replica_table_mutex_);
        return client.next_owned_replay_id_;
    }
    static void FenceBudget(Client& client, size_t limit) {
        std::lock_guard lock(client.local_replica_table_mutex_);
        client.local_replica_table_ = RebuildReplicaTable(limit);
        client.UpdateRebuildFenceMetrics();
    }
    template <typename Ticket>
    static bool Wait(Client& client, const Ticket& ticket) {
        return client.WaitForPendingNotifies({ticket});
    }
    template <typename Ticket>
    static void Expire(Client& client, const Ticket& ticket) {
        std::lock_guard<std::mutex> lock(client.pending_notifies_mutex_);
        ticket->created_at =
            std::chrono::steady_clock::now() - Client::kNotifyTtl;
    }
    template <typename Ticket>
    static size_t Attempts(Client& client, const Ticket& ticket) {
        std::lock_guard<std::mutex> lock(client.pending_notifies_mutex_);
        return ticket->attempts;
    }
    static void Flush(Client& client) { client.FlushPendingNotifies(); }
    static auto Resend(Client& client, ViewVersionId view) {
        return client.ResendLocalReplicaTable(view);
    }
    static auto WaitOwned(Client& client, ViewVersionId view) {
        return client.WaitForOwnedReplays(view);
    }
    static void Record(Client& client, const KeyReplicaEntry& entry) {
        std::lock_guard<std::mutex> lock(client.local_replica_table_mutex_);
        for (const auto& replica : entry.replicas)
            client.local_replica_table_.Record(entry, replica);
    }
    static void StopHeartbeat(Client& client) {
        client.storage_heartbeat_running_.store(false);
        if (client.storage_heartbeat_thread_.joinable())
            client.storage_heartbeat_thread_.join();
    }
    static auto Epoch(Client& client) {
        auto ping = client.master_client_.Ping().value();
        return ping.allocation_epoch.value_or(ping.view_version_id);
    }
    static auto View(Client& client) {
        return client.master_client_.Ping().value().view_version_id;
    }
    static void SetOwnedPending(Client& client, uint64_t id, bool failed) {
        std::lock_guard<std::mutex> lock(client.local_replica_table_mutex_);
        client.owned_replay_pending_[id] = failed;
        client.owned_replay_cv_.notify_all();
    }
    static void ClearOwnedPending(Client& client) {
        std::lock_guard<std::mutex> lock(client.local_replica_table_mutex_);
        client.owned_replay_pending_.clear();
        client.owned_replay_cv_.notify_all();
    }
    static auto RegisteredBuffers(Client& client) {
        std::vector<SegmentBufferInfo> buffers;
        CHECK_EQ(client.transfer_engine_->getSegmentBuffers(LOCAL_SEGMENT_ID,
                                                            buffers),
                 0);
        std::vector<std::tuple<uint64_t, uint64_t, std::string>> ranges;
        for (const auto& buffer : buffers)
            ranges.emplace_back(buffer.addr, buffer.length, buffer.location);
        return ranges;
    }
    static auto Move(Client& client, const std::string& key,
                     const std::string& source, const std::string& target) {
        return client.Move(key, source, target);
    }
    static auto Move(Client& client, const std::string& key,
                     const std::string& tenant_id, const std::string& source,
                     const std::string& target) {
        return client.Move(key, tenant_id, source, target);
    }
};
}  // namespace mooncake
