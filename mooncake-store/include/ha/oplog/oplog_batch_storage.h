#pragma once

#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_types.h"
#include "types.h"

namespace mooncake {

class OpLogBatchStorage {
   public:
    OpLogBatchStorage(std::string cluster_id, HaKvBackend& backend);

    // Conservative complete-transaction cost, including encoded payload,
    // max-width sequencing metadata, prefix/fence and protobuf envelopes.
    size_t EntryBytesUpperBound(const OpLogEntry& entry) const;
    size_t TransactionEnvelopeBytes() const;
    bool CanWriteEntries(const std::vector<OpLogEntry>& entries) const;
    size_t transactionByteLimit() const { return transaction_byte_limit_; }
    ErrorCode InitDurablePrefix(DurablePrefix& prefix);
    ErrorCode ClaimProducerView(ViewVersionId producer_view_version);
    ErrorCode ValidateProducerView(ViewVersionId producer_view_version) const;
    ErrorCode ReadProducerView(ViewVersionId& producer_view_version) const;
    ErrorCode ReadCompactionFloor(uint64_t& floor) const;
    ErrorCode ReadDurablePrefix(DurablePrefix& prefix);
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix);
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix,
                                         ViewVersionId producer_view_version);
    ErrorCode ReadBatch(uint64_t batch_id, OpLogBatchRecord& batch);
    ErrorCode ReadBatchesAfter(uint64_t after_batch_id, size_t limit,
                               std::vector<OpLogBatchRecord>& batches);
    // Idempotently delete records with id <= batch_id in this cluster only.
    // Caller must first publish a reader-visible compaction floor covering
    // this cutoff. Does not choose/validate the cutoff or retry backend errors.
    ErrorCode DeleteBatchesThrough(uint64_t batch_id);

   private:
    bool IsValidClusterId() const;
    ErrorCode WriteBatchAndAdvancePrefixImpl(
        const OpLogBatchRecord& batch, const DurablePrefix& expected_prefix,
        const ViewVersionId* producer_view_version);
    ErrorCode RejectLegacyLayout() const;
    ErrorCode ValidateDurablePrefixAtStartup(const DurablePrefix& prefix);

    std::string cluster_id_;
    HaKvBackend& backend_;
    bool cluster_id_valid_{false};
    size_t transaction_byte_limit_;
};

}  // namespace mooncake
