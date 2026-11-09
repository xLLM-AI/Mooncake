# Client metadata rebuild

Rebuild defaults off. Set `MC_ENABLE_HA_REBUILD=1` before creating clients,
including xLLM's `RealClient`, and enable the HA master gate separately:

```bash
export MC_ENABLE_HA_REBUILD=1
mooncake_master --enable_ha=true --enable_ha_rebuild_gate=true <other-options>
```

HA store writes/reads also use checksums, regardless of `MOONCAKE_STORE_CHECKSUM`,
to reject changed bytes after an unconfirmed write reuses a recorded address.
Replay verifies active local replicas and discards known checksum mismatches;
device/read failures remain retryable. This is not data persistence: overwritten
bytes cannot be recovered. Reads retain the checksum guard for in-flight writes.
Local hot cache keeps its existing commit/recovery invalidation; HA does not
disable it when the standalone checksum option is off.

Restart every owner when changing the client flag. Disabled owners can delay a
closed gate until its degraded timeout. `ServiceReady()`/discovery remain available;
`/health` reports data readiness, including `service_ready=true` when degraded.

## Restart ordering and lifetime

HA tokens use the leadership view and ReplicaID. Standalone masters allocate a
strictly increasing durable incarnation before admitting writes, even with the
gate/tracking off, preventing new pre-replay writes from sharing legacy `(0,id)`.
`MC_REBUILD_EPOCH_FILE` defaults to `$HOME/.mooncake-rebuild-epoch`; its parent
must exist and be writable. Retain this file and its `.lock` across replacements
of the same logical master. Do not delete, reset or roll back the counter while
ownership survives. File locking, atomic rename and file/directory fsync happen
once per startup, not per write. Corruption, exhaustion or persistence failure
reject startup; a first-start counter begins at 1, above legacy epoch 0.
This is a standalone filesystem requirement even when tracking is disabled.
HA/standalone token domains are not interchangeable: mode changes against live
ownership/persisted tokens are unsupported; use coordinated cold-store/refill.
Rollback to pre-rebuild binaries also requires coordinated client restart.

Owners retain committed locations without recreating the TransferEngine or
re-registering surviving buffers. All replicas of a write share one token;
object/address/segment fences reject stale ownership. Successful normal/graceful
unmount retires only that region's records, preserving fences and other segments.
Graceful unmount retains its registration until removal is confirmed. Delayed
notifications must match an active segment's address/extent and supplied UUID.

Descriptor epoch/extent/UUID use compatible fields. Legacy snapshots/OpLogs
have epoch 0/no UUID; reserve infers absent extent from the offset size class.
Earlier unmerged ordinary-field formats are not baseline-format compatible.
Replay retains size/type/group, tenant charges, pins, checksum/session/residency.
Equal-version merges validate immutable properties. Reserve/bind precede route
replacement; RAII frees failed staging and exact in-place locations are reused.
Overlapping resize that cannot reserve beside old handles retains the route/retries.

## Notifications and completion

New opt-in Put/Upsert writes wait up to 30 s for remote ownership ACKs. An owner
that explicitly rejects a notification replies with NACK: the affected write
gets the owner's error (e.g. `REBUILD_FENCE_EXHAUSTED`), and that request leaves
the retry queue immediately. Other pending requests in a batch still share its
ACK deadline. Failure
may follow data commit and does not mean transactional rollback. Batch writes
fetch committed metadata in one RPC, group by owner and share one deadline;
only affected keys fail. Upsert invalidates hot cache at commit even if ACK fails.
`OBJECT_ALREADY_EXISTS` Put/BatchPut preserve bytes and idempotent success;
ownership maintenance is best-effort and does not wait for ACKs.

The queue coalesces exact retries, caps messages/entries/payload at
1,024/65,536/64 MiB, takes at most 64 due messages per sweep, and backs off from
100 ms to 10 s. Entries expire after 12 attempts or five minutes, or are evicted
oldest-first **within the same endpoint** under pressure. Each endpoint is
limited to one quarter of each global budget (256 messages / 16,384 entries /
16 MiB); admission never evicts another endpoint. Global exhaustion can still
reject new work. Capacity rejection/eviction increments
`client_rebuild_notify_capacity_drops_total` in addition to the general failure
counter; retry/TTL expiry does not increment the capacity counter.
Dropping is not an ACK; dropped records may be unrecoverable. The queue is not
durable and survives view changes because entries describe ownership facts.

Completion waits for accepted locally-owned replay work, not historical outgoing
ACKs. A lost remote owner cannot block this sender's completion, but its memory
is lost unless another replica survives; the master's roster/timeout still apply.
No table mutex spans a replay RPC. Successful completion closes the active replay
window; already-confirmed writes issue no extra rebuild RPC. Late old-master
records still replay before ACK, and failed/stale recovery remains retryable.
On batch `INVALID_PARAMS`, isolate rows, log/count rejected keys, conditionally
quarantine only the rejected snapshot version and continue valid replay. Transport,
resource and stale-view errors retry. Completion does not certify rejected rows.

## Ownership-fence lifecycle and bounds

Successful ordinary/graceful unmount removes address fences in the retired
region. Key/version and unused-suffix fences on still-active segments are **not**
ordinary cache entries: dropping them on overwrite or when the object table is
empty can admit delayed notifications and resurrect an old key at another address.
All fences are cleared only after the last active segment retires; delayed
notifications must still match a current segment UUID.

Each owner caps key and range fences at 1,048,576 entries each, and retained
identity text at 128 MiB (map/node overhead is additional). At the limit, new
tracking returns the distinct `REBUILD_FENCE_EXHAUSTED` (-1008), not the
segment-allocation error `NO_AVAILABLE_HANDLE`. It logs a WARNING and increments
`client_rebuild_fence_exhausted_total` as well as the general rebuild-failure
counter, rather than evicting safety history or ACKing an untracked record.
Remote writers still observe an ACK timeout (the notification protocol does not
carry a NACK code); inspect the **owner's** exhaustion counter and gauges.
`client_rebuild_fence_keys`, `client_rebuild_fence_bytes` and
`client_rebuild_fence_ranges` expose O(1) usage snapshots after record/retirement.
At 80% of any budget the owner warns once per threshold crossing (rearmed after
usage falls below all thresholds). Identity bytes exclude map/node overhead.
Existing identities may still update if no other budget is exceeded. A fresh
write may therefore fail ownership confirmation after data commit; it is not
rolled back. Retire the owner's segments/recreate it through the supported
lifecycle to reset history; do not delete fences while segments remain active.

Read-only storage configuration is available during REBUILDING, but new segment
mounts must wait for SERVING/DEGRADED. A failed MountSegment does not destroy a
caller-owned Client: reuse or close that initialized client, rather than leaking
it on every retry. Client/RealClient destruction releases its backend/resources
through existing RAII/teardown. Observational capacity/usage RPCs may report an
incomplete rebuilding view; consumers must consult `master_rebuild_state`
(0 REBUILDING, 1 SERVING, 2 DEGRADED), not treat an early usage sample as final.

## Unsupported recovery

Client replay imports OFFSET memory replicas, not SSD/3FS/offloaded or cachelib
allocations. Without separate applicable snapshot/OpLog recovery, unsupported-
tier-only objects need refill/recomputation. Managed-weight state/manifests require
that authoritative recovery: group -> master-state -> key locks and import guards
prevent client replay from bypassing control operations. Missing authority or
closed/non-importing mutations are ineligible (`INVALID_PARAMS`); exact restored
duplicates are no-ops. An import currently closing uses its retryable activity guard.
Copy/Move return `UNAVAILABLE_IN_CURRENT_MODE` with client rebuild or the master
gate enabled; destination ownership is not implemented. Matching client/master
schemas are required; mixed versions are unsupported. Data needs a surviving
owner/memory. Lazy-deleted, unreused addresses may revive; no durable deletion,
owner-failure persistence or all-tier HA guarantee is provided.

## Validation

```bash
ctest --test-dir build --output-on-failure -R \
  '^(offset_allocator_test|rebuild_replica_table_test|master_rebuild_replay_test|client_metadata_rebuild_test|ha_rebuild_gate_test|rebuild_retry_test)$'
BUILD_DIR="$PWD/build" bash mooncake-store/tests/ha_live_test.sh
python3 mooncake-store/tests/ha_leader_failover_live_test.py \
  --build-dir "$PWD/build" --etcd-bin /path/to/etcd \
  --run-dir /tmp/mooncake-ha-failover-new
```

CTest enables the client flag for `client_metadata_rebuild_test`; set it when
running the binary directly. Live drivers enable it and stop only their own
children. The two-master driver checks closed gate, 1/1 owner completion, every
value, unchanged registrations and fresh post-recovery writes. Use a new run dir.
For real HBM, rebuild with the intended CANN/`llm_datadist` SDK and a backend
supporting HBM inputs/outputs. Pass `--protocol=ascend --npu-device=0`, or
`NPU_DEVICE=0 PROTOCOL=ascend` for the shell. Record `USE_ASCEND_DIRECT`, SDK/runtime
paths and executable hashes: host/container SDKs may differ. Old binaries or
inference smokes are not current-branch HA acceptance; this driver is not a mock.
