# Client metadata rebuild

Rebuild defaults off. Set `MC_ENABLE_HA_REBUILD=1` before creating clients,
including xLLM's `RealClient`, and enable the HA master gate separately:

```bash
export MC_ENABLE_HA_REBUILD=1
mooncake_master --enable_ha=true --enable_ha_rebuild_gate=true <other-options>
```

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

New opt-in Put/Upsert writes wait up to 30 s for remote ownership ACKs. Failure
may follow data commit and does not mean transactional rollback. Batch writes
fetch committed metadata in one RPC, group by owner and share one deadline;
only affected keys fail. Upsert invalidates hot cache at commit even if ACK fails.
`OBJECT_ALREADY_EXISTS` Put/BatchPut preserve bytes and idempotent success;
ownership maintenance is best-effort and does not wait for ACKs.

The queue coalesces exact retries, caps messages/entries/payload at
1,024/65,536/64 MiB, takes at most 64 due messages per sweep, and backs off from
100 ms to 10 s. Entries expire after 12 attempts or five minutes, or are evicted
oldest-first under pressure, with WARNING and `rebuild_failed_batches` increment.
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
