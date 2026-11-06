# Client metadata rebuild

Metadata rebuild is opt-in. The master flag `--enable_ha_rebuild_gate` defaults
to `false`. Clients leave ownership tracking, notifications, and metadata replay
disabled unless `MC_ENABLE_HA_REBUILD=1` is set before creating the client. The
same environment setting applies to xLLM's `MooncakeStoreBackend` through
`RealClient`.

Enable both sides for HA deployments:

```bash
export MC_ENABLE_HA_REBUILD=1
mooncake_master --enable_ha=true --enable_ha_rebuild_gate=true <other-master-options>
```

Set the environment variable on every client, including clients that only own
segments. Restart clients after changing it. Enabling only the master gate can
leave recovery waiting for disabled clients until the degraded-service timeout.
Non-HA restart tests enable the client setting while leaving the master gate off.

`/health` reports `service_ready=false` while the data RPC rebuild gate is
closed, and `true` once it opens, including degraded service after timeout.
Leader discovery and rebuild RPCs remain available during this interval;
the leader role or label identifies the elected leader rather than data readiness.

Recovery currently imports memory replicas managed by the offset allocator.
Descriptors carry the original allocation extent and a master-issued ordering
token derived from the HA view and replica IDs. All replicas reported for one
write share its token. Owners reject older notifications for both object
identity and overlapping address ranges. The master also selects the newest
reported write when owners report different versions of an object.

This client-driven replay does not import SSD/3FS/offloaded replicas or cachelib
allocations. Without a separately configured and applicable snapshot/OpLog
recovery path, objects held only in these unsupported tiers are not restored by
this feature after failover; callers must refill them from their source or
recompute them. HA rebuild is not an all-tier persistence guarantee.

If a snapshot batch is rejected with `INVALID_PARAMS`, the client retries its
rows individually, logs/counts invalid rows, and conditionally quarantines only
the rejected snapshot version before continuing valid replay. Completion means
all eligible, non-rejected locally-owned records were replayed, not that rejected
objects were restored. Transport and stale-view errors are not quarantined and
remain retryable. No table mutex is held across a replay RPC.

Successful writes wait for each remote owner to apply and acknowledge its
ownership record. Unacknowledged records remain queued for retry; a write
returns an error if acknowledgements do not arrive within 30 seconds. Owners
replay notifications arriving after their recovery snapshot into the current
master view before acknowledging them. Recovery completion also waits for the
current view's accepted locally-owned replay work, not the sender's historical
remote-owner ACK queue. A permanently lost remote owner therefore cannot block
this client's completion signal; its data is lost unless another replica survives.
This does not bypass the master's roster of expected surviving owners or its
explicit degraded-serving timeout.

The remote notification queue survives view changes because records describe
allocation/write versions, not a master RPC for an obsolete view. It coalesces
exact retransmissions, limits queued messages to 1,024, entries to 65,536 and
serialized payload to 64 MiB, sends at most 64 due messages per sweep, and retries
with exponential backoff (100 ms up to 10 s). Records are dropped after 12 attempts
or five minutes, or oldest-first on capacity pressure, with a WARNING and
`rebuild_failed_batches` increment. Dropping is never treated as an ACK: a waiting
new write still fails. Dropped ownership records may not be recoverable after
failover; the queue is not durable.

An `OBJECT_ALREADY_EXISTS` Put/BatchPut remains idempotently successful and
preserves the committed bytes. Ownership maintenance is best-effort for this
case; it does not wait for remote ACKs or override success. New Put/Upsert writes
retain the acknowledgement/error contract above.

Batch writes fetch committed records in one RPC, group ownership notifications
by destination owner, and share one 30-second acknowledgement deadline for the
batch. Errors are returned for the affected keys. Upsert invalidates the local
hot cache immediately after commit, even if ownership acknowledgement fails.

Ownership records obtain the committed size, group, data type and pin settings
from the master, including the attributes preserved by Upsert. Rebuild restores
group routing and membership, pin protection and tenant memory charges.
Copy and Move return `UNAVAILABLE_IN_CURRENT_MODE` while client rebuild or the
master rebuild gate is enabled. Their destination ownership protocol is not
implemented yet; these operations remain available when rebuild is disabled.

The descriptor and notification schemas changed. Deploy matching client and
master binaries before enabling this feature; mixed-version operation is not
supported. Data survives only while the segment owner and its memory survive.
The existing lazy-delete behavior remains: a removed object whose address has
not been reused may reappear after rebuilding metadata. This mechanism does not
provide durable deletion or persist data across owner failure.

Run the focused regressions with:

```bash
ctest --test-dir build --output-on-failure -R \
  '^(offset_allocator_test|rebuild_replica_table_test|master_rebuild_replay_test|client_metadata_rebuild_test|ha_rebuild_gate_test|rebuild_retry_test)$'
```

CTest enables `MC_ENABLE_HA_REBUILD=1` for `client_metadata_rebuild_test`; set it
explicitly when running that binary directly. The live-test and scale-test
drivers also enable it explicitly.


The v0.3.14 real-leader regression starts two masters and an isolated etcd,
observes the promoted leader's closed gate, requires exactly one owner to
complete metadata replay, verifies every value, checks unchanged transfer
registrations, and exercises a new write/read after recovery:

```bash
python3 mooncake-store/tests/ha_leader_failover_live_test.py \
  --build-dir "$PWD/build" --etcd-bin /path/to/etcd \
  --run-dir /tmp/mooncake-ha-failover-new
```

Use `--protocol=ascend --npu-device=0` only with a native Ascend build and a
backend supporting HBM sources **and** HBM read destinations. This option
allocates real device memory and byte-verifies the device roundtrip; it is not
a mock test or a substitute for an unavailable hardware backend. The run
directory must be new; all subprocesses and etcd data are isolated to the run.
`ServiceReady()` is the v0.3.14 connection/version handshake and remains
available while rebuilding. `/health` and business RPCs carry data readiness.


For the Ascend Direct NPU regression, rebuild this branch against the intended
CANN SDK (including its `llm_datadist` runtime), then run the same driver with
`--protocol=ascend --npu-device=0`. Merely using the same physical server does
not imply the same backend: a host SDK and a container SDK can differ. Record
`USE_ASCEND_DIRECT`, the SDK/runtime paths, and executable hashes with the test
results; do not substitute an older inference smoke or another backend's
results for HA acceptance.
