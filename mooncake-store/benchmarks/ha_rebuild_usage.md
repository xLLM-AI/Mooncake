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

Successful writes wait for each remote owner to apply and acknowledge its
ownership record. Unacknowledged records remain queued for retry; a write
returns an error if acknowledgements do not arrive within 30 seconds. Owners
replay notifications arriving after their recovery snapshot into the current
master view before acknowledging them. Recovery completion also waits for the
client's pending acknowledgements.

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
