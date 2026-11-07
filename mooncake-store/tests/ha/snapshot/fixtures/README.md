# v0.3.14 patch-baseline persistence fixtures

These bytes were generated on November 7, 2026 by compiling against the actual
`ff39d883480920d0fab0d754c02438c81e43a788` checkout and pinned YLT `45c6463c`
(GCC 13, 64-bit little-endian). They are **not** current-schema roundtrips.

The baseline producer serialized `MetadataPayload` and
`BatchOpLogSnapshotObjectChunk{7, {entry}}`. The object is `legacy-tenant/legacy-key`,
size 128, memory replica ID 99 at `0x10000`, endpoint `legacy-owner`, group
`legacy-group`, hard-pinned, checksum `0xabcdef`, session `legacy-session`,
residency `legacy-residency`, client `{42, 43}`. Its buffer has exactly the four
baseline reflected fields: size, address, protocol and transport endpoint.

SHA-256:
- `ff39-oplog-payload.bin`: `4ab732801642d31075ee5445c28bf76a8ff3b7c8717ba743bbfe1960db0811c9`
- `ff39-snapshot-object-chunk.bin`: `7a4a37700a58b96ba703f414929a7ead91ff83e67aa4954b42083aa6c5dd0756`

`MasterRebuildReplayTest.BaselineSnapshotAndOpLogRemainReadable` exercises the
current snapshot codec and OpLog applier on these fixed bytes. Do not regenerate
these with the current descriptor: doing so would remove the compatibility test.
