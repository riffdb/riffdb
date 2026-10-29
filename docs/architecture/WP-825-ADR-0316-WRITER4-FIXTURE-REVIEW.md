# WP-825: ADR-0324 writer-4 predecessor fixture review

Status: fixture addition approved by the maintainer in session on 2026-10-16:
"Approve the fixture addition". The reviewed bytes are unchanged. Successor
codec vectors and their inventory changes require their own concrete review.

The proposed addition is
`fixtures/storage/writer-4-redb-4.4-native-commands-and-backup.sparsehex`.
It freezes genuine current writer-4 bytes before adding the ADR-0324 record
registry and writer-5 upgrade edge. The source uses redb 4.4.0 and the Standard
commit profile. No predecessor fixture is relabeled or regenerated.

The sparse candidate is 602,022 bytes. Its SHA-256 is
`0ee27d1c32c3926bcd2f5991901fa26b0b722bd07d9215455f20c24f0f9c9d38`.
It contains exactly these eight synthetic relative artifacts:

| Artifact | Decoded bytes | SHA-256 |
| --- | ---: | --- |
| `backup/database.redb` | 151,552 | `d3d809700c84da0b64ad3d2b9cdd8ed61ee0bb8b05ede917fc1e0c03b51ee183` |
| `backup/format.riffdb` | 114 | `68e2b15818d8352e67947af6fade2e6f32c3acbbca211a4ce9b0019c1dd48f1b` |
| `backup/journal.riffextent` | 41,951,232 | `ac68c4705abd3ed572d56c5de5e32a3daaece8d11d87bb292f885cbb79bd9794` |
| `backup/manifest.riffdb` | 423 | `5e69747385612d57c27a47b895e4f8570f79f5c0a01c38fa3e6845baae8b0714` |
| `db.redb` | 151,552 | `d3d809700c84da0b64ad3d2b9cdd8ed61ee0bb8b05ede917fc1e0c03b51ee183` |
| `db.redb.riffdb-format-v1` | 114 | `68e2b15818d8352e67947af6fade2e6f32c3acbbca211a4ce9b0019c1dd48f1b` |
| `db.redb.riffjournal` | 41,951,232 | `ac68c4705abd3ed572d56c5de5e32a3daaece8d11d87bb292f885cbb79bd9794` |
| `expected-outcomes.hex` | 1,682 | `31bee451dba2a307e5304286821348c7a88d17d8de3d58b37a42ab784d8470ef` |

The fixture contains two disjoint complete command record sets bound to the
compiled indexed recovery contract and committed through the existing coordinator
candidate chain. Generation checks exact entity, terminal outcome, admission,
commit, provenance, durable event, event-route, index, pending-outbox and command
audit records. The source closes cleanly and reopens with the same assertions.

Ordinary offline backup creates the artifact. Its manifest names exactly three
checksums: database, journal and format marker. Each matches the actual artifact
hash, and the backup directory contains exactly those three artifacts plus its
manifest. Ordinary restore into a new empty target passes the same exact command
graph assertions.

The exported sparse bytes are reconstructed independently. The unchanged writer-4
reader opens the reconstructed source and ordinarily restores the reconstructed
backup into another new empty target, repeating every graph assertion. The
original owned fixture and reconstructed backup remain byte-identical. A separate
decoded-span hash reconstruction confirms all eight lengths and hashes.

C3D check receipt SHA-256:
`9c13c07bbf631c5f46c56c7fd7c879d6688d5fbd94717303f15ddd57930b7161`.
Generation and reconstructed-restore checks both passed. The frozen source was
restored and its complete source manifest verified after the check.

Privacy checks cover rendered headers and decoded artifact bytes, including
cross-span boundaries. The export contains synthetic identities and relative
artifact names; no compiled binaries, machine addresses, private paths,
credentials or captured logs are included.

All existing V1/V2 catalog records, journal V2/V3/V4/V5 vectors, prior engine
fixtures and writer-3 fixture bytes remain unchanged. This addition alone does
not authorize a writer-5 upgrade or certify ADR-0324 runtime source closure. The
successor codecs, their generated vectors, exact upgrade tests and human fixture
review remain required.

Package: WP-825. Tier: guarantee. No application or operational behavior changes
in this fixture-only proposal; the public handbook therefore needs no behavioral
update.
