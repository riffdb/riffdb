# WP-825: replay-source values, streaming roots and native accounting

Package: WP-825. Tier: guarantee. Decisions: ADR-0316 and ADR-0317.

This foundation adds checked replay-source identities, descriptors, immutable
references and lifecycle values; a bounded streaming semantic row-root owner;
and one source-work accountant per actual native engine. It does not activate
catalog V3, publish a source checkpoint or run a source maintenance loop.
Those runtime and durable-codec integration steps remain open.

The semantic row root binds projection identity, frontier, ordered canonical
groups, measures, last-changed sequence and final count/charge. Storage ordinal,
generation and page boundaries cannot change it. A fixed previous-key buffer
and caller-admitted canonical scratch avoid a population-sized in-memory map.
Malformed ordering, foreign identity, insufficient scratch and incomplete
input refuse; a failed owner cannot resume as valid. Hash equality is evidence
about bytes, never independent evaluation or publication authority.

The independent source ledger admits at most 64 MiB, including its fixed
accountant and retained control. Snapshot captures share the actual engine's
ledger. Settled data and shared lineage retain their charges until their final
physical references disappear, including after engine destruction. They do
not retain an engine backreference or borrow provider/native capacity.

When an exact provider retains the source accountant, its conservative overlap
charge lives with that accountant's final Arc, rather than ending at engine
retirement. Whole-plan preparation rolls back on failure; self-binding or
binding to a source ledger refuses. Existing provider limits remain unchanged.
The complete accountant/control bound fits the existing 512-byte allowance.

A move-only claim serializes source workers sharing that ledger. Construction
failure, cancellation and panic release the claim; poisoned accounting stays
fail-closed. This primitive grants no database role, writer or publication
authority. The future runtime must check native role and retirement eligibility,
retain the primary engine's ledger across materialized/read-only captures, and
join the existing maintenance cancellation/drain/pacing owner. It must not
create a new allowance per cursor or start a worker on a read-only copy.
Retention Hold behavior and background scheduling are unchanged in this change.

C3D passed 57 selected semantic and regression tests: six new control/worker
proofs, five real-engine/capture proofs, five source-budget controls, nineteen
provider ownership controls, seventeen physical/concurrency controls and five
native provider controls. Both all-target/all-feature and default-feature
Clippy passed, along with scope, file-size and panic-allowance checks.
Receipt SHA-256:
`4cdbbd5e83cc8242f0fe4ed98117438d889e89bc0940ed060a501a92e90b2dc8`.

Earlier C3D proofs cover the unchanged checked values and streaming row-root
semantics. Their receipts are
`8f9548542b5b6a0806a8d84d01ec9978bb28dc51c97efbab4d2d4db76a432693`
and `0651417da5271060d61d6ab97eda8e8ca8c8dad8cdd4acf185a17e44abb82679`.
The imported runtime files are the complete formatted bytes captured by those
checks and the final accounting qualification. No tests ran on local disk.

Compatibility: no existing durable format, application interface or configured
limit changes. Source codecs and their separately reviewed successor fixtures
are not included here. This is internal ownership and semantic plumbing; it
adds no supported operator command or available checkpoint behavior, so no
handbook behavior changes are appropriate yet.

Hazards and follow-ups: implement independent evaluation and staging, attributed
coordinator publication, exact closure pins, generation rollover, retention,
follower and restore integration. Run integrated acceptance/full CI and process
crash proofs before qualification. The original full-population preparation
CorruptData failure remains unlocalized. No new eight-hour or 24-hour run is
qualified by this foundation.
