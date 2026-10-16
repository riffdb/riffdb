# WP-825: accepted native replay-source closure and generation rollover

Package: WP-825. Tier: guarantee. The maintainer accepted the exact
ADR-0316 (`adr/0316-native-authoritative-replay-checkpoints-and-projection-generation-rollover.md`),
SPEC amendments and package registration in session on 2026-10-16 ("i approve").
This decision registration contains no runtime implementation or fixture change.

The [native three-command regression](WP-825-RETENTION-PROJECTION-RECOVERY-REVIEW.md)
establishes a source-closure defect rather
than a slow harness: the database independently validates Clean and returns the
exact aggregate, then a permitted prune of command 1 leaves correct cache rows
but makes independent recovery fail with `HistoryPruned` /
`ProjectionStateIntegrity`. An empty replacement restored without that prefix
cannot reconstruct the aggregate either. Reclassifying equal-looking cache rows
as authority would hide the defect. The failed original source remains evidence;
this proposal does not make an already-pruned fixture recoverable.

The accepted repair independently evaluates complete authoritative input into a
bounded native commit-log compaction source for one exact projection identity.
The coordinator publishes it through ordinary attributed V3 transactions; normal
bootstrap and physical backup retain it. A disjoint local derived generation
seeds from that source and applies its complete suffix. Retention and source
collection preserve the base **and** contiguous tail of every live or captured
generation. Followers pause destructive replay before losing local closure and
roll over using still-retained input. No projection cache/control crosses the
wire, and derived apply continues outside the primary command mutation gate.

Decisions 2, 3, 4, 6, 12 and 21 explicitly supersede private v4 wording before any
acceptance. New acquisition/seeding requires the currently selected Published
source, while a valid recorded generation base or captured pin retains its exact
Superseded base plus tail, including after restart. Old generations cannot silently
switch sources; missing/corrupt closure invokes Decision 20. Full source identity is
`(DatabaseId, allocation-history-incarnation, nonzero CheckpointOrdinal)`: all source
keys and root/generation-base references bind the complete tuple, never a bare
ordinal. Numeric nonreuse applies within that allocation incarnation. Restored
published sources keep their checked origin identities through `RestoreAnchor`;
normalization installs the new incarnation's allocator at `Next(1)` before any new
allocation. Allocations cannot reuse retained source identities within the existing
checked transition/source domain. This makes no universal cross-fork numeric
uniqueness claim: ADR-0072's existing `max(target, staged) + 1` restore rule is
unchanged. The tuple is a locator, never authority alone. Every base/root/capture
also binds the immutable source digest/origin commitment and verified transition
evidence; matching numeric tuples cannot admit foreign or forked attachments.
Conflicting bytes under one retained source key refuse. No origin-ID rewrite,
cache-derived authority, new entropy source or external high-water service is introduced.
Every checked new-incarnation transition, including promotion, establishes its
own allocator at `Next(1)` before allocation, while same-incarnation maintenance
cannot reset it. Restored/inherited source origin IDs remain unchanged; existing
verified restore/promotion/bootstrap transition evidence establishes validity,
never a bare origin/current-incarnation equality test. Copied unfinished candidates
cannot resume after source-incarnation change and safely abort under Decision 10.
Row/object keys use full source identity without a redundant projection-identity
prefix; repeated record identity is checked against the object. The 32-byte source
tuple replaces at least 49 bytes of old identity/generation prefix, preserving the
existing complete 4-KiB group-key capacity without a hash surrogate.
The extra normative reconciliation amends SPEC 10.2 / `STO-012` and ADR-0019's closed
inventory for four authoritative source namespaces plus the permanent versioned
checkpoint-ordinal allocator binding allocation incarnation and `Next(nonzero) |
Exhausted`. Only the coordinator allocates ordinals atomically with candidate
allocation; checked exhaustion survives abort and collection within that incarnation.
SPEC 5.2 assigns evaluation to projection, semantic ports/records to storage API,
messages to proto, persistence/custody to redb and authoritative maintenance to
commit. Its narrow commit-to-projection proof-consumption dependency introduces
no reverse edge or arbitrary callback. SPEC 15.2, `PRJ-003`, `PRJ-004` and
`REC-003` distinguish source-seeded prefix representation from individually
applied suffix equality; `PRJ-001` and `PRJ-002` remain unchanged.

| Accepted capacity policy | Limit |
|---|---|
| Published identity roots | 64 |
| One source object | 65,536 rows and 64 MiB canonical key/value charge |
| Selected sources / all source-table ownership | 256 MiB / 512 MiB |
| Concurrent source candidates | One per database |
| Complete descriptor, root or maintenance record | 4 KiB each |
| Aggregate incremental source-worker allocations | 64 MiB; admission before allocation |
| Private staging storage | 64 MiB encoded; 128 MiB allocated disk |
| One copy/collection step | At most 128 row mutations and 4 MiB complete row bytes, reduced by every existing transaction/frame ceiling |
| Generation rollover / hard derived-apply ceiling | Start by 32,768 individually applied tail commits / refuse further derived apply at 131,072 |
| Uncollected retired generations | At most one per identity |

These are accepted pre-alpha capacity choices, not measured
keep-up claims. Physical redb overhead, pinned extent capacity and snapshot
holds require separate attribution; canonical bytes do not bound disk pages.
The existing exact-provider 1-GiB aggregate, locator and other admitted pools
remain independent. New authority is never evicted to relieve cache pressure.
Capacity refusal preserves existing source obligations, including all retention
holds. The ordinary maintenance owner keeps its nineteen-times gate-cost rest;
no harness offset, retry, offered rate, timeout or fault gate changes.

The accepted WP-825 registration adds the record, affected requirements and
implementation paths. It includes `crates/riffdb-projection/src/recovery.rs`
within projection scope so the independently demonstrated healthy captured-head
fence race can be classified without treating a later append as corruption.
That narrower correction still needs its own deterministic proof; it neither
repairs missing source history nor grants checkpoint implementation authority.

Checks run: the 8-vCPU, 32-GiB Linux SSD host validated the exact accepted record with the repository parser,
record form, generated ADR index, requirement coverage, obligations, named
artifacts, performance freeze, and formatting. The scope check found no offenders
and exactly the ten accepted added paths, with no removals. Its self-edit result
requires the accepted registration to precede implementation in a separate commit;
that separate commit is now recorded. A focused follow-up passed the acceptance
commit policy and the rendered handbook check after correcting this review's ADR
reference. Review documentation is committed separately from acceptance. The
original proposed-record checks remain historical evidence; they do not prove a
runtime implementation. No local repository checks, builds or tests ran. Full
source acceptance, CI and campaign qualification remain open.

Required implementation checks begin with the unchanged tiny prune-one failure,
independent source evaluation,
seed-plus-tail exact results, duplicate/prefix/retired refusal, changed-plan
preactivation refusal, corrupt-cache versus corrupt-source classification, and
budget exact/one-over/cancellation proofs. Then exercise follower/bootstrap,
native empty restore, promotion with inherited source IDs and fresh-incarnation
ordinal reuse without aliasing, bootstrap of those sources, repeated restores,
same-incarnation exhaustion/nonreset, conflicting retained-key bytes and foreign/forked
attachments with equal numeric tuples, parent digest/origin/closure mismatch refusal,
append/source-pin races and process kills at each
candidate/page/root/rollover/prune/collection/RestoreAnchor boundary. Only then
measure full-population restore and loaded checkpoint keep-up, physical storage,
writer interference and a final authorized Ready query, followed by final-source
acceptance/full CI and both campaigns. No new campaign is qualified by this text.

Compatibility: new versioned native records, allocator, registry entries, key/hash
domains and authority-catalog identity need migration or controlled refusal and
separate human fixture review. Existing V1 projection bytes and V3 frame bytes
are preserved. An unchanged plan may reuse its exact source; a changed plan
without complete exact input refuses before catalog activation. No future-only
aggregate semantics or caller-selected source/budget are introduced.

Hazards and follow-ups: immutable old/new source coexistence, retained generation
pins and slow collection can exhaust capacity and block safe pruning. A single
extreme valid commit may exceed worker admission. Loaded rollover may miss its
ceiling; neither failure permits widening limits or relaxing workload gates.
Follower destructive-frame preflight and unknown-outcome retirement must be
closed before source deletion. Repeated source maintenance must not create an
unbounded eligible V3 backlog. The unchanged 5-second administration, fixed
45-second release, 60-second recovery, pacing/interference and complete 8-hour /
24-hour qualification requirements remain. Once implemented, update the
handbook's sustainable lifecycle, backup/restore and affected operational pages
through `docs/SUMMARY.md`; this registration makes no shipping availability claim.
