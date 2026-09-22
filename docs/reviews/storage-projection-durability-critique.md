# Design review: storage engine, projection plane, and durability

Scope: `SPEC.md` §10, §14, §15, §18.5, §4.10 (ADR-0178 amendments), §20.6, §21;
ADR-0004, ADR-0010, ADR-0017, ADR-0035, ADR-0038/0039, ADR-0050, ADR-0072,
ADR-0083, ADR-0085, ADR-0093, ADR-0100, ADR-0101, ADR-0104, ADR-0156, ADR-0160,
ADR-0178, ADR-0182, ADR-0183; and the implementation under `crates/`.

Severity scale: **Critical** (breaks a stated guarantee), **High** (capability
claim not achievable as specified), **Medium** (scope/authority mismatch or
operational hazard), **Low** (documentation drift).

---

## F-01 — redb is not the durability authority; the design already had to build a
## first-party WAL beside it. Severity: **High**

**Evidence.** ADR-0101 §1: "For the standard application durability profile,
authoritative state is: 1. the newest known-durable redb checkpoint; and 2. the
exact, gap-free, checksummed sequence of durable writer frames after that
checkpoint. The combination is one logical database. Neither component alone may
be presented as complete." The frame format is first-party
(`crates/riffdb-storage-redb/src/journal.rs`: `RDBJRN01`/`RDBFRM01`/`RDBJEX03`
magics, `MAX_JOURNAL_TRANSITIONS = 256`, `MAX_JOURNAL_SUFFIX_TRANSITIONS = 8_192`,
`MAX_JOURNAL_SUFFIX_BYTES = 32 MiB`). SPEC §10.1 `STO-023` requires the clean-close
certificate to be written "only after … every published journal frame reaches the
matching redb checkpoint".

**Consequence.** The framing "redb (single-writer, pure-Rust B-tree embedded KV)
as the baseline" understates what was built. redb is the *materialized* state at a
checkpoint; the acknowledged durability boundary is a bespoke append-only log with
its own checksums, extent files, and reclamation rules. That is a reasonable
engineering outcome, but it means:

- The engine-neutral semantic API (ADR-0004) is doing real work, but the
  *durability* portability claim is weaker than the storage-portability claim:
  swapping redb for Fjall would not remove the journal, and the journal's
  correctness is now first-party code that no engine comparison exercises.
- `R-04`'s mitigation ("benchmark Fjall at POC exit", SPEC line 7635) tests the
  wrong layer. The Fjall comparison cannot validate the journal, because the
  journal is not part of the storage API surface.

---

## F-02 — R-04's Fjall exit comparison is structurally incapable of producing
## decision evidence. Severity: **Medium**

**Evidence.** `benchmarks/storage-fjall/README.md`: "The comparison **explicitly
fails** the unchanged semantic conformance suite… Benchmark timings are therefore
marked `not_publishable_conformance_failure` and must not be used as
redb-versus-Fjall decision evidence." `benchmarks/reports/status-v1.json` records
`"performance_claim": "qualified_poc_semantic_and_budget_comparison_with_fjall_conformance_failure"`.
`docs/known-limitations.md` lines 35–37: "The production server uses redb only. The
isolated Fjall adapter fails the [semantic suite]… are ineligible and the POC makes
no redb-versus-Fjall speed claim." `docs/poc-evidence-audit.md` records that the
Fjall conformance test also freezes a durable registry that is already 17 records
behind `crates/riffdb-proto/src/durable.rs`.

**Consequence.** The risk register's stated mitigation for R-04 is a comparison
that is by construction not decision evidence. The real mitigation that landed is
the semantic trait boundary plus the memory reference implementation, which is
sound — but R-04 should be closed on *that* basis, not on a benchmark that is
explicitly marked unpublishable. Leaving the register text as-is invites a future
reader to believe an engine swap was evaluated when it was not.

---

## F-03 — One poison projection is a permanent, operator-mediated outage for that
## projection *and* an unbounded disk-growth hazard for the whole database.
## Severity: **High**

**Evidence.** Per-identity frontiers are correct (SPEC §15.2 `PRJ-001`/`PRJ-002`;
ADR-0010). But the retention fence takes the **minimum over all non-detached
projection controls**: `crates/riffdb-storage-redb/src/retention.rs`
`min_projection_durable_frontier` iterates every row of `PROJECTION_FRONTIER` and
keeps the smallest published frontier; ADR-0085 A2 lines 32 and 135–142 ("The
watermark MUST NOT advance past the minimum projection frontier… every projection
control's durable frontier (detached projections excluded)"); SPEC `STO-030`
(SPEC lines 1752–1762) adds follower frontiers to the same set.

A deterministic evaluation failure records one closed generation failure and does
not advance (SPEC §15.2 lines 6230–6235: "A failed published generation is
suspended until replacement publication. `Invalid` has no v1 recovery transition").
SPEC §15.3 then returns `Degraded`/`Invalid` with no rows for that identity, and
`WaitTimedOut` for `after_sequence` waits.

**Consequence.** A single projection whose evaluation fails deterministically
(e.g. a malformed event that the catalog materialization view rejects, or a
schema mismatch introduced by a compatible-looking deployment) produces:

1. That projection never serves rows again without an explicit replacement
   publication or the audited `detach_projection` verb
   (`retention.rs` `administer_projection_hold`).
2. The retention watermark is pinned at that projection's last frontier, so
   `commits`, `events`, `outbox`, and `outbox_status` grow without bound
   (`retention.rs` prunes exactly those tables, lines 450–452). ADR-0182 §6
   makes retention online but does not remove the fence.
3. Every other projection and every follower is unaffected — the blast radius is
   correctly scoped — but the *database* is not, because the fence is global.

The design has no automatic escape: `Degraded` recovery allocates a new generation
(`crates/riffdb-projection/src/recovery.rs`
`validate_and_recover_projection_generation`), but that path is only reached from
*derived-state mismatch* findings, not from a deterministic evaluation failure,
which lands in `record_generation_failure` and stops. A poison-pill event is
therefore a durable, human-required intervention. This should be an explicit
accepted limitation with a documented runbook, or the fence should be
per-projection with a bounded, audited auto-detach.

---

## F-04 — Projection recovery validation is quadratic in commits × groups, not
## bounded work. Severity: **High**

**Evidence.** `crates/riffdb-projection/src/recovery.rs`:

- `next_group_key` (line 388) calls `scan_through(self.retained_frontier, …)`,
  which pages the **entire commit log from sequence 1 through the retained
  frontier** (`scan_through`, line 488, starts at `CommitScanRequest::initial`),
  evaluating every commit, to find the single next group key.
- `next_state_row` (line 368) calls `next_group_key` once per state row, and the
  recovery loop pages rows one at a time (`one_recovery_limit()`, line 583).
- `replay_key_through` (line 440) scans the whole log again per key.
- `replay_apply_request` (line 407) calls `replay_key_through` for **every changed
  group of every sequence** in the marker page.

So validating one generation costs `O(frontier × groups × frontier)` commit
evaluations in the worst case. The page limits (`REPLAY_SCAN_PAGE_ROWS = 500`,
`ProjectionRecoveryPageLimit`) bound *memory*, not *work*.

**Consequence.** `crates/riffdb-server/src/projection_worker.rs`
`validate_retained_generations` runs this for **both** the published and candidate
generations of **every** identity in the active bundle, on every startup, before
the identity is marked validated. At the production-scale checkpoint ADR-0182
itself cites (~1.1 M entities, 115,690 commands in ADR-0156's context), this is
not a startup cost that can be hidden behind ADR-0182's bounded dirty path — the
dirty path bounds *structural* validation, not projection recovery. The
"bounded" claim in the recovery doc comment ("recovery never retains the complete
group namespace in memory") is true and beside the point.

---

## F-05 — The vector ANN tier cannot engage above 500 admitted rows per
## organization, so `VEC-009`/`VEC-010` are unachievable as specified.
## Severity: **High**

**Evidence.** `crates/riffdb-columnar/src/ann_cache.rs`:
`const MAX_ROWS: usize = 500;` and `Population::retain` returns `None` when
`view.vectors.len() > MAX_ROWS` (line 61). `lookup_or_offer` returns `None` in
that case, and `crates/riffdb-columnar/src/query.rs` line 785 falls through to
`crate::nearest::exact_knn` with `NearestSearchKind::Exact`.

Meanwhile the contract permits a declared threshold up to
`MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG = 65_536`
(`crates/riffdb-contract-ir/src/schema.rs:1343`), and the routing condition is
`candidate_refs.len() > config.row_threshold()` (query.rs line 784).

**Consequence.** For any declared `row_threshold` in `(500, 65_536]`, the query
routes to the ANN branch, the cache refuses the population, and the engine
silently executes exact brute force while reporting `search_kind: Exact`. That is
*correct* (VEC-009 requires the exact fallback and honest reporting), but it means:

- The declared `recall_target_bps` (VEC-010) is never exercised in production for
  any organization above 500 rows, so the recall contract is unmeasurable outside
  the test harness.
- The approximate tier's practical ceiling is 500 rows per organization — far
  below any corpus where ANN matters. The 64 MiB global `MEMORY_LIMIT` and the
  8 KiB/node reservation in `Population::retain` are what force the cap; the
  reservation is ~16× the actual graph size for a 12-neighbor, 8-level graph.
- The graph is keyed on byte equality of the **entire admitted population**
  (`Population::matches` compares `rows` and `vectors` element-wise), so any write
  to any row in the organization invalidates the graph and triggers a full
  rebuild (`build_loop_inner`). Under a write-heavy workload the cache is
  effectively always cold.

---

## F-06 — Full-text search is not a KV projection: it is a per-partition
## in-memory index with a whole-file 256 MiB checkpoint outside redb and outside
## backup. Severity: **High**

**Evidence.** `crates/riffdb-projection/src/tokenized_text.rs`:
`MAX_TOKENIZED_DOCUMENTS_V1 = 4_096`,
`MAX_TOKENIZED_CHECKPOINT_BYTES_V1 = 256 * 1024 * 1024`.
`crates/riffdb-server/src/exact_text_adapter.rs`:
`MAX_REGISTERED_EXACT_PARTITIONS = 256`; `persist_tokenized_checkpoint` →
`persist_predicate_checkpoint_bytes` (line 3353) writes the **complete** provider
bytes to a `.pending` file, `sync_all`, `rename`, then `sync_all` the parent
directory (lines 3366–3390). `read_checkpoint` reads the whole file back.

**Consequence.**

1. **Write amplification is proportional to corpus, not to the delta.** Every
   checkpoint rewrites the entire partition index. FTS-007 requires write
   amplification to be "statically bounded"; a whole-file rewrite is bounded only
   by the 256 MiB ceiling, which is not a useful bound.
2. **The durable state is not in redb and not in the backup.** SPEC §18.5 lists
   "storage-engine data files or a verified logical snapshot" plus checksums;
   ADR-0050's backup is the redb artifact. 256 partitions × 256 MiB = up to 64 GiB
   of checkpoint files under the projections root are outside that artifact. They
   are rebuildable (FTS-005 says so), so this is not a correctness hole — but it
   means restore is followed by a full FTS rebuild whose cost is not in the
   restore receipt or the RPO story.
3. **4,096 documents per partition is a hard ceiling** that contradicts the
   headline "full-text search in one process" for any real corpus. FTS-007
   permits a typed lifecycle refusal rather than truncation, which is honest, but
   the capability is bounded at a scale where a purpose-built inverted index would
   not be.
4. **It is a second durability substrate.** The checkpoint protocol (pending file,
   fsync, rename, parent fsync) is a hand-rolled atomic-replace that duplicates
   what redb already provides, with its own crash-recovery reasoning. SPEC §15.5
   and FTS-005 describe this as a "projection"; the implementation is a separate
   storage engine with a separate durability protocol.

---

## F-07 — The outbox has no dispatcher in the daemon and no HTTP connector; the
## SPEC text overstates what exists. Severity: **Medium**

**Evidence.** `OutboxDispatcher` appears only in `crates/riffdb-outbox/src/worker.rs`
and `tests/outbox/outbox_crash_and_duplicate.rs` — no production composition.
`crates/riffdb-server/src/process_graph.rs` line 477 runs `recover_outbox` at
startup and composes `NoDestinationOutboxHealth`
(`crates/riffdb-server/src/outbox_adapter.rs:33`). The `http-webhook` feature is
declared with an empty feature list (`crates/riffdb-outbox/Cargo.toml:11`), and
`DisabledHttpWebhookConnector::deliver` unconditionally returns
`ConnectorDisposition::PermanentFailure` (`connector.rs:441–445`) with the comment
"Enabling `http-webhook` adds no network client and grants no ambient I/O."

**Consequence.** SPEC §14.3 requires "An optional HTTP webhook connector disabled
by default and clearly outside command execution." What exists is a feature flag
that adds no client and a connector that always fails permanently. The POC
therefore has: atomic intent (real, proven), a status state machine (real), a
deterministic/file-test connector (real), and **no** external delivery path. That
is a defensible POC scope, but the SPEC sentence reads as if the connector exists.

On the two sub-questions:

- **Retry policy is specified and implemented.** `DeliveryPolicy`
  (`crates/riffdb-outbox/src/policy.rs`) requires an explicit delay between every
  attempt, caps attempts at 32, caps timeout at 3,600 s, caps lease/delay at
  86,400 s, and refuses a lease shorter than the connector timeout. Good.
- **At-least-once is specified** (SPEC §14.3 line 6033: "The POC promises atomic
  event intent and at-least-once dispatch. It does not promise exactly-once
  external effects."). **Consumer idempotency is only declared, not specified.**
  `ConnectorIdempotency` (`connector.rs:35`) is a declaration enum, and
  `DeliveryAttempt::idempotency_key()` exposes the 12-byte `EventId`
  (`connector.rs:131–133`) — but no normative wire mapping (header name, field,
  encoding) exists anywhere in SPEC §14 or the proto surface. A downstream
  consumer cannot implement deduplication against a contract that does not name
  the carrier.

---

## F-08 — Backup is offline-exclusive, the receipt is not part of the artifact,
## and the online story depends entirely on unbuilt packages. Severity: **Medium**

**Evidence.** SPEC §18.5 lines 7102–7105: "Backup/restore is exclusive. The server
drains accepted work under a fixed deadline, closes redb, performs the backend
operation…" ADR-0182 context lines 98–104: "Between maintenance windows the commit
log, events, provenance, and outbox grow without bound. Backups are offline for
the same reason." ADR-0178 context line 101: "Production still installs
`NoChangelogPublicationPort` (`crates/riffdb-storage-redb/src/store.rs`), there is
no replication RPC… no incremental backup." SPEC §10.2 lines 3922–3927: the
checksummed maintenance receipts "live only beneath the configured external
`backup_root/.maintenance` subtree. They are not database metadata, not a redb
table/key, not a `StoredEnvelope`, not copied into a backup artifact."

**Consequence.** The receipt design is sound as an *audit* artifact — it is
versioned, checksummed, bounded, atomically replaced, and explicitly outside the
durable format so it cannot become a seventh metadata category. But because it is
not copied into the backup artifact and is not replicated, the receipt is a
single-copy record of the only operation that can close or replace the database.
Losing `backup_root/.maintenance` loses the ability to reconcile an interrupted
restore. ADR-0178 §7's archive manifest is the intended fix (it binds the
full-backup manifest digest), but it is unbuilt.

Until WP-746–750 land, RPO is "last offline backup" and every backup is a full
service outage. ADR-0183 §1 explicitly ranks availability above performance for
exactly this reason, which is the right call — but the SPEC's §18.5 text should
not be read as if incremental backup exists.

---

## F-09 — SPEC §18.5 still states the POC has no incarnation fence, contradicting
## accepted ADR-0072. Severity: **Medium** (governance)

**Evidence.** SPEC line 7122–7124: "The POC has no incarnation/history-epoch
fence; documentation and destructive confirmation must state that limitation."
But ADR-0072 (accepted 2026-07-30) adds `history_incarnation/v1` to the retained
metadata set, SPEC §10.2 line 3913 lists it among the exact `meta` keys, SPEC
`STO-012` (line 3938) names it as a required durable operational metadata
category, and SPEC lines 1046, 1134, 1157, 1198, 1246–1265 all bind cursors,
tokens, and replication handshakes to it. `R-16`'s mitigation text ("require an
incarnation ADR before stronger claims") is satisfied.

**Consequence.** Two authoritative documents disagree about whether a fence
exists. AGENTS.md is explicit: "When these conflict, stop and request human
review. Do not choose whichever interpretation is easiest to implement." §18.5
needs an amendment that states the fence exists, that restore advances it via
`max(target, staged) + 1`, and that the residual limitation is narrower than
"no fence" — namely that a *pre-fence* client that omits
`observed_history_incarnation` is still unvalidated (ADR-0072: "Absent means
unvalidated").

---

## F-10 — R-13's RSM-shaped boundary is real at the interface but the durable log
## is a *derived* view, which is the wrong direction for Raft. Severity: **High**
## (roadmap)

**Evidence for "real".** SPEC §10.1 and ADR-0004 give a genuinely
replication-friendly boundary: owned `ReadSnapshot` (no live engine view), a
consuming typed batch protocol with no closures or caller-selected sequences, no
engine handle crossing the API, and a closed error taxonomy. ADR-0093 §1 defines
the changelog as "the complete, checksummed set of durable record writes bound to
*s* across every authoritative table" — a physical write-set log, which is the
shape an RSM needs. ADR-0178 §2 applies one frame per storage transaction through
storage ports, with the applier as the follower's sole writer. ADR-0083 removed
post-images from commit records, so the log is a reference log, not a
duplicate-state log.

**Evidence for "nominal".** ADR-0093 §1: "The changelog is a derived view of
durable state, **not a second durable structure on the primary**: no new
write-time obligation enters the commit path." ADR-0100 aligns frames to
*published durable frontiers*. ADR-0178 §8: "Acknowledgement semantics, the write
hot path, durable record encodings, the backup manifest, and every registry pin
are untouched. Quorum durability, automatic election, cascading followers, and
multi-primary remain outside this record." ADR-0178 Options Considered #2 rejects
OpenRaft for this stage because "It changes acknowledgement semantics and the
commit hot path, contradicts `REP-001`."

**Consequence.** The boundary is real enough that a follower can be built without
redesign — that part of R-13 is genuinely mitigated, and ADR-0178 is the right
sequencing. But the *log* is not a Raft log:

- A Raft log is the **source** of order and durability; here the log is a
  projection of already-durable state. A leader cannot acknowledge on log
  replication because the log entry does not exist until after the local commit.
- The local journal (F-01) is reclaimed by redb checkpoints
  (`MAX_JOURNAL_SUFFIX_TRANSITIONS = 8_192`, 32 MiB) and is explicitly a *local*
  artifact. It has no term, index, or leader epoch, and it is not the changelog.
- Adding quorum acknowledgement means the commit coordinator's sequence
  assignment (ADR-0004 step 9) must move *after* a quorum round-trip, which
  changes `PERF-009` and the hot path — precisely what ADR-0178 §8 refuses.

So the honest statement is: the *apply* side is RSM-shaped and ready; the *order
and acknowledgement* side is not, and retrofitting Raft requires inverting the
changelog from derived to authoritative plus a new acknowledgement mode. That is
a larger change than "OpenRaft behind a facade" implies, and SPEC §20.6's
"OpenRaft or equivalent consensus behind `riffdb-replication` only as a later
amendment" understates it. The design does **not** need a WAL-first rewrite from
day one — the journal already exists — but it does need an accepted record that
the changelog becomes authoritative before any consensus work starts.

---

## F-11 — No online compaction or space reclamation; redb's file high-water mark
## is permanent. Severity: **Medium**

**Evidence.** SPEC §10.1 line 3804: "The concrete `redb` adapter **may** contain
private compaction, backup, integrity, and statistics APIs" — permissive, not
required. No `compact()` call exists anywhere under `crates/riffdb-storage-redb/`
(grep for `compact` returns only unrelated identifiers: compact envelopes, compact
durable format V2, compact export heads). `crates/riffdb-storage-redb/src/retention.rs`
reclaims space only by deleting `commits`, `events`, `outbox`, and `outbox_status`
rows below the watermark (lines 450–452) and rewriting command segments; it does
not shrink the file. `docs/performance/wp-452-write-amplification.md` records a
13-table inventory and a 65,536-allocated-bytes-per-command ceiling, i.e. every
command touches ~13 redb B-trees, each of which is copy-on-write.

**Consequence.** Deleted pages are reused within the file, so steady-state growth
is bounded by live data — but the file never shrinks, and there is no online
reclamation path. The only way to reclaim is the offline exclusive backup/restore
(SPEC §18.5) or the offline retention operation (ADR-0085 A2). For a multi-tenant
system of record this is a capacity-planning hazard: a tenant that churns 100 GB
of data leaves a 100 GB file behind. ADR-0182 §6 moves retention online but does
not add compaction. This should be either an accepted, documented limitation or a
named package.

---

## Summary table

| ID | Finding | Severity |
|---|---|---|
| F-01 | redb is not the durability authority; a first-party WAL already exists beside it | High |
| F-02 | R-04's Fjall exit comparison cannot produce decision evidence | Medium |
| F-03 | One poison projection pins the global retention fence and blocks read-after-commit forever | High |
| F-04 | Projection recovery validation is O(frontier² × groups) work, not bounded work | High |
| F-05 | Vector ANN cannot engage above 500 rows/org; recall contract unmeasurable | High |
| F-06 | FTS is a separate engine with whole-file 256 MiB checkpoints outside redb and backup | High |
| F-07 | No outbox dispatcher in the daemon, no HTTP connector, no normative idempotency-key carrier | Medium |
| F-08 | Offline-exclusive backup; receipt not part of the artifact; online story unbuilt | Medium |
| F-09 | SPEC §18.5 contradicts accepted ADR-0072 on the incarnation fence | Medium |
| F-10 | RSM boundary is real for apply, nominal for order/acknowledgement; Raft needs the changelog to become authoritative | High |
| F-11 | No online compaction; redb file high-water mark is permanent | Medium |

## What the design gets right

Worth stating, because the findings above are all about gaps:

- The semantic storage boundary (ADR-0004) is genuinely engine-neutral: owned
  `ReadSnapshot`, no closures, no caller-selected sequences, no engine handle
  crossing the API, closed error taxonomy. This is the strongest part of the
  design and it is what makes F-10's apply side tractable.
- Per-identity projection frontiers with atomic marker + rows + frontier
  (ADR-0010/ADR-0017) are correct, and the generation model handles rebuild
  without regressing a published frontier.
- The outbox retry/lease policy is properly bounded and refuses a lease shorter
  than the connector timeout.
- ADR-0182's move from population-walk dirty recovery to journal-suffix replay
  plus bounded roots plus first-access validation plus a background scrub is the
  right shape, and its 64 MiB recovery ceiling is a named, measured constraint.
- ADR-0183's decision to freeze performance work behind availability and recovery
  is the correct prioritization, and it is honest that the c32 gap may remain at
  0.88.
