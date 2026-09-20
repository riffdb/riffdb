# ADR review: performance & UX improvements that keep every VISION promise

Scope: all 238 records in `adr/`, reviewed against `docs/VISION.md`,
`docs/known-limitations.md`, the ADR-0239 baseline
(`docs/performance/c3d-programme-bank-2026-09.md`), and
`docs/performance/deferred-dormant-path-optimizations.md`.

**Method.** Every suggestion below is framed as a new or amending ADR
(accepted text is only amended by a new record), is bounded, and was checked
against each VISION promise: single total order; committed = indexed =
deliverable; one typed freshness vocabulary; opinions only in the data layer;
à-la-carte pillars with changelog/export as the graduation path; unsafety
unexpressible at the public surface; bounded/finite surfaces; deploy-time
rejection over runtime surprise; agent-learnable from docs and errors at
runtime; no per-operation re-payment of proven guarantees; no SQL/arbitrary
joins on the critical path. Nothing proposed adds a caller-selected plan,
provider, artifact, fallback, or any way to opt out of durability, scoping,
or typed freshness.

**Two reviewer errors corrected during consolidation.** ADR-0166 is already
superseded by the accepted ADR-0187 (bounded columnar shutdown abandonment) —
"accept 0166" is not actionable. ADR-0232 is already accepted (2026-09-16) —
its gap is an unregistered implementing package, not acceptance.

---

## A. Highest impact: performance

### A1. Retire ADR-0165's three durable locator tables (amend ADR-0165)
ADR-0165's own measured consequences: **−13.5% seed throughput, +4.96% DB size
per table, +180 B journal frame per command (+5.78%)** — for rows that re-encode
keys ADR-0102's segment manifest already proves. ADR-0165 chose durable rows
only because the transient index was then the sole locator and dormant meant
absent-for-present; ADR-0102's checksummed index snapshot (bound to database
identity, incarnation, registry digest, segment frontier and root digest, with
suffix-only startup scanning) removes that premise, and ADR-0234's finding-8
repair plus ADR-0236's retirement of the fresh-locator witness show the
direction of travel. New ADR: make the ADR-0102 derived index the locator
(snapshot-loaded, else bounded rebuild), keeping ADR-0165 §3's fail-closed rule
(missing/mismatched locator ⇒ typed corruption, never absence). Pre-alpha
durable-format cut, so no compatibility ceremony beyond the epoch gate.
*VISION: strengthens "no per-operation re-payment of already-proven
guarantees" — the locator rows are a second durable encoding of proven bytes.*

### A2. Ship WP-760 (ADR-0182 + ADR-0203): the dirty-restart failure is live
Known-limitations still records that dirty restart at production population
**fails closed with `LimitExceeded` at the 512 MiB historical-evidence ceiling
and never publishes readiness**. ADR-0182 (accepted) fixes exactly this —
journal-suffix replay + bounded-root validation, first-access row validation,
background scrub, 64 MiB recovery ceiling — and ADR-0203 supplies the public
scrub. No new decision needed; this is an execution priority. Two small
amendments worth attaching: (a) guarantee the offline-scrub path is reachable
and documented as the typed remediation when a dirty start itself fails (the
background scrub only runs *after* readiness, so a failed dirty start currently
has no in-process exit); (b) let scrub receipts report progress in
rows-per-step so operators can estimate completion. WP-760 is also a
replication prerequisite (follower retention fencing depends on online
retention). *VISION: preserves fail-closed recovery by construction; bounded
validation, no skip/force selector.*

### A3. Dormant-path repairs are pure internal wins once their features activate
The four findings in `deferred-dormant-path-optimizations.md` are not blocked
by any accepted decision — only ADR-0231 §8's scope (incremental catch-up
excludes tokenized/long-pattern providers) needs extending:
- **O(N²) projection index construction** (`long_pattern.rs` clone-and-rebuild
  per insert/remove; `exact_predicate.rs` per-batch clone): fix by building
  into a private candidate and publishing atomically — exactly the shape
  ADR-0160 §5/§6 and ADR-0162 §3 require. Exact-text/exact-predicate are
  already authorized by ADR-0231; a new ADR extends that discipline to
  `long_pattern_v1` and the tokenized provider.
- **Per-query `CorpusStatistics`** (built per query and recomputed per
  candidate): ADR-0173 doesn't block caching — it defines the cache key
  (epoch, authorized set, policy revision). Never an authorization cache.
- **Materialize-before-offset in tokenized text**: a bounded top-(offset+limit)
  heap — the shape ADR-0161 §7 already uses for columnar top-N — preserves
  ADR-0173's byte-exact rank equality.
- **Per-row index scans in row-policy evidence**: ADR-0136 §Consequences
  explicitly anticipates the pay-once cache; batch lookups into one bounded
  multi-key range scan per page (ADR-0054/0150 shape), preserving
  policy-before-observation.
ADR-0239's review trigger already requires this doc be revisited before
activation; fold these into that gate. *VISION: "no per-operation re-payment of
proven guarantees" applied literally.*

### A4. Re-sweep the batch-concurrency cap (ADR-0097's 128) on the new baseline
ADR-0097 rejected 192/256 because serialized validation/encoding/staging erased
the flush savings — a measurement that predates ADR-0129 (pay-once preparation)
and ADR-0104 (no synchronous redb apply on the ack path), i.e. the exact stages
the rejection blamed. New ADR: re-run the bounded 128/192/256 sweep under
ADR-0239 discipline (C3D, vs banked revision, variance-gated), raising the
compiled default only if retain-or-revert passes. *VISION: still a compiled
ceiling, no caller knob; no durability or ack change.*

### A5. Bounded FIFO-pipelined session mode (amend ADR-0140/0127)
An application pipelining independent commands today chooses between the
multiplexed session's measured router machinery (BTreeMap, oneshot,
`FuturesUnordered`, background routing task — the residual ADR-0140 §Context
documents) or N full TLS + session establishments for N serial lanes. Missing
shape: up to K in-flight (K ≤ the existing 128 ceiling) with **responses in
strict submission order** — a fixed FIFO ring replaces the out-of-order router
entirely while overlapping client encode/network with server execution.
Cancellation still destroys the lane generation; uncertain commands still
resolve only by idempotency identity; stream order still grants no freshness
(causal reads still pass the explicit frontier). Reject-first activation under
ADR-0239, with ADR-0140-style architecture tests proving no map/router exists
in the pipeline path. *VISION: bounded (fixed K), no new wire identity, single
total order untouched.*

### A6. Prune steps as ordinary writer frames (amend ADR-0182 §6 / ADR-0101 §3)
Each ≤256-sequence online-retention prune step is currently an
administration-class **hard barrier** that drains the journal lane and
checkpoints — making online retention slow and writer-interfering (ADR-0182 §7
bounds it to 5% p95). New ADR: classify bounded retention prune steps as
ordinary typed writer frames (the treatment ADR-0101 §3 already gives validated
standalone service-audit appends), retaining the full fencing set, watermark
atomicity, and crash-window recovery. Companion amendment: after a prune step
consumes the validated-prefix checkpoint, opportunistically re-earn it in
writer idle windows (mechanism already named in ADR-0085 A2) so
retention-active databases don't permanently live on the dirty-recovery path.
*VISION: single total order preserved; no ack or durability change.*

### A7. Smaller, well-scoped perf items
- **ADR-0084 legacy success-row mirroring**: its own consequences say it
  "doubles the encoded payload of an all-success batch, halving effective
  aggregate response capacity" and is "removable with the legacy field after
  alpha clients migrate." Retire it via a new ADR once no pre-0084 reader
  exists. *Bounded surfaces; no per-item semantic change.*
- **Cover-eligibility witness reuse** (amend ADR-0133/0130): key the witness by
  catalog generation + plan/module identity, as ADR-0130 §6 already does for
  descriptors. *Pay-once rule.*
- **Vector graph reuse on continuation pages** (amend ADR-0229 §7): bind exact
  graph selection into the cursor — the "separately reviewed cursor binding"
  0229 itself names. *Committed=indexed=deliverable; bounded.*
- **Byte-aware limit refinement** (amend ADR-0158/0167): a compiler-declared
  per-row byte bound alongside `Limit<MAX>` so wide queries get a proven
  byte-safe maximum instead of failing compilation on a conservative one.
  *Deploy-time rejection, kept bounded.*
- **Packed carriage beyond covered named queries** (amend ADR-0133/0135):
  extend the existing production-proven packed codec to eligible projected and
  exact-provider result sets rather than a second cell format. *Pay-once.*

---

## B. Highest impact: UX (agents are the primary builders)

### B1. Make `Building` self-teaching: bounded await + retry guidance (amend ADR-0195/0196/0086)
The first query against a cold columnar source — and every concurrent query
during activation — returns a rowless typed `Building` with **no progress
signal and no discoverable retry policy**; an activation failure is rowless
for the whole process generation. Three compatible repairs, none touching the
review trigger "a public or operator input could choose eager versus demand
activation":
1. **Deploy-time activation**: activation becomes a consequence of deploying a
   contract that declares the projection — server-owned, still validated before
   any view installs; not a caller knob.
2. **Bounded await**: reuse ADR-0164's existing server-owned maximum wait so a
   caller's freshness declaration can wait through activation once, returning
   the existing typed lifecycle outcome on expiry — no new enum.
3. **Retry guidance + progress class**: give `Building` the bounded
   `retry_after`/reset guidance `ProjectionLagging` already carries
   (ADR-0086 §7), and expose a closed activation-progress class
   {validating, installing-view, building-from-snapshot} plus checked frontier
   distance through the *already-permitted, non-activating* read-only status
   observations.
*VISION: strengthens "one typed freshness vocabulary" and
"learnable-by-agents from docs and errors at runtime."*

### B2. Ship the explain surface ADR-0087 already promises
ADR-0087 §Governance normatively requires "every query can report its access
shape and budget consumption without executing," but known-limitations records
"no request vocabulary and no explain surface" — agents learn cost only by
being rejected after paying it. This is the one place the record set and the
shipped surface openly diverge, on the agent-generated-query path 0087 itself
calls a launch requirement. New ADR: expose the compiler-owned access shape
(ADR-0150 §2 already compiles it) as a bounded, redacted, read-only
validate/dry-run mode returning the access shape and which budget binds — no
rows, no new grammar, no caller-selected plan. *VISION: explain is
pre-execution rejection — the strongest form of "deploy-time rejection over
runtime surprise."*

### B3. Surface compatibility culprits, not counts (amend ADR-0066/0046)
An agent whose successor deploy fails `Incompatible` gets a bounded summary of
code **counts only**; the full bounded path report (≤4,096 findings) exists at
the compiler boundary but is unreachable via MCP. The agent iterates
deploy-fail cycles, mutating one declaration at a time. New ADR: expose the
complete bounded compatibility path report through the existing
`riffdb_contract_validate` fixed tool (and a
`riffdb://contract/<lineage>/<version>/compatibility` resource) at
validate-time — not in the hot `contract.get_active` descriptor, keeping the
4 MiB ledger and redaction intact. *VISION: strengthens both deploy-time
rejection and agent-learnability; bounded, compiler-owned.*

### B4. A generated error-code catalog resource (amend ADR-0122)
The system has excellent typed codes (`RDB-AUTH-0214`, `type_mismatch`,
`RDB-CURSOR-0101`, input-error codes) but an agent receiving an unfamiliar code
must leave its authorized surface to learn the recovery action. ADR-0179's
operation registry makes a generated catalog cheap: one additive
non-subscribable resource (`riffdb://errors` or an extension of
`riffdb://application/guide`) rendering the closed public error-code inventory
— code, class, bounded meaning, recovery action — generated from the registry
with the same drift referee, authorization-filtered like the guide.
*VISION: directly serves "every surface learnable from documentation and error
messages at runtime."*

### B5. Typed continuation-failure classes (amend ADR-0194/0008)
Stale, expired, mismatched, cross-query, and denied cursors all "fail without
structured content"; an agent mid-page-walk cannot distinguish "snapshot
expired, restart from page 1" from "wrong cursor family" (two coexisting
dialects: 0194's 22-char base64url vs 0008's 32-hex — a *likely* agent error)
from "authorization narrowed." New ADR: a closed continuation-failure code set
(`cursor_stale`, `cursor_expired`, `cursor_family_mismatch`, `cursor_denied`)
in the existing typed error mapping; family mismatch is locally checkable from
string shape alone. Also make per-provider cursor retention windows part of
the generated operation documentation (ADR-0173 itself says ranked-cursor
expiry "must be documented as a property of ranked search rather than
discovered as a defect"). *VISION: typed outcomes / finite grammars; no cursor
payload or redaction change.*

### B6. Uniform retry/uncertainty helpers across every generated operation (amend ADR-0040/0106)
Retry helpers exist only for the operations stdio and WP-150 needed; every
other generated command leaves the agent to hand-classify outcome-unknown and
reconstruct the "resubmit with the *original* idempotency identity" rule from
prose. A mis-retry that mints a new identity creates a second operation — the
exact failure ADR-0041 warns about. New ADR: extend the checked
retry/uncertainty classification to the full generated operation registry as a
driver conformance obligation — one closed vocabulary
(retryable-with-same-identity / terminal / outcome-unknown-resolve-by-
idempotency), driver-owned bounded retry budget, typed attempt disposition —
proven in the ADR-0148 conformance corpus so all four bindings classify
identically. *VISION: makes learnable-from-errors mechanical; never converts
uncertainty into a safe fresh retry.*

### B7. Smaller, well-scoped UX items
- **Multi-violation input diagnostics** (amend ADR-0064): return the first k≤8
  violations in canonical order instead of one per call; same codes, pointer,
  redaction. An agent with five mistakes in a twelve-field input currently
  pays five full round trips. *Bounded, closed envelope.*
- **Batch outcome recovery** (new operation): uncertainty resolution is
  per-item; a lost 16-item transport response costs up to 16 recovery round
  trips. One bounded, input-ordered batch resolution operation (≤ the existing
  16-item bound), same per-item authorization/redaction, no atomicity claim.
- **Observer-session terminal reasons** (amend ADR-0048): meter exhaustion
  currently "fails closed and terminates the session" with no typed signal;
  surface a closed session-terminal reason (`meter_exhausted` /
  `lifetime_expired` / `idle_expired` / `auth_lost`) — session-lifecycle
  facts, not per-call budget forensics, so the non-disclosure rule stands.
  Document the reconnect-and-resubscribe flow in the cookbook.
- **`ResponseTooLarge` remediation** (amend ADR-0027): carry the SDK's existing
  recovery-action field with a fixed per-operation remediation string ("reduce
  the declared page limit" vs "this record exceeds the POC retrieval bound;
  use export"). Still static, bounded, size-free.
- **Ad-hoc aggregate surface** (amend ADR-0087): the projected ad-hoc CLI
  computes exactly one function per ungrouped request and rejects decimal/money
  `sum`; the named-query path (ADR-0108/0152) already supports multi-measure
  and decimal sums, so a compiler-declared finite multi-measure list and
  decimal sums on the ad-hoc surface are a small amending ADR, not new
  machinery. Add a compiler-declared presentation order over group keys
  (already a total order) instead of telling agents to sort client-side.
- **Driver/transport self-description** (amend ADR-0120): a bounded
  diagnostic surface reporting active transport shape, negotiated session
  ceiling, pool fill state, last close cause, and the observed durability mode
  of the last acknowledged command — fixed-cardinality, authorization-filtered,
  pinned in the cross-driver conformance matrix.
- **Export session summary** (post-ADR-0232, note on ADR-0217): pages emitted,
  work consumed, sequences covered, on the existing receipt path, so an agent
  can estimate a graduation export's remaining work. Read-only; no bound
  becomes configurable.

### B8. Leave these alone (deliberate, correct decisions)
- Discovery absence ambiguity (ADR-0064/0065: "absent, stale, or unauthorized —
  do not infer which") is inference protection; any richer signal would
  backtrack the authorization boundary.
- The 200 µs / 2 ms completion-edge windows stay falsified (WP-624: waiting
  cost 3.9% throughput).
- ADR-0137/0138/0139's removed direct lanes stay removed; ADR-0141 governs any
  revival.
- The 128 in-flight ceiling, per-app pool tuning, 0-RTT, and measurement
  retries beyond ADR-0143/0146 stay refused.
- ADR-0196's refusal to merge the event-derived and columnar `Degraded`
  registries was correct as a *shape* decision; the unification worth doing is
  the published freshness→action mapping (B1/B4), not a merged enum.

---

## Suggested sequencing

1. **Now, no new decisions needed:** land WP-760 (A2); register the
   implementing package for accepted ADR-0232 (O(P²) export repair); implement
   ADR-0108's already-accepted multi-measure/decimal aggregates on the
   named-query path.
2. **First new ADRs (perf):** A1 (locator-table retirement), A4 (cap re-sweep),
   A6 (prune frames + checkpoint re-earning).
3. **First new ADRs (UX):** B1 (Building await/guidance), B3 (compatibility
   culprits), B4 (error catalog), B6 (retry helpers) — all four are cheap,
   generated-from-registry, and serve the agent-builder promise directly.
4. **Feature-activation-gated:** A3 (dormant-path repairs, folded into
   ADR-0239's revisit trigger), A5 (FIFO pipeline, reject-first).
