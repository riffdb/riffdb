# WP-747 verification and review

**Package:** WP-747 — Follower reads, typed follower-mode refusal, and sequence-lag health

**Tier:** guarantee

**Status:** implementation, obligation proofs and full CI verified on 2026-09-15.

## Behavior added or changed

Followers serve compiled, projected and discovery reads from completed local
application prefixes through the shared service and current authorization.
`Available` and `Bounded` use the local applied head; source telemetry cannot
satisfy freshness. `Causal` waits register before observing and remain bounded.
`AdmissionHead` captures the completed local head after authorization.

Projected freshness tokens and frontiers now carry database identity and history
incarnation in opaque V2 values. Foreign databases and history incarnations
refuse before observation or waiting. Discovery preserves the caller's existing
history claim across presentation-generation changes and refuses a detected
history mismatch after authorization. Unresolved process-local cursors retain
`RDB-CURSOR-0101`, as explicitly accepted in the cursor clarification.

Configured scalar and vector columnar sources start cold. A single owned worker
builds disposable V2 views from completed follower snapshots on demand, with
independent logical validation, existing bounds and bounded scratch cleanup.
Cold, activating, failed and stopped views cannot report healthy or release rows.
No follower materialization writes authoritative controls or claims the source
artifact's checksum. Canonical vectors occupy existing V2 byte lanes losslessly;
typed/dimension validation precedes publication and byte-order pruning is absent.

A single replication health component and typed Statistics fields expose applied,
acknowledged and observed source frontiers, with lag measured in application and
administrative sequences. Unknown progress remains absent. Commands and other
operations requiring local authority return the typed follower-mode refusal.
The accepted audit amendment preserves current authorization: policy-required
audited reads refuse; authorized Health/Statistics and denied reads use bounded
redacted telemetry without local durable audit.

## Requirement and obligation evidence

All daemon proofs run through verified TLS and actual primary/follower processes.
Explicit relay barriers and registered-wait observations establish ordering.

| Requirement or obligation | Proof | Assertion |
| --- | --- | --- |
| REP-004; OBL-0178-3 | `follower_causal_read_waits_for_token_then_matches_primary` | Hold delivery after a known prefix; Available and zero-lag Bounded return that local prefix; a registered causal read remains pending, then equals the primary's complete result after release; writes refuse |
| REP-004 | `follower_scalar_and_vector_views_match_primary_after_advance_and_restart` | Scalar and nearest-vector results match at exact heads after bootstrap, advance and restart; another tenant is excluded; independently issued foreign-database tokens with identical numeric incarnation/sequence refuse |
| REP-004 | `follower_exact_providers_match_primary_after_tail_and_restart` | Exact text, exact predicate, tokenized text and long-pattern providers match under AdmissionHead; valid second pages match; foreign-process and obsolete continuations release no page |
| REP-004 | `tls_follower_daemon_serves_replicated_catalog_refuses_writes_and_reopens` | Replicated catalog discovery and writes use shared admission; command/resource discovery history mismatches refuse with either equal or changed generation; equal fences retain CatalogUnchanged |
| REP-004 | `replication_stream_resumes_gap_free_after_repeated_kills` | Held delivery exposes exact positive primary lag and completed follower progress; both nodes report sequence counts; repeated recovery preserves complete prefixes |
| PRJ-005–009 | `columnar_admission_tests` | Full bounded source/control inventory, exact schema and identity validation, missing/mismatched/excessive state refusal, no control mutation or artifact adoption at admission |
| PRJ-008–009; REC-002 | `follower_columnar_demand_builds_disposable_views_and_refuses_withdrawal` and `follower_columnar_owned_worker_wakes_registered_demand_and_rebuilds_after_restart` | Concurrent demand coalesces into one owner; status stays cold; validated install wakes waiters; withdrawal refuses even retained views; restart rebuilds derived state |
| PRJ-009; REC-002 | `follower_columnar_cancelled_build_drains_scratch_without_publishing` and `follower_columnar_failed_source_stays_closed_until_owned_restart` | Cancel a real scratch build before publication; files drain and controls remain exact; requests cannot clear failure or retry; owned restart recovers |
| PRJ-005–009; REC-002 | TLS scalar/vector restart campaign and authoritative namespace oracle | Invalid inventory fails on demand; local repair does not clear failed state; restart discards abandoned corrupt material; all replicated authoritative namespaces equal the exact source prefix |

The canonical-vector tests separately prove nullable and maximum-dimension values,
exact canonical payloads, typed reopen, checksum-valid payload corruption refusal,
unchanged row/lane limits, and nearest-result equality after reopening and removal
of the original files. Existing primary control/CAS tests remain part of full CI.

Shared service tests prove every intrinsically mutating or audited operation
family refuses without a writer. The follower policy tests cover ordinary reads,
initial required audit, initial denial, revocation at the read safe point and a
new audit obligation at reauthorization. The completed-prefix authentication
adapter rereads current capability state and withdraws admission on close.

## Checks run

- Scoped-token acceptance: all nine steps passed; 562 tests passed.
- Lifecycle/cursor acceptance: all twelve steps passed; 401 tests passed, six
  existing skips. The missing handbook summary entry found by the first run was
  added before the passing rerun.
- Discovery acceptance: all eight steps passed; 921 tests passed, six existing
  skips. The TLS regression failed before preserving the history claim and
  passed for both discovery operations after the fix.
- Full `scripts/ci-all` passed, including workspace tests and doctests, Rustdoc,
  dependency checks, operator/adapter conformance, generated artifacts, Helm
  checks, public authoring acceptance and source-install smoke.
- Final closure acceptance: all twelve steps passed with `--no-tests`. Only
  documentation and the completion record changed after the passing full CI;
  the implementation's tests were not repeated. Log:
  `/tmp/wp-747-closure-acceptance.log`.
- The Python adapter build reports installed `maturin` 1.15.0 versus its declared
  1.14.1 build requirement. The same environment warning appears in the passing
  WP-746 full-CI log; this package changes neither dependency nor build pin.

Logs are retained under `/tmp/wp-747-scoped-acceptance.log`,
`/tmp/wp-747-lifecycle-acceptance2.log`,
`/tmp/wp-747-discovery-acceptance.log` and `/tmp/wp-747-ci-all.log`.

## Compatibility and accepted review

The maintainer accepted the follower columnar amendment (`1d910c83`), canonical
vector amendment (`1b3a97fc`) and exact cursor clarification (`3fe8826b`). The
already accepted follower audit boundary continues to apply. No new durable
columnar control, source authority, cursor bytes or discovery wire field is added.
Primary scalar V2 artifacts and V1 freshness fixture bytes remain unchanged.

V2 projected response tokens require readers supporting V2. Primary services
continue to accept legacy unscoped V1 input tokens under their trusted identity;
followers refuse them because they cannot establish database scope. Generated
client/error bindings expose follower mode through the existing typed taxonomy.

Handbook pages: [consistency](../concepts/CONSISTENCY.md),
[ingress and sequence-lag health](../operations/REMOTE-INGRESS.md),
[configuration](../configuration.md), [errors](../reference/ERRORS.md), and
[V3 implementation](CHANGELOG-V3.md). Review records and this evidence are
reachable from the handbook summary.

## Decisions, hazards and follow-ups

- Retain one completed immutable follower publication as the authority for reads,
  identity, capabilities and local frontier; reported source heads are telemetry.
- Use independently validated disposable follower V2 bytes under the accepted
  amendment, with no local durable control publication or source-checksum claim.
- Bind opaque projected freshness to database identity using the evolution
  explicitly permitted by ADR-0086; preserve legacy codecs and reject unbound
  follower inputs instead of guessing their database.
- Preserve discovery history separately from process-generation cache hints and
  reuse the shared post-authorization history check. Legacy absent history keeps
  ADR-0072 behavior; generation-only refresh keeps ADR-0040 behavior.
- Retain existing opaque-cursor refusal exactly as the maintainer clarified;
  never restart a failed continuation internally.
- WP-748 owns promotion, leadership fencing and follower-hold retirement. WP-749
  owns archive restore; WP-750 owns their combined acceptance campaign. This
  package does not lift the performance freeze or provide automatic failover.
