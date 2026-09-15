# WP-747 follower columnar materialization — accepted amendment

Status: accepted 2026-09-15. The maintainer approved the exact text below in
session: "Approve the exact amendment". Standalone acceptance commit
`1d910c83` records it in SPEC §4.10 and ADR-0178, with the explicit qualifications
in ADR-0192, ADR-0195 and ADR-0209. Runtime implementation remains WP-747 work.

## Conflict established

ADR-0178 §4 requires follower projected and vector reads. ADR-0178 §2 and
ADR-0186 make the applier the sole follower database writer and require exact
replication of authoritative controls. Columnar controls are replicated; their
external generation roots, manifests and segments are not in the database
bootstrap or authoritative mutation stream. Projection-segment shipping is
explicitly deferred by ADR-0178.

SPEC PRJ-005 through PRJ-009, ADR-0192 decisions 14–16, ADR-0195 decisions 4–6,
and ADR-0209 decision 7 require publication through durable control and serving
only its exact validated artifact. The primary worker creates or replaces that
control when rebuilding. Installing that worker on a follower would originate
local authority; selecting a freshly rebuilt artifact under the source's
checksum would falsely claim byte identity. Ignoring control selection or
falling back is forbidden. The existing process test
`columnar_control_recovery_refuses_corrupt_selected_v2_without_v1_fallback`
proves that selection remains closed after an invalid root.

The follower-audit amendment remains independently accepted. This separately
accepted columnar amendment governs materialization; the acceptance commit itself
changes no production runtime behavior.

## Exact accepted amendment

The following text amends ADR-0178 §4 and supplies the explicit follower-only
exception to SPEC PRJ-005 through PRJ-009, ADR-0192 decisions 14–16, ADR-0195
decisions 4–6, and ADR-0209 decision 7. Their primary behavior remains intact.

> **Follower columnar views.** Scalar and vector columnar sources on a follower
> retain the same checked schema-bound source, definition semantics, specification,
> provider descriptors and bounds as on a primary. Admission validates the
> complete at-most-256 source set and each corresponding replicated control
> against the current completed-prefix catalog and history. A missing,
> malformed, foreign or mismatched control never authorizes a view. No follower
> action initializes, changes, repairs, acknowledges or allocates a durable
> columnar control or generation. Replicated control bytes remain exact.
>
> A follower's columnar query view is a separately validated, rebuildable local
> derivation. The source control's physical artifact selection, candidate,
> publication and physical failure state describe the source's artifacts; they
> do not select or certify a follower artifact. A source checksum must never be
> attached to different local bytes. The follower may materialize its view
> independently of source artifact readiness, using only a pinned completed
> follower snapshot and, where needed, a checked contiguous retained suffix.
> Its source, checked specification, database/history identity and exact applied
> frontier come from that immutable authority, never from an advertised source
> head, acknowledgement, file name or attempted control transition.
>
> Materialization reuses the existing bounded V2 construction, complete
> root/member validation and independent logical-equality checks. No V1 path,
> full-population shortcut around the existing resource bounds, unverified
> snapshot, partial generation, or new artifact-shipping protocol is admitted.
> Only the one server-owned worker may atomically install a completely validated
> local view and notify waiters. The installed view binds the existing process
> generation and an immutable local view identity; neither allocates replicated
> authority or substitutes for a durable control witness on a primary.
>
> Follower artifact files are disposable process-local build material beneath
> a distinct follower-owned area of the configured projections root. They use
> existing V2 formats and bounds and are never reopened as authority after
> restart, promoted into primary selection, or placed in the changelog. Rebuild
> after restart begins on demand from a new completed snapshot. Cleanup touches
> only bounded, exactly owned paths after all views and workers using them are
> drained; it never searches for an alternate generation or deletes primary
> selected files. No new persistent control store, durable identity, allocator,
> metadata key, or authoritative namespace is introduced.
>
> Cold/Activating/Active/Failed/Stopped ownership, no population work before
> demand, immediate typed Building responses, current policy and redaction,
> cancellation, worker shutdown, and all input/work/output/diagnostic limits
> retain ADR-0195's rules. Available and Bounded report the local validated
> frontier; Causal and AdmissionHead require it to satisfy the existing local
> applied-head floor and bounded register-before-read discipline. Foreign or
> pre-promotion tokens and obsolete process-bound cursors fail closed. Withdrawal
> of completed-prefix publication closes admission even while an older pin or
> local view remains alive. Local artifact failure stays rowless and typed;
> recovery may discard and rebuild local material without changing source
> control or claiming that an attempted rebuild succeeded.

## Required proof before WP-747 closure

- A follower with a source-selected V2 control and no source artifact files
  independently serves scalar and vector results equal to the primary at the
  same application frontier, then repeats after restart and source advancement.
- Cold startup/status performs no population work. Concurrent demand has one
  worker owner; failed/cancelled builds cannot publish partial data, and shutdown
  drains pins and local files before engine release.
- Missing/corrupt local material is never adopted, relabelled or used as a
  fallback; a validated rebuild uses current completed authority and retains
  all V2 bounds and independent logical validation.
- The exact `follower_causal_read_waits_for_token_then_matches_primary` proof
  holds replication below a new token, observes the registered wait, advances
  the follower, compares results and checks typed write refusal. Bounded,
  Available, AdmissionHead, stale/foreign tokens, revocation and cursor checks
  retain their required semantics.
- The full authoritative namespace oracle remains byte-exact after reads,
  local rebuild/restart/failure and refusals; no local control/generation write
  or source-artifact checksum substitution occurs. Primary selection and crash
  proofs remain unchanged and pass.

## Tradeoff and acceptance mechanics

Follower readiness is certified by a complete local derivation of applied
history, rather than by possession of the source-selected physical artifact.
This is an explicit guarantee amendment, not an implementation deviation.
Restart rebuild cost is accepted; durable reuse of follower views is deferred.

The maintainer's words/date and the exact normative text were recorded in
standalone commit `1d910c83`. WP-747 now names the affected requirements and
records and owns the required proofs. Its existing crate scope suffices; the
referenced ADRs are its own records under the scope rule. All seven amendment
acceptance checks passed, and the installed wording was compared with this exact
review text. Implementation follows acceptance; the audit exception is unchanged.
