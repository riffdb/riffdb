# WP-749 — exact archive restore selection in maintenance receipts

Status: accepted 2026-09-15. The maintainer approved the exact text in session:
"Approve exact text". Standalone acceptance commit `bce0fa59` records it in SPEC
§4.10 and ADR-0050/0178. Receipt V3, archive restore admission and publication
remain implementation work; this acceptance does not expose the operation.

## Conflict demonstrated

ADR-0178 §7 and REP-007 require restoring a verified full backup plus an archived
suffix, optionally stopping at an earlier application commit sequence. Restore
must retain ADR-0050's audit, authorization, exact retry identity and destructive
publication ceremony.

ADR-0050's accepted retirement amendment explicitly says that create and restore
continue writing V1, and only retirement writes V2. V1's canonical input hash
contains only operation kind, backup name and replacement confirmation.
`OfflineMaintenanceReceiptV1::from_canonical_parts` recomputes that exact hash;
it rejects a hash that also binds an archive or stop sequence. Neither its
semantic fields nor its frozen encoding retain an archive selection.

The regression test
`legacy_restore_receipt_refuses_archive_aware_input_identities` demonstrates
that distinct archive/stop identities cannot be admitted as V1 restores.
Reusing the ordinary V1 identity would allow semantically different requests to
collide, and resolving a later archive `CURRENT` on recovery could publish more
history under an already accepted operation. Neither is an acceptable fallback.

Private full-backup conversion and exact follower-applier replay are implemented
and tested separately. Their result intentionally has no publication capability.

## Exact accepted amendment

The following qualifies ADR-0050's restore input, RPC inventory and V1-only writer rules and
completes ADR-0178 §7's archive restore ceremony. It does not change backup
manifest V1, V3 changelog bytes, archive-manifest/v1, or REP-007's required
application-commit recovery granularity.

> **Archive restore receipt V3.** Offline maintenance receipt format V3 is an
> additive external identity, written only for restore requests that select an
> archive. Ordinary full-backup create/restore continue writing their frozen V1
> encoding, and immutable-backup retirement continues writing its frozen V2
> encoding. Existing V1/V2 bytes, hashes and meanings remain unchanged. An older
> binary encountering V3 refuses the maintenance subtree before mutation.
>
> Archive restore remains the existing administrative restore operation and
> uses its capability, policy approval, destructive replacement confirmation,
> drain, staged authorization and post-publication validation requirements. It
> adds no MCP operation, principal-less restore, arbitrary path, URI or credential
> selector. A request names one configured archive using a distinct checked
> archive-name type with the same 1–64 ASCII-byte grammar as BackupNameV1. Server
> configuration resolves that name to the sink; applications cannot select or
> influence it. Credentials and sink locations are never stored in the receipt.
>
> Expose archive restore through one distinct additive AdminService RPC,
> RestoreArchivedBackup, and its typed Rust-client operation. Do not add optional
> archive fields to the existing RestoreOfflineBackup request: an older server
> could discard unknown fields and perform the wrong restore. An unavailable
> archive RPC must fail without falling back to ordinary restore. The CLI entry
> is `riffdb storage restore <backup-name> --archive <archive-name>`, with optional
> `--stop-at-sequence <sequence>` and the existing
> `--confirm-replace-current-database` confirmation. Existing CLI identities and
> ordinary restore request semantics remain unchanged.
>
> The archive restore request carries the checked backup name, checked archive
> name, replacement confirmation, and a closed stop choice: last archived, or
> one explicit application CommitSequence. Its canonical input identity binds
> all those fields and a distinct archive-restore input-domain version. Retries
> keep the same maintenance operation ID and exact semantic input; changing any
> field is an input mismatch. A fresh transport RequestId remains required.
>
> For an admitted ordinary restore, before the first replay mutation the offline
> owner verifies the full backup and complete selected archive prefix, then
> durably freezes their selection in the V3 receipt. Selection includes the exact full-backup manifest identity,
> original V3 lineage and backup fence, and the exact selected terminal
> archive-manifest/v1 bytes, or a closed empty-suffix designation. The manifest
> chain binds every frame from that backup fence through that terminal. Empty
> suffix is valid only at the verified backup fence. A last-archived request
> resolves at this selection step; an explicit stop must be within the verified
> backup/selected-suffix interval. Selection is immutable once recorded.
>
> In ordinary maintenance admission, the existing receipt is durable before
> returning Accepted; it may lack resolved artifact selection until the offline
> verification step. Recovery after selection must reuse that exact selection,
> even if the archive later advances. Recovery before selection may perform the
> first verified selection, because no replay or publication was permitted yet.
> The existing source-less recovery route may verify, replay and authorize a
> private candidate before admission. Its selection remains fixed throughout
> that attempt, and admission must durably record that exact selection before
> publication. A crash before admission grants no accepted-operation or publish
> authority. Private artifacts never substitute for receipt authority.
>
> Missing, conflicting, foreign or corrupt selected evidence refuses before
> target replacement. Neither a directory listing nor a later CURRENT may
> replace recorded selection. Replay uses the existing follower applier and
> exact original receipt semantics. The requested application stop must be
> satisfied exactly: no rounding to a frame/group boundary and no claim that a
> partial group has the source receipt's identity or checksum. This amendment
> grants no exception to those replay or granularity requirements.
>
> After exact replay and complete validation, V3 records the actual restored
> application/administration frontier before publication. Staged authorization
> reads the restored candidate under the existing authentication and current
> policy rules, without originating follower authority. After those readers are
> closed, publication uses the existing durable incarnation decision,
> max(target incarnation, staged incarnation) + 1, and RestoreAnchor. The exact
> selected request and restored frontier remain bound to that decision on every
> retry. No success is reported until the existing publication and subsequent
> full startup validation gates succeed. Archive-aware results report the actual
> restored frontier separately from the original full-backup frontier; neither
> may be relabeled as the other.
>
> V3 retains the existing bounded receipt size, checksum, at-most-16 monotonic
> transitions, phase ordering, safe failure classes, atomic replacement and
> parent-sync uncertainty rules. Selection and frontier fields have closed,
> canonical presence rules and bounded sizes. Unknown versions, impossible
> combinations, changed immutable fields and regressing evidence fail closed.
> Status and audit output remain bounded and redacted; they do not expose frame
> payloads, keys, credentials, sink paths or raw receipt/manifest bodies.
>
> Register V3 in the existing maintenance-receipt family, receipt union and
> durable-format topology, with additive compatibility fixtures. V1/V2 fixtures
> stay byte-exact. Implement the admitted archive restore CLI through the same
> API-neutral service and administrative transport path; no CLI-local redb open
> or filesystem bypass is introduced.

## Proof required before package closure

- V1/V2 golden bytes and ordinary create/restore/retire behavior remain exact;
  V3 has canonical fixtures and old/unknown-reader fail-closed evidence.
- Each archive request field affects retry identity; selection and restored
  frontier cannot change once frozen, including after uncertain parent sync.
- Crashes before/after selection, replay, staged authorization, incarnation
  recording, stamping and publication preserve one exact request and result.
- Advancing CURRENT cannot change an already selected restore. Missing or
  corrupt selected prefix, foreign backup/lineage and out-of-range stops refuse.
- Full replicated-authority comparison, exact earlier application-sequence
  stopping, ordinary post-restore writes, and the named WP-749 end-to-end proof
  pass through the real service/CLI and publication paths.
- Administrative denial, staged reauthorization, secret redaction, required
  replacement confirmation and source-less recovery retain their existing gates.
- An older server lacking RestoreArchivedBackup causes a typed unsupported
  failure without issuing an ordinary restore or creating a maintenance receipt.

## Acceptance mechanics

The maintainer's words/date and exact normative text are recorded in standalone
acceptance commit `bce0fa59`, with SPEC/work-package references updated. All seven
amendment acceptance checks passed. Implement and register V3 afterward. WP-749
remains open until all deliverables and the full acceptance/CI gates pass.

## Internal maintenance driver increment

Package: WP-749. Tier: guarantee, under the accepted receipt and exact-stop
amendments above. This increment does not close the package.

### Behavior and decisions

- Operator TOML binds up to 16 checked archive names per database to bounded,
  absolute, disjoint directories and an explicit external encryption posture.
  Requests cannot supply a filesystem path. Restore refuses a missing archive
  directory or ownership lock instead of initializing an empty sink. These
  bindings do not start a worker.
- The exclusive maintenance driver recognizes V3 without changing V1/V2 routing.
  It persists drain/offline phases and freezes verified backup/archive selection
  before replay. The complete exact-stop preparation owner provides the snapshot
  for real capability authentication and current policy authorization.
- The actual dual frontier and checked `max(target, staged) + 1` incarnation are
  durable before the existing stage stamp and publication. Fresh startup and
  equality with the retained published authority precede a terminal success.
- Before a publication phase is durable, credential-free reconciliation requires
  byte-identical stage/target files, the exact receipt-bound RestoreAnchor, a
  current marker and the fresh empty journal. The check holds a staged read-only
  engine and a target file lock; it does not repair or replay either artifact.
- A process crash during final startup dirties redb bytes and can retain an empty
  `DirtyActivation` receipt. After durable publication, validation therefore uses
  a bounded-row comparison of the complete catalog-defined authoritative state
  against the retained published stage. Lineage, anchor and dual frontier must
  match; an exact receipt cursor permits only empty `DirtyActivation` successors.
  Other source receipts, even at the same application/audit frontier, refuse.
  This follows the existing startup lifecycle; no new receipt identity or format
  is introduced and unpublished stages cannot be rebuilt without credentials.

### Checks and documentation

The driver fixture uses a real capability grant that exists only in the archived
suffix. Checks cover drain/offline persistence, staged authorization, required
approval refusal, unknown archive and missing credential refusal, all three
published recovery phases without archive access, and process abort during fresh
validation. A separate test rejects a non-startup receipt after publication.
Storage reconciliation tests change or remove target marker/journal bytes;
configuration tests cover duplicate names, explicit encryption, bounded counts,
relative paths and ownership overlap across databases. Final command results
are recorded in the commit note and handoff.

Handbook pages updated: `docs/configuration.md`, `docs/backup-restore.md`.

### Compatibility and remaining work

V1/V2 receipt bytes, archive selection identities, public command surfaces and
full-backup checksums remain unchanged. Normal startup still owns DIRTY activation.
V3 public admission, daemon startup routing, source-less recovery, archive worker,
RPC/client/CLI, complete crash qualification and write-path measurements remain
open. An unfinished V3 receipt still refuses automatic daemon startup pending
that routing integration. This increment alone does not expose archive restore.
