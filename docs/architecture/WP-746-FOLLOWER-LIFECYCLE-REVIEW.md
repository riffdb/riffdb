# WP-746 follower lifecycle review

Status: accepted by the maintainer in session on 2026-09-14: “I approve of these changes”.
The exact amendment is committed in ADR-0186 as `96dcc300`. WP-746 remains open.
The earlier approval and commit `96968c96` concern allowed paths only.

## Conflict reproduced before the amendment

ADR-0178 section 2 makes the follower applier the sole writer and requires
strict source-frame sequence order. ADR-0186 section 4 requires an exact
bootstrap fence and its successor. However, ADR-0186 section 5 also says:

> Every `CLEAN` close and every `DIRTY` activation or clean consumption
> allocates one V3 sequence and writes its exactly attributed materialized
> control receipt in the same existing `Immediate` lifecycle transaction.

The existing startup owner calls
`SharedRedb::advance_dirty_lifecycle_before_activation` after validation.
This is not an optional call under the existing lifecycle decision:
ADR-0157's lifecycle-transition section requires an absent certificate to
become `DIRTY(1)` immediately before first operational activation, and an
existing DIRTY certificate to become its successor before activation.
That owner follows ADR-0186 exactly: it allocates a receipt even when the
retained follower state is attached. At upstream fence N, the follower's local
restart consumes N+1. The primary can independently assign N+1 to its next
command. The receiver must reject that command as a conflicting history.
Accepting it, skipping it, or renumbering it would violate the exact-prefix
guarantee.

`follower_restart_lifecycle_must_preserve_the_exact_upstream_successor` in
`crates/riffdb-storage-redb/src/store_changelog_lifecycle_tests.rs` reproduces
this using the actual lifecycle writer and an isolated attached-root fixture.
It originally failed with observed sequence 2 versus required sequence 1. This is not
evidence of an implemented bootstrap or follower activation path.

The catalog additionally classifies `clean_close_certificate/v1` as
`ReplicationControl(SourceOnly)`. Validated-prefix certificate/head rows are
`ReplicatedAuthoritative`, so a follower cannot locally refresh them into
different bytes while claiming the source's exact position.

## Accepted amendment to ADR-0186 section 5

The following is the complete text accepted by the maintainer:

> **Follower lifecycle isolation.** This amends ADR-0157's mandatory activation
> transition for attached followers. The lifecycle receipt-allocation rule
> applies to source-mode databases. An attached follower never allocates a
> source changelog sequence for its own startup, shutdown, validation,
> checkpointing, or acknowledgement. Only applying an upstream frame advances
> its lineage-shared V3 tail and allocator. A follower does not install or
> consume the source-only CLEAN/DIRTY lifecycle certificate. Its startup runs
> the existing authoritative and catalog validators, including validation of
> replicated validated-prefix evidence and complete fallback when that
> evidence is unusable. It does not locally rewrite replicated-authoritative
> validated-prefix certificates or head rows. Required rebuildable-local
> state is rebuilt and validated before readiness. All follower-local durable
> progress belongs to the sole applier and uses the already accepted follower
> metadata; this amendment adds no durable identity or encoding. Shutdown
> drains the applier's local durability boundary without a source lifecycle
> receipt. Primary lifecycle atomicity and final-CLEAN ordering are unchanged.

This preserves source receipt bytes, namespace classes, source write
acknowledgements, complete validation, and one authoritative writer. It changes
the scope of the accepted universal lifecycle-allocation sentence; it cannot
be recorded as merely an implementation choice under D-003.

## Required implementation and evidence after acceptance

- Refuse a primary-mode open of attached follower state until the separately
  authorized promotion transition exists; mode selection cannot detach it.
- Apply the follower distinction at the existing startup and shutdown owners,
  retaining the same structural/catalog proof join and complete validators.
- Make the regression above pass, retain the existing primary lifecycle and
  crash tests, and add follower restart/crash coverage at nonterminal source
  positions with distinct subsequent commands.
- Complete staged bootstrap, sole-writer apply, complete-authority comparison,
  transport/concurrency/crash proofs, handbook
  updates, full acceptance and the WP-746 closure. These remain unfinished.

## Review trigger

The supplied AGENTS.md requires human review when a required guarantee appears
impossible under an accepted ADR or a test reveals an authoritative conflict.
D-003 likewise requires review when changing accepted Decision text. The
maintainer's acceptance above satisfies that review; implementation is now
proceeding under the accepted amendment.

## Implementation checkpoint and PR note

- **Package:** WP-746, open; no closure or `completed_at` is claimed.
- **Tier:** guarantee.
- **Behavior added or changed:** the working tree implements the shared typed
  follower refusal, administrative replication permission and current-policy
  checks, bounded V3 source framing over published snapshots, and the native
  streaming RPC on confidential listeners. The transport uses the API-neutral
  service and existing policy owner. The service owns no storage dependency;
  the server adapts its bounded request to the storage-owned handshake. A
  canceled blocking read retains its stream-capacity permit until it finishes.
  Each connection seeds its frame chain from the requested receipt history
  hash; subsequent frames chain from the previous encoded frame checksum.
  The coarse internal terminal telemetry class remains StorageUnavailable for
  follower refusals; the public typed refusal is preserved.
- **Checks run:** scoped acceptance ran 3,026 tests: 3,025 passed, the lifecycle
  regression above failed, and eight existing tests were skipped. After fixing
  a Clippy module-order finding and regenerating dependent fixtures, final
  `./scripts/acceptance --wp WP-746 --range aa33c150..HEAD --no-tests` passed
  all 11 steps: scope, formatting, scoped Clippy, generated artifacts, version
  topology, handbook, workspace policy, dependency policy, unused dependencies,
  file-size guard, and panic allowances. The unchanged test suite was not
  repeated at that checkpoint. Full acceptance was red before the accepted
  lifecycle fix; the package exit gate and `ci-all` remain unsatisfied.
  Subsequent focused tests added five passing service authorization tests and
  one passing real-redb capability audit test; both test targets also pass
  Clippy. The authorization tests use the real authenticator/current-policy
  evaluator and explicit pending futures to prove revocation, expiry, missing
  capability state, repository failure, and clock failure withhold bytes after
  admission or frame waits. They also cover database/permission rejection
  before source access, canceled waits, and terminal frame/source failures.
  `replication_capability_create_revoke_and_retries_preserve_durable_audit_provenance`
  proves the explicit replication grant is issued by the existing bootstrap
  administrator, survives reopening, and preserves create/revoke initiator and
  sequence evidence without duplicate transitions on retries. This uses the
  real coordinator and redb; it is not yet an end-to-end transport proof.
  Four further passing tests in
  `crates/riffdb-server/src/replication_transport_tests.rs` exercise the
  production `HostedGrpc` listener and router, real credential authentication,
  current policy, and replication service over TCP/TLS. They prove cleartext
  refusal before authentication even with a proxy claim; exact handshake
  forwarding; typed terminal source refusals; malformed/oversized wire and
  credential rejection; frame withholding after revocation or lifecycle stop;
  and cancellation releasing a pending source. A test-only unchecked codec
  sends malformed input past the generated client's own validation guard.
  Frame custody is an injected bounded source, so these tests do not establish
  durable source publication, follower apply, or process-crash behavior.
- **Compatibility:** additive Protobuf service, permission, and error variants;
  generated bindings, schema inventories, application locks, and dependent
  conformance/portability fixtures were regenerated from their owners. No
  durable encoding was added by this checkpoint. Applications must regenerate
  strict error/enum bindings to recognize the new variants. Replication remains
  unavailable through MCP and cleartext listeners.
- **Handbook pages:** `docs/architecture/CHANGELOG-V3.md`, `docs/security.md`,
  `docs/reference/ERRORS.md`, `docs/known-limitations.md`, both WP-746 review
  pages, and their `docs/SUMMARY.md` links.
- **Hazards and follow-ups:** the lifecycle amendment is accepted. Bootstrap,
  follower activation/apply, complete-authority comparison, full durable-source
  and follower transport integration, real
  workload coverage, and the required follower process-crash matrix remain
  unfinished. The source work is uncommitted and is not a completed replication
  feature. The earlier path amendment is the separate commit `96968c96`.

## WP-747 through WP-750 dependencies

The active objective includes WP-746 through WP-750. Their current briefs
require merged hard dependencies: WP-747 and WP-749 depend on WP-746, WP-748
depends on WP-747, and WP-750 depends on both WP-748 and WP-749. All remain
open. Acceptance of the lifecycle amendment unblocks WP-746 implementation;
the source-side tests above do not justify starting a later package or
claiming its exit gate.

## Implementation after acceptance

`96dcc300` is the standalone acceptance commit. The lifecycle regression now
passes: attached-state activation preserves the exact history, allocator, and
durable commit epoch, installs no source certificate, and refuses conflicting
source certificate bytes without repairing them. The existing primary
DIRTY/CLEAN lifecycle and process-crash tests pass alongside it.

The current source-mode storage constructor refuses attached state before
locator installation, journal recovery, or preparation-worker creation. The
source graceful-close owner likewise refuses it before entering its barrier.
The validated-prefix write owner preserves source evidence on attached state.
The existing watermark crash test uses an attached restore fixture: source-mode
reopen refuses and the entire stored image stays unchanged. The destructive
restore crash test still proves that restore detaches the follower and permits
source-mode reopen.

A separate `RedbFollowerStore` now opens only an attached current-format engine
and runs the existing structural/catalog proof path. Its dormant bundle cannot
release source operational ports or a local index-migration writer. The follower
handoff rebuilds transient indexes and releases one non-cloneable
`RedbFollowerApplier`. The adapter applies one frame per hardened Immediate
transaction with exact pre-images and per-receipt physical frontier checks;
updates the shared V3 roots and existing follower applied receipt hash
atomically; and fuses the handle on failure. Exact terminal retries are
read-only. Local acknowledgement changes only the existing follower state;
shutdown writes no source lifecycle receipt.

The focused follower tests pass, including the original lifecycle regression,
repeated unchanged structural/catalog startup with and without a verified
validated-prefix checkpoint, rollback of a later pre-image failure, malformed
frame/lineage/epoch/sequence refusal, and process exits before and after frame
and acknowledgement durability. Their attached fixtures now have empty
source-only tables; they do not constitute a staged bootstrap or the app-baseline
exit gate. The complete redb library suite passed 446 tests after this change.

Implementation choice within ADR-0186: follower progress's existing canonical
receipt hash binds the complete last receipt and its predecessor hash chain.
Exact terminal retries compare against that durable commitment; followers do
not copy or materialize source-only receipt-history rows. Missing roots,
detached state with empty history, mismatched applied hashes, source holds, and
source lifecycle certificates fail closed. Source receipt ancestry validation
and all authoritative/catalog validation remain required.

The external bootstrap page/manifest codecs now enforce one held fence,
complete namespace/key order, explicit ends, page/count/byte bounds and chained
checksums. Their V1 fixtures and version-topology entry implement the external
receipt identity already named by ADR-0186. The source cursor cannot release a
manifest without its own exact end. This is transfer integrity evidence, not
proof of durable hold installation or staged publication.

Implementation choice within ADR-0186: external progress V1 binds the final
manifest, exact counts, namespace/prior key, and last page hash. The private
redb transfer database commits a page and its progress in one Immediate
transaction, avoiding separate page/receipt rename recovery. Reopening checks
one bounded last page; complete transfer verification still rereads every page.
Process tests exit before/after commit and prove atomic recovery and read-only
exact retry. Corrupt earlier pages and path substitutions are refused. This
transfer database is not a structurally validated follower database.

Source preparation now persists the complete artifact from one published pin,
then registers its exact durable bootstrap hold before exposing a manifest or
page. Completed artifacts survive source restarts; hold retries are read-only.
Incomplete, unreleased source builds cannot resume with a different pin and
fail closed. Dropping a source artifact retains the hold. Process tests cover
page/manifest completion and staged/receipted/committed hold registration.

Source construction and verification now advance one bounded page at a time.
Exact source EOF releases the snapshot pin before rereading the private artifact.
Cancellation releases non-durable owners; errors fuse the build. The async
source-job owner caps live jobs at four, checks a fifteen-minute deadline, and
keeps the capacity permit with each actual blocking storage step even after
waiter cancellation or timeout. Deterministic schedules prove resource release
before capacity reuse, and real-file tests prove bounded admission and exact
held-artifact reopen. Scratch cleanup and persistent inventory remain to be
composed with authenticated bootstrap, stream-wide timeout and hold release.

The source-side attachment transition now replaces one bootstrap hold with a
follower acknowledgement at the identical ID, lineage, position, hash and
frontiers in one existing source-control transaction. The retention floor does
not move. Exact retries are read-only; a conflicting follower hold is never
overwritten. The transition works at the existing hold-count ceiling because
it does not grow the final population. Old artifact registration is refused
while that follower hold exists. Process tests interrupt all insertion/removal/
receipt/commit boundaries repeatedly and reclaim between attempts: recovery
always retains one complete fence and every successor. These are source-side
mechanics; the authenticated RPC must still bind real receiver publication and
durable acknowledgement before invoking this transition. No generic hold
removal or new durable identity was added.

Implementation choice within ADR-0186: a separate private follower file stores
copy progress in the existing staged V1 receipt domain, atomically with each
inserted authoritative page. The temporary table is removed with exact follower
root installation. Unsealed resume reads a bounded last-page checkpoint;
completion and sealed resume compare the entire authority and transfer manifest.
The input type distinguishes complete durable progress from a complete reread,
so crash resume does not hide a full scan. Source receipt cursor admission is
unchanged; a private attached-state cursor reuses the authority iterator after
checking the exact attached root and empty source history.

The existing offline structural/catalog scrub now shares its unchanged validation
body between source and follower construction. New physical-file tests cover
copy/seal crashes, exact byte comparison, alternative valid page boundaries,
corrupt authority refusal, construction exclusion, and normal follower startup
with and without a copied validated-prefix checkpoint. These are construction
and validation proofs; complete derived-state orchestration and publication are not
implemented yet.

Remaining integration includes the authenticated bootstrap transfer and durable
attachment/hold-release protocol, follower publication, required durable derived-state
rebuild, daemon follower composition and
all write-surface refusals, the independent complete-authority oracle, and the
end-to-end repeated-kill workload. No package closure is claimed.

### Derived projection replay progress

The excluded private candidate now owns a derived-only replay port. It reuses
existing row/marker codecs, checked write sets and recovery readers. Each
Immediate transaction leaves source controls and frontiers byte-exact and
persists rows plus a local apply marker atomically. Identical retries are
read-only; mismatches fuse the owner until reopen. Storage has no production
dependency on the projection engine. Tests run the existing pure evaluator and
independent semantic validator over events, exercise both crash edges, and
reject well-formed but semantically incorrect derived rows. Full catalog-bound
orchestration, every retained generation, publication, and tail-driven derived
maintenance are still required; these isolated fixtures do not claim full
startup or app-baseline exit evidence.


### Catalog-bound worker progress

Scope was widened in standalone commit a74a9d50 before entering the catalog
crate. The catalog now resolves a pure projection replay plan from its existing
validated lineage, sharing the active-catalog resolver and checked group schema.
A private bootstrap evidence session retains construction exclusion, supplies
unchanged historical evidence, refuses structural completion and source ports,
and returns only its exact still-private candidate after historical EOF.

The server worker checks the proof's database/open-session binding, enumerates
all retained published and candidate generations, applies one commit per replay
step, independently validates each completed generation and requires exact
control EOF before the full scrub. Its sealed rebuilt result has a private
constructor. Cancellation is checked between bounded evidence/replay reads;
a cancelled or failed worker cannot finish, and reopen reruns catalog preflight.
The real-catalog test covers two projections with published and replacement
generations at BeforeFirst, cancellation/reopen, unchanged controls and final
scrub. Nonempty real-command composition and the full app-baseline exit gate
remain outstanding, alongside publication, RPC jobs, and live tail integration.


The real gRPC budget restart test now additionally bootstraps its stopped
nonempty database. It uses the original command graph (entities, events,
idempotent outcomes, provenance and audit links), deterministically catches the
source projection through both commands, builds a new candidate, reopens during
replay, and finishes the actual server worker plus complete follower scrub.
This closes the combined nonempty reconstruction check; the named app-baseline,
bootstrap/tail gap and repeated-process-kill exit evidence is still outstanding.

Receiver publication now uses same-filesystem hard links, keeping the private
candidate available for retries. The configured database is mode 0600 before
exposure. The new inode's actual engine lock spans marker removal, database
replacement, marker-last publication and parent sync. Existing targets are
classified without writable-open side effects using a read-only engine bound
to the retained descriptor on Linux. A primary, live writer, foreign database,
older same-lineage fence, or decreased incarnation/epoch is refused. Repeated
process kills cover each namespace durability boundary for fresh and existing
targets; missing markers withhold startup and every stage remains recoverable.
Other filesystems refuse linking; non-Linux platforms refuse replacement of a
nonempty target because that descriptor-bound read-only open is unavailable.

Follower open now independently pins its database/marker paths and passes the
retained descriptor through the existing redb backend seam. File and parent
syncs must succeed under engine exclusion before a store escapes. A publisher's
uncertain final directory sync cannot be inherited as acknowledgement authority.
Per-owner fault injection proves startup refusal and handle release on sync
failure; descriptor-substitution tests preserve unrelated replacement files.

The server's sealed rebuild result alone drives its production publication
path. After physical publication, it runs the same evidence driver and
structural/catalog/retained-metadata proof join as source startup, without
initialization, migration or source activation, and checks the exact manifest
history before releasing the sole applier. The real gRPC budget test now
publishes its nonempty reconstruction, passes this startup gate, persists its
local acknowledgement, performs source hold attachment, and applies the exact
successor control receipts. Authenticated transport/receiver jobs, daemon serving
composition, scratch cleanup and the full app-baseline/crash exit gate remain.


Receiver async custody now shares one construction slot across transfer,
materialization, semantic replay, full scrub, publication and final production
startup. Its 15-minute lifetime and cancellation guard never release the slot
while detached blocking work retains file/engine ownership. Cancellation is
checked between authority pages, structural/catalog evidence reads, projection
replay/validation and transient-index records. Final startup uses the same
validators and index rebuild functions; cancellation cannot grant a proof,
applier or acknowledgement. Non-interruptible engine/durability calls may finish
and leave durable private progress or a published target for exact recovery.

Tests use barriers and paused Tokio time to prove cancellation/deadline custody,
including a real page append cancelled after its durable commit. Resume sees
that exact page, changed retries fuse the handle, and early publication refuses.
The real gRPC budget test now resumes its interrupted reconstruction through the
async receiver owner and completes publication/startup/local acknowledgement and
source attachment. Bootstrap RPC integration, persistent scratch inventory and
cleanup, daemon serving, tail-derived maintenance and the full named
app-baseline/repeated-kill exit evidence remain open.


The existing StreamChangelog wire contract now includes mutually exclusive tail,
bootstrap begin/resume, and attachment requests, plus manifest/page response
items. No second RPC or durable encoding was introduced. The service owns typed
item custody, phase ordering, closed bounds, and current authority checks before
and after each wait. Public wire preflight rejects duplicate nested singular
fields before Prost can discard that evidence; unknown public fields retain
ADR-0006's ignored/not-relayed behavior. TLS tests with injected sources
cover phase/ack mapping, revocation before manifest/page release and the full
32 MiB + 512-byte page ceiling. Service tests cover all current-policy changes
around admission and release. These tests do not prove source hold registration
or receiver publication over the network. Production still refuses bootstrap and
attachment until bounded persistent source inventory/jobs are wired into its
source adapter; all prior daemon/crash-gate gaps remain open.
