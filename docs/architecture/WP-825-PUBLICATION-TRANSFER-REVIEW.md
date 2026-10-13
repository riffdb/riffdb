# WP-825: sealed publication ownership transfer proposal

Status: exact ADR-0294 accepted in session on 2026-10-12; the intent codec is under focused qualification and transfer activation remains
pending. WP-825 stays open. Neither the
8-hour rehearsal nor the 24-hour run has started.

## Measured problem

The accepted ADR-0293 implementation still fails the unchanged primary-loss
recovery gate. Its saved populated failure reaches restore success at
57.949 seconds and first write at 59.867 seconds, then times out replaying one
original acknowledged identity. Full state reconciliation is not reached.
Two subsequent optimization candidates fail the whole gate and are removed.
Their focused successes do not qualify recovery.

A separate diagnostic uses the ordinary frozen native implementation and the
same original population. It completes first write at 59.539 seconds. Exact
first-write replay takes 0.114 seconds, original acknowledged replay 0.115
seconds, and all 65,536 ticket checks 2.582 seconds. The total is 62.350 seconds.
The diagnostic grants a separate larger observation budget and explicitly keeps
`whole_backup_restore_fault_proved` false. The official 60-second gate remains
failed. The campaign's fixed 45-second fault window remains unqualified.

Private profiles attribute approximately 11.918 seconds to complete predecessor
validation and approximately nine seconds to publication, including work that
this proposal retains. No saving is claimed for all nine publication seconds.
Fixed live ticket count does not bound history or final scheduled backup size.

## Concrete change to review

[ADR-0294](../../adr/0294-native-restore-transfers-one-sealed-artifact-set-into-publication.md)
proposes a bounded native publication intent and ownership transfer only for an
ordinary self-contained restore into an empty replacement on the same filesystem.

The intent binds the operation, original manifest, database, chosen incarnation,
restored frontiers and the exact lengths and hashes of the three final artifacts.
It is synchronized before moving any artifact. A full content read verifies the
transferred files against those original seals before marker-last publication.
The existing complete preparation, history checks, authorization, final sealing,
original-backup revalidation and fresh published startup remain required.

Restart reconstructs ownership from the intent and actual artifact locations;
it cannot regenerate seals from unknown bytes. A separate durable handoff marks
when native startup may change the primary. A nonterminal restart after that
handoff performs complete validation of the actual published target. It never
rebuilds over that target from the old backup. Terminal cleanup completes before
application admission, preserving acknowledged writes.

This explicitly amends the copying and restart rules in ADR-0289, ADR-0290 and
ADR-0293, including ADR-0293's retained full stage and second content copy.
That is a guarantee change, even though public restore commands stay unchanged.
The proposed intent belongs to the existing closed receipts directory, where
prior readers reject unknown suffixes before removing anything. Genuine prior
reader tests and separate human fixture review must freeze that compatibility
boundary. Other restore states retain their current copy protocol.

The preceding WP-825 scope commit registers this ADR path separately. Exact
acceptance now adds ADR-0294 to the package requirements and authorizes its native
implementation. The private intent codec is being qualified before transfer activation; no
new durable fixture has been accepted yet.

## Acceptance sequence after review

1. Freeze the intent encoding and submit its concrete fixture diff for review.
2. Identify the failure first, then implement ownership and restart transitions
   with deterministic mutation/refusal tests and process-crash cuts.
3. Build the ordinary daemon on the required host and run the saved populated
   60-second gate first, with exact replay and full state checks unchanged.
4. Qualify the actual scheduled-fault population and every fixed fault window.
   A focused pass does not qualify the 45-second campaign window.
5. Run scoped acceptance and final-source full CI before measured rehearsals.

## Completed independent repairs

The controller's original sixteen maintenance polls now span its original
remaining deadline rather than exhausting at approximately 32 seconds. The
actual-action regression fails first, then both scheduling tests, six protocol
tests and all 367 harness tests pass. No poll, retry, worker or deadline increases.

Native deferred-history admission now requires all three command locator tables.
The regression preserves the complete path's `IncompatibleFormat` refusal for
an unsupported current image; all seven readiness and 35 owned compatibility
tests pass. The unused native receipt import is removed. The handbook documents
this admission boundary. These repairs do not claim timing qualification.

Authorized temporary cleanup recovers approximately 145 GiB and observes roughly
157 GiB free, above the unchanged 134-GiB prerequisite. Historical observations,
the original reproduction fixture and the latest failures remain intact.
Main is fetched and merged again without additional changes.

## Accepted record and fixtures

AGENTS.md requires a full guarantee record whose exact text a human accepts
before merge. ADR-0293 Decision 10 expressly preserves the retained stage and
publication copying and says it does not replace them with links or renames.
ADR-0294 supplies that separate authority; its exact text was accepted in session
on 2026-10-12 and recorded in a separate acceptance commit. The maintainer
separately approved the exact four-file fixture diff linked from
[its review note](WP-825-ADR-0294-FIXTURE-REVIEW.md).

Exact proposed ADR-0294 text SHA-256:
`1cb928dca0d928fe9bb11faf2fd0ec1d5576a747925afd69f5501413f60070d3`.

## Native ownership checkpoint

The real receipt-authorized ordinary publication regression reproduces the
remaining copy: the configured primary has a different inode from the sealed
stage. The first attempt had a missing test import; it proves no native behavior.
The corrected native test reaches the inode assertion and fails there.

On the required host, six original-artifact tests and 24 native custody tests
pass, together with storage Clippy for every target and feature. The new handoff
consumes the actual unique closed owner and retains its original descriptors and
native exclusion. Independent child processes observe both native and legacy
locks across cross-directory moves and final rename. Live or retained engine
leases, cancellation, writable hard-link aliases and directory substitution
refuse. The source directory pin remains checked after moving its primary.

Three-file tests preserve every original inode, byte seal and immutable backup.
They exercise same-length mutations with restored modification times on each
artifact before handoff, during the known rename span and after complete readback.
Even deliberately refreshed rename metadata cannot replace the original SHA-256
seal. All three full readbacks precede any final artifact publication; the marker
moves last. Failure retains transferred files. A safe API from the existing
rustix dependency provides atomic non-overwriting rename through retained
capabilities; no dependency or unsafe first-party code is added.

Verified ownership checkpoint receipt SHA-256:
`0d2f07a8b8eabf8d52cc9c5c6b5d7221cbc699142d1c322f2b49d2e4f5eab657`.

These helpers remain test-only while durable intent, complete restart recovery,
startup consumption and success cleanup are connected. They do not enable
production transfer or prove end-to-end crash recovery. The native publication
regression, saved populated 60-second gate, actual scheduled fault windows,
final-source CI and both campaigns remain outstanding. WP-825 stays open.

## Durable intent and actual transfer checkpoint

Six intent-store tests pass on the required host, including 15 actual child
process aborts at file-sync, rename and parent-sync boundaries across all three
states. Replacement requires the same receipt, original operation and seals;
unknown or conflicting inventories refuse before ordinary temporary cleanup.
A retained original record descriptor and its exact prior bytes are checked
before replacement. The initial record uses atomic non-overwriting rename.
These synthetic intent-store cases certify record durability and binding only.

A separate real native restore test passes through fresh authorization evidence
release, its actual Offline receipt, the chosen incarnation stamp, observed
close, durable Prepared intent, three original-file transfers, full readback,
marker-last publication and durable Published intent. Every original inode and
byte seal is preserved; the immutable backup is unchanged. Publication facts
come from bounded root/retention reads on the same stamped engine before its
observed close. No extra history walk or reopened engine constructs those facts.

The combined checkpoint passes one actual native transfer test, six artifact
checks, 24 custody tests, six intent-store tests including the 15 abort cuts,
five codec tests and all-target/all-feature storage Clippy. Independently
verified receipt SHA-256:
`7956c41efc9b5364cf5661e1284f9055d62e0ed09075a2563491a26577acb29b`.

The complete mechanism is still test-only and uses a private qualification
entry point. The ordinary shipping restore API retains copying. Restart from
partial transfers, StartupConsumed before engine open, complete dirty recovery,
Succeeded cleanup before admission, and their full native process-crash coverage
remain required before activation. This checkpoint proves neither application
readiness nor a recovery timing gate, rehearsal or final-source CI.

Follow-up qualification strengthens the real transfer test to decode the durable
Published record and compare every seal and length with the original artifacts.
It also proves the ordinary receipt remains Offline; an intent cannot certify
readiness. All 219 maintenance unit regressions and all 35 owned materialization
compatibility checks pass, with storage Clippy unchanged.

The diagnostic checkout's path check initially refuses because its Git history
predates the separately committed ADR-0294 path extension. An isolated checkout
of the current branch, with exactly the qualified source bytes, passes the path
check, package brief, acceptance dry plan, handbook source inventory and whitespace
check. No path rule is weakened. Full acceptance and final-source CI remain open.

## Interrupted transfer recovery checkpoint

Qualification first reproduces two cleanup defects: a complete conflicting
intent temporary was discarded, and copy-only reconciliation removed a primary
left at its intent-owned target temporary. Both now refuse before cleanup. A
third retained-owner regression proves that a duplicate or foreign stage entry
introduced after the initial check also refuses before receipt cleanup.
Complete temporary records must decode and bind as the exact monotone successor;
unknown versions, changed seals, foreign operations and impossible successors
preserve evidence. Positively owned torn writes carry no publication authority.

Cold recovery takes a fresh maintenance owner and native exclusion over the
actual primary, then verifies exactly one permitted location for each original
artifact, the complete immutable backup and every original content seal. It
resumes the recorded moves with original inodes and incarnation, without opening
an engine or minting readiness. All three full readbacks precede any remaining
final rename; the format marker remains last. Consumed records cannot resume
publication or demand obsolete primary content seals.

The required host passes 43 actual process-abort cuts: 19 move/readback boundaries
on fresh transfer, the same 19 on cold continuation, and five Published-intent
file-sync/rename/directory-sync boundaries on actual native artifacts. Each case
recovers the same operation, all original inodes and seals, and incarnation 2;
the ordinary receipt remains Offline. Another 38 returned failures prove typed
publication uncertainty and same-operation continuation. Missing, changed,
duplicated, linked or foreign artifacts refuse before cleanup. Six artifact,
24 custody, ten intent-store and five codec regressions pass, together with
all-target/all-feature storage Clippy. Independently verified receipt SHA-256:
`831e949b8ab1507471b398cc1c73f6e9c1e7dd1f0df36b879fe81dd9bfc93bef`.

Native recovery remains test-only. The ordinary copy-only reconciler is fenced
from intent-owned transfers during qualification. Production activation still
requires StartupConsumed before engine open, complete restarted validation,
Succeeded cleanup before admission and their process-crash tests. No recovery
timing gate, final-source CI or measured campaign is claimed by this checkpoint.


## Consumed startup crash conflict and proposed correction

ADR-0294's nonrepairing consumed restart refuses a legitimate nested crash.
The actual process-abort regression commits startup's dirty-generation record,
retains the live native engine while the receipt is Validating, and aborts.
Cold artifact ownership checks succeed and the published primary keeps its inode,
but the native nonrepairing reopen returns CorruptData through the repair callback.
Verified failing receipt SHA-256:
`1f8497c3ae8d28ae0744999b0284b76818169f3e1ead7071f39106ea2c1ab45b`.

A separate diagnostic performs the same cut, uses ordinary engine crash recovery
on the actual target, and completes current structural/catalog validation with
exact database, incarnation and application-frontier checks. The immutable backup
is unchanged and the ordinary receipt remains Validating. Verified diagnostic
receipt SHA-256:
`eb1ec0c74fa89ce62ac339cb7f2f09b4b515bca56edaa2ce9287b07b55373f65`.
This diagnostic does not change the native restore policy or qualify its failure.

[Accepted ADR-0295](../../adr/0295-consumed-native-restore-uses-ordinary-engine-crash-recovery.md)
amends only the previously durable StartupConsumed restart: ordinary engine crash
recovery must precede complete current validation. Fresh publication and private
opens remain nonrepairing. All publication ownership, immutable-backup, admission,
format, resource and timing guarantees remain. The maintainer accepted the exact record in session on
2026-10-13 (proposed text SHA-256
`bc94b55348da24df345515b5f11e5a5de5818a525ae41928e21577e55957f6a6`). No harness
workaround, successful timing claim or campaign readiness follows from these tests.


## Explicit successful cleanup checkpoint

A focused actual native startup test first reproduces the missing cleanup join:
validation finishes and the ordinary Succeeded receipt is durable, but copy-only
restart still refuses the remaining consumed intent. The new explicit cleanup
helper removes only the matching consumed record after checking the whole closed
receipt inventory, terminal success, retained original record descriptor, exact
canonical bytes and unchanged receipt. It synchronizes the receipts directory
before returning, including an idempotent retry after a previous unlink.

The required host passes eight actual process-abort cuts across the Succeeded
receipt and intent removal boundaries. Before terminal success, cleanup refuses
and preserves the target and observations. After success, cleanup completes and
ordinary native recovery plus complete validation preserves the same actual
primary inode, database, incarnation and application frontier; the immutable
backup remains unchanged. Three returned cleanup failures prove typed uncertainty,
repeatable directory synchronization and no changes to a still-live validated
engine's artifacts. Nonterminal, foreign-operation and unknown inventories refuse.
Ten existing intent-store regressions and storage Clippy also pass. Independently
verified receipt SHA-256:
`488af4ab99c2529eb6a3852146496f7522da0197ea65f4c358f437981136bdce`.

The helper and native startup protocol remain test-only. The success test calls
cleanup explicitly before ordinary reconciliation; production automatic cleanup,
server admission wiring and first-acknowledgement crash coverage remain open.
The accepted ADR-0295 correction now passes the original dirty-generation
crash through the native entry point with complete current validation. All nine
native startup tests, seven existing readiness regressions and storage Clippy
pass. Verified receipt SHA-256:
`83e584dfa6e0ac8e7238f2e7fa49c60197548b6b1d934e16e4a4f313ff7c8309`.
Freshly published targets and private preparation still use nonrepairing opens.
Neither the diagnostic nor this cleanup checkpoint qualifies a timing gate,
final-source CI, rehearsal or production activation.


## Local main integration on 2026-10-13

Local main includes completed WP-833 and WP-834 beyond the remote-tracking branch.
Its validation walk remembers one successfully validated exact stored bundle and
one successfully decoded exact retained segment; its command history, entity
chains and historical commit plans share one ordered pass. The merge keeps those
implementations and their semantic/refusal tests. WP-825's redundant per-page
catalog cache is replaced by the accepted walk memo; its invalid-command,
changed-bundle and eviction assertions remain. The terminal single-decode census,
restore ownership, full consumed recovery and admission fences remain intact.

Main owns ADR-0286 and ADR-0287. The accepted follower record moves to
[ADR-0296](../../adr/0296-bounded-native-follower-acknowledgement-validation.md);
its Decision section is byte-for-byte unchanged and its original acceptance is
preserved. [ADR-0288](../../adr/0288-bounded-native-bootstrap-source-custody-validation.md)
updates only that dependency/context identifier; its Decision is also unchanged.
WP-825's references and allowed paths follow the moves. The unaccepted direct
mutation handoff proposal moves to ADR-0297 and remains unimplemented. Main's
accepted validation decisions and completed packages retain their own identifiers.

The maintainer approved these accepted-record identifier/reference edits in
session on 2026-10-13: "Approve the identifier-only reconciliation". The original
follower acceptance remains unchanged. Required-host qualification passes main's
three validation-reuse tests, WP-825's invalid-schema and terminal decode checks,
public native transfer, complete dirty recovery and automatic success cleanup,
the all-target server build check and storage Clippy. Verified receipt SHA-256:
`497f4e3e05cfc6a23afcc029fa83a0a4b5940ce059107ba9c68bfd010197cb59`.
Final-source CI and timed restore qualification remain open. No ordering, durability, authorization,
format, workload, resource ceiling or timing gate changes are proposed.


## Shipping restore and server admission checkpoint

The ordinary shipping restore API now selects native transfer for eligible
self-contained empty-target Linux restores on the same filesystem. Other states
retain copying, selected before any intent is written. Cold reconciliation
resumes the original artifacts and reports actual-target ownership separately
from copied-stage semantic evidence. Previously consumed startup performs the
accepted engine recovery and complete validation. Server admission follows the
durable Succeeded receipt and matching intent removal plus directory sync.

An actual server test exposed an early engine open in promotion discovery. That
optional probe opened and closed the newly published primary before consumption,
changing sealed native bytes and causing the following readiness check to refuse.
Native intent inventory now defers that probe without opening an engine. Complete
closed receipt validation and the existing promotion fence still reject pending
or unmatched promotion evidence; owned restore startup independently rejects
promotion or follower state before admission. Copied/promoted paths retain their
existing discovery behavior. No checksum, authorization or semantic check is
removed to resolve the failure.

The unchanged failing server test now passes through native transfer with the
original primary inode, incarnation 8, durable success and completed intent
cleanup before returning startup. All three cleanup uncertainty hooks return
typed CommitStatusUnknown and cannot return application startup. A separate test
proves promotion discovery preserves all three original seals and the Published
intent before actual startup consumes it. Sixteen server promotion regressions,
server Clippy, 168 startup tests and 248 maintenance tests pass on the required
host after the local-main merge. The startup suite's one pre-existing ignored
case is the large synthetic shutdown-checkpoint timing benchmark, not a skipped
correctness failure. Verified receipt SHA-256:
`0667d697e0cfc6afb66387c3430781141f0274074dae2c725b737479a1783c08`.

Package: WP-825. Tier: guarantee. Compatibility: frozen native intent and all
existing database, journal, registry and public interfaces remain unchanged.
Updated handbook: `docs/backup-restore.md`. Hazards/follow-ups: final-source CI,
the unchanged populated 60-second check, fixed campaign windows, post-acknowledgment
crash coverage and both measured campaigns remain required. This checkpoint is
not a performance or rehearsal qualification.

## Saved populated gate and acknowledged-write crash check

The unchanged saved primary-loss check passes on ordinary shipping binaries at
`aacaaf5469458727dacbb9b86a3c4d46d919dcbc`, including local main's validated bundle
reuse and fused history walk. All four binaries were rebuilt together from a
fresh, clean committed checkout; its 5,197 tracked source hashes were verified
before and after the build. This establishes the focused regression result,
without claiming final-source full CI or a complete campaign.

The selected 2,823,103,187-byte backup completed before primary loss. Backup
creation took 17.55 seconds, reported separately. There were 150,000 acknowledged
commands before loss and none between backup and loss. Recovery admission took
5.24 seconds from loss, restore succeeded at 52.73 seconds and the first public
compiled command was acknowledged at 54.59 seconds. Exact replay of that command
and the original acknowledged command, plus independent reconciliation of all
65,536 tickets, completed at 58.07 seconds inside the unchanged 60-second clock.
The original saved fixture and all four binary hashes remained unchanged.
Verified receipt SHA-256:
`571ffe7ebe77241aa5a72bfb93fad9d5bc6aff4a124b101692e69f44932783c8`.

A separate copy of the restored target accepted another compiled command and
then suffered an immediate owned-process kill. Ordinary restart preserved the
actual target inode, the new acknowledgement, both earlier acknowledged command
identities and their outcomes and provenance. All three exact replays and the
full 65,536-ticket oracle passed with 150,002 updates. Backup and maintenance
receipt bytes remained unchanged; no intent or original-backup republication
was needed. Verified receipt SHA-256:
`38ec59b20612c433de3126be9b40ca755092cae7aa06bcfff440e751aeccadbe`.

The fixed 45-second campaign fault window remains unqualified and is a timing
blocker: the saved first-write result exceeds it. The fixture has 1,500 seconds
of offered work, not the scheduled 5,400-second campaign population. Further
local-main CI fixes are integrated separately; campaign-clock integration,
final-source acceptance and full CI, the scheduled-population checks and both
measured campaigns remain required. Neither eight-hour nor 24-hour run has
started. Existing failed attempts remain failures and are preserved separately.
