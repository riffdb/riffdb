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

## Accepted record and remaining fixture review

AGENTS.md requires a full guarantee record whose exact text a human accepts
before merge. ADR-0293 Decision 10 expressly preserves the retained stage and
publication copying and says it does not replace them with links or renames.
ADR-0294 supplies that separate authority; its exact text was accepted in session
on 2026-10-12 and recorded in a separate acceptance commit. Fixture review
remains separately required before merge.

Exact proposed ADR-0294 text SHA-256:
`1cb928dca0d928fe9bb11faf2fd0ec1d5576a747925afd69f5501413f60070d3`.
