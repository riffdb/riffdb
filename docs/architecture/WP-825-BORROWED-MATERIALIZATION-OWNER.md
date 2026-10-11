# WP-825 retained native materialization owner

Package: WP-825 (open). Tier: guarantee, under accepted ADR-0285.

Private restore previously closed its first engine before hydration and then
reopened the stage through ordinary recovering watermark helpers. The candidate
retains the first repair-refusing native engine through both existing operations.
Hydration and watermark preparation borrow this engine. The ordinary offline
restore path retains its original open/close order. Watermark transactions,
immediate durability, two-phase commit, receipt validation, fencing minima and
tombstone-chain truth use the same extracted native implementations.

A deterministic channel barrier attempts a real competing native engine open
before hydration and before watermark preparation. Both must refuse with native
DatabaseAlreadyOpen. The baseline has the first-open repair correction but
releases its engine before these boundaries; it must fail this regression.
The test always releases the barrier and joins the restore before assertions.
A real V3 fixture additionally proves borrowed watermark handling preserves
unpruned absence, refuses a fabricated chain root, retains engine exclusion,
receipts the transition and makes an identical retry read-only.

Checks are pending in the combined required-host validation. The combined bound receipt must show the actual
baseline failure, corrected native tests, backup/V3 and archive regressions,
process recovery, Clippy, scope and formatting before integration. Final
combined scoped acceptance and full CI remain required.

Handbook: docs/backup-restore.md explains the materialization ownership interval.
Compatibility: no durable, public, fixture, dependency or mutation-order change.
Hazards and follow-ups: this retains ownership only during materialization; it
does not implement continuous custody through validation and publication,
complete native proof, authorization reader, engine close observation, or removal
of the extra verification copy. Original recovery deadline and campaign gates
remain unqualified. The preceding first-open change also needs its own receipt.
