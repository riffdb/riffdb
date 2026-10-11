# WP-825 private archive and promotion repair refusal

Package: WP-825 (open). Tier: guarantee, implementing accepted ADR-0285.

The common native builder refused repair for offline scrub and explicitly
guarded ordinary staged authorization. Its private archive and promotion modes
still used the recovering default. Archive conversion, command-prefix reconstruction after marker quarantine,
and private incarnation stamping also opened writable engines without repair
refusal. A successful later
validation cannot supply evidence that an earlier open did not repair its stage.

All private validation modes now abort an engine repair callback. Private
archive replay startup and projection preparation also select repair refusal
through a closed first-party follower constructor. Published follower crash
recovery retains its original native recovery policy. Descriptor
opens for archive conversion, command-prefix reconstruction and private stamping use the same closed native
repair-refusal policy as path-bound private opens. Each caller retains its
existing cache bound, transaction, native binding and complete validation.
An observed repair request returns redacted `CorruptData`, never a repaired
candidate or publication authority. Ordinary source and reconciled-promotion
recovery retain their existing policy.

Checks required on the 8-vCPU, 32-GiB Linux SSD host: five genuine native red
counterexamples for archive validation, promotion validation, archive
incarnation stamping, prefix construction and private follower preparation; their corrected tests; full archive, promotion, backup
and process-crash regressions; native Clippy, scope, formatting and acceptance.
Verify the actual terminal receipt and exact formatted source before committing.

Handbook: `docs/backup-restore.md` names repair refusal at these private opens.
Compatibility: unchanged durable formats, fixtures, command ordering, public
selectors, receipt authority, transaction boundaries and resource ceilings.
Immutable backup bytes and configured targets remain protected.

Hazards and follow-ups: this does not implement continuous custody or complete
the private factory. Source-bound materialization and retention-watermark opens
must also stay under the final owner. Retained complete proof, authorization
readers, final combined-source CI and the unchanged public recovery and
rehearsal gates remain required. No package closure or qualification is claimed.
