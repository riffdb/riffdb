# WP-825 complete logical evidence retention

Package: WP-825 (open). Tier: guarantee, implementing accepted ADR-0285.

The existing complete offline validator discarded its immutable read and
catalog proof, then opened a replacement read for its final continuity check.
The owned restore lifecycle needs those original proofs retained together.
This change makes complete logical validation return one move-only native
evidence owner holding the original read, engine owner, mutation lease,
frontier metadata, database/session identities and validated catalog history.
It checks continuity on the retained read and rejects a changed durable epoch,
cancellation, substituted catalog, incomplete ends or a startup shortcut.

The existing offline receipt path consumes and releases that evidence before
its existing physical-copy owner performs final checks. No application can
construct or obtain this internal evidence, and it grants no operational,
readiness, checkpoint, authorization or publication capability. It is logical
evidence only: physical validation, verified materialization and uninterrupted
artifact custody remain separate requirements.

Checks required on C3D: two genuine original completion-boundary failures;
the corrected boundary and native lifetime, mutation, cancellation, exact-end
and catalog-session checks; existing physical integrity and compatibility
checks; native Clippy, formatting, scope and scoped acceptance. Verify the
terminal receipt and exact formatted source before committing.

Compatibility: unchanged public interfaces, receipts, durable formats,
validators, source/copy isolation, ordering and resource bounds. No new decoded
population cache, overlapping validation pin or validation worker is added.

Handbook: no public or operator behavior changes in this internal evidence
plumbing; ordinary offline scrub still validates its owned copy completely.

Hazards and follow-ups: this is one native ownership seam, not the complete
ADR-0285 factory. It does not remove the second restore verification copy or
replace staged startup with authorization readers. Whole-action recovery and
the actual rehearsal remain unqualified; no package closure is claimed.
