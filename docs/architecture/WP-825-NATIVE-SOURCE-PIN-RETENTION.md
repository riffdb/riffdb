# WP-825 native retention source-pin correction

Package: WP-825 (open). Tier: guarantee, implementing accepted ADR-0278 and
ADR-0279 without changing their guarantees or accepted text.

Journaled source reclamation captures and validates an immutable composite
source. Receipt-floor planning accepted only its checkpoint component through
an implicit dereference, while checkpoint licensing also resolved its exact
licensed point from that component. An acknowledged suffix can contain a valid
receipt absent from the older checkpoint. A later independently installed
checkpoint can also license a point beyond an older candidate's own tail.

The correction resolves journaled planning and licensing receipt points on the
same validated source pin. Planning stays within that captured history tail;
licensing yields when its fixed ceiling is outside the candidate's coverage.
Missing receipts inside claimed coverage still return `CorruptData`. The
existing independently durable checkpoint observations, exact point hashes,
lineage, qualification, current holds and writer admission remain required.

The native regressions use actual ordinary journal acknowledgements, retained
composite pins and explicit independently durable checkpoints. They cover a
licensed receipt in the captured suffix, a candidate older than the licensed
point, and a missing receipt inside claimed coverage. The original source must
fail the intended regressions before the corrected source is tested.

Checks on C3D: failing native baselines, the covered-hole corruption control,
corrected floor and finite-license tests, native Clippy, formatting, allowed
paths and scoped acceptance. Verify the bound terminal receipt before committing.

Handbook: `docs/configuration.md` explains captured-source coverage and refusal.
Compatibility: no application interface, ordering, writer policy, durable
encoding, receipt bound, checkpoint schedule or retention fence changes. Each
source cut still requires the existing native proof and sole writer admission.

Hazards and follow-ups: this candidate is not yet the confirmed cause of the
failed preparation. The preserved native refusal must be correlated with the
diagnostic reproduction. Full combined-source CI and unchanged preparation,
whole-action recovery and actual rehearsal remain mandatory. ADR-0285's owned
private restore lifecycle is separate unfinished work. No campaign or package
closure is claimed by these component checks.
