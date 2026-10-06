# WP-825 registry completion implementation review

Package: WP-825 (open). Tier: guarantee.
Authority: accepted ADR-0275, ADR-0276 and ADR-0277.

Behavior added or changed:

- Native full structural and catalog validation earns a canonical V3 registry
  completion binding for the exact database, history incarnation and registry.
  Pruning still deletes prefix checkpoints; an exact completion binding preserves
  bounded dirty-start eligibility without granting prefix reclamation authority.
- The physical writer advances from 2 to 3 through the existing backup-bound
  offline transition. Frozen predecessor registries, encodings and backup bytes
  remain intact; ordinary successor startup refuses predecessor markers.
- Offline upgrade holds both existing engine locks before publishing Accepted
  progress. Initial and resumed attempts enforce the declared free-space floor
  with bounded logical-file accounting and principal-available filesystem bytes.
  Insufficient space produces a typed, redacted refusal and the existing closed
  CLI error envelope, with a provision-space-and-retry recovery action.
- Restore and promotion earn successor completion only through full native proof.
  That proof may replace a copied checkpoint of an earlier incarnation with a
  successor genesis checkpoint in the same completion receipt. Ordinary checkpoint
  refresh cannot do so.
- Archive restore comparison checks each legitimate startup receipt and every
  namespace and row. A bounded certificate-delta fold preserves exact initial
  preimages and each intervening edge across repeated validation crashes. It
  accepts only prefix heads matching live chain heads, exact completed registry
  bytes and successor-bound checkpoint evidence; application rows and allocators
  remain byte-exact.

Checks run:

- Focused native format-upgrade tests, including a reproduced zero-space failure
  before the repair, exact floor and overflow tests, and existing real upgrades.
- Exact archive comparison tests and repaired restore validation crash cases;
  the process fixture also covers repeated completed-validation crashes.
- Five startup promotion unit tests and the affected promotion integration cases.
- Public archive restore and V1-index migration readiness/query regressions.
- Four-crate acceptance before free-space implementation: ten checks passed.
- Five affected crates pass all-target, all-feature clippy. Dependency policy and
  advisory checks pass; the lockfile adds only this crate's existing rustix edge.

Compatibility:

The current manifest, descriptors, registry inventory and fixtures add only the
reviewed V3 record and writer edge. Existing per-record wire vectors, frozen V2
registry schema and predecessor identity/corpus fixtures remain unchanged.
Supported predecessor backups authenticate upgrade without being rewritten or
promoted into current backups. A new current backup may be restored normally.

Handbook pages updated: `docs/compatibility.md` and
`docs/operations/STARTUP-INTEGRITY.md`; both are already in `docs/SUMMARY.md`.

Hazards and follow-ups:

This is an implementation review, not package closure or host qualification.
Final-source scoped acceptance and full CI, fixture review and the required host
preparation remain gates. Retain failed rehearsal originals. Measure normal-rate
latency, actual eligible history bytes, physical extent generations, aggregate
memory and every public fault/recovery path on the final artifact identity.
A free-space observation reserves no capacity; later I/O failures retain the
existing fencing and backup-bound recovery. The 24-hour run is not started or
approved by this implementation.
