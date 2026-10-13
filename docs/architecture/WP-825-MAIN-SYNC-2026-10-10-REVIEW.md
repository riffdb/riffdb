# WP-825 local main reconciliation, 2026-10-10

Merge `96db3a219` integrates local main `8c1b6678f`, including WP-830's derived-sidecar
absence observation and WP-831's per-walk segment reuse. Remote main had not
received these commits when integration began. Both accepted main decisions
retain their identifiers and exact text.

Main owns ADR-0282 and ADR-0283. The branch's unaccepted finite checkpoint
handoff proposal moves from ADR-0282 to ADR-0287; it remains proposed and
unimplemented. The accepted follower acknowledgement decision moves from
ADR-0283 to ADR-0286.
Its Decision section is byte-for-byte identical to the previously accepted
record. Its original acceptance references are preserved. The bootstrap
record ADR-0284
changes only references from ADR-0283 to ADR-0286. Package requirements and
allowed paths follow that same identity. Historical observations retain the
identifiers used at the time; current references use the reconciled identity.
ADR-0285 is reserved for the separate, unaccepted private-restore proposal.
The [follower implementation review](WP-825-ADR-0286-IMPLEMENTATION-REVIEW.md)
and [bootstrap implementation review](WP-825-ADR-0284-IMPLEMENTATION-REVIEW.md)
provide handbook navigation; the decision records remain in the repository
under `adr/`.

The maintainer approved this identifier and reference reconciliation in session
on 2026-10-10: "Approve the identifier-only reconciliation";
no new decision, format, ordering boundary or public guarantee is introduced.
The startup test conflict retains both WP-825's shared-evidence module and
main's segment-reuse regressions. The ADR index was regenerated on C3D and record checks passed.

The separate journal append-worker candidate passes scoped acceptance on C3D
against its original base: all six planned checks and 1,378 native tests, with
two existing skips. That result cannot qualify the combined source. The worker
change is integrated separately at `c53f19712`.

Combined-source compilation exposes an additional initializer in WP-825's
parallel offline validator that must adopt main's segment memo. The correction
shares the session's single memo across its bounded inspectors and serializes
exact-byte lookup, decoding and successful replacement under one mutex. It
retains one cache entry per walk; refused decodes remain uncached. A barrier
regression checks concurrent intact and incorrect-provenance links against one
segment and requires exactly one successful segment decode. Validation runs
only on C3D. The concurrent cache test, unchanged byte/refusal and service-link
regressions, all four append-worker tests, formatting and storage Clippy pass.
Combined-source full CI and fresh ordinary preparation remain
required. Earlier failures remain failures.


Identifier reconciliation on 2026-10-13: the historical follower record called
ADR-0286 below is now ADR-0296; the unaccepted handoff proposal called ADR-0287
is now ADR-0297. Live links use the current paths. Historical review and acceptance
text retains its original identifiers; the maintainer approved these identifier
and reference updates without changing any accepted Decision section.
