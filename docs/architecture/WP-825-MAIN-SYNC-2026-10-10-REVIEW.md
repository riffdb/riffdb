# WP-825 local main reconciliation, 2026-10-10

Merge `96db3a219` integrates local main `8c1b6678f`, including WP-830's derived-sidecar
absence observation and WP-831's per-walk segment reuse. Remote main had not
received these commits when integration began. Both accepted main decisions
retain their identifiers and exact text.

Main owns ADR-0282 and ADR-0283. The branch's unaccepted finite checkpoint
handoff proposal moves from ADR-0282 to ADR-0287; it remains proposed and
unimplemented. The accepted follower acknowledgement decision moves from
ADR-0283 to [ADR-0286](../../adr/0286-bounded-native-follower-acknowledgement-validation.md).
Its Decision section is byte-for-byte identical to the previously accepted
record. Its original acceptance references are preserved. The bootstrap
record [ADR-0284](../../adr/0284-bounded-native-bootstrap-source-custody-validation.md)
changes only references from ADR-0283 to ADR-0286. Package requirements and
allowed paths follow that same identity. Historical observations retain the
identifiers used at the time; current references use the reconciled identity.
ADR-0285 is reserved for the separate, unaccepted private-restore proposal.

The maintainer approved this identifier and reference reconciliation in session
on 2026-10-10: "Approve the identifier-only reconciliation";
no new decision, format, ordering boundary or public guarantee is introduced.
The startup test conflict retains both WP-825's shared-evidence module and
main's segment-reuse regressions. The ADR index will be regenerated on C3D.

The separate journal append-worker candidate is still undergoing scoped
acceptance on C3D against its original base. That result cannot qualify the
combined source. The candidate is now integrated separately; run combined-source acceptance
and full CI on C3D before another ordinary
preparation. Earlier failures remain failures.
