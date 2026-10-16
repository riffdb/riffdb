# Follower projection replay preserves native command membership

Package: WP-825. Tier: guarantee, within accepted native locator and follower
projection decisions. No new format, authority or application interface.

Each closed follower replay transaction writes only local projection rows and
markers. Previously its durable commit replaced the captured native command
directory with Pending, despite unchanged command membership and source binding.
Subsequent replay and acknowledgement then refused a healthy complete source.

The transaction now retains its exact predecessor capability under the existing
mutation lease. Complete stays Complete; prevalidation Pending stays Pending;
invalid proof refuses before writing. Database identity, incarnation, registry,
authoritative head and history remain unchanged. No new construction owner or
readiness proof is created. Existing crash boundaries and source-footprint
invalidation remain in place.

Checks on C3D: both original follower startup/recovery crash controls, all five
original follower shipping regressions and 47 native replay, snapshot,
acknowledgement, generation and builder controls pass. Scoped server/native
all-target/all-feature Clippy and allowed-path checks pass. Native tests also
assert unchanged Pending ownership, source binding, command scan and history,
and refusal of invalid capability without a commit. Earlier public transport,
promotion and crash controls passed; combined final-source CI remains required.

Historical fixture reads explicitly finish their captured native construction
where completeness is a precondition. Deliberate cold/Pending assertions execute
before that synchronization. No harness retry, production wait, deadline or
readiness change is used to repair replay.

Handbook: Follower Administration documents capability continuity. Compatibility:
durable bytes, replication ordering, authorization and audit are unchanged.
Hazards: this does not reconstruct pruned projection history, fix native close
ownership or qualify the long campaign. Both packages remain open.
