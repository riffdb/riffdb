# WP-825 main integration review, 2026-10-17

Package: WP-825. Tier: guarantee. Focused integration qualification passed;
full acceptance and CI remain pending.

Main adds WP-869 through WP-872: observed-incarnation tracking, projected
aggregate order and limits, capability rotation and renewal, and retention
telemetry. The integration retains these features and the native admission,
retention, and startup fixes already on this branch.

Main owns ADR-0314. The previously accepted locator decision moves to ADR-0327
under the maintainer's standing identifier-only reconciliation approval. Its
original acceptance and entire Decision are unchanged. Obligation identifiers,
ADR-0323's locator references, WP-825 registration and architecture references
follow the move; main's ADR-0314 and its four packages keep their identities.

Both retention tests survive the append conflict. Pending native admission
keeps its detailed `awaiting_native_dependency` census state; the existing
telemetry vocabulary reports the nonterminal blocked state `fenced`, with
binding `none` and no invented pruning progress. Configuration documents this
mapping, and the existing wake/cancellation schedule asserts the observation.

Projected-query conflicts retain the independent money-coefficient row oracle
and canonical group-order assertions. Main's descending top-two group query
and typed rejection of summing a string column remain executable assertions.

The ADR index was regenerated on the qualification host. The maintainer approved
main's ADR-0314 heading-only correction from `Options and consequences` to
`Consequences` on 2026-10-17; every other byte of that record is unchanged.

Checks run on the 8-vCPU Linux qualification host: both retention schedule tests,
four projected-query semantic tests, server and application-service Clippy,
workspace formatting, ADR form and index, acceptance-commit protocol, scope,
file-size and panic-allowance checks. These checks passed. Full acceptance and
CI are still required before merging the completed package to main.

Compatibility: both accepted locator decisions retain their original Decision
text and acceptance; existing query and telemetry interfaces remain intact.
The configuration and known-limitations handbook pages describe the merged
behavior. Hazards and follow-ups: Source maintenance integration and the
rehearsal gates remain open. No recovery, retention, release, memory or workload
gate changes.
