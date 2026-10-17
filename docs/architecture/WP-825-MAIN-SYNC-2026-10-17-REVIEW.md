# WP-825 main integration review, 2026-10-17

Package: WP-825. Tier: guarantee. Qualification is pending.

Main adds WP-869 through WP-872: observed-incarnation tracking, projected
aggregate order and limits, capability rotation and renewal, and retention
telemetry. The integration retains these features and the native admission,
retention, and startup fixes already on this branch.

Main owns ADR-0314. The previously accepted locator decision moves to ADR-0319
under the maintainer's standing identifier-only reconciliation approval. Its
original acceptance and entire Decision are unchanged. Obligation identifiers,
ADR-0315's locator references, WP-825 registration and architecture references
follow the move; main's ADR-0314 and its four packages keep their identities.

Both retention tests survive the append conflict. Pending native admission
keeps its detailed `awaiting_native_dependency` census state; the existing
telemetry vocabulary reports the nonterminal blocked state `fenced`, with
binding `none` and no invented pruning progress. Configuration documents this
mapping, and the existing wake/cancellation schedule asserts the observation.

Projected-query conflicts retain the independent money-coefficient row oracle
and canonical group-order assertions. Main's descending top-two group query
and typed rejection of summing a string column remain executable assertions.

The ADR index is regenerated on the qualification host. Focused merged-path
checks precede broader qualification. No recovery, retention, release, memory,
or workload gate changes.
