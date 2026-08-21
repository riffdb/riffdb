# WP-657 indexed exact-result provider closure

WP-657 implements the provider-owned half of ADR-0134. Exact predicate state
uses additive format V4 and binds the semantic program, plan, provider
descriptor, policy shape, partition, authoritative history incarnation,
generation, frontier, static work/state bounds, and checksum. V1 through V3
checkpoints remain separate formats and are never reinterpreted as V4.

The provider maintains sparse state, scalar, binary-text, reversed-text, and
suffix postings for every compiler-referenced field. It constructs a separate
total-order coordinate space for each compiler-declared mixed-direction order.
Predicate nodes combine bounded bitsets, exact count uses population counts,
and ordinal windows use word-level rank/select. Only the requested bounded rows
are released. Authoritative entities are scanned and hydrated only by the
background rebuild owner; the request port contains no authoritative scan,
point read, result sort, skipped-row/page walk, full-match allocation, or
provider bridge.

For partition-aligned policy, the partition itself is the authorized universe.
For bounded row admission, the background owner obtains a complete checked
admission decision and removes denied rows before constructing any V4 index.
The slot identity includes the role shape and current capability/revision, so a
policy revision selects a disjoint generation rather than reusing prior allow
decisions. Query execution proves the descriptor, state schema, plan, policy,
history, generation, and exact frontier once for the opened result set.

Canonical checkpoint recovery verifies the envelope, slot, history,
program/descriptor, rows, checksum, derived indexes, and byte-exact re-encode.
Atomic pending-file replacement and directory synchronization preserve restart
behavior. Corruption, truncation, trailing bytes, old formats, excessive state,
invalid order state, duplicate mutations, non-advancing epochs, stale
frontiers, and mixed identities fail closed. Compaction rebuilds only derived
indexes and preserves byte-identical canonical state.

Automated evidence includes randomized comparison with the independent WP-656
reference evaluator; every activated operator and scalar profile; missing,
null, and present states; optional and disjunctive families; independent order;
zero/end/beyond-end offsets; update/delete atomicity; compaction; restart;
corruption and format refusal; exact epoch proof; and source-boundary tests for
policy-before-indexing and the absence of request-time authoritative work.

WP-658 retains ownership of public named-query parameter binding, shared
service dispatch, gRPC/CLI/MCP exposure, generated Rust/Go/TypeScript/Python
methods, external publication, and adapter acceptance. V4 cannot be selected
by an application caller before that package completes.
