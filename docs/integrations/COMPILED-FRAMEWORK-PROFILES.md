# Compiled framework profiles

RiffDB does not implement an external framework's generic CRUD store or
transaction callback. An integration repository resolves the framework's
configured models and plugins at generation time, emits an ordinary RiffDB
contract with named commands and queries, and dispatches only to that compiled
surface. Unknown models, hooks, plugins, and transaction shapes fail during
generation instead of degrading into sequential writes at runtime.

The adapter remains application code. No framework dependency, identity
protocol, session logic, or privileged storage path enters RiffDB. A framework
"transaction" is supported only when one generated command describes its
complete atomic graph. Bounded collection commands cover finite repeated
graphs; workflow commands cover revision-checked session transitions. An
adapter must never run several commands and report that they were one atomic
transaction.

The framework-agnostic acceptance contract at
`fixtures/adapters/framework-profile/riffdb/contract.riff` demonstrates the
generic capabilities:

- transactional unique identities and provider/account links;
- one bounded atomic user/account/session create graph;
- compiler-owned session initialization plus exact-revision refresh/revoke;
- secret-classified session and verification-token digests;
- consume-once verification with transaction-time expiry refusal; and
- server-observed transaction time and UUID values.

It is a workload shape, not a bundled framework adapter.

## Repository ownership boundary

Framework-specific contracts, profiles, generated SDKs, route hosts, and route-
level compatibility tests belong to the integration repository that owns the
adapter. That repository pins an immutable RiffDB release and records its
framework version, enabled plugin/model surface, generated identities, and
acceptance observations. RiffDB does not copy those artifacts into its own
source tree.

The RiffDB repository retains framework-neutral fixtures for the compiler,
providers, authorization, generated transports, and cross-language semantics.
For exact result sets, the generic `Document` corpus proves contains, starts-
with, ends-with, typed optional filtering, deterministic orders, bounded direct
offset, and complete exact total. An adapter can depend on that public
capability, but neither its route vocabulary nor its userland policy becomes a
RiffDB feature.

External adapters can pin
`fixtures/riffql/operational-query-capability-v4.json`. V4 preserves the
immutable V1 through V3 receipts and adds the neutral operational-access corpus
for shared equality, membership, prefix, range/complement, nullable, cursor,
policy, and dependent-key shapes. It binds the exact application lock and
generated Rust, Go, TypeScript, Python, and MCP artifacts.

V4 also binds one value-free external tuple-consumer receipt demonstrating that
one bytewise text-key index can supply exact prefix, bounded membership, and
order without a redundant-index fallback. That receipt contains hashes,
bounded work figures, and the typed result only; it contains no external
schema, route, adapter, generated profile, or stored value. It explicitly does
not claim full external application or framework conformance. The owning
adapter repository must still prove that its real public surface delegates only
to pinned generated operations without filtering, sorting, counting, page
walking, raw query construction, or storage access.

## Keyless upstream retries

When an upstream framework supplies no idempotency key, the adapter owns the
logical request identity:

1. Mint one bounded RiffDB command idempotency value before the first dispatch.
2. Freeze the complete generated command input containing that value.
3. Reuse that exact input for every transport retry and uncertainty-resolution
   attempt until a terminal declared outcome is known.
4. Allow the driver to mint a fresh transport request ID for each attempt; the
   transport ID is not the command's idempotency identity.

If retry must survive adapter process loss, retain the mapping from a stable
upstream operation identity to the minted command identity before dispatch. If
the framework provides no stable operation identity and the adapter did not
retain one, it cannot honestly claim process-loss idempotency and must surface
that limitation. Never mint a new key after an uncertain result, and never
derive the key from a plaintext token or other secret.

The Rust driver test
`adapter_minted_idempotency_survives_driver_retries_with_fresh_transport_ids`
proves that bounded retries preserve the byte-identical command input while
using fresh transport request IDs. On the admission side,
`equal_key_commands_commit_once_and_replay_exactly` proves the property the
minted identity rides: equal-identity submissions commit exactly once, the
replay returns the byte-equal stored outcome under one commit sequence and
one provenance record, and different input under the same identity is
refused as an input mismatch.

## Acceptance evidence

Run the focused profile gate:

```bash
./scripts/adapter-framework-profile-acceptance
```

It compiles and round-trips the profile, exercises the atomic graph and token/
workflow refusal paths, runs the real-redb concurrent uniqueness schedule, and
checks retry identity preservation. The full deployable-alpha gate invokes the
same focused phase.
