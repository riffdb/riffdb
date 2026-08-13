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
using fresh transport request IDs.

## Acceptance evidence

Run the focused profile gate:

```bash
./scripts/adapter-framework-profile-acceptance
```

It compiles and round-trips the profile, exercises the atomic graph and token/
workflow refusal paths, runs the real-redb concurrent uniqueness schedule, and
checks retry identity preservation. The full deployable-alpha gate invokes the
same focused phase.
