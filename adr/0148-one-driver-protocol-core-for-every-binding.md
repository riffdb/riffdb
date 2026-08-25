# ADR-0148: One Driver Protocol Core for Every Binding

- **Status:** Proposed
- **Direction approved:** 2026-08-25 (maintainer)
- **Exact text accepted:** No
- **Decision deadline:** Before WP-682 changes either driver's request
  validation, value marshalling, or error mapping
- **Requires:** ADR-0074 and ADR-0106 accepted; amends the boundary both
  describe
- **Defines or blocks:** WP-682
- **Provisional requirements:** `DRV-015`, to be added to SPEC on acceptance

## Context

RiffDB reaches applications through two shapes today. Go and server-side
TypeScript speak a length-framed JSON protocol to `riffdb-driverd`, the
first-party Rust driver host ADR-0106 selected. Python calls
`riffdb-client-python-native`, a PyO3 extension ADR-0074 accepted.

Both delegate their semantics to the same crate. `riffdb-client-rust` owns
transport trust, retries, uncertainty recovery, identity negotiation, TLS,
cancellation, and read-after-commit for both shapes, and both marshal into the
same `riffdb_types::ApplicationValue`. There is no second implementation of any
command guarantee, and this ADR does not propose one.

What is duplicated is the layer above that core. `riffdb-client-python-native`
carries its own `parse_value`, `parse_command`, and `parse_query` against
roughly two thousand lines, and `riffdb-driver-host` carries its own protocol
structs, `validate_values`, and dispatch. Two independent paths convert an
application's JSON into `ApplicationValue`, each with its own bounds, its own
rejection classes, and its own error mapping. A change to a value rule, a bound,
or an error class must land twice and be proven twice, and the two can silently
disagree.

The split is historical rather than considered. ADR-0074 predates ADR-0106; when
it was accepted "the current TypeScript transport delegates to the CLI" and no
driver host existed. Python received a native extension because that was the
available answer, and Go and TypeScript later received the host. Nobody decided
Python's protocol surface should differ.

Two measurements bound the alternatives. Moving Python onto `riffdb-driverd`
would remove about seventeen microseconds per operation of GIL-held Rust JSON
work, worth roughly eleven percent of the GIL ceiling, but it adds a socket
round trip to the single-client latency cell, which is already Python's weakest
result, and it replaces a self-contained abi3 wheel with a wheel plus a daemon
the operator must obtain and run. Keeping two marshalling layers costs no
throughput and every future driver feature twice.

## Proposed Decision

### 1. One protocol core, several bindings

`riffdb-driver-host` exposes its request and response types, validation, and
dispatch as a library surface. Every binding constructs the same
`DriverRequest`, receives the same `DriverResponse`, and inherits the same
validation and error mapping. The core performs no transport of its own.

Two transports carry it, and no binding may introduce a third protocol:

- a local socket, for Go and server-side TypeScript, unchanged; and
- an in-process call, for the Python extension.

`riffdb-client-python-native` keeps its PyO3 boundary and its self-contained
wheel, and stops carrying a parallel protocol. Its remaining first-party code
converts Python objects to and from the shared request and response types.

### 2. The core owns every protocol rule exactly once

Value admission, depth and collection bounds, byte budgets, retained-byte
ceilings, duplicate-field rejection, integral-number rules, request identity,
and the closed public error classes are defined once in the core. A binding may
not widen, narrow, reorder, or reinterpret any of them, and may not accept a
request the core would refuse.

Where the two paths disagree today, the driver-host rule is authoritative and
the difference is recorded as a fixed finding in the work package rather than
silently adopted.

### 3. Equivalence is proven, not asserted

One conformance corpus drives every binding. For each entry the shared core
must produce byte-identical request material, an identical `ApplicationValue`
graph, and an identical public error class through the socket transport and the
in-process transport alike. A binding that cannot reproduce an entry fails; the
corpus is not narrowed to keep a binding green.

This obligation stands independently of the consolidation and is the first
deliverable, so drift between the existing paths is caught before either moves.

### 4. No application-visible change

The consolidation changes no public gRPC surface, generated API, wire protocol,
durable byte, credential handling, deployment artifact, or supported platform.
The Python wheel remains self-contained and installs without a daemon. Go and
TypeScript continue to require `riffdb-driverd`. No application can select a
transport, a binding, a validation mode, or a protocol version it could not
select before.

### 5. Sequencing

The conformance corpus lands first and independently. The consolidation itself
delivers no functional or performance change, so it may not preempt alpha gate
work; it is scheduled after the `PERF-008` comparative gates qualify.

## Options Considered

1. **Move Python onto `riffdb-driverd` and delete the extension:** rejected. It
   unifies the process model at the cost of the self-contained wheel, adds a
   socket round trip to Python's weakest cell, and makes the first-run
   experience for the largest driver population require a daemon.
2. **Leave both paths and document the duplication:** rejected. Every value
   rule, bound, and error class lands twice with no mechanism that proves the
   two agree, and the paths have no shared test.
3. **Reimplement the Python protocol in Python:** rejected for the reason
   ADR-0074 and ADR-0106 already give — checked semantics would leave
   first-party Rust.
4. **One shared protocol core behind two transports:** proposed. It removes the
   duplicated surface, keeps both deployment shapes, and adds no unsafe code
   beyond the PyO3 boundary that already exists.

## Consequences

- `riffdb-driver-host` gains a library API and becomes a dependency of
  `riffdb-client-python-native`.
- Roughly two thousand lines of parallel marshalling leave the Python crate.
- A future driver feature lands once and reaches every binding.
- The conformance corpus becomes the place a protocol rule is stated.
- The work delivers no user-visible benefit, so it must be scheduled honestly
  against feature work rather than presented as an improvement.

## Compatibility

No public gRPC, MCP, CLI, generated SDK, contract grammar, RiffQL, IR, bundle
hash, capability, durable record, journal frame, backup, export, changelog, or
replication format changes. The driver protocol version does not change. An
existing Python or TypeScript application continues to run unmodified against
an unchanged daemon.

## Security

The Python binding keeps the PyO3 boundary ADR-0074 accepted and gains no new
unsafe code. Credential handling, the protected application-role credential
boundary, socket owner checks, and redaction are unchanged. Consolidating
validation reduces the surface on which the two paths could diverge, which is
the only way a binding could today accept a request the host would refuse.

## Standing Design Tests

- **Interface safety:** an application still invokes only a compiled operation
  with typed input and an idempotency identity. It cannot select a binding, a
  transport, a validation mode, a protocol version, or a fallback, and every
  operation retains fresh authorization, transaction-current dependency checks,
  atomic mutation and outcome, and typed uncertainty.
- **Scale:** the shared core keeps the existing bounded pool, in-flight,
  reorder, frame, value, and retained-byte ceilings. No per-application cache,
  per-operation label, or unbounded retained state is introduced, and the
  in-process transport reuses the same admission bounds as the socket
  transport.

## Testing

- One conformance corpus asserting byte-identical request material, identical
  `ApplicationValue` graphs, and identical public error classes across both
  transports.
- Architecture tests proving `riffdb-client-python-native` defines no value
  parser, bound, or error mapping of its own, and that no binding crate reaches
  `riffdb-client-rust` except through the shared core.
- The existing Python and TypeScript driver suites, adapter conformance, and
  the installed-artifact golden corpus, unchanged and green.
- A wheel installation test proving the Python artifact still runs with no
  daemon present.

## Requirements and Work Packages

- **Requirements:** `DRV-001` through `DRV-014` retained; `DRV-015` registered
  for the single-protocol-core obligation
- **Defines or blocks:** `WP-682`
- **Final evidence:** `WP-682`

## Decision Deadline

Exact human acceptance is required before WP-682 changes either driver's
request validation, value marshalling, or error mapping.
