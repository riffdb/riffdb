# ADR-0074: Rust-Backed Python Application Driver

- **Status:** Accepted
- **Proposed:** 2026-07-31
- **Accepted:** 2026-07-31
- **Acceptance reference:** Maintainer exact-text approval in the implementation
  session after review of the WP-394 interface freeze
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PYD-001` through `PYD-012`
- **Related work packages:** `WP-394` through `WP-398`
- **Requires:** ADR-0037, ADR-0040, ADR-0041, ADR-0052, ADR-0055,
  ADR-0056, ADR-0057, ADR-0063, ADR-0065, and ADR-0066
- **Amends if accepted:** `SYS-002`, SPEC Sections 3, 11.5, 20, and 24
- **Implementation boundary:** Exact acceptance before Python or PyO3
  production code, Cargo dependencies, or application format V2 changes

## Context

RiffDB already generates complete stable-application clients for Rust and
TypeScript, and the MVP roadmap names generated Python clients. The current
TypeScript transport delegates to the CLI, while a Python driver is expected
to behave as a normal `pip` or `uv` dependency and use native gRPC.

Reimplementing the transport in Python `grpcio` would duplicate checked
Protobuf error decoding, identity validation, request-ID generation, and
uncertainty recovery. More importantly, `grpcio` does not expose a stable
public equivalent of Tonic's local transport-error source, which the accepted
Rust client uses to distinguish connection loss from a peer's malformed
details-free status. Guessing from a gRPC code or message would weaken the
fail-closed protocol boundary.

There is also an explicit policy conflict to resolve. `SYS-002` says all
first-party production code is Rust, while accepted ADR-0056 requires a real
first-party TypeScript application runtime and SPEC names generated Python
clients. This record narrows the intended exception without moving database
semantics out of Rust.

## Decision

### Language boundary

If this record is accepted, `SYS-002` means that the database server, compiler,
storage, command runtime, authorization, protocols, transport trust decisions,
and authoritative semantics are first-party Rust. First-party generated
application bindings and their language-idiomatic value/facade code may use
their target language when an accepted ADR freezes the boundary and equivalent
semantic tests.

Python code may own immutable presentation values, generated dataclasses,
deterministic name mapping, and typed facade assembly. It may not own gRPC
status interpretation, retry classification, request identity, authorization,
contract selection, mutation semantics, canonical hashing, or durable state.

### Package and platform

The distribution is `riffdb-application`; the import is
`riffdb_application`. Versioning follows the RiffDB release. The first
supported runtime is CPython 3.13 or newer on manylinux x86_64 and aarch64.
The native wheel uses the CPython limited API with a Python 3.13 floor and is
tested on 3.13 and 3.14. macOS, Windows, PyPy, and publication credentials are
not part of this package.

The release produces platform wheels and a self-contained source distribution.
Wheel installation requires no Rust toolchain. The source distribution pins
Rust 1.97.0 and stages every required internal Rust crate so it builds without
a RiffDB checkout. PyPI upload remains a distinct human release action.

### Native implementation

One safe-Rust workspace crate, `riffdb-client-python-native`, builds the private
PyO3 module `riffdb_application._native`. It depends on and delegates to
`riffdb-client-rust`; it does not generate or expose Python Protobuf stubs.

The approved exact new native dependencies are:

- `pyo3` 0.29.0 with only `extension-module` and `abi3-py313` in production;
- `pyo3-async-runtimes` 0.29.0 with only the Tokio runtime bridge; and
- Maturin 1.14.1 as the pinned PEP 517 build backend.

These dependencies contain reviewed third-party native/unsafe implementation,
but first-party code retains `#![forbid(unsafe_code)]`. Cargo and Python lock
files, licenses, advisories, features, build scripts, wheel linkage, and the
source-distribution contents are acceptance evidence. A version or feature
change requires renewed dependency review.

The bridge converts bounded private DTOs to existing Rust application values
before awaiting, releases the GIL during network waits, converts only checked
Rust results back to Python, catches every Rust error without panic text, and
propagates cancellation so non-durable client resources are released. The
sync and async transports own separate explicit channel lifecycles. Async is a
direct Rust-future bridge, not a thread-pool wrapper around sync.

### Public facade and values

The public interface is frozen by
`fixtures/application-parity/python-public-api-v1.pyi`. Raw native objects,
Protobuf messages, Tonic types, gRPC status, and generic kernel/admin methods
are private and absent from `__all__`.

Generated values use UUID, Decimal, bytes, checked integers, immutable money,
an exact seconds/nanoseconds timestamp, generated `StrEnum`, frozen slotted
dataclasses, and tuples. `RiffDate` stores signed epoch days and supplies
checked `datetime.date` conversion; a valid database date is never rejected
solely because Python's calendar range is narrower.

Generated clients expose explicit `AttemptBudget`, paired sync/async methods,
typed query options, exact outcome unions, batches, pagination, and
read-after-commit. Python names are deterministic. Keywords gain one trailing
underscore; any collision after that transform is a source-spanned compiler
diagnostic rather than an unstable suffix.

Credentials wrap the existing Rust owner, are redacted, nonextractable, and
non-pickleable, and reuse the protected Linux file loader. Call metadata reuses
the exact bearer, `riffdb-database`, and traceparent validation. No public error
contains peer text, raw gRPC exceptions, native panic text, credentials,
submitted values, or internal sources.

### Application format V2

V1 source manifests, locks, canonical bytes, and generated artifacts retain
their exact meanings. V2 adds one required `python` generation target alongside
`mcp`, `rust`, and `typescript`; lock V2 adds a closed Python artifact kind and
locks its path, bytes, and digest.

`riffdb application migrate --to v2` is read-only and renders the canonical
proposed source manifest. `--write` atomically rewrites only the source
manifest. It never writes a lock or generated artifact, deploys, binds, seeds,
or contacts a server. The normal explicit `application lock --write` remains
the compiler-identity and generated-artifact review boundary.

New Python scaffolds use source layout, `pyproject.toml`, `uv.lock`, a generated
client, strict checks, tests, and the release's matching local wheel for
offline `uv sync --locked`. Documentation also covers direct wheel/sdist and
eventual registry installation with both pip and uv.

## Compatibility

This decision adds no public Protobuf field, RPC, MCP tool, durable record,
storage key, contract IR, plan hash, authorization operation, or command
semantic. Application source/lock V2 is an explicit local-format successor;
V1 is never silently upgraded. The native module is private. The checked
Python facade and generated text become public compatibility artifacts.

## Security

The Python package is application-only and holds no authority beyond its
credential. Every call uses the same server authentication, application
service, policy, runtime, and coordinator as existing clients. Boundary linting
rejects raw protocol/native imports and handwritten RiffDB transports in
application code. Private-module naming is defense in depth, not an
authorization boundary.

The build is a new supply-chain boundary. Exact dependencies, abi3 wheel tags,
manylinux linkage, sdist inventory, hashes, licenses, advisories, and clean
offline installs are reviewed before release. Credentials and Rust-owned native
objects cannot be copied through repr, serialization, exceptions, or generated
result values.

## Testing and Evidence

- Golden generated Python and public-stub fixtures cover every value and
  operation shape, keyword collisions, exact identities, and V1/V2 formats.
- Native unit and adversarial tests cover malformed DTOs, malformed peer
  responses, cancellation, GIL release, panic containment, credential
  redaction, retry, replay, and unresolved outcomes.
- Installed tests exercise sync and async clients against two independently
  authorized databases and compare normalized observations with Rust and
  TypeScript.
- Python 3.13 and 3.14 install the x86_64 and aarch64 abi3 wheels; a clean
  Rust-1.97 environment builds and installs the self-contained sdist.
- Repeated builds compare wheel/sdist inventories and hashes. Prior sealed
  Agent Application Alpha evidence is not modified.

## Consequences

- Python gets native gRPC behavior without a second transport trust model.
- Binary wheels make ordinary pip/uv installation simple; source installation
  is heavier because it deliberately compiles the existing Rust client.
- The supported platform matrix is narrower than pure Python but honest and
  testable for the current Linux product.
- Application format V2 becomes the first format that requires all three
  generated programming-language clients.

## Rejected Alternatives

1. **Direct `grpcio` implementation:** rejected because it duplicates semantic
   validation and cannot preserve the accepted local-transport classification
   without a Python-specific recovery rule.
2. **CLI subprocess transport:** rejected for Python because the requested
   driver must be a normal native dependency and the Rust client already owns
   the correct gRPC behavior.
3. **No automatic retry:** rejected because it would fail generated-client
   parity and weaken uncertainty recovery.
4. **`datetime.date` only:** rejected because it cannot represent the complete
   valid RiffDB epoch-day range.
5. **Expose raw generated stubs:** rejected because it creates a convenient
   kernel/admin bypass around the stable application facade.
6. **Publish internal Rust crates first:** deferred because a staged
   self-contained sdist avoids broadening the Rust publication surface.

## Work-Package Mapping

- WP-394: exact interface, dependency, and compatibility acceptance.
- WP-395: source/lock V2, migration, and generated Python clients.
- WP-396: native Rust bridge and Python runtime.
- WP-397: scaffold, package, release artifacts, and documentation.
- WP-398: installed cross-language and multi-database proof.
