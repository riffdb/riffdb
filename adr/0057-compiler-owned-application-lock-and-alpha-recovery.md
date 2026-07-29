# ADR-0057: Compiler-Owned Application Lock and Alpha Recovery

- **Status:** Accepted
- **Proposed:** 2026-07-29
- **Direction approved:** 2026-07-29
- **Exact text accepted:** 2026-07-29
- **Acceptance reference:** Maintainer approval of the documented Terra
  campaign recovery plan and direction to build it
- **Requires:** ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0008,
  ADR-0009, ADR-0013, ADR-0027, ADR-0040, ADR-0041, ADR-0051,
  ADR-0052, ADR-0055, and ADR-0056
- **Amends:** SPEC Sections 20 and 24.4
- **Implementation boundary:** Before another sealed Agent Application Alpha
  campaign or a claim that application authors never manage compiler identities

## Context

Terra campaign 01 completed four sealed evaluations and produced ratings of
1, 2, 3, and 2. No evaluator completed its unfamiliar-domain workload.

The public application path contains a contradiction. An application manifest
contains compiler-derived contract and query-module hashes. Generation correctly
rejects stale hashes, but the public CLI exposes no operation that computes and
records replacements after a symbolic source edit. Compiler, planner, manifest,
and role errors are also collapsed into generic scaffold failures. The sealed
bundle omits a complete contract-language reference and machine-readable
manifest schema, `riffdb new` does not accept the already-created empty
evaluation directory, and the TypeScript scaffold requires manual package
repair before it is a working offline web application.

The campaign showed no RiffQL execution or expressiveness defect. The checked
Blog and Orders corpora already compile and execute through the first-party
application path. Kernel exclusion, zero handwritten transport glue, and source
isolation held.

## Decision

### Product rule

Application authors own symbolic intent. The compiler alone owns numeric, hash,
plan, schema, and derived-authority identities.

Exact compiler-owned identities remain mandatory for generation,
authorization, deployment, and execution. Removing hashes from the author-owned
document does not permit ambient selection, negotiation, downgrade, or
unreviewed deployment.

### Source manifest and exact lock

Add an author-owned application source manifest and a compiler-owned exact lock:

```text
riffdb.application.json
riffdb.application.lock.json
```

The source manifest binds symbolic contract/query sources, symbolic roles,
generation targets, and seed inputs. It contains no contract bundle hash,
query-module hash, query ID, plan hash, field ID, capability mask, or derived
visibility set.

The lock covers the canonical source-manifest identity, every normalized source
digest, exact contract bundle identity, module/query/plan/schema identities,
compiled role-authority identity, compiler and format versions, and generated
artifact digests. It contains no clock, machine path, credential, runtime
parameter, active deployment pointer, or application value.

V1 pinned manifests remain readable for exact generation. Migration is explicit
and deterministic; V1 is never silently reinterpreted as the new source
manifest.

### Authoring operations

The public local authoring surface is:

```text
riffdb application check
riffdb application lock --write
riffdb application lock --check
riffdb application generate --locked
```

`check` performs no write. `lock --write` compiles every source, query, and role
and renders a bounded symbolic operation/authority diff before atomically
replacing compiler-owned lock and generated artifacts. It performs no server
operation. `lock --check`, generation, role binding, and non-development
deployment reject source drift, lock drift, compiler-format mismatch, partial
output, and identity mismatch.

`riffdb dev` may refresh a lock, regenerate, deploy, and bind only for its
product-owned local ephemeral database and a manifest-declared development
role. A failed iteration leaves the last accepted generation active. No
production operation automatically refreshes a lock or widens authority.

### Authoring diagnostics

Local compiler, planner, manifest, role, generation, and scaffold failures use
one bounded versioned diagnostic shape containing a stable stage/code, source
path/span when one exists, symbolic path, closed cause/fix codes, file-change
disposition, and retry classification.

The diagnostic never contains credentials, submitted runtime values, hidden
schema, arbitrary engine prose, or internal sources. Machine output and MCP are
value-free. A local human renderer may point into the caller-owned local source
without copying source literals into logs or persistent evidence.

### Scaffold and public kit

`riffdb new <name> --directory .` accepts only an existing empty regular
writable directory. It never follows the destination as a symlink or overwrites
a file. The normal new-child form remains supported.

The public release bundle includes a complete contract-language reference,
command/invariant cookbook, machine-readable manifest schema, inspection
commands, domain-neutral examples, negative diagnostic examples, and equivalent
builder-MCP resources.

The TypeScript scaffold is a complete offline-buildable server-side web
application with exact product runtime and toolchain inputs. Rust and TypeScript
consume the same application lock and operation schemas.

Builder MCP exposes the same bounded local describe, check, diagnostic,
lock-preview, explicit lock-write, and generation operations as CLI. Builder
MCP has no storage access, deployment authority, role-binding authority, or
credential.

### Rehearsal and evaluation

Before another official four-run campaign, a sealed public-only rehearsal must
complete Blog and Orders in both languages and exercise invalid source, stale
lock, missing index, unsafe role, and interrupted generation. Two fresh canary
agents must then complete Blog/Rust and Orders/TypeScript with zero product
workarounds and ratings of at least 8.5.

Evaluation metrics count first write/read only when a generated RiffDB operation
returns the expected exact identities. Failed campaigns remain immutable under
separate campaign directories. A later campaign never deletes or rewrites Terra
campaign 01.

The official gate thresholds remain unchanged.

### Performance remains an independent gate

The later report of synchronous redb command throughput degrading with database
growth is not repaired by weakening acknowledgement durability. A dedicated
package must reproduce and decompose that behavior. Changing redb two-phase
commit, enabling production group scheduling, changing acknowledgement
semantics, or changing atomic record layout requires a separate accepted
durability/performance ADR and crash evidence.

The alpha rehearsal records growing-database seed and command rates so an
ergonomically successful client cannot mask unusable storage behavior.

## Compatibility

The source-manifest and lock formats are additive versioned local artifacts.
They change no existing public Protobuf field, durable database record, storage
key, command/query plan hash, command execution semantics, or kernel API.

V1 exact manifests and generated clients remain supported. Production
deployment continues to select exact immutable contract and query-module
identities. Any new wire operation is separately reviewed; this decision does
not require one.

## Security

Compiler ownership removes a manual identity-forging opportunity while
preserving exact review. Lock writing is local, bounded, staged, deterministic,
and non-authorizing. Development auto-refresh is confined to a product-owned
ephemeral database and development role. Production never auto-refreshes.

Diagnostics are redaction-safe and bounded. Builder MCP cannot deploy, bind,
read storage, execute a command, or obtain a credential.

The recovery adds no SQL, arbitrary join, unbounded read, client-side semantic
composition, generic write, transaction callback, or kernel escape.

## Work-package mapping

- WP-345: source manifest, exact lock, V1 compatibility, and local operations.
- WP-350: complete authoring diagnostics.
- WP-355: empty-directory scaffold and public authoring kit.
- WP-360: TypeScript and builder-MCP parity.
- WP-362: durability-preserving growing-database performance investigation.
- WP-365: public-only rehearsal, evidence correction, and canaries.
- WP-370: official Agent Application Alpha campaign 02.
