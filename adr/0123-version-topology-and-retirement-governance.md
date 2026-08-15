# ADR-0123: Version Topology and Retirement Governance

- **Status:** Accepted
- **Date:** 2026-08-15
- **Decision owners:** RiffDB maintainers
- **Direction approved:** 2026-08-15 (maintainer, in session)
- **Exact text accepted:** Yes — 2026-08-15, maintainer acceptance as written
- **Requires:** ADR-0002, ADR-0006, ADR-0013, ADR-0057, ADR-0112, and ADR-0120
- **Defines or blocks:** WP-612 and the first alpha release checklist

## Context

RiffDB correctly versions many boundaries independently: storage envelopes,
record revisions, the redb layout, journal frames and extents, contract bundles
and executable IR, query modules, migration bundles, application sources and
locks, Protobuf packages, MCP, driver protocols, CLI output, backups, receipts,
exports, generated packages, and simulator traces. Those numbers describe
different compatibility problems and must not advance in lockstep.

The ownership is nevertheless scattered across Rust constants, schemas,
generated fixtures, accepted ADRs, and release manifests. A reviewer can prove
one boundary at a time, but cannot currently answer four repository-wide
questions mechanically:

1. Which version domains ship in this release, and who owns each one?
2. Which identities can the current release read and write?
3. Does a change require an additive revision, a new domain version, a durable
   writer transition, or a breaking alpha epoch?
4. When may an old decoder and its fixtures be retired?

This is a control-plane gap, not evidence that the domains should be collapsed.
A single global version would couple unrelated changes, hide the decoder that
actually owns compatibility, and encourage incorrect comparisons between such
things as an MCP date baseline and a storage writer number.

## Decision

### 1. Keep domain versions separate; centralize their governance

RiffDB will publish one canonical, machine-readable version topology. Each
release-significant domain has one record containing:

- a stable domain identifier, human title, classification, and owning component;
- authoritative source locators and the exact current identities they pin;
- the complete readable identity set and writable identity set;
- a writer-selection policy;
- the compatibility rule and the conditions that make a change breaking;
- lifecycle state and decoder-retirement evidence;
- fixtures and checks that falsify the claim; and
- the supported-release or external-standard posture that bounds the promise.

The topology is an inventory and review gate. It does not become a runtime
super-version, alter encoded bytes, authorize a decoder, or replace an owning
format registry. The durable-format manifest remains authoritative for physical
database compatibility; the topology cross-checks and links it.

### 2. Use a closed classification and writer-policy vocabulary

The classifications are:

- `durable`: database, journal, changelog, backup, or receipt bytes;
- `executable_ir`: compiler-produced executable or migration artifacts;
- `wire`: public network message schemas and envelopes;
- `protocol`: negotiated or fixed interaction baselines;
- `application_artifact`: source, lock, manifest, role, or module artifacts;
- `generated_surface`: published generated bindings or package interfaces; and
- `evidence`: test traces and compatibility-fixture formats that must replay.

Writer policies are:

- `single_current`: every new artifact uses one current identity;
- `least_sufficient`: the producer emits the oldest registered identity that
  represents the requested semantics without loss;
- `multi_current`: more than one identity is intentionally emitted for distinct
  closed artifact classes; and
- `external_baseline`: RiffDB implements one externally owned protocol identity.

Readable and writable identities are explicit ordered lists, never an inferred
numeric range. A number being smaller than the current number proves nothing.

### 3. Classify changes at the boundary that owns them

Every version-affecting change is one of:

- `no_format_change`: implementation changes while canonical bytes and public
  semantics remain identical;
- `additive_same_identity`: permitted only where the owning accepted ADR and
  compatibility fixtures explicitly define unknown/additive behavior;
- `new_domain_identity`: retain old readers as declared and write under a new
  identity or under a least-sufficient policy;
- `writer_transition`: durable readers remain in the same alpha epoch but an
  exact receipted upgrade changes the writer identity; or
- `breaking_epoch`: ADR-0112's backup, export/reimport, reconciliation, refusal,
  release-note, and retained-old-binary ceremony is mandatory.

Public source/API compatibility follows the same review record but is released
under SemVer and the exact generated application lock. Pre-1.0 status permits a
declared breaking minor release; it does not permit an unclassified silent
break. Protobuf field numbers, enum values, durable tags, and other reserved
identifiers remain reserved permanently after publication even when their
decoder retires.

### 4. Decoder retirement is evidence-driven and forward-only

A decoder moves through `active`, `read_only`, `retirement_candidate`, and
`retired`. WP-612 retires no existing decoder.

Retirement requires all of the following in one reviewed change:

1. no release in the declared supported-release window writes the identity;
2. at least one previously published release carried it as `read_only` and
   announced the future retirement;
3. every supported upgrade edge, physical backup range, application migration,
   or export/reimport path either consumes it or refuses before mutation;
4. frozen fixtures continue to prove the last readable behavior, and a
   retirement fixture proves the new typed refusal;
5. release notes name affected artifacts, the last reader, the replacement,
   required operator action, downtime, backup, and downgrade posture;
6. reserved tags, fields, enum values, hashes, and symbolic identities are not
   reused; and
7. the topology, owning registry, generated artifacts, and release verification
   change atomically.

`retired` means current binaries intentionally refuse the identity. It does not
mean historical fixtures or identifier reservations may be deleted. An
emergency security retirement may skip the notice interval only through a new
accepted ADR that names the threat and safe recovery path.

### 5. CI owns drift detection

`scripts/check-version-topology` validates canonical ordering, the closed
vocabulary, unique domain identities, nonempty evidence, writer/read
consistency, lifecycle completeness, source assertions, and durable-manifest
agreement. It runs from `scripts/check-generated`.

A source version, reader window, writer window, protocol baseline, or durable
manifest claim cannot change green without updating the topology and its
classification. Adding a genuinely new domain requires a new topology record;
the review checklist treats an unregistered release-significant version as a
release blocker.

## Options Considered

1. **One global RiffDB format version.** Rejected: it conflates unrelated
   compatibility domains and cannot state which decoder or migration is needed.
2. **One version number per crate.** Rejected: crate boundaries do not coincide
   with wire, durable, or application-artifact ownership.
3. **Continue with independent registries and documentation only.** Rejected:
   existing local checks are strong but cannot detect cross-registry drift or
   answer the retirement question consistently.
4. **Central topology over independently owned versions.** Selected: it adds a
   small enforceable control plane without changing runtime formats.

## Consequences

- Releases gain one reviewable map from each visible version to its owner,
  compatibility promise, evidence, and retirement state.
- Version numbers continue to advance only when their own boundary changes.
- Existing formats, readers, writers, hashes, and public protocols remain
  unchanged by WP-612.
- Adding or retiring a version requires more explicit metadata and fixtures;
  this is intentional release work, not runtime overhead.
- The first alpha freezes the initial topology baseline. It does not promise
  every pre-alpha decoder forever.

## Compatibility

WP-612 changes no runtime, public wire, executable IR, application artifact, or
durable encoding. The topology schema is itself a release artifact at
`riffdb.version-topology/v1`; incompatible topology-schema evolution requires a
new topology schema identity while old release manifests remain readable as
historical evidence.

## Security

The topology contains version identities, repository paths, hashes, and check
names only. It contains no credentials, data values, filesystem deployment
paths, or authority. It cannot enable fallback decoding, downgrade, reset, raw
import, or an application-visible opt-out. Unsupported inputs continue to fail
closed at their owning boundary.

## Standing Design Tests

- **Interface safety:** the registry is descriptive and check-time only. It
  cannot grant runtime compatibility or weaken the public safe surface; every
  migration and decoder remains owned by its existing accepted interface.
- **Scale:** the inventory and each source assertion are bounded. The checker
  reads only declared repository files and manifests and performs no database
  scan or network operation.

## Testing

- Schema, vocabulary, ordering, uniqueness, lifecycle, and reader/writer
  consistency checks over the topology.
- Exact source assertions for each current version authority and external
  baseline.
- Durable entries cross-checked against
  `release/durable-format-manifest-v1.json`.
- Negative self-tests for an unregistered bump, a writer outside the reader
  set, a missing retirement gate, a reordered domain, and a durable-manifest
  mismatch.
- `check-generated`, handbook, links, and requirement coverage remain green.

## Requirements and Work Packages

- **Carries forward:** AFC-008 and AFC-010.
- **Defines:** WP-612.
- **Future requirement registration:** if accepted, `VER-001` through
  `VER-008` will register topology completeness, explicit windows, source
  pins, change classification, durable cross-checking, retirement ceremony,
  reserved-identity permanence, and CI enforcement.

## Decision Deadline

Exact acceptance is required before WP-612 freezes the topology schema or CI
rejects version changes under this policy.
