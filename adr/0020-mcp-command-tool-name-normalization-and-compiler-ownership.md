# ADR-0020: MCP Command Tool-Name Normalization and Compiler Ownership

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13; status cross-reference amended 2026-07-22
- **Accepted:** 2026-07-13
- **Requires:** ADR-0002, ADR-0006, ADR-0007, and ADR-0013
- **Amends:** ADR-0008 by moving the command tool-name grammar,
  normalization, collision handling, and compiler/catalog ownership into this
  accepted record; ADR-0008 and ADR-0040 later accepted the remaining resource
  URI, audience, transport, cursor-presentation, and MCP-fixture decisions at
  acceptance reference `21a8cfb`
- **Decision deadline:** Before WP-040 emits a compiled contract bundle

The human maintainer accepted the exact command tool-name rule and its early
compiler/catalog ownership on 2026-07-13. At that acceptance, this record
accepted no other part of then-Proposed ADR-0008.

## Context

Contract source identifiers are case-sensitive ASCII and are not normalized.
MCP command tools require a lowercase public name. If normalization is deferred
to WP-140, the compiler and catalog could accept a contract whose commands
collide only at the public MCP boundary. Repairing such a collision with a hash
or population-dependent suffix would make a command's public name unstable.

The name is derivable from compiler-owned contract and command identifiers and
belongs with the transport-neutral command metadata and JSON Schemas. MCP is a
policy-filtered adapter over the shared service; it must consume this compiled
identity rather than create another naming implementation.

## Decision

### Exact v1 name

Every compiled command has one MCP command tool name:

```text
riffdb.cmd.<contract-segment>.<command-segment>
```

Each segment is produced from the exact source contract or command identifier
by mapping every ASCII `A` through `Z` byte to the corresponding ASCII lowercase
byte. ASCII `a` through `z`, digits, and underscores are preserved byte for
byte. No separator insertion, word splitting, Unicode case conversion,
trimming, escaping, or other normalization occurs.

The source grammar already excludes every other byte. After lowercase mapping,
each segment must match `[a-z][a-z0-9_]*`. A source identifier beginning with an
underscore therefore cannot name an MCP-exposed contract or command. Empty
segments reject. The complete ASCII name, including `riffdb.cmd.` and both dots,
must be at most 128 bytes. A 129-byte name rejects; it is never truncated.

The fixed prefix and dot separators are literal and lowercase. The primary
golden is:

```text
source contract: Legal_Spend
source command:  Allocate_Budget2
tool name:       riffdb.cmd.legal_spend.allocate_budget2
```

The canonical budget contract's unchanged source identifiers provide a second
golden and demonstrate that this rule never invents word boundaries:

```text
source contract: LegalSpend
source command:  AllocateBudget
tool name:       riffdb.cmd.legalspend.allocatebudget
```

### Collision and compatibility rules

Normalization is many-to-one because source identifiers are case-sensitive.
WP-040 rejects a contract when two commands in the compiled name registry have
the same complete normalized name. This includes collisions caused only by
ASCII case. It also rejects an invalid segment or an over-length complete name.
It must not append an ordinal, hash, contract version, plan hash, or other
suffix.

The generated command-name registry is a versioned, deterministic compiler
artifact in the immutable contract bundle. Entries are ordered by stable
`CommandId`; each entry binds the command's lineage, `CommandId`, exact source
identifier, and complete v1 tool name. The registry is covered by the contract
bundle's canonical encoding and hashes under ADR-0013. Adding a later naming
version requires a compatibility decision and distinct registry version; an
implementation must not reinterpret v1 bytes in place.

WP-050 independently revalidates the trusted compiled registry during catalog
activation: every executable command appears exactly once, every entry agrees
with the deterministic v1 derivation, IDs and lineage agree with the bundle,
ordering is canonical, and complete names are unique. Invalid compiled metadata
rejects activation. The catalog never repairs or regenerates a different public
name.

Changing a source contract or command identifier in a way that changes this
name is an MCP public compatibility change, even if command semantics are
otherwise compatible. Contract compatibility reporting must expose that change
before activation.

### Ownership and layering

- `riffdb-contract-ir` owns the checked versioned command tool-name and registry
  value shapes.
- `riffdb-contract-compiler` owns exact derivation, validation, collision
  rejection, deterministic ordering, compatibility reporting, and fixtures.
- `riffdb-catalog` owns activation-time revalidation and preservation of the
  compiled registry.
- `riffdb-service` exposes policy-filtered command descriptors carrying the
  compiled name; it does not normalize names.
- `riffdb-api-mcp` consumes the name verbatim for discovery and invocation
  dispatch, reauthorizes every invocation through the shared service, and does
  not access storage directly.

Neither the name nor discovery visibility is authorization. Stale or unknown
names fail closed. The fixed name contains no tenant, actor, capability,
database, version, key, or caller-controlled transport data.

## Deferred MCP Decisions

This record deliberately does not accept or freeze:

- resource URI grammar, percent encoding, or resource identity fixtures;
- HTTP audience and canonical endpoint text;
- stdio-over-gRPC protocol and operational fixtures;
- cursor text representation;
- MCP descriptions, result text, progress, pagination, or cancellation text;
- JSON-to-canonical-value conversion fixtures beyond the compiler-owned JSON
  Schemas.

ADR-0008 and ADR-0040 later accepted those decisions on 2026-07-22. Their named
future exact-byte checkpoints still require review before the first dependent
public fixture merges. This split does not permit WP-040 or WP-050 to implement
MCP transport behavior.

## Alternatives Considered

1. **Normalize and reject in the compiler and revalidate in the catalog:**
   accepted; invalid public names never enter an active bundle.
2. **Normalize only in the MCP adapter:** rejected; collisions would be found
   after compilation and could diverge across adapters.
3. **Require source identifiers to be lowercase globally:** rejected; it would
   incompatibly narrow the accepted contract grammar for a transport concern.
4. **Append hashes or ordinals on collision:** rejected; public identity would
   depend on unrelated catalog contents or an extra algorithm.
5. **Use stable numeric IDs as public names:** rejected; they are opaque and do
   not provide the required qualified command identity.

## Compatibility and Testing

WP-040 freezes:

- primary, uppercase, digit, underscore, leading-underscore, 128-byte, and
  129-byte fixtures;
- case-only and exact normalized-collision diagnostics with stable source spans;
- deterministic registry ordering and bundle/hash reproduction;
- compatibility reports for unchanged and renamed public names; and
- property tests that valid outputs match the exact grammar and derive
  identically independent of input declaration order.

WP-050 freezes activation rejection for missing, duplicate, reordered,
misbound, noncanonical, over-length, or derivation-mismatched entries. WP-140
freezes discovery and invocation use of the compiled names, stale-name denial,
authorization, and adapter non-normalization. WP-200 provides final end-to-end
evidence.

## Requirements and Work Packages

- **Requirements:** `CMP-010`, `CMP-020`, `CMP-021`, `MCP-020`
- **Defines or blocks:** `WP-040`, `WP-050`, `WP-120`, `WP-140`, `WP-185`
- **Final evidence:** `WP-200`
