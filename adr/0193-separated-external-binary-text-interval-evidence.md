---
adr: 0193
title: Separated External Binary-Text Interval Evidence
status: accepted
tier: surface
date: 2026-09-03
accepted: "2026-09-03"
requires: [ADR-0055, ADR-0108, ADR-0112, ADR-0124, ADR-0129, ADR-0148, ADR-0150, ADR-0154]
amends:
  - ADR-0154 section 7 and its testing and package allocation only by separating repository-local conformance from the required external execution receipt
supersedes: []
requirements: [OQ-061]
packages: [WP-701, WP-775]
obligations:
  - id: OBL-0193-1
    package: WP-701
    proof: generated_binary_text_interval_surfaces_preserve_logical_strings
    says: Repository-local generated Rust, Go, TypeScript, Python, CLI, MCP, local-driver, and remote-gRPC proofs preserve the exact bytewise interval semantics and return logical strings rather than physical bytes.
  - id: OBL-0193-2
    package: WP-775
    proof: external_binary_text_interval_execution_receipt_is_fresh_value_free_and_exact
    says: A fresh owning-repository receipt proves actual strict-lower and strict-upper execution, bytewise ascending logical-string results, and reuse of the returned consumer-owned logical token.
  - id: OBL-0193-3
    package: WP-775
    proof: historical_binary_text_interval_receipt_remains_byte_exact_and_nonterminal
    says: The historical WP-701 source-only receipt remains byte-exact and cannot satisfy or be upgraded into the external execution proof.
review_triggers:
  - ADR-0154 text, OQ-061 semantics, or any accepted binary-text interval byte, identity, comparator, bound, or public surface would change.
  - WP-701 would claim external execution, WP-775 would close from the historical receipt, or either package's proof would substitute for the other package-scoped obligation.
  - External schema, route, source, generated profile, authorization vocabulary, artifact, value, or database-owned continuation token would enter this repository.
  - The external run would omit actual execution or returned-token reuse, or would add client filtering, sorting, cursor walking, duplicate indexes, framework claims, or unbounded evidence.
  - Any evidence identity other than the new riffdb.external-binary-text-interval-execution/v1 receipt, or any public or durable format, Protobuf field, query identity, plan, module, lock, cursor, generated binding, storage key, or historical fixture would change.
---
# ADR-0193: Separated External Binary-Text Interval Evidence

## Context

ADR-0154 and OQ-061 require both repository-local generated and transport
conformance and one value-free external consumer receipt. WP-701 currently owns
both. Its historical receipt proves source generation and compilation, but it
explicitly records `external_runtime_execution_claimed: false`; treating it as
execution evidence would weaken the accepted requirement.

External execution depends on coordination with the owning consumer repository.
That coordination should not delay closure of complete repository-local work or
permit an unrelated receipt to satisfy a globally tagged requirement.

## Decision

1. ADR-0154 remains byte-exact and fully authoritative. This amendment changes
   only evidence custody: WP-701 owns repository-local generated, public, and
   transport conformance; WP-775 owns the fresh external execution receipt.
   Complete acceptance of OQ-061 still requires both package-scoped obligations.

2. WP-701 proves identical strict binary UTF-8 interval behavior through
   generated Rust, Go, TypeScript, Python, CLI, MCP, the local driver, and remote
   gRPC. Every surface returns the original logical strings, never physical
   index bytes. WP-701 may close that local scope without claiming external
   consumer execution, and is explicitly `surface` tier because this amendment
   changes the allocation of a public acceptance obligation.

3. WP-775 depends on WP-701 and obtains a fresh receipt from the owning consumer
   repository. The distinct terminal evidence identity is
   `riffdb.external-binary-text-interval-execution/v1` with status `passed`.
   The receipt is checksum-bound to exact consumer, RiffDB, and generated-
   artifact identities and proves an actual strict-lower/strict-upper query run
   in bytewise ascending order. It also proves that the consumer uses a returned
   original logical string as its next consumer-owned request token. Client
   filtering, sorting, page walking, duplicate indexes, database-owned external-
   token conversion, and framework or parity claims remain forbidden.

4. External evidence is value-free. It may contain only bounded row, operation,
   and timing counts; exact source and artifact identity hashes; and closed
   verification facts. External schema names, routes, adapter source, generated
   profiles, authorization vocabulary, fixtures, artifacts, and values remain
   in their owning repository. No external token becomes a RiffDB cursor.

5. The historical receipt
   `fixtures/riffql/wp701-external-binary-text-interval-capability-v1.json` is
   immutable source/compilation evidence. Its SHA-256 is
   `22e4a8a69869fd4867a578319a9a80d5ed2d84a37f372a64e3dfe3354ee12673`.
   It is outside WP-775's writable paths and may not be edited, relabelled,
   copied, upgraded, or accepted as execution proof.

6. The current V5 capability fixture is immutable at SHA-256
   `18b7659b3d20853c6171a49e63981e3d7542094f063eb584978600eecd50737c`
   with top-level status `development`. Neither package may edit, relabel, copy,
   upgrade, or advance it. The new WP-775 execution-v1 receipt is the separate
   terminal `passed` evidence and binds both this exact V5 hash and the exact
   historical WP-701 receipt hash above. It does not become a V5 successor or
   alter any V5 field, reference, status, or byte.

7. The obligations above are package-scoped. A global `OQ-061` requirement tag
   or a proof owned by one package cannot discharge the other package's work.
   WP-775 retains the one-way hard dependency on WP-701; WP-701 does not depend
   on external coordination.

8. This proposal, its generated index row, and the package allocation edits are
   one governance review range, not WP-701 or WP-775 implementation or closure
   evidence. WP-775 implementation begins only after this exact amendment is
   accepted and merged. This record authorizes only the new versioned evidence-
   receipt identity named above; it authorizes no public application or durable
   identity, Protobuf, language, comparator, bound, authority, or runtime change.

## Consequences

Repository-local acceptance can close when its complete generated and transport
matrix is green, while external execution remains visible and independently
fail-closed. Reviewers can distinguish the historical source-only receipt from
the fresh execution proof without editing the original accepted ADR text or any
existing fixture bytes.

## Standing design tests

- **Interface safety:** generated callers retain only named-query values and an
  opaque RiffDB cursor. The external consumer reuses its own returned logical
  string and gains no physical comparator, index, storage, cursor, or authority.
- **Scale:** both packages use finite endpoints, bounded pages, declared output
  and work ceilings, and bounded value-free evidence. No population-sized or
  duration-proportional state enters RiffDB.

## Checks

- `generated_binary_text_interval_surfaces_preserve_logical_strings` proves the
  complete repository-local surface matrix and logical-string return rule.
- `external_binary_text_interval_execution_receipt_is_fresh_value_free_and_exact`
  proves fresh execution, identity binding, bytewise order, token reuse, and
  forbidden-client-work absences.
- `historical_binary_text_interval_receipt_remains_byte_exact_and_nonterminal`
  freezes the historical receipt hash and rejects it as execution evidence.
- `scripts/generate-operational-query-capability --check` preserves immutable V5
  and validates the distinct execution-v1 receipt and both checksum bindings.
