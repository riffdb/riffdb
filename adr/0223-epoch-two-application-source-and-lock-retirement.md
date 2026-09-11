---
adr: "0223"
title: Epoch-Two Application Source and Lock Retirement
status: accepted
tier: guarantee
date: 2026-09-08
accepted: "2026-09-11"
requires: [ADR-0074, ADR-0079, ADR-0181]
amends: [ADR-0074, ADR-0079]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, PYD-008, PYD-009, VER-001, VER-002, VER-003, VER-004, VER-008]
packages: [WP-757]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - PYD-008 or PYD-009 would remain a live epoch-two compatibility promise rather than historical epoch-one evidence.
  - An application source or lock V1/V2 reader, writer, parser, encoder, locator, alias, fixture, or generated artifact would remain in epoch two.
  - The local `riffdb application migrate --to v2` command or either of its preview/write modes would remain discoverable or callable.
  - An input beyond the total byte bound would be parsed, or a field other than the top-level schema member would be decoded or validated before a retired or unknown schema is refused.
  - A predecessor source or lock would reach semantic lowering, canonicalization, generation, locking, filesystem effects, deployment, authority binding, or network access before refusal.
  - Historical byte/hash evidence would preserve a live fixture, compatibility artifact, topology identity, or decoder contrary to WP-757.
  - Current source/lock semantics, application authority, generated behavior, storage, protocol, or transaction ordering would change.
---
# ADR-0223: Epoch-Two Application Source and Lock Retirement

## Context

ADR-0181 and WP-757 require every epoch-two topology domain to have one equal
readable, writable, and current identity, with superseded decoders, fixtures,
generated artifacts, and topology rows deleted during the pre-external window.
Application source and lock are such domains. Their current identities are
`riffdb.application-source/v7` and `riffdb.application-lock/v8`.

PYD-008 and ADR-0074 instead permanently retain source and lock V1 bytes and
meaning, require V2 as an additive successor, and preserve V1/V2 compatibility
fixtures. PYD-009 requires the local
`riffdb application migrate --to v2 --write` operation, while ADR-0079 names
its preview command as an existing surface. Keeping those promises in epoch two
would force extra readable identities and a predecessor-only command, directly
weakening the accepted reset. Implementation cannot choose between them.

## Decision

1. PYD-008 and PYD-009 are historical epoch-one guarantees. Their exact
   accepted V1/V2 bytes, meanings, migration preview, atomic source-only write,
   and lock-review boundary remain truthful for epoch-one releases and are not
   reinterpreted. They impose no reader, writer, fixture, command, or public
   compatibility requirement on an epoch-two binary.

2. At the ADR-0181 epoch-two ceremony, application source and lock follow the
   same pre-external retirement rule as every other ordinary topology domain.
   `riffdb.application-source/v7` is the sole readable, writable, current, and
   active source identity, and `riffdb.application-lock/v8` is the sole
   readable, writable, current, and active lock identity. V1 and V2 are
   deleted from every live lifecycle set and permanently reserved. WP-757's
   existing obligation to delete every other predecessor remains unchanged;
   this record creates no exception for V3 through V6 source or V3 through V7
   lock.

3. WP-757 deletes source and lock V1/V2 constants, descriptors, parsers,
   encoders, readers, writers, source locators, aliases, dispatch, compatibility
   fixtures, generated artifacts, catalog or inventory rows, and topology
   identities. Their symbolic identities, numeric versions, reserved fields,
   and historical hashes must never name a new encoding.

4. Every application-source and application-lock entry point enforces its
   total input-byte bound first, then performs only the bounded syntactic JSON
   decoding needed to extract and validate the top-level `schema` member. A
   retired or unknown schema is refused before decoding or validating any
   other field, semantic lowering, canonicalization, generation, lock creation
   or checking, filesystem mutation, deployment, authority binding, seed work,
   or network access. There is no fallback, negotiation, reinterpretation,
   migration mode, or caller-selected compatibility path.

5. The complete local command `riffdb application migrate --to v2`, including
   preview and `--write`, is retired at the ceremony and removed from CLI
   parsing, help, generated CLI reference, tests, examples, and handbook text.
   No replacement migration command is introduced. An author presents the
   sole-current source to the existing explicit application lock/check flow;
   an old source receives the closed version refusal and is not rewritten.

6. Every first-party consumer, scaffold, template, example, application
   fixture, generated application, script, test, and public handbook page is
   updated to the sole-current source and lock identities or deleted when its
   only purpose was predecessor compatibility. SPEC records PYD-008 and
   PYD-009 with their epoch-one scope; ADR-0074's Application Format V2,
   Compatibility, Testing, Consequences, and WP-395 mapping are historical for
   epoch one; ADR-0079 no longer describes the removed local command as an
   existing surface.

7. WP-757 must delete predecessor byte fixtures as ADR-0181 requires. Exact
   old bytes or hashes may remain only where they are already immutable
   historical decision or repository-history evidence and cannot be loaded,
   generated, checked, installed, discovered, or treated as compatibility
   evidence by current tooling. A checked-in fixture, generated artifact,
   topology row, live catalog entry, or executable hash assertion is not
   historical evidence and must be deleted. If the WP-757 inventory cannot
   distinguish a proposed retained record from a live predecessor artifact,
   the record is deleted rather than exempted.

8. This retirement adds no application operation or migration authority and
   changes no semantics of source V7, lock V8, generated operations, Python or
   other SDK values, compiler output for current input, authorization,
   redaction, protocol, authoritative storage, database ceremony, transaction,
   publication, acknowledgement, or recovery. It is exactly the breaking-epoch
   surface deletion already authorized by ADR-0181, not an in-place upgrade.

9. No new obligation is created. WP-757's existing sole-identity topology,
   ceremony, fixture-inventory, generated-application, requirement, and
   compatibility proofs must cover these removals without weakening their
   exact names or assertions.

10. This proposal changes only this record and the generated ADR index. It
    remains ineffective until exact human acceptance and authorizes no WP,
    source, lock, CLI, fixture, generated, documentation, or SPEC edit by
    itself.

## Options considered

1. Keeping V1/V2 readers and the migration command is rejected because it
   directly violates ADR-0181's one-identity epoch-two topology.
2. Exempting application source and lock like authoritative changelog is
   rejected because no accepted bounded compatibility exception exists and no
   external consumer requires one.
3. Reinterpreting or automatically upgrading V1/V2 is rejected because it
   changes their accepted meaning and bypasses fail-closed author review.
4. Epoch-scoping PYD-008/PYD-009 and deleting the predecessor surface is chosen
   because it preserves historical truth without weakening WP-757.

## Consequences

- Epoch two has one application source identity and one lock identity, and no
  obsolete local migration command or hidden predecessor decoder.
- Existing author repositories using V1/V2 must be regenerated or rewritten
  outside the epoch-two binary before current application checking; the binary
  deliberately offers no conversion path.
- The handbook, generated reference, examples, fixtures, and tests lose the
  additive V1-to-V2 migration walkthrough and describe only current authoring.
- Online source/lock migration, a compatibility service, and any post-external
  retirement policy change remain deferred.

## Standing design tests

- **Interface safety:** applications and agents cannot select an old format,
  request a conversion, opt into fallback, or gain deployment or migration
  authority; predecessor input is refused before it can affect semantics or
  state.
- **Scale:** the total byte ceiling bounds the syntactic JSON pass needed to
  extract the top-level schema, and refusal precedes decoding or validation of
  every other field; current compiler, generation, lock, and filesystem bounds
  remain unchanged, with no scan, migration queue, retained decoder matrix, or
  unbounded diagnostic.

## Checks

- WP-757's sole-current application-operation proof, topology single-identity
  checks, epoch-two fixture inventory, ceremony evidence, and generated-source
  checks prove that no V1/V2 source, lock, command, fixture, or dispatch remains
  and that current artifacts are independently regenerated.
- CLI help/reference generation proves `application migrate` is absent;
  bounded hostile-input tests prove the total byte ceiling is enforced first
  and that V1/V2 or unknown top-level schema values refuse after only schema
  extraction, before any other field decode or validation and before
  filesystem or network effects.
- Handbook, requirement coverage, ADR obligations, allowed paths, workspace
  tests, clippy, and `ci-all` preserve current behavior and the historical-only
  scope of PYD-008/PYD-009.
