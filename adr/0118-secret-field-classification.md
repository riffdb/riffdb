# ADR-0118: Secret Field Classification and Display-Surface Redaction

- **Status:** Accepted
- **Direction approved:** 2026-08-11
- **Exact text accepted:** Yes — 2026-08-11, maintainer acceptance as written
- **Decision deadline:** Before WP-597 changes the contract grammar or any
  public error/diagnostic surface

## Context

Authentication-shaped workloads store values whose accidental display is a
security incident: session tokens, verification tokens, credential hashes,
provider refresh secrets. The contract language today has no field
classification vocabulary — nothing marks a field as unsafe to echo — so
redaction depends on every surface author remembering which fields are
sensitive. The house redaction rule (AGENTS.md: redaction before logging,
metrics, MCP text, or public errors) exists but has no compiler-carried
anchor for application-declared secrets, and the alpha's rescoped adapter
set (ADR-0117) makes the gap gate-relevant.

One boundary must be drawn precisely: durable storage and backups remain
full-fidelity. A backup that silently dropped or masked secret fields would
restore a broken application; what is stored for an auth workload is already
the appropriate at-rest form (hashes, opaque tokens). The guarantee this
record adds is about *display surfaces*, not storage.

## Proposed Decision

1. **Grammar and IR.** A field may be declared `secret`. The classification
   is carried through AST, typed IR (versioned encoding with compatibility
   fixtures), bundle hash, generated catalogs, and generated bindings.
2. **The display-surface guarantee.** A secret-classified field's value MUST
   NOT appear in: tracing/log output, typed public errors and diagnostics
   (including compiler and admission diagnostics that echo input), MCP tool
   output text, CLI human/JSON rendering of records unless explicitly
   requested through a surface that requires read authority for that field,
   provenance and audit summaries, health/telemetry output, and generated
   example/documentation output. Redaction is structural: the surfaces
   consume a value wrapper whose display/serialize-for-diagnostics forms are
   redacted, so forgetting is unrepresentable rather than discouraged.
3. **Field visibility defaults deny.** Secret fields are excluded from
   capability field-visibility grants unless named explicitly; a wildcard or
   role default never includes them. Reads that project a secret field
   require that explicit visibility; everything else (predicates on the
   field, uniqueness, index participation) works without ever returning the
   value.
4. **Storage, backup, export.** Durable records, backups, and ADR-0112/0115
   exports carry secret fields at full fidelity under their existing
   authority rules; export receipts and diagnostics never echo the values.
   Changelog frames (ADR-0100) likewise carry them; follower-side display
   surfaces inherit the same wrapper.
5. **Not in scope.** Encryption-at-rest, key management, and field-level
   crypto are explicitly not this record; `secret` is a display-and-
   visibility classification, not a cryptographic promise, and the handbook
   page must say so in those words.

## Options Considered

1. **Convention plus review** (status quo): the gap this record closes;
   rejected.
2. **Redaction at each surface by field-name heuristics**: payload names as
   authority — the exact anti-pattern ADR-0116 just rejected for events;
   rejected.
3. **Encrypt-instead-of-classify**: different problem, heavier machinery,
   still needs display redaction anyway; deferred as a possible future ADR.
4. **Compiler-carried classification with structural redaction** — accepted.

## Consequences

- Application authors (and generators per ADR-0117) declare secrecy once;
  every present and future display surface inherits it.
- A new surface that renders field values must consume the redacting wrapper
  to compile, making the guarantee grow with the codebase.
- Secret fields become slightly less ergonomic to debug by design; the CLI
  explicit-reveal path (with field visibility authority) is the escape.
- Existing contracts are unaffected until they adopt the keyword; adopting
  it is a compatible contract evolution under the normal migration rules.

## Compatibility

Additive grammar keyword; versioned IR successor with fixtures; bundle-hash
rotation follows the exact-identity ceremony. No durable record format
changes — classification lives in the contract/IR, not in stored rows. Wire
DTOs gain no new field; generated bindings mark secret fields in their types
where the language allows.

## Security

Default is deny at every display surface and in field-visibility grants.
Redaction is fail-closed: an unclassified surface change cannot leak what it
cannot obtain unredacted. The classification itself is not a secret;
diagnostics may name a field as secret-classified.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no public surface can be
  asked to display a secret field without explicit field-visibility
  authority, and no error/diagnostic path can echo one at all; the wrapper
  makes the unsafe rendering unexpressible rather than reviewed-against.
- **Scale:** classification is per-field static metadata; zero per-row or
  per-query cost beyond the existing field-visibility check.

## Testing

- Grammar/IR/source-span tests plus compatibility fixtures for the
  classification through bundle hash and generated catalogs.
- A redaction sweep test per display surface: a fixture contract with secret
  fields drives log capture, public-error rendering, MCP output, CLI both
  modes, provenance/audit summaries, and telemetry, asserting the value's
  absence and the redaction marker's presence.
- Field-visibility default-deny tests: wildcard grants exclude secrets;
  explicit grants reveal; predicates and unique enforcement function without
  read visibility.
- Negative-example documentation entry showing the classification and what
  it does not promise (no crypto).

## Requirements and Work Packages

- **Requirements:** to be registered as `SEC-F-*` (secret-field family) in
  `SPEC.md` at package time, traceable to §Proposed Decision items 2–4.
- **Defines or blocks:** WP-597 (implementation); feeds ADR-0117's gate
  workload shapes.

## Decision Deadline

Exact acceptance before WP-597 merges grammar, IR, or any redaction-surface
change.
