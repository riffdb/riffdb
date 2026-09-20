---
adr: "0241"
title: Rust 1.98.1 Toolchain Upgrade
status: accepted
tier: surface
date: 2026-09-19
accepted: 2026-09-19
acceptance: 'maintainer, in session, 2026-09-19: "i approve the text"
  (ADR-0241 as written)'
requires: [ADR-0171, ADR-0239]
amends: []
supersedes: []
requirements: []
packages: [WP-792]
obligations:
  - id: OBL-0241-1
    package: WP-792
    proof: scripts/check-workspace-policy
    says: Every workspace package declares exactly the pinned toolchain as its
      rust-version, so the pin and the declared minimum cannot drift apart.
  - id: OBL-0241-2
    package: WP-792
    proof: scripts/check-python-package
    says: The Python artifact receipt names the pinned toolchain as the Rust
      source toolchain that built it.
review_triggers:
  - A toolchain move would be taken without a paired A/B at one fixed revision,
    or without the binary digests that prove the arms differed.
  - The pinned toolchain and a package rust-version, a shipped SDK manifest, or
    the published Rust driver runtime declaration would be allowed to differ.
  - chunks_exact_to_as_chunks would be adopted or its allow removed.
---
# ADR-0241: Rust 1.98.1 Toolchain Upgrade

## Context

The workspace pinned Rust 1.97.0. Moving the pin is not a neutral edit here: the
toolchain version is restated in manifests, CI workflows, acceptance scripts,
SPEC.md, operator documentation, the shipped SDK manifests, and the runtime
declaration published for the Rust driver. Left behind, any of those becomes a
statement that is simply false — a consumer told the driver builds on a compiler
it no longer builds on.

A compiler change also cannot be assumed free. ADR-0239 records that this
programme measures rather than asserts, and the honest question for an upgrade
is not whether it is faster but whether it costs anything.

## Decision

1. Pin Rust 1.98.1, and move every declaration that names the toolchain with it
   — manifests, workflows, scripts, SPEC.md, PLAN.md, operator and contributor
   documentation, the Python artifact receipt, the shipped SDK manifests, and
   the adapter conformance driver runtime.
2. Keep the pin on measured evidence. The measurement is a paired A/B at one
   fixed source revision, counterbalanced across repetitions, with the binary
   digests recorded; it is banked in
   `docs/performance/rust-1-98-1-toolchain-ab-2026-09.md`.
3. Fix the two new lints that fire at single sites at their real source, in the
   frozen adapter fixture, rather than suppressing them in generated files.
4. Defer `chunks_exact_to_as_chunks` to `allow` at the workspace root with a
   comment recording why: it fires at 72 sites including hot paths, and
   adopting it during the bump would have meant measuring a code change and a
   compiler change at once. Its adoption is a separate change.
5. Do not rewrite banked evidence. `release/evidence/**` and the WP-711 receipt
   validator record what was actually run under 1.97.0; changing them would
   destroy the thing they exist to preserve.

## Evidence

Measured on C3D at revision `b8ee16934`, two repetitions per arm, arms
counterbalanced, RiffDB only. Geometric mean throughput ratio 1.98.1/1.97.0:
write_only 1.0036, read_only 1.0005, interactive 1.0027. Every one of the ten
cells lands between 0.989 and 1.017, while the worst within-toolchain
repetition spread is 1.023 — wider than the between-toolchain difference in
every cell but one.

The upgrade is therefore not distinguishable from run-to-run noise at this
resolution. That is the claim: no regression was found. It is not a claim of
improvement, and two repetitions resolve roughly 2% and larger, so a sub-1%
systematic effect would not have been seen. One host; the ratio does not port.

Two method notes are recorded because both could have produced a false null.
`benchmarks/run-app-baseline` selects its toolchain with an explicit
`cargo +1.97.0`, which overrides `RUSTUP_TOOLCHAIN`; a first run that set only
the environment variable would have measured a 1.97.0 binary in both arms. And
`--smoke` reports carry no build provenance, so the guard against that is the
binary digest taken directly from each arm's target directory.

## Consequences

Raising the pin raises the workspace minimum supported Rust version, which is
consumer-visible. The published Rust driver runtime declaration moved from
`rust1.97.0` to `rust1.98.1`, changing the adapter manifest hash in five
adapter conformance manifests and the portability manifests derived from them —
45 fixture files, each a one-line hash update, with no contract, role or
operation change. Downstream adapter repositories consume these manifests and
should be re-checked.
