---
adr: "0206"
title: Composite Fresh-Locator Arming and Observable Production Proof
status: proposed
tier: guarantee
date: 2026-09-06
accepted: null
requires: [ADR-0070, ADR-0100, ADR-0104, ADR-0165, ADR-0197, ADR-0205]
amends: []
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-705]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0206: Composite Fresh-Locator Arming and Observable Production Proof

## Context

The force requiring a durable decision, the authoritative requirements it
touches, and the conflict that cannot be left to an implementation package.
At most two paragraphs.

## Decision

1. One testable statement.
2. One testable statement.

## Options considered

1. **Option:** consequence, and the reason to choose or reject it.

## Consequences

- Positive consequence.
- Cost or constraint.
- Explicitly deferred behavior.

## Standing design tests

- **Interface safety:** what stays unexpressible on the public surface.
- **Scale:** what stays bounded as data or concurrency grows.

## Checks

- The fixtures, tests, scripts, and architecture checks that freeze this decision.
