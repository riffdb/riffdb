---
adr: "0238"
title: Server Global Allocator Selection
status: proposed
tier: guarantee
date: 2026-09-19
accepted: null
acceptance: null
requires: [ADR-0171]
amends: []
supersedes: []
requirements: []
packages: []
obligations:
  - id: OBL-0238-1
    package: null
    proof: scripts/check-workspace-policy
    says: Exactly one package may restate the workspace lints, and only with
      unsafe_code at deny; every other package inherits the workspace forbid.
  - id: OBL-0238-2
    package: null
    proof: exactly_one_file_allows_unsafe_code_and_only_for_the_global_allocator
    says: Exactly one file in the workspace carries an allow(unsafe_code), it
      relaxes the lint for exactly one item, and that file declares the global
      allocator.
review_triggers:
  - A second package, compilation unit, or item would sit below the workspace
    forbid(unsafe_code).
  - The allocator crate would be changed, removed, upgraded across a major
    version, or made configurable at build or run time.
  - The allocator would be applied to a process other than riffdbd, or a
    library crate would select an allocator.
---
# ADR-0238: Server Global Allocator Selection

## Context

The write path spends a growing share of its time in the system allocator as
concurrency rises. Measured on the C3D bench host with the standard write_only
load, replacing the system allocator raised throughput by roughly 2 percent at
one client and 11 to 16 percent at 8, 32 and 128 concurrent clients, and cut
c=128 p95 latency from 80 ms to 55 ms. Group batch sizes were unchanged, so
the same work is being done faster rather than differently; the gain grows with
concurrency, which is the signature of contention inside the allocator rather
than of per-allocation cost. jemalloc and mimalloc were both measured on the
same harness across two runs each; jemalloc led at every level by 1 to 2
percent.

Selecting a global allocator requires an unsafe item, and this workspace sets
`unsafe_code = "forbid"`. A `forbid` cannot be relaxed by an `#[allow]` on the
item, which is its purpose. Adopting an allocator therefore cannot be a local
code change: it needs a recorded decision about where, and how narrowly, the
workspace stops forbidding unsafe code. AGENTS.md makes unsafe Rust, native
code, and a critical dependency each a stop-and-request-human-review trigger,
and this proposal is all three at once.

## Decision

1. The `riffdbd` binary selects `tikv-jemallocator` as its global allocator,
   pinned to an exact version as house style requires.
2. The relaxation is exactly one compilation unit and exactly one item. The
   `riffdb-server` package restates the workspace lints with `unsafe_code` at
   `deny`; the `riffdb-server` library re-forbids itself at its own root; the
   `riffdbd` binary root carries `#![deny(unsafe_code)]` and one
   `#[allow(unsafe_code)]` on the allocator static.
3. No library crate selects an allocator, and no other process does. A library
   that chose an allocator would impose it on every embedder.
4. The lint boundary is machine-checked, not conventional.
   `scripts/check-workspace-policy` permits exactly one named package and one
   named file, requires the named package to restate the lint at `deny` and
   rejects `allow`, and requires the named file to declare the global
   allocator and to carry exactly one allowance.
   `tests/unsafe_code_boundary.rs` independently asserts the same boundary from
   the source tree.
5. Adding a second exception is a guarantee-tier decision. Widening either
   constant in the checker is the change that must be reviewed, not a fix to
   make a build pass.

## Standing design tests

- **Interface safety (AGENTS.md boundary 11):** no. The allocator is process
  configuration in the server binary. No application developer or agent can
  observe it, select it, or opt out of any guarantee through it, and no public
  surface changes. It adds a native transitive dependency to the server
  binary, which is why the record exists.
- **Scale:** no. A general-purpose allocator with per-thread arenas assumes
  nothing about co-located authoritative storage, single-node memory, or
  full-state rewrite. It reduces contention on a shared allocator, which is
  the opposite of a scale ceiling.

## Consequences

The server binary gains a native C dependency, so it now requires a working C
toolchain to build and carries jemalloc's own memory behaviour, including its
retention and decay policy. Resident memory may read higher at rest than under
the system allocator without indicating a leak. Binary size grows.

The workspace can no longer say, without qualification, that it forbids unsafe
code. It can say something narrower and still strong: one file, one item,
checked two ways. The value of `forbid` was always that it could not be
relaxed quietly; that property is preserved by making the exception explicit,
enumerated, and tested rather than by keeping a `forbid` that would have to be
worked around.

## Options considered

Keeping the system allocator forgoes 11 to 16 percent of write throughput at
the concurrency levels that matter and leaves c=128 p95 at 80 ms. It was
rejected on the measurement.

mimalloc is a smaller and simpler dependency and captured most of the gain,
trailing jemalloc by 1 to 2 percent at every level across two runs each. It
remains the obvious fallback if jemalloc's behaviour proves unsuitable in
operation; the decision is reversible by changing one static and one
dependency.

Making the allocator a build feature was rejected for now: it doubles the
configuration matrix that must be kept green for a gain that only matters if
the choice turns out wrong, and the choice is cheap to revisit.

## Checks

- `scripts/check-workspace-policy` fails if any other package restates the
  lints, if the named package lowers the lint below `deny`, if the named file
  loses its `deny` or its allocator declaration, or if it carries more than one
  allowance. Each of those five failures was confirmed by violating it.
- `tests/unsafe_code_boundary.rs` asserts the workspace still forbids, that no
  other package declares its own `unsafe_code` lint, that every library root
  still forbids, and that exactly one file relaxes the lint. Each assertion
  was confirmed to fail when its boundary is violated.
