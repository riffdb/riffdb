---
adr: "0243"
title: Public Repository Disclosure Guards
status: accepted
tier: guarantee
date: 2026-09-19
accepted: 2026-09-19
acceptance: 'maintainer, in session, 2026-09-19: "Approved"
  (ADR-0243 as written)'
requires: []
amends: []
supersedes: []
requirements: []
packages: [WP-794]
obligations: []
review_triggers:
  - A machine would be recorded by address, hostname or DNS name rather than by
    its specifications.
  - An ignore rule for build artifacts, secrets or agent session state would be
    narrowed, anchored to a single directory, or overridden with `git add -f`.
  - The repository would be made public again after being made private, or a
    new public mirror would be created, without re-running the disclosure scan.
---
# ADR-0243: Public Repository Disclosure Guards

## Context

This repository is public. On 2026-09-19 an audit found that its history
disclosed the addresses of all three benchmark hosts — C3D, N1 and E2 — several
of them listed beside the machine fingerprint hashes that identify them, and
11,025 occurrences of an absolute developer home directory.

Two mechanisms put them there, and neither was a one-off mistake.

The addresses were written into performance records as a normal part of
documenting where a measurement was taken. Nothing rejected them, so they
recurred: the same address was added again, by a different session, months after
the first.

Most of the home-directory occurrences were not written by anyone. They were
compiled into build artifacts — `target/debug/deps/*.rlib`, `*.rmeta`, a
compiled Go binary, a `.pyc` — which were committed because the ignore rules
were anchored to specific directories and missed nested crates such as
`benchmarks/*/target` and `examples/*/postgres/target`. Those artifacts also more
than doubled the repository, from 204 MB to 66 MB once removed.

A third mechanism was observed during the same session: `git add -A` in a
checkout shared by several agents and worktrees swept up another session's
untracked files twice, including agent transcripts that quoted host addresses
verbatim.

## Decision

1. A machine is recorded by its **specifications only** — core count, CPU model,
   storage class. Never by IP address, hostname, DNS name or URL. Examples that
   need an address use the RFC 5737 documentation ranges or `example.com`.
2. Credentials, personal information, absolute home directory paths, build
   artifacts, and verbatim agent session transcripts are never committed.
3. The prohibition is stated in `AGENTS.md`, which every agent reads, rather
   than left to convention.
4. Ignore rules for build output are **unanchored**, matching a `target/` or
   `build/` directory at any depth, because the anchored rules are what failed.
   Secret-shaped filenames and agent session state are ignored by default, with
   the two cowboy configuration files allowed back by explicit exception.
5. `git add -A` and `git add .` are prohibited in this repository. Paths are
   staged explicitly.

## Options considered

**Rotate the exposed addresses and leave history alone** was the first proposal,
and is cheaper: it neutralises the disclosure without rewriting a public branch.
The maintainer chose to rewrite history as well, so both were done — the
addresses are gone from every reachable object, and rotation remains available
as defence in depth.

**Redacting only the current tree** was rejected. It leaves the disclosure fully
readable in history, which is where it already sat for months.

**A pre-commit hook** was considered and not taken here. The ignore rules and the
written prohibition remove the two mechanisms that actually caused this; a hook
that scans every diff is a larger change with its own failure modes, and is
worth doing on its own evidence rather than bundled into a cleanup.

## Consequences

History was rewritten and force-pushed, so every commit identifier before
2026-09-19 changed. Existing clones, worktrees and open branches must be rebased
onto the new history or re-cloned; anything not reachable from the rewritten
refs keeps the old objects until it is garbage collected.

Removing committed build artifacts changes what old revisions contain. A
revision that previously included compiled output no longer does, so an exact
byte-for-byte rebuild of a historical artifact from this repository alone is no
longer possible. Those artifacts were never a reproducible build input; they
were accidental commits.

The measurement records now name hosts as C3D, N1 and E2 with their
specifications. Anyone who needs to reach one gets the address out of band,
which is where it should have been.
