---
adr: "0232"
title: Append Only Export Page Commitments
status: accepted
tier: guarantee
date: 2026-09-16
accepted: 2026-09-16
acceptance: 'maintainer, in session, 2026-09-16: "Accept all four as written" (ADR-0227, ADR-0230, ADR-0231, ADR-0232)'
requires: [ADR-0115, ADR-0207, ADR-0217]
amends: []
supersedes: []
requirements: [EXP-002, EXP-006, EXP-007, EXP-008, EXP-009, EXP-010]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0232: Append Only Export Page Commitments

## Context

Every export page rewrites a canonical state containing all prior page hashes.
Canonical verification, cursor hashing, serialization and authoritative storage
capture repeat that growing prefix. The implementation is bounded by 4,096 page
hashes, a 256 KiB durable state envelope and 64 MiB replay bytes, so the reported
100,000-page example cannot execute. The O(P squared) cumulative rewrite cost
nevertheless remains real within those limits and warrants a repair proposal.

Export operations are replicated authoritative namespace 33, not disposable
adapter cache. Moving their commitments to another table changes durable
identity, atomic compare-and-swap and startup/replay proof. D-003 therefore
requires an accepted record; this proposal changes existing functionality's cost,
not its authority or application export semantics. It is not accepted.

## Decision

1. Introduce a versioned compact export-state encoding and a versioned export
   page-commitment record in a separately registered authoritative namespace.
   Allocate its namespace, record and key identities through the existing durable
   registries and format transition; do not overload another namespace or mint
   synthetic operation IDs to store pages in the operation table.
2. The compact operation head retains the complete existing snapshot, authority,
   lease, selection, phase, cursor position and count bindings. Replace its growing
   page-hash vector with a page count and a domain-separated prefix commitment.
   Genesis binds the immutable operation/snapshot identity. Each next commitment
   binds the prior commitment and the complete canonical new page-ledger entry.
   No bare, unbound hash or process-local counter substitutes for this evidence.
3. Page entries are keyed canonically by exact operation ID and nonzero increasing
   page ordinal. Each entry binds owner, ordinal, class, page hash and row/byte
   totals. Existing page content hashing is unchanged. Entries are append-only;
   an equal retry is a no-op and an unequal duplicate fails closed.
4. One closed storage transition compares the complete expected operation head,
   verifies the exact next ordinal, installs one page entry, and replaces the
   compact head atomically with immediate durability. Changelog attribution stays
   ApplicationExportOperation. The V3 capture includes both mutations in the same
   transaction, with complete fresh-locator accounting and reciprocal validation.
   A head without its entry, or an entry without its head transition, is invalid.
5. Page release still revalidates current capability revision, scope, policy and
   immutable snapshot before serialization/counting/hashing. Cursor construction
   binds the compact state, including its prefix commitment, under a new explicit
   cursor version. Cache hits and retries cannot bypass those safe points.
6. At completion, cancellation or terminal failure, read the bounded ledger in
   ordinal order, verify its contiguous prefix, owner, totals and final commitment,
   and construct the same public manifest and receipt bytes as the existing
   implementation for the same logical export. Validate the whole prefix once at
   terminalization. Missing, extra, reordered or substituted entries fail closed.
   Persist terminal evidence before advertising completion.
7. Preserve the 4,096-page ceiling and all current row, byte, lease, replay and
   operation-count limits. Charge the compact head plus retained encoded ledger
   entries to the existing 256 KiB durable-state budget, including terminal-document
   headroom before releasing a page. The representation may be smaller; it cannot
   silently raise the aggregate bound or turn a failed terminal write into success.
8. Preserve exact retry/unknown-outcome handling. Reconcile the head and exact page
   entry before reporting a durable transition; do not advance twice after a lost
   reply. Existing bounded page-response replay still owns emitted bytes. Neither
   a ledger nor a cursor reconstructs a lost snapshot or authorizes a newer one.
9. Startup, backup/restore, follower replay and export-operation cleanup validate
   the new namespace and its reciprocal head/entry prefix. Existing retention
   eligibility still controls cleanup. When an operation may be removed, remove
   its bounded head and ledger atomically; the existing write/entry ceilings can
   contain the entire 256 KiB operation. Never leave a resumable partial prefix.
10. Keep existing export-state and cursor decoders and their strict canonical
    checks. Existing operations retain their original encoding/path until terminal;
    do not rewrite a live cursor's meaning. New operations use the compact encoding
    only after the declared durable-format transition. Older binaries must refuse
    the new format through the normal compatibility gate. Public JSONL records,
    manifests, receipts, authorization and supported application export remain
    unchanged. Human review of permanent compatibility fixtures is mandatory.

## Options and consequences

Caching decoded state alone leaves quadratic writes and cursor preimages.
Dropping old hashes loses terminal integrity evidence. A rolling commitment alone
cannot reproduce the existing ordered manifest hash list. An append-only ledger
plus a compact head makes per-page durable work independent of prior page count,
with one O(P) terminal verification. It adds a durable namespace and compatibility
obligation, which is why it is not an implementation-only shortcut.

## Standing design tests

- **Interface safety:** applications cannot write ledger entries, supply counts,
  select a different snapshot, bypass current authority or claim completion.
- **Scale:** existing aggregate byte/page/lease bounds cover head, ledger, terminal
  documents and replay. Normal page advancement retains only its bounded entry.
- **Recovery:** exact atomic head/entry transitions and complete terminal-prefix
  validation remain independently provable after process loss and replication.

## Checks

- Compare public page, manifest and receipt bytes with the existing implementation
  for every export class, scope, empty class, cancellation and terminal failure.
- Count canonicalized and persisted bytes at 16, 128 and 1,024 fitting pages;
  advancing one page must not read or rewrite the prior ledger prefix.
- Crash before/after atomic page commit and before response release. Retry must
  recover exactly the prior or next page, never double-count or skip a page.
- Corrupt owner/ordinal/hash/totals/chain, omit or duplicate a ledger entry, or
  substitute operation authority: startup, terminalization and replay refuse it.
- Verify V1/V2 operation compatibility, old cursor handling, format refusal,
  full follower prefix equivalence, bounded reclamation and backup/restore.
- Exercise byte/page limits including reserved terminal-document headroom; a
  fitting completed export retains identical output and a truthful final receipt.
- Update export operations and durable compatibility documentation with the
  accepted implementation and its permanent fixtures.
