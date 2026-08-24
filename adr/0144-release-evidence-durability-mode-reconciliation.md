# ADR-0144: Release-Evidence Durability-Mode Reconciliation

- **Status:** Proposed
- **Direction approved:** Not yet
- **Exact text accepted:** No
- **Decision deadline:** Before WP-200 changes or republishes its public budget
  comparison evidence
- **Requires:** ADR-0058, ADR-0101, ADR-0132
- **Amends:** the stale production-mode paragraph in `SPEC.md` Section 10.5 and
  WP-200's read-only owner-package boundary for the named evidence adapter

This record captures a conflict discovered while refreshing WP-200 evidence at
current HEAD. It is not authoritative until the maintainer accepts its exact
text.

## Context

The public command protocol has always defined a closed durability vocabulary
containing `sync`, `group`, and test-only `memory`. Later accepted decisions
activated bounded production group durability: ADR-0058 requires independent
command identity and acknowledged durable completion inside a physical group;
ADR-0101 makes the journal-fenced prefix authoritative; and ADR-0132 preserves
ordered durable-prefix publication and response release.

One old paragraph in `SPEC.md` Section 10.5 still says the POC production server
exposes only `sync`, contradicting the later revision history and accepted ADRs.
The frozen WP-200 budget evidence adapter repeats that obsolete assumption. A
current command commits successfully and returns `durability_mode = "group"`,
but the adapter rejects the response because it requires the literal `"sync"`.
The same adapter also fabricates `"sync"` when decoding a notified commit and
requires commit notifications to carry the synchronous enum.

This is not evidence of weaker durability. Both modes acknowledge only after a
known durable boundary, and the current group result is withheld until the
covering journal fence and ordered publication complete. The defect is that the
legacy evidence adapter checks a physical mechanism label instead of checking
that the response, stored outcome, and notified commit agree on one accepted
durable mode.

WP-200 currently declares the comparison adapter, fixture, public-run protocol,
and guarantee profile read-only except for a narrower performance-support
harness amendment. Repair therefore requires explicit owner-boundary review;
an implementation package may not silently loosen the frozen assertion.

## Proposed Decision

### 1. Production acknowledgement may report `sync` or `group`

`SPEC.md` Section 10.5 is reconciled with the later accepted architecture. A
production command response may report exactly `sync` or `group` according to
the closed coordinator path that durably committed it. `memory` remains
test-only and is refused by production startup. Empty or unknown durability
values remain invalid on a successful command response.

Both production modes retain the same semantic acknowledgement contract:

- the command's complete mutation, outcome, events, provenance, audit, and
  commit record are durably recoverable;
- every predecessor required by the single public frontier is durable and
  published;
- an authorized read using the returned sequence can observe that published
  frontier; and
- retry and restart recover the identical outcome and durability identity.

The names disclose physical durability strategy; they do not create an
application-selectable safety level. No public request gains a durability knob.

### 2. WP-200 receives one narrow owner-package amendment

WP-200 may change only the following frozen evidence behavior:

- the public budget adapter records the checked durability mode in
  `PublicCommandMetadata`;
- successful and replayed responses accept only the closed production set
  `{sync, group}`;
- commit-notification evidence converts the exact public commit durability enum
  to the matching response spelling instead of fabricating `sync`;
- response, replay, notified commit, and exact-end scanned commit must all agree
  on the same mode for one command identity; and
- mismatch, `memory`, unknown, absent, or malformed durability fails closed as
  invalid evidence.

The amendment covers the adapter implementation and its semantic, public
process, replay, safety, and architecture tests. It does not authorize changing
the canonical workload, business oracle, command inputs or outcomes, public-run
JSONL success fixture, gRPC schema, generated command bindings, application
service, server, coordinator, storage engine, or accepted durability behavior.

### 3. Comparator claims name the observed mode

The WP-200 report records the exact observed RiffDB production durability mode.
Its guarantee profile says that both `sync` and `group` are acknowledged durable
production modes and that the compared run used the reported one. It must not
rename `group` to `sync`, collapse both to an unlabeled "durable" value, or
claim a synchronous physical commit when the response says group.

Historical reports retain their original labels and source revisions. The new
receipt supersedes them only as current release evidence; it does not rewrite
their results.

## Options Considered

1. **Force current production back to `sync` for WP-200:** rejected. That would
   change the release product to satisfy an obsolete evidence assertion and
   discard accepted, recovery-proven group durability.
2. **Ignore the durability field in the adapter:** rejected. It would weaken
   the evidence by allowing response/commit disagreement or an unknown mode.
3. **Accept any protocol-valid string:** rejected. `memory` is test-only and a
   successful production response has a closed two-mode vocabulary.
4. **Accept `sync` or `group` and require identity-wide equality:** proposed.
   It follows the current authoritative architecture while strengthening the
   evidence against mislabeled durability.

## Consequences

- WP-200 can exercise and publish the actual release-selected group-durable
  path instead of failing on a stale literal.
- Evidence becomes stricter about cross-surface durability equality.
- The old `sync`-only POC sentence is removed from the normative specification;
  test-only `memory` remains prohibited in production.
- This decision does not select a new durability mechanism or alter the product
  path.

## Compatibility

No public Protobuf field, enum value, request, response shape, generated SDK,
contract, RiffQL, IR, bundle/module/plan hash, capability, durable record,
journal frame, backup, export, changelog, or replication format changes.

The strings and enum variants already exist and are already accepted by public
protocol validation. Only stale release-evidence expectations and contradictory
specification prose change.

## Security

Applications receive no selector or downgrade path. The server remains the
sole owner of the closed production durability mode. Evidence rejects test-only
`memory`, missing or unknown modes, and every response/commit mismatch before a
result can qualify. Authorization, redaction, atomicity, uncertainty, recovery,
and response release remain unchanged.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no application can request a
  weaker mode or opt out of acknowledged durability. The change only makes an
  evidence consumer understand both already-authoritative production modes and
  prove their equality.
- **Scale:** the decision adds one fixed enum field to bounded evidence objects
  and constant-time comparisons. It assumes no co-location, full-state cache,
  or history-sized work.

## Testing

- Unit tests accept exact `sync` and `group` response/commit pairs and reject
  mismatched, `memory`, unknown, and absent values.
- Public process evidence runs sequential, contention, same-key replay, and the
  checked-failure control through the current release server.
- Replay and commit-scan tests prove the original mode survives response loss,
  notification, exact-end scan, and same-key resolution.
- Architecture tests freeze the narrow WP-200 amendment and prevent changes to
  the public-run fixture or comparator workload.
- `benchmarks/verify-evidence --kind budget`, `scripts/demo --assert`, and
  `scripts/release-poc --verify` consume the regenerated receipt.

## Requirements and Work Packages

- **Requirements:** `POC-003`, `POC-004`, `POC-006`, `POC-008`, `PERF-005`
- **Defines or blocks:** `WP-200`, `WP-205`, `WP-579`
- **Final evidence:** `WP-200`

## Decision Deadline

Exact acceptance is required before any frozen budget-comparison adapter,
metadata type, durability assertion, guarantee-profile text, test, or report is
changed.
