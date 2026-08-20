# WP-653 large bounded named-result ledger

Status: closed honestly; internal exact-projection candidate rejected at the
workstation mechanics gate; no production optimization activated.

## Scope and frozen comparison

This package owns the ordinary compiled `BoardPage50`, `BoardPage200`, and
`BoardPage450` named-query path. It does not change query text, plan or module
identity, result bytes, authorization, freshness, continuation semantics,
durable validation, the projected provider, or the public protocol. The
PERF-018 comparator remains the generated Rust client over public gRPC against
the safe-application PostgreSQL implementation.

The last paired cloud receipts reported `BoardPage50` at 1.02x and
`BoardPage200` at 0.93x on N1, while `BoardPage450` was 2.91x on N1 and 3.25x
on E2. The projected-provider controls were 27--29 ms on the workstation and
are rejected: moving this query to that provider would make the measured
defect materially worse.

## Workstation size ledger

The diagnostic invocation retained only the three compiled board pages, ran
them scenario-major, and enabled the production fixed-cardinality query stage
windows. It used 500 samples after the full 19,220-command seed. The receipt is
`/home/kevin/tmp/wp653-board-profile-split.json` with SHA-256 to be banked at
package close.

| Page | Public p50 | Storage execute mean |
|---|---:|---:|
| 50 rows | 0.529 ms | 0.303 ms |
| 200 rows | 1.436 ms | 0.908 ms |
| 450 rows | 3.288 ms | 2.095 ms |

The 50-to-450 public slope is 6.90 us/row. Storage execution owns 4.48 us/row
(65%); response conversion, tonic/protobuf carriage, generated-client decode,
and benchmark row assembly jointly own the remaining 2.42 us/row (35%). The
service's named-result conversion is measured separately at about 0.113 ms per
mixed-size request; tonic serialization and client decode occur outside that
timer and remain part of the customer-paid remainder.

The closed storage-execute windows are:

| Stage | 50 rows | 200 rows | 450 rows | 450 - 50 |
|---|---:|---:|---:|---:|
| Program drive / executor shaping | 186 us | 443 us | 963 us | 777 us |
| Entity point lookup | 34 us | 133 us | 332 us | 298 us |
| Semantic reconstruction | 39 us | 156 us | 365 us | 326 us |
| Row materialization | 15 us | 67 us | 181 us | 166 us |
| Protobuf decode | 10 us | 38 us | 89 us | 79 us |
| Payload checksum | 5 us | 21 us | 49 us | 44 us |
| Durable wire preflight | 5 us | 19 us | 44 us | 40 us |
| Canonical re-encode verification | 4 us | 18 us | 41 us | 36 us |
| Identity, target, and row policy | 3 us | 8 us | 29 us | 26 us |
| Total | 303 us | 908 us | 2,095 us | 1,792 us |

Every storage window is a full fixed-width ordinal window. Its sum is the
reported storage-execute time by construction. The public-to-storage remainder
is explicit rather than attributed to an unmeasured server stage.

## Predeclared candidate bounds

The ordinary row-store scan currently materializes every authorized access
field, then the executor re-evaluates the same bound predicates and builds a
second `BTreeMap` containing only selected result fields. For terminal scan
bindings, the storage view can instead:

1. revalidate every bound predicate directly against the current authoritative
   entity record inside the same snapshot;
2. apply row policy at the unchanged safe point;
3. materialize exactly the compiler-selected result fields once; and
4. return an internal exact-projection proof that lets the closed executor skip
   only its redundant predicate and projection passes.

The proof is first-party and non-serializable. It carries no storage,
authorization, durability, sequence, policy, or public authority. Pages
without that proof retain the existing executor validation and shaping path.

Perfect removal of executor drive plus duplicate row materialization recovers
at most 943 us from the 50-to-450 increment, or 34.2% of public time. A 60%
realization predicts about a 20.5% public improvement and therefore clears the
WP mechanics threshold. The candidate is rejected unless the workstation
`BoardPage450` improvement is at least 20%, `BoardPage50` and `BoardPage200`
regress no more than 5%, and semantic/adversarial tests prove that a stale index
whose current entity no longer matches the predicates is filtered before an
exact-projection proof is issued.

Rejected standalone candidates:

- skipping default durable revalidation has only about a 5% public ceiling and
  would weaken corruption detection if scoped per operation;
- an entity cache is not justified by the ledger and introduces invalidation,
  policy, and bounded-memory hazards;
- covering-index syntax/provider changes and a positional public wire result
  could be material, but cross accepted compiler/durable/protocol boundaries
  and require their own ADR rather than entering WP-653 silently.

If the internal candidate clears the workstation mechanics gate, the same
50/200/450 ledger and paired safe-PostgreSQL pages run on N1 before production
activation. Otherwise WP-653 closes honestly with the current path selected.

## Candidate result and decision

The candidate was implemented behind an internal move-only exact-projection
proof, with storage-side current-entity predicate validation and executor-side
exact field-set validation. Both memory and redb row stores compiled and their
semantic suites passed before measurement. The same 500-sample diagnostic then
reported:

| Page | Baseline p50 | Candidate p50 | Change |
|---|---:|---:|---:|
| 50 rows | 0.529 ms | 0.556 ms | 5.1% slower |
| 200 rows | 1.436 ms | 1.409 ms | 1.9% faster |
| 450 rows | 3.288 ms | 3.128 ms | 4.9% faster |

The 450-row gain is far below the predeclared 20% mechanics threshold and the
50-row control crossed the 5% regression boundary. The candidate was therefore
removed in full. It was not run on cloud hardware because a second CPU profile
cannot turn a 4.9% workstation gain into satisfaction of the conjunctive
workstation-and-cloud mechanics gate. Existing production query semantics and
selection remain unchanged.

This falsifies duplicate executor predicate/projection shaping as the owner of
the large-result defect. The measured customer-paid remainder still repeats
entity and field names and rebuilds generic maps for every row across service
conversion, protobuf, and generated-client decoding. A compact positional
named-result representation could remove that repetition, but it changes the
public protocol and generated-client boundary. It requires a separately
accepted ADR and cannot be smuggled into WP-653 as an internal optimization.

## Receipt hashes

| Receipt | SHA-256 |
|---|---|
| Workstation baseline | `8a7fa1b2ee16661564859d1114d03e576f2dc497fd67280db65cbd32d362017e` |
| Rejected workstation candidate | `72b995d2e16ed4f68c11d30187c2b2567b51bda19b65d83aca2a3e3a6ed1be73` |

Both raw receipts remain under `/home/kevin/tmp/` and contain no application
values or credentials.
