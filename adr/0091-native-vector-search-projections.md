# ADR-0091: Native Vector Search as a Projection

- **Status:** Accepted
- **Date:** 2026-08-02
- **Decision owners:** RiffDB maintainers
- **Related:** ADR-0086 (projection plane, freshness, frontiers, authorization),
  ADR-0087 (ad-hoc grammar governance), ADR-0083 (commit records carry no
  post-images), ADR-0092 (full-text search; shares the per-tenant
  corpus-statistics rule)

## Context

RiffDB's product thesis is the one-stop shop for the average multi-tenant
SaaS: patterns that normally require complex, risky external wiring become
opinionated native parts of the database. Vector search today is exactly such
a pattern — teams hand-wire an embedding pipeline (change capture, model
calls, retries, versioning, backfill) into an external vector store, and the
result is the least reliable component of the average AI-adjacent product.
The ADR-0086 projection plane was deliberately designed index-type-agnostic:
`Projected(name)` sources, freshness policies, opaque frontiers, typed
lifecycle outcomes, and twice-enforced authorization transfer to a vector
index unchanged. What does NOT transfer is the exact-result contract, and
what does not exist yet is an opinionated answer to where embeddings come
from. This record decides both.

## Decision

### 1. A vector index is a projection

Vector indexes are projections under ADR-0086 in full: derived and
non-authoritative, fed from the single total order, org-partitioned segments
(exactly one organization scope per query), frontier-reporting, served under
`Causal`/`Bounded`/`Available` freshness with typed lifecycle outcomes, and
subject to the replay budget. Nothing in this record weakens any 0086
obligation except the explicitly amended result contract in §3.

### 2. Embeddings are authoritative data; the database owns the loop

A contract declares a **vector field** on an entity: dimension, distance
metric, the source fields it is derived from, and a named embedding model
with an explicit version. The embedding pipeline is the database's job, not
the application's: a write touching the source fields enqueues an embedding
intent through the existing outbox machinery; an embedding worker computes
the vector (calling the configured model endpoint); the result is written
back as an authoritative system command carrying the model version. The
vector projection then consumes the embedding like any other field.

The declared vector field also carries an **exposure bound**: the set of
source fields whose content may be sent to the model endpoint is exactly the
declared source-field list, checked at deploy time against field
classifications — a model endpoint is an egress surface and is governed like
one (the projection-envelope principle of ADR-0086 §5 applied to outbound
data). Embedding freshness is a typed, budgeted state: each vector field
declares a staleness SLO (like ADR-0086 §6 lag profiles); entities whose
source fields outrun their embeddings are countable and queryable, and a
breached budget changes health — never a silent search-quality degradation.

Consequences of this shape, all deliberate:
- Embeddings add real weight to authoritative state and therefore to
  backups (order of kilobytes per row at common dimensions). This is the
  accepted price of rebuildability; deployments that cannot pay it should
  not declare vector fields on high-cardinality entities, and a quantized
  authoritative representation is a future amendment, not an assumption.
- Apply-time external calls are PROHIBITED — projection apply stays
  deterministic and fast, and apply lag can never be held hostage by a model
  endpoint.
- The projection is snapshot-rebuildable (§1 of 0086): embeddings live in
  authoritative state, so the index rebuilds from current entities alone.
- Embedding staleness is observable, typed state (the entity has source
  fields newer than its embedding's model input hash), never silent.
- A model version change is a declared migration: new embeddings backfill
  through the same outbox loop; the projection reports `Rebuilding` progress;
  both versions never silently mix in one index.

### 3. The approximate-result contract (amends 0086's acceptance for vector projections only)

ADR-0086's acceptance requires projected results to match a reference
evaluator exactly at the same frontier, and compaction to be
result-invariant. Approximate nearest-neighbor structures cannot satisfy
either literally. For vector projections only:

- A query declares K; results are top-K under a **declared recall target**
  (per projection, e.g. `recall@10 >= 0.95`), measured against exact scan at
  the same frontier.
- Compaction and structure maintenance are bounded by **recall regression**,
  not byte-equality: the acceptance harness compares recall before and after
  maintenance at a frozen frontier.
- Exact KNN remains available under ADR-0087 budgets as the reference path
  and as the small-partition default (the average tenant's partition is
  small; the approximate structure engages above a declared row threshold).
- The frontier contract itself is NOT weakened: frontier F still means every
  relevant commit ≤ F is fully reflected in the index.

### 4. Grammar

One new operator enters the 0087 surface by this amendment: nearest-neighbor
(`nearest(field, $vector, k)`) with mandatory K and organization scope,
composable with the existing equality/range predicates (filtered ANN — the
filter applies BEFORE ranking). Deferred, each a future amendment:
multi-vector fields, hybrid lexical+vector score fusion (interaction with
ADR-0092), and cross-projection joins.

### 5. Authorization and inference protection

Row-level policy applies **before ranking**: distance computation and top-K
selection run only over rows the principal may see, so scores, ranks, and
result presence can never leak an unauthorized row's existence. All index
statistics that shape results (graph entry points, centroids, quantization
codebooks) are computed **per organization** — shared with ADR-0092's
per-tenant corpus-statistics rule — so no tenant's data influences another
tenant's rankings or timings beyond stated policy.

## Rejected alternatives

- **Application-managed embedding pipelines.** The risky wiring this
  database exists to absorb; also unrebuildable by RiffDB's own classification
  when embeddings live outside authoritative state.
- **Apply-time embedding computation.** Couples apply lag to an external
  service and destroys deterministic replay.
- **An external vector database sidecar.** Two systems of record, two
  frontier semantics, hand-rolled consistency — the exact anti-pattern.
- **Exact-only search.** Fails at the scale tier the product targets;
  approximate-with-declared-recall is honest and measurable.

## Acceptance criteria (feasibility prototype + evidence)

Recall targets met against exact scan at matched frontiers across randomized
histories; embedding loop crash/retry never loses or duplicates an embedding
(outbox semantics); model-version migration backfills completely with typed
progress; row policy before ranking verified adversarially (unauthorized
rows influence nothing — presence, scores, or timing beyond stated policy);
per-tenant statistics isolation verified; frontier and crash invariants of
ADR-0086 hold unchanged; replay budget detaches a stuck index.

## Acceptance

Accepted by the maintainer on 2026-08-02, with the exposure-bound,
staleness-SLO, and backup-weight clauses folded in at acceptance.
