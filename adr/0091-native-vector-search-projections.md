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

### 2. Embeddings are authoritative data; the application owns the computation

A contract declares a **vector field** on an entity: dimension, distance
metric, and the source fields it semantically derives from (for staleness
tracking). The application computes the embedding externally (using whatever
model, provider, or pipeline it chooses) and writes the vector to the entity
through a typed command that carries the model identity and version. RiffDB
validates dimension, stores the vector as authoritative state, and feeds the
vector projection — but never calls an external model endpoint itself.

The declared vector field also carries a **source-field binding**: the set
of source fields whose content the embedding semantically derives from. This
binding enables typed staleness tracking: an entity whose source fields have
been written more recently than its last embedding write is countable and
queryable as stale. Staleness is observable, typed state — never a silent
search-quality degradation. Each vector field declares a staleness SLO;
breached budgets change health.

Consequences of this shape, all deliberate:
- Embeddings add real weight to authoritative state and therefore to
  backups (order of kilobytes per row at common dimensions). This is the
  accepted price of rebuildability; deployments that cannot pay it should
  not declare vector fields on high-cardinality entities, and a quantized
  authoritative representation is a future amendment, not an assumption.
- Apply-time external calls are PROHIBITED — projection apply stays
  deterministic and fast.
- The projection is snapshot-rebuildable (§1 of 0086): embeddings live in
  authoritative state, so the index rebuilds from current entities alone.
- Embedding staleness is observable, typed state (the entity has source
  fields newer than its last embedding write), never silent.
- A model version change is the application's responsibility: the typed
  command carries the model version; a change in version is observable in
  authoritative state and the staleness surface reports entities whose
  embedding version does not match the contract's declared current version.
- RiffDB never bundles, configures, or calls LLM/embedding provider APIs.
  The model computation boundary lives entirely in the application client.

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

- **Database-managed embedding pipelines (outbox-worker model).** Couples
  the database to external LLM provider APIs, adds a credential and endpoint
  configuration surface, and makes the database responsible for retrying
  external calls. The application is better positioned to manage model
  selection, versioning, batching, and provider failover. Moved from the
  original accepted text to rejected by Amendment 1 (2026-08-10).
- **Apply-time embedding computation.** Couples apply lag to an external
  service and destroys deterministic replay.
- **An external vector database sidecar.** Two systems of record, two
  frontier semantics, hand-rolled consistency — the exact anti-pattern.
- **Exact-only search.** Fails at the scale tier the product targets;
  approximate-with-declared-recall is honest and measurable.

## Acceptance criteria (feasibility prototype + evidence)

Recall targets met against exact scan at matched frontiers across randomized
histories; client-supplied embedding writes validated for dimension, metric,
and model version; typed staleness tracking correct (source-field mutation
without a subsequent embedding write marks the entity stale); model-version
observability proven (entities with outdated model versions queryable);
row policy before ranking verified adversarially (unauthorized rows influence
nothing — presence, scores, or timing beyond stated policy); per-tenant
statistics isolation verified; frontier and crash invariants of ADR-0086 hold
unchanged; replay budget detaches a stuck index.

## Acceptance

Accepted by the maintainer on 2026-08-02, with the exposure-bound,
staleness-SLO, and backup-weight clauses folded in at acceptance. (Amendment 1
below subsequently removed the exposure-bound clause; the staleness-SLO and
backup-weight clauses stand.)

### Amendment 1 (2026-08-10)

§2 rewritten: the application supplies pre-computed embeddings through a typed
command; the database does not own an embedding computation loop and never
calls external model endpoints. The exposure-bound clause (field
classification checked against a model endpoint's egress classification) is
removed — the application controls what it sends to its own model. The
staleness SLO, source-field binding, backup-weight, and all other clauses
remain. The rejected-alternatives list is updated accordingly.
