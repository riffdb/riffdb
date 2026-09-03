# RiffQL v1 planning

`riffdb-query-compiler` compiles one parsed query against one exact immutable
contract catalog. Successful compilation produces a `QueryAccessProgramV1`;
execution does not receive source text or perform access-path discovery.

The v1 planner accepts only:

- one exact partition parameter shared by every binding;
- complete primary-key point reads for `one` and `maybe`;
- bounded `many` reads whose equality/membership prefix and complete ordering
  match one declared index;
- a bounded dependent primary-key batch where one earlier `many` field supplies
  exactly one complete-key component through `in`, the target bound does not
  exceed the source bound, and a missing-target outcome is declared;
- one wholly forward or wholly reverse traversal direction;
- explicit application-visible row bounds no greater than the 65,534-row page
  ceiling (the 65,535-row physical scan ceiling reserves one continuation probe).

Operational version-2 members may additionally use null/existence or binary
prefix predicates when the selected contract index carries the matching sealed
physical encoding. `presence(field)` expands one logical optional field into a
missing/null/value discriminator and a typed payload component. The executor
forms only the finite discriminator prefixes required by the source predicate;
it never fetches an unbounded candidate set to answer existence. A
`text_key(field, binary_utf8_v1)` component uses a zero-escaped ordered-byte
codec, allowing an exact leading-byte range while returning the original field
value. The same component may provide an ordinary bounded `order by field`
suffix even when the query has no prefix predicate on that field. Its order is
the exact UTF-8 byte order frozen by `binary_utf8_v1`, not the canonical
length-first string order. Exact equality on that field consumes the complete
text-key component and advances the ordered suffix, so one index can serve a
wider `order by relation, user` query and a narrower `relation == $relation`
plus `order by user` query. Runtime converts the typed equality string to the
same physical profile bytes only while constructing the index prefix; residual
predicate evaluation still compares the original typed value. The remaining
index suffix must equal the complete declared order, including its deterministic
entity-key tie-breaker; forward and reverse cursor traversal use the same
encoded key bytes. Compiler, command index maintenance, and both storage readers
consume the same versioned key schema.

The compiler's closed component registry treats equality, membership,
interval/complement, state, prefix, order, and tie-breaking as composable roles
of one declared physical component. A component is not assigned one exclusive
purpose: for example, `binary_utf8_v1` may consume bounded membership or one
bytewise interval/complement and produce the first remaining order term in the
same plan. The caller cannot
select any role, index, encoding, comparator, or fallback.

After typed parameter validation, the executor forms one bounded ordered range
schedule per access step and request. Canonical set normalization, text-profile
encoding, typed endpoint construction, range sorting, deduplication, and shape
validation happen once. One page, continuation probe, scan/fuel allowance,
policy/hydration allowance, cursor, and output budget covers the entire
schedule. Memory and redb consume that semantic schedule; neither backend may
apply a logical predicate after page selection or invent a storage-local
profile transform.

The program contains ordered accesses, dependency edges, cardinality and row
bounds, and the complete entity/field/index authorization requirement. The
application supplies names; stable numeric IDs remain compiler-internal.
Authorization is fail-closed before execution and never silently removes a
selected field.

Canonical program bytes are hashed in the accepted
`riffdb.query-plan/v1` domain. Explain output is a bounded, deterministic,
name-only view of the same program. An absent compatible index produces
`RDB-QP003` with a source span and a suggested symbolic index declaration.
Every predicate and the complete total order must be represented by that one
selected index. An order-compatible index cannot leave a predicate for
post-scan filtering: doing so could apply `take`, an ordinal offset, or a
continuation before the predicate and silently omit matches.

RiffQL v1 does not perform an unbounded fallback scan, client-side sort,
cross-partition join, or optimizer-dependent plan choice. ADR-0175's additive
finite partition-set route is not a join: it replicates one compiler-sealed
local index plan over caller-submitted bounded routes in one read view. Uniform
orders use a bounded k-way heap; mixed physical directions use a bounded
complete-group path. Both produce one globally ordered page and one opaque
cursor only after whole-set policy and epoch observation succeeds.
Compiler-proved exact predicates may consume index components between the
partition route and order suffix. Those values form each local physical prefix
and are applied before merge, limit, and cursor selection; an absent compatible
covering index remains a typed `RDB-QP003` refusal rather than a residual filter.

One-level expansion repeats a compiler-selected same-partition index access for
each row of an earlier bounded driver. Planning proves the driver maximum,
per-driver maximum, product ceiling, index-prefix equality, driver-first then
index order, and direct result nesting before emitting the V18 access tag. An
expanded binding cannot drive another expansion and has no independent cursor.
Rejected operational shapes additionally carry an anonymized class containing
only operator, cardinality, and partition categories; names, identifiers, and
submitted values are excluded.

## Candidate-set plans

Language V11 candidate bindings compile to complete internal source steps plus
one root-hydration step. Each ordinary source uses the exact declared index,
partition equality, predicate prefix, projected root key, and independent
`within` ceiling. Source steps have no page or cursor semantics: a backend
continuation means the complete source exceeded its bound and the whole query
refuses before root work.

The executor canonicalizes and deduplicates every complete source, evaluates
the sealed single/intersection/union/difference operator, then batch-hydrates
every surviving root key in the same read view. Missing and policy-hidden roots
are indistinguishable and are removed before the compiler-declared mixed root
order is evaluated. Only after the complete authorized root population has
been sorted does the executor apply the requested bounded page and one
continuation marker. Resume repeats candidate completion at the cursor-bound
snapshot and epochs, locates the complete root-key marker, and continues in the
same total order; it does not filter or sort a pre-paginated index page.

The plan and role retain source indexes, relationship proof, operator, bounds,
root order, cost, and authority. Aggregate query fuel charges all complete
sources plus hydration. Role format V6 records that candidate authority is
present; its `max_scan_rows` remains the per-access ceiling representable by
the public `u16` grant, while the sealed whole-query cost independently limits
the sum across sources. There is no scan fallback, intermediate output, or
application-supplied set.

A `pattern_index` candidate step is a provider participant rather than an
ordinary row-store scan. The service obtains its complete, exactly verified
candidate batch before opening final root execution, validates the compiler
descriptor, policy shape, plan, generation, frontier, and observed work, and
negotiates one result-set epoch proof for the whole query. Final root hydration,
policy, mixed total ordering, page selection, and cursor formation still occur
inside the authoritative storage snapshot. The request path cannot scan
entities to decide pattern membership; only the bounded background rebuild
worker may populate provider state from an exact authoritative snapshot.

## Finite operational plan families

Language-version-2 optional predicates compile at deployment into a closed
family of ordinary access programs. Presence parameters are canonically ordered
and capped at eight, so the family contains exactly `2^n` members and no more
than 256. Every member has the same contract, query name, partition parameter,
and result schema. Each member must independently pass the ordinary index,
locality, cardinality, and bounded-cost planner.

The family has its own canonical identity over the resolved query surface,
ordered presence domain, every member program, complete authorization union,
and component-wise maximum cost. Policy therefore authorizes the whole deployed
family rather than only the member selected by one request. Runtime selection
is an exact presence-bit lookup; it performs no parsing or access-path planning.
The selected member drives physical execution while responses and generated
bindings retain the stable family identity.

Language-version-2 aggregate declarations compile only through the finite
operational-family path. The ordinary single-plan compiler returns
source-spanned `RDB-QP008`, preventing an aggregate declaration from being
ignored. The operational family seals exact aggregate descriptors, source
fields, authorization union, cost, and the source-row-limit group clamp. At
runtime the selected member access and all folds execute in one engine-owned
snapshot; no storage scan fallback or client-side fold exists.

Language-version-8 aggregate queries use query-IR/module version 11. The plan
seals exact distinct cardinality, partial-state byte, and arithmetic-operation
ceilings independently from its source row, group, scan, cell, and output
bounds. The reference and columnar evaluators consume those resources across
the whole result, apply row policy before contribution, and withhold every
aggregate cell if any dimension is exhausted.

Declared relationship metadata may justify symbolic navigation only when it
lowers to the target's complete primary-key point read or an already bounded
dependent-key batch. It cannot infer colocation, omit a partition predicate,
introduce a hidden scan, or accept an otherwise unsupported join.

Dependent batches execute as ordered point reads inside the same engine-owned
snapshot as the source scan. They do not become public N+1 requests. Null,
duplicate, noncanonical, over-bound, or missing dependent keys fail closed
before a partial result can be released.

`explain` remains value-free and names a dependent collection as `dependent
primary-key batch from source.binding_field`. The complete target/source
mapping, dependencies, and absence outcome are already sealed in canonical
plan bytes; changing any of them changes plan identity. WP-689 deliberately
keeps existing explain bytes stable because application manifests include that
representation.

The current repository-wide relationship inventory is deliberately closed to
three shapes: an exact complete-key point, a singular-key-driven separately
bounded index access, and ADR-0054's dependent complete-key point batch. A new
semijoin, correlated existence test, bounded one-to-many expansion, or other
composition requires a real-consumer amendment with independent fan-out,
intermediate, probe, byte, output, policy, snapshot, and identity evidence.
Arbitrary or caller-supplied joins, cross-partition work, Cartesian products,
recursion, and runtime join optimization remain unavailable.

WP-689 validates that inventory against three independent applications:
TicketDesk, agent-blog, and agent-orders. Its checked fixture records every
current source/target mapping together with the access shape, driver and target
row maxima, shared partition proof, continuation-probe and complete access-key
byte maxima, whole-query intermediate/projected-value/result-byte ceilings,
missing behavior, cursor eligibility, authority ownership, explain evidence,
and plan-identity participation. The current
dependent batches are TicketDesk labels (50), blog tags (32), order-line
products (100), and inventory products (499). Exact point dependencies have a
one-row driver and target. Singular-key-driven index reads retain their own
declared `take`, scan, point, byte, output, and optional opaque-cursor bounds;
they do not become a relationship runtime operator.

Required point or batch targets select their declared absence outcome. An
optional point produces `None`, and an empty bounded-index driver produces an
empty collection. A row-policy-denied point is intentionally indistinguishable
from a missing point, preventing an existence leak. A dependent batch is
all-or-nothing: empty input produces an empty collection, while null,
duplicate, noncanonical, out-of-order, over-bound, missing, or policy-denied
targets release neither partial rows nor a cursor. Memory and redb preserve
input position and apply one shared row-policy context while the same
engine-owned snapshot remains open.

The closure corpus is
`fixtures/riffql/operational-access-corpus-v1`. It is intentionally domain-
neutral and generates an exact cross-language application surface. External
consumer evidence is retained only as the value-free receipt
`fixtures/riffql/wp690-external-tuple-capability-v1.json`; external schemas,
routes, adapters, and generated profiles remain in their owning repository.

## Whole-query authorization and execution fuel

ADR-0055, WP-280, and WP-285 establish the exact application-query
authorization and work boundary below.

The service resolves the entire query before data access and presents policy
with one application-query request. For a named query this includes the exact
contract lineage, immutable module hash, query name, plan hash, partition route,
complete entity/field/index/output requirements, principal and capability
revision, and one whole-request cost vector. Ad-hoc execution uses a distinct
permission and classification.

An allow decision produces a process-local, non-cloneable and nonserializable
proof bound to that exact request and current capability revision. The service
consumes the proof at the executor boundary with the matching plan and
parameters, then reauthorizes before releasing output. It cannot be converted
into, or reused to authorize, public `GetEntity`/`ScanIndex` requests.

The plan-hashed cost vector includes at least step count, scanned rows, point
reads, dependent keys, intermediate rows, projected values, and encoded result
bytes. Policy compares the complete vector once. Execution consumes matching
fuel and checks backend-reported work; exhaustion returns no partial result or
cursor. Per-step bounds remain defense in depth, not the aggregate authority
model.

The compiler derives each component conservatively:

- index scans charge their complete declared `take` maximum;
- point reads include direct reads, every dependent key, and index-result
  hydration;
- dependent batches charge their complete source-key maximum;
- intermediate rows are summed across all bindings rather than reset per
  entity or step;
- projected values include every declared result copy and every maximum
  aggregate group cell; and
- result bytes use contract field byte bounds plus deterministic envelope
  reserves.

The complete vector is appended to the canonical access-program bytes before
the `riffdb.query-plan/v1` hash is computed. Changing any component therefore
changes plan identity and invalidates substitution.

The predecessor compatible capability record has one `max_scan_rows` field. For
ordinary non-candidate plans it is the whole-query aggregate allowance for
index-scan rows; every scan step is summed and no step may independently reuse
it. Candidate-aware role V6 instead treats the same bounded field as a
per-access ceiling, because candidate plans can contain several independently
complete 65,535-row sources; their aggregate is still sealed and enforced by
the whole-query cost vector and fuel. Point reads, dependent keys, and
retained intermediate rows have separate conservative ceilings derived as that
scan bound times the fixed maximum query-step count. Projected values add the
existing maximum visible-field multiplier, and encoded bytes retain the fixed
4 MiB service ceiling. The exact plan cost is still authorized once and
consumed as execution fuel, so these derived ceilings do not create ambient or
unmetered read authority.

Each `Limit<MAX>` access is charged at its immutable declared `MAX`; two uses
of `Limit<100>` charge 200 rows even when a request submits smaller values.
Fixed `take` bounds let a multi-collection page divide authority deliberately.
Defaults and submitted values never narrow static cost. The separate 65,535-row
physical scan ceiling reserves one row for a continuation probe; it is not an
application-visible page size. The 4 MiB encoded-result ceiling and all
provider-, policy-, hydration-, authority-, and transport-specific ceilings
remain independent, so a large structural maximum is not permission to exceed
any of them.

An ordinary cursor-paged ordered access treats the submitted runtime limit as
invocation cardinality rather than continuation identity. The compiler derives
that exclusion only when every use of the parameter is a `take` on a cursor-
paged index access. Mixed use, unpaged limits, and nearest K remain bound. The
cursor-specific invariant-parameter hash is paid once per admitted request;
the shared query parameter hash and immutable plan identity are unchanged.

The V12 row-limit IR retains the parameter name, positive declared maximum,
and optional default. Runtime validates the effective value before opening a
provider or storage access. One accepted invocation executes the selected
page directly; it does not walk one-row cursors, overfetch and discard, or
re-plan per page.

At runtime the executor creates a move-only fuel value from the exact program
cost. It decrements fuel for every access step, backend-reported scanned row,
point read, dependent key, retained intermediate row, projected value, and
encoded result byte. Backend scan reports are reconciled with returned rows.
An impossible, under-reported, over-reported, or exhausted execution returns a
closed failure before a result or cursor is constructed.

Aggregate grouping uses length-framed canonical value encodings as the ordered
key. Whole-set folds always produce one record, including for an empty source;
grouped folds produce no record for an empty source. Sum arithmetic is checked
over `i128`; exact decimal scale comes from sealed result type metadata, never
from a row or caller. Empty `min`/`max` outer absence is distinct from a
contributed canonical null for optional inputs: the aggregate record omits the
field only for the outer absence, while a present null remains a normal value.
