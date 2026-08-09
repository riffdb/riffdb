# Why RiffDB Exists

RiffDB is the data layer for the average multi-tenant SaaS application: safe
transactions, schema migrations, event subscribers, analytics, vectors, and
full-text search as opinionated native parts of one database, so that the
patterns which normally require complex, risky external wiring are simply
present, correct, and bounded. Its primary builders are expected to be AI
agents, and every surface is designed to be learned from documentation and
error messages at run time: typed outcomes, finite grammars, deploy-time
rejection over runtime surprise.

## The seams argument

The obvious objection: capable agents will happily wire Postgres, Kafka,
Elasticsearch, and a vector store together, so an integrated database saves
labor that is about to be free.

The objection mistakes what the wiring costs. Agent labor makes *assembly*
free. It does not make *seams* correct. A five-service stack has no
transaction across its parts, no freshness contract between its source of
truth and its indexes, and a dual-write problem in every seam that no amount
of careful glue can remove — it is removed by architecture (a changelog and a
single total order) or it is not removed. Glue code produced quickly and
confidently fails in the invisible ways: lost updates under concurrency,
replay storms, search results that are wrong until a customer notices. And
the operational bill remains: five failure domains, five upgrade cycles, five
things to secure and monitor, and no coherent backup of the whole.

RiffDB's answer is that the seams do not exist. Committed means indexed means
deliverable, inside one total order, behind one typed freshness vocabulary,
in one process with one backup unit. That is a lot of architecture for most
projects to buy any other way — and most projects are exactly who this is
for. In a world of capable agents, the stacks they wire together
mass-produce the pathology this design removes by construction.

## Two boundaries that keep it honest

**Opinions live only in the data layer.** RiffDB's capability model governs
data authority — what an application principal may read and write. It does
not and will not do end-user identity, sessions, sign-in flows, or OAuth;
that is application land, and applications keep their own frameworks,
languages, and auth providers. Platforms whose data layer is a commodity
must expand upward to differentiate. RiffDB differentiates inside the data
layer, so it can afford to stay narrow — and narrowness is load-bearing: a
finite surface is a provable surface, and a provable surface is one an agent
can be taught completely in context.

**Everything is à la carte, and no one is trapped.** Use the transactions
and ignore the projections; use the events and ignore the search. When an
application outgrows an opinionated pillar, the changelog is the graduation
path: RiffDB remains the system of record and feeds the specialist system,
rather than holding data hostage to its own feature ceiling. Export is a
supported operation, not a negotiation.

## What safety means here

Unsafety is unexpressible at the public surface, not discouraged
(`AGENTS.md`, non-negotiable boundaries): no interactive transactions to
leave half-open, no raw query language, no omittable tenant scoping, no way
to opt out of durability on an acknowledged write. Internals may use unsafe
machinery freely so long as the boundary holds. Every architecture decision
record answers two standing design tests — can a developer or agent express
an unsafe operation through this surface, and does this assume co-located
storage or single-node memory — so the boundaries survive the people and
agents who wrote them.

Claims here are meant to be checked, not believed: the decision records in
`adr/`, the requirement inventory in `SPEC.md`, and the crash, recovery, and
falsifiability evidence attached to each work package are the primary
documents. This page is only the argument that connects them.
