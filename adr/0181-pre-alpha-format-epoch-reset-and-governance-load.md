# ADR-0181: Pre-Alpha Format Epoch Reset and Governance Load

- **Status:** Accepted
- **Obligations:**
  - `OBL-0181-1` WP-757 must prove that every topology domain carries
    exactly one readable and one writable identity, and the topology check
    refuses a second readable identity while no external database is
    registered; planned proof
    `topology_refuses_second_readable_identity_before_external_database`.
  - `OBL-0181-2` WP-757 must prove that an epoch-1 database is refused in
    place by an epoch-2 binary and crosses only through the complete
    ADR-0112 ceremony with reconciled export/reimport counts and hashes;
    planned proof
    `epoch_two_ceremony_refuses_in_place_open_and_reconciles_reimport`.
  - `OBL-0181-3` WP-757 must prove that the durable-fixture inventory and
    release manifest name no epoch-1 identity after the reset, and every
    deleted Protobuf field number and enum value remains `reserved`; planned
    proof `epoch_two_fixture_inventory_contains_no_epoch_one_identity`.
  - `OBL-0181-4` WP-758 must prove that the obligations ledger is empty and
    the checker rejects any new ledger entry rather than acknowledging it;
    planned proof `check-adr-obligations --self-test`.
  - `OBL-0181-5` WP-758 must prove that every ADR carries only the required
    sections; the form check warns above 260 lines and rejects a SPEC
    document-control row above one paragraph; planned proof
    `check-adr-form`.
  - `OBL-0181-6` WP-759 must prove that no public handbook page carries a
    work-package or ADR number, the limitations page states one sentence per
    limit, and campaign 04 records the agent-audience metrics against the
    stated target; planned proof `check-handbook-audience`.
- **Direction approved:** 2026-09-01
- **Exact text accepted:** Yes, 2026-09-01
- **Accepted:** 2026-09-01
- **Acceptance reference:** Maintainer acceptance of the exact text in the
  current Claude Code session on 2026-09-01, all seven consolidation records together
- **Decision deadline:** Before WP-757 declares alpha format epoch 2 or
  deletes any decoder, fixture, or topology row
- **Requires:** ADR-0006, ADR-0056, ADR-0112, ADR-0122, ADR-0124
- **Amends:** ADR-0124 section 4 and `VER-006`/`VER-007` only while no
  external database is registered; `HBK-002` and `HBK-005` gain the
  agent-audience rules in section 6; ADR-0112's ceremony is reused unchanged
- **Defines or blocks:** WP-757 through WP-759

The maintainer accepted the exact text of this record on 2026-09-01. Its packages
may begin. Each deferred obligation above is tracked in
`adr/obligations-outstanding.yaml` until its planned proof exists, at which
point the owning package discharges it by declaring the proof.

## Context

RiffDB is in alpha format epoch 1, writer 1, with downgrade unsupported and a
breaking-epoch posture of export/reimport only
(`release/durable-format-manifest-v1.json`). No database exists outside this
repository's harnesses, examples, and evaluation bundles. The same manifest
declares 97 readable and 74 writable durable record versions and two readable
storage and receipt versions. The topology (`release/version-topology-v1.json`)
registers 59 domains; the compiled application-role codec alone lists
identities 1 through 6 as readable, writable, and current. RiffQL keeps
readers for language versions 1 through 9, query IR and modules through V17,
and contract grammar, executable IR, and bundle through V23. The workspace
carries 824 distinct version-suffixed Rust types.

Every one of those readers, fixtures, and rows is maintained so a database
nobody has can still be opened. ADR-0124 section 4 was written for the
post-release case: retirement needs a prior-release notice, frozen
last-readable fixtures, a typed-refusal fixture, and permanent reservation, and
`VER-006`/`VER-007` codify it. Applied before the first external database, the
ceremony turns every additive identity into carrying cost with no consumer.

The process ledger has grown alongside. The repository holds 174 ADRs, with
ADR-0150 at 529 lines and ADR-0174 at 497; `SPEC.md` is 9,456 lines with
document-control rows that run to a page; `work_packages.yaml` is 21,529
lines; and `adr/obligations-outstanding.yaml` acknowledges 144 obligations that
predate `scripts/check-adr-obligations`. That checker exists because prose
obligations shipped undischarged (ADR-0156 section 5). A ledger that only
grows names the gap rather than closing it.

The handbook is the product surface for the builders the vision names: agents
that learn RiffDB from documentation and error messages. It is 190,116 words
across 214 pages, 146 of which cite a work-package or ADR number, and
`docs/known-limitations.md` is 253 lines written in the ADR vocabulary.
Campaign 03 measured time to first committed row at 11, 316, 600, and 1,680
seconds across its four retained cells with zero rescues, so the evaluation
already measures what documentation changes should move.

## Decision

### 1. One reset to alpha format epoch 2 through the ADR-0112 ceremony

Exactly one breaking pre-1.0 epoch is declared: alpha format epoch 2, writer
1. It is crossed only through the ceremony ADR-0112 accepts and
`AFC-005`/`AFC-006` require: validate and back up, export with receipt,
install an empty epoch-2 database, reimport through compiled commands,
reconcile identities/counts/hashes/events, and retain old artifacts until
validation passes. An epoch-2 binary refuses an epoch-1 database before
mutation with the existing `RDB-FORMAT-0101` refusal and names `riffdb storage
preflight` and the ceremony as the sole safe action. No in-place upgrade,
physical restore, or best-effort decode crosses the epoch (`AFC-004`,
`AFC-007`, `AFC-012`).

After the reset, every topology domain lists exactly one readable and one
writable identity, and they are equal. Superseded identities are removed from
source, fixtures, generated artifacts, and the topology in the same change.

### 2. "External database" is defined, and the pre-external window is named

An external database is one whose `DatabaseId` was minted by a binary running
outside this repository's harnesses, examples, evaluation bundles, benchmark
hosts, and maintainer dogfood installs, for a party other than the maintainer.
The pre-external window is open while `release/version-topology-v1.json`
lists no entry under a new `external_databases` array. Registering the first
entry (an operator-provided identity digest, date, and release label, never
the raw identifier) closes the window permanently.

While the window is open, ADR-0124 section 4 and `VER-006` are amended:
retirement needs no notice interval and no last-readable fixture, and
superseded decoders, fixtures, generated artifacts, and topology rows are
deleted in the same reviewed change rather than parked. `VER-007` is amended
in one respect: fixtures and topology rows may be deleted. The identifier rule
is unchanged: Protobuf field numbers, enum values, durable tags, hashes, and
symbolic identities of deleted messages stay `reserved` under ADR-0006, so
nothing is reused. When the window closes, ADR-0124's lifecycle resumes
unchanged with no retroactive notice for identities deleted during the window.

### 3. One identity per domain is enforced until the window closes

`scripts/check-version-topology` gains a rule: while the window is open, a
domain with more than one readable, writable, or current identity fails, and a
lifecycle state other than `active` fails. A new identity can therefore enter
only by replacing the current one in the same change. The rule is keyed on the
`external_databases` array, so closing the window needs no code change and
cannot be forgotten. `VER-005` manifest agreement remains and proves the
manifest reports one version per family.

### 4. The obligations ledger is closed and cannot reopen

Every entry in `adr/obligations-outstanding.yaml` is discharged by declaring it
in its ADR header with a proof that exists, or struck with a one-line reason
recorded in the discharging change. The ledger reaches zero and the file is
removed. `scripts/check-adr-obligations` then rejects any ledger file or entry,
so a deferred obligation in an accepted ADR has one form: an `OBL-NNNN-n`
declaration whose proof exists at acceptance. An ADR whose proof cannot exist
at acceptance registers its package first and is accepted when the proof
lands, or the obligation becomes a requirement ID covered by that package.

### 5. ADR form is bounded and the sealed vocabulary is defined once

An ADR contains only the template's sections, and each decision subsection
states one testable decision. The terms this repository seals by convention
(compiler-sealed, compiler-bounded, bounded, typed refusal, least-sufficient,
fail closed, admission-head fenced, rebuildable, epoch, generation, frontier)
are defined once in `adr/GLOSSARY.md` and referenced rather than re-explained.
A new `scripts/check-adr-form` warns above 260 lines, fails on a missing or
unknown section, and fails on a `SPEC.md` document-control row longer than one
paragraph. Existing ADRs are not rewritten. Requirement registration in
`SPEC.md` stays authoritative: the row summarizes, the section body carries
the normative text.

### 6. The handbook is written for the agent audience and measured

Public handbook pages, meaning every page not listed in
`docs/handbook-exclusions.txt`, carry no work-package or ADR number; the
number belongs in a source comment or an excluded evidence page.
`docs/known-limitations.md` is rewritten as one sentence per limit, grouped by
what an author or operator is trying to do, in vocabulary the concept pages
define. The quickstarts follow the same rule. `HBK-002` is refined: internal
decision provenance stays absent from public pages, as proposed behavior
already does.

The rewrite is measured, not asserted. Campaign 04 reruns the sealed
agent-application evaluation with the rewritten handbook under the campaign-03
bundle protocol. The target is stated in advance: median time to first
committed row and total rescue count not worse than campaign 03 on the same
language and brief cells, plus a published goal of a 25 percent reduction in
the slowest retained cell, a target rather than a release gate. A regression
on either metric blocks closure of the handbook change.

### 7. Explicitly deferred

No wire, durable, IR, or source byte changes except through the ceremony in
section 1. No compatibility promise is made before the first external
database. Online epoch migration, partial-domain resets, and automatic
ceremony execution remain out of scope.

## Options Considered

1. **Keep every identity readable until 1.0:** rejected. The cost is visible
   in byte-exact multi-version proofs on every language change and a topology
   that treats six role codecs as current, with no consumer for any of it.
2. **Retire identities one at a time under ADR-0124 as written:** rejected.
   The notice interval and last-readable fixtures protect external databases;
   before one exists they cost a release cycle per identity and preserve the
   fixtures they were meant to let go.
3. **Reset without a ceremony by wiping the maintainer's databases:**
   rejected. ADR-0112 and `AFC-004` forbid destructive reset as an upgrade,
   and running the ceremony exercises export/reimport before a user needs it.
4. **Reset the format ledger only:** rejected. The obligations ledger, ADR
   length, and handbook vocabulary are the same growth in other files, and the
   handbook is the surface agents learn from.

## Consequences

- One identity per domain removes multi-version readers, fixtures, and
  topology rows and makes an additive identity a replacement rather than an
  accumulation until the window closes.
- The ceremony runs once for real, producing the first end-to-end evidence
  for `AFC-005` and `AFC-006` outside a fixture.
- Cost: every database this repository created is crossed or discarded, every
  fixture proving an old identity is deleted, the change touches most crates,
  and closing the ledger takes 144 recorded decisions.
- Deferred: online migration, partial reset, and notice-free retirement once
  an external database exists.

## Compatibility

Public gRPC, MCP, CLI output, driver protocol, and generated-surface domains
keep their current identity and lose superseded ones; a client built against
a superseded identity receives the existing typed version refusal. Durable
data crosses the epoch only through export/reimport. Contract IR, query IR,
query modules, locks, and manifests keep their current version number as the
sole identity; older compiled artifacts are refused and regenerated by
`riffdb application check`. Current source still compiles. The topology gains
`external_databases` and loses every superseded identity row.

## Security

The ceremony reuses ADR-0112's authorization and refuses before any target
byte changes; old artifacts are retained until validation. Deleting decoders
removes attack surface, and a retired identity fails closed. Registering an
external database records a digest, date, and release label, never an
identifier or credential. The handbook rewrite changes no redaction rule.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no application or agent
  surface gains an operation. The ceremony is operator-only and refuses in
  place; an application cannot select an epoch, decoder, or fallback, and
  cannot opt out of the typed refusal. The record removes readers and adds no
  expressibility.
- **Scale:** nothing here assumes co-located storage or single-node memory.
  The ceremony is bounded by the existing export and reimport ceilings, and
  one identity per domain is the state a billion-row tier would also start
  from. The window is reversible by construction: it closes on the first
  registration and ADR-0124's lifecycle resumes.

## Testing

- `topology_refuses_second_readable_identity_before_external_database`, with
  a paired case proving the rule relaxes once `external_databases` is
  nonempty.
- `epoch_two_ceremony_refuses_in_place_open_and_reconciles_reimport`: an
  end-to-end ceremony over a seeded TicketDesk epoch-1 database asserting the
  typed refusal, export receipt, reimport reconciliation, and full startup
  validation of the epoch-2 result.
- `epoch_two_fixture_inventory_contains_no_epoch_one_identity` over the
  fixture inventory, release manifest, and `.proto` reserved ranges.
- `check-adr-obligations --self-test` (ledger file and undeclared obligation
  rejected), `check-adr-form` fixtures (missing, unknown, 261-line, oversize
  row), and `check-handbook-audience` over every non-excluded page plus the
  campaign-04 receipt checked against the stated target.

## Requirements and Work Packages

- **Requirements:** `GOV-001` through `GOV-006`
- **Defines or blocks:** WP-757 through WP-759
- **Final evidence:** WP-759

## Decision Deadline

Exact acceptance is required before WP-757 declares epoch 2, deletes any
decoder or fixture, or amends `scripts/check-version-topology`. WP-758 and
WP-759 may inventory under direction approval but remove no ledger entry and
publish no rewritten page before acceptance.
