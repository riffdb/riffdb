# What check-adr-obligations establishes

Development evidence for WP-804, recorded 2026-09-23. This page characterises
what `./scripts/check-adr-obligations` can and cannot establish, driven by
reproducing the three instances that opened the package against the check as
it stands after the 2026-09-21 front-matter repair. It is the recorded
characterisation the package's first deliverable names, and it is the
reference a reader should consult before trusting a green obligation check.

## The three instances, reproduced

Each instance was reproduced against the check in a fabricated tree
(`RIFFDB_GOVERNANCE_ROOT` pointing at a scratch repository containing one
accepted v2 record, one work package, and one test file). The exact shapes
and results:

### Instance 1 — ADR-0240: the proof exists but does not what the record says

The record's obligation said its proof "runs the perf-surface contract and
compares throughput"; the test of that name asserted gate tickets and
non-blocking. Reproduced as: an obligation whose `says:` claims a throughput
comparison, proof `semantic_proof`, and a test body that is
`assert!(true);`. **The check reports `1 proven` and exits 0.** It verifies
that a definition of the named proof exists in code; it does not read the
test's body, and no check that names proofs by string can.

### Instance 2 — ADR-0250: the proof exercises a type production never wires

The record's decisions were implemented in `ProductionServiceDiagnostics`,
constructed only inside a test module, while production wired
`ProductionObservabilityDiagnostics`. Reproduced as: proof
`wired_sink_proof` whose body constructs the test-only type. **The check
reports `1 proven` and exits 0.** A test of the named name exists and runs;
the check cannot know which type production constructs, and a name beginning
with `Production` is exactly the shape that defeats a name-based check.

### Instance 3 — ADR-0251: a mistyped colon silently dropped obligations

One colon inside a plain scalar made the record's front matter unparseable.
Before the 2026-09-21 repair, `RiffdbAdr.split_front_matter` rescued the
parse error, returned "no front matter", and the record read as an untitled
legacy one with no obligations: `check-adr-obligations` reported three fewer
declared obligations and passed, and `adr-index` rendered an accepted
guarantee record as untitled/unknown/legacy. Reproduced against the repaired
tools: **the check now fails with the record's name, the YAML error's line
and column, and why it matters, exiting 1** — this instance is closed, not
merely characterised, and the self-test
(`check-adr-obligations --self-test`, last case) pins it.

## What the check establishes

1. **A declared obligation's proof exists as a definition.** An `fn`/`def`
   (or `test_<name>`) outside the prose directories, or a `scripts/<name>`
   that CI actually runs (`scripts/ci-all`, `governance/tiers.yaml`, or a
   workflow). A wrapper script that only invokes a test does not count, and
   an unwired script does not count.
2. **A prose imperative cannot masquerade as a proof.** Strict excludes keep
   `adr/`, `docs/`, `work_packages.yaml`, `SPEC.md` and `governance/` out of
   the definition scan.
3. **Every deferred prose obligation is accounted for.** A sentence matching
   the obligation shape (`… WP-NNN … must …`) is bound to its declaration by
   the SHA-256 of its whitespace-normalized text, so adding a different
   imperative beside an existing declaration does not discharge it, and a
   changed normative sentence invalidates its binding.
4. **A record that cannot be parsed fails every tool that parses records.**
   The `MalformedFrontMatter` raise in the shared parser is caught by
   `check-adr-obligations`, `adr-index`, and `check-adr-form` as a named
   finding with exit 1; `brief` and `wp-new` fail on the raise itself. The
   retired obligations ledger cannot be reintroduced, even empty.
5. **Acceptance is a human act.** A record accepted since the configured
   cutover without an `acceptance:` reference fails, and a record accepted
   on or after 2026-09-21 cannot defer a proof that does not yet exist.
6. **Closure pays its debts.** A package closed while an obligation it owns
   is undischarged fails with the proof, the `says`, and the instruction to
   land the proof or reopen the package.

## What the check cannot establish

1. **That a proof exercises what its record says** (instance 1). The check
   matches names, not semantics. Reading test bodies would not close this:
   the gap between "compares throughput" and `assert!(gate_tickets == 0)` is
   semantic, and no static check in this repository's power can adjudicate
   it. The honest instrument is review: the obligation's `says:` is printed
   verbatim in the closure-failure message and in `--report`, so a human
   reading the proof against the `says` is the established control. ADR-0240
   Amendment 1 and ADR-0250 Amendment 1 are the recorded repairs when that
   review caught it.
2. **That a proof reaches production wires** (instance 2). Whether a type is
   constructed by production or only by tests is a property of the wiring in
   `main`/daemon composition, not of the proof's name. The check cannot see
   it. The mitigations that exist are structural and already landed: e2e
   tests that drive the real daemon (the replication-follower and
   server-composition suites construct the production graph), and the
   review control above.
3. **That a proof name is unique or unambiguous.** Two tests of the same
   name in different modules both satisfy the check; the check reports the
   name, not the path. This is accepted: the closure-failure message prints
   the `says` so a human resolves the ambiguity.

## Recorded finding: naming the production symbol is the wrong shape

The package's third deliverable offered two exits: strengthen the check for
instances 1 and 2, or record that naming the production symbol a proof must
reach is the wrong shape. **The finding is the latter.** Requiring an
obligation to name, say, the exact production type its proof must construct
would move the record's `says` into a machine-checked contract — but the
check could still only verify that the symbol *appears* in the proof, not
that the proof's assertions mean what the record means. It would add a
second name to keep synchronized, the same defect instance 1 exposed, one
layer down. The check's own output already states its limit: `--report`
prints each outstanding obligation's `says` verbatim, and the
closure-failure message prints it with the proof name, which is the
interface a human reviewer needs. The control for instances 1 and 2 is
review of the proof against the printed `says` at package closure — the
place where `check-adr-obligations` already forces a human decision — and
that is where both real instances were caught.

## Tool inventory for the malformed-record shape

The exit gate requires that a malformed record fails *every* tool that reads
it. As of this characterisation, against a record whose front matter opens
with `---` and fails to parse:

| Tool | Reads records via | Result |
|---|---|---|
| `check-adr-obligations` | shared parser, guarded | fails, exit 1, named finding |
| `adr-index` (`--check` and `--write`) | shared parser, guarded | fails, exit 1, named finding |
| `check-adr-form` | shared parser, guarded | fails (error row, exit 1) |
| `brief` | shared parser, unguarded raise | fails, exit 1 (raise names the record) |
| `wp-new` | shared parser, unguarded raise | fails, exit 1 (raise names the record) |
| `check-requirement-coverage` | does not parse records (reads `work_packages.yaml` and `SPEC.md`) | unaffected by design; a record's obligations are not its input |
| `adr-new` | filename scan only, never parses existing records | unaffected by design; allocation reads `adr_paths`, not content |
| `check-named-artifacts` | regexes `status:`/`packages:` lines, never parses YAML | see below |

`check-named-artifacts` deliberately reads package state and record status
without a YAML parser (its own comment records why: no dependency on the
27,000-line manifest staying parseable). A malformed record therefore does
not crash it, and in the instance-3 shape — colon in the `title`, `status:
accepted` line intact — it still reads the record as accepted and checks its
named artifacts, because the `status` line survives the title's broken YAML.
One adjacent shape was found while characterising and is recorded here
rather than fixed: `status : accepted` (space before the colon) is *valid*
YAML that defeats the unquoted-status regex, so such a record is read as not
accepted and its named-artifact claims escape that one check. The record
still fails `check-adr-obligations`, `adr-index`, `check-adr-form`, `brief`
and `wp-new` through the shared parser or its own rules, and
`check-named-artifacts`'s regex deliberately trades that narrow escape for
parser independence. It is noted as a known limit of that check, not an
open defect of this package: no accepted record in the tree carries that
spelling, and any record that did would fail five other tools first.

## Method and scale

The characterisation was produced by reproducing each instance in a
fabricated tree rather than by reading the check's source alone: instance 1
and instance 2 as obligations with passing-but-vacuous proofs (the check
reported both `proven`), instance 3 as a colon-in-plain-scalar record run
against every tool in the inventory above. The claims about what the check
establishes are each pinned by an existing self-test case in
`check-adr-obligations --self-test` (source bindings, whitespace rewrapping,
added imperatives, changed sentences, prose mentions, strikes, ledger
rejection, closed-package enforcement, acceptance references, and the
malformed-front-matter case), so the characterisation holds for the 192
declared v2 obligations the check currently reports, not only for the three
that produced it.
