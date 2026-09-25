# Fresh-context authoring and MCP catalog evidence

Completed 2026-09-22. This internal pilot covers both the RDB-C048 authoring
case and SPEC R-06's catalog size/stability risk. It adds observations, not a
sealed-alpha qualification or a claim that R-06 is closed.

## Authoring result

Two fresh contexts received separate contract snapshots, the public
command/invariant cookbook and the offline compiler containing WP-809. They had
no conversation history or evaluator solution. The [protocol](PROTOCOL.md) was
frozen before either launched.

| Source condition | Initial compiler result | Repair | Final result |
| --- | --- | --- | --- |
| Before adapter migration | 13 RDB-C048 diagnostics | 13 latest-binding `set` → `overwrite` changes | 14 commands compile on attempt two |
| Retained adapter migration | Four RDB-C049 diagnostics | Four fenced-child `overwrite` → `set` changes | 14 commands compile on attempt two |

Both results have the same SHA-256 as the independently pinned repaired source
from the earlier evaluator replay. The evaluator checked that every other
source byte was retained, including the parent revision comparisons, increments,
selection predicates, values and command interfaces, then independently compiled
both outputs again. Both participants reported no outside access. The
[curated results](authoring-results.json) preserve every compiler attempt,
diagnostic location, mutation-keyword change and source/material hash.

This supports a narrow conclusion: these two fresh participants repaired both
source conditions using the current compiler and public cookbook. It does not
establish that error messages alone teach the language, since guidance was
available. Runtime execution, deployment/lock renewal, business validity of the
latest-metric overwrite policy and satisfaction were not tested. Existing
[WP-809 runtime tests](../../../docs/architecture/WP-809-VERIFICATION.md) remain
separate evidence for stale revision and overflow behavior.

## R-06 catalog result

The [catalog protocol](CATALOG-PROTOCOL.md) fixed four conditions and two fresh
contexts per condition before launch. Every participant completed three tasks
against an initial catalog and three after a second snapshot, without correctness
feedback or a repair opportunity. Gold answers and stage-two snapshots were
withheld until their prescribed use; gold was published only after every response
was final. All eight launched participants and all 48 task responses are retained.

The catalogs came from actual hosted `riffdbd` discovery using normal scoped
capabilities, not mock backend descriptors. A synthetic WorkflowDesk contract
provides near-neighbor actions across eight entity types. Small capabilities
expose ticket commands; large capabilities expose all 64 commands.

| Snapshot | Dynamic + fixed tools | Serialized bytes | Pages | Identical repeated lists |
| --- | ---: | ---: | ---: | ---: |
| Small initial | 8 + 2 | 38,950 | 1 | 3 |
| Small changed | 7 + 2 | 35,022 | 1 | 3 |
| Large initial | 64 + 2 | 263,715 | 1 | 3 |
| Large changed | 63 + 2 | 259,787 | 1 | 3 |

Every dynamic descriptor was compared with the compiler-owned input/output
schemas. Visible dynamic names exactly matched the capability grant. Across
versions, approval becomes authorization, cancellation disappears, and submission
requires `review_reference`. Seven small-catalog descriptors and 63 large-catalog
descriptors remain identical, including the two fixed tools.

The three user tasks request approval, cancellation and submission with concrete
IDs, revision expectations, reasons and idempotency keys. Stage two supplies new
idempotency keys and an available review reference. Its prompt explicitly says
to include that reference if the current schema requires it. The changed
cancellation task requires abstention; choosing another mutation is scored wrong.

| Condition | Fresh contexts | Initial selection + arguments | Second-stage selection + arguments |
| --- | ---: | ---: | ---: |
| Small / stable | 2 | 6/6 | 6/6 |
| Large / stable | 2 | 6/6 | 6/6 |
| Small / changed | 2 | 6/6 | 6/6 |
| Large / changed | 2 | 6/6 | 6/6 |

All **48/48** first responses matched the frozen name/argument oracle and the
applicable input schema, including four correct abstentions after cancellation
was removed. All participants reported no outside access. These are repeated
tasks within eight contexts, not 48 independent model samples. The
[results](catalog-results.json), [gold](gold.json), [launch plan](launch-plan.json)
and [generated catalogs](catalogs/large-stable.json) permit score recomputation.

Separately, the real server refused a capability-hidden operation and an old
removed name with JSON-RPC `-32601`. Submitting the old argument shape to the
new submission tool produced `-32602`, `required_property_missing`, at
`/review_reference`. Those checks establish server behavior; the participants'
answers were not executed as business commands, and no entity population was
seeded for that purpose.

## What remains open

- Catalogs were inspected as files, with shell/Python filtering available. They
  were not dynamically installed as native tools in the subagent client. This
  does not measure client context truncation, token pressure or native tool-use
  accuracy when every schema is inserted into the model context.
- Changed catalogs came from independently initialized v1/v2 databases, followed
  by an explicit simulated change notice. No incompatible in-place upgrade was
  forced. Autonomous notification handling, stale cached clients and churn over
  a long session were not tested.
- Isolation used fresh contexts and separate packets with access instructions;
  the shared filesystem was not sealed by the OS. Outside-access reports are
  participant attestations. All subjects inherited the same parent-session model
  and settings; an exact model revision was not independently attested. There
  is no cross-model generalization or statistical size-effect conclusion.
- Ten-minute limits were participant instructions; exact participant wall time
  was not instrumented. Compiler invocation durations are recorded. This is a
  measurement omission, and no completion-time comparison is claimed.
- The catalog uses clear operation names and synthetic neighboring domains.
  Larger catalogs, multi-page discovery, ambiguous descriptions, native MCP clients, realistic
  applications and independently identified model configurations still need
  evidence before broader R-06 closure.

R-06 now has bounded real-model evidence for explicit-refresh, file-assisted
selection at 10/66 visible tools. It remains open outside those conditions.

## Reproduction and checks

The real-server fixture generator lives in
[catalog_risk.rs](../../../tests/mcp/catalog_risk.rs), included by the existing
server-composition test target. It generates only synthetic descriptors and safe
numeric metadata; capabilities, addresses and sessions are not exported.
The exact generated contract text is retained in [contracts.json](catalogs/contracts.json),
including its original whitespace, with independently checked source hashes.

```sh
RIFFDB_CATALOG_RISK_OUTPUT=/tmp/catalog-snapshots \
  cargo test -p riffdb-server --test server_composition \
  real_catalog_risk_snapshots --all-features
python3 evaluations/agent-authoring/fresh-context-2026-09-22/check.py
```

The scoring checker uses the evaluation environment's Python `jsonschema` and
also checks the compiler's UTF-8 byte-bound extensions. Its calibration rejects
a stale name, missing required argument, wrong operation and UTF-8 overflow.
It recomputes recorded scores; it does not rerun models or turn model failures
into missing data. The Rust generator reproduces catalog materials; fresh model
runs must use new contexts and the frozen protocol.

One initial harness setup failed before snapshot generation: adding invocation
grants to the bootstrap request was refused. The harness retained its original
bootstrap grant and created normal capabilities afterward. No product check
was weakened and no participant had started. This setup failure remains in the
result metadata. All four server-composition tests and targeted Clippy passed.
The broader scoped run retains eight pre-existing ignored tests: one explicit
release-mode activation receipt, two CLI archive-restore cases, two offline
maintenance cases and three migration-recovery cases. No ignore annotation or
test-runner policy changed for this pilot.

## PR note

- **Package / tier:** none / internal; tests and evidence only.
- **Behavior:** no application, protocol, authorization or durable behavior
  changes. Adds actual catalog size/stability checks and ten fresh-context
  authoring/selection observations.
- **Checks:** independent compilation/source comparison for both repairs,
  server-composition tests, Clippy, rubric calibration and 48-answer recomputation.
  Final scoped acceptance passed all four checks: formatting, MCP/server Clippy,
  720 passing MCP/server tests (eight pre-existing ignored), and handbook
  validation. Requirement coverage and introduced-content disclosure checks pass.
- **Compatibility:** unchanged; generated experimental contracts are deployed
  only into fresh temporary databases.
- **Documentation:** this report, the authoring evidence index and the existing
  design-review handbook page; earlier reproduction notes now name the actual
  `UpdateRunInfo` command. No new user interface is introduced.
- **Hazards:** explicit-refresh/file inspection, small sample, one inherited model
  configuration, instruction-only isolation, no business execution or native
  MCP client qualification. R-06 remains open within those limits.
