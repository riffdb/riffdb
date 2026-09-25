# R-06 catalog selection pilot, 2026-09-22

Frozen before participant launch. This is an internal, descriptive pilot of
catalog size and stale-catalog selection. It is not a broad client/model
qualification or proof that SPEC R-06 can be closed.

## Materials and conditions

Generate descriptors through an actual RiffDB application service and MCP
transport with actual capability filtering. The evaluator must retain source,
descriptor and material hashes, tool counts including fixed tools, serialized
bytes, pagination counts, repeated-list equality and visibility assertions.
Mock conformance catalogs are not eligible substitutes for these materials.

Use a small capability-filtered catalog with eight dynamic commands and a
large catalog with 64 dynamic commands. Both contain the same target operations
and realistic neighboring names. A second independently deployed contract
renames the approval operation, removes cancellation and adds a required review
reference to submission. The changed small/large catalogs therefore contain
seven/63 dynamic commands. Record actual counts rather than inferring them.
This is an evaluator-simulated catalog transition between independent database
deployments, not evidence of a compatible in-place upgrade or notification
delivery. Never weaken migration checks to manufacture that transition.

The factorial cells are small/stable, large/stable, small/changed and
large/changed, with two fresh participant contexts per cell (eight total).
Use the parent model and settings without override. Each receives only its
current catalog and task packet, no history, repository access or gold answers.
Separate directories and instructions provide isolation; no OS-level sealed
boundary is claimed. Participants must disclose outside access.

## Tasks and sequence

Before launch, freeze three unambiguous business tasks and their exact expected
names and arguments against the generated schemas: approval, cancellation and
submission. Include concrete IDs, idempotency keys and review-reference values
where needed. Gold answers remain evaluator-only until every response is final.

Each participant first selects one tool and constructs arguments for each task
using the initial catalog. After its answer is final, deliver a second packet.
Stable participants receive the same catalog and fresh task values. Changed
participants receive the changed catalog and corresponding task values, with
an explicit simulated catalog-change notice. The new catalog is withheld until
stage two. The changed cancellation task requires an explicit unavailable
answer; inventing an alternative mutating operation is a failure.

Catalogs are supplied as MCP descriptor JSON for file-assisted inspection;
the subagent harness cannot dynamically install them as native model tools.
This measures descriptor-based selection and argument construction, not native
MCP client execution, autonomous notification handling or truncation behavior.
Participants may inspect their catalog programmatically; record that affordance
and do not generalize it to clients that put every schema directly in context.

No correctness feedback, repair attempt or replacement participant is allowed.
Each stage has one final three-answer response and a ten-minute bound. Retain
incomplete, malformed, wrong-name, wrong-argument and unavailable outcomes.

## Scoring and interpretation

Score first responses against the frozen name/argument oracle independently:
correct selection, exact required argument values, schema-valid shape, and
correct abstention when the operation is unavailable. Report each task and stage,
not only an aggregate pass rate. Compare stable/changed and small/large cells
descriptively; two contexts per cell cannot establish population reliability.

Separately measure server-side repeated-list stability, capability filtering and
refusal of stale names against the changed deployed service. A model's correct
JSON is not evidence that a command committed, and a server-side refusal is not
evidence that the model refreshed its catalog autonomously.

Publish curated answer objects, material hashes, scoring rules and every run's
result. Exclude raw transcripts, private paths, capabilities and machine data.
R-06 remains open beyond the tested sizes, model configuration and explicit
refresh/file-inspection conditions regardless of a perfect pilot result.
