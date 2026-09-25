# Decision vocabulary

These definitions describe the repository's proof conventions. A decision
must name the concrete owner, bound or frontier when applying one of them.
The term alone is not evidence that its conditions hold.

| Term | Meaning |
| --- | --- |
| **compiler-sealed** | A value or plan whose permitted shape and semantic facts are established by the first-party compiler and whose checked construction prevents an application from supplying an arbitrary replacement. Validation at a later trust boundary still applies. |
| **compiler-bounded** | A compiler-derived upper bound on work, bytes or cardinality from admitted source and schema facts, carried to and enforced by the consuming runtime. A runtime limit chosen by a caller is not a compiler proof. |
| **bounded** | A named finite maximum is checked before the corresponding allocation, scan, recursion, wait, output or mutation can exceed it; checked arithmetic also bounds the calculation. State the unit and enforcement point. |
| **typed refusal** | A closed, documented result or error that preserves its semantic category and safe public fields across interfaces. It releases no forbidden partial success or sensitive diagnostic detail. |
| **least-sufficient** | Select the oldest supported identity or smallest admitted capability set that can represent the actual semantics, without silently omitting a required feature or guarantee. It does not authorize retaining an identity that an accepted retirement decision removes. |
| **fail closed** | Missing, malformed, stale, mismatched or unprovable evidence prevents the protected action. It is never interpreted as absence, permission, readiness or successful completion unless the governing protocol explicitly proves that interpretation. |
| **admission-head fenced** | The admitted operation binds its relevant authoritative and derived observations to the exact accepted head and required identities; validation prevents releasing a result assembled from incompatible heads. It is not a promise to observe commits admitted later. |
| **rebuildable** | Derived state can be discarded and reconstructed from retained authoritative inputs under the accepted protocol. Rebuilding proves readiness before service resumes; it does not turn the derived copy into authority or remove retention requirements. |
| **epoch** | A named compatibility or validity domain whose change invalidates specified prior artifacts or assumptions. An epoch number alone conveys neither a commit sequence nor readiness. Each use identifies the domain and transition ceremony. |
| **generation** | The identity of one incarnation or materialization within a named domain. A successor is distinct even if its contents match; publication, retention and reuse follow that domain's protocol. |
| **frontier** | An explicitly typed position delimiting a proven contiguous prefix or completed boundary in one ordered history. State whether it is durable, published, applied, retained or another defined kind; those frontiers are not interchangeable. |

Use links to these definitions in new ADRs instead of restating them. Keep
individual decisions testable: name the condition, its owner, its refusal and
the test or check that demonstrates it. Historical ADR text remains unchanged
under ADR-0181's preservation rule.
