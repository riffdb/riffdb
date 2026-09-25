# Fresh-context authoring pilot, 2026-09-22

Frozen before participant launch. This internal evidence pilot complements the
retrospective RDB-C048 case. It is not a sealed-alpha acceptance campaign or a
statistical estimate of model reliability.

Two fresh subagent contexts receive no conversation history or evaluator gold.
They use the parent session's inherited model and settings, without a model
override. Each receives a separate temporary directory containing one MLflow
contract snapshot, the current public command/invariant cookbook, an offline
compiler binary and a compiler wrapper that records attempt hashes and outcomes.
The compiler contains WP-809. Agent isolation is enforced by instructions and
separate materials, not by an OS access-control boundary: the shared workspace
remains technically accessible. Participants must disclose any outside access.

The two source conditions are the adapter commit immediately before its 17-token
migration and the retained migration at revision
`7617c92ac1cd383322db3cc6d9850ed2219ecf34`. Their source SHA-256 values are already
pinned in the retrospective case. Neither participant receives those condition
labels or the expected number and location of edits.

Both receive the same task: make the supplied contract compile with the current
compiler while preserving its business behavior, existing revision checks,
checked increments, selection predicates, inputs and outputs; make the smallest
necessary source repair. They may read only their packet and use its compiler
wrapper. No network, repository browsing, evaluator contact or delegation is
allowed. The maximum is six compiler attempts and ten minutes of work. Stop at
success or that bound. Report the edit rationale and any material uncertainty.

The evaluator scores every launched run, with no replacement for a failed run:

- Initial and final compiler status; diagnostics and source hashes per attempt.
- Exact source diff: permitted repairs change only mutation spelling; existing
  fences, predicates, field values, inputs, outputs and command count must remain.
- Whether the result equals the independently pinned repaired source, and the
  number of spelling edits. Expected counts are withheld from participants.
- Compiler attempts, elapsed time, completion status and disclosed contamination.
- No model satisfaction rating or production runtime success is inferred from
  compilation. The existing runtime fence tests remain separate evidence.

Only curated results, patch locations, source/material hashes and this protocol
are published. Raw transcripts, local paths and machine identifiers are excluded.
All failures and deviations remain in the result table. No causal comparison
between conditions or broad risk closure is inferred from two observations.
