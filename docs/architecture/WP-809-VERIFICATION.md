# WP-809: cross-binding write protection

Package: WP-809. Tier: guarantee. Completed: 2026-09-21.

The compiler recognizes an unconditional same-root revision comparison and
checked increment as protection for child writes. Removing either half rejects
a child `set` without independent local protection. A protected child
`overwrite` is rejected in the opposite direction. Invariants constrain final
state and never establish guardedness.

The proof admits only equal `i64` or `u64` input/revision types, literal-one
increments, an ordinary root `mutate`, the same aggregate, and identical
root-key argument fingerprints. Conditional or unrelated fences do not qualify.
No runtime, coordinator, authorization, idempotency, provenance, or durable
encoding path changed. Previously deployed plans retain their interpretation.

## Evidence

- Seven compiler tests in `cross_binding_fence.rs` cover the positive rule,
  removing both fence halves independently, mismatched spelling, signed and
  unsigned revisions, commuted operands, different root keys, unrelated
  aggregates, child-only fences, initialization, conditional effects,
  non-advancing assignments and invariants. They assert diagnostic codes and
  exact source targets, explain classification and distinct plan hashes.
- Generated `fixtures/compiler/diagnostics.txt` pins diagnostic messages, help
  and source spans for the removed fence and incorrectly spelled overwrite.
- `stale_root_revision_refuses_child_update_and_overflow_never_wraps` executes
  the compiled plan with explicit before/after snapshots for both integer
  types. It checks the first root increment and child value, zero mutations
  with the declared stale outcome, a successful next revision, and exact
  arithmetic refusal at the integer maximum. This is deterministic runtime
  evidence, not a new storage crash or coordinator scheduling claim.
- Package acceptance passed all nine steps: formatting, Clippy, 639 tests
  across the four affected crates, generated artifacts, handbook, downstream
  contracts, file-size and panic-allowance checks. The existing ignored CLI
  public-process test requires separately built daemon/example binaries and
  remains outside the scoped nextest run.

## Motivating adapter reproduction

The read-only sibling MLflow contract snapshot has SHA-256
`54873de3b57a5ad3a263e07eef5814ca7a3288ef686ff7409baead93c65d92ee`.
It reproduces four RDB-C049 diagnostics: the `tag.value` and
`tag.value_digest` writes in `SetExperimentTag`, and the
`run_name_tag.text_value` and `run_name_tag.value_digest` writes in `UpdateRunInfo`.
Each uses an unconditional unsigned parent revision fence.

A temporary copy changing only those four `overwrite` tokens to `set` compiles
all 14 commands. Removing the parent revision comparisons from that copy
produces four RDB-C048 diagnostics on those same child writes, even with the
parent revision increments and lifecycle requirements retained. The sibling
checkout was not edited. Its advisory failure is an expected migration
requirement, not an unexplained acceptance failure.

## Compatibility and follow-up

The cookbook and known-limitations page now describe the local and root-fence
rules. The generated editor highlights include the new cookbook example.
Recompiling a fenced child overwrite requires `set` and the normal plan-hash
and application-lock update. Conditional fences and more general arithmetic
remain outside this structural analysis; they are not inferred by this POC.

This evidence settles the compiler defect. It does not by itself measure
independent agent authoring, MCP catalog selection, or the R-06 risk.
