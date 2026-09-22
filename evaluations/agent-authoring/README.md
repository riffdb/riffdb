# RDB-C048 authoring case

The [fresh-context follow-up](fresh-context-2026-09-22/README.md) now adds two
independent repair participants and eight MCP catalog-selection participants.
Both repairs compile after one edit cycle; all 48 catalog responses match the
frozen rubric under explicit-refresh, file-assisted conditions. That bounded
pilot supplies R-06 evidence while leaving native-client and broader model
qualification open. The retrospective case below remains a separate observation.

The [bounded evidence record](rdb-c048-mlflow-2026-09-21.json) preserves one
retrospective application-authoring case and four evaluator compiler replays.
It supplies evidence about a real repair burden. It does not close SPEC R-06,
which concerns MCP catalog size, stability, and model tool selection.

The requester identified the MLflow adapter work as agent-authored. Its retained
commit changes 17 `set` statements to `overwrite` and reports a successful build
and 21 tests. The diff is directly observed; the historical test result is
reported evidence. Neither the model identity, original compiler revision,
interaction count, elapsed time, nor a satisfaction rating is available. They
remain unknown. No session transcript or application data is published here.

Four edits affect experiment tags and a run-name child. Their commands already
compare the expected parent revision and increment it unconditionally. The
binding-local compiler rule nevertheless required an unguarded spelling. The
other 13 edits affect a latest-metric binding whose selection reads a different
binding; they have no qualifying guard under the accepted structural rule.
That distinction matters: the case exposes a compiler defect as well as a
legitimate need to state overwrite intent. A diagnostic-induced edit count
alone would conflate the two.

The current compiler, containing WP-809, gives these reproducible outcomes:

| Source snapshot | Evaluator result |
| --- | --- |
| Before all 17 adapter spelling edits | 13 RDB-C048 diagnostics on latest-metric writes; the four parent-fenced child writes are accepted |
| Adapter's retained 17-edit migration | Four RDB-C049 diagnostics on the parent-fenced child overwrites |
| Change those four tokens back to `set` | All 14 commands compile, grammar v23 |
| Also remove the two expected-parent-revision comparisons | Four RDB-C048 diagnostics, despite retaining revision increments and lifecycle requirements |

The JSON pins source hashes, diagnostic codes and source lines, the adapter
commit and compiler revision. Replays use the repository's offline
`check_contracts` example; they are evaluator checks with implementation access,
not fresh independent model trials. The source checkout remains unchanged.
`riffdb contract validate` is a remote authorized service call and is not this
offline driver. The initial credential-free attempt refused before compilation
and is excluded from the four compiler observations.

For a checkout containing the pinned adapter commit, extract its parent source
and committed source with `git show`. In a temporary copy of the committed
source, change only `tag.value`, `tag.value_digest`, `run_name_tag.text_value`
and `run_name_tag.value_digest` from `overwrite` to `set`. For the negative
control, remove the `current_revision` requirements in `SetExperimentTag` and
`UpdateRunInfo`. Match the resulting hashes before running:

```sh
cargo run --locked --quiet --package riffdb-contract-compiler \
  --example check_contracts -- <source.riff>
```

The source-independent regression fixtures and semantic assertions are in
[WP-809's verification note](../../docs/architecture/WP-809-VERIFICATION.md).
They additionally prove stale revision refusal and checked overflow. This case
does not independently execute the adapter or qualify a production deployment.

The observed conclusion is narrow: one application author encountered a false
positive, recognized the cross-binding limitation, and made four source edits
that the corrected compiler now reverses. It does not establish that most
models learn the language, that diagnostics alone caused a correct repair, or
that the 13 latest-metric overwrites satisfy the application's business intent.
Those claims need independent authoring and runtime evidence.

R-06 remains open. A catalog experiment needs fresh model contexts, actual
capability-filtered catalogs, predeclared small/large and stable/changed
conditions, task-level correct selection and argument outcomes, and every
failure retained. The existing sealed application campaigns establish different
authoring outcomes; they cannot substitute for missing catalog-selection data.
No catalog result or new sealed-alpha pass is inferred from this case.
