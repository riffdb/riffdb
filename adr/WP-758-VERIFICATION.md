# WP-758: obligation and ADR form governance

Package: WP-758. Tier: guarantee. Completed: 2026-09-22.

The 143-entry outstanding ledger is removed. Its entries have 64 proof-backed
declarations and 79 explicit strike reasons in the owning records. The same
audit disposed of 18 previously untracked imperative sentences. All 161 source
sentences are bound to their declaration or strike by whitespace-normalized
SHA-256; editing an obligation cannot silently inherit another sentence's proof.
The checker rejects a recreated ledger, undeclared obligations, stale bindings,
and comments or prose offered as function definitions.

The report has zero acknowledged and zero undeclared obligations. Separately,
the pre-existing v2 declarations have 120 existing proofs and 69 outstanding
proofs owned by open packages. Those 69 remain visible and fail their package
closure; they are not represented as discharged. Acceptances from 2026-09-21
require an existing proof at acceptance. The contributing guide explains how
to register future work through package requirements.

`check-adr-form` enforces canonical sections for new records, warns above 260
lines, and rejects multi-paragraph SPEC document-control cells. The frozen
inventory preserves 201 historical noncanonical forms under ADR-0181's rule
that existing records are not rewritten. It pins exact section names and
multiplicities; it does not exempt later heading changes. All 254 records pass
with 84 length warnings. The template references the eleven-term glossary.

## Validation and failed attempts

- Scoped WP-758 acceptance passed all eleven steps after implementation.
- Obligation and form self-tests cover introduced ledgers, undeclared and
  changed obligations, proof impostors, package closure, malformed metadata,
  missing/unknown sections, historical section drift, fenced examples and
  document-control paragraphs. Both pass on the final tree.
- The full `./scripts/ci-all` completed with exit status zero, including
  workspace tests, Clippy, documentation, dependency checks, generated
  artifacts, operator conformance, Helm and source-install verification.
- The first full attempt reached generated-artifact verification and failed
  because the app-baseline nested lock still referenced the previous CRC
  dependency. Commit `74f05ad84` synchronizes it with the already accepted
  workspace graph; the locked runner now builds.
- The second attempt exposed an intermittent subscriber test with zero
  captured records. Commit `de1803458` runs that assertion in a child process
  isolated from concurrent no-subscriber emitters and requires five records
  in both subscriber layers. Production tracing and redaction are unchanged.
  The original canary assertions remain. The final full run passes.

The full run reports two nonfatal cargo-machete IO notices for intentionally
unrendered template/negative-fixture `src` directories, while dependency
analysis passes. The installed Maturin 1.15.0 also warns that the Python build
declaration pins 1.14.1; the resulting wheel's operator conformance passes.
These are environment/tool notices, not ignored test failures. The 84 ADR
length warnings are the deliberately nonblocking outcome required by ADR-0181.

## Compatibility and handoff

The contribution guide and SPEC's historical ledger references are updated.
There is no application, operator, wire, storage, or SDK behavior change.
Historical decision bodies are preserved; the source-specific dispositions
were reviewed under the requester's explicit delegation to approve ADRs.

Keep the 69 older package-owned proofs visible until their owners supply them.
Do not use the historical form inventory to exempt new ADRs or change an old
record's section structure without a recorded governance decision. The removed
ledger is not a replacement backlog for either form of work.
