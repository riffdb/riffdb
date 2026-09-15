# WP-772 verification report

Package: WP-772. Tier: guarantee. Verification date: 2026-09-14.
Status: complete. Automated obligations are proven, and the maintainer approved
all five final codec vectors in session on 2026-09-14.

## Behavior and compatibility

The authoritative changelog V3 substrate covers the generated 62-domain catalog,
including all 52 replicated-authoritative domains. Published bounded authority
and receipt cursors use the same immutable checkpoint-plus-journal view. Exact
receipts survive overwrite, deletion, checkpoint materialization and recovery;
direct, maintenance and lifecycle receipts share their existing transaction.
Activation is exclusive and validated. Retention respects durable checkpoint
evidence and every follower, archive and bootstrap fence.

Application command ordering, atomicity, idempotency, outcome, event, provenance,
acknowledgement and supported export semantics remain unchanged. Journal V1 and
changelog V1/V2 bytes retain their original meanings. Legacy emitters are only
compatibility test evidence. WP-746 still owns production emitter/RPC, follower
apply and bootstrap activation; WP-749 owns archives. No adapter was published.

Exact source-build pins rustls 0.23.45 and rustls-webpki 0.103.14 remediate
RUSTSEC-2026-0285 under accepted ADR-0226 and D-005. No advisory exception,
provider, feature or other root dependency graph change was introduced.

## Obligation proofs

| Obligation | Actual proof | Evidence |
| --- | --- | --- |
| OBL-0186-1 | `replication_inventory_classifies_every_storage_namespace` | Physical namespace inventory, exact classification and generated catalog agreement. |
| OBL-0186-2 | `successor_changelog_receipts_survive_checkpoint_and_recovery` | Real direct groups and journal overwrite/delete; complete authority replay, original command graphs, byte-identical checkpoints, repeated recovery and process crashes. |
| OBL-0186-4 | `changelog_history_reclamation_respects_checkpoint_and_fences` | Real journaled audits, source materialization, all hold kinds, unchanged authoritative bytes and old pins, seven control/reclamation crash edges. |
| OBL-0207-2 | `changelog_v3_is_only_production_replication_identity` | Frozen V1/V2/V3 digests, round trips, resealed downgrade refusal, retained compatibility evidence and production legacy-consumer scan. |
| OBL-0226-1 | `lockfile_has_one_exact_ring_tls_stack_and_no_alternative_or_compression_stack` | Exact patched pins, single ring-backed closure; feature graph equal after only the two version substitutions. |
| OBL-0226-2 | `scripts/ci-all` | PASS on 8c0dd960, including advisory, conformance, generated-artifact, governance and release-install gates. |

All four changelog proof tests passed in full CI on 8c0dd960. An earlier run on
27da3ee0 continued
through security and workspace policy but stopped on an existing CLI conformance
argument bug: a base64url cursor beginning with a hyphen was treated as an option.
Commit 8c0dd960 binds the unchanged value with `--cursor=value`; the exact failed
operator gate then passed across Rust, Go, TypeScript, Python, CLI and MCP.

Additional tests cover actual activation, final CLEAN/first DIRTY, checkpoint
workers, unknown/interior corruption, migration, restore, watermarks, exhaustion,
hold limits, concurrency and cancellation. The integrated recovery matrix passed
all nine process-level cases after the TLS patch. Earlier public migration Gate A
passed all 70 rows; B/C, model and index-coverage gates passed, as did the three
explicitly selected migration recovery cases.

## Checks

- Full CI: PASS on 8c0dd960, `/tmp/riffdb-wp772-ci-all-cursor.log`; all 25
  generators, Helm checks, authoring acceptance and release-source install smoke
  passed alongside workspace/xtask lint, tests, doctests and rustdoc.
- Integrated recovery: PASS, 9 cases, `/tmp/riffdb-wp772-recovery-full-tls.log`.
- Scoped TLS/checker acceptance: PASS, 15 steps, base dcc24c0f.
- Scoped cursor-check repair acceptance: PASS, 7 steps, base 27da3ee0.
- Operator acceptance after cursor repair: PASS, all six surfaces.
- `cargo deny check` and `cargo audit`: PASS, no advisory exemption.
- Targeted direct-TLS real-process acceptance: PASS.
- Verification-report acceptance: PASS, 8 steps, base 8c0dd960;
  `/tmp/riffdb-wp772-verification-acceptance.log`.
- Final closed-package full CI: PASS, `/tmp/riffdb-wp772-ci-all-closure.log`,
  including the completed-package obligation check and release-install smoke.
- Closure acceptance: PASS, 8 steps, base af689099;
  `/tmp/riffdb-wp772-closure-acceptance.log`. The obligation-checker self-test,
  including all negative script-wiring cases, also passes.

Closing the package exposed a checker-only omission: `scripts/ci-all` was not
recognized as its own full-battery entry point. The checker now recognizes that
existing root without requiring self-invocation; missing, unwired and lookalike
scripts still refuse in `full_battery_root_proof_preserves_script_wiring`.
Neither the accepted obligation nor the full CI command changes.

Scope expansions and ADR acceptance were committed separately before the affected
implementation. Earlier substrate proof changes passed their scoped acceptance
and 1,438 tests (nine pre-existing exclusions); explicit recovery runs supplement
rather than misrepresent those exclusions. Full CI logs are local evidence, not
published artifacts. No benchmark or zero-regression performance claim is made.

## Human review

The maintainer approved the generated inventory and initial V1/V2/V3 vectors,
then the three source-hold vectors. ADR-0186 Amendment 1 and ADR-0226 have their
authorized standalone commits. On 2026-09-14 the maintainer replied, in session,
"Approve all five fixtures" to the final review batch. Approval covers these
unchanged synthetic format vectors (SHA-256 of the tracked .hex files):

- `changelog-receipt-v3-command-admission.hex`: `e7928a72aeb36572ccfb000cfa03312f3b0a6d637f808c699f5ae7669736756b`.
- `changelog-receipt-v3-command-execution-failure.hex`: `e13e1bfbb58557f163f0e338c4dd82797888d779f6907dca81dc0babcda9712e`.
- `changelog-receipt-v3-command-execution-failure-audited.hex`: `2cda40cd4618d882bae6f0936c29606cd9d74eb34d8296d7ab0e6276b3db33cd`.
- `changelog-frame-v3-command-lifecycle.hex`: `fdaa985cd863f6e89fbd2dc112a663fdd056dbc4d75300c4884922a7eefe8e5d`.
- `changelog-frame-v3.hex`: `09c1b936d3cbe41e9c6fed224fc6c0d1bc32edcf8bc3ced272e6d9b4a78a6895`.

Fixture approval does not accept a new architecture record or prove application
record validity. The exact implementation choices are recorded in WP-772's
`closure.decisions_taken` and the corresponding commit `Decisions:` lists.

## Hazards and follow-ups

- Replication networking, consumer durability evidence, bootstrap attachment and
  audited hold release remain WP-746; private substrate values grant no authority.
- The root and agent-alpha Rust locks received only the two exact TLS patches.
  Stale independent example/fuzz locks were not broadly refreshed; published
  template/scaffold locks were not silently rewritten. Their separate dependency
  and publication work remains outside this source-build security claim.
- CI emitted the pre-existing duplicate-version notices from cargo-deny and an
  environment warning that installed maturin 1.15.0 differs from the unchanged
  Python build-system pin 1.14.1. No pin was relaxed or warning hidden.
  Cargo-machete's missing `src` notices concern the unmaterialized application
  template and intentionally invalid negative-kernel-dependency manifest; both
  manifests document why there is no target source, and the check passes.
- Documentation: [Changelog V3 substrate](CHANGELOG-V3.md), reachable from the handbook,
  describes current ownership, formats, compatibility and POC limits;
  [Security](../security.md) identifies the patched source-build baseline.
- Completed worktrees are removed only after their closure commit is on main;
  preserved unmerged recovery branch tips must not be removed as completed work.
