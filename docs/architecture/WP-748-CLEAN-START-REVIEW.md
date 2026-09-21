# WP-748 — V2 primary admission and clean-start eligibility

Status: accepted by the maintainer in this Codex session, 2026-09-18. Package: WP-748.
Tier: guarantee. This amendment does not change any durable bytes.

Acceptance: the maintainer quoted this review and answered “Approve exact
amendment”. Restored authority commit: `a5b9cfa6`; incorporated in ADR-0156 Amendment 6,
ADR-0157 Amendment 1, and SPEC PERF-019.

## Reproduced conflict

The accepted promotion-fencing amendment requires contradictory source admission
state to fail closed at startup. ADR-0157 section 3 freezes the V1 clean-close
binding stream and says adding an item requires a successor lifecycle record
and separately accepted ADR. The stream excludes the new primary-admission row.

`v2_clean_root_admission_rollback_must_not_reuse_v1_binding` constructs a valid
V2 source, commits its complete fence, then substitutes its original canonical
Active admission row. Full retained-history validation rejects the contradiction.
The V1 bounded-state hash remains identical and the bounded root reader accepts
the substituted row. This is a test of the clean-start primitives, not a claim
that a complete production promotion/startup exploit has been demonstrated.
Observed failure: `/tmp/wp748-v2-clean-binding-red.log`, 2026-09-18.

## Exact accepted amendment

> Qualify ADR-0156's automatic clean-start selection, ADR-0157's lifecycle
> eligibility, and PERF-019 for AuthoritativeStateCatalogV2 source databases.
> A V1 clean-close certificate does not establish complete source-admission
> evidence for such a database. V2 source startup must decline that certificate
> and perform complete ordinary validation, including retained fence evidence,
> before granting source authority. Missing or contradictory admission remains
> corruption; validation must never synthesize Active or clear a fence.
>
> V2 sources may retain the existing V1 DIRTY/CLEAN lifecycle and its exact
> transitions and bytes. CLEAN cannot select a bounded fast startup for a V2
> source. V1 database behavior and every existing hash, key, record, registry
> identity and compatibility fixture remain unchanged. Attached followers keep
> their existing lifecycle rules and gain no local lifecycle writer.
>
> V2 source startup therefore has the cost of complete validation. Documentation
> and evidence must state this limit and must not claim population-independent
> clean startup for V2. This is an explicit qualification of PERF-019 for V2,
> not permission to weaken validation or reinterpret the V1 hash. Restoring a
> bounded V2 clean-start path requires a separately accepted successor lifecycle
> design and its durable-format and crash proofs; this amendment authorizes no
> successor record and no implementation of one.

## Acceptance evidence required

- Preserve the stale-Active regression and prove clean eligibility declines.
- Prove V2 sources take complete validation after CLEAN; missing, substituted
  and contradictory admission cannot grant authority.
- Preserve V1 bounded clean-start and lifecycle crash/compatibility tests.
- Document the V2 startup cost and keep the outstanding fast-start design visible.
- Run WP-748 scoped acceptance before committing the runtime correction.

## Interface safety and review reason

No caller gains a bypass, new option, writer, sequence selector or authority.
The cost is slower V2 startup. AGENTS.md requires review when a test reveals
a specification/accepted-ADR conflict; D-003 prohibits resolving it through an
implementation-package deviation. Existing fencing approval does not approve
changing ADR-0157's frozen hash or this PERF-019 qualification.
