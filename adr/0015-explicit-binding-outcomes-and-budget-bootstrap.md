# ADR-0015: Explicit Binding Outcomes and Budget Bootstrap

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13
- **Amends:** ADR-0002 grammar-v1 binding productions and corpus
- **Decision deadline:** Before the WP-030 grammar follow-up and WP-040 fixtures

## Context

Accepted grammar version 1 gives `create` a declared duplicate outcome but gives
`read` and `mutate` no way to declare entity absence. The canonical
`AllocateBudget` command therefore has two conflicting requirements: an absent
budget can occur, while `DSL-009` requires business precondition failures to map
to declared outcomes and the public error taxonomy has no implicit `NotFound`
escape hatch.

The authoritative POC scenario also requires seeding a budget through a compiled
command, but the canonical contract contains no creation command. Direct fixture,
storage, or administrative seeding is forbidden.

The human maintainer accepted this exact text on 2026-07-13.

## Decision

This is a pre-release correction to grammar version 1. Every entity binding has
one explicit terminal binding-failure outcome:

```text
read = "read" Ident "(" expr_list ")" "as" Ident
       "else" outcome_expr
mutate = "mutate" Ident "(" expr_list ")" "as" Ident
         "else" outcome_expr
create = "create" Ident "(" expr_list ")" "as" Ident
         "else" outcome_expr
```

For `read` and `mutate`, `else` means the entity is absent. For `create`, it means
the entity already exists. The outcome is a declared business result, not a
public infrastructure error, and is typed/deduplicated with other command
outcomes. Binding-failure payload expressions may use validated command inputs
and deterministic constants only; they cannot reference the missing/new binding,
another entity, `tx.time`, `tx.date`, or storage state.

The old read/mutate spelling without `else` is invalid grammar-v1 source after
this correction. No parser alias or compiler-synthesized outcome is accepted.
The language reference, AST, parser fixtures, canonical SPEC examples, and parser
fuzz wrappers change together.

Upon exact-text acceptance, this ADR explicitly amends ADR-0002: its `read` and
`mutate` productions and the sentence saying they retain the prior Section 7.2
spelling are replaced by the productions above. All other ADR-0002 grammar,
bounds, dependency, diagnostic, and compatibility decisions remain unchanged.
The accepted ADR-0002 record receives a non-semantic cross-reference to this
amendment, and the pre-release grammar-v1 compatibility corpus is regenerated
rather than supporting both spellings.

The canonical `AllocateBudget` binding becomes:

```text
mutate Budget(organization_id, fiscal_year) as budget
  else BudgetNotFound {
    organization_id: organization_id,
    fiscal_year: fiscal_year
  }
```

The canonical `LegalSpend` contract also adds this command before
`AllocateBudget` in both SPEC copies and checked fixtures:

```text
command CreateBudget {
  input idempotency_key: string<128>
  input organization_id: uuid
  input fiscal_year: i64
  input approved_amount: decimal<28,2>

  idempotency_key idempotency_key
  create Budget(organization_id, fiscal_year) as budget
    else BudgetAlreadyExists {
      organization_id: organization_id,
      fiscal_year: fiscal_year
    }

  require positive_approval: approved_amount > 0.00
    else InvalidApprovedAmount { minimum: 0.01 }

  set budget.approved_amount = approved_amount
  set budget.allocated_amount = 0.00
  set budget.updated_at = tx.time

  return BudgetCreated { budget: budget }
}
```

The command initializes every non-key `Budget` field exactly once, uses the same
aggregate conflict derivation and commit-coordinator path as all mutations, emits
no allocation event, and has no storage/admin bypass. The POC demo's deterministic
seed step invokes `CreateBudget` with approved amount `100.00`.

## Options Considered

1. **Mandatory declared binding outcomes plus `CreateBudget`:** Selected. It keeps
   absence and duplicate existence inside typed command semantics.
2. **Implicit generic not-found execution error:** Rejected. It bypasses declared
   outcomes and leaves retry/recovery behavior unowned.
3. **Assume bound entities always exist:** Rejected. Runtime state can violate the
   assumption and the canonical first-run scenario starts empty.
4. **Seed storage or use an admin write:** Rejected. It creates an authoritative
   mutation bypass.
5. **Optional `else`:** Rejected. It preserves the same semantic hole for sources
   that omit it.

## Consequences

- The already merged WP-030 parser receives a focused grammar-v1 correction
  before WP-040 starts; this is not WP-040 compiler behavior.
- Existing pre-release read/mutate fixtures must add declared absence outcomes.
- Stable-ID allocation and schema fixtures include `CreateBudget` and its declared
  outcomes from the first canonical lineage version.
- WP-045 and the final demo use the same long-lived command-based seed path.

## Compatibility and Security

This deliberately changes the unreleased grammar-v1 compatibility corpus. After
acceptance, the corrected productions and canonical sources are immutable v1
boundaries. Explicit outcomes prevent adapters from leaking storage-specific
not-found behavior and keep missing keys out of public diagnostics unless the
contract deliberately returns them.

## Testing

- Parser valid/invalid fixtures for all three mandatory `else` bindings.
- Exact canonical source equality between SPEC Sections 7.2 and 23.1 and the
  checked LegalSpend fixture.
- Compiler assertions that failure payloads are input/constant-only and typed.
- Runtime/reference-model cases for missing read, missing mutate, duplicate
  create, successful create, idempotent replay, and concurrent duplicate create.
- End-to-end seed through `CreateBudget` with no direct authoritative write.

## Requirements and Work Packages

- **Requirements:** `DSL-004`, `DSL-009`, `DSL-010`, `DSL-011`, `POC-001`,
  `POC-002`, `CMP-001`
- **Defines or blocks:** WP-030 grammar follow-up; `WP-040`; `WP-045`; `WP-200`
- **Final evidence:** `WP-080`, `WP-100`, `WP-150`, `WP-200`

## Decision Deadline

The exact text must be accepted before parser code changes, canonical source
changes, or WP-040 binding/outcome interfaces merge.
