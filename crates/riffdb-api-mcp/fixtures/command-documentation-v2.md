# AllocateBudget

Execute the compiled AllocateBudget command from contract LegalSpend version 2.

## MCP Tool

    riffdb_cmd_legalspend_allocatebudget

## Inputs

    field required type constraints
    approved_amount yes string pattern, decimal
    fiscal_year yes integer minimum, maximum
    idempotency_key yes string minimum length, maximum length
    organization_id yes string UUID

## Call Example

    {"arguments":{"approved_amount":"0.00","fiscal_year":0,"idempotency_key":"a","organization_id":"00000000-0000-0000-0000-000000000000"},"name":"riffdb_cmd_legalspend_allocatebudget"}

## Retry and Uncertainty

Cancellation after command submission may not prevent commit. Resolve an uncertain result with the same idempotency key or the returned outcome URI.

## Declared Outcomes

### Allocated

    {"budget":{"allocated_amount":"0.00","approved_amount":"0.00","fiscal_year":0,"organization_id":"00000000-0000-0000-0000-000000000000","updated_at":{"nanos":0,"seconds":"0"}},"remaining":"0.00","type":"Allocated"}

### InvalidAmount

    {"minimum":"0.00","type":"InvalidAmount"}

### BudgetNotFound

    {"fiscal_year":0,"organization_id":"00000000-0000-0000-0000-000000000000","type":"BudgetNotFound"}

### InsufficientBudget

    {"allocated":"0.00","approved":"0.00","requested":"0.00","type":"InsufficientBudget"}

## Plan Resource

    riffdb://command/LegalSpend/2/plan
