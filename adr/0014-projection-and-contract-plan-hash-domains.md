# ADR-0014: Projection and Contract Plan Hash Domains

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13
- **Decision deadline:** Before WP-040 plan interfaces or fixtures merge

## Context

ADR-0011 accepted `PlanHash` and `riffdb.plan/v1` for executable command plans.
ADR-0013 requires separate identities for projection rebuild plans and the
aggregate semantic plan set in a contract bundle. Reusing `PlanHash` for all
three would erase domain meaning at Rust boundaries and conflict with the
existing command-plan type documentation.

The human maintainer accepted this exact text on 2026-07-13.

## Decision

The ADR-0011 unkeyed hash-domain registry is extended, without changing its
algorithm or outer frame, by exactly these entries:

| Typed digest | Domain label | Owner |
|---|---|---|
| `ProjectionPlanHash` | `riffdb.projection-plan/v1` | one validated projection plan |
| `ContractPlanRootHash` | `riffdb.contract-plan-root/v1` | one bundle's ordered semantic plan set |

`riffdb-types` owns both as distinct 32-byte newtypes and exposes typed hash
functions. `PlanHash` remains command-plan-only. No conversion among the three
typed digests is provided.

The SHA-256 algorithm, `RIFFDB-HASH\0` frame, scheme byte, length framing, and all
existing labels remain exactly as accepted by ADR-0011. ADR-0013 owns the
canonical payload bytes for the two new domains. Existing golden vectors do not
change; new cross-domain vectors prove that identical payload bytes produce
different digests in all three plan domains.

## Options Considered

1. **Separate typed domains:** Selected. It preserves newtype meaning and explicit
   domain separation.
2. **One `PlanHash` with an internal kind byte:** Avoids two types but permits
   accidental substitution at component boundaries.
3. **Use only `ContractBundleHash`:** Cannot identify an unchanged projection or
   aggregate plan set independently of source and compatibility metadata.

## Consequences

- This is an additive hash-registry change before either new digest has durable
  data.
- The WP-010 follow-up changes only `riffdb-types` hash IDs/functions, registry
  collision tests, and golden fixtures.
- Adding another semantic hash meaning still requires its own accepted owner and
  domain; internal enum tags are not a substitute.

## Compatibility and Security

Labels and meanings are immutable after acceptance. These are content identities,
not signatures or authorization proofs. Code must not accept a raw `[u8; 32]`
where one of the typed digests is required.

## Testing

- Golden payload/digest vectors for both labels.
- Central registry uniqueness and identical-payload cross-domain inequality.
- Compile-time/API assertions that command, projection, and root hashes are not
  interchangeable.

## Requirements and Work Packages

- **Requirements:** `ID-003`, `CMP-020`, `CMP-021`, `PRJ-001`
- **Defines or blocks:** WP-010 interface follow-up; `WP-040`; `WP-170`
- **Final evidence:** `WP-140`, `WP-170`, `WP-200`

## Decision Deadline

The exact text must be accepted before the new hash types, functions, or fixtures
merge and before ADR-0013 is used by WP-040.
