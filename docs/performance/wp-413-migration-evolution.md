# WP-413 Migration Evolution Evidence

This evidence compares operational shape, bounded work, and failures. It is not
a general RiffDB-versus-PostgreSQL speed claim.

## Workloads

The RiffDB measurement executes the final compiler/format acceptance followed
by the populated TicketDesk Gate-A test through a real `riffdbd` child and
public gRPC. Gate A migrates 70 rows, crosses the 64-mutation transaction bound,
builds a new projection, retains its backup, retries by operation identity, and
keeps a sibling database usable.

The optional PostgreSQL control creates up to 10,000 assistant attention rows,
applies explicit DDL/DML for an enum variant, required-field backfill, index,
and new entity, then validates all rows. Its negative control attempts to add a
required column without a value to a populated table and must fail.

## Reproduce

```bash
TMPDIR="$HOME/tmp" ./scripts/migration-evolution-comparison \
  --output "$HOME/tmp/riffdb-migration-evolution.json"
```

Pass `--postgres-url` only for a caller-owned disposable database. The runner
uses a dedicated schema and removes it on exit.

## Interpretation

The RiffDB values report checked/apply rows per second, selected-database
downtime, peak database/maintenance bytes, and total matrix time. They are
bounded regression observations, not release thresholds. PostgreSQL records
only successor elapsed time because it does not implement the same artifact,
authorization, preflight, immutable backup, staged publication, or recovery
protocol.

The checked report is `release/evidence/wp413-migration-evolution-v1.json`.
It records the exact measured revision and whether a live PostgreSQL control was
available. A report without PostgreSQL remains valid RiffDB evidence but cannot
claim the SQL control executed.
