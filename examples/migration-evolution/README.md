# Migration Evolution Comparison

This example evolves a populated assistant-style application in both RiffDB and
PostgreSQL. It is correctness and operational evidence, not a claim that unlike
migration mechanisms have identical work or should have identical latency.

The RiffDB side adds `AttentionOrigin.Email`, introduces `EmailDraft`, adds a
required field and index to existing `AttentionItem` rows, and compiles one
exact parent-specific `.riffm` artifact. The P7 acceptance runner also exercises
the populated TicketDesk Gate-A workflow through the public gRPC service,
including sibling-database availability, exact retries, retained backup, and
invalid-predecessor failure.

The PostgreSQL side expresses the corresponding evolution as operator-authored
DDL and DML. `003-invalid-successor.sql` is a negative control: a populated
table rejects an added required column with no value. This demonstrates a
database error, not RiffDB's compile-time proof or migration preflight.

Run the RiffDB evidence only:

```bash
TMPDIR="$HOME/tmp" ./scripts/migration-evolution-comparison \
  --output "$HOME/tmp/riffdb-migration-evolution.json"
```

Add a disposable PostgreSQL database owned by the caller:

```bash
TMPDIR="$HOME/tmp" ./scripts/migration-evolution-comparison \
  --postgres-url postgres://riffdb:riffdb@127.0.0.1:55432/riffdb_app_baseline \
  --rows 1000 \
  --output "$HOME/tmp/riffdb-migration-evolution.json"
```

The fixed row bound is 1 through 10,000. The script creates and removes a
dedicated schema, validates every migrated row, records monotonic durations,
and emits bounded JSON. Do not publish a report without its revision,
environment, durability, and filesystem context.
