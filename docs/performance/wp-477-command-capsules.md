# WP-477 Canonical Command Capsules

WP-477 changes the physical storage of a successful command without changing
its application semantics or public types. One retained, sequence-keyed command
capsule owns the facts shared by the persisted outcome, provenance, and linked
Started/successful audit records. The existing idempotency, provenance, and
audit tables retain their accepted lookup keys but store checked locators to
that capsule instead of duplicate payloads.

The `commits` table stores a separate, prunable commit extension containing
read dependencies, entity references, event references, and outbox identities.
A commit read joins that extension with the retained capsule and authoritative
event rows in one snapshot. Outcome retry, provenance, and audit reads join only
the retained capsule, so they remain valid after normal history retention has
pruned the commit extension and event bodies. A commit read below the retention
watermark still returns the existing typed `HistoryPruned` result.

Every locator is fail-closed. Startup and normal reads verify its physical key,
located sequence, selected audit member, reconstructed identity, and reciprocal
capsule data. A missing capsule is corruption, not ordinary absence and not
pruned history. Entity state, index state, durable events, routes, outbox
intents, commit sequence assignment, acknowledgement, idempotency, provenance,
and audit atomicity are unchanged.

## Upgrade behavior

Opening the immediately preceding durable registry runs an offline migration
before operational ports are exposed. The migration:

- processes commands in sequence order through retained successful audit
  lifecycles;
- handles at most 500 commands and 4 MiB of inspected plus replacement bytes in
  one durable page;
- validates the old commit, outcome, provenance, audit, and request-index graph
  before replacing any row;
- migrates retained commands whose prunable commit body was already removed by
  retention;
- permits mixed old/new rows only while the predecessor registry remains
  active;
- resumes by revalidating complete capsule rows after interruption; and
- publishes the successor registry only after the complete locator/capsule
  graph validates.

Rollback to a binary that predates command capsules is unsupported after the
successor registry is published. Backup and restore preserve the exact rows and
run the same dormant migration when the restored registry is a predecessor.

The capsule and locator records are storage-internal. gRPC, RiffQL, CLI, MCP,
Rust, TypeScript, and Python application interfaces continue to expose the
established typed command outcome, commit, provenance, and audit views.
