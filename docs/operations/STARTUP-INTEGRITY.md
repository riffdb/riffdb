# Startup Integrity and Clean Restarts

RiffDB chooses its startup-validation mode automatically. There is no flag,
configuration setting, API, or maintenance shortcut that can force the faster
mode or suppress complete validation.

After a graceful shutdown, RiffDB writes a private clean-close lifecycle record
as the final authoritative database mutation. The record binds the database
identity and history incarnation, durable-format registry, exact application
and administration frontiers, selected journal header, fixed metadata roots,
active contract catalog, active query modules, and initial authority roots.
The next process may use bounded startup validation only when all of those
facts still match and the storage engine reports no repair. Before any writer,
worker, maintenance task, or public service becomes active, RiffDB durably
replaces that clean state with its next dirty generation. A crash after
readiness therefore cannot leave reusable clean evidence.

Missing, dirty, malformed, stale, copied, or contradictory lifecycle evidence
selects complete startup validation. So do storage-engine repair, format or
registry migration, restore, history-incarnation change, an incomplete journal
tail, or any mismatch in a bound root. These cases are ordinary fail-closed
recovery behavior; RiffDB does not ask the operator to classify the preceding
shutdown.

## Assurance boundary

A clean restart is a continuity proof, not a full database scrub. It verifies
the bounded roots needed to establish operational readiness without walking
every live or retained population row. Rows skipped by clean startup retain
their local envelope, checksum, canonical-key, schema, size, and required
relationship checks before they can affect authorization, execution, output,
delivery, projection, or mutation. Latent corruption unrelated to the bounded
roots can therefore be reported when the affected row is first accessed.

Dirty startup retains the complete exact-end structural and historical
validation path. RiffDB also retains a sealed internal primitive for that
complete-integrity work. It is not a public operation, storage handle,
startup-mode selector, or operator shortcut. The authorized maintenance
service and CLI are not yet available, so operators cannot request an on-demand
full scrub. Do not simulate one by editing or deleting lifecycle metadata.

## Graceful shutdown

Configured columnar sources remain cold while the authoritative graph reaches
core readiness. Startup resolves their bounded checked registrations but does
not open or decode a generation artifact, build a snapshot, catch up, publish,
or checkpoint. Consequently a no-demand graceful close also performs no
columnar population work. The `projection` health component is degraded while
any registered columnar source remains cold or is activating; this derived
health signal does not withdraw authoritative storage, catalog, or command
readiness.

The first semantic projected read requests activation from the single
server-owned columnar worker and immediately receives the existing typed
`Building` result with no rows. Concurrent first reads coalesce onto that same
activation, and cancelling a requesting read does not cancel the shared worker
operation. There is no prewarm, eager-mode, activation, artifact-selection, or
validation-bypass setting in the CLI, configuration, SDKs, MCP, or transports.

Only the current selected generation becomes queryable after its complete
bounded validation. A corrupt, mismatched, stale, excessive, incompatible, or
unavailable selection fails closed and serves no rows; RiffDB never substitutes
another generation, layout, source, frontier, or authoritative scan. Repeated
requests do not create an artifact reopen loop. Correct the underlying storage
or selection problem and restart the process; a separately published vector
generation may also be admitted through its normal durable lifecycle. Do not
weaken freshness or emulate the query against authoritative rows.

Shutdown first stops admission and drains writers, workers, and the journal.
If the columnar worker is catching up, it observes the internal stop only after
finishing its current authoritative page of at most 64 commit records. It may
discard later unpublished in-memory columnar work at that boundary. Any safe
prefix published before the stop remains valid, but the stop itself does not
publish, notify, checkpoint, or advance the durable projection frontier.
There is no application, agent, configuration, or operator option for choosing
this behavior or changing its page size.

After restart, columnar state resumes from its last durable projection frontier
and replays the authoritative log idempotently. Projected reads can temporarily
return their existing typed building or lagging outcome until replay catches
up; this does not mean authoritative entity or commit data was lost. An
operator should retain the authoritative history required by the projection
and allow normal catch-up rather than editing projection files or weakening a
query's declared freshness policy.

After the durable journal-suffix barrier, RiffDB inspects the optional
validated-prefix checkpoint through fixed metadata and table cardinalities. It
never rebuilds, repairs, deletes, or rewrites that proof during shutdown. An
exact-current checkpoint and every companion proof row remain byte-identical;
an absent checkpoint remains absent, and stale or ineligible bytes remain
unchanged. Those states do not prevent a clean close because the lifecycle
certificate is independent evidence.

RiffDB then rereads the bounded lifecycle roots and writes the clean lifecycle
record with immediate durability as the final database mutation. Failure before
that commit leaves DIRTY; commit uncertainty is resolved on reopen as either the
complete prior DIRTY state or the complete CLEAN successor. A later dirty open
still verifies the retained prefix and exact suffix or performs complete
validation. Shutdown never moves that population work into the close path.

The internal shutdown receipt reports only a closed checkpoint disposition, a
closed lifecycle outcome, and three saturating stage durations. It contains no
database identity, path, frontier, generation, hash, key, value, row count, or
engine diagnostic.

Startup and shutdown durations are observations, not correctness deadlines or
comparative database benchmarks. Their cost depends on durability mode,
storage engine recovery, dataset shape, filesystem, and whether the preceding
close produced eligible clean evidence. The emitted lifecycle observations use
only closed stage names, bounded durations, success/failure state, and the
automatically selected startup mode. They contain no database path, identifier,
frontier, table population, hash, key, or application value, and none is an
accepted configuration or command input.

See also [Compatibility](../compatibility.md), [Backup and
Restore](../backup-restore.md), and [Troubleshooting](TROUBLESHOOTING.md).
