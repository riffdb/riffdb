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
validation path. The separately specified authorized offline integrity-scrub
operation is not yet exposed by this POC; until it is delivered, operators
cannot request an on-demand full scrub through the public CLI. Do not simulate
one by editing or deleting the lifecycle record.

## Graceful shutdown

Shutdown first stops admission and drains writers, workers, and the journal.
The optional validated-prefix checkpoint remains non-fatal. RiffDB then writes
the clean lifecycle record with immediate durability; that record is the final
authoritative mutation. Failure to write it does not make shutdown data unsafe:
the following process sees dirty or absent evidence and performs complete
validation.

Startup and shutdown durations are observations, not correctness deadlines or
comparative database benchmarks. Their cost depends on durability mode,
storage engine recovery, dataset shape, filesystem, and whether the preceding
close produced eligible clean evidence.

See also [Compatibility](../compatibility.md), [Backup and
Restore](../backup-restore.md), and [Troubleshooting](TROUBLESHOOTING.md).
