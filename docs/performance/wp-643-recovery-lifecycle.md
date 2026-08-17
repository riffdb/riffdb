# WP-643 process recovery ready/shutdown ordering

Date: 2026-08-16

## Exact failure

The unchanged process recovery gate failed after entering the successful
single-database offline-maintenance rebound path:

```text
full_recovery_matrix failed: child emitted an unexpected readiness line
```

Receipt:

- `/home/kevin/tmp/wp643-baseline.log`
- SHA-256
  `0c65e069352c879dc178fa45ddd17d16e2c11fb049e89af09993f9d077a3679d`

The process controller assumed every next stdout line requested by a caller was
readiness. A successful offline backup instead performs this valid lifecycle:

1. the public maintenance request enters `Draining`;
2. routes stop and the production graph drains;
3. the database closes and emits its complete shutdown evidence;
4. the database is reopened and the replacement graph is built; and
5. rebound readiness is published.

The first shutdown-evidence line therefore reached the strict readiness reader
before the required rebound ready line. Package-level recovery tests could not
prove this process boundary and were not treated as a substitute.

## Closed lifecycle protocol

`ChildProcessController::wait_for_evidence_then_readiness` now represents that
transition explicitly. The recovery gate declares the existing eight-line
single-database evidence protocol in its byte-stable order:

1. write completion groups;
2. dispatch reasons;
3. read stages;
4. write-service stages;
5. command stages;
6. writer evidence;
7. writer frame census; and
8. writer flush census.

All eight lines and the following ready line share one 30-second deadline.
Every line remains bounded by the process controller's existing 4,096-byte
limit. No line is skipped: absence, reordering, an additional line, a malformed
prefix, disconnect, or failure to publish rebound readiness is typed and fails
the arm. Diagnostics identify evidence-versus-readiness and the one-based
timed-out evidence ordinal without retaining line contents.

The multi-database recovery path retains its separate server lifecycle. It does
not emit this single-database eight-line block before rebound readiness and was
not made to pretend that it does; its existing recovery, terminal receipt, and
clean-stop assertions remain unchanged.

## Verification

The repaired `./scripts/recovery_full` completes all nine process arms,
including both response-loss cases, both bootstrap-retention crashes, all four
offline-maintenance crash boundaries, and multi-database staged authorization.
No crash point, expected signal, maintenance phase, durability assertion, or
recovery result was removed or reordered.

Final receipt:

- `/home/kevin/tmp/wp643-fixed-final.log`
- SHA-256
  `e6726b53ee31a3103903e224a98fc8aa7df85f9fc1e6eae1c10ec57578dc646b`

The unchanged storage/server package recovery suite is also required by the
work package. The controller has a deterministic no-sleep unit test proving a
complete ordered transition succeeds and a reordered evidence line fails.

## Scope

The package changes only the bounded recovery-test process protocol. It cannot
publish runtime readiness, authorize maintenance, suppress a server failure, or
turn an unhealthy database ready. The added `riffdb-testkit` path was registered
in WP-643 because that crate owns the process pipe and total-deadline state;
duplicating or bypassing it in one recovery test would have weakened the gate.
