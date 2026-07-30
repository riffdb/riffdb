# WP-365 sealed canary protocol

The evaluator supplies one sealed bundle, one brief, one existing empty
application directory, and one existing empty evidence directory. The agent
has no RiffDB implementation or TicketDesk source. Network access is disabled.

Use only:

- binaries on the supplied `PATH`;
- public documentation and SDK/runtime files inside the bundle;
- the selected brief;
- the empty application and evidence directories.

Do not inspect another filesystem tree to find examples or implementation
details. Do not ask for product guidance. A missing public instruction is a
compiler/runtime failure or unsupported shape, not a reason to access source.

Write these evidence files:

- `events.jsonl`, conforming to `event-schema.json`;
- `qualified-events.jsonl`, conforming to
  `qualified-event-schema.json`;
- `report.json`, conforming to `report-schema.json`;
- `riffdb.application.lock.json`, copied byte-for-byte from the completed
  application.

All evidence is value-free. Never include credentials, endpoints, command
inputs, entity identifiers, fixture values, returned application records, or
source text. Stable operation names and compiler/runtime identity hashes are
required and are not application values.

The ordinary event transcript records chronology. Record `first_write` and
`first_page_read` only after runtime success. Each must have exactly one
qualified event:

- first write: elapsed time, generated command name, returned/replay-validated
  command plan hash, contract identity from the generated package, durable
  commit sequence, and replay status;
- first page read: elapsed time, generated query name, returned contract,
  module, query, and plan identity, application head, and the exact prior
  commit used as the read-after-commit fence.

If the public generated result does not expose and verify an identity required
by the qualified schema, do not infer it or read product source. Record no
qualified success, report the public product defect, and leave the run failed.

Before `complete`, run the bundled application-boundary checker against the
application and the domain's complete workload. The rating is the agent's
independent assessment; it must never be chosen to satisfy the gate.
