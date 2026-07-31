# Resumable command batches

`riffdb command batch` is a bounded client-side scheduler for ordinary symbolic
commands. It is not a bulk-write API and it does not create a transaction
covering the input collection.

Each non-empty JSONL line is the complete named-command input. The selected
idempotency field must be a non-empty string and must be unique within the
file:

```json
{"idempotency_key":"seed:ticket:1","organization_id":{"$uuid":"01900000-0000-7000-8000-000000000001"},"status":{"$enum":"Open"},"title":"Cannot sign in"}
{"idempotency_key":"seed:ticket:2","organization_id":{"$uuid":"01900000-0000-7000-8000-000000000001"},"status":{"$enum":"Closed"},"title":"Export is slow"}
```

Run or resume it with:

```bash
riffdb command batch CreateOrganization fixtures/batches/01-CreateOrganization.jsonl \
  --checkpoint .riffdb/create-organization.batch.json \
  --concurrency 8 \
  --progress
```

The application boundary deliberately has these semantics:

- one exact command name and optional exact contract version per input file;
- one separately authenticated public command request per line;
- one stable idempotency key, typed outcome, provenance record, and commit
  decision per item;
- at most 32 in-flight items, 4,096 items, 64 MiB of source, and 1 MiB per
  line;
- no collection atomicity, generic entity mutation, storage batch, or
  authorization shortcut;
- a checksummed checkpoint containing only source/item digests and safe public
  results, never command inputs, capability tokens, or idempotency keys;
- automatic resume only when the exact source bytes, command identity,
  expected contract version, checkpoint checksum, and every completed item
  digest still agree.

The client parses and validates the entire bounded source before submitting the
first item. Malformed input, duplicate idempotency keys, an invalid checkpoint,
or a changed source therefore fails closed without creating partial work.
JSON strings remain strings even when they resemble another scalar type.
UUIDs use `{"$uuid":"canonical-uuid"}` and enum variants use
`{"$enum":"VariantName"}`. RiffDB never guesses a type from string contents,
and neither form contains a compiler-allocated numeric ID.

Successful and declared business outcomes are terminal per item. Add
`--error-outcome OutcomeName` to classify a durable declared outcome as a
recorded item failure for import policy purposes. Authorization, contract,
validation, and idempotency-reuse failures are also terminal and resumable.
Transport uncertainty, cancellation, deadlines, and storage unavailability
remain pending; a later invocation resubmits the identical command input and
key through normal idempotency recovery.

The checkpoint is also the stable receipt. It is replaced atomically after each
completed item. A killed client may lose only knowledge of a completion, never
the server's idempotency identity; replay recovers the already durable result.
An application deploy scopes its generated seed-checkpoint filename to the
locked contract version. A successor therefore starts a distinct checkpoint
without deleting the predecessor receipt. A manually selected checkpoint from
another version fails with
`batch_checkpoint_contract_version_mismatch`, naming the checkpoint file and
both versions; resume it against its original version or archive it after
reviewing the prior outcomes.

Generated Rust and TypeScript clients expose the same bounded policy for every
symbolic command. Rust emits `create_ticket_batch(inputs, options)` and returns
input-ordered independent item results plus a checkpoint. TypeScript emits
`createTicketBatch(inputs, { concurrency, checkpoint })`; it uses a bounded
worker pool and records either the typed result or the public error for each
item. Both reject zero items, more than 4,096 items, and concurrency outside
`1..=32` before submitting work.

Both bindings report a monotonically increasing completed count and the largest
contiguous input checkpoint. Completion may arrive out of order, but a reported
checkpoint advances only after every earlier item has an independent result.
Persist that checkpoint and pass it back when resuming; skipped inputs are never
resubmitted, while any uncertain later item still retains its original
idempotency key.

Generated batch methods do not introduce a second protocol. They call the
ordinary generated command method for every item, preserve the idempotency key
already declared in each typed input, and never claim collection atomicity.
Consequently the server may physically group compatible durable transitions
while every caller still receives an independent outcome and uncertainty
classification.

## Why this is not a server batch RPC

The existing unary application-command protocol already multiplexes requests
over one reusable HTTP/2 connection. Bounded client concurrency removes the
sequential round-trip cost that made seed data slow without creating a second
write semantic. Keeping every item on the ordinary path means capability
revocation, deadlines, contract selection, provenance, command validation,
commit sequencing, crash recovery, and typed outcomes retain exactly their
single-command definitions.
