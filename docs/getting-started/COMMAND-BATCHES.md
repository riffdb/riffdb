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

For `riffdb dev --seed`, RiffDB selects the command's compiler-declared
idempotency input automatically. A command may call it `request_id`,
`request_key`, or another valid symbolic name; seed files use that exact name.
The standalone `riffdb command batch` operation retains its explicit
`--idempotency-field` option and defaults that option to `idempotency_key`.

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

Exact decimals use a string tag, never a JSON floating-point number:

```json
{"unit_price":{"$decimal":"12.34"},"total":{"$money":{"currency":"USD","amount":"12.34"}}}
```

The decimal text accepts canonical base-10 notation with an optional leading
minus and optional fractional digits. Exponents, a leading plus, leading
zeroes such as `01.25`, and a trailing decimal point are rejected. The
selected command schema supplies the declared precision; the text supplies
the exact coefficient and scale. The older explicit
`coefficient_twos_complement`/`scale` object remains accepted for compatible
machine-generated inputs.

Successful and declared business outcomes are terminal per item. Add
`--error-outcome OutcomeName` to classify a durable declared outcome as a
recorded item failure for import policy purposes. Authorization, contract,
validation, and idempotency-reuse failures are also terminal and resumable.
Transport uncertainty, cancellation, deadlines, and storage unavailability
remain pending; a later invocation resubmits the identical command input and
key through normal idempotency recovery. Response-size rejection is a terminal
recorded item failure under the CLI path (one ordinary `Execute` per line).

When any item is rejected, `--output json` includes at most 16
`rejected_items`, each containing only its one-based `ordinal`, optional
symbolic `outcome`, public `error_code`, and public `error_message`.
`rejected_items_truncated` reports how many additional rejections were omitted.
The summary never copies command input, idempotency keys, credentials, or
business values. Use the ordinal to correct the corresponding JSONL line, then
start a reviewed new batch or resume only items still marked pending.

The checkpoint is also the stable receipt. To avoid turning one small seed into
hundreds of checkpoint `fsync` operations, the CLI replaces it atomically after
each bounded wave of 16 observed terminal results and once more before a normal
or interrupted return. A process kill may therefore lose receipt knowledge for
at most one transport wave, never the server's idempotency identity or durable
outcome. Resume replays those same command inputs and keys, so committed items
recover their stored results without duplicating business state.
An application deploy scopes its generated seed-checkpoint filename to the
locked contract version. A successor therefore starts a distinct checkpoint
without deleting the predecessor receipt. A manually selected checkpoint from
another version fails with
`batch_checkpoint_contract_version_mismatch`, naming the checkpoint file and
both versions; resume it against its original version or archive it after
reviewing the prior outcomes.

Generated Rust, TypeScript, and Python clients expose the same bounded policy
for every symbolic command. Rust emits
`create_ticket_batch(inputs, options)` and returns input-ordered independent
item results plus a checkpoint. TypeScript emits
`createTicketBatch(inputs, { concurrency, checkpoint })`; Python emits paired
sync and async `create_ticket_batch` methods. Each uses a bounded worker or
transport pool and records either the typed result or the public error for each
item. All three reject zero items, more than 4,096 items, and concurrency
outside `1..=384` before submitting work. This SDK ceiling is distinct from
the CLI's operator-facing `1..=32` bound documented above.

Both bindings report a monotonically increasing completed count and the largest
contiguous input checkpoint. Completion may arrive out of order, but a reported
checkpoint advances only after every earlier item has an independent result.
Persist that checkpoint and pass it back when resuming; skipped inputs are never
resubmitted, while any uncertain later item still retains its original
idempotency key.

Batch methods do not introduce a second write semantic. The Rust SDK and CLI
coalesce independent items into the public bounded transport batch
(`ExecuteBatch`, at most 16 items) while preserving each item's idempotency
key, typed outcome, and uncertainty classification. A whole-RPC failure or a
retryable per-item error re-enters only the affected ordinary commands through
the same-key recovery path. The server may also physically group compatible
durable transitions; callers still receive independent per-item results.
At SDK concurrency 384, Rust opens at most 24 simultaneous transport exchanges
of at most 16 items each; the setting never creates a 384-item RPC or an
application-visible transaction. The coordinator remains independently bounded
at 512 queued messages and 32 MiB, and one physical transaction remains bounded
at 256 commands and 16 MiB.

## Per-item results and recovery

Current servers return an always-populated, input-ordered item list on the
batch response (ADR-0084). Each item is either the ordinary command response
or a typed application error that names the batch operation. Sibling
successes stay intact when one item fails. The Rust SDK classifies each item
error by its registry-derived recovery action: `Retry` and
`ResolveWithSameIdempotencyKey` re-enter that item through ordinary
same-idempotency-key recovery (with the same attempt budget and Overloaded
backoff as a single command); other recovery actions surface immediately as
certain failures with zero re-entry. Service-control failures (cancellation,
deadline, response size, emergency containment) still fail the whole
transport RPC. Older servers that omit the item list keep the historical
whole-RPC recovery path.

A batch checkpoint is the largest contiguous input prefix that already has
an independent terminal result (success or non-reentered failure after the
item's attempt budget). Resume skips that prefix and never resubmits those
indices; retryable items are re-entered inside the original batch call before
the checkpoint advances past them.

## Why batching is not a bulk-write API

Bounded transport and client concurrency amortize admission without creating
a second write semantic. Every item still has its own authorization,
idempotency, provenance, commit sequencing, crash recovery, and typed
outcome — exactly the single-command definitions.
