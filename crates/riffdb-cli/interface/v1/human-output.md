# Human Output Baseline

These shapes are reviewed and tested for WP-150 usability, redaction, and
boundedness, but they are explicitly **not** `riffdb.cli.output/v1`
compatibility bytes. Wording and layout may improve without a machine-schema
version change, provided the checked terminal model, redaction, exit code, and
bounds do not change.

- Success begins with `<command>: <status>` and then prints one indented
  `label: value` line per present top-level result field in DTO order.
- A collection prints its count, followed by indented indexed entries in
  semantic order. Empty collections print `[]`.
- A RiffDB Value prints its explicit type and exact scalar spelling; bytes are
  padded base64 and 64-bit integers remain canonical decimal text.
- Failure is one line on stderr: `<command>: <code>: <message>`. Structured
  public, local, client, or uncertainty detail may follow as indented fixed
  labels; arbitrary server, transport, parser, path, OS, child-process, or
  debug text never appears.
- UUIDs, hashes, sequences, enums, and optional omission follow the machine DTO
  semantics. Credentials/bootstrap documents are never rendered.
- Fully staged human output is at most 4,194,304 bytes. Rendering failure or
  overflow leaves stdout empty and writes only the matching fixed emergency
  stderr fixture.

Representative tested shapes:

```text
contract.validate: valid
```

```text
command.execute: committed
  commit_sequence: 42
  contract_version: 1
  outcome_type: Allocated
```

```text
entity.get: not_found
```

```text
capability.create: created
  capability_id: 01234567-89ab-7def-8123-456789abcdef
  revision: 1
  administration_sequence: 1
  credential_retained: true
```

```text
command.execute: outcome_unknown: the command outcome remains unknown
  recovery_action: resolve_with_same_idempotency_key
```
