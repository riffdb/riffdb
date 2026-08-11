# Application Installation Campaigns

RiffDB installation campaigns retain one caller-stable campaign identity and
one immutable, compiler-checked application plan. They are an operator surface,
not an application credential, generated-client, or MCP capability.

The campaign is deliberately not described as one transaction. Contract and
module publication, offline migration, role reconciliation, credential
rotation, driver proof, and ordinary command seeds retain their own durable
boundaries. An interrupted campaign reports the exact completed stages and the
next safe action; only a sealed receipt is installation success.

## Start or resume

The plan must be canonical `riffdb.application-installation-plan/v1` bytes and
the campaign ID must be a caller-retained UUIDv7:

```bash
riffdb --config operator.toml application install \
  --plan installation-plan.json \
  --campaign-id 018f2f85-3c20-7a31-8f11-112233445566
```

The CLI validates the complete plan locally before making a connection. A
transport retry changes only the outer request ID; the campaign ID and plan
bytes remain exact. Reusing the campaign ID with another plan or lineage fails
closed. If the outcome remains uncertain, the CLI returns the campaign ID and
the recovery action `observe_application_installation`.

Observe retained progress without resubmitting local plan bytes:

```bash
riffdb --config operator.toml application installation \
  --campaign-id 018f2f85-3c20-7a31-8f11-112233445566
```

After every plan-declared driver has performed its exact generated-client or
driver-host identity handshake, resume the current `driver_proof` stage with
the complete plan-ordered set:

```bash
riffdb --config operator.toml application install \
  --plan installation-plan.json \
  --campaign-id 018f2f85-3c20-7a31-8f11-112233445566 \
  --driver-proof rust \
  --driver-proof typescript
```

This flag is an explicit installation-authority attestation. It does not run
the driver and must not be supplied before the controller has checked the
driver's deployed identity. A subset, duplicate, reordered value, undeclared
driver, or proof submitted at another stage fails closed.

After nonempty seed batches have run through ordinary compiled commands, the
controller can submit one canonical receipt document:

```json
{"schema":"riffdb.application-installation-seed-receipts/v1","plan_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","seeds":[{"name":"initial-data","content_hash":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","succeeded":5,"replayed":2}]}
```

The document is compact canonical JSON followed by one newline. `plan_hash`
must identify the submitted installation plan; seed rows must have the exact
plan order, names, lowercase content hashes, and item totals. It contains no
command inputs, idempotency keys, credentials, or host paths. Submit it only at
the current `seeds` stage:

```bash
riffdb --config operator.toml application install \
  --plan installation-plan.json \
  --campaign-id 018f2f85-3c20-7a31-8f11-112233445566 \
  --seed-receipts seed-receipts.json
```

`--driver-proof` and `--seed-receipts` are mutually exclusive because one
resume request can complete only the one exact current external stage.

Machine output names only symbolic stages, the plan hash, lineage, phase,
next action, typed safe failure, and a redacted terminal receipt. It never
returns bearer credentials, host paths, seed values, numeric schema IDs, or
durable storage encodings.

## Durable and authorization behavior

- Campaign state lives inside the selected RiffDB database in a versioned,
  startup-validated durable record. A separate local journal is not authority.
- Start/resume requires the dedicated installation permission for the exact
  database, environment, and application lineage. Deploy, migration, or
  capability-administration permission alone does not imply it.
- Observation first resolves the retained lineage without disclosing campaign
  contents, then authorizes that exact lineage before returning progress.
- The same campaign and plan are idempotent. Compare-and-swap disagreement is
  outcome uncertainty, not permission to overwrite another controller's
  progress.
- Partial or failed stages never carry a terminal receipt. `installed` requires
  every closed stage plus the receipt stage in dependency order.

## Current POC limit

On every start or resume, the server now reconciles the campaign against its
authoritative contract catalog, permanent migration edge, query/reactive
module records, compiled-role identity permissions, and successor capability
records. Exact already-completed remote work advances durably; absent work
remains a symbolic next action, and conflicting identity becomes a typed
partial campaign. This makes resuming existing deployment operations safe
without accepting caller-asserted remote identities.

The CLI and Rust operator SDK can resume the exact current `driver_proof` stage with
`StartApplicationInstallation::with_driver_proof` and the exact current
`seeds` stage with `with_seed_receipts`. Both forms are checked against the
immutable plan before transport and again at service admission. They cannot
assert a server-observed stage, skip or replay a stage, carry seed values, or
seal the receipt directly. An empty seed plan is completed by the server; a
nonempty plan requires one canonically ordered receipt per declared batch,
whose successful and replayed counters add to the plan's exact item count.

These two forms are controller-observed attestations made under the dedicated
installation authority. A controller must first run each declared first-party
driver identity handshake and each seed through ordinary compiled commands.
The attestation does not grant application-write authority and does not turn
seed execution into a privileged bulk-write path.

The existing `application deploy`, migration, role, credential, driver, and
seed operations remain the available executors. The current CLI `application
install` command starts, observes, and accepts exact driver/seed completion,
but does not yet invoke those executors automatically. Until that controller
composition lands, do not interpret a `running` campaign as a completed
application deployment.
