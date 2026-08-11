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

## Execute the reviewed deployment

`application deploy` can bind its existing exact deployment executor to the
same immutable plan and campaign:

```bash
riffdb --config operator.toml application deploy \
  --installation-plan installation-plan.json \
  --installation-campaign-id 018f2f85-3c20-7a31-8f11-112233445566 \
  --provision-role AppRole
```

Both flags are required together. Before opening a transport, the CLI strictly
decodes the source, exact lock, generated artifacts, manifest, query/reactive
modules, selected role, seed inputs, target database/environment/lineage, and
plan. Any mismatch fails locally and makes no remote application mutation. The
campaign is resumed immediately before deployment so conflicting remote state
also stops the invocation before the existing deploy executor runs.

The reviewed successor capability ID comes from the plan's credential
destination. The deployer never substitutes a freshly generated capability ID
for a plan-bound install or rotation. Existing local deployment state must be
empty, already name that successor, or name the plan's exact predecessor with
explicit `--replace-role-credential`.

Driver proof remains an external observation and therefore splits a seeded
installation into honest phases:

1. Run plan-bound `application deploy` without `--seed`.
2. Exercise every declared generated driver, then submit the complete
   `--driver-proof` set with `application install`.
3. Resume the same plan-bound deploy with `--seed`. Each seed remains an
   ordinary resumable command batch; the CLI submits only its value-free
   terminal counters to the campaign.

The final deploy output includes `installation_campaign_id`,
`installation_plan_hash`, `installation_phase`, and
`installation_next_action`. A running campaign is never labeled installed.
Only the server-sealed receipt yields `installation_phase: installed`.

The v1 local artifact inventory uses fixed symbolic singleton names
`manifest`, `contract`, `rust`, `typescript`, `go`, `python`, `mcp`, and
`migration`; query and reactive artifacts use their declared module names.
Manifest seed inputs use `seed-001`, `seed-002`, and so on in manifest order,
with the domain-separated content hash and exact nonempty command count. Host
paths never enter the plan.

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

Newly sealed terminal receipts use
`riffdb.application-installation-receipt/v2`. The canonical, content-addressed
document includes the exact nonsecret capability ID for every credential
destination, value-free per-seed succeeded/replayed checkpoints, the adapter
manifest digest, the installed terminal state, and exact migration and backup
receipt references when a migration was required. It never includes bearer
credentials, seed values, host paths, or hidden schema. Accepted v1 receipts
and campaign states remain readable; resuming one preserves its original
receipt identity instead of silently rotating it.

## Adapter conformance manifests

An adapter may bind a canonical
`riffdb.adapter-conformance-manifest/v1` document to the installation plan.
The document names one exact application manifest and lock, contract lineage,
generated artifacts, symbolic roles and operations, first-party driver/runtime
and platform identities, bounded conformance probes, feature dispositions, and
empty/populated evolution cases. It is declarative data: executable hooks,
shell commands, numeric storage IDs, caller-defined permissions, version
ranges, and silent fallback behavior are not fields in the schema.

Validate a manifest and its exact installation-plan binding before any remote
installation mutation:

```bash
riffdb application conformance adapter.conformance.json \
  --plan installation-plan.json
```

The command accepts only canonical encodings. When a plan carries an
`adapter_manifest_hash`, it must also contain the corresponding
`adapter_manifest` artifact content hash. Deployment preflight checks that
artifact alongside every generated application artifact; omitting or
substituting either identity fails closed. The installed receipt records the
same manifest digest, but never bearer credentials, probe values, or host
paths.

The current command validates the bounded document and exact plan identity. It
does not execute adapter tests supplied by the document; conformance probes are
first-party symbolic operation identities and expected observation digests,
not executable extension points. The release-owned adapter acceptance runner
executes those operations through public clients.

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

Plan-bound deployment composes the existing exact migration and selected-role
deploy shapes. When the campaign reaches `apply_migration`, deployment submits
the plan's locked migration bundle and confirmation under a deterministic
migration operation ID derived from the campaign ID. A running migration
returns `migration_running` with its phase and `resume_application_deploy`;
the invocation does not continue into contract, module, role, or seed work.
Rerunning the identical command resumes the retained operation, rechecks its
parent, successor, and migration identities, and advances only after the
migration reports `succeeded`. Failed-closed or substituted operations never
advance the campaign. The server also requires the campaign-derived migration
operation to have a retained succeeded apply receipt with its verified backup
manifest before the migration stage can complete, so terminal receipt
references cannot name a guessed or unrelated operation.

Every
role and credential destination remains present in the immutable plan and is
verified by the server; a multi-role application invokes the same plan-bound
deploy once per role. Each destination gets a disjoint protected deployment
state and credential directory, so one role cannot overwrite or silently
replace another. The seed invocation must select a planned role authorized for
every declared seed command. Installation never invents migration confirmation,
backup policy, or downtime approval: all three are exact, reviewed inputs of
the immutable plan and the underlying migration operation retains its existing
backup and offline-exclusive owners.
