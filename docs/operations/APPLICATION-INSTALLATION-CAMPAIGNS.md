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

The existing `application deploy`, migration, role, credential, driver, and
seed operations remain the available executors. First-party driver proof and
nonempty seed receipt verification are still part of WP-568, so the campaign
stops before those stages and cannot seal a terminal receipt. Until those
receipts and their controller composition land, do not interpret a `running`
campaign as a completed application deployment.
