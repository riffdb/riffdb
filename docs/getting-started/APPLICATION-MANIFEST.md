# Symbolic application source and exact lock

The normal author-owned input is `riffdb.application.json`. New project-shaped
repositories use `riffdb.application-source/v7`; existing V1 through V6
documents retain their exact writers and decoders. The source names paths, operations, roles, and output
targets. It never asks an author or agent to discover, copy, or maintain a
compiler-derived identity.

The compiler writes `riffdb.application.lock.json` using the lock generation
corresponding to the source schema (V1 through V8 today). That exact lock covers the normalized symbolic
source; contract source, bundle, plan-root, lineage, version, and compiler
formats; query module, source, and plan identities; tenant-unbound role
definitions and their exact operation authority; and every generated artifact
hash. It contains no clock, host path, credential, active pointer, tenant
value, or runtime-selected identity.

```text
author source: riffdb.application-source/v1
compiler lock: riffdb.application-lock/v1
```

Source and lock V1 are permanent compatibility formats. They contain Rust,
TypeScript, and MCP generation targets and are never upgraded implicitly. A
Python application uses the explicit V2 successor, which adds exactly one
required generation member:

```json
{
  "generation": {
    "mcp": "generated/ticketdesk.mcp.json",
    "python": "src/ticketdesk/generated.py",
    "rust": "src/generated/riffdb.rs",
    "typescript": "src/generated/riffdb.ts"
  },
  "schema": "riffdb.application-source/v2"
}
```

Preview migration locally with `riffdb application migrate --to v2`. The
`--write` form atomically changes only `riffdb.application.json`; it does not
write a lock, generate code, deploy, bind a role, seed data, or contact a
server. Run `riffdb application lock --write` separately after reviewing the
canonical source change. Lock V2 then covers the exact generated Python path,
bytes, and digest alongside the existing three artifacts.

Application Source V3 adds an exact `migrations` array while retaining the V2
Python target. Each entry names one `.riffm` source and one retained canonical
parent bundle. Lock V4 pins those sources, parent identities, generated
migration bundles, and the one canonical successor bundle. Source V3 is for
contract-data migration; `application migrate --to v2` remains only the
source-format V1-to-V2 helper and does not create V3.

```json
{
  "schema": "riffdb.application-source/v3",
  "migrations": [
    {
      "parent_bundle": "retained/ticketdesk-v1.riffdb.contract.bundle",
      "source": "riffdb/migrations/ticketdesk-v1-to-v2.riffm"
    }
  ]
}
```

See [Contract Migrations](../contracts/MIGRATIONS.md) for the complete identity
model, supported Gate A changes, and read-only planning workflow.

Application Source V4 adds declared `.riffr` reactive modules and three closed
role allowlists: `event_streams`, `watch_queries`, and `agent_subscriptions`.
The compiler emits exact Application Manifest V2 and Lock V5 artifacts. Lock V5
pins separate reactive source, operation, and module hashes and retains the V4
migration and canonical contract-bundle closure.

```json
{
  "schema": "riffdb.application-source/v4",
  "reactive_modules": [
    {
      "name": "TicketActivity",
      "source": "riffdb/reactive/ticket_activity.riffr",
      "version": 1
    }
  ],
  "roles": [
    {
      "agent_subscriptions": ["TicketAgent"],
      "commands": [],
      "environment": "development",
      "event_streams": ["TicketEvents"],
      "name": "TicketDeskAgent",
      "queries": [],
      "tenant_scope": "tenant",
      "watch_queries": ["TicketWatch"]
    }
  ]
}
```

These members are required arrays in V4, including when a particular role has
no operations of that kind. Consume authority never implies stream seek
authority. See [Reactive Modules](../reactive/MODULES.md) for grammar and fixed
bounds.

Application Source V5 adds one exact Go generation target. It emits
Application Manifest V3 and Lock V6; the lock pins Go, Rust, TypeScript,
Python, MCP, contract-bundle, migration, and reactive artifacts in one closure.
The `reactive_modules` member remains required but may be empty—an application
with named reads and commands does not invent a dummy stream.

```json
{
  "schema": "riffdb.application-source/v5",
  "generation": {
    "go": "generated/go/client.go",
    "mcp": "generated/mcp/tools.json",
    "python": "generated/python/client.py",
    "rust": "generated/rust/client.rs",
    "typescript": "generated/typescript/client.ts"
  },
  "migrations": [],
  "reactive_modules": []
}
```

`riffdb new --language rust|go|typescript|python` uses this same V5/V3/V6
identity chain for every language. Selecting a language changes the runnable
starter, not the compiled application identity or operation schemas.

Application Source V6 adds a required symbolic `row_policies` allowlist to
every role. It emits exact Application Manifest V4 and Lock V7. The lock's V3
role-definition receipt names each selected policy, protected entity, and
operation class; executable policy bytecode remains covered by the contract
bundle hash and is never copied into an application request or generated
client.

```json
{
  "schema": "riffdb.application-source/v6",
  "roles": [
    {
      "agent_subscriptions": [],
      "commands": [],
      "environment": "development",
      "event_streams": [],
      "name": "DocumentReader",
      "queries": ["GetDocument"],
      "row_policies": ["DocumentAccess"],
      "tenant_scope": "tenant",
      "watch_queries": []
    }
  ]
}
```

WP-570 exposes this schema for compiler review and exact-lock generation.
Protected queries, commands, projections, live views, event delivery, and
contextual reactions now use the shared transaction-current evaluator. A
database-wide reactive wakeup remains unavailable to protected roles because
it has no compiler-owned stream and partition identity; use the protected
consumer's bounded `next`/long-poll operation instead. Application middleware
is not an accepted substitute. See
[Compiled Row Policies](../security/ROW-POLICIES.md).

Application Source V7 keeps V6 command, query, reactive, migration, role, and
row-policy semantics, but replaces the complete generation object with one
nonempty closed subset of `rust`, `go`, `typescript`, `python`, and `mcp`. It
emits Application Manifest V5 and Lock V8. Each present member is generated in
memory and hashed into the exact lock; an absent member is not generated,
hashed, checked, materialized, or used to discover a language toolchain.

```json
{
  "generation": {
    "go": "generated/go/client.go"
  },
  "schema": "riffdb.application-source/v7"
}
```

The five target names and their artifact kinds come from one compiler-owned
closed registry. Paths must be unique. Empty maps, aliases, unknown targets,
unsafe paths, missing declared artifacts, and extra undeclared SDK or MCP lock
artifacts fail closed. Removing a target changes source, manifest, and lock
identity; RiffDB leaves any old undeclared file in place as unowned content
until the user removes it explicitly.

`riffdb.toml` may request only targets declared by V7. It may materialize a
subset in one checkout, but it cannot add a target to the lock or change the
authoritative identity. Omitting `mcp` suppresses only the generated catalog
file; it does not disable or change the hosted MCP service, authorization, or
operation visibility. A local generated driver still needs a closed dispatch
catalog. `riffdb dev --run` recompiles that catalog once from the exact source
and lock into its private temporary directory, verifies every operation against
the locked role, and passes its compiler-domain hash through the local
handshake. The temporary runtime catalog is not an application artifact, does
not appear in Manifest V5 or Lock V8, and is removed with the disposable
development process.

The source document is closed JSON with exactly these top-level members:

```json
{
  "application": "ticketdesk",
  "contract": {
    "lineage": "TicketDesk",
    "source": "riffdb/contract.riff",
    "version": 1
  },
  "generation": {
    "mcp": "generated/ticketdesk.mcp.json",
    "rust": "src/generated/riffdb.rs",
    "typescript": "src/generated/riffdb.ts"
  },
  "query_modules": [
    {
      "name": "ticketdesk",
      "queries": [
        {
          "name": "TicketPage",
          "source": "riffdb/queries/ticket_page.riffq"
        }
      ],
      "version": 1
    }
  ],
  "roles": [
    {
      "commands": ["CreateTicket"],
      "environment": "development",
      "name": "TicketDeskAgent",
      "queries": ["TicketPage"],
      "tenant_scope": "tenant"
    }
  ],
  "schema": "riffdb.application-source/v1",
  "seed_inputs": ["riffdb/seed/dev.jsonl"]
}
```

All paths are normalized, workspace-relative paths. Absolute paths, parent
components, empty components, duplicate paths, duplicate names, undeclared
query references, and unknown members are rejected. Versions are positive.
Names and collections are bounded; the source document may not exceed one MiB.
No hash field is valid in author source.

ADR-0121 also permits the domain-empty bootstrap emitted by `riffdb init`: it
contains one named structural query module whose `queries` array is empty and
an empty `roles` array. The module still receives a canonical identity for
generation and deployment, but it exposes no query, command, tool, or
authority. Authors add explicit queries and roles as the domain grows; no
placeholder operation is required.

Each role declares an exact environment and either `global` scope or `tenant`
scope. A tenant-scoped role must receive one concrete tenant at binding time;
a global role rejects a tenant argument. The role author names only commands
and named queries. Field visibility, stable IDs, indexes, partitions, result
shape, and scan ceilings are compiler-private consequences of those names. If
a selected `.riffq` named query declares an exact secret returned leaf, the
compiler also derives its `query/entity/field` authority atom; application
source has no field-authority list or wildcard.

RiffDB sorts every set-like collection, serializes the validated source as
compact canonical JSON with a final line feed, and hashes those bytes under the
source version's domain. The compiler exposes byte spans for
source-defined symbols and paths so diagnostics can point to the reviewed
input. Reformatting or reordering set-like members does not change identity;
changing a semantic value does.

Lock compilation resolves all symbolic names against exact compiled inputs and
privately derives role authority. Locked generation byte-compares the
recompiled lock before writing. It never silently selects an ambient active
version, trusts an author-provided hash, widens a role, or regenerates against
a different identity.

The older `riffdb.application-manifest/v1` exact document remains a supported
compatibility artifact for deployments and stable V1 tooling. It includes
bundle and module hashes, but is now emitted under
`generated/riffdb.application.exact.json`; application authors do not edit it.
The checked-in TicketDesk compatibility fixture is
`fixtures/application-manifests/ticketdesk-v1.json` in the repository root.

Run source, lock, generation, and drift checks with:

```bash
riffdb application check
riffdb application lock --write
riffdb application lock --check
riffdb application generate --locked
./scripts/check-application-bindings
```

Generated Rust and TypeScript facades are present in V1; V2 also requires the
generated Python facade. V3 and V4 retain all four generation targets. V5 adds
the generated Go facade and requires all five targets; V6 retains that complete
set. V7 declares any nonempty subset and binds exactly that subset in Manifest
V5 and Lock V8. The language facades own
parameter serialization, response decoding,
exact identity checks, opaque cursors, read-after-commit fences, typed command
outcomes, and retry-safe uncertainty recovery. Generated MCP schemas come from
the same operation registry but omit secret-output queries. Application code
uses the generated facades for those authorized reads; it does not turn the
query into a reveal-shaped command.
text; numeric IDs, field masks, protobuf records, and raw RPC wrappers remain
generated or internal implementation details.

Check, inspect, bind, and revoke roles symbolically:

```bash
riffdb role check riffdb.application.json --role TicketDeskAgent --tenant acme
riffdb role describe riffdb.application.json --role TicketDeskAgent --tenant acme
riffdb role bind riffdb.application.json \
  --role TicketDeskAgent \
  --tenant acme \
  --principal app:ticketdesk \
  --actor-kind service \
  --audience ticketdesk \
  --credential-output .riffdb/ticketdesk.credential
riffdb role revoke <capability-uuidv7> --reason replaced
```

If the role selects a row policy with declared principal facts, add one
operator-owned `--principal-facts <JSON_OBJECT_PATH>` document. Its object keys
must exactly match the symbolic `principal_fact_schemas` reported by
`riffdb role describe`; values use the natural JSON forms documented in
[Row policies](../security/ROW-POLICIES.md). Generated application clients
never receive the document, policy bytecode, or a policy-bypass parameter.

`tenant_scope` is exact author intent. A role declaring `"tenant"` requires
`--tenant <tenant-id>` on check, describe, bind, development binding, and
provisioning. A role declaring `"global"` requires that `--tenant` be omitted.
The CLI reports `application_role_tenant_required` or
`application_role_tenant_forbidden` with that precise recovery action before
role compilation; it never silently widens a tenant role or invents a tenant.

The compiled role has a domain-separated identity covering the manifest,
contract, immutable query modules, environment, tenant binding, named
operations, compiler-derived visibility, and resource ceiling. That identity
is retained as a non-authorizing capability marker for audit and substitution
detection. The marker cannot execute any operation.

## One identity chain at deployment

`riffdb application deploy` is the normal installed path for contract, query
module, application role, generated client/MCP configuration, and seed data.
It binds these steps into one checked chain:

```text
application lock
  -> pinned canonical contract bundle
  -> exact parent version + bundle hash
  -> contract bundle hash
  -> query module version + hash
  -> reactive module version + hash + exact query dependencies
  -> compiled role identity
  -> capability ID + private credential
```

The private deployment journal records each exact identity after remote
verification. Query-module publication uses the observed active module hash as
its compare-and-swap precondition. A concurrent change reports the selected
database and the lock, locked-module, expected-active, and actual-active hashes
instead of degrading to `invalid input`. A process interruption before local
publication is recovered by rerunning the same deploy; retained request and
capability identities make replay deterministic.

For a V4 application, deployment then publishes each `.riffr` module against
that exact contract and the canonical set of locked query-module identities.
The server recompiles source and the CLI accepts the result only when the
returned module name, version, content hash, and contract identity equal the
lock. Immutable replay is accepted; a same-name/same-version content change,
missing contract, or missing query-module dependency fails closed. The private
deployment journal records reactive module identities separately, so a process
interruption after remote publication resumes by verifying and replaying the
same immutable module rather than inventing a new identity.

A changed lock is deliberately not resumed under an old role. If a role is
already retained, deploy requires `--provision-role <name>` together with
`--replace-role-credential`. The replacement is a resumable rotation: RiffDB
retains the predecessor, creates and proves the exact successor, atomically
switches the protected generated client and MCP configuration, and only then
revokes the predecessor. A retry resumes from the retained phase; it never
revokes the only proven credential. Application code and scripts should
read identity from the generated client or lock and should never hardcode a
contract version or module hash.

Role compilation for lock V3 always consumes
`generated/riffdb.contract.bundle` and the exact locked query modules. Both
`application bind-dev-role` and standalone `role check|describe|bind` discover
that lock from either `riffdb.application.json` or the generated exact manifest.
A successor is never reinterpreted as genesis during role compilation. The
legacy `--replace-expired-credential` spelling remains an alias for
`--replace-role-credential`.

Lock V3 makes the contract bundle part of this same chain. Genesis
`application lock --write` remains local. For a successor, the command performs
an authorized, read-only server preview against the active parent and writes
`generated/riffdb.contract.bundle`; it does not deploy. Offline
`application lock --check` and `application generate --locked` decode that
pinned bundle instead of recompiling the successor as genesis.

When a V3 lock already pins byte-identical contract source, `application lock
--write` refreshes query, role, and generated-client artifacts locally from that
checked bundle. This is the supported path after installing a corrected code
generator: it rewrites compiler-owned artifact hashes without treating the
already-active contract as another successor. Any contract-source change still
requires the authorized server preview against the active parent, and a damaged
or substituted pinned bundle fails closed instead of falling back to the
network path.

Lock V4 retains the V3 contract-bundle pin and adds exact direct-parent
migration artifacts. `riffdb migration plan --application
riffdb.application.json` validates and prints that local plan. In the current
WP-406 implementation this command is inspection only; it cannot mutate or
activate a database.

Before deployment, the server compares the lock's expected parent version and
bundle hash and its candidate bundle hash. A disagreement returns the lock,
expected-parent, actual-active, and server-compiled candidate identities and
changes no catalog, module, role, seed, or deployment-journal state. If the
exact candidate is already active, retry is an idempotent success.
