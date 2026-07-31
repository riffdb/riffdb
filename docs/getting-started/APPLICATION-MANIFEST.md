# Symbolic application source and exact lock

The normal author-owned input is `riffdb.application.json` using
`riffdb.application-source/v1`. It names source paths, operations, roles, and
output paths. It never asks an author or agent to discover, copy, or maintain a
compiler-derived identity.

The compiler writes `riffdb.application.lock.json` using
`riffdb.application-lock/v1`. That exact lock covers the normalized symbolic
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

Each role declares an exact environment and either `global` scope or `tenant`
scope. A tenant-scoped role must receive one concrete tenant at binding time;
a global role rejects a tenant argument. The role author names only commands
and named queries. Field visibility, stable IDs, indexes, partitions, result
shape, and scan ceilings are compiler-private consequences of those names.

RiffDB sorts every set-like collection, serializes the validated source as
compact canonical JSON with a final line feed, and hashes those bytes under the
`riffdb.application-source/v1` domain. The compiler exposes byte spans for
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
[`fixtures/application-manifests/ticketdesk-v1.json`](../../fixtures/application-manifests/ticketdesk-v1.json).

Run source, lock, generation, and drift checks with:

```bash
riffdb application check
riffdb application lock --write
riffdb application lock --check
riffdb application generate --locked
./scripts/check-application-bindings
```

Generated Rust and TypeScript facades are present in V1; V2 also requires the
generated Python facade. They own parameter serialization, response decoding,
exact identity checks, opaque cursors, read-after-commit fences, typed command
outcomes, and retry-safe uncertainty recovery. Generated MCP schemas come from
the same operation registry. Application code uses these facades or RiffQL
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

A changed lock is deliberately not resumed under an old role. If a role is
already retained, deploy requires `--provision-role <name>` together with
`--replace-expired-credential`, revokes the predecessor capability, and binds
the role compiled from the successor lock. Application code and scripts should
read identity from the generated client or lock and should never hardcode a
contract version or module hash.

Lock V3 makes the contract bundle part of this same chain. Genesis
`application lock --write` remains local. For a successor, the command performs
an authorized, read-only server preview against the active parent and writes
`generated/riffdb.contract.bundle`; it does not deploy. Offline
`application lock --check` and `application generate --locked` decode that
pinned bundle instead of recompiling the successor as genesis.

Before deployment, the server compares the lock's expected parent version and
bundle hash and its candidate bundle hash. A disagreement returns the lock,
expected-parent, actual-active, and server-compiled candidate identities and
changes no catalog, module, role, seed, or deployment-journal state. If the
exact candidate is already active, retry is an idempotent success.
