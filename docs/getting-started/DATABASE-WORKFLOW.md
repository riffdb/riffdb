# Database-Shaped Project Workflow

Use the project verbs when RiffDB is one data dependency inside an existing
Rust, Go, TypeScript, or Python project. These commands do not create or own the
application's source tree, package manifest, web framework, identity system, or
process lifecycle.

The older `riffdb application ...` interface remains available for
self-contained application packages. Project verbs are an additive orchestration
layer over the same compiler, lock, authorization, deployment, and migration
paths.

## Initialize an existing project

From the project root:

```bash
riffdb init inventory --generator rust --generator typescript
```

Initialization creates only:

- `riffdb.toml`, containing client selection and project settings;
- `riffdb.application.json`, the symbolic application source; and
- `riffdb/contract.riff`, a domain-empty version-1 contract.

It creates no application code, package manifest, credential, lock, generated
binding, entity, command, query, or role. The application source retains one
zero-query structural module so subsequent generation has a stable exact module
identity; it grants no authority and exposes no operation.

Initialization is idempotent when every target byte is already exact. It
preflights the complete bounded file set and refuses before writing if any
existing target differs or is unsafe.

The generated configuration has this closed shape:

```toml
[client]
endpoint = "http://127.0.0.1:7443"
database = "default"

[project]
schema = "riffdb.application.json"
generators = ["rust", "typescript"]
```

`schema` is relative to the configuration file's directory. Generator names
are limited to `rust`, `go`, `typescript`, and `python`; the list must contain
one through four unique values. Project commands discover `riffdb.toml` in the
current directory unless `--config` or `RIFFDB_CONFIG` selects another file.
General client flags and environment values retain their documented fieldwise
precedence.

## Author and push

Edit the contract and application source using the [Contract Authoring
Reference](../contracts/AUTHORING.md), [Application Source and Exact
Lock](APPLICATION-MANIFEST.md), and [RiffQL Language](../riffql/LANGUAGE.md).
Add an explicit application role before expecting application credentials or
MCP tools; an empty project deliberately has none.

With the intended database running and an ordinary authorized capability
credential selected:

```bash
riffdb push
```

`push` composes symbolic checking, parent-aware lock compilation, exact local
publication, and the existing resumable application deployment path.

- A fresh project needs no acceptance because there is no previous identity to
  compare.
- A no-op exact push needs no acceptance and safely resumes deployment.
- A changed compiler-owned lock stops before any local or remote mutation and
  reports the proposed lower-hex lock hash. After review, accept exactly that
  proposal:

```bash
riffdb push --accept-lock <proposed-lock-hash>
```

A different or malformed value fails closed. Contract changes classified as
requiring migration or incompatible are never deployed by `push`; after the
same exact acceptance boundary, the command retains the candidate artifacts and
points to `riffdb migrate plan`.

## Generate configured SDKs

```bash
riffdb generate
```

The exact lock binds deterministic artifacts for every supported language, but
`riffdb.toml` selects which language artifacts are materialized in this
checkout. Internal manifest, MCP, contract-bundle, reactive, and migration
artifacts are always exact. An already-present unselected SDK must also remain
exact, so changing a target selection cannot silently leave stale generated
code behind.

`generate` writes only selected language targets and never deletes an
unselected target. Changing only `generators` neither changes database state nor
requires push acceptance.

## Observe local and installed identity

```bash
riffdb status
riffdb diff
```

Both commands are read-only locally and remotely. They verify the selected
local materialization and compare its exact contract and structural query-module
identity with one authenticated catalog observation from the configured
database. Results distinguish `not_installed`, `exact`, and `different` and
contain bounded symbolic names and lower-hex identities, never schema source or
credentials.

## Run a staged migration

The project wrappers preserve the existing migration gates:

```bash
riffdb migrate plan
riffdb migrate check --operation-id <uuid-v7>
riffdb migrate apply \
  --operation-id <same-uuid-v7> \
  --confirm-apply <exact-migration-hash>
riffdb migrate operation <same-uuid-v7>
```

`plan` is local and read-only. `check` is a read-only server preflight. `apply`
requires the exact locked migration hash and preserves caller-stable outcome
recovery. `operation` observes the retained operation after interruption or
database restart. See [Contract Migrations](../contracts/MIGRATIONS.md) for the
migration source and compatibility model.

## POC limitations

The POC expects a running RiffDB service and an already provisioned ordinary
capability credential for authenticated project operations. Run
`riffdb agent init` to install the generated repository skill, managed AGENTS
section, and project MCP entry; the command deliberately does not provision or
embed that credential. Public ecosystem package placement remains owned by
WP-601.
