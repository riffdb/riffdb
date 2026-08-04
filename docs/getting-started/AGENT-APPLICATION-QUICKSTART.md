# Agent application quickstart

The normal RiffDB application path is contract and RiffQL text compiled into
generated application operations. The kernel gRPC protocol is not an
application-development API.

Before replacing the sample contract, read the repository-local `AUTHORING.md`
and complete its transaction-route worksheet: list each command's complete
atomic mutation set, assign that set one aggregate root and route parameter,
then list the route for every page. Define entity keys and indexes only after
those decisions. This prevents the most expensive authoring mistake—modeling
records as independent roots and later discovering that one command must
change them atomically.

```text
$ riffdb new order-desk
created application `order-desk` at order-desk
next: read order-desk/AUTHORING.md, replace the sample domain, then run `riffdb application check --source-only`

$ cd order-desk
$ riffdb dev --seed --run
generated exact application bindings
application boundary check passed
riffdb-dev-seed-v1  1  application-manifest
riffdb-dev-context-v1 database=default role=OrderDeskApplication
riffdb-dev-ready-v1 http://127.0.0.1:... ... OrderDeskApplication
```

The context line names values that application clients need but that must not
be guessed from the role symbol. Pass `database=default` as the generated
client's database alias. The readiness line remains positional for existing
launch supervisors; the context line is labelled for humans and agents.

`riffdb new` compiles before it writes and refuses to overwrite an existing
file or non-empty directory. It accepts either a new child directory or an
already-created empty directory:

```text
mkdir my-empty-repository
cd my-empty-repository
riffdb new order-desk --directory .
```

The destination must be a regular, writable, empty directory. A destination
symlink, file, populated directory, or missing parent fails before publication.
The repository is compiled in a bounded sibling staging directory. A new child
is published as one directory generation. An existing directory retains its
inode; known top-level entries are moved in and the compiler lock is published
last. Interrupted staging is therefore never treated as a valid application
lock, and the retained sibling staging directory is recoverable evidence rather
than silently discarded.

The scaffold creates:

```text
riffdb.application.json
riffdb.application.lock.json
riffdb/
  contract.riff
  queries/item_page.riffq
  seed/01-CreateItem.jsonl
generated/
  riffdb.application.exact.json
  rust/client.rs
  typescript/client.ts
  mcp/tools.json
src/
```

The Rust scaffold additionally includes `Cargo.toml` and an exact
`Cargo.lock`; its first `cargo check --locked` is reproducible without a
separate resolver step. TypeScript and Python scaffolds likewise include their
native exact locks.

`riffdb.application.json` is author-owned symbolic source. It contains names
and paths, never compiler hashes, numeric IDs, masks, plans, or encoded keys.
`riffdb.application.lock.json` is compiler-owned and pins the exact contract,
query plans, role authority definitions, compiler formats, and generated
artifact hashes. The compatible V1 exact manifest under `generated/` is also
compiler-owned.

The explicit authoring operations are:

```text
riffdb application check --source-only
riffdb application lock --write
riffdb application check
riffdb application generate --locked
```

`check --source-only` performs no writes and compiles the complete author-owned
application and roles without comparing an intentionally stale lock during an
edit cycle. Exact `check` performs no writes and verifies source, lock, and
every generated artifact; it cannot report a false green over drift.
`lock --write` is the only operation that accepts
new derived identities. It stages generated files and publishes the lock last.
`generate --locked` reproduces bindings only when the current symbolic sources
compile to the exact reviewed lock. A stale, substituted, interrupted, or
partially generated application therefore fails before role binding,
authorization, deployment, or application execution.

Every successful check also prints the configured seed-input count and the
exact development command. Use `riffdb dev --seed --run` only with one or more
ordered `seed_inputs`; use `riffdb dev --run` for an intentionally seedless
application. The seeded form fails before startup when the list is empty and
names up to eight unreferenced JSONL files as an actionable correction.

Generated query calls pin the compiler-owned plan hash as well as the contract
and module. Successful Rust and TypeScript results expose the exact
server-returned contract, module, query, and plan identity after verifying the
whole tuple. A missing or different identity rejects the response; application
code never infers an identity from its request or lock.

Complete public references:

- [contract language and bounds](../contracts/)
- [command and invariant cookbook](../contracts/COMMAND-INVARIANT-COOKBOOK.md)
- [application source schema](application-source-v1.schema.json)
- [symbolic inspection](INSPECTION.md)
- [RiffQL v1](../riffql/LANGUAGE.md)
- [authoring diagnostics](AUTHORING-DIAGNOSTICS.md)

Every new scaffold also includes `AUTHORING.md`, a local compact guide to the
safe aggregate, relationship, invariant, indexed-page, dependent-key batch,
and generated read-after-write patterns. Start there while replacing the
sample; use the complete references when a diagnostic points to a narrower
rule.

The sealed kit also provides `riffdb-builder-mcp --workspace <path>`. It is a
credential-less local MCP server with reference resources and only six
authoring tools: describe, check, diagnostic explanation, lock preview,
explicit lock write, and exact generation. It cannot deploy, bind a role,
execute a query or command, read storage, or obtain a credential. The mutating
lock/generation tools remain explicit and are marked accordingly in MCP.

`riffdb dev` starts one local server, waits for explicit readiness, deploys the
exact contract and module, compiles and binds the symbolic application role,
refreshes the lock only for its product-owned ephemeral database, regenerates
all bindings, checks the application boundary, executes seed JSONL
through ordinary idempotent commands, and shuts down its child on failure or
interrupt. Credentials live only in a mode-protected temporary directory.

With `--run`, the same product-owned workflow starts the repository's generated
application after readiness. A repository must contain exactly one supported
runner: `Cargo.toml` for Rust or `package.json` for TypeScript. RiffDB passes
the loopback endpoint and scoped application credential directly to that child;
the application does not need to parse the readiness line or keep a separate
bootstrap process alive. Rust one-shot runners exit normally. TypeScript web
runners remain attached and may print `riffdb-app-ready-v1` before accepting
requests. Application stdout is caller-owned output and is streamed directly,
not copied into RiffDB diagnostics or logs.

A release installation places the reviewed `riffdb-dev` workflow beside the
`riffdb` and `riffdbd` binaries. That installed workflow takes precedence over
an application-local script, so opening an unfamiliar repository cannot
replace the development control plane. Source checkouts retain the
application-local fallback for first-party development. The release workflow
uses only installed binaries and public packages; it never rebuilds or reads
RiffDB implementation source.

## The application boundary

Handwritten application code may use the stable application facade and
compiler-generated operations. It may not:

- depend on kernel, storage, service, protobuf, gRPC, Tonic, or Prost packages;
- construct raw entity/index requests, field masks, numeric schema IDs, or
  encoded keys;
- pack or decode named-query records by hand;
- create handwritten RiffDB transport wrappers;
- place handwritten code under `generated/`.

Run the mandatory check with:

```text
scripts/check-application-boundary .
```

Generated bindings contain a compiler marker and are the only files allowed to
contain protocol adaptation. That exception is narrow: application authors
cannot obtain a trusted marker by moving handwritten code into the generated
tree.

Administrative and kernel credentials remain separate from the generated
application role. The normal scaffold has no kernel dependency, feature, role,
credential, MCP tool, or documentation path. Kernel access is an explicit
source-repository operational workflow and must not reuse an application
credential.

## Whole-query bounds

`Limit` means a caller may request any page size through the 500-row service
ceiling. The compiler therefore charges a `take $limit` binding as 500 rows,
even when the parameter has a smaller default. Index-scan charges from every
binding in one named query are added before a role is accepted.

For a page with several independently bounded collections, use reviewed fixed
limits whose aggregate index-scan maximum is at most 500:

```riffql
many comments from Comment
    where site_id == $site_id && post_id == post.post_id
    order by created_at asc, comment_id asc
    take 100 after $comments_after

many post_tags from PostTag
    where site_id == $site_id && post_id == post.post_id
    order by tag_id asc
    take 50
```

`riffdb application check` performs this role derivation before writing a lock
or starting a server. `RDB-AR007` names the role and query when its aggregate
worst-case scan exceeds the stable bound. RiffDB never replaces this rejection
with a hidden scan, partial result, or larger ambient grant.

## Product safety rule

Application authors express only symbolic intent. The compiler owns every
derived identity, access plan, schema, capability requirement, visibility set,
cost bound, cursor codec, and transport adaptation.

If an application can hand-author or override one of those derived values, the
surface is a kernel or administrative API, not an application API. Normal
templates, generated clients, CLI commands, MCP tools, and documentation must
not expose it. RiffDB rejects the whole operation on identity, authorization,
cardinality, boundedness, locality, or generated-artifact drift; it does not
silently omit fields, choose an ambient version, accept a broader role, or
continue with partially updated output.
