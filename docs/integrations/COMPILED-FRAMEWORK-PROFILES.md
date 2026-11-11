# Compiled framework profiles

RiffDB does not implement an external framework's generic CRUD store or
transaction callback. An integration repository resolves the framework's
configured models and plugins at generation time, emits an ordinary RiffDB
contract with named commands and queries, and dispatches only to that compiled
surface. Unknown models, hooks, plugins, and transaction shapes fail during
generation instead of degrading into sequential writes at runtime.

The adapter remains application code. No framework dependency, identity
protocol, session logic, or privileged storage path enters RiffDB. A framework
"transaction" is supported only when one generated command describes its
complete atomic graph. Bounded collection commands cover finite repeated
graphs; workflow commands cover revision-checked session transitions. An
adapter must never run several commands and report that they were one atomic
transaction.

The framework-agnostic acceptance contract at
`fixtures/adapters/framework-profile/riffdb/contract.riff` demonstrates the
generic capabilities:

- transactional unique identities and provider/account links;
- one bounded atomic user/account/session create graph;
- compiler-owned session initialization plus exact-revision refresh/revoke;
- secret-classified session and verification-token digests;
- consume-once verification with transaction-time expiry refusal; and
- server-observed transaction time and UUID values.

It is a workload shape, not a bundled framework adapter.

## Repository ownership boundary

Framework-specific contracts, profiles, generated SDKs, route hosts, and route-
level compatibility tests belong to the integration repository that owns the
adapter. That repository pins an immutable RiffDB release and records its
framework version, enabled plugin/model surface, generated identities, and
acceptance observations. RiffDB does not copy those artifacts into its own
source tree.

The RiffDB repository retains framework-neutral fixtures for the compiler,
providers, authorization, generated transports, and cross-language semantics.
For exact result sets, the generic `Document` corpus proves contains, starts-
with, ends-with, typed optional filtering, deterministic orders, bounded direct
offset, and complete exact total. An adapter can depend on that public
capability, but neither its route vocabulary nor its userland policy becomes a
RiffDB feature.

External adapters can pin
`fixtures/riffql/operational-query-capability-v5.json`. V5 preserves the
immutable V1 through V4 receipts and extends the neutral operational-access
corpus with binary UTF-8 interval/complement alongside shared equality,
membership, prefix, canonical range/complement, nullable, cursor, policy, and
dependent-key shapes. It binds the exact application lock and generated Rust,
Go, TypeScript, Python, and MCP artifacts.

V5 also binds one value-free external tuple-changelog receipt demonstrating
that one bytewise text-key index can supply strict lower/upper selection and
order, and that its exact package may declare only a generated Go surface.
That receipt contains hashes and typed verification classes only; it contains
no external schema, route, adapter, generated profile, or stored value. It
explicitly does not claim external runtime, full external application, or
framework conformance.

The distinct terminal
`fixtures/riffql/wp775-external-binary-text-interval-execution-v1.json`
receipt now proves one narrow owning-repository runtime case: two invocations
of the same generated strict-lower/strict-upper query returned original logical
strings in bytewise ascending order, and the consumer reused a returned string
as its own next lower token. The run used one existing index, one partition,
two bounded result pages, and no client filter, sort, page walk, duplicate
index, or token conversion. This is not evidence that a framework route, the
complete adapter surface, or a full external application conforms; those wider
claims still require their own owning-repository acceptance.

## Better Auth support boundary

The Better Auth integration supports only the exact materialized profile its
owning repository generates, configures, and tests. The framework's generic
facade is a profile-bound dispatcher to exact generated commands and named queries;
it is not generic RiffDB CRUD or a public transaction surface. The profile fixes
its models, fields, plugins, schemas, atomic command shapes, queries, roles, and
generated identities before deployment.

Additional fields or plugins require generation and review of a new materialized profile,
followed by its generated operations, installation review, and conformance
evidence. Unsupported configuration fails while the profile is generated or
configured. Unknown runtime calls refuse instead of synthesizing an operation,
omitting a field, falling back to a generic store, or splitting one framework
transaction across several RiffDB commands.

## Keyless upstream retries

When an upstream framework supplies no idempotency key, the adapter owns the
logical request identity:

1. Mint one bounded RiffDB command idempotency value before the first dispatch.
2. Freeze the complete generated command input containing that value.
3. Reuse that exact input for every transport retry and uncertainty-resolution
   attempt until a terminal declared outcome is known.
4. Allow the driver to mint a fresh transport request ID for each attempt; the
   transport ID is not the command's idempotency identity.

If retry must survive adapter process loss, retain the mapping from a stable
upstream operation identity to the minted command identity before dispatch. If
the framework provides no stable operation identity and the adapter did not
retain one, it cannot honestly claim process-loss idempotency and must surface
that limitation. Never mint a new key after an uncertain result, and never
derive the key from a plaintext token or other secret.

The Rust driver test
`adapter_minted_idempotency_survives_driver_retries_with_fresh_transport_ids`
proves that bounded retries preserve the byte-identical command input while
using fresh transport request IDs. On the admission side,
`equal_key_commands_commit_once_and_replay_exactly` proves the property the
minted identity rides: equal-identity submissions commit exactly once, the
replay returns the byte-equal stored outcome under one commit sequence and
one provenance record, and different input under the same identity is
refused as an input mismatch.

## Adapter Compatibility Matrix (Owning Repositories, Versions, and Boundaries)

Whether an external framework is a good fit for RiffDB depends on whether its
storage contract can be expressed as a **fixed compiled profile** of entities,
commands, and bounded queries, rather than arbitrary runtime SQL callbacks or
unbounded dynamic schemas.

Each external adapter repository pins its own upstream framework version,
supported configuration, explicitly refused dynamic calls, and runnable
compatibility suite:

| Integration | Owning Repository | Pinned Upstream Target | Supported Profile Surface | Refused / Out-of-Scope Calls | Runnable Compatibility Command (in owning repo) |
|---|---|---|---|---|---|
| **OpenFGA** (`Datastore`) | `github.com/riffdb/riffdb-openfga` | `openfga v1.18.3` (Go) | Complete `storage.OpenFGADatastore` interface: stores, authorization models, relationship tuples (`Write` up to 100 deletes / 100 writes per call), `Read`, `ReadStartingWithUser`, `ReadUsersetTuples`, and `ReadChanges` changelog pagination | Tuple batch sizes above the compiled 100-item command bound | `make conformance` (`go test -v ./...` running `test.RunAllTests(t, ds)`) |
| **Better Auth** (Compiled Profile) | `github.com/riffdb/riffdb-better-auth` | `better-auth ~1.7.18` (TypeScript) | Materialized profiles (`core-auth` and `organization-security`): `user`, `session`, `account`, `verification`, `organization`, `member`, `invitation`, and admin ban/role/impersonation operations | Unmaterialized plugins, undeclared model fields, arbitrary runtime `transaction(cb)` callbacks that do not map to one compiled command, and unbounded unindexed scans | `npm test` (profile verification, route-host integration, and refusal tests) |
| **MLflow** (`AbstractStore` + Analytics) | `github.com/riffdb/riffdb-mlflow` | `mlflow==3.15.1` (Python) | Experiment/Run lifecycle, params/metrics/tags, `search_runs`, `TraceInfoV3` & out-of-order `LiveSpan` ingestion (`IngestTraceSpans`), `Assessment` CRUD, `LoggedModel` CRUD, `log_inputs`/`log_outputs`, and `query_trace_metrics` backed by `columnar_projection` and event `projection` rollups | SQL warehouse pass-through (`sql_warehouse_id`), arbitrary SQL `WHERE` clauses outside MLflow's filter grammar, and online scorer daemon management | `make conformance` (`pytest` unit, surface, trace/analytics, and live `riffdb dev` conformance suite) |
| **LangGraph** (`BaseCheckpointSaver`) | `github.com/riffdb/riffdb-langgraph` | `langgraph-checkpoint>=4.0.0` (Python) | Complete sync and async `BaseCheckpointSaver`: `put`/`aput`, `put_writes`/`aput_writes` (with deterministic conflict resolution), `get_tuple`/`aget_tuple`, `list`/`alist` (metadata filter + `before` cursor), `delete_thread`/`adelete_thread`, `copy_thread`, `prune`, and `delete_for_runs` | Cross-thread unbounded scans without a configured `scope` partition | `make conformance` (official `langgraph-checkpoint-conformance` suite + end-to-end `StateGraph` tests) |

### Worked Example: Adding a Field or Plugin to a Compiled Profile, Regenerating, and Testing

When an application adds a custom field (for example, `department:
optional<string<120>>` on `User`) or enables an additional plugin in a
materialized framework profile, you update the profile's `.riff` contract and
`.riffq` queries at build time rather than mutating schema dynamically at
runtime:

1. **Add the field to the entity and command in `riffdb/contract.riff`**:
   ```riff
   entity User {
     key (scope: string<32>, user_id: string<64>)
     field email: string<320>
     field name: string<200>
     field department: optional<string<120>>
     unique by_email (scope, email)
   }
   ```
   Update the corresponding `CreateUser` / `UpdateUser` command inputs and
   `set` / `overwrite` assignments in `riffdb/contract.riff`, and add
   `department` to the `return` projection in `riffdb/queries/get_user.riffq`.
2. **Recompile and regenerate the typed SDK bindings offline**:
   ```bash
   riffdb generate --sync-manifest
   riffdb application check
   ```
   If the new field is required on an already-deployed database, the compiler
   classifies the successor contract as `migration_required` and guides you
   through authoring a `.riffm` migration proof (see
   [Contract Migrations](../contracts/MIGRATIONS.md)). Adding an `optional<...>`
   field is backward-compatible and deploys directly.
3. **Run the profile compatibility suite against an ephemeral local server**:
   ```bash
   riffdb dev --reset --exec "npm test"
   ```
   Any call that attempts to write or filter by an undeclared field is rejected
   before hitting storage, while the newly declared `department` field is
   statically typed across the generated client and verified end-to-end.

## Acceptance evidence

Run the focused profile gate:

```bash
./scripts/adapter-framework-profile-acceptance
```

It compiles and round-trips the profile, exercises the atomic graph and token/
workflow refusal paths, runs the real-redb concurrent uniqueness schedule, and
checks retry identity preservation. The full release acceptance gate invokes the
same focused phase.
