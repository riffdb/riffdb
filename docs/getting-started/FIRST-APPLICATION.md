# Your First Application: Build From Scratch

This guide walks you through running a working RiffDB starter in under a
minute, then evolving it into a multi-command **TaskTracker** application:
authoring a contract (`.riff`), writing a bounded RiffQL query (`.riffq`),
generating a type-safe SDK offline, and running commands and queries against an
ephemeral local database with `riffdb dev`.

Make sure `riffdb` and `riffdbd` are installed on your `PATH` (see
[Installation](../installation.md#source-checkout-convenience-install)).

## 1. Run a Working Starter Immediately

Before learning the contract language, you can scaffold and execute a complete,
working application in one command. Choose **Option A** (adding RiffDB to an
existing or empty directory) or **Option B** (creating a full language-specific
starter repository with a runnable entrypoint and seed data):

### Option A: Initialize in an existing or empty directory (`riffdb init --starter`)

```bash
mkdir task-tracker && cd task-tracker
riffdb init task-tracker --generator typescript --starter
riffdb generate
```

Pass one or more `--generator` targets (`typescript`, `python`, `go`, `rust`,
`mcp`). Adding `--starter` scaffolds a ready-to-compile starter contract
(`Item` + `CreateItem`), a named query (`ItemPage`), and a least-privilege
application role (`TaskTrackerApplication`) in four files without touching your
existing application code:

| File | Owner | Purpose |
|---|---|---|
| `riffdb.toml` | Author | Project configuration and selected SDK generators |
| `riffdb.application.json` | Author | Symbolic manifest declaring contract, queries, and least-privilege roles |
| `riffdb/contract.riff` | Author | Entities, aggregates, commands, invariants, and outcomes |
| `riffdb/queries/*.riffq` | Author | Named, compiler-bounded RiffQL read queries |

*(If you omit `--starter`, `riffdb init` creates an empty contract with zero
roles; see [Database-Shaped Project Workflow](DATABASE-WORKFLOW.md).)*

### Option B: Create and run a standalone starter repository (`riffdb new`)

```bash
riffdb new task-tracker --language rust
cd task-tracker
riffdb dev --seed --run
```

`riffdb new` supports `--language rust|typescript|go|python` and includes a
runnable entrypoint (`src/`) and seed data (`riffdb/seed/01-CreateItem.jsonl`)
out of the box. Running `riffdb dev --seed --run` starts an isolated local
database, deploys the contract and query module, seeds the initial item, and
runs the generated client.

## 2. Author the Contract (`riffdb/contract.riff`)

In RiffDB, applications never issue ad-hoc `INSERT` or `UPDATE` statements.
Instead, you declare your entities, partition boundaries, invariants, and state
transitions in `riffdb/contract.riff`.

Replace `riffdb/contract.riff` with a `TaskTracker` contract that defines a
`Task` entity, a `TaskCreated` durable event, a `CreateTask` command (enforcing
a priority range `1..5`), and a `CompleteTask` command:

```riff
contract TaskTracker version 1 {
  enum TaskState { Open, Done }

  entity Task {
    key (workspace_id: uuid, task_id: uuid)
    field title: string<120>
    field state: TaskState
    field priority: u64
    field created_at: timestamp
    field completed_at: optional<timestamp>
    index by_workspace_created (workspace_id, created_at, task_id)
  }

  aggregate Tasks {
    root Task
    partition_by workspace_id
    conflict_key (workspace_id, task_id)
  }

  event TaskCreated {
    workspace_id: uuid
    task_id: uuid
    title: string<120>
    priority: u64
  }

  command CreateTask {
    input request_id: string<128>
    input workspace_id: uuid
    input task_id: uuid
    input title: string<120>
    input priority: u64
    service now: transaction_time
    idempotency_key request_id

    create Task(workspace_id, task_id) as task else AlreadyExists {}

    require ValidPriority: priority >= 1 && priority <= 5 else InvalidPriority {}

    set task.title = title
    set task.state = TaskState.Open
    set task.priority = priority
    set task.created_at = now
    set task.completed_at = null

    emit TaskCreated {
      workspace_id: workspace_id,
      task_id: task_id,
      title: title,
      priority: priority
    }

    return Created { task: task }
  }

  command CompleteTask {
    input request_id: string<128>
    input workspace_id: uuid
    input task_id: uuid
    service now: transaction_time
    idempotency_key request_id

    mutate Task(workspace_id, task_id) as task else NotFound {}

    require MustBeOpen: task.state == TaskState.Open else AlreadyDone {}

    overwrite task.state = TaskState.Done
    overwrite task.completed_at = now

    return Completed { task: task }
  }
}
```

Notice what the contract guarantees at compile time:

- **Partition & conflict safety**: Every command on `Tasks` routes by
  `workspace_id` and serializes conflicting writes on `(workspace_id, task_id)`
  so tasks in different workspaces—or different tasks in the same workspace—never
  contend.
- **Explicit overwrite semantics**: Initial field population on `create` uses
  `set`, whereas replacing an existing field on `mutate` requires `overwrite` so
  accidental clobbers are rejected at compile time.
- **Closed outcomes**: Callers receive a typed `Created`, `AlreadyExists`,
  `InvalidPriority`, `Completed`, `NotFound`, or `AlreadyDone` variant rather
  than untyped row counts.

## 3. Author a Bounded RiffQL Query (`riffdb/queries/list_tasks.riffq`)

Every read query in RiffDB is a named RiffQL query with a compiler-proven index
access plan and byte bound. Create `riffdb/queries/list_tasks.riffq`:

```riffql
query ListTasks(
    $workspace_id: Task.workspace_id,
    $after: Cursor?,
) {
    many tasks from Task
        where workspace_id == $workspace_id
        order by created_at desc, task_id asc
        take 20 after $after

    return Page {
        items: tasks {
            task_id
            title
            state
            priority
            created_at
            completed_at
        }
    }

    outcomes Page
}
```

Remove the starter `riffdb/queries/item_page.riffq` file if you created the
project with `--starter` or `riffdb new`, and update `riffdb.application.json`
so your `TaskTrackerApplication` role grants `CreateTask`, `CompleteTask`, and
`ListTasks`:

```json
{
  "application": "task-tracker",
  "contract": {
    "lineage": "TaskTracker",
    "source": "riffdb/contract.riff",
    "version": 1
  },
  "generation": {
    "typescript": "generated/typescript/client.ts"
  },
  "migrations": [],
  "query_modules": [
    {
      "name": "task_tracker",
      "queries": [
        {
          "name": "ListTasks",
          "source": "riffdb/queries/list_tasks.riffq"
        }
      ],
      "version": 1
    }
  ],
  "reactive_modules": [],
  "roles": [
    {
      "agent_subscriptions": [],
      "commands": [
        "CompleteTask",
        "CreateTask"
      ],
      "environment": "development",
      "event_streams": [],
      "name": "TaskTrackerApplication",
      "queries": [
        "ListTasks"
      ],
      "row_policies": [],
      "tenant_scope": "global",
      "watch_queries": []
    }
  ],
  "schema": "riffdb.application-source/v7",
  "seed_inputs": []
}
```

*(Tip: When you add new commands to `riffdb/contract.riff` or new `.riffq` files
to `riffdb/queries/`, running `riffdb generate --sync-manifest` automatically
discovers and registers them in `riffdb.application.json`.)*

## 4. Generate Typed SDK Bindings Offline

Compile the contract and query module, write the deterministic lockfile
(`riffdb.application.lock.json`), and generate your typed client completely
offline:

```bash
riffdb generate
```

`riffdb generate` verifies that every declared command and query compiles within
its static scan and byte bounds, writes `riffdb.application.lock.json`, and
emits `TaskTrackerClient` to `generated/typescript/client.ts` (and any other
configured target languages).

## 5. Run Locally and Call Your Generated Client

`riffdb dev` boots an isolated ephemeral `riffdbd` instance on a random loopback
port, bootstraps capabilities, deploys your contract and RiffQL module, mints a
role-bound credential in `.riffdb/dev/profile.credential`, and exports
connection variables to your application process:

- **Rust & Python**: `RIFFDB_ENDPOINT`, `RIFFDB_CREDENTIAL_FILE`, and
  `RIFFDB_DATABASE=default`.
- **TypeScript & Go**: `RIFFDB_DRIVER_SOCKET` and the compiler-verified
  `RIFFDB_DRIVER_HANDSHAKE` (or positional runner arguments when started via
  `riffdb dev --run`).

Generated `TaskTrackerClient` bindings across all four languages provide:

- **Typed command and query methods** (`createTask` / `create_task`,
  `completeTask` / `complete_task`, `listTasks` / `list_tasks`).
- **Causal read-your-writes consistency** via `causalSession()` /
  `causal_session()` or `listTasksAfterCommit` / `list_tasks_after_commit`.
- **Bounded pagination** via `listTasksPages` / `list_tasks_pages` and
  `listTasksWithOptions` / `list_tasks_with_options`.

### TypeScript (`generated/typescript/client.ts`)

```typescript
import {
  DriverApplicationTransport,
  DriverGeneratedApplicationTransport,
} from "@riffdb/application";
import { TaskTrackerClient, TaskState } from "./generated/typescript/client.js";

const [
  socketPath,
  applicationManifestHash,
  operationCatalogHash,
  database,
  role,
  roleDefinitionHash,
  remoteIdentityHash,
  contractLineage,
  contractVersion,
  contractBundleHash,
] = process.argv.slice(2);

const driver = await DriverApplicationTransport.connect({
  socketPath: socketPath!,
  identity: {
    applicationManifestHash: applicationManifestHash!,
    operationCatalogHash: operationCatalogHash!,
    database: database!,
    role: role!,
    roleDefinitionHash: roleDefinitionHash!,
    remoteIdentityHash: remoteIdentityHash!,
    contractLineage: contractLineage!,
    contractVersion: BigInt(contractVersion!),
    contractBundleHash: contractBundleHash!,
  },
});
const client = TaskTrackerClient.fromTransport(
  new DriverGeneratedApplicationTransport(driver),
).causalSession();

const workspaceId = "01900000-0000-7000-8000-000000000001";
const taskId = "01900000-0000-7000-8000-000000000002";

const created = await client.createTask({
  request_id: "req-create-001",
  workspace_id: workspaceId,
  task_id: taskId,
  title: "Ship RiffDB 0.2.0 onboarding guide",
  priority: 1n,
});
console.log("Created outcome:", created.outcome.outcome);

const completed = await client.completeTask({
  request_id: "req-complete-001",
  workspace_id: workspaceId,
  task_id: taskId,
});
console.log("Completed outcome:", completed.outcome.outcome);

const page = await client.listTasks({ workspace_id: workspaceId, after: null });
for (const row of page.value.items) {
  console.log(`${row.title}: ${row.state} (priority=${row.priority})`);
}
await driver.shutdown();
```

Expected output:

```text
Created outcome: Created
Completed outcome: Completed
Ship RiffDB 0.2.0 onboarding guide: Done (priority=1)
```

### Python (`generated/python/client.py`)

```python
import os
from uuid import UUID
from riffdb_application import (
    BearerCredential,
    CallMetadata,
    DatabaseAlias,
    SyncApplicationTransport,
)
from generated.python.client import (
    CompleteTaskInput,
    CreateTaskInput,
    ListTasksParams,
    TaskTrackerClient,
)

credential = BearerCredential.from_protected_file(os.environ["RIFFDB_CREDENTIAL_FILE"])
metadata = CallMetadata.authenticated(credential).with_database(
    DatabaseAlias(os.environ.get("RIFFDB_DATABASE", "default"))
)

workspace_id = UUID("01900000-0000-7000-8000-000000000001")
task_id = UUID("01900000-0000-7000-8000-000000000002")

with SyncApplicationTransport.connect_uri(
    os.environ["RIFFDB_ENDPOINT"], metadata
) as transport:
    client = TaskTrackerClient(transport).causal_session()

    created = client.create_task(
        CreateTaskInput(
            request_id="req-create-001",
            workspace_id=workspace_id,
            task_id=task_id,
            title="Ship RiffDB 0.2.0 onboarding guide",
            priority=1,
        )
    )
    print("Created outcome:", created.outcome.outcome)

    completed = client.complete_task(
        CompleteTaskInput(
            request_id="req-complete-001",
            workspace_id=workspace_id,
            task_id=task_id,
        )
    )
    print("Completed outcome:", completed.outcome.outcome)

    page = client.list_tasks(ListTasksParams(workspace_id=workspace_id))
    for item in page.value.items:
        print(f"{item.title}: {item.state.value} (priority={item.priority})")
```

### Go (`generated/go/client.go`)

```go
session, err := riffdb.Connect(ctx, os.Args[1], riffdb.Identity{
    ApplicationManifestHash: os.Args[2],
    OperationCatalogHash:    os.Args[3],
    Database:                os.Args[4],
    Role:                    os.Args[5],
    RoleDefinitionHash:      os.Args[6],
    RemoteIdentityHash:      os.Args[7],
    ContractLineage:         os.Args[8],
    ContractVersion:         1,
    ContractBundleHash:      os.Args[10],
})
if err != nil {
    log.Fatal(err)
}
defer session.Close()

client, err := tasktracker.NewClient(session)
if err != nil {
    log.Fatal(err)
}
client = client.CausalSession()

workspaceID := "01900000-0000-7000-8000-000000000001"
taskID := "01900000-0000-7000-8000-000000000002"

if _, err := client.CreateTask(ctx, tasktracker.CreateTaskInput{
    RequestId:   "req-create-001",
    WorkspaceId: workspaceID,
    TaskId:      taskID,
    Title:       "Ship RiffDB 0.2.0 onboarding guide",
    Priority:    1,
}); err != nil {
    log.Fatal(err)
}

if _, err := client.CompleteTask(ctx, tasktracker.CompleteTaskInput{
    RequestId:   "req-complete-001",
    WorkspaceId: workspaceID,
    TaskId:      taskID,
}); err != nil {
    log.Fatal(err)
}

page, err := client.ListTasks(ctx, tasktracker.ListTasksParams{
    WorkspaceId: workspaceID,
})
if err != nil {
    log.Fatal(err)
}
if listPage, ok := page.Value.(tasktracker.ListTasksPage); ok {
    for _, item := range listPage.Items {
        fmt.Printf("%s: %s (priority=%d)\n", item.Title, item.State, item.Priority)
    }
}
```

### Rust (`generated/rust/client.rs`)

```rust,ignore
use std::env;
use std::path::Path;
use generated::{
    CompleteTaskInput, CreateTaskInput, ListTasksParams, TaskTrackerClient,
};

let endpoint = env::var("RIFFDB_ENDPOINT")?;
let credential_file = env::var("RIFFDB_CREDENTIAL_FILE")?;
let database = env::var("RIFFDB_DATABASE").unwrap_or_else(|_| "default".to_owned());

let mut client = TaskTrackerClient::connect_with_credential_file(
    endpoint,
    Path::new(&credential_file),
    Some(&database),
)
.await?
.causal_session();

let workspace_id = "01900000-0000-7000-8000-000000000001".to_owned();
let task_id = "01900000-0000-7000-8000-000000000002".to_owned();

let created = client
    .create_task(CreateTaskInput {
        request_id: "req-create-001".to_owned(),
        workspace_id: workspace_id.clone(),
        task_id: task_id.clone(),
        title: "Ship RiffDB 0.2.0 onboarding guide".to_owned(),
        priority: 1,
    })
    .await?;
println!("Created outcome: {:?}", created.outcome);

let completed = client
    .complete_task(CompleteTaskInput {
        request_id: "req-complete-001".to_owned(),
        workspace_id: workspace_id.clone(),
        task_id: task_id.clone(),
    })
    .await?;
println!("Completed outcome: {:?}", completed.outcome);

let page = client
    .list_tasks(ListTasksParams {
        workspace_id,
        after: None,
    })
    .await?
    .into_page()
    .map_err(|_| "unexpected outcome")?;
for row in page.items {
    println!("{}: {:?} (priority={})", row.title, row.state, row.priority);
}
```

## 6. Evolve Your Schema and Deploy

As your application grows:

1. Edit `riffdb/contract.riff` or add queries under `riffdb/queries/`.
2. Run `riffdb generate --sync-manifest` to recompile, update role grants, and
   refresh your generated SDK bindings offline.
3. During local iteration with `riffdb dev`, pass `riffdb dev --reset` whenever
   you want to wipe `.riffdb/dev` and start with a clean ephemeral database.
4. When deploying to a persistent RiffDB server, run `riffdb push` (see
   [Database-Shaped Project Workflow](DATABASE-WORKFLOW.md) and
   [Contract Migrations](../contracts/MIGRATIONS.md)).

## Next Steps

- [Database-Shaped Project Workflow](DATABASE-WORKFLOW.md) — `riffdb init`,
  `riffdb generate`, `riffdb push`, `riffdb status`, and `riffdb migrate`.
- [Contract Authoring Reference](../contracts/AUTHORING.md) — entities,
  aggregates, invariants, domain events, projections, and workflows.
- [RiffQL Language](../riffql/LANGUAGE.md) — point lookups, index range scans,
  cursor pagination, text/vector search, and projections.
- [Language Guides](../sdks/RUST.md) — [Rust](../sdks/RUST.md),
  [TypeScript](TYPESCRIPT-APPLICATIONS.md), [Python](../python-driver.md),
  [Go](../sdks/GO.md), and [MCP for Agents](../mcp/agent-cookbook.md).
