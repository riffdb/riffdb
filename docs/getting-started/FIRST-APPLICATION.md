# Your First Application: Build From Scratch

This guide walks you through building a RiffDB application from scratch:
authoring a contract (`.riff`), writing a bounded RiffQL query (`.riffq`),
generating a type-safe SDK offline, and running commands and queries against an
ephemeral local database with `riffdb dev`.

Make sure `riffdb` and `riffdbd` are installed on your `PATH` (see
[Installation](../installation.md#source-checkout-convenience-install)).

## 1. Initialize Your Project

Choose either **Option A** (adding RiffDB to an existing or empty directory) or
**Option B** (creating a full language-specific starter repository):

### Option A: Initialize in an existing project (`riffdb init --starter`)

```bash
mkdir task-tracker && cd task-tracker
riffdb init task-tracker --generator typescript --starter
```

Pass one or more `--generator` targets (`typescript`, `python`, `go`, `rust`,
`mcp`). Adding `--starter` scaffolds a ready-to-compile starter contract, named
query, and application role (`TaskTrackerApplication`) in four files without
touching your existing application code:

| File | Owner | Purpose |
|---|---|---|
| `riffdb.toml` | Author | Project configuration and selected SDK generators |
| `riffdb.application.json` | Author | Symbolic manifest declaring contract, queries, and least-privilege roles |
| `riffdb/contract.riff` | Author | Entities, aggregates, commands, invariants, and outcomes |
| `riffdb/queries/*.riffq` | Author | Named, compiler-bounded RiffQL read queries |

*(If you omit `--starter`, `riffdb init` creates an empty contract with zero
roles; see [Database-Shaped Project Workflow](DATABASE-WORKFLOW.md).)*

### Option B: Create a standalone starter repository (`riffdb new`)

```bash
riffdb new task-tracker --language typescript
cd task-tracker
```

`riffdb new` supports `--language rust|typescript|go|python` and includes a
runnable entrypoint and seed data out of the box.

## 2. Author the Contract (`riffdb/contract.riff`)

In RiffDB, applications never issue ad-hoc `INSERT` or `UPDATE` statements.
Instead, you declare your entities, partition boundaries, invariants, and state
transitions in `riffdb/contract.riff`.

Replace `riffdb/contract.riff` with a `TaskTracker` contract that defines a
`Task` entity, a `CreateTask` command, and a `CompleteTask` command:

```text
contract TaskTracker version 1 {
  enum TaskStatus {
    Open
    Completed
  }

  entity Task {
    key (workspace_id: uuid, task_id: uuid)
    field title: string<1..200>
    field status: TaskStatus
    field created_at_ms: i64

    index by_created (workspace_id, created_at_ms, task_id)
    invariant title_present: len(title) > 0
  }

  aggregate TaskRoot {
    root Task
    partition_by workspace_id
    conflict_key (workspace_id, task_id)
  }

  record TaskView {
    workspace_id: uuid
    task_id: uuid
    title: string<1..200>
    status: TaskStatus
  }

  command CreateTask on TaskRoot {
    input {
      workspace_id: uuid
      task_id: uuid
      title: string<1..200>
      created_at_ms: i64
    }

    outcomes {
      Created(TaskView)
      AlreadyExists(TaskView)
    }

    read Task(input.workspace_id, input.task_id) as existing
    if is_some(existing) {
      return AlreadyExists({
        workspace_id: existing.workspace_id,
        task_id: existing.task_id,
        title: existing.title,
        status: existing.status
      })
    }

    create Task {
      workspace_id: input.workspace_id,
      task_id: input.task_id,
      title: input.title,
      status: TaskStatus::Open,
      created_at_ms: input.created_at_ms
    }

    return Created({
      workspace_id: input.workspace_id,
      task_id: input.task_id,
      title: input.title,
      status: TaskStatus::Open
    })
  }

  command CompleteTask on TaskRoot {
    input {
      workspace_id: uuid
      task_id: uuid
    }

    outcomes {
      Completed(TaskView)
      NotFound
    }

    read Task(input.workspace_id, input.task_id) as existing
    if !is_some(existing) {
      return NotFound
    }

    mutate Task(input.workspace_id, input.task_id) as task {
      overwrite task.status = TaskStatus::Completed
    }

    return Completed({
      workspace_id: existing.workspace_id,
      task_id: existing.task_id,
      title: existing.title,
      status: TaskStatus::Completed
    })
  }
}
```

Notice what the contract guarantees at compile time:

- **Partition & conflict safety**: Every command on `TaskRoot` routes by
  `workspace_id` and serializes conflicting writes on `(workspace_id, task_id)`.
- **Explicit overwrite semantics**: Updating an existing field uses `overwrite`
  so accidental clobbers are caught by the compiler.
- **Closed outcomes**: Callers receive a typed `Created`, `AlreadyExists`,
  `Completed`, or `NotFound` variant rather than untyped row counts.

## 3. Author a Bounded RiffQL Query (`riffdb/queries/list_tasks.riffq`)

Every read query in RiffDB is a named RiffQL query with a compiler-proven scan
and byte bound. Create `riffdb/queries/list_tasks.riffq`:

```text
query ListTasks($workspace_id: Task.workspace_id, $after: Cursor?)
for contract TaskTracker version 1
returns page {
  many tasks from Task
    where workspace_id == $workspace_id
    order by created_at_ms desc, task_id asc
    after $after
    take 50
    select {
      workspace_id
      task_id
      title
      status
      created_at_ms
    }
}
```

Remove the starter `riffdb/queries/item_page.riffq` file if you created the
project with `--starter`, and update `riffdb.application.json` so your
`TaskTrackerApplication` role grants `CreateTask`, `CompleteTask`, and
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
registers them in `riffdb.application.json`.)*

## 4. Generate Typed SDK Bindings Offline

Compile the contract and query module, write the deterministic lockfile
(`riffdb.application.lock.json`), and generate your typed client completely
offline:

```bash
riffdb generate
```

`riffdb generate` prints a role coverage summary confirming that every declared
command and query is granted to `TaskTrackerApplication`, and writes the
generated client to `generated/typescript/client.ts` (or your configured target
languages).

## 5. Run Locally and Call Your Generated Client

`riffdb dev` boots an isolated ephemeral `riffdbd` instance on a random loopback
port, bootstraps capabilities, deploys your contract and RiffQL module, mints a
role-bound credential in `.riffdb/dev/profile.credential`, and exports
`RIFFDB_ENDPOINT`, `RIFFDB_CREDENTIAL_FILE`, and `RIFFDB_DATABASE=default` to
your application process:

```bash
riffdb dev --exec "node app.mjs"
```

Generated role clients across all four languages provide:

- **One-line environment setup** (`connectFromEnv` / `connect_env`) with a
  default `AttemptBudget` of 3 retries.
- **Automatic UUIDv7 idempotency keys** when `idempotencyKey` is omitted on a
  command call.
- **Causal read-your-writes consistency** tracked automatically across commands
  and subsequent queries on the same client handle.
- **Ergonomic result helpers** (`.items()`, `.unwrapFound()`, `.orNone()`) and
  bounded pagination iterators (`listTasksItems` / `list_tasks_items`).

### TypeScript (`generated/typescript/client.ts`)

```typescript
import { TaskTrackerApplicationClient } from "./generated/typescript/client.js";

const client = await TaskTrackerApplicationClient.connectFromEnv(transport);
const workspaceId = "01900000-0000-7000-8000-000000000001";
const taskId = "01900000-0000-7000-8000-000000000002";

const created = await client.createTask({
  workspaceId,
  taskId,
  title: "Ship RiffDB 0.1.0 onboarding guide",
  createdAtMs: 1730000000000n,
});
console.log("Created outcome:", created.outcome);

await client.completeTask({ workspaceId, taskId });

for await (const row of client.listTasksItems({ workspaceId })) {
  console.log(`${row.title}: ${row.status}`);
}
```

### Python (`generated/python/client.py`)

```python
from generated.python.client import TaskTrackerApplicationClient

client = TaskTrackerApplicationClient.connect_env()
workspace_id = "01900000-0000-7000-8000-000000000001"
task_id = "01900000-0000-7000-8000-000000000002"

created = client.create_task(
    workspace_id=workspace_id,
    task_id=task_id,
    title="Ship RiffDB 0.1.0 onboarding guide",
    created_at_ms=1730000000000,
)
print("Created outcome:", created.outcome_tag)

client.complete_task(workspace_id=workspace_id, task_id=task_id)

for task in client.list_tasks_items(workspace_id=workspace_id):
    print(f"{task.title}: {task.status}")
```

### Go (`generated/go/client.go`)

```go
client, err := tasktracker.ConnectTaskTrackerApplicationFromEnv(ctx)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

workspaceID := "01900000-0000-7000-8000-000000000001"
taskID := "01900000-0000-7000-8000-000000000002"

if _, err := client.CreateTask(ctx, tasktracker.CreateTaskInput{
    WorkspaceId: workspaceID,
    TaskId:      taskID,
    Title:       "Ship RiffDB 0.1.0 onboarding guide",
    CreatedAtMs: 1730000000000,
}); err != nil {
    log.Fatal(err)
}

page, err := client.ListTasks(ctx, tasktracker.ListTasksParams{
    WorkspaceId: workspaceID,
})
if err != nil {
    log.Fatal(err)
}
for _, item := range page.Items() {
    fmt.Printf("%s: %s\n", item.Title, item.Status)
}
```

### Rust (`generated/rust/client.rs`)

```rust,ignore
let client = TaskTrackerApplicationClient::connect_env().await?;
let workspace_id = ApplicationUuid::parse("01900000-0000-7000-8000-000000000001")?;
let task_id = ApplicationUuid::parse("01900000-0000-7000-8000-000000000002")?;

client
    .create_task(CreateTaskInput {
        workspace_id,
        task_id,
        title: "Ship RiffDB 0.1.0 onboarding guide".to_owned(),
        created_at_ms: 1_730_000_000_000,
    })
    .await?;

client
    .complete_task(CompleteTaskInput {
        workspace_id,
        task_id,
    })
    .await?;

let page = client
    .list_tasks(ListTasksParams {
        workspace_id,
        after: None,
    })
    .await?;
for row in page.items() {
    println!("{}: {:?}", row.title, row.status);
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
