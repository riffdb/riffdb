# App-Baseline Benchmark Accuracy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `examples/app-baseline` benchmark measure what it claims to measure: warm-connection PostgreSQL latencies, no per-call Tokio runtime construction inside timed regions, and RiffDB interactive write scenarios that execute a genuinely new durable command per sample instead of idempotency replays.

**Architecture:** Three measurement bugs, two tasks. Task 1 threads one persistent Tokio runtime handle through the RiffDB backend (replacing per-call runtime construction) and caches one PostgreSQL connection in the Postgres backend (replacing connection-per-scenario-call). Task 2 makes the four write scenarios derive per-sample distinct identities (unique idempotency keys + unique created-entity IDs) so both backends do equivalent, real write work on every sample, while keeping the read-probe rows untouched so read scenarios measure stable data.

**Tech Stack:** Rust (toolchain pinned 1.97.0), tokio, tonic, `postgres` (blocking) crate. The benchmark lives in the standalone Cargo workspace `examples/app-baseline` (its own `Cargo.toml`/`Cargo.lock`/`target/`).

## Global Constraints

- All commands run from the repo worktree root; build/test the example with `cargo +1.97.0 <cmd> --manifest-path examples/app-baseline/Cargo.toml`. `test` and `clippy` additionally need `--workspace`, because that manifest is both a package and the workspace root — without it cargo only runs the root package's targets and skips the `core`, `postgres`, and `riffdb` members entirely.
- Workspace lints: `unsafe_code = forbid`, `missing_docs = warn`, clippy `-D warnings` in CI. Every new public item needs a doc comment.
- Do NOT change the `AppBackend` trait method signatures (`create_comment(&mut self, &CommentSeed)`, `close_ticket_with_comment(&mut self, &CloseTicketWithCommentSeed)`, `swap_member_roles(&mut self, &SwapMemberRolesSeed)`, `open_ticket_with_labels(&mut self, &OpenTicketWithLabelsSeed)`), the seed dataset content, the seed phases, or the report JSON schema keys (adding one string to the existing `limitations` array is allowed).
- Do NOT touch anything outside `examples/app-baseline/` and `docs/`.
- The TicketDesk contract command `CloseTicketWithComment` has no open-status `require`; re-closing a ticket succeeds, but its `create Comment(...)` fails with `CommentExists` on a duplicate `comment_id`. `SwapMemberRoles` only requires both members to exist. `OpenTicketWithLabels` fails on a duplicate `ticket_id` or duplicate `(ticket, label)` link.
- Contract input bound: `idempotency_key: string<128>` — all generated keys must stay under 128 bytes.
- Avoid panics on recoverable conditions (`AGENTS.md` Rust rules); `expect` is acceptable only for statically-impossible states.

---

### Task 1: Persistent runtime (RiffDB backend) and persistent connection (Postgres backend)

**Files:**
- Modify: `examples/app-baseline/riffdb/src/lib.rs` (struct `RiffDbPublicBackend`, fn `block_on_runtime` at lines 79-94, every `AppBackend` method)
- Modify: `examples/app-baseline/postgres/src/lib.rs` (struct `PostgresAppBackend` lines 109-130, every `AppBackend` method)
- Modify: `examples/app-baseline/src/main.rs` only if needed (the 4-worker runtime built at lines 76-80 must outlive every backend call — verify; it already does since `session.shutdown()` at line 104 is inside the same scope)

**Interfaces:**
- Consumes: existing `RiffDbPublicBackend::connect(endpoint, bearer_token)` (async, called from `RiffDbServerSession::start`, which `main.rs` awaits inside `runtime.block_on`).
- Produces: unchanged public API. `RiffDbPublicBackend` gains a private `runtime: tokio::runtime::Handle` field; `PostgresAppBackend` gains a private `client: Option<Client>` field and a private `fn client(&mut self) -> Result<&mut Client, PostgresError>`.

**Background (why):** `block_on_runtime` currently checks `Handle::try_current()`; every call site in the benchmark runs on the plain main thread, so the `Err` branch **builds and tears down a fresh 4-worker multi-thread Tokio runtime inside every timed scenario sample and every seed call**. The Postgres backend calls `self.connect()` — a full TCP + startup + auth handshake to a Docker-proxied port — at the top of **every** scenario method, inside the timed region. Both distort the report.

- [ ] **Step 1: RiffDB backend — store the runtime handle**

In `examples/app-baseline/riffdb/src/lib.rs`:

1. Add field to the struct (keep existing fields):

```rust
/// Public symbolic application backend.
pub struct RiffDbPublicBackend {
    transport: StableApplicationClient,
    metadata: CallMetadata,
    command_attempts: AttemptBudget,
    runtime: tokio::runtime::Handle,
}
```

2. In `connect` (async fn — always awaited inside the harness runtime), capture the ambient handle when constructing `Self`:

```rust
        Ok(Self {
            transport,
            metadata,
            command_attempts: AttemptBudget::new(3).expect("positive command attempt budget"),
            runtime: tokio::runtime::Handle::current(),
        })
```

3. Delete the free fn `block_on_runtime` (lines 79-94) entirely and add a private method:

```rust
    /// Drives a backend future on the persistent harness runtime.
    ///
    /// Every `AppBackend` method is called from the synchronous benchmark
    /// thread, never from async context, so `Handle::block_on` is safe here
    /// and no per-call runtime is ever constructed.
    fn block_on<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, RiffDbError>>,
    ) -> Result<T, RiffDbError> {
        self.runtime.clone().block_on(future)
    }
```

4. Replace every `block_on_runtime(async { ... })` call in the `AppBackend` impl (there are 12: `seed`, `point_get_ticket`, `point_get_user`, `list_tickets_by_project_status`, `list_open_tickets_for_assignee`, `list_comments_for_ticket`, `list_project_members`, `ticket_detail_page`, `create_comment`, `close_ticket_with_comment`, `swap_member_roles`, `open_ticket_with_labels`) with `self.block_on(async { ... })`. The async bodies themselves are unchanged. If the borrow checker rejects `self.block_on(async { self.ticketdesk()... })` in any method, use this shape instead (clone the handle first, then call `handle.block_on`):

```rust
        let runtime = self.runtime.clone();
        runtime.block_on(async { /* unchanged body */ })
```

5. The `RiffDbError::Runtime` variant loses its only constructor — keep the variant (public API, and `missing_docs` already satisfied) but if clippy flags it as dead, allow it via an explicit `#[allow(dead_code)]` is NOT possible on enum variants used publicly; the variant is `pub` so clippy will not flag it. Leave it.

- [ ] **Step 2: Compile-check the RiffDB backend**

Run: `cargo +1.97.0 build --manifest-path examples/app-baseline/Cargo.toml -p riffdb-app-baseline-riffdb`
Expected: success, no warnings about `block_on_runtime`.

- [ ] **Step 3: Postgres backend — cache one connection**

In `examples/app-baseline/postgres/src/lib.rs`:

1. Change the struct and add the lazy accessor (replace the existing `fn connect`):

```rust
/// PostgreSQL comparison adapter.
pub struct PostgresAppBackend {
    config: Config,
    client: Option<Client>,
}

impl PostgresAppBackend {
    /// Creates an adapter from a database URL.
    pub fn new(database_url: impl Into<String>) -> Result<Self, PostgresError> {
        let database_url = database_url.into();
        if database_url.is_empty() || database_url.len() > 4_096 {
            return Err(PostgresError::InvalidConfiguration);
        }
        let mut config = database_url
            .parse::<Config>()
            .map_err(|_| PostgresError::InvalidConfiguration)?;
        config.connect_timeout(Duration::from_secs(5));
        Ok(Self {
            config,
            client: None,
        })
    }

    /// Returns the persistent connection, opening it on first use.
    ///
    /// One warm connection for the whole benchmark run mirrors how the
    /// RiffDB side reuses one HTTP/2 channel; connection setup must not be
    /// paid inside timed scenario samples.
    fn client(&mut self) -> Result<&mut Client, PostgresError> {
        if self.client.is_none() {
            let client = self.config.connect(NoTls).map_err(db_err)?;
            self.client = Some(client);
        }
        self.client.as_mut().ok_or(PostgresError::InvalidConfiguration)
    }
}
```

2. In every `AppBackend` method (all 13: `reset`, `seed`, `point_get_ticket`, `point_get_user`, `list_tickets_by_project_status`, `list_open_tickets_for_assignee`, `list_comments_for_ticket`, `list_project_members`, `ticket_detail_page`, `create_comment`, `close_ticket_with_comment`, `swap_member_roles`, `open_ticket_with_labels`) replace

```rust
        let mut client = self.connect()?;
```

with

```rust
        let client = self.client()?;
```

Methods that call `client.transaction()` need `let client = self.client()?;` followed by `let mut tx = client.transaction().map_err(db_err)?;` — the transaction borrows the cached client mutably and auto-rolls-back on drop, so the early `return Err(...)` paths in `close_ticket_with_comment` remain correct.

- [ ] **Step 4: Build + clippy + existing tests for the whole example workspace**

Run:
```bash
cargo +1.97.0 build --manifest-path examples/app-baseline/Cargo.toml
cargo +1.97.0 clippy --workspace --all-targets --manifest-path examples/app-baseline/Cargo.toml -- -D warnings
cargo +1.97.0 test --workspace --manifest-path examples/app-baseline/Cargo.toml
```
`--workspace` is required: `examples/app-baseline/Cargo.toml` is both a package and the workspace root, so without it cargo runs only the root package's targets and silently skips the `core`, `postgres`, and `riffdb` member tests.

Expected: all pass (the workspace has unit tests in `src/main.rs` for the parity gate; they are unaffected).

- [ ] **Step 5: Commit**

```bash
git add examples/app-baseline/riffdb/src/lib.rs examples/app-baseline/postgres/src/lib.rs examples/app-baseline/src/main.rs
git commit -m "fix(app-baseline): persistent runtime and warm Postgres connection"
```

---

### Task 2: Real per-sample writes in both backends

**Files:**
- Modify: `examples/app-baseline/core/src/seed.rs` (struct `ScenarioProbes` lines 340-363, `SeedDataset::probes` lines 233-337; add `mod tests`)
- Modify: `examples/app-baseline/core/src/scenarios.rs` (sample loop lines 102-208, stale comment line 173)
- Modify: `examples/app-baseline/postgres/src/lib.rs` (remove `ON CONFLICT ... DO NOTHING` from `create_comment`, `close_ticket_with_comment`, `open_ticket_with_labels`)
- Modify: `examples/app-baseline/core/src/report.rs` (append one string to the `limitations` array at line 71)

**Interfaces:**
- Consumes: `CommentSeed`, `CloseTicketWithCommentSeed`, `SwapMemberRolesSeed`, `OpenTicketWithLabelsSeed` (unchanged shapes, defined in `seed.rs`); `uuid_from_ordinal(namespace: u8, ordinal: u64) -> [u8; 16]` from `crate::ids`.
- Produces: `ScenarioProbes` loses its four fixed seed fields (`write_comment`, `close_ticket_with_comment`, `swap_member_roles`, `open_ticket_with_labels`) and gains write-target fields plus four **per-sample generator methods**:
  - `pub fn write_comment(&self, sample: usize) -> CommentSeed`
  - `pub fn close_ticket_with_comment(&self, sample: usize) -> CloseTicketWithCommentSeed`
  - `pub fn swap_member_roles(&self, sample: usize) -> SwapMemberRolesSeed`
  - `pub fn open_ticket_with_labels(&self, sample: usize) -> OpenTicketWithLabelsSeed`
  `run_scenarios` in `scenarios.rs` is their only caller besides tests.

**Background (why):** All four write scenarios currently reuse one fixed idempotency key and fixed created-entity IDs across all 9 measured samples. On RiffDB, samples 2-9 therefore measure the **idempotency replay path** (a stored-outcome read — no new commit); on PostgreSQL the `ON CONFLICT DO NOTHING` inserts degrade to near-no-ops while the `UPDATE`s still write. The two columns of the report measure different operations. After this task every sample executes one genuinely new durable write on both backends, and the write targets are kept disjoint from the read-probe rows so the seven read scenarios keep measuring identical data across all samples.

**Placement rules (why each target is chosen):**
- Scenario comments and closes go to a **write ticket** (`write_ticket_id`) that is a different open ticket than the read-probe ticket, so `list_comments_for_ticket` / `ticket_detail_page` row counts stay fixed.
- Opened tickets go to a **write project** (`write_project_id ≠ project_id`) with a **write assignee** (`write_assignee_id ≠ assignee_id`), so `list_tickets_by_project_status` and `list_open_tickets_for_assignee` row counts stay fixed.
- Role swaps stay on the read-probe project's first two members but alternate direction by sample parity, so each sample writes a real value change while `list_project_members` row count stays fixed.

- [ ] **Step 1: Write the failing tests**

Append to `examples/app-baseline/core/src/seed.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::SeedDataset;
    use crate::Scale;

    #[test]
    fn write_probes_are_disjoint_from_read_probes() {
        for scale in [Scale::smoke(), Scale::full()] {
            let dataset = SeedDataset::generate(scale);
            let probes = dataset.probes();
            assert_ne!(probes.write_ticket_id, probes.ticket_id);
            assert_ne!(probes.write_project_id, probes.project_id);
            assert_ne!(probes.write_assignee_id, probes.assignee_id);
            assert_ne!(probes.write_label_a, probes.write_label_b);
        }
    }

    #[test]
    fn write_probes_are_unique_per_sample_and_deterministic() {
        let dataset = SeedDataset::generate(Scale::smoke());
        let probes = dataset.probes();
        let mut keys = BTreeSet::new();
        let mut created_ids = BTreeSet::new();
        for sample in 0..100 {
            let comment = probes.write_comment(sample);
            let close = probes.close_ticket_with_comment(sample);
            let swap = probes.swap_member_roles(sample);
            let open = probes.open_ticket_with_labels(sample);

            assert!(keys.insert(comment.idempotency_key.clone()));
            assert!(keys.insert(close.idempotency_key.clone()));
            assert!(keys.insert(swap.idempotency_key.clone()));
            assert!(keys.insert(open.idempotency_key.clone()));
            assert!(comment.idempotency_key.len() < 128);
            assert!(close.idempotency_key.len() < 128);
            assert!(swap.idempotency_key.len() < 128);
            assert!(open.idempotency_key.len() < 128);

            assert!(created_ids.insert(comment.row.comment_id));
            assert!(created_ids.insert(close.comment_id));
            assert!(created_ids.insert(open.ticket_id));

            assert_eq!(comment.row.ticket_id, probes.write_ticket_id);
            assert_eq!(close.ticket_id, probes.write_ticket_id);
            assert_eq!(open.project_id, probes.write_project_id);
            assert_eq!(open.assignee_id, probes.write_assignee_id);
            assert_eq!(swap.project_id, probes.project_id);

            assert_eq!(comment, probes.write_comment(sample));
            assert_eq!(close, probes.close_ticket_with_comment(sample));
            assert_eq!(swap, probes.swap_member_roles(sample));
            assert_eq!(open, probes.open_ticket_with_labels(sample));
        }
        // Adjacent samples swap in opposite directions (a real value change
        // per sample on the same two membership rows).
        let even = probes.swap_member_roles(0);
        let odd = probes.swap_member_roles(1);
        assert_eq!(even.role_a, odd.role_b);
        assert_eq!(even.role_b, odd.role_a);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo +1.97.0 test --workspace --manifest-path examples/app-baseline/Cargo.toml`
Expected: FAIL to compile — `write_ticket_id`, `write_project_id`, etc. do not exist yet.

- [ ] **Step 3: Restructure `ScenarioProbes`**

In `examples/app-baseline/core/src/seed.rs`, replace the `ScenarioProbes` struct (lines 340-363) with:

```rust
/// Fixed keys exercised by timed scenarios.
///
/// Read probes (`ticket_id`, `project_id`, `assignee_id`, ...) are never
/// mutated by write scenarios, so every measured sample of a read scenario
/// sees identical data. Write scenarios derive a distinct idempotency key
/// and distinct created-entity IDs per sample so both backends execute one
/// genuinely new durable write per sample (no idempotent replays and no
/// conflict-suppressed inserts).
#[derive(Clone, Debug)]
pub struct ScenarioProbes {
    /// Organization under test.
    pub organization_id: [u8; 16],
    /// Project under test (read probes only).
    pub project_id: [u8; 16],
    /// Ticket under test (read probes only).
    pub ticket_id: [u8; 16],
    /// User under test.
    pub user_id: [u8; 16],
    /// Assignee under test (read probes only).
    pub assignee_id: [u8; 16],
    /// Open status constant.
    pub open_status: TicketStatus,
    /// Ticket receiving write-scenario comments and closes.
    pub write_ticket_id: [u8; 16],
    /// Author of write-scenario comments.
    pub write_author_id: [u8; 16],
    /// Project receiving write-scenario opened tickets.
    pub write_project_id: [u8; 16],
    /// Assignee of write-scenario opened tickets.
    pub write_assignee_id: [u8; 16],
    /// First member of the role-swap pair.
    pub swap_user_a: [u8; 16],
    /// Second member of the role-swap pair.
    pub swap_user_b: [u8; 16],
    /// First label attached by the open-ticket scenario.
    pub write_label_a: [u8; 16],
    /// Second label attached by the open-ticket scenario.
    pub write_label_b: [u8; 16],
}

impl ScenarioProbes {
    /// Distinct comment insert for measured sample `sample`.
    #[must_use]
    pub fn write_comment(&self, sample: usize) -> CommentSeed {
        CommentSeed {
            row: CommentRow {
                organization_id: self.organization_id,
                comment_id: uuid_from_ordinal(NS_WRITE_PROBE, 99_100_000 + sample as u64),
                ticket_id: self.write_ticket_id,
                author_id: self.write_author_id,
                body: format!("baseline write-path comment {sample}"),
            },
            idempotency_key: format!("app-baseline-write-comment-v2-{sample}"),
        }
    }

    /// Distinct close-with-comment write for measured sample `sample`.
    ///
    /// `CloseTicketWithComment` has no open-status requirement, so re-closing
    /// the same write ticket stays a real two-entity mutation on every
    /// sample; only the created comment identity must be fresh.
    #[must_use]
    pub fn close_ticket_with_comment(&self, sample: usize) -> CloseTicketWithCommentSeed {
        CloseTicketWithCommentSeed {
            organization_id: self.organization_id,
            ticket_id: self.write_ticket_id,
            author_id: self.write_author_id,
            comment_id: uuid_from_ordinal(NS_WRITE_PROBE, 99_200_000 + sample as u64),
            body: format!("baseline close-with-comment note {sample}"),
            idempotency_key: format!("app-baseline-close-ticket-with-comment-v2-{sample}"),
        }
    }

    /// Distinct role swap for measured sample `sample`.
    ///
    /// Alternating direction by parity makes every sample a genuine value
    /// change on the same two membership rows.
    #[must_use]
    pub fn swap_member_roles(&self, sample: usize) -> SwapMemberRolesSeed {
        let (role_a, role_b) = if sample % 2 == 0 {
            ("lead", "contributor")
        } else {
            ("contributor", "lead")
        };
        SwapMemberRolesSeed {
            organization_id: self.organization_id,
            project_id: self.project_id,
            user_a: self.swap_user_a,
            user_b: self.swap_user_b,
            role_a: role_a.to_owned(),
            role_b: role_b.to_owned(),
            idempotency_key: format!("app-baseline-swap-member-roles-v2-{sample}"),
        }
    }

    /// Distinct open-ticket-with-labels write for measured sample `sample`.
    #[must_use]
    pub fn open_ticket_with_labels(&self, sample: usize) -> OpenTicketWithLabelsSeed {
        OpenTicketWithLabelsSeed {
            organization_id: self.organization_id,
            ticket_id: uuid_from_ordinal(NS_WRITE_PROBE, 99_300_000 + sample as u64),
            project_id: self.write_project_id,
            reporter_id: self.write_author_id,
            assignee_id: self.write_assignee_id,
            title: format!("baseline multi-command open ticket {sample}"),
            label_a: self.write_label_a,
            label_b: self.write_label_b,
            idempotency_key: format!("app-baseline-open-ticket-with-labels-v2-{sample}"),
        }
    }
}
```

Add the namespace constant next to the existing `NS_*` constants at the top of the file:

```rust
const NS_WRITE_PROBE: u8 = 0x7f;
```

- [ ] **Step 4: Rebuild `SeedDataset::probes`**

Replace the body of `probes()` (keep the existing `ticket`/`user`/`close_ticket`/`project_members`/`member_a`/`member_b`/`labels`/`label_a`/`label_b` derivations, they are reused) so it computes the new write-target fields and returns the new struct:

```rust
        let write_project_id = self
            .projects
            .iter()
            .find(|project| {
                project.organization_id == organization_id && project.project_id != project_id
            })
            .map(|project| project.project_id)
            .unwrap_or(project_id);
        let write_assignee_id = self
            .users
            .iter()
            .find(|candidate| {
                candidate.organization_id == organization_id && candidate.user_id != assignee_id
            })
            .map(|candidate| candidate.user_id)
            .unwrap_or(assignee_id);
        ScenarioProbes {
            organization_id,
            project_id,
            ticket_id: ticket.ticket_id,
            user_id: user.user_id,
            assignee_id,
            open_status: TicketStatus::Open,
            write_ticket_id: close_ticket.ticket_id,
            write_author_id: user.user_id,
            write_project_id,
            write_assignee_id,
            swap_user_a: member_a.user_id,
            swap_user_b: member_b.user_id,
            write_label_a: label_a,
            write_label_b: label_b,
        }
```

Delete the now-unused `write_comment` / `close_ticket_with_comment` / `swap_member_roles` / `open_ticket_with_labels` literal constructions and the `uuid_from_ordinal(0x7f, 99_000_00X)` lines inside `probes()`. Update the two stale doc comments on `CloseTicketWithCommentSeed::idempotency_key` and `SwapMemberRolesSeed::idempotency_key` from "Stable idempotency key (replay-safe across samples)." / "Stable idempotency key." to "Per-sample idempotency key (each measured sample is a new durable write).".

- [ ] **Step 5: Update the scenario loop**

In `examples/app-baseline/core/src/scenarios.rs`, change the measured loop (line 102) from `for _ in 0..samples` to `for sample in 0..samples`, and update the four write arms:

```rust
                ScenarioId::CreateComment => {
                    // Each sample inserts a distinct comment (new idempotency
                    // key + comment id) so RiffDB never takes the replay path
                    // and PostgreSQL never no-ops on conflict.
                    let input = probes.write_comment(sample);
                    let (value, elapsed) = time_call(|| backend.create_comment(&input));
                    value.map(|()| (1, elapsed))
                }
                ScenarioId::CloseTicketWithComment => {
                    let input = probes.close_ticket_with_comment(sample);
                    let (value, elapsed) =
                        time_call(|| backend.close_ticket_with_comment(&input));
                    // Two entity mutations: ticket + comment.
                    value.map(|()| (2, elapsed))
                }
                ScenarioId::SwapMemberRoles => {
                    let input = probes.swap_member_roles(sample);
                    let (value, elapsed) = time_call(|| backend.swap_member_roles(&input));
                    value.map(|()| (2, elapsed))
                }
                ScenarioId::OpenTicketWithLabels => {
                    let input = probes.open_ticket_with_labels(sample);
                    let (value, elapsed) =
                        time_call(|| backend.open_ticket_with_labels(&input));
                    // Ticket + two label links.
                    value.map(|()| (3, elapsed))
                }
```

Note: `backend` is borrowed mutably by `time_call`'s closure, so the per-sample `input` must be constructed **before** `time_call` (as shown) — the construction cost stays outside the timed region on both backends.

- [ ] **Step 6: Remove conflict-suppression from the Postgres write scenarios**

In `examples/app-baseline/postgres/src/lib.rs`, delete the `ON CONFLICT (...) DO NOTHING` clause from all four INSERT statements (in `create_comment`, `close_ticket_with_comment`, and both the ticket and the two `ticket_label` inserts in `open_ticket_with_labels`). IDs are now unique per sample, so a conflict is a harness bug that must surface as an error, and PostgreSQL does the same real insert work as RiffDB. Also update the comment `// Idempotent insert for repeated samples of the write scenario.` to `// Each sample inserts a distinct comment; a conflict is a harness bug.`.

- [ ] **Step 7: Record the methodology in the report limitations**

In `examples/app-baseline/core/src/report.rs`, append two strings to the `limitations` array:

```rust
            "Write scenarios execute one new durable write per measured sample on both backends (no idempotent replays).",
            "PostgreSQL runs behind a Docker userland port proxy; RiffDB listens directly on loopback.",
```

- [ ] **Step 8: Run tests to verify they pass**

Run:
```bash
cargo +1.97.0 test --workspace --manifest-path examples/app-baseline/Cargo.toml
cargo +1.97.0 clippy --workspace --all-targets --manifest-path examples/app-baseline/Cargo.toml -- -D warnings
cargo +1.97.0 build --release --manifest-path examples/app-baseline/Cargo.toml
```
`--workspace` is required here too; without it the new `core` tests never run.

Expected: all pass, both new tests green.

- [ ] **Step 9: Commit**

```bash
git add examples/app-baseline/core/src/seed.rs examples/app-baseline/core/src/scenarios.rs examples/app-baseline/core/src/report.rs examples/app-baseline/postgres/src/lib.rs
git commit -m "fix(app-baseline): real per-sample writes on both backends"
```

---

## Verification (controller, after both tasks)

From the worktree root, run the smoke profile end-to-end (requires Docker):

```bash
./benchmarks/run-app-baseline --smoke
```

Expected: completes, report written, all four write scenarios succeed on both backends across all samples (no `CommentExists`, no PG unique violations). Then run the full profile for the corrected numbers:

```bash
./benchmarks/run-app-baseline --full
```

Compare `target/app-baseline/report-v1.json` against the previous run: PostgreSQL scenario p50s should drop (warm connection), RiffDB write p50s should rise (real commits instead of replays), seed ratio should be roughly unchanged.
