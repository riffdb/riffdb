//! Runs one variant and reports what a write cost under it.
//!
//! Read the byte figures at one client and the CPU figures under load. Group
//! size is not constant across variants -- one run grouped 7.8 documents per
//! commit on the baseline and 4.8 with a tokenized index -- and per-group
//! framing overhead spreads over however many documents a group happens to
//! carry, so a byte-per-document delta measured under concurrency mixes the
//! mechanism with the grouping. At one client every command is its own commit,
//! which removes the confound. Writer-busy microseconds per document are work
//! per document rather than per group, so they survive concurrency.
//!
//! Throughput alone would attribute poorly: the differences between these
//! variants are small relative to host noise, and a percent of throughput is
//! not evidence on a host whose run-to-run spread is several percent. The
//! daemon's own shutdown census reports bytes and stage times per command, and
//! those are deterministic for a fixed workload, so the census carries the
//! attribution and the wall clock is reported beside it as context.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use riffdb_client_rust::{ApplicationUuid, ApplicationValue};

use crate::daemon::Daemon;
use crate::session::{
    SessionError, application_client, attempts, bearer, bootstrap_and_deploy, command,
    issue_command_capability, publish_document_input,
};

/// What one variant cost.
#[derive(Clone, Debug)]
pub struct VariantMeasurement {
    /// Variant name, matching [`crate::variants`].
    pub name: String,
    /// Documents published.
    pub documents: u64,
    /// Concurrent publishers. Group commit barely engages at one, so a single
    /// client measures per-command cost in isolation rather than under load.
    pub concurrency: usize,
    /// Wall time for the publish phase only, excluding setup and seeding.
    pub elapsed: Duration,
    /// Commands the writer committed, from the shutdown census.
    pub committed_commands: u64,
    /// Total command frame bytes the writer wrote.
    pub frame_bytes: u64,
    /// Total command segment bytes the writer staged.
    pub segment_bytes: u64,
    /// Microseconds the writer thread was busy, from the writer evidence.
    pub writer_busy_us: u64,
    /// Total group-commit microseconds.
    pub commit_us: u64,
    /// Total final-apply microseconds. Projection maintenance lands here
    /// rather than in the command frame, which is why bytes alone do not
    /// attribute it.
    pub final_apply_us: u64,
}

impl VariantMeasurement {
    /// Published documents per second over the publish phase.
    #[must_use]
    pub fn documents_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        self.documents as f64 / seconds
    }

    /// Documents per committed group.
    ///
    /// Reported because it is the reason every other figure is normalised per
    /// document rather than per commit: group size is not constant across
    /// variants, so a per-commit figure compares different amounts of work.
    #[must_use]
    pub fn documents_per_commit(&self) -> f64 {
        if self.committed_commands == 0 {
            return 0.0;
        }
        self.documents as f64 / self.committed_commands as f64
    }

    /// Frame bytes per published document.
    #[must_use]
    pub fn frame_bytes_per_document(&self) -> f64 {
        self.per_document(self.frame_bytes)
    }

    /// Segment bytes per published document.
    #[must_use]
    pub fn segment_bytes_per_document(&self) -> f64 {
        self.per_document(self.segment_bytes)
    }

    /// Writer-busy microseconds per published document. Measured inside the
    /// server, so it excludes client and transport noise that dominates wall
    /// clock on a host running anything else.
    #[must_use]
    pub fn writer_busy_us_per_document(&self) -> f64 {
        self.per_document(self.writer_busy_us)
    }

    /// Group-commit microseconds per published document.
    #[must_use]
    pub fn commit_us_per_document(&self) -> f64 {
        self.per_document(self.commit_us)
    }

    /// Final-apply microseconds per published document.
    #[must_use]
    pub fn final_apply_us_per_document(&self) -> f64 {
        self.per_document(self.final_apply_us)
    }

    /// Normalises by documents published, never by committed groups.
    ///
    /// Group size varies between variants -- one run produced 101 commits for
    /// 400 documents on the baseline and 60 for the same 400 with a projection
    /// -- so dividing by commits reports a mechanism that packs more documents
    /// per group as though it cost more per unit of work. Documents published
    /// is fixed by the caller and is the same for every variant.
    fn per_document(&self, total: u64) -> f64 {
        if self.documents == 0 {
            return 0.0;
        }
        total as f64 / self.documents as f64
    }
}

/// Measures one variant: deploy, seed a workspace, publish `documents`, and
/// read the census the daemon emits as it closes.
///
/// The same document bodies are used for every variant so a difference between
/// two runs is the mechanism and not the payload.
pub async fn measure_variant(
    binary: &Path,
    run_dir: &Path,
    name: &str,
    source: &str,
    documents: u64,
    concurrency: usize,
) -> Result<VariantMeasurement, SessionError> {
    let concurrency = concurrency.max(1);
    let daemon = Daemon::start(binary, run_dir)
        .map_err(|error| SessionError::Rpc(format!("daemon start: {error}")))?;
    let endpoint = daemon.endpoint();

    let bootstrap_token =
        bootstrap_and_deploy(&endpoint, &run_dir.join("bootstrap.credential"), source).await?;
    let runner = issue_command_capability(&endpoint, &bootstrap_token, source).await?;
    let metadata = bearer(&runner)?;
    let mut client = application_client(&endpoint).await?;

    let workspace = [0x11_u8; 16];
    let mut create = BTreeMap::new();
    create.insert(
        "idempotency_key".to_owned(),
        ApplicationValue::String("perf-surface-workspace".to_owned()),
    );
    create.insert(
        "workspace_id".to_owned(),
        ApplicationValue::Uuid(ApplicationUuid::from_bytes(workspace)),
    );
    create.insert(
        "name".to_owned(),
        ApplicationValue::String("perf surface".to_owned()),
    );
    client
        .execute_command(command("CreateWorkspace", create)?, attempts(), &metadata)
        .await
        .map_err(|error| SessionError::Rpc(format!("CreateWorkspace: {error:?}")))?;

    let title = "perf surface document title";
    let body = "lorem ipsum ".repeat(64);

    // Each publisher owns its own connection: sharing one would serialise the
    // clients on a single channel and measure the harness instead of the
    // server. The workspace is created once above and shared, so every
    // publisher contends on the same conflict key, which is what makes group
    // commit engage.
    let mut publishers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        publishers.push(application_client(&endpoint).await?);
    }

    // Timed phase: publishes only. Setup above and shutdown below are excluded
    // so the figure is the cost of the writes rather than of the harness.
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for (slot, mut publisher) in publishers.into_iter().enumerate() {
        let metadata = metadata.clone();
        let title = title.to_owned();
        let body = body.clone();
        let stride = u64::try_from(concurrency).unwrap_or(1);
        let start = u64::try_from(slot).unwrap_or(0);
        tasks.push(tokio::spawn(async move {
            let mut index = start;
            while index < documents {
                let mut document = [0_u8; 16];
                document[..8].copy_from_slice(&index.to_be_bytes());
                document[8] = 0x5A;
                let input = publish_document_input(
                    workspace,
                    document,
                    format!("document-{index}"),
                    title.clone(),
                    body.clone(),
                    body.len() as i64,
                );
                publisher
                    .execute_command(command("PublishDocument", input)?, attempts(), &metadata)
                    .await
                    .map_err(|error| {
                        SessionError::Rpc(format!("PublishDocument {index}: {error:?}"))
                    })?;
                index += stride;
            }
            Ok::<(), SessionError>(())
        }));
    }
    for task in tasks {
        task.await
            .map_err(|error| SessionError::Rpc(format!("publisher panicked: {error}")))??;
    }
    let elapsed = started.elapsed();

    let stdout = daemon
        .shutdown()
        .map_err(|error| SessionError::Rpc(format!("shutdown: {error}")))?;
    let census = parse_frame_census(&stdout).ok_or_else(|| {
        SessionError::Rpc("shutdown emitted no writer frame census".to_owned())
    })?;

    let evidence = parse_writer_evidence(&stdout).ok_or_else(|| {
        SessionError::Rpc("shutdown emitted no writer evidence".to_owned())
    })?;

    Ok(VariantMeasurement {
        name: name.to_owned(),
        documents,
        concurrency,
        elapsed,
        committed_commands: census.0,
        frame_bytes: census.1,
        segment_bytes: census.2,
        writer_busy_us: evidence.0,
        commit_us: evidence.1,
        final_apply_us: evidence.2,
    })
}

/// Reads (commands, frame bytes, segment bytes) from the writer frame census.
///
/// The census is a comma-separated counter vector; the harness reads only the
/// leading fields it understands and refuses a shorter line rather than
/// guessing, so a census format change fails the run instead of silently
/// producing zeros.
fn parse_frame_census(stdout: &[String]) -> Option<(u64, u64, u64)> {
    let line = stdout
        .iter()
        .find(|line| line.starts_with("riffdb-writer-frame-census-v1"))?;
    let values: Vec<u64> = line
        .split('\t')
        .nth(1)?
        .split(',')
        .map(|value| value.trim().parse().unwrap_or_default())
        .collect();
    if values.len() < 6 {
        return None;
    }
    Some((values[0], values[2], values[4]))
}

/// Reads (writer busy us, commit us, final apply us) from the writer evidence.
///
/// The line is `name\tkey=value;...\tname:count:sum:buckets;...`. Only the
/// sums are read; a missing field fails the run rather than reporting zero,
/// because a zero here would look like a mechanism that costs nothing.
fn parse_writer_evidence(stdout: &[String]) -> Option<(u64, u64, u64)> {
    let line = stdout
        .iter()
        .find(|line| line.starts_with("riffdb-writer-evidence-v1"))?;
    let mut fields = line.split('\t').skip(1);
    let counters = fields.next()?;
    let histograms = fields.next()?;

    let busy = counters
        .split(';')
        .find_map(|entry| entry.strip_prefix("busy_us="))
        .and_then(|value| value.parse().ok())?;

    let sum_of = |name: &str| -> Option<u64> {
        histograms
            .split(';')
            .find(|entry| entry.starts_with(&format!("{name}:")))
            .and_then(|entry| entry.split(':').nth(2))
            .and_then(|value| value.parse().ok())
    };

    Some((busy, sum_of("commit_us")?, sum_of("final_apply_us")?))
}
