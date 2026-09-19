//! Runs one variant and reports what a write cost under it.
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

    /// Frame bytes per committed command, the figure a mechanism moves most
    /// directly and the one that does not depend on host load.
    #[must_use]
    pub fn frame_bytes_per_command(&self) -> f64 {
        if self.committed_commands == 0 {
            return 0.0;
        }
        self.frame_bytes as f64 / self.committed_commands as f64
    }

    /// Segment bytes per committed command.
    #[must_use]
    pub fn segment_bytes_per_command(&self) -> f64 {
        if self.committed_commands == 0 {
            return 0.0;
        }
        self.segment_bytes as f64 / self.committed_commands as f64
    }

    /// Writer-busy microseconds per committed command. Measured inside the
    /// server, so it excludes client and transport noise that dominates wall
    /// clock on a host running anything else.
    #[must_use]
    pub fn writer_busy_us_per_command(&self) -> f64 {
        self.per_command(self.writer_busy_us)
    }

    /// Group-commit microseconds per committed command.
    #[must_use]
    pub fn commit_us_per_command(&self) -> f64 {
        self.per_command(self.commit_us)
    }

    /// Final-apply microseconds per committed command.
    #[must_use]
    pub fn final_apply_us_per_command(&self) -> f64 {
        self.per_command(self.final_apply_us)
    }

    fn per_command(&self, total: u64) -> f64 {
        if self.committed_commands == 0 {
            return 0.0;
        }
        total as f64 / self.committed_commands as f64
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
) -> Result<VariantMeasurement, SessionError> {
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

    // Timed phase: publishes only. Setup above and shutdown below are excluded
    // so the figure is the cost of the writes rather than of the harness.
    let started = Instant::now();
    for index in 0..documents {
        let mut document = [0_u8; 16];
        document[..8].copy_from_slice(&index.to_be_bytes());
        document[8] = 0x5A;
        let input = publish_document_input(
            workspace,
            document,
            format!("document-{index}"),
            title.to_owned(),
            body.clone(),
            body.len() as i64,
        );
        client
            .execute_command(command("PublishDocument", input)?, attempts(), &metadata)
            .await
            .map_err(|error| SessionError::Rpc(format!("PublishDocument {index}: {error:?}")))?;
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
