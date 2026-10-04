//! ADR-0182 section 6 in a real `riffdbd` (WP-761): a configured online
//! retention lane compacts history while the daemon serves, a process kill
//! with the lane running reopens valid, and the restarted lane resumes.
//!
//! Exact crash placement across one step's delete, tombstone append,
//! watermark advance, and checkpoint deletion is proven by the storage
//! crash-child matrix and the seeded simulation; this arm proves the same
//! guarantee holds for the integrated process under a genuine kill.
// req: STO-030, STO-031, STO-032, REC-001, REC-002

use super::*;
use riffdb_client_rust::PublicErrorKind;

/// Sequences the lane keeps below the head.
const RETAINED_SEQUENCES: u64 = 4;
/// Bounded public command population large enough to checkpoint an extent.
const MAX_COMMANDS_BEFORE_KILL: u64 = 32_768;
/// Commands committed after the restart.
const COMMANDS_AFTER_RESTART: u64 = 8;
const RETENTION_DOCUMENT: &str =
    "[maintenance.retention]\nretained_sequences = 4\nsequences_per_epoch = 2\n";

fn create_at(index: u64) -> TestResult<CreateBudget> {
    Ok(CreateBudget {
        idempotency_key: format!("wp761-online-retention-{index}"),
        organization_id: ORGANIZATION_ID,
        fiscal_year: 3_000 + i64::try_from(index)?,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    })
}

async fn commit_create(
    client: &mut RiffDbClient,
    index: u64,
    authenticated: &CallMetadata,
) -> TestResult<()> {
    let created = bounded_rpc(
        "online retention CreateBudget",
        client.execute_generated(&create_at(index)?, one_attempt(), authenticated),
    )
    .await?;
    if created.response().status != v1::execute_command_response::CompletionStatus::Committed as i32
        || created.response().commit_sequence != index + 1
    {
        return Err(test_failure(format!(
            "CreateBudget {index} did not commit at sequence {}",
            index + 1
        )));
    }
    Ok(())
}

/// Waits until a public scan from the first commit is typed pruned: the lane
/// compacted history while the daemon kept serving.
async fn await_pruned_scan(client: &mut RiffDbClient, metadata: &CallMetadata) -> TestResult<()> {
    timeout(PROCESS_START_TIMEOUT, async {
        loop {
            match client.scan_commits(commit_scan_request()?, metadata).await {
                Err(error)
                    if error
                        .public_error()
                        .is_some_and(|error| error.kind() == PublicErrorKind::HistoryPruned) =>
                {
                    return Ok(());
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(test_failure(format!(
                        "commit scan failed while the lane compacted: {error}"
                    )));
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| test_failure("the online retention lane never compacted history"))?
}

pub(super) async fn run_online_retention_kill_case() -> TestResult<()> {
    let fixture = ProcessFixture::new("online-retention")?;
    let retained_bootstrap = fixture.retained_bootstrap()?;
    let authenticated = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);

    let mut process = fixture.spawn_riffdbd_with_config(RETENTION_DOCUMENT)?;
    let address =
        parse_ready_address(&process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?)?;
    let mut client = connect(address).await?;
    bootstrap_and_deploy(&mut client, &retained_bootstrap, &authenticated).await?;
    let sealed_prefix = format!(
        "{}.riffjournal.sealed.",
        fixture
            .database_path()
            .file_name()
            .ok_or_else(|| test_failure("database name"))?
            .to_string_lossy()
    );
    let directory = fixture
        .database_path()
        .parent()
        .ok_or_else(|| test_failure("database parent"))?;
    let mut commands_before_kill = 0;
    for index in 0..MAX_COMMANDS_BEFORE_KILL {
        commit_create(&mut client, index, &authenticated).await?;
        commands_before_kill = index + 1;
        // ADR-0263 permits pruning only sealed extents. Observe the actual
        // background checkpoint publication, rather than expect a tiny
        // workload below its threshold to become eligible after a timeout.
        if commands_before_kill % 64 == 0
            && fs::read_dir(directory)?.take(64).any(|entry| {
                entry.is_ok_and(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&sealed_prefix)
                })
            })
        {
            break;
        }
    }
    if let Err(error) = await_pruned_scan(&mut client, &authenticated).await {
        drop(client);
        let _ = process.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT);
        let status = riffdb_storage_redb::RedbOfflineRetention::bind(fixture.database_path())
            .status()
            .map(|status| (status.max_permissible_watermark, status.fence_binding));
        return Err(test_failure(format!(
            "{error}; retention bound after stop: {status:?}"
        )));
    }
    drop(client);
    let killed = process.kill(PROCESS_KILL_TIMEOUT)?;
    if killed.status.success() {
        return Err(test_failure("the killed riffdbd exited successfully"));
    }

    // The killed database reopens through the dirty path with a valid
    // watermark and tombstone chain, and the allocator did not move.
    let mut process = fixture.spawn_riffdbd_with_config(RETENTION_DOCUMENT)?;
    let address =
        parse_ready_address(&process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?)?;
    let mut client = connect(address).await?;
    // A pruned command's idempotent retry still answers its first outcome.
    let replay = match timeout(
        RPC_TIMEOUT,
        client.execute_generated(&create_at(0)?, one_attempt(), &authenticated),
    )
    .await
    {
        Ok(Ok(replay)) => replay,
        Ok(Err(error)) => {
            let exit = process.wait_for_exit(Duration::from_secs(5));
            return Err(test_failure(format!(
                "pruned command retry failed: {error:?}; daemon: {exit:?}"
            )));
        }
        Err(_) => return Err(test_failure("pruned command retry timed out")),
    };
    if replay.response().commit_sequence != 1 {
        return Err(test_failure(
            "a pruned command's retry did not answer its original commit",
        ));
    }
    for index in commands_before_kill..commands_before_kill + COMMANDS_AFTER_RESTART {
        commit_create(&mut client, index, &authenticated).await?;
    }
    await_pruned_scan(&mut client, &authenticated).await?;
    drop(client);
    process.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;

    // Offline status validates the watermark and tombstone chain the online
    // lane wrote; it never ran an offline prune.
    let head = commands_before_kill + COMMANDS_AFTER_RESTART;
    let status = riffdb_storage_redb::RedbOfflineRetention::bind(fixture.database_path())
        .status()
        .map_err(|error| test_failure(format!("offline retention status: {error:?}")))?;
    if status.watermark_sequence == 0
        || status.watermark_sequence > head - RETAINED_SEQUENCES
        || status.tombstone_count == 0
    {
        return Err(test_failure(format!(
            "online retention left watermark {} with {} tombstones at head {head}",
            status.watermark_sequence, status.tombstone_count
        )));
    }
    // Two complete structural and catalog passes over the stopped database
    // find nothing and agree on the frontier: the online lane moved no
    // command, allocator, or catalog state.
    for pass in 0..2 {
        let ports = open_stopped(fixture.database_path())?;
        let frontier = riffdb_storage_api::SnapshotFenceReader::application_frontier(
            &riffdb_storage_api::OwnedSnapshotReader::open_owned_snapshot(&ports)?,
        )?
        .map_or(0, CommitSequence::get);
        if frontier != head {
            return Err(test_failure(format!(
                "online retention moved the recovered frontier to {frontier} (pass {pass})"
            )));
        }
    }
    Ok(())
}

/// Opens a stopped database through the complete structural and catalog
/// pass, refusing any finding.
fn open_stopped(path: &Path) -> TestResult<riffdb_storage_redb::RedbOperationalPorts> {
    use riffdb_storage_api::{
        StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
        StructuralEvidenceSession, StructuralOpenOutcome,
    };
    let store = riffdb_storage_redb::RedbStore::open(path)?;
    let mut session = store.begin_structural_evidence(startup_inputs()?)?;
    let limit =
        riffdb_storage_api::EvidencePageLimit::new(64).ok_or_else(|| test_failure("page limit"))?;
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let structural_end = loop {
        match session.read_structural_evidence(cursor, limit)? {
            StructuralEvidencePage::Page { findings, next, .. } => {
                if !findings.is_empty() {
                    return Err(test_failure(format!(
                        "structural findings after online retention: {findings:?}"
                    )));
                }
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (_, historical_end) = riffdb_catalog::validate_catalog_history(&mut session)?.into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural_end, historical_end)?
    else {
        return Err(test_failure("stopped database did not open cleanly"));
    };
    let (_, _, _, dormant) = opened.into_parts();
    Ok(dormant.into_operational_after_catalog_validation()?)
}
