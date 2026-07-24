//! Live public-RiffDB and isolated-PostgreSQL report assembly.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use riffdb_budget_comparison_core::{BudgetOutcome, ContentionObservation};
use riffdb_budget_comparison_riffdb_grpc::{
    PublicAllocationReplaySafetyEvidence, PublicCommandCompletion, PublicContentionSafetyEvidence,
    PublicIdempotencyMismatchSafetyEvidence, PublicInvalidAmountSafetyEvidence,
    RiffDbPublicBudgetAdapter,
};
use riffdb_client_rust::{BearerCredential, PublicErrorKind};
use tokio::time::timeout;
use tonic::transport::Endpoint;

use crate::{
    CommandCompletion, DirectDmlScenario, DuplicateRetryScenario, LostUpdateScenario,
    PostgresNegativeControlObservations, PostgresSafetyNegativeControl, ProtectedPostgresUrl,
    PublicErrorDetailsName, PublicErrorKindName, RiffDbDirectDmlObservation,
    RiffDbDuplicateRetryObservation, RiffDbLostUpdateObservation,
    RiffDbSameKeyDifferentInputObservation, SafetyEvidenceReport, SameKeyDifferentInputScenario,
    expected_report, render_report_jsonl, safety_workloads,
};

const PUBLIC_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const SECRET_CANARY: &str = "wp139-secret-canary";

/// Executes all four live contrasts and returns the exact accepted report.
pub async fn run_live_safety_evidence(
    postgres_url: &ProtectedPostgresUrl,
    endpoint: Endpoint,
    credential: BearerCredential,
) -> Result<SafetyEvidenceReport, SafetyRunError> {
    let workloads = safety_workloads();
    let postgres_url = postgres_url.clone();
    let postgres_workloads = workloads.clone();
    let postgres = tokio::task::spawn_blocking(move || {
        PostgresSafetyNegativeControl::new(&postgres_url)?.run_all(&postgres_workloads)
    })
    .await
    .map_err(|_| SafetyRunError::Postgres)?
    .map_err(|_| SafetyRunError::Postgres)?;
    let endpoint = endpoint
        .connect_timeout(PUBLIC_OPERATION_TIMEOUT)
        .timeout(PUBLIC_OPERATION_TIMEOUT);
    let mut riffdb = RiffDbPublicBudgetAdapter::connect(endpoint, credential)
        .await
        .map_err(|_| SafetyRunError::RiffDb)?;

    let contention = timeout(
        PUBLIC_OPERATION_TIMEOUT,
        riffdb.prove_contention_commit_facts(&workloads.lost_update),
    )
    .await
    .map_err(|_| SafetyRunError::RiffDb)?
    .map_err(|_| SafetyRunError::RiffDb)?;
    let direct_dml = timeout(
        PUBLIC_OPERATION_TIMEOUT,
        riffdb.prove_committed_invalid_amount(
            &workloads.direct_dml_seed,
            &workloads.direct_dml_initial,
            &workloads.direct_dml_invalid,
        ),
    )
    .await
    .map_err(|_| SafetyRunError::RiffDb)?
    .map_err(|_| SafetyRunError::RiffDb)?;
    let duplicate_retry = timeout(
        PUBLIC_OPERATION_TIMEOUT,
        riffdb.prove_allocate_discarded_response_replay(
            &workloads.duplicate_retry_seed,
            &workloads.duplicate_retry_allocation,
        ),
    )
    .await
    .map_err(|_| SafetyRunError::RiffDb)?
    .map_err(|_| SafetyRunError::RiffDb)?;
    let same_key = timeout(
        PUBLIC_OPERATION_TIMEOUT,
        riffdb.prove_same_key_different_input(
            &workloads.same_key_seed,
            &workloads.same_key_first,
            &workloads.same_key_mismatch,
        ),
    )
    .await
    .map_err(|_| SafetyRunError::RiffDb)?
    .map_err(|_| SafetyRunError::RiffDb)?;

    assemble_checked_report(postgres, contention, direct_dml, duplicate_retry, same_key)
}

fn assemble_checked_report(
    postgres: PostgresNegativeControlObservations,
    contention: PublicContentionSafetyEvidence,
    direct_dml: PublicInvalidAmountSafetyEvidence,
    duplicate_retry: PublicAllocationReplaySafetyEvidence,
    same_key: PublicIdempotencyMismatchSafetyEvidence,
) -> Result<SafetyEvidenceReport, SafetyRunError> {
    let report = SafetyEvidenceReport::new(
        LostUpdateScenario {
            postgres_negative_control: postgres.lost_update,
            riffdb_public: map_contention(contention)?,
        },
        DirectDmlScenario {
            postgres_negative_control: postgres.direct_dml,
            riffdb_public: map_direct_dml(direct_dml)?,
        },
        DuplicateRetryScenario {
            postgres_negative_control: postgres.duplicate_retry,
            riffdb_public: map_duplicate_retry(duplicate_retry)?,
        },
        SameKeyDifferentInputScenario {
            postgres_negative_control: postgres.same_key_different_input,
            riffdb_public: map_same_key(same_key)?,
        },
    );
    if report != expected_report() {
        return Err(SafetyRunError::EvidenceMismatch);
    }
    let encoded = render_report_jsonl(&report).map_err(|_| SafetyRunError::EvidenceMismatch)?;
    let debug = format!("{report:?}");
    if encoded.contains(SECRET_CANARY) || debug.contains(SECRET_CANARY) {
        return Err(SafetyRunError::EvidenceMismatch);
    }
    Ok(report)
}

fn map_contention(
    evidence: PublicContentionSafetyEvidence,
) -> Result<RiffDbLostUpdateObservation, SafetyRunError> {
    require_commit_name(&evidence.allocated_commit.outcome_name, "Allocated")?;
    require_commit_name(
        &evidence.insufficient_budget_commit.outcome_name,
        "InsufficientBudget",
    )?;
    let (allocated_count, insufficient_count) = contention_counts(&evidence.observation)?;
    let final_budget = evidence
        .observation
        .final_budget
        .ok_or(SafetyRunError::EvidenceMismatch)?;
    Ok(RiffDbLostUpdateObservation {
        allocated_commit_affected_entity_count: count(
            evidence.allocated_commit.affected_entity_count,
        )?,
        allocated_commit_event_count: count(evidence.allocated_commit.event_count)?,
        allocated_count,
        final_allocated_amount: final_budget.allocated_amount,
        insufficient_budget_commit_affected_entity_count: count(
            evidence.insufficient_budget_commit.affected_entity_count,
        )?,
        insufficient_budget_commit_event_count: count(
            evidence.insufficient_budget_commit.event_count,
        )?,
        insufficient_budget_count: insufficient_count,
        terminal_declared_outcome_count: count(evidence.terminal_declared_outcome_count)?,
    })
}

fn contention_counts(observation: &ContentionObservation) -> Result<(u32, u32), SafetyRunError> {
    let allocated = observation
        .outcomes
        .iter()
        .filter(|outcome| matches!(outcome, BudgetOutcome::Allocated { .. }))
        .count();
    let insufficient = observation
        .outcomes
        .iter()
        .filter(|outcome| matches!(outcome, BudgetOutcome::InsufficientBudget { .. }))
        .count();
    Ok((count(allocated)?, count(insufficient)?))
}

fn map_direct_dml(
    evidence: PublicInvalidAmountSafetyEvidence,
) -> Result<RiffDbDirectDmlObservation, SafetyRunError> {
    require_commit_name(&evidence.rejection_commit.outcome_name, "InvalidAmount")?;
    if !matches!(
        evidence.rejection.observation.outcome,
        BudgetOutcome::InvalidAmount { .. }
    ) {
        return Err(SafetyRunError::EvidenceMismatch);
    }
    Ok(RiffDbDirectDmlObservation {
        completion: completion(evidence.rejection.metadata.completion),
        final_allocated_amount: evidence.final_budget.allocated_amount,
        generic_application_dml_rpc_present: false,
        outcome: crate::DeclaredOutcomeName::InvalidAmount,
        rejection_commit_affected_entity_count: count(
            evidence.rejection_commit.affected_entity_count,
        )?,
        rejection_commit_event_count: count(evidence.rejection_commit.event_count)?,
        rejection_has_commit_sequence: evidence.rejection.metadata.commit_sequence.get() != 0,
        rejection_has_provenance_uri: !evidence.rejection.metadata.provenance_uri.is_empty(),
    })
}

fn map_duplicate_retry(
    evidence: PublicAllocationReplaySafetyEvidence,
) -> Result<RiffDbDuplicateRetryObservation, SafetyRunError> {
    require_commit_name(&evidence.allocation_commit.outcome_name, "Allocated")?;
    if !matches!(
        evidence.observation.outcome,
        BudgetOutcome::Allocated { .. }
    ) {
        return Err(SafetyRunError::EvidenceMismatch);
    }
    Ok(RiffDbDuplicateRetryObservation {
        allocation_commit_affected_entity_count: count(
            evidence.allocation_commit.affected_entity_count,
        )?,
        allocation_commit_event_count: count(evidence.allocation_commit.event_count)?,
        final_allocated_amount: evidence.final_budget.allocated_amount,
        matching_allocation_commit_count: count(evidence.matching_allocation_commit_count)?,
        replay_completion: completion(evidence.replay_metadata.completion),
        replay_outcome_lookup_matches: evidence.outcome_lookup_matches,
        replay_same_commit_sequence: evidence.same_commit_sequence,
        replay_same_declared_outcome: evidence.same_declared_outcome,
        replay_same_plan_hash: evidence.same_plan_hash,
        replay_same_provenance_uri: evidence.same_provenance_uri,
    })
}

fn map_same_key(
    evidence: PublicIdempotencyMismatchSafetyEvidence,
) -> Result<RiffDbSameKeyDifferentInputObservation, SafetyRunError> {
    require_commit_name(&evidence.allocation_commit.outcome_name, "Allocated")?;
    if !matches!(
        evidence.first.observation.outcome,
        BudgetOutcome::Allocated { .. }
    ) || evidence.mismatch_error_kind != PublicErrorKind::IdempotencyKeyReuse
        || !evidence.mismatch_error_details_none
    {
        return Err(SafetyRunError::EvidenceMismatch);
    }
    Ok(RiffDbSameKeyDifferentInputObservation {
        allocation_commit_affected_entity_count: count(
            evidence.allocation_commit.affected_entity_count,
        )?,
        allocation_commit_event_count: count(evidence.allocation_commit.event_count)?,
        final_allocated_amount: evidence.final_budget.allocated_amount,
        matching_allocation_commit_count: count(evidence.matching_allocation_commit_count)?,
        mismatch_error_details: PublicErrorDetailsName::None,
        mismatch_error_kind: PublicErrorKindName::IdempotencyKeyReuse,
        mismatch_incident_id_present: evidence.mismatch_incident_id_present,
        post_mismatch_application_frontier_unchanged: evidence.application_frontier_unchanged,
        secret_canary_absent: evidence.public_error_canary_absent,
    })
}

fn require_commit_name(actual: &str, expected: &str) -> Result<(), SafetyRunError> {
    if actual == expected {
        Ok(())
    } else {
        Err(SafetyRunError::EvidenceMismatch)
    }
}

fn completion(value: PublicCommandCompletion) -> CommandCompletion {
    match value {
        PublicCommandCompletion::Committed => CommandCompletion::Committed,
        PublicCommandCompletion::Replayed => CommandCompletion::Replayed,
    }
}

fn count(value: usize) -> Result<u32, SafetyRunError> {
    u32::try_from(value).map_err(|_| SafetyRunError::EvidenceMismatch)
}

/// A closed, redaction-safe live evidence failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SafetyRunError {
    /// PostgreSQL could not produce the exact checked observation.
    Postgres,
    /// The public RiffDB path could not produce the exact checked observation.
    RiffDb,
    /// Successful calls did not assemble to the accepted report.
    EvidenceMismatch,
}

impl fmt::Display for SafetyRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Postgres => "PostgreSQL safety evidence failed",
            Self::RiffDb => "public RiffDB safety evidence failed",
            Self::EvidenceMismatch => "safety observations differ from the accepted report",
        })
    }
}

impl Error for SafetyRunError {}
