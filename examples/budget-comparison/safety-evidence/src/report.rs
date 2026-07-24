//! Typed, bounded `riffdb.budget.safety-evidence/v1` report.

use std::error::Error;
use std::fmt;

use riffdb_budget_comparison_core::Amount;
use serde_json::{Map, Value, json};

/// Exact report and runner protocol identifier.
pub const SAFETY_EVIDENCE_SCHEMA: &str = "riffdb.budget.safety-evidence/v1";
/// Exact qualifier on every claim in the report.
pub const CLAIM_SCOPE: &str = "supported_application_mutation_surface";
/// Maximum encoded report size.
pub const MAX_REPORT_BYTES: usize = 32_768;
/// Exact checked runner failure.
pub const CHECKED_ERROR: &[u8] = b"riffdb budget safety evidence failed\n";
/// Exact invalid-invocation failure.
pub const INVALID_INVOCATION: &[u8] = b"riffdb budget safety invocation invalid\n";

/// One typed version-one safety evidence report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafetyEvidenceReport {
    /// Four scenarios in their accepted fixed order.
    pub scenarios: [SafetyScenario; 4],
}

impl SafetyEvidenceReport {
    /// Constructs a report from the four accepted scenarios.
    #[must_use]
    pub const fn new(
        lost_update: LostUpdateScenario,
        direct_dml: DirectDmlScenario,
        duplicate_retry: DuplicateRetryScenario,
        same_key_different_input: SameKeyDifferentInputScenario,
    ) -> Self {
        Self {
            scenarios: [
                SafetyScenario::LostUpdateWithoutLock(lost_update),
                SafetyScenario::DirectDmlPreconditionBypass(direct_dml),
                SafetyScenario::DuplicateRetryAfterDiscardedResponse(duplicate_retry),
                SafetyScenario::SameKeyDifferentInput(same_key_different_input),
            ],
        }
    }
}

/// One member of the closed scenario set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SafetyScenario {
    /// Deterministic unlocked PostgreSQL lost update versus RiffDB contention.
    LostUpdateWithoutLock(LostUpdateScenario),
    /// Direct SQL precondition bypass versus a declared RiffDB outcome.
    DirectDmlPreconditionBypass(DirectDmlScenario),
    /// Duplicate PostgreSQL retry versus RiffDB replay.
    DuplicateRetryAfterDiscardedResponse(DuplicateRetryScenario),
    /// Reused PostgreSQL key versus RiffDB identity mismatch rejection.
    SameKeyDifferentInput(SameKeyDifferentInputScenario),
}

/// Complete observations for `lost_update_without_lock`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LostUpdateScenario {
    /// Deliberately unsafe PostgreSQL observation.
    pub postgres_negative_control: PostgresLostUpdateObservation,
    /// Public RiffDB observation.
    pub riffdb_public: RiffDbLostUpdateObservation,
}

/// PostgreSQL observation for `lost_update_without_lock`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresLostUpdateObservation {
    /// Number of handlers that reported allocation success.
    pub accepted_count: u32,
    /// Whether the unchanged safe adapter still passes its contention oracle.
    pub canonical_adapter_oracle_passed: bool,
    /// Durable final allocation after the controlled schedule.
    pub final_allocated_amount: Amount,
    /// Sum of allocations accepted by both handlers.
    pub logical_accepted_amount: Amount,
    /// Whether both table constraints remain true.
    pub row_checks_hold: bool,
}

/// Public RiffDB observation for `lost_update_without_lock`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbLostUpdateObservation {
    /// Affected entities in the successful allocation commit.
    pub allocated_commit_affected_entity_count: u32,
    /// Durable events in the successful allocation commit.
    pub allocated_commit_event_count: u32,
    /// Number of `Allocated` outcomes.
    pub allocated_count: u32,
    /// Durable final allocation.
    pub final_allocated_amount: Amount,
    /// Affected entities in the insufficient-budget commit.
    pub insufficient_budget_commit_affected_entity_count: u32,
    /// Durable events in the insufficient-budget commit.
    pub insufficient_budget_commit_event_count: u32,
    /// Number of `InsufficientBudget` outcomes.
    pub insufficient_budget_count: u32,
    /// Number of committed declared outcomes.
    pub terminal_declared_outcome_count: u32,
}

/// Complete observations for `direct_dml_precondition_bypass`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectDmlScenario {
    /// Deliberately unsafe PostgreSQL observation.
    pub postgres_negative_control: PostgresDirectDmlObservation,
    /// Public RiffDB observation.
    pub riffdb_public: RiffDbDirectDmlObservation,
}

/// PostgreSQL observation for `direct_dml_precondition_bypass`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresDirectDmlObservation {
    /// Whether the direct DML committed.
    pub direct_dml_committed: bool,
    /// Durable final allocation.
    pub final_allocated_amount: Amount,
    /// Negative amount passed to the safe adapter and direct DML.
    pub requested_amount: Amount,
    /// Whether both table constraints remain true.
    pub row_checks_hold: bool,
    /// Declared result returned by the unchanged safe adapter.
    pub safe_adapter_outcome: DeclaredOutcomeName,
    /// Allocation before the direct DML.
    pub starting_allocated_amount: Amount,
}

/// Public RiffDB observation for `direct_dml_precondition_bypass`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbDirectDmlObservation {
    /// Command completion class.
    pub completion: CommandCompletion,
    /// Durable final allocation.
    pub final_allocated_amount: Amount,
    /// Whether the public application protocol contains generic DML.
    pub generic_application_dml_rpc_present: bool,
    /// Declared command outcome.
    pub outcome: DeclaredOutcomeName,
    /// Affected entities in the rejection commit.
    pub rejection_commit_affected_entity_count: u32,
    /// Durable events in the rejection commit.
    pub rejection_commit_event_count: u32,
    /// Whether the rejection has a nonzero commit sequence.
    pub rejection_has_commit_sequence: bool,
    /// Whether the rejection has a provenance locator.
    pub rejection_has_provenance_uri: bool,
}

/// Complete observations for `duplicate_retry_after_discarded_response`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplicateRetryScenario {
    /// Deliberately unsafe PostgreSQL observation.
    pub postgres_negative_control: PostgresDuplicateRetryObservation,
    /// Public RiffDB observation.
    pub riffdb_public: RiffDbDuplicateRetryObservation,
}

/// PostgreSQL observation for `duplicate_retry_after_discarded_response`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresDuplicateRetryObservation {
    /// Number of committed allocation mutations.
    pub committed_allocation_count: u32,
    /// Durable final allocation.
    pub final_allocated_amount: Amount,
    /// First declared outcome.
    pub first_outcome: DeclaredOutcomeName,
    /// Retry declared outcome.
    pub second_outcome: DeclaredOutcomeName,
}

/// Public RiffDB observation for `duplicate_retry_after_discarded_response`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbDuplicateRetryObservation {
    /// Affected entities in the single allocation commit.
    pub allocation_commit_affected_entity_count: u32,
    /// Durable events in the single allocation commit.
    pub allocation_commit_event_count: u32,
    /// Durable final allocation.
    pub final_allocated_amount: Amount,
    /// Matching allocation commits in the exact-end scan.
    pub matching_allocation_commit_count: u32,
    /// Replay completion class.
    pub replay_completion: CommandCompletion,
    /// Whether public outcome resolution matched the replay locator and value.
    pub replay_outcome_lookup_matches: bool,
    /// Whether replay retained the original commit sequence.
    pub replay_same_commit_sequence: bool,
    /// Whether replay retained the original declared outcome.
    pub replay_same_declared_outcome: bool,
    /// Whether replay retained the original plan hash.
    pub replay_same_plan_hash: bool,
    /// Whether replay retained the original provenance locator.
    pub replay_same_provenance_uri: bool,
}

/// Complete observations for `same_key_different_input`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SameKeyDifferentInputScenario {
    /// Deliberately unsafe PostgreSQL observation.
    pub postgres_negative_control: PostgresSameKeyDifferentInputObservation,
    /// Public RiffDB observation.
    pub riffdb_public: RiffDbSameKeyDifferentInputObservation,
}

/// PostgreSQL observation for `same_key_different_input`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresSameKeyDifferentInputObservation {
    /// Number of committed allocation mutations.
    pub committed_allocation_count: u32,
    /// Durable final allocation.
    pub final_allocated_amount: Amount,
    /// First allocation amount.
    pub first_amount: Amount,
    /// Second allocation amount.
    pub second_amount: Amount,
}

/// Public RiffDB observation for `same_key_different_input`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbSameKeyDifferentInputObservation {
    /// Affected entities in the one successful allocation commit.
    pub allocation_commit_affected_entity_count: u32,
    /// Durable events in the one successful allocation commit.
    pub allocation_commit_event_count: u32,
    /// Durable final allocation.
    pub final_allocated_amount: Amount,
    /// Matching allocation commits in the exact-end scan.
    pub matching_allocation_commit_count: u32,
    /// Safe public mismatch detail class.
    pub mismatch_error_details: PublicErrorDetailsName,
    /// Safe public mismatch kind.
    pub mismatch_error_kind: PublicErrorKindName,
    /// Whether the safe public error contains an incident ID.
    pub mismatch_incident_id_present: bool,
    /// Whether the application frontier stayed fixed across the mismatch.
    pub post_mismatch_application_frontier_unchanged: bool,
    /// Whether secret canaries are absent from all checked presentation surfaces.
    pub secret_canary_absent: bool,
}

/// Declared outcome spellings used by the fixed scenarios.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeclaredOutcomeName {
    /// `Allocated`.
    Allocated,
    /// `InvalidAmount`.
    InvalidAmount,
}

impl DeclaredOutcomeName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Allocated => "Allocated",
            Self::InvalidAmount => "InvalidAmount",
        }
    }
}

/// Public command completion spellings used by the report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandCompletion {
    /// A new durable command record committed.
    Committed,
    /// The original durable command record was replayed.
    Replayed,
}

impl CommandCompletion {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "Committed",
            Self::Replayed => "Replayed",
        }
    }
}

/// Safe public error kind fixed by the mismatch scenario.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicErrorKindName {
    /// Canonical `idempotency_key_reuse` spelling.
    IdempotencyKeyReuse,
}

impl PublicErrorKindName {
    const fn as_str(self) -> &'static str {
        "idempotency_key_reuse"
    }
}

/// Safe public error detail class fixed by the mismatch scenario.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicErrorDetailsName {
    /// No public details are exposed.
    None,
}

impl PublicErrorDetailsName {
    const fn as_str(self) -> &'static str {
        "none"
    }
}

/// One generated fixture relative to the comparison workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafetyFixture {
    /// Workspace-relative fixture path.
    pub relative_path: &'static str,
    /// Complete fixture bytes.
    pub contents: Vec<u8>,
}

/// Creates the accepted deterministic report.
#[must_use]
pub fn expected_report() -> SafetyEvidenceReport {
    SafetyEvidenceReport::new(
        LostUpdateScenario {
            postgres_negative_control: PostgresLostUpdateObservation {
                accepted_count: 2,
                canonical_adapter_oracle_passed: true,
                final_allocated_amount: amount("80.00"),
                logical_accepted_amount: amount("160.00"),
                row_checks_hold: true,
            },
            riffdb_public: RiffDbLostUpdateObservation {
                allocated_commit_affected_entity_count: 1,
                allocated_commit_event_count: 1,
                allocated_count: 1,
                final_allocated_amount: amount("80.00"),
                insufficient_budget_commit_affected_entity_count: 0,
                insufficient_budget_commit_event_count: 0,
                insufficient_budget_count: 1,
                terminal_declared_outcome_count: 2,
            },
        },
        DirectDmlScenario {
            postgres_negative_control: PostgresDirectDmlObservation {
                direct_dml_committed: true,
                final_allocated_amount: amount("20.00"),
                requested_amount: amount("-10.00"),
                row_checks_hold: true,
                safe_adapter_outcome: DeclaredOutcomeName::InvalidAmount,
                starting_allocated_amount: amount("30.00"),
            },
            riffdb_public: RiffDbDirectDmlObservation {
                completion: CommandCompletion::Committed,
                final_allocated_amount: amount("30.00"),
                generic_application_dml_rpc_present: false,
                outcome: DeclaredOutcomeName::InvalidAmount,
                rejection_commit_affected_entity_count: 0,
                rejection_commit_event_count: 0,
                rejection_has_commit_sequence: true,
                rejection_has_provenance_uri: true,
            },
        },
        DuplicateRetryScenario {
            postgres_negative_control: PostgresDuplicateRetryObservation {
                committed_allocation_count: 2,
                final_allocated_amount: amount("60.00"),
                first_outcome: DeclaredOutcomeName::Allocated,
                second_outcome: DeclaredOutcomeName::Allocated,
            },
            riffdb_public: RiffDbDuplicateRetryObservation {
                allocation_commit_affected_entity_count: 1,
                allocation_commit_event_count: 1,
                final_allocated_amount: amount("30.00"),
                matching_allocation_commit_count: 1,
                replay_completion: CommandCompletion::Replayed,
                replay_outcome_lookup_matches: true,
                replay_same_commit_sequence: true,
                replay_same_declared_outcome: true,
                replay_same_plan_hash: true,
                replay_same_provenance_uri: true,
            },
        },
        SameKeyDifferentInputScenario {
            postgres_negative_control: PostgresSameKeyDifferentInputObservation {
                committed_allocation_count: 2,
                final_allocated_amount: amount("70.00"),
                first_amount: amount("30.00"),
                second_amount: amount("40.00"),
            },
            riffdb_public: RiffDbSameKeyDifferentInputObservation {
                allocation_commit_affected_entity_count: 1,
                allocation_commit_event_count: 1,
                final_allocated_amount: amount("30.00"),
                matching_allocation_commit_count: 1,
                mismatch_error_details: PublicErrorDetailsName::None,
                mismatch_error_kind: PublicErrorKindName::IdempotencyKeyReuse,
                mismatch_incident_id_present: false,
                post_mismatch_application_frontier_unchanged: true,
                secret_canary_absent: true,
            },
        },
    )
}

/// Renders canonical lexicographically ordered pretty JSON with one final LF.
pub fn render_report_pretty(report: &SafetyEvidenceReport) -> Result<String, ReportJsonError> {
    finish_render(
        serde_json::to_string_pretty(&report_value(report))
            .map_err(|_| ReportJsonError::Serialization)?,
    )
}

/// Renders canonical compact JSONL with one final LF.
pub fn render_report_jsonl(report: &SafetyEvidenceReport) -> Result<String, ReportJsonError> {
    finish_render(
        serde_json::to_string(&report_value(report)).map_err(|_| ReportJsonError::Serialization)?,
    )
}

/// Parses and requires the canonical pretty representation.
pub fn parse_report_pretty(source: &str) -> Result<SafetyEvidenceReport, ReportJsonError> {
    parse_report(source, ReportEncoding::Pretty)
}

/// Parses and requires the canonical compact JSONL representation.
pub fn parse_report_jsonl(source: &str) -> Result<SafetyEvidenceReport, ReportJsonError> {
    parse_report(source, ReportEncoding::Jsonl)
}

/// Generates all exact WP-139 fixtures.
pub fn generated_safety_fixtures() -> Result<Vec<SafetyFixture>, ReportJsonError> {
    let report = expected_report();
    Ok(vec![
        SafetyFixture {
            relative_path: "fixtures/safety/report-v1.json",
            contents: render_report_pretty(&report)?.into_bytes(),
        },
        SafetyFixture {
            relative_path: "fixtures/safety/report-v1.jsonl",
            contents: render_report_jsonl(&report)?.into_bytes(),
        },
        SafetyFixture {
            relative_path: "fixtures/safety/checked-error.txt",
            contents: CHECKED_ERROR.to_vec(),
        },
        SafetyFixture {
            relative_path: "fixtures/safety/invalid-invocation.txt",
            contents: INVALID_INVOCATION.to_vec(),
        },
    ])
}

fn finish_render(mut output: String) -> Result<String, ReportJsonError> {
    output.push('\n');
    if output.len() > MAX_REPORT_BYTES {
        return Err(ReportJsonError::SourceTooLarge);
    }
    Ok(output)
}

#[derive(Clone, Copy)]
enum ReportEncoding {
    Pretty,
    Jsonl,
}

fn parse_report(
    source: &str,
    encoding: ReportEncoding,
) -> Result<SafetyEvidenceReport, ReportJsonError> {
    if source.len() > MAX_REPORT_BYTES {
        return Err(ReportJsonError::SourceTooLarge);
    }
    let value: Value = serde_json::from_str(source).map_err(|_| ReportJsonError::Malformed)?;
    let report = parse_report_value(&value)?;
    let canonical = match encoding {
        ReportEncoding::Pretty => render_report_pretty(&report)?,
        ReportEncoding::Jsonl => render_report_jsonl(&report)?,
    };
    if source != canonical {
        return Err(ReportJsonError::NonCanonical);
    }
    Ok(report)
}

fn parse_report_value(value: &Value) -> Result<SafetyEvidenceReport, ReportJsonError> {
    let object = as_object(value)?;
    exact_keys(
        object,
        &[
            "benchmark_eligible",
            "claim_scope",
            "postgres_control",
            "riffdb_surface",
            "scenarios",
            "schema",
            "workload_version",
        ],
    )?;
    require_bool(object, "benchmark_eligible", false)?;
    require_str(object, "claim_scope", CLAIM_SCOPE)?;
    require_str(object, "postgres_control", "canonical_adapter_preserved")?;
    require_str(object, "riffdb_surface", "public_rust_sdk_over_grpc")?;
    require_str(object, "schema", SAFETY_EVIDENCE_SCHEMA)?;
    require_u32(object, "workload_version", 1)?;
    let scenarios = field(object, "scenarios")?
        .as_array()
        .ok_or(ReportJsonError::InvalidShape)?;
    if scenarios.len() != 4 {
        return Err(ReportJsonError::InvalidShape);
    }
    Ok(SafetyEvidenceReport::new(
        parse_lost_update(&scenarios[0])?,
        parse_direct_dml(&scenarios[1])?,
        parse_duplicate_retry(&scenarios[2])?,
        parse_same_key(&scenarios[3])?,
    ))
}

fn parse_lost_update(value: &Value) -> Result<LostUpdateScenario, ReportJsonError> {
    let object = scenario_object(
        value,
        "lost_update_without_lock",
        "callers_cannot_select_weaker_conflict_handling",
    )?;
    let postgres = as_object(field(object, "postgres_negative_control")?)?;
    exact_keys(
        postgres,
        &[
            "accepted_count",
            "canonical_adapter_oracle_passed",
            "final_allocated_amount",
            "logical_accepted_amount",
            "row_checks_hold",
        ],
    )?;
    let riffdb = as_object(field(object, "riffdb_public")?)?;
    exact_keys(
        riffdb,
        &[
            "allocated_commit_affected_entity_count",
            "allocated_commit_event_count",
            "allocated_count",
            "final_allocated_amount",
            "insufficient_budget_commit_affected_entity_count",
            "insufficient_budget_commit_event_count",
            "insufficient_budget_count",
            "terminal_declared_outcome_count",
        ],
    )?;
    Ok(LostUpdateScenario {
        postgres_negative_control: PostgresLostUpdateObservation {
            accepted_count: u32_field(postgres, "accepted_count")?,
            canonical_adapter_oracle_passed: bool_field(
                postgres,
                "canonical_adapter_oracle_passed",
            )?,
            final_allocated_amount: amount_field(postgres, "final_allocated_amount")?,
            logical_accepted_amount: amount_field(postgres, "logical_accepted_amount")?,
            row_checks_hold: bool_field(postgres, "row_checks_hold")?,
        },
        riffdb_public: RiffDbLostUpdateObservation {
            allocated_commit_affected_entity_count: u32_field(
                riffdb,
                "allocated_commit_affected_entity_count",
            )?,
            allocated_commit_event_count: u32_field(riffdb, "allocated_commit_event_count")?,
            allocated_count: u32_field(riffdb, "allocated_count")?,
            final_allocated_amount: amount_field(riffdb, "final_allocated_amount")?,
            insufficient_budget_commit_affected_entity_count: u32_field(
                riffdb,
                "insufficient_budget_commit_affected_entity_count",
            )?,
            insufficient_budget_commit_event_count: u32_field(
                riffdb,
                "insufficient_budget_commit_event_count",
            )?,
            insufficient_budget_count: u32_field(riffdb, "insufficient_budget_count")?,
            terminal_declared_outcome_count: u32_field(riffdb, "terminal_declared_outcome_count")?,
        },
    })
}

fn parse_direct_dml(value: &Value) -> Result<DirectDmlScenario, ReportJsonError> {
    let object = scenario_object(
        value,
        "direct_dml_precondition_bypass",
        "declared_precondition_cannot_be_omitted_or_bypassed",
    )?;
    let postgres = as_object(field(object, "postgres_negative_control")?)?;
    exact_keys(
        postgres,
        &[
            "direct_dml_committed",
            "final_allocated_amount",
            "requested_amount",
            "row_checks_hold",
            "safe_adapter_outcome",
            "starting_allocated_amount",
        ],
    )?;
    let riffdb = as_object(field(object, "riffdb_public")?)?;
    exact_keys(
        riffdb,
        &[
            "completion",
            "final_allocated_amount",
            "generic_application_dml_rpc_present",
            "outcome",
            "rejection_commit_affected_entity_count",
            "rejection_commit_event_count",
            "rejection_has_commit_sequence",
            "rejection_has_provenance_uri",
        ],
    )?;
    Ok(DirectDmlScenario {
        postgres_negative_control: PostgresDirectDmlObservation {
            direct_dml_committed: bool_field(postgres, "direct_dml_committed")?,
            final_allocated_amount: amount_field(postgres, "final_allocated_amount")?,
            requested_amount: amount_field(postgres, "requested_amount")?,
            row_checks_hold: bool_field(postgres, "row_checks_hold")?,
            safe_adapter_outcome: outcome_field(postgres, "safe_adapter_outcome")?,
            starting_allocated_amount: amount_field(postgres, "starting_allocated_amount")?,
        },
        riffdb_public: RiffDbDirectDmlObservation {
            completion: completion_field(riffdb, "completion")?,
            final_allocated_amount: amount_field(riffdb, "final_allocated_amount")?,
            generic_application_dml_rpc_present: bool_field(
                riffdb,
                "generic_application_dml_rpc_present",
            )?,
            outcome: outcome_field(riffdb, "outcome")?,
            rejection_commit_affected_entity_count: u32_field(
                riffdb,
                "rejection_commit_affected_entity_count",
            )?,
            rejection_commit_event_count: u32_field(riffdb, "rejection_commit_event_count")?,
            rejection_has_commit_sequence: bool_field(riffdb, "rejection_has_commit_sequence")?,
            rejection_has_provenance_uri: bool_field(riffdb, "rejection_has_provenance_uri")?,
        },
    })
}

fn parse_duplicate_retry(value: &Value) -> Result<DuplicateRetryScenario, ReportJsonError> {
    let object = scenario_object(
        value,
        "duplicate_retry_after_discarded_response",
        "same_request_replays_without_duplicate_mutation",
    )?;
    let postgres = as_object(field(object, "postgres_negative_control")?)?;
    exact_keys(
        postgres,
        &[
            "committed_allocation_count",
            "final_allocated_amount",
            "first_outcome",
            "second_outcome",
        ],
    )?;
    let riffdb = as_object(field(object, "riffdb_public")?)?;
    exact_keys(
        riffdb,
        &[
            "allocation_commit_affected_entity_count",
            "allocation_commit_event_count",
            "final_allocated_amount",
            "matching_allocation_commit_count",
            "replay_completion",
            "replay_outcome_lookup_matches",
            "replay_same_commit_sequence",
            "replay_same_declared_outcome",
            "replay_same_plan_hash",
            "replay_same_provenance_uri",
        ],
    )?;
    Ok(DuplicateRetryScenario {
        postgres_negative_control: PostgresDuplicateRetryObservation {
            committed_allocation_count: u32_field(postgres, "committed_allocation_count")?,
            final_allocated_amount: amount_field(postgres, "final_allocated_amount")?,
            first_outcome: outcome_field(postgres, "first_outcome")?,
            second_outcome: outcome_field(postgres, "second_outcome")?,
        },
        riffdb_public: RiffDbDuplicateRetryObservation {
            allocation_commit_affected_entity_count: u32_field(
                riffdb,
                "allocation_commit_affected_entity_count",
            )?,
            allocation_commit_event_count: u32_field(riffdb, "allocation_commit_event_count")?,
            final_allocated_amount: amount_field(riffdb, "final_allocated_amount")?,
            matching_allocation_commit_count: u32_field(
                riffdb,
                "matching_allocation_commit_count",
            )?,
            replay_completion: completion_field(riffdb, "replay_completion")?,
            replay_outcome_lookup_matches: bool_field(riffdb, "replay_outcome_lookup_matches")?,
            replay_same_commit_sequence: bool_field(riffdb, "replay_same_commit_sequence")?,
            replay_same_declared_outcome: bool_field(riffdb, "replay_same_declared_outcome")?,
            replay_same_plan_hash: bool_field(riffdb, "replay_same_plan_hash")?,
            replay_same_provenance_uri: bool_field(riffdb, "replay_same_provenance_uri")?,
        },
    })
}

fn parse_same_key(value: &Value) -> Result<SameKeyDifferentInputScenario, ReportJsonError> {
    let object = scenario_object(
        value,
        "same_key_different_input",
        "same_identity_different_input_fails_without_execution",
    )?;
    let postgres = as_object(field(object, "postgres_negative_control")?)?;
    exact_keys(
        postgres,
        &[
            "committed_allocation_count",
            "final_allocated_amount",
            "first_amount",
            "second_amount",
        ],
    )?;
    let riffdb = as_object(field(object, "riffdb_public")?)?;
    exact_keys(
        riffdb,
        &[
            "allocation_commit_affected_entity_count",
            "allocation_commit_event_count",
            "final_allocated_amount",
            "matching_allocation_commit_count",
            "mismatch_error_details",
            "mismatch_error_kind",
            "mismatch_incident_id_present",
            "post_mismatch_application_frontier_unchanged",
            "secret_canary_absent",
        ],
    )?;
    Ok(SameKeyDifferentInputScenario {
        postgres_negative_control: PostgresSameKeyDifferentInputObservation {
            committed_allocation_count: u32_field(postgres, "committed_allocation_count")?,
            final_allocated_amount: amount_field(postgres, "final_allocated_amount")?,
            first_amount: amount_field(postgres, "first_amount")?,
            second_amount: amount_field(postgres, "second_amount")?,
        },
        riffdb_public: RiffDbSameKeyDifferentInputObservation {
            allocation_commit_affected_entity_count: u32_field(
                riffdb,
                "allocation_commit_affected_entity_count",
            )?,
            allocation_commit_event_count: u32_field(riffdb, "allocation_commit_event_count")?,
            final_allocated_amount: amount_field(riffdb, "final_allocated_amount")?,
            matching_allocation_commit_count: u32_field(
                riffdb,
                "matching_allocation_commit_count",
            )?,
            mismatch_error_details: details_field(riffdb, "mismatch_error_details")?,
            mismatch_error_kind: kind_field(riffdb, "mismatch_error_kind")?,
            mismatch_incident_id_present: bool_field(riffdb, "mismatch_incident_id_present")?,
            post_mismatch_application_frontier_unchanged: bool_field(
                riffdb,
                "post_mismatch_application_frontier_unchanged",
            )?,
            secret_canary_absent: bool_field(riffdb, "secret_canary_absent")?,
        },
    })
}

fn report_value(report: &SafetyEvidenceReport) -> Value {
    json!({
        "benchmark_eligible": false,
        "claim_scope": CLAIM_SCOPE,
        "postgres_control": "canonical_adapter_preserved",
        "riffdb_surface": "public_rust_sdk_over_grpc",
        "scenarios": report.scenarios.iter().map(scenario_value).collect::<Vec<_>>(),
        "schema": SAFETY_EVIDENCE_SCHEMA,
        "workload_version": 1,
    })
}

fn scenario_value(scenario: &SafetyScenario) -> Value {
    match scenario {
        SafetyScenario::LostUpdateWithoutLock(scenario) => json!({
            "claim": "callers_cannot_select_weaker_conflict_handling",
            "postgres_negative_control": {
                "accepted_count": scenario.postgres_negative_control.accepted_count,
                "canonical_adapter_oracle_passed": scenario.postgres_negative_control.canonical_adapter_oracle_passed,
                "final_allocated_amount": scenario.postgres_negative_control.final_allocated_amount.to_string(),
                "logical_accepted_amount": scenario.postgres_negative_control.logical_accepted_amount.to_string(),
                "row_checks_hold": scenario.postgres_negative_control.row_checks_hold,
            },
            "riffdb_public": {
                "allocated_commit_affected_entity_count": scenario.riffdb_public.allocated_commit_affected_entity_count,
                "allocated_commit_event_count": scenario.riffdb_public.allocated_commit_event_count,
                "allocated_count": scenario.riffdb_public.allocated_count,
                "final_allocated_amount": scenario.riffdb_public.final_allocated_amount.to_string(),
                "insufficient_budget_commit_affected_entity_count": scenario.riffdb_public.insufficient_budget_commit_affected_entity_count,
                "insufficient_budget_commit_event_count": scenario.riffdb_public.insufficient_budget_commit_event_count,
                "insufficient_budget_count": scenario.riffdb_public.insufficient_budget_count,
                "terminal_declared_outcome_count": scenario.riffdb_public.terminal_declared_outcome_count,
            },
            "scenario": "lost_update_without_lock",
        }),
        SafetyScenario::DirectDmlPreconditionBypass(scenario) => json!({
            "claim": "declared_precondition_cannot_be_omitted_or_bypassed",
            "postgres_negative_control": {
                "direct_dml_committed": scenario.postgres_negative_control.direct_dml_committed,
                "final_allocated_amount": scenario.postgres_negative_control.final_allocated_amount.to_string(),
                "requested_amount": scenario.postgres_negative_control.requested_amount.to_string(),
                "row_checks_hold": scenario.postgres_negative_control.row_checks_hold,
                "safe_adapter_outcome": scenario.postgres_negative_control.safe_adapter_outcome.as_str(),
                "starting_allocated_amount": scenario.postgres_negative_control.starting_allocated_amount.to_string(),
            },
            "riffdb_public": {
                "completion": scenario.riffdb_public.completion.as_str(),
                "final_allocated_amount": scenario.riffdb_public.final_allocated_amount.to_string(),
                "generic_application_dml_rpc_present": scenario.riffdb_public.generic_application_dml_rpc_present,
                "outcome": scenario.riffdb_public.outcome.as_str(),
                "rejection_commit_affected_entity_count": scenario.riffdb_public.rejection_commit_affected_entity_count,
                "rejection_commit_event_count": scenario.riffdb_public.rejection_commit_event_count,
                "rejection_has_commit_sequence": scenario.riffdb_public.rejection_has_commit_sequence,
                "rejection_has_provenance_uri": scenario.riffdb_public.rejection_has_provenance_uri,
            },
            "scenario": "direct_dml_precondition_bypass",
        }),
        SafetyScenario::DuplicateRetryAfterDiscardedResponse(scenario) => json!({
            "claim": "same_request_replays_without_duplicate_mutation",
            "postgres_negative_control": {
                "committed_allocation_count": scenario.postgres_negative_control.committed_allocation_count,
                "final_allocated_amount": scenario.postgres_negative_control.final_allocated_amount.to_string(),
                "first_outcome": scenario.postgres_negative_control.first_outcome.as_str(),
                "second_outcome": scenario.postgres_negative_control.second_outcome.as_str(),
            },
            "riffdb_public": {
                "allocation_commit_affected_entity_count": scenario.riffdb_public.allocation_commit_affected_entity_count,
                "allocation_commit_event_count": scenario.riffdb_public.allocation_commit_event_count,
                "final_allocated_amount": scenario.riffdb_public.final_allocated_amount.to_string(),
                "matching_allocation_commit_count": scenario.riffdb_public.matching_allocation_commit_count,
                "replay_completion": scenario.riffdb_public.replay_completion.as_str(),
                "replay_outcome_lookup_matches": scenario.riffdb_public.replay_outcome_lookup_matches,
                "replay_same_commit_sequence": scenario.riffdb_public.replay_same_commit_sequence,
                "replay_same_declared_outcome": scenario.riffdb_public.replay_same_declared_outcome,
                "replay_same_plan_hash": scenario.riffdb_public.replay_same_plan_hash,
                "replay_same_provenance_uri": scenario.riffdb_public.replay_same_provenance_uri,
            },
            "scenario": "duplicate_retry_after_discarded_response",
        }),
        SafetyScenario::SameKeyDifferentInput(scenario) => json!({
            "claim": "same_identity_different_input_fails_without_execution",
            "postgres_negative_control": {
                "committed_allocation_count": scenario.postgres_negative_control.committed_allocation_count,
                "final_allocated_amount": scenario.postgres_negative_control.final_allocated_amount.to_string(),
                "first_amount": scenario.postgres_negative_control.first_amount.to_string(),
                "second_amount": scenario.postgres_negative_control.second_amount.to_string(),
            },
            "riffdb_public": {
                "allocation_commit_affected_entity_count": scenario.riffdb_public.allocation_commit_affected_entity_count,
                "allocation_commit_event_count": scenario.riffdb_public.allocation_commit_event_count,
                "final_allocated_amount": scenario.riffdb_public.final_allocated_amount.to_string(),
                "matching_allocation_commit_count": scenario.riffdb_public.matching_allocation_commit_count,
                "mismatch_error_details": scenario.riffdb_public.mismatch_error_details.as_str(),
                "mismatch_error_kind": scenario.riffdb_public.mismatch_error_kind.as_str(),
                "mismatch_incident_id_present": scenario.riffdb_public.mismatch_incident_id_present,
                "post_mismatch_application_frontier_unchanged": scenario.riffdb_public.post_mismatch_application_frontier_unchanged,
                "secret_canary_absent": scenario.riffdb_public.secret_canary_absent,
            },
            "scenario": "same_key_different_input",
        }),
    }
}

fn scenario_object<'a>(
    value: &'a Value,
    scenario: &str,
    claim: &str,
) -> Result<&'a Map<String, Value>, ReportJsonError> {
    let object = as_object(value)?;
    exact_keys(
        object,
        &[
            "claim",
            "postgres_negative_control",
            "riffdb_public",
            "scenario",
        ],
    )?;
    require_str(object, "scenario", scenario)?;
    require_str(object, "claim", claim)?;
    Ok(object)
}

fn as_object(value: &Value) -> Result<&Map<String, Value>, ReportJsonError> {
    value.as_object().ok_or(ReportJsonError::InvalidShape)
}

fn field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a Value, ReportJsonError> {
    object.get(name).ok_or(ReportJsonError::InvalidShape)
}

fn exact_keys(object: &Map<String, Value>, expected: &[&str]) -> Result<(), ReportJsonError> {
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(ReportJsonError::InvalidShape);
    }
    Ok(())
}

fn bool_field(object: &Map<String, Value>, name: &str) -> Result<bool, ReportJsonError> {
    field(object, name)?
        .as_bool()
        .ok_or(ReportJsonError::InvalidShape)
}

fn u32_field(object: &Map<String, Value>, name: &str) -> Result<u32, ReportJsonError> {
    let value = field(object, name)?
        .as_u64()
        .ok_or(ReportJsonError::InvalidShape)?;
    u32::try_from(value).map_err(|_| ReportJsonError::InvalidValue)
}

fn str_field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a str, ReportJsonError> {
    field(object, name)?
        .as_str()
        .ok_or(ReportJsonError::InvalidShape)
}

fn amount_field(object: &Map<String, Value>, name: &str) -> Result<Amount, ReportJsonError> {
    Amount::parse(str_field(object, name)?).map_err(|_| ReportJsonError::InvalidValue)
}

fn outcome_field(
    object: &Map<String, Value>,
    name: &str,
) -> Result<DeclaredOutcomeName, ReportJsonError> {
    match str_field(object, name)? {
        "Allocated" => Ok(DeclaredOutcomeName::Allocated),
        "InvalidAmount" => Ok(DeclaredOutcomeName::InvalidAmount),
        _ => Err(ReportJsonError::InvalidValue),
    }
}

fn completion_field(
    object: &Map<String, Value>,
    name: &str,
) -> Result<CommandCompletion, ReportJsonError> {
    match str_field(object, name)? {
        "Committed" => Ok(CommandCompletion::Committed),
        "Replayed" => Ok(CommandCompletion::Replayed),
        _ => Err(ReportJsonError::InvalidValue),
    }
}

fn kind_field(
    object: &Map<String, Value>,
    name: &str,
) -> Result<PublicErrorKindName, ReportJsonError> {
    match str_field(object, name)? {
        "idempotency_key_reuse" => Ok(PublicErrorKindName::IdempotencyKeyReuse),
        _ => Err(ReportJsonError::InvalidValue),
    }
}

fn details_field(
    object: &Map<String, Value>,
    name: &str,
) -> Result<PublicErrorDetailsName, ReportJsonError> {
    match str_field(object, name)? {
        "none" => Ok(PublicErrorDetailsName::None),
        _ => Err(ReportJsonError::InvalidValue),
    }
}

fn require_str(
    object: &Map<String, Value>,
    name: &str,
    expected: &str,
) -> Result<(), ReportJsonError> {
    if str_field(object, name)? != expected {
        return Err(ReportJsonError::InvalidValue);
    }
    Ok(())
}

fn require_bool(
    object: &Map<String, Value>,
    name: &str,
    expected: bool,
) -> Result<(), ReportJsonError> {
    if bool_field(object, name)? != expected {
        return Err(ReportJsonError::InvalidValue);
    }
    Ok(())
}

fn require_u32(
    object: &Map<String, Value>,
    name: &str,
    expected: u32,
) -> Result<(), ReportJsonError> {
    if u32_field(object, name)? != expected {
        return Err(ReportJsonError::InvalidValue);
    }
    Ok(())
}

fn amount(text: &str) -> Amount {
    match Amount::parse(text) {
        Ok(amount) => amount,
        Err(_) => unreachable!("accepted report amount is valid"),
    }
}

/// A redaction-safe report parsing or rendering failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportJsonError {
    /// Source or rendered output exceeds 32,768 bytes.
    SourceTooLarge,
    /// Input is malformed JSON.
    Malformed,
    /// The report contains the wrong fields or JSON types.
    InvalidShape,
    /// A fixed value, enum spelling, amount, or integer is invalid.
    InvalidValue,
    /// Input is valid but not in the required canonical encoding.
    NonCanonical,
    /// A typed report could not be serialized.
    Serialization,
}

impl fmt::Display for ReportJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SourceTooLarge => "safety evidence report exceeds its size bound",
            Self::Malformed => "safety evidence report is malformed JSON",
            Self::InvalidShape => "safety evidence report has an invalid shape",
            Self::InvalidValue => "safety evidence report has an invalid value",
            Self::NonCanonical => "safety evidence report is not canonical",
            Self::Serialization => "safety evidence report serialization failed",
        })
    }
}

impl Error for ReportJsonError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_report_round_trips_both_canonical_encodings() {
        let report = expected_report();
        let pretty = render_report_pretty(&report).expect("pretty report");
        let jsonl = render_report_jsonl(&report).expect("compact report");
        assert_eq!(parse_report_pretty(&pretty), Ok(report.clone()));
        assert_eq!(parse_report_jsonl(&jsonl), Ok(report));
        assert!(pretty.ends_with('\n'));
        assert!(jsonl.ends_with('\n'));
        assert!(pretty.len() <= MAX_REPORT_BYTES);
        assert!(jsonl.len() <= MAX_REPORT_BYTES);
    }

    #[test]
    fn parser_rejects_unknown_fields_noncanonical_bytes_and_oversize_input() {
        let report = expected_report();
        let pretty = render_report_pretty(&report).expect("pretty report");
        let mut value: Value = serde_json::from_str(&pretty).expect("JSON");
        value
            .as_object_mut()
            .expect("object")
            .insert(String::from("unknown"), Value::Null);
        let mut unknown = serde_json::to_string_pretty(&value).expect("JSON");
        unknown.push('\n');
        assert_eq!(
            parse_report_pretty(&unknown),
            Err(ReportJsonError::InvalidShape)
        );
        assert_eq!(
            parse_report_pretty(pretty.trim_end()),
            Err(ReportJsonError::NonCanonical)
        );
        assert_eq!(
            parse_report_jsonl(&" ".repeat(MAX_REPORT_BYTES + 1)),
            Err(ReportJsonError::SourceTooLarge)
        );
    }

    #[test]
    fn report_debug_contains_no_input_secret_canary() {
        let debug = format!("{:?}", expected_report());
        assert!(!debug.contains("wp139-secret-canary"));
    }
}
