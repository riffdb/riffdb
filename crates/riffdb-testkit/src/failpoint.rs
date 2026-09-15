//! Closed inventory of integrated recovery evidence.

use std::collections::BTreeSet;

/// How one named recovery boundary is exercised.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RecoveryEvidenceKind {
    /// A public request crosses the production `riffdbd` composition.
    IntegratedRiffdbd,
    /// A dedicated process uses an owner's closed test controller.
    DedicatedCrashChild,
    /// The owning package already supplies the exact deterministic evidence.
    OwnerPackage,
    /// No available hook can establish the required process boundary.
    ProductionGap,
}

impl RecoveryEvidenceKind {
    /// Stable machine-readable spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IntegratedRiffdbd => "integrated-riffdbd",
            Self::DedicatedCrashChild => "dedicated-crash-child",
            Self::OwnerPackage => "owner-package",
            Self::ProductionGap => "production-gap",
        }
    }
}

/// One stable row in the WP-190 recovery matrix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryScenario {
    /// Stable dotted scenario name.
    pub name: &'static str,
    /// Exact evidence class.
    pub evidence: RecoveryEvidenceKind,
    /// Test target or command that owns the assertion.
    pub evidence_target: &'static str,
    /// Comma-separated normative requirement IDs.
    pub requirements: &'static str,
    /// Safe bounded coverage note or explicit gap.
    pub note: &'static str,
}

/// Complete checked-in WP-190 scenario inventory.
///
/// Rows classified as `ProductionGap` are deliberate: retaining them in the
/// same closed inventory prevents a missing production synchronization surface
/// from being mistaken for passing evidence.
pub const RECOVERY_SCENARIOS: &[RecoveryScenario] = &[
    scenario(
        "catalog.deployment.after-active-pointer",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-catalog --test contract_deploy_recovery",
        "REC-001,REC-002,TEST-001",
        "Closed catalog deployment notification failpoint.",
    ),
    scenario(
        "catalog.deployment.before-active-pointer",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-catalog --test contract_deploy_recovery",
        "REC-001,REC-002,TEST-001",
        "Closed catalog deployment notification failpoint.",
    ),
    scenario(
        "catalog.deployment.before-bundle-persist",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-catalog --test contract_deploy_recovery",
        "REC-001,REC-002,TEST-001",
        "Closed catalog deployment notification failpoint.",
    ),
    scenario(
        "cli.bootstrap.after-credential-file-sync",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "full_recovery_matrix::bootstrap_retention_failpoints",
        "REC-001,REC-002,TEST-001",
        "Injected CLI controller aborts after file sync; restart uses the protected credential for the first public RPC.",
    ),
    scenario(
        "cli.bootstrap.after-parent-directory-sync",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "full_recovery_matrix::bootstrap_retention_failpoints",
        "REC-001,REC-002,TEST-001",
        "Injected CLI controller aborts after parent sync; restart uses the protected credential for the first public RPC.",
    ),
    scenario(
        "command.capacity.before-reservation",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "TXN-041,TXN-042,TEST-001",
        "Owner typestate tests prove sequence-free reservation and exact excess rejection.",
    ),
    scenario(
        "command.commit.after-engine-commit",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "REC-001,REC-002,TXN-042,TXN-044,TEST-001",
        "Closed RedbTestController process abort after atomic command commit.",
    ),
    scenario(
        "command.commit.before-engine-commit",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "REC-001,REC-002,TXN-041,TXN-042,TEST-001",
        "Closed RedbTestController process abort before atomic command commit.",
    ),
    scenario(
        "command.envelope.before-staging",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "TXN-041,TXN-042,TEST-001",
        "Owner typestate tests prove canonical-envelope verification precedes staging.",
    ),
    scenario(
        "command.public-response.kill-riffdbd",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "full_recovery_matrix::kill_after_response_frame",
        "POC-004,REC-001,REC-002,TXN-044,TEST-001",
        "A response frame is held, riffdbd is killed, and the immutable SDK command is replayed.",
    ),
    scenario(
        "command.public-response.partial-frame",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "full_recovery_matrix::lose_incomplete_response_frame",
        "POC-004,REC-001,REC-002,TXN-044,TEST-001",
        "Only an incomplete HTTP/2 response-frame prefix reaches the client before disconnect.",
    ),
    scenario(
        "command.sequence.after-assignment",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "TXN-041,TXN-042,TEST-001",
        "Owner typestate tests prove transaction-local assignment is invisible before commit.",
    ),
    scenario(
        "maintenance.backup.after-publication",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Closed maintenance controller abort after immutable named backup publication.",
    ),
    scenario(
        "maintenance.daemon.after-database-close",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "full_recovery_matrix::maintenance_daemon_failpoints",
        "REC-001,REC-002,TEST-001",
        "Public maintenance aborts after graph destruction and restarts through exact receipt reconciliation.",
    ),
    scenario(
        "maintenance.daemon.after-drain",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "full_recovery_matrix::maintenance_daemon_failpoints",
        "REC-001,REC-002,TEST-001",
        "Public maintenance aborts after accepted work and workers drain but before storage ports close.",
    ),
    scenario(
        "maintenance.daemon.after-fresh-validation",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "full_recovery_matrix::maintenance_daemon_failpoints",
        "REC-001,REC-002,TEST-001",
        "Public maintenance aborts after fresh startup validation and completes its terminal receipt on restart.",
    ),
    scenario(
        "maintenance.daemon.after-staged-authorization",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "full_recovery_matrix::maintenance_daemon_failpoints",
        "REC-001,REC-002,TEST-001",
        "Public restore aborts after staged authorization and resumes only with the same operation and credential.",
    ),
    scenario(
        "maintenance.public.backup-restore-rewind",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "cargo test -p riffdb-server --test offline_maintenance_recovery -- --ignored",
        "POC-009,REC-001,REC-002,REC-003,MCP-046",
        "Public maintenance proves identity preservation, rewind, retry, and no MCP surface.",
    ),
    scenario(
        "maintenance.receipt.after-file-sync",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort after replacement receipt bytes are synchronized.",
    ),
    scenario(
        "maintenance.receipt.after-parent-sync",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort after receipt and containing directory are synchronized.",
    ),
    scenario(
        "maintenance.receipt.after-rename",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort after atomic receipt rename but before directory synchronization.",
    ),
    scenario(
        "maintenance.receipt.before-file-sync",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort before replacement receipt synchronization.",
    ),
    scenario(
        "maintenance.receipt.before-rename",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort immediately before receipt rename.",
    ),
    scenario(
        "maintenance.restore.after-stage",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort after exact staged restore materialization.",
    ),
    scenario(
        "maintenance.restore.after-target-parent-sync",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort after destructive target publication and parent synchronization.",
    ),
    scenario(
        "maintenance.restore.after-target-publication",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort after destructive rename but before parent synchronization.",
    ),
    scenario(
        "maintenance.restore.before-target-publication",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "offline_maintenance_matrix::adapter_failpoints",
        "REC-001,REC-002,TEST-001",
        "Process abort before destructive target publication.",
    ),
    scenario(
        "migration.batch.after-engine-commit",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "REC-001,REC-002,TEST-001",
        "Closed RedbTestController process abort after one migration batch.",
    ),
    scenario(
        "migration.batch.before-engine-commit",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "REC-001,REC-002,TEST-001",
        "Closed RedbTestController process abort before one migration batch.",
    ),
    scenario(
        "outbox.delivery.after-claim",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-outbox --test outbox_crash_and_duplicate",
        "EFF-003,REC-001,REC-002,TEST-001",
        "Closed outbox failpoint with deterministic repository state.",
    ),
    scenario(
        "outbox.delivery.after-connector-success",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-outbox --test outbox_crash_and_duplicate",
        "EFF-003,REC-001,REC-002,TEST-001",
        "Closed outbox failpoint proves duplicate delivery is possible without identity change.",
    ),
    scenario(
        "outbox.recovery.after-normalize",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-outbox --test outbox_crash_and_duplicate",
        "EFF-003,REC-001,REC-002,TEST-001",
        "Closed recovery failpoint with Delivering normalization evidence.",
    ),
    scenario(
        "projection.apply.after-marker",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-projection --test projection_prefix_and_recovery",
        "REC-001,REC-002,REC-003,TEST-001",
        "Closed projection hook proves atomic row, marker, and frontier behavior.",
    ),
    scenario(
        "projection.apply.before-frontier",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-projection --test projection_prefix_and_recovery",
        "REC-001,REC-002,REC-003,TEST-001",
        "Closed projection hook proves a frontier never outruns state.",
    ),
    scenario(
        "projection.rebuild.before-publish",
        RecoveryEvidenceKind::OwnerPackage,
        "cargo test -p riffdb-projection --test projection_prefix_and_recovery",
        "REC-001,REC-002,REC-003,TEST-001",
        "Closed projection hook proves new-generation publication is atomic.",
    ),
    scenario(
        "replication.applier.crash",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "cargo test -p riffdb-storage-redb --lib follower_process_crashes_preserve_whole_frames_and_exact_retry_positions",
        "REP-002,REP-003,REC-001",
        "Native process exits at receipt, roots and committed edges; whole-frame rollback or durability and exact read-only retries are checked after reopen.",
    ),
    scenario(
        "replication.bootstrap.crash",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "cargo test -p riffdb-server --test replication_follower bootstrap_to_tail_fence_is_gap_free_across_crash",
        "REP-002,REP-003,REC-001",
        "Two source and two receiver crashes preserve the exact bootstrap fence and all authoritative bytes; foreign incarnation refuses before gap-free tail attachment.",
    ),
    scenario(
        "replication.stream.kill-riffdbd",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "cargo test -p riffdb-server --test replication_follower replication_stream_resumes_gap_free_after_repeated_kills",
        "REP-002,REP-003,REC-001",
        "Three mid-workload process kills resume at the exact durable position; four completely validated prefixes match the independent all-namespace oracle.",
    ),
    scenario(
        "startup.initialization.after-engine-commit",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "REC-001,REC-002,TEST-001",
        "Process abort preserves the one installed DatabaseId.",
    ),
    scenario(
        "startup.initialization.before-engine-commit",
        RecoveryEvidenceKind::DedicatedCrashChild,
        "cargo test -p riffdb-storage-redb --test storage_recovery_matrix",
        "REC-001,REC-002,TEST-001",
        "Process abort leaves a truly empty store without an exposed candidate.",
    ),
    scenario(
        "startup.repeated-complete-validation",
        RecoveryEvidenceKind::IntegratedRiffdbd,
        "full_recovery_matrix::repeated_offline_inspection",
        "REC-001,REC-002,TEST-001",
        "Two complete offline startup passes return equal authoritative inspection snapshots.",
    ),
];

/// Stable schema identifier for the checked WP-190 release evidence.
pub const RECOVERY_REPORT_SCHEMA_V1: &str = "riffdb.wp190.recovery-report.v1";

/// Cases executed directly by the two WP-190 ignored acceptance targets.
///
/// Owner-package evidence and explicit production gaps remain in
/// [`RECOVERY_SCENARIOS`], but are not misrepresented as results from these
/// two integration executables.
pub const WP190_EXECUTED_CASE_IDS: &[&str] = &[
    "cli.bootstrap.after-credential-file-sync",
    "cli.bootstrap.after-parent-directory-sync",
    "command.public-response.kill-riffdbd",
    "command.public-response.partial-frame",
    "maintenance.backup.after-publication",
    "maintenance.daemon.after-database-close",
    "maintenance.daemon.after-drain",
    "maintenance.daemon.after-fresh-validation",
    "maintenance.daemon.after-staged-authorization",
    "maintenance.receipt.after-file-sync",
    "maintenance.receipt.after-parent-sync",
    "maintenance.receipt.after-rename",
    "maintenance.receipt.before-file-sync",
    "maintenance.receipt.before-rename",
    "maintenance.restore.after-stage",
    "maintenance.restore.after-target-parent-sync",
    "maintenance.restore.after-target-publication",
    "maintenance.restore.before-target-publication",
];

/// Production boundaries that remain without process-level evidence.
pub const WP190_PRODUCTION_GAP_IDS: &[&str] = &[];

/// Renders the canonical deterministic JSON evidence contract consumed by
/// WP-200. Passing acceptance tests verify the behavior named by these rows.
#[must_use]
pub fn render_wp190_recovery_report_v1() -> String {
    let mut output = String::from(
        "{\n  \"schema\": \"riffdb.wp190.recovery-report.v1\",\n  \"acceptance_commands\": [\n    \"cargo test -p riffdb-testkit --test full_recovery_matrix -- --ignored\",\n    \"cargo test -p riffdb-testkit --test offline_maintenance_matrix -- --ignored\"\n  ],\n  \"coverage\": {\n",
    );
    use std::fmt::Write as _;
    writeln!(
        output,
        "    \"inventory_cases\": {},",
        RECOVERY_SCENARIOS.len()
    )
    .expect("writing to a String cannot fail");
    writeln!(
        output,
        "    \"executed_cases\": {},",
        WP190_EXECUTED_CASE_IDS.len()
    )
    .expect("writing to a String cannot fail");
    output.push_str("    \"production_gap_cases\": [");
    for (index, id) in WP190_PRODUCTION_GAP_IDS.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        push_json_string(&mut output, id);
    }
    output.push_str("]\n  },\n  \"cases\": [\n");
    for (index, id) in WP190_EXECUTED_CASE_IDS.iter().enumerate() {
        let scenario = RECOVERY_SCENARIOS
            .iter()
            .find(|scenario| scenario.name == *id)
            .expect("executed WP-190 case is present in the closed inventory");
        output.push_str("    {\n      \"id\": ");
        push_json_string(&mut output, scenario.name);
        output.push_str(",\n      \"evidence_kind\": ");
        push_json_string(&mut output, scenario.evidence.as_str());
        output.push_str(",\n      \"evidence_target\": ");
        push_json_string(&mut output, scenario.evidence_target);
        output.push_str(",\n      \"requirements\": [");
        for (requirement_index, requirement) in scenario.requirements.split(',').enumerate() {
            if requirement_index != 0 {
                output.push_str(", ");
            }
            push_json_string(&mut output, requirement);
        }
        output.push_str("],\n      \"guarantee\": ");
        push_json_string(&mut output, scenario.note);
        output.push_str("\n    }");
        if index + 1 != WP190_EXECUTED_CASE_IDS.len() {
            output.push(',');
        }
        output.push('\n');
    }
    output.push_str("  ]\n}\n");
    output
}

/// Verifies that the checked JSON artifact is the exact canonical projection
/// of the closed scenario registry.
pub fn verify_wp190_recovery_report_v1() -> Result<(), RecoveryReportError> {
    validate_recovery_scenarios().map_err(|_| RecoveryReportError::InvalidScenarioRegistry)?;
    let checked = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/recovery/fixtures/wp190-report-v1.json"
    ));
    if checked != render_wp190_recovery_report_v1() {
        return Err(RecoveryReportError::FixtureMismatch);
    }
    Ok(())
}

/// Closed failure from the deterministic WP-190 report verifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryReportError {
    /// The source scenario registry was not canonical.
    InvalidScenarioRegistry,
    /// Checked JSON differed from the canonical registry projection.
    FixtureMismatch,
}

fn push_json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                write!(output, "\\u{:04x}", u32::from(character))
                    .expect("writing to a String cannot fail");
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

const fn scenario(
    name: &'static str,
    evidence: RecoveryEvidenceKind,
    evidence_target: &'static str,
    requirements: &'static str,
    note: &'static str,
) -> RecoveryScenario {
    RecoveryScenario {
        name,
        evidence,
        evidence_target,
        requirements,
        note,
    }
}

/// Validates stable naming, strict order, uniqueness, and explicit gap text.
pub fn validate_recovery_scenarios() -> Result<(), RecoveryScenarioError> {
    let mut prior = None;
    let mut names = BTreeSet::new();
    for scenario in RECOVERY_SCENARIOS {
        if !valid_name(scenario.name)
            || scenario.evidence_target.is_empty()
            || scenario.requirements.is_empty()
            || scenario.note.is_empty()
        {
            return Err(RecoveryScenarioError::InvalidRow);
        }
        if prior.is_some_and(|prior| prior >= scenario.name) {
            return Err(RecoveryScenarioError::NonCanonicalOrder);
        }
        if !names.insert(scenario.name) {
            return Err(RecoveryScenarioError::DuplicateName);
        }
        if scenario.evidence == RecoveryEvidenceKind::ProductionGap
            && scenario.evidence_target != "none"
        {
            return Err(RecoveryScenarioError::InvalidGap);
        }
        prior = Some(scenario.name);
    }
    Ok(())
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 96
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
}

/// Closed validation error for the checked-in matrix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryScenarioError {
    /// A row contained an empty, oversized, or noncanonical field.
    InvalidRow,
    /// Stable names were not in strict lexical order.
    NonCanonicalOrder,
    /// A stable name appeared twice.
    DuplicateName,
    /// A gap row incorrectly claimed an evidence target.
    InvalidGap,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_scenario_inventory_is_closed_and_canonical() {
        assert_eq!(validate_recovery_scenarios(), Ok(()));
        assert!(
            RECOVERY_SCENARIOS
                .iter()
                .any(|scenario| scenario.evidence == RecoveryEvidenceKind::IntegratedRiffdbd)
        );
        assert!(
            RECOVERY_SCENARIOS
                .iter()
                .any(|scenario| scenario.evidence == RecoveryEvidenceKind::DedicatedCrashChild)
        );
        assert!(
            RECOVERY_SCENARIOS
                .iter()
                .all(|scenario| scenario.evidence != RecoveryEvidenceKind::ProductionGap)
        );
    }

    #[test]
    fn wp190_report_case_list_is_closed_and_canonical() {
        assert!(
            WP190_EXECUTED_CASE_IDS
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
        assert!(WP190_EXECUTED_CASE_IDS.iter().all(|id| {
            RECOVERY_SCENARIOS.iter().any(|scenario| {
                scenario.name == *id
                    && matches!(
                        scenario.evidence,
                        RecoveryEvidenceKind::IntegratedRiffdbd
                            | RecoveryEvidenceKind::DedicatedCrashChild
                    )
            })
        }));
        assert!(
            WP190_PRODUCTION_GAP_IDS
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
        assert_eq!(
            WP190_PRODUCTION_GAP_IDS,
            RECOVERY_SCENARIOS
                .iter()
                .filter(|scenario| scenario.evidence == RecoveryEvidenceKind::ProductionGap)
                .map(|scenario| scenario.name)
                .collect::<Vec<_>>()
        );
        assert_eq!(verify_wp190_recovery_report_v1(), Ok(()));
    }
}
