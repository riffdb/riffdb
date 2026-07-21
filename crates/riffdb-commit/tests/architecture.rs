//! Dependency and authority checks for the commit orchestration boundary.

use std::{
    fs,
    path::{Path, PathBuf},
};

const LOCKFILE: &str = include_str!("../../../Cargo.lock");
const MANIFEST: &str = include_str!("../Cargo.toml");
const AUDIT_SOURCE: &str = include_str!("../src/audit.rs");
const AUDIT_EXECUTOR_SOURCE: &str = include_str!("../src/audit_executor.rs");
const COMMAND_ADMISSION_SOURCE: &str = include_str!("../src/command_admission.rs");
const COMMAND_ATTEMPT_SOURCE: &str = include_str!("../src/command_attempt.rs");
const COMMAND_VALIDATION_SOURCE: &str = include_str!("../src/command_validation.rs");
const COMMAND_PREPARATION_SOURCE: &str = include_str!("../src/command_preparation.rs");
const LIB_SOURCE: &str = include_str!("../src/lib.rs");
const CLOCK_SOURCE: &str = include_str!("../src/clock.rs");
const INITIALIZATION_SOURCE: &str = include_str!("../src/initialization.rs");
const OUTCOME_SOURCE: &str = include_str!("../src/outcome.rs");
const PROVENANCE_SOURCE: &str = include_str!("../src/provenance.rs");

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn production_dependency_owners(dependency: &str) -> Vec<String> {
    let root = crate_root();
    let crates = root.parent().expect("workspace crates directory");
    let mut owners = fs::read_dir(crates)
        .expect("read workspace crates")
        .filter_map(|entry| {
            let path = entry.expect("crate entry").path();
            let manifest = fs::read_to_string(path.join("Cargo.toml")).ok()?;
            let owns_dependency = manifest
                .lines()
                .skip_while(|line| *line != "[dependencies]")
                .skip(1)
                .take_while(|line| !line.starts_with('['))
                .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
                .any(|name| name == dependency);
            owns_dependency.then(|| {
                path.file_name()
                    .expect("crate directory name")
                    .to_string_lossy()
                    .into_owned()
            })
        })
        .collect::<Vec<_>>();
    owners.sort();
    owners
}

fn manifest_section(header: &str) -> &str {
    MANIFEST
        .split_once(header)
        .map(|(_, remainder)| remainder)
        .and_then(|remainder| remainder.split_once("\n[").map(|(section, _)| section))
        .unwrap_or_else(|| panic!("manifest section {header}"))
}

fn riffdb_dependencies(section: &str) -> Vec<&str> {
    section
        .lines()
        .filter_map(|line| line.split_once(" = ").map(|(name, _)| name))
        .filter(|name| name.starts_with("riffdb-"))
        .collect()
}

fn rust_sources_under(directory: &Path, sources: &mut Vec<(PathBuf, String)>) {
    for entry in fs::read_dir(directory).expect("read Rust source directory") {
        let path = entry.expect("Rust source entry").path();
        if path.is_dir() {
            rust_sources_under(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source = fs::read_to_string(&path).expect("read Rust source");
            sources.push((path, source));
        }
    }
}

#[test]
fn command_validation_is_sealed_until_candidate_identity_is_preserved_by_construction() {
    let production = COMMAND_VALIDATION_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(COMMAND_VALIDATION_SOURCE, |(source, _)| source);

    for forbidden in [
        "pub(crate)",
        "TransactionCurrentCommand",
        "CommandValidationDecision",
        "ValidatedCommandSemantics",
        "ValidatedZeroMutation",
        "ValidatedNonzeroCommand",
        "RejectedCommandCandidate",
        "read_transaction_current_command",
        "AffectedIndexEpochTargets",
        ".read_transaction_current(",
        ".plan_validated(",
        ".reject(",
    ] {
        assert!(
            !production.contains(forbidden),
            "sealed validation exposes an unreviewed pairing or progression path through {forbidden}"
        );
    }

    let attempt_production = COMMAND_ATTEMPT_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(COMMAND_ATTEMPT_SOURCE, |(source, _)| source);
    assert!(attempt_production.contains("pub(crate) struct EvaluatedCommandAttempt"));
    assert!(attempt_production.contains("Ok(ExecutionResult::CommitRequired(evaluated))"));
    assert!(attempt_production.contains("EvaluatedCommandAttempt {"));
    let evaluated_attempt_impl = attempt_production
        .split_once("impl EvaluatedCommandAttempt {")
        .and_then(|(_, rest)| {
            rest.split_once("\n}\n\nimpl fmt::Debug for EvaluatedCommandAttempt")
                .map(|(body, _)| body)
        })
        .expect("evaluated-attempt implementation");
    assert!(evaluated_attempt_impl.contains("fn into_parts(\n        self,"));
    assert!(!evaluated_attempt_impl.contains("pub(crate)"));
    for forbidden in [
        "pub(crate) const fn resolved_plan(&self)",
        "pub(crate) const fn normalized_input(&self)",
        "pub(crate) const fn logical_time(&self)",
        "pub(crate) const fn evaluated(&self)",
    ] {
        assert!(
            !attempt_production.contains(forbidden),
            "evaluated attempt exposes a sibling bypass through {forbidden}"
        );
    }

    let mut commit_sources = Vec::new();
    rust_sources_under(&crate_root().join("src"), &mut commit_sources);
    for (path, source) in &commit_sources {
        for forbidden in [
            ".begin_candidate(",
            ".recheck_admission(",
            ".read_transaction_current(",
            ".plan_validated(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} calls storage candidate progression outside the future reviewed wrapper via {forbidden}",
                path.display()
            );
        }
    }

    let value_source = production
        .split_once("struct TransactionCurrentValues {")
        .and_then(|(_, rest)| rest.split_once("\n}").map(|(body, _)| body))
        .expect("owned transaction-current value source");
    for forbidden in [
        "&'",
        "TransactionCurrentState",
        "EvaluatedCommandAttempt",
        "Awaiting",
    ] {
        assert!(!value_source.contains(forbidden));
    }
    let materializer = production
        .split_once("fn materialize_current_entity_record(")
        .and_then(|(_, rest)| {
            rest.split_once("\nfn validate_post_image_and_project")
                .map(|(body, _)| body)
        })
        .expect("current-record materializer");
    assert!(materializer.contains("record.schema_binding().matches_plan(plan)"));
    assert!(!materializer.contains("None if field.value_type().is_optional()"));

    let validation = production
        .split_once("fn validate_transaction_current_command_parts(")
        .and_then(|(_, rest)| {
            rest.split_once("\nfn validate_identity_positions_and_output")
                .map(|(body, _)| body)
        })
        .expect("pure validation core");
    let identity = validation
        .find("validate_identity_positions_and_output(")
        .expect("identity validation");
    let dependencies = validation
        .find("dependencies_from_current(current)")
        .expect("dependency reconstruction");
    let zero_branch = validation
        .find("if evaluated.mutations().is_empty()")
        .expect("zero-mutation branch");
    let coverage = validation
        .find("prove_mutation_coverage(")
        .expect("nonzero coverage");
    let evaluator = validation
        .find("evaluate_commit_checks(")
        .expect("commit-check evaluator");
    assert!(identity < dependencies);
    assert!(dependencies < zero_branch);
    assert!(zero_branch < coverage);
    assert!(coverage < evaluator);
    assert_eq!(production.matches("evaluate_commit_checks(").count(), 1);

    for required in [
        "plan.execution_class() != ExecutionClass::IdempotentMutation",
        "!request.range_targets().is_empty()",
        "!current.ranges().is_empty()",
        "&current_dependencies != evaluated.read_dependencies()",
        "CandidateValidationRejection::DependencyChanged",
        "validate_evaluated_output(",
        "validate_post_image_and_project(",
        "materialize_current_entity_record(",
        "struct TransactionCurrentValues",
        "input: CanonicalRecord",
        "bindings: Box<[PositionedBindingRecord]>",
        "roots: Box<[PositionedRootRecord]>",
        "record.schema_binding().matches_plan(plan)",
    ] {
        assert!(
            production.contains(required),
            "command validation is missing reviewed mechanism {required}"
        );
    }
    for forbidden in [
        "derive_input_command_facts",
        "execute_command(",
        "SnapshotReader",
        "StorageEngine",
        "ApplicationTransaction",
        "SystemTime",
        "Instant::now",
        "std::fs",
        "std::net",
        "getrandom",
        "rand::",
        "async fn",
        ".await",
        "assign_sequence",
        "reserve_sequence",
    ] {
        assert!(
            !production.contains(forbidden),
            "command validation gained forbidden authority through {forbidden}"
        );
    }

    assert!(LIB_SOURCE.contains("mod command_validation;"));
    assert!(!LIB_SOURCE.contains("pub mod command_validation"));
    assert!(!LIB_SOURCE.contains("pub use command_validation"));
}

#[test]
fn manifest_has_only_the_reviewed_dependencies_needed_by_commit_orchestration() {
    let production = manifest_section("[dependencies]\n");
    assert_eq!(
        riffdb_dependencies(production),
        vec![
            "riffdb-catalog",
            "riffdb-conflict",
            "riffdb-contract-ir",
            "riffdb-idempotency",
            "riffdb-invariant",
            "riffdb-policy",
            "riffdb-runtime",
            "riffdb-storage-api",
            "riffdb-types",
        ]
    );
    assert_eq!(
        riffdb_dependencies(manifest_section("[dev-dependencies]\n")),
        vec!["riffdb-contract-compiler", "riffdb-testkit"]
    );

    for forbidden in [
        "riffdb-service",
        "riffdb-proto",
        "riffdb-api-grpc",
        "riffdb-api-mcp",
        "riffdb-storage-memory",
        "riffdb-storage-redb",
        "riffdb-auth",
        "riffdb-contract-compiler",
        "riffdb-contract-syntax",
        "riffdb-testkit",
        "tonic",
        "rmcp",
        "redb",
        "getrandom",
        "rand",
        "uuid",
    ] {
        assert!(
            !production.contains(forbidden),
            "commit production manifest contains forbidden dependency {forbidden}"
        );
    }

    assert_eq!(
        production.lines().find(|line| line.starts_with("tokio =")),
        Some(
            "tokio = { version = \"=1.52.0\", default-features = false, features = [\"rt\", \"sync\"] }"
        ),
        "Tokio must retain the exact reviewed current-thread channel feature graph"
    );
}

#[test]
fn command_preparation_is_a_move_only_exact_join_without_ambient_authority() {
    let production_source = COMMAND_PREPARATION_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(COMMAND_PREPARATION_SOURCE, |(production, _)| production);
    for required in [
        "pub struct CommandCancellationHandle",
        "pub fn cancel(&self)",
        "pub struct CommandRequestControl",
        "deadline: Instant",
        "cancellation: CancellationToken",
        "pub fn new(deadline: Instant) -> (Self, CommandCancellationHandle)",
        "let cancellation = CancellationToken::new()",
        "pub struct CommandExecutionPreparation",
        "let Some(idempotency_field) = plan.idempotency_input()",
        "idempotency.matches_preparation(reference, &normalized_input, idempotency_field)",
        "input_facts.matches_command(plan, &normalized_input)",
        "authorization.lineage() != reference.contract_lineage()",
        "authorization.version() != reference.contract_version()",
        "authorization.command_id() != reference.command_id()",
        "authorization.database_id() != database_id",
        "authorization.environment() != environment",
        "plan.execution_class() != ExecutionClass::IdempotentMutation",
        "authorization.class() != CommandExecutionClass::Mutation",
        "authorization.partition().lineage() != reference.contract_lineage()",
        "authorization.partition().partition_key() != input_facts.partition_key()",
        "idempotency.matches_scope(",
        "authorization.actor().tenant_scope()",
        "authorization.actor().principal_id()",
        "pub(crate) fn into_parts(self) -> CommandExecutionPreparationParts",
        "pub struct CommandExecutionPreparationError",
        "_private: ()",
    ] {
        assert!(
            production_source.contains(required),
            "command preparation is missing reviewed mechanism {required}"
        );
    }

    for forbidden in [
        "Instant::now",
        "SystemTime",
        ".is_cancelled()",
        "Serialize",
        "Deserialize",
        "prost::",
        "StorageEngine",
        "AdmissionRepository",
        "CommitIntent",
        "pub fn deadline(",
        "pub fn cancellation(",
        "pub fn request_id(",
        "pub fn resolved_plan(",
        "pub fn normalized_input(",
        "pub enum CommandExecutionPreparationError",
        "IdempotencyBindingMismatch",
        "IdempotencyScopeMismatch",
        "InputFactsBindingMismatch",
        "AuthorizationBindingMismatch",
        "ExecutionClassMismatch",
        "PartitionBindingMismatch",
        "pub fn new(deadline: Instant, cancellation:",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "command preparation crosses reviewed boundary through {forbidden}"
        );
    }

    let control = production_source
        .split_once("pub struct CommandRequestControl {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("request-control body");
    let preparation = production_source
        .split_once("pub struct CommandExecutionPreparation {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("preparation body");
    let cancellation_handle = production_source
        .split_once("pub struct CommandCancellationHandle {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("cancellation-handle body");
    assert!(
        !cancellation_handle.contains("pub "),
        "cancellation-handle fields must remain private"
    );
    assert!(
        !control.contains("pub "),
        "request-control fields must remain private"
    );
    assert!(
        !preparation.contains("pub "),
        "preparation retained fields must remain private"
    );
    assert!(
        !LIB_SOURCE.contains("CancellationToken"),
        "commit must not re-export the conflict-manager cancellation type"
    );
}

#[test]
fn command_admission_is_private_move_only_and_orders_external_calls_exactly() {
    let production_source = COMMAND_ADMISSION_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(COMMAND_ADMISSION_SOURCE, |(production, _)| production);
    for required in [
        "pub(crate) enum CommandAdmissionResult",
        "Execute(Box<CommandExecutionCandidate>)",
        "Outcome(StoredOutcomeV1)",
        "ExecutionFailed(StoredExecutionFailedV1)",
        "pub(crate) enum CommandAdmissionError",
        "Recheck(IdempotencyRecheckError)",
        "AdmissionWrite(StorageError)",
        "struct CommandSnapshotRequestProof",
        "pub(crate) struct CommandExecutionCandidate",
        "resolved_plan: ResolvedExecutablePlan",
        "normalized_input: CanonicalRecord",
        "commit_context: PreEvaluationCommitContext",
        "raw_conflict_keys: Vec<ConflictKey>",
        "invocation_request_id: RequestId",
        "deadline: Instant",
        "cancellation: CancellationToken",
        "pub(crate) fn reduce_command_admission(",
        "IdempotencyRecheckExecutor::new(repository)",
        ".recheck(idempotency)",
        "lower_provenance_claims(&parts.authorization)",
        "StoredPendingAdmissionV1::new(",
        "AdmissionRequestV1::new(lookup_candidates, &context)",
        ".admit_or_resolve(request)",
        "AdmissionResultV1::Created(created) if created == pending",
        "raw.sort_unstable()",
        "raw.dedup()",
        "raw.is_empty() || raw.len() > MAX_COMMAND_CONFLICT_KEYS_V1",
        "hashes.sort_unstable()",
        "hashes.windows(2).any(|pair| pair[0] == pair[1])",
        "SnapshotRequest::new(",
        "Vec::new()",
    ] {
        assert!(
            production_source.contains(required),
            "command admission is missing reviewed mechanism {required}"
        );
    }

    assert!(LIB_SOURCE.contains("mod command_admission;"));
    assert!(!LIB_SOURCE.contains("pub mod command_admission"));
    assert!(!LIB_SOURCE.contains("pub use command_admission"));
    assert_eq!(production_source.matches(".recheck(").count(), 1);
    assert_eq!(production_source.matches(".admit_or_resolve(").count(), 1);

    let candidate_body = production_source
        .split_once("pub(crate) struct CommandExecutionCandidate {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("execution-candidate body");
    let snapshot_proof_body = production_source
        .split_once("struct CommandSnapshotRequestProof {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("snapshot-proof body");
    assert!(!candidate_body.contains("pub "));
    assert!(!snapshot_proof_body.contains("pub "));
    for forbidden in [
        "#[derive(Clone",
        "#[derive(Copy",
        "impl Clone for CommandExecutionCandidate",
        "impl Clone for CommandSnapshotRequestProof",
        "Serialize",
        "Deserialize",
        "prost::",
        "StorageEngine",
        "StorageWrite",
        "riffdb_runtime",
        "riffdb_contract_compiler",
        "SystemTime",
        "Instant::now",
        "async fn",
        ".await",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "command admission crosses reviewed boundary through {forbidden}"
        );
    }

    let vacant_body = production_source
        .split_once("fn admit_vacant(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn resume_pending("))
        .map(|(body, _)| body)
        .expect("bounded vacant-admission implementation");
    let lowering = vacant_body
        .find("lower_preparation(parts, normalized_input, conflict_hasher)")
        .expect("pure lowering");
    let clock = vacant_body.find("clock.now()").expect("one clock sample");
    let mutation = vacant_body
        .find(".admit_or_resolve(request)")
        .expect("one admission mutation");
    assert!(
        lowering < clock,
        "pure lowering must finish before clock sampling"
    );
    assert!(
        clock < mutation,
        "clock sampling must precede admission mutation"
    );
}

#[test]
fn command_attempt_owns_one_lease_around_synchronous_recheck_snapshot_and_runtime() {
    let production_source = COMMAND_ATTEMPT_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(COMMAND_ATTEMPT_SOURCE, |(production, _)| production);
    for required in [
        "pub(crate) const MAX_COMMAND_EVALUATION_ATTEMPTS_V1: usize = 3",
        "pub(crate) struct PendingCommandAttempts",
        "commit_context: PreEvaluationCommitContext",
        "lookup_candidates: IdempotencyLookupCandidatesV1",
        "completed_attempts: usize",
        "pub(crate) enum CommandAttemptResolution",
        "Evaluated(EvaluatedCommandAttempt)",
        "ExecutionFault(ExecutionFaultAttempt)",
        "OutcomeReplay(StoredOutcomeV1)",
        "ExecutionFailureReplay(StoredExecutionFailedV1)",
        "lease: MutationLease",
        "snapshot: ReadSnapshot",
        "pub(crate) async fn evaluate_next_command_attempt(",
        ".acquire_mut(",
        ".lookup_admission(state.lookup_candidates.clone())",
        ".read_snapshot(state.snapshot_request.clone())",
        "state.completed_attempts >= MAX_COMMAND_EVALUATION_ATTEMPTS_V1",
        "state.completed_attempts = state",
        "let execution = execute_command(",
        "pending.admission_request_id()",
        "pending.actor().clone()",
        "pending.logical_time()",
        "pending.partition_key().clone()",
        "if pending == *state.commit_context.pending()",
        "outcome_matches_context(&outcome, &state.commit_context)",
        "if failure.pending() == state.commit_context.pending()",
        "ExecutionFailureCode::ArithmeticFault",
        "ExecutionFailureCode::ResourceLimit",
        "Ok(ExecutionResult::ReadOnly(_)) | Err(ExecutionFault::Integrity)",
    ] {
        assert!(
            production_source.contains(required),
            "command attempt is missing reviewed mechanism {required}"
        );
    }

    assert!(LIB_SOURCE.contains("mod command_attempt;"));
    assert!(!LIB_SOURCE.contains("pub mod command_attempt"));
    assert!(!LIB_SOURCE.contains("pub use command_attempt"));
    assert_eq!(
        production_source.matches(".await").count(),
        1,
        "capability acquisition must remain the attempt's only await point"
    );
    let after_acquisition = production_source
        .split_once(".await")
        .map(|(_, tail)| tail)
        .expect("one acquisition await");
    assert!(
        !after_acquisition.contains(".await"),
        "no await is permitted while a mutation lease may be owned"
    );

    let context = production_source
        .split_once("fn transaction_context(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn check_request_control("))
        .map(|(body, _)| body)
        .expect("bounded transaction-context lowering");
    assert!(
        !context.contains("invocation_request_id"),
        "runtime context must use the original admitted request identity"
    );

    for forbidden in [
        "pub mod command_attempt",
        "pub use command_attempt",
        "StorageEngine",
        "StorageWrite",
        "CommitIntent",
        "ProvenanceId",
        "ProvenanceIdSource",
        "riffdb_contract_compiler",
        "SystemTime",
        "tokio::",
        "spawn(",
        "sleep(",
        "std::net",
        "std::fs",
        "begin_command",
        "assign_sequence",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "command attempt crosses reviewed boundary through {forbidden}"
        );
    }
}

#[test]
fn reviewed_tokio_owner_and_lock_graph_are_frozen() {
    assert_eq!(
        production_dependency_owners("tokio"),
        ["riffdb-commit"],
        "a new production Tokio owner requires dependency and feature-unification review"
    );
    for exact_entry in [
        "name = \"tokio\"\nversion = \"1.52.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"a91135f59b1cbf38c91e73cf3386fca9bb77915c45ce2771460c9d92f0f3d776\"\ndependencies = [\n \"pin-project-lite\",\n]",
        "name = \"pin-project-lite\"\nversion = \"0.2.17\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"a89322df9ebe1c1578d689c92318e070967d1042b512afbe49518723f4e6d5cd\"",
    ] {
        assert!(
            LOCKFILE.contains(exact_entry),
            "reviewed coordinator dependency lock entry changed: {exact_entry}"
        );
    }
}

#[test]
fn this_slice_has_no_concrete_clock_entropy_transport_or_engine_authority() {
    let sources = [
        LIB_SOURCE,
        AUDIT_SOURCE,
        CLOCK_SOURCE,
        INITIALIZATION_SOURCE,
        OUTCOME_SOURCE,
        PROVENANCE_SOURCE,
    ]
    .join("\n");

    for forbidden in [
        "std::time::",
        "SystemTime",
        "UNIX_EPOCH",
        "getrandom::",
        "rand::",
        "tokio::",
        "tonic::",
        "rmcp::",
        "redb::",
        "riffdb_service",
        "riffdb_proto",
        "riffdb_storage_memory",
        "riffdb_storage_redb",
    ] {
        assert!(
            !sources.contains(forbidden),
            "commit source contains forbidden authority {forbidden}"
        );
    }
}

#[test]
fn audit_view_is_borrowed_and_has_no_durable_or_policy_authority() {
    let production_source = AUDIT_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(AUDIT_SOURCE, |(production, _)| production);

    assert!(production_source.contains("pub trait AdministrationAuditInputView: Send + Sync"));
    let trait_body = production_source
        .split_once("pub trait AdministrationAuditInputView: Send + Sync {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("audit view trait body");
    assert_eq!(
        trait_body.matches("    fn ").count(),
        11,
        "audit view must expose exactly the approved borrowed fields"
    );
    assert_eq!(
        production_source
            .matches("impl AdministrationAuditInputView for")
            .count(),
        0,
        "commit must not own the concrete service audit input"
    );
    for signature in [
        "fn request_id(&self) -> &RequestId",
        "fn operation(&self) -> &ServiceOperationV1",
        "fn phase(&self) -> &ServiceAuditPhaseV1",
        "fn principal_id(&self) -> &ActorId",
        "fn actor_kind(&self) -> &ActorKind",
        "fn capability_id(&self) -> &CapabilityId",
        "fn capability_revision(&self) -> &NonZeroU64",
        "fn ingress(&self) -> &ServiceIngressKindV1",
        "fn targets(&self) -> &ServiceAuditTargetsV1",
        "fn approval_id(&self) -> Option<&ApprovalId>",
        "fn link(&self) -> &ServiceAuditLinkV1",
    ] {
        assert!(
            production_source.contains(signature),
            "audit view is missing borrowed field {signature}"
        );
    }

    for forbidden in [
        "fn administration_sequence(&self)",
        "fn audit_record_sequence(&self)",
        "fn assigned_sequence(&self)",
        "Timestamp",
        "ServiceAuditAppendIntentV1",
        "ServiceAuditAppendRepository",
        "append_service_audit",
        "StorageEngine",
        "StorageWrite",
        "PolicyDecision",
        "AuthorizationDecision",
        "RawCredential",
        "riffdb_service",
        "serde::",
        "prost::",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "audit interface crosses forbidden authority through {forbidden}"
        );
    }
}

#[test]
fn coordinator_actor_uses_only_the_reviewed_current_thread_channel_surface() {
    let production_source = AUDIT_EXECUTOR_SOURCE
        .split_once("#[cfg(test)]\nmod tests {")
        .map_or(AUDIT_EXECUTOR_SOURCE, |(production, _)| production);

    for required in [
        "enum CoordinatorMessage",
        "runtime::Builder::new_current_thread()",
        "mpsc::channel(channel_capacity)",
        ".try_reserve_owned()",
        ".reserve_owned()",
        "permit.send(CoordinatorMessage::AdministrationAudit",
        "permit.send(CoordinatorMessage::Shutdown)",
        "self.receiver.close()",
        ".store(LIFECYCLE_FENCED, Ordering::Release)",
        "thread::Builder::new()",
    ] {
        assert!(
            production_source.contains(required),
            "coordinator actor is missing reviewed mechanism {required}"
        );
    }
    for forbidden in [
        "new_multi_thread",
        "tokio::spawn",
        "tokio::time",
        "tokio::net",
        "tokio::fs",
        "tokio::signal",
        "SystemTime",
        "Instant",
        "getrandom",
        "rand::",
        "redb::",
        "select!",
        "join!",
        "spawn!",
        "std::sync::Mutex",
        "std::sync::RwLock",
        "ServiceAuditAppendRepository for",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "coordinator actor crosses reviewed boundary through {forbidden}"
        );
    }
    assert_eq!(
        production_source.matches(".append_service_audit(").count(),
        1,
        "the private synchronous audit driver must own the sole append call"
    );

    let shutdown_publication = production_source
        .split_once("fn initiate_shutdown_after_publication")
        .expect("shutdown publication implementation")
        .1
        .split_once("impl fmt::Debug for RunningCommandCoordinator")
        .expect("bounded shutdown publication implementation")
        .0;
    assert!(
        shutdown_publication
            .find(".compare_exchange(")
            .expect("draining-state publication")
            < shutdown_publication
                .find("submission_gate.close()")
                .expect("shutdown gate close")
    );
    let fence_publication = production_source
        .split_once("fn execute_audit(")
        .expect("audit execution implementation")
        .1
        .split_once("async fn reject_remaining_after_fence")
        .expect("bounded audit execution implementation")
        .0;
    assert!(
        fence_publication
            .find(".store(LIFECYCLE_FENCED, Ordering::Release)")
            .expect("fenced-state publication")
            < fence_publication
                .find("submission_gate.close()")
                .expect("fenced gate close")
    );

    let drop_body = production_source
        .split_once("impl Drop for RunningCommandCoordinator {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("running coordinator Drop implementation");
    assert!(
        !drop_body.contains(".join()"),
        "coordinator Drop must initiate shutdown without blocking on a join"
    );
}

#[test]
fn bootstrap_audit_proof_is_sealed_and_nonserializable() {
    let production_source = AUDIT_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(AUDIT_SOURCE, |(production, _)| production);

    assert!(production_source.contains("pub struct BootstrapCompoundAuditProof"));
    assert!(production_source.contains("_private: BootstrapCompoundAuditProofSeal"));
    for forbidden in [
        "derive(Clone",
        "derive(Copy",
        "derive(Default",
        "impl Default for BootstrapCompoundAuditProof",
        "Serialize",
        "Deserialize",
        "Message",
        "Encode",
        "Decode",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "bootstrap proof gains forbidden capability through {forbidden}"
        );
    }
}

#[test]
fn initialization_transition_has_one_private_call_site_and_no_handle_escape() {
    let production_source = INITIALIZATION_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(INITIALIZATION_SOURCE, |(production, _)| production);
    assert_eq!(
        production_source.matches(".initialize_database(").count(),
        1
    );
    assert!(production_source.contains("pub fn probe(self)"));
    assert!(production_source.contains("Existing(InitializedDatabase<Storage>)"));
    assert!(production_source.contains("Storage: StructuralEvidenceOpen"));
    assert_eq!(
        production_source
            .matches("pub fn begin_structural_evidence(")
            .count(),
        1
    );
    for forbidden in [
        "impl DatabaseInitializationPort for",
        "into_inner",
        "into_storage",
        "storage_mut",
        "pub fn storage",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "initialization wrapper leaks or duplicates storage authority through {forbidden}"
        );
    }
}

#[test]
fn response_wrapper_does_not_introduce_a_persisted_replay_record() {
    assert!(OUTCOME_SOURCE.contains("stored_outcome: StoredOutcomeV1"));
    assert!(OUTCOME_SOURCE.contains("CommittedOutcomeDisposition"));
    for forbidden in [
        "StoredReplay",
        "StoredCommittedOutcome",
        "StoredEnvelope",
        "riffdb_proto",
        "prost::",
        "encode_to_vec",
        "decode(",
    ] {
        assert!(
            !OUTCOME_SOURCE.contains(forbidden),
            "response wrapper crosses a durable-format boundary through {forbidden}"
        );
    }
}
