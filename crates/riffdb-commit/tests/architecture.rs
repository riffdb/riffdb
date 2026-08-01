//! Dependency and authority checks for the commit orchestration boundary.

use std::{fs, path::PathBuf};

const LOCKFILE: &str = include_str!("../../../Cargo.lock");
const MANIFEST: &str = include_str!("../Cargo.toml");
const AUDIT_SOURCE: &str = include_str!("../src/audit.rs");
const AUDIT_EXECUTOR_SOURCE: &str = include_str!("../src/audit_executor.rs");
const COMMAND_ADMISSION_SOURCE: &str = include_str!("../src/command_admission.rs");
const COMMAND_ATTEMPT_SOURCE: &str = include_str!("../src/command_attempt.rs");
const COMMAND_EXECUTION_SOURCE: &str = include_str!("../src/command_execution.rs");
const COMMAND_INDEX_SOURCE: &str = include_str!("../src/command_index.rs");
const COMMAND_RECORDS_SOURCE: &str = include_str!("../src/command_records.rs");
const COMMAND_VALIDATION_SOURCE: &str = include_str!("../src/command_validation.rs");
const COMMAND_PREPARATION_SOURCE: &str = include_str!("../src/command_preparation.rs");
const LIB_SOURCE: &str = include_str!("../src/lib.rs");
const CLOCK_SOURCE: &str = include_str!("../src/clock.rs");
const INITIALIZATION_SOURCE: &str = include_str!("../src/initialization.rs");
const NOTIFICATION_SOURCE: &str = include_str!("../src/notification.rs");
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

fn production_source(source: &str) -> &str {
    source
        .split_once("\n#[cfg(test)]\nmod tests")
        .map_or(source, |(production, _)| production)
}

fn braced_item_body<'a>(source: &'a str, declaration: &str) -> &'a str {
    let remainder = source
        .split_once(declaration)
        .unwrap_or_else(|| panic!("missing declaration {declaration}"))
        .1;
    let mut brace_depth = 1_usize;

    for (offset, character) in remainder.char_indices() {
        match character {
            '{' => brace_depth += 1,
            '}' => {
                brace_depth -= 1;
                if brace_depth == 0 {
                    return &remainder[..offset];
                }
            }
            _ => {}
        }
    }

    panic!("unterminated declaration {declaration}");
}

fn top_level_enum_variant_names(source: &str, declaration: &str) -> Vec<String> {
    let body = braced_item_body(source, declaration);
    let mut variants = Vec::new();
    let mut segment_start = 0;
    let mut delimiter_depth = 0_usize;

    for (offset, character) in body.char_indices() {
        match character {
            '(' | '[' | '{' => delimiter_depth += 1,
            ')' | ']' | '}' => delimiter_depth -= 1,
            ',' if delimiter_depth == 0 => {
                let segment = &body[segment_start..offset];
                let declaration = segment
                    .lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty() && !line.starts_with("///"))
                    .expect("enum variant declaration");
                let name = declaration
                    .chars()
                    .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
                    .collect::<String>();
                assert!(!name.is_empty(), "enum variant name");
                variants.push(name);
                segment_start = offset + character.len_utf8();
            }
            _ => {}
        }
    }

    assert!(
        body[segment_start..].trim().is_empty(),
        "enum variants must retain trailing commas"
    );
    variants
}

#[test]
fn command_validation_seals_one_exact_attempt_before_index_or_record_authority() {
    let production = production_source(COMMAND_VALIDATION_SOURCE);
    let attempt_production = COMMAND_ATTEMPT_SOURCE
        .split_once("\n#[cfg(test)]\nimpl EvaluatedCommandAttempt")
        .map_or(COMMAND_ATTEMPT_SOURCE, |(production, _)| production);

    assert!(attempt_production.contains("pub(crate) struct EvaluatedCommandAttempt"));
    assert!(attempt_production.contains("fn has_exact_semantic_join(&self) -> bool"));
    assert!(attempt_production.contains("snapshot_matches_request(&self.state.snapshot_request"));
    assert!(
        attempt_production
            .contains("snapshot.validation_request() == *self.evaluated.validation_request()")
    );
    assert!(
        attempt_production
            .contains("snapshot.read_dependencies() == self.evaluated.read_dependencies()")
    );
    assert!(
        !attempt_production
            .contains("EvaluatedCommandAttempt {\n    /// Consumes the attempt into")
    );
    assert!(!attempt_production.contains("pub(super) fn into_parts("));
    assert!(!attempt_production.contains("into_retry_state"));
    for required in [
        "pub(super) struct ProvenanceBoundCommandAttempt",
        "attempt: EvaluatedCommandAttempt",
        "intent: CommitIntent",
        "pub(super) fn bind_provenance(",
        "CommitIntent::new(",
        "pub(super) fn storage_intent(&self) -> Box<CommitIntent>",
        "Box::new(self.intent.clone())",
        "self.intent.evaluated() == self.attempt.evaluated()",
        "pub(super) fn reject_storage_and_rollback<C>(",
        "let abandoned = candidate.reject(reason)",
        "let (prior, storage_intent) = abandoned.into_parts()",
        "let intent_matches = *storage_intent == self.intent",
        "drop(prior)",
        "drop(storage_intent)",
        "self.finish_after_candidate_rollback(intent_matches, reason)",
        "pub(super) enum RolledBackCandidateDisposition",
        "CandidateValidationRejection::CommitCheckArithmeticFault",
        "ExecutionFaultAttempt::Arithmetic",
        "RolledBackCandidateDisposition::Retry {",
        "state: Box::new(state)",
    ] {
        assert!(
            attempt_production.contains(required),
            "missing provenance-bound rollback mechanism {required}"
        );
    }
    assert!(attempt_production.contains("pub(crate) enum ExecutionFaultAttempt"));
    assert!(attempt_production.contains("Arithmetic {"));
    assert!(attempt_production.contains("ResourceLimit {"));
    assert!(!attempt_production.contains("code: ExecutionFailureCode"));

    for required in [
        "pub(super) struct CheckedCandidateSeal",
        "pub(super) struct CheckedValidatedCommand<C>",
        "attempt: ProvenanceBoundCommandAttempt",
        "current: MaterializedTransactionCurrentState",
        "pub(super) enum CommandCandidateChainStart<S>",
        "pub(super) struct BoundCommandCandidateStateRead<S>",
        "pub(super) fn begin_bound_command_candidate<P>(",
        "let empty = match port.begin_empty_batch()",
        "let candidate = match empty.begin_candidate(attempt.storage_intent())",
        "let rechecked = match candidate.recheck_admission()",
        "pub(super) enum TransactionCurrentAttemptDecision<C>",
        "Ready(CheckedTransactionCurrentAttempt<C>)",
        "DependencyChanged(CheckedDependencyChangedAttempt<C>)",
        "pub(super) struct CheckedTransactionCurrentAttempt<C>",
        "pub(super) fn read_transaction_current(",
        "let (candidate, current) = match state_read.read_transaction_current()",
        "dependencies != *attempt.evaluated().read_dependencies()",
        ".materialized_snapshot()",
        ".materialize_transaction_current(current)",
        "pub(super) fn validate_checked_transaction_current<C>(",
        "checked_current: CheckedTransactionCurrentAttempt<C>",
        "let CheckedTransactionCurrentAttempt {",
        "if !attempt.has_exact_semantic_join()",
        "CheckedCandidateSeal::after_successful_validation()",
        "pub(super) struct CheckedCandidateRejection<C>",
        "self.attempt\n            .reject_storage_and_rollback(self.candidate, self.reason)",
        "pub(super) fn plan_validated(",
        "let candidate = candidate.plan_validated(affected_targets)",
    ] {
        assert!(
            production.contains(required),
            "missing checked-attempt mechanism {required}"
        );
    }
    assert!(
        !production.contains("pub(super) fn recheck_transaction_current_attempt("),
        "no detached attempt/current recheck entrypoint may exist"
    );
    for sole_call in [
        "port.begin_empty_batch()",
        "empty.begin_candidate(attempt.storage_intent())",
        "candidate.recheck_admission()",
        "candidate.plan_validated(affected_targets)",
        "state_read.read_transaction_current()",
    ] {
        assert_eq!(
            production.matches(sole_call).count(),
            1,
            "checked storage progression must retain one reviewed call site: {sole_call}"
        );
    }
    let checked_entry = production
        .split_once("pub(super) fn read_transaction_current(")
        .and_then(|(_, rest)| {
            rest.split_once("\n    }\n}\n\n/// Closed result of validating")
                .map(|(body, _)| body)
        })
        .expect("bound storage-current checked entrypoint");
    let dependencies = checked_entry
        .find("dependencies_from_current(&current)")
        .expect("dependency comparison");
    let raw_recheck = checked_entry
        .find(".materialize_transaction_current(current)")
        .expect("exact attempt raw recheck");
    assert!(dependencies < raw_recheck);
    let bound_fields = production
        .split_once("pub(super) struct BoundCommandCandidateStateRead<S> {")
        .and_then(|(_, rest)| rest.split_once("\n}").map(|(fields, _)| fields))
        .expect("bound candidate fields");
    assert!(
        bound_fields.find("state_read: S").expect("storage state")
            < bound_fields
                .find("attempt: ProvenanceBoundCommandAttempt")
                .expect("attempt lease"),
        "storage state must drop before the attempt releases its conflict lease"
    );
    let checked_fields = production
        .split_once("pub(super) struct CheckedTransactionCurrentAttempt<C> {")
        .and_then(|(_, rest)| rest.split_once("\n}").map(|(fields, _)| fields))
        .expect("checked current fields");
    assert!(
        checked_fields
            .find("candidate: C")
            .expect("storage candidate")
            < checked_fields
                .find("attempt: ProvenanceBoundCommandAttempt")
                .expect("attempt lease")
    );
    let seal_impl = production
        .split_once("impl CheckedCandidateSeal {")
        .and_then(|(_, rest)| {
            rest.split_once("\n}\n\n/// Exact validated values")
                .map(|(body, _)| body)
        })
        .expect("checked seal implementation");
    assert_eq!(seal_impl.matches("Self { _private: () }").count(), 1);
    let rejection = attempt_production
        .split_once("pub(super) fn reject_storage_and_rollback<C>(")
        .and_then(|(_, rest)| {
            rest.split_once("\n    fn finish_after_candidate_rollback")
                .map(|(body, _)| body)
        })
        .expect("provenance-bound rejection rollback");
    let drop_prior = rejection
        .find("drop(prior)")
        .expect("drop prior transaction");
    let drop_intent = rejection
        .find("drop(storage_intent)")
        .expect("drop storage intent");
    let recover_retry = rejection
        .find("self.finish_after_candidate_rollback")
        .expect("select rolled-back disposition");
    assert!(drop_prior < drop_intent);
    assert!(drop_intent < recover_retry);

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
    let zero_branch = validation
        .find("if evaluated.mutations().is_empty()")
        .expect("zero-mutation branch");
    let coverage = validation
        .find("prove_mutation_coverage(")
        .expect("nonzero coverage");
    let evaluator = validation
        .find("evaluate_commit_checks(")
        .expect("commit-check evaluator");
    assert!(identity < zero_branch);
    assert!(zero_branch < coverage);
    assert!(coverage < evaluator);
    assert_eq!(production.matches("evaluate_commit_checks(").count(), 1);

    for required in [
        "plan.execution_class() != ExecutionClass::IdempotentMutation",
        "!request.range_targets().is_empty()",
        "!current.ranges().is_empty()",
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
fn command_index_derivation_preserves_the_sealed_storage_progression_chain() {
    let production = production_source(COMMAND_INDEX_SOURCE);

    for required in [
        "pub(super) struct CheckedCommitCandidate",
        "authority: CheckedAttemptAuthority",
        "struct CheckedAttemptAuthority<S>(Box<CheckedValidatedCommand<S>>)",
        "attempt.has_exact_semantic_join() && intent == attempt.commit_intent()",
        "pub(super) const fn exact_intent(&self) -> &CommitIntent",
        "pub(super) fn derive_checked_command_indexes<C>(",
        "checked: CheckedValidatedCommand<C>",
        "checked.mutation_positions()",
        "let checked = checked.plan_validated(derived.affected_targets.clone())",
        "pub(super) fn read_affected_epoch_current(",
        "match checked.read_affected_epoch_current()",
        "pub(super) fn reserve_capacity(self)",
        "fn prepare_sequence_free_write_set(",
        "let shape = match ValidatedCommandWriteSetShapeV1::new(",
        "fn classify_sequence_free_write_set_sizing(",
        "Ok(EncodedWriteSetUpperBoundResultV1::Fits(bound))",
        "EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_codec_origin)",
        "Err(_) => SequenceFreeWriteSetSizing::Integrity",
        "return CheckedReserveDecision::CapacityUnavailable",
        "match checked.reserve_capacity(write_plan)",
        "checked.capacity_reserved().write_plan() == &expected_write_plan",
        "pub(super) fn assign_sequence(self)",
        "match checked.assign_sequence()",
        "checked.sequence_assigned().write_plan() == &expected_write_plan",
        "pub(super) fn stage(",
        "match checked.stage(records)",
        "fn derive_grammar_v1_indexes(",
        "evaluated.mutations().is_empty()",
        "IndexEntryMutationV1::Delete(old_key)",
        "IndexEntryMutationV1::Put(",
        "CanonicalRecord::new(Vec::new())",
        "encode_index(&new_values",
        "encode_index_prefix(values)",
        "IndexRangePrefixBuilder::new(index.id())",
        "storage.index_id() != ir_prefix.index_id() || storage.as_bytes() != ir_prefix.as_bytes()",
        "PartitionIndexTarget::new(partition.clone(), index.id())",
        "MAX_INDEX_DELTAS",
        "MAX_AFFECTED_INDEX_EPOCH_TARGETS",
        "MAX_VALIDATION_TARGETS",
        "MAX_READ_SNAPSHOT_BYTES",
        "CommandIndexError::internal_defect()",
    ] {
        assert!(
            production.contains(required),
            "sealed command-index derivation is missing {required}"
        );
    }

    let semantic_validation = production
        .find("let shape = match ValidatedCommandWriteSetShapeV1::new(")
        .expect("sequence-free semantic shape validation");
    let codec_sizing = production
        .find("match classify_sequence_free_write_set_sizing(command_write_set_upper_bound_v1(")
        .expect("sequence-free codec sizing");
    let frozen_plan = production
        .find("CommandWriteSetPlanV1::from_validated_shape(shape, bound)")
        .expect("frozen write plan construction");
    assert!(semantic_validation < codec_sizing && codec_sizing < frozen_plan);
    let sequence_free_preparation = production
        .find("let write_plan = match prepare_sequence_free_write_set(")
        .expect("sequence-free write-plan preparation");
    let storage_reservation = production
        .find("match checked.reserve_capacity(write_plan)")
        .expect("storage capacity reservation");
    assert!(sequence_free_preparation < storage_reservation);
    let capacity_branch = production
        .split_once("SequenceFreeWriteSetPreparation::CapacityUnavailable => {")
        .and_then(|(_, remainder)| {
            remainder.split_once("SequenceFreeWriteSetPreparation::Integrity => {")
        })
        .map(|(body, _)| body)
        .expect("origin-specific sequence-free capacity branch");
    assert!(capacity_branch.contains("drop(checked);"));
    assert!(capacity_branch.contains("return CheckedReserveDecision::CapacityUnavailable;"));
    assert!(!capacity_branch.contains("checked.reserve_capacity"));
    assert!(!production.contains("fn exact_mutation_positions("));
    for sole_call in [
        "checked.plan_validated(derived.affected_targets.clone())",
        "checked.read_affected_epoch_current()",
        "checked.reserve_capacity(write_plan)",
        "checked.assign_sequence()",
        "checked.stage(records)",
    ] {
        assert_eq!(
            production.matches(sole_call).count(),
            1,
            "sealed index carrier must retain one reviewed progression call: {sole_call}"
        );
    }
    for forbidden in [
        "pub(crate)",
        "pub fn ",
        "pub struct ",
        "pub enum ",
        "ApplicationTransaction",
        "StorageEngine",
        "SnapshotReader",
        ".begin_candidate(",
        ".recheck_admission(",
        ".read_transaction_current(",
        ".commit(",
        "async fn",
        ".await;",
        "ResourceLimit",
    ] {
        assert!(
            !production.contains(forbidden),
            "sealed command-index derivation gained forbidden authority through {forbidden}"
        );
    }
    assert!(LIB_SOURCE.contains("mod command_index;"));
    assert!(!LIB_SOURCE.contains("pub mod command_index"));
    assert!(!LIB_SOURCE.contains("pub use command_index"));
}

#[test]
fn production_group_collection_never_parks_on_a_submillisecond_tokio_timer() {
    let production = production_source(AUDIT_EXECUTOR_SOURCE);

    for forbidden in [
        "COMMAND_GROUP_WINDOW",
        "IDEMPOTENCY_INSPECTION_GROUP_WINDOW",
        "tokio::time::timeout_at",
        ".enable_time()",
        ".enable_all()",
        "MAX_GROUP_WAIT_MICROSECONDS",
        "tokio::time::sleep",
        "tokio::time::timeout",
        "tokio::time::interval",
    ] {
        assert!(
            !production.contains(forbidden),
            "production grouping retained timer-wheel mechanism {forbidden}"
        );
    }
    for required in [
        "receiver.blocking_recv()",
        "receiver.try_recv()",
        "CommitGroupDispatchReason::QueueDrained",
    ] {
        assert!(
            production.contains(required),
            "timer-free grouping is missing reviewed mechanism {required}"
        );
    }
}

#[test]
fn durable_graph_construction_requires_checked_input_and_retains_attempt_through_commit() {
    let production = production_source(COMMAND_RECORDS_SOURCE);
    for required in [
        "struct CheckedRecordGraphInput",
        "fn from_assigned_candidate<S>(",
        "candidate: &CheckedCommitCandidate<S>",
        "if !candidate.matches_intent(write_plan.intent())",
        "fn build_atomic_command_record_set(\n    input: &CheckedRecordGraphInput,",
        "pub(super) fn build_and_stage_checked_candidate<S>",
        "candidate: CheckedCommitCandidate<S>",
        "let records = match build_atomic_command_record_set(&input, durability_mode)",
        "drop(candidate)",
        "match candidate.stage(records)",
        "S::Prior: EmptyCommandBatch",
        "CheckedCandidateStage::StorageFailure(error)\n            if error.kind() == StorageErrorKind::CommitStatusUnknown",
        "CheckedCommandStageError::InternalDefect(",
        "pub(super) struct CheckedStagedCommand",
        "candidate: RetainedCheckedCommitCandidate",
        "expected_outcome: StoredOutcomeV1",
        "pub(super) enum CheckedCommandCommitResult",
        "StatusUnknown(Box<UncertainCommandCommit>)",
        "pub(super) struct UncertainCommandCommit",
        "lookup_candidates: IdempotencyLookupCandidatesV1",
        "pub(super) fn commit(\n        self,",
        "pub(super) fn commit_group(",
        ".commit_with_service_audit_transitions(durability_mode, audits)",
        "None => staged.commit(durability_mode)",
        "finish_checked_commit(",
        "batch.outcomes() == std::slice::from_ref(&expected_outcome)",
        "CheckedCommandCommitResult::StatusUnknown(Box::new(UncertainCommandCommit",
        "pub(super) fn resolve_uncertain_command_commit(",
        "repository.lookup_admission(uncertain.lookup_candidates.clone())",
        "ProvenNotCommitted(ProvenNonCommitCommand)",
        "uncertain: Box<UncertainCommandCommit>",
        "into_pending_after_proven_noncommit()",
        "if outcome == uncertain.expected_outcome",
        "StoredAdmissionStateV1::ExecutionFailed(failure)",
        "if failure.pending() == uncertain.candidate.exact_intent().pending()",
    ] {
        assert!(
            production.contains(required),
            "missing checked record-chain mechanism {required}"
        );
    }
    let builder = production
        .split_once("fn build_atomic_command_record_set(")
        .and_then(|(_, rest)| {
            rest.split_once("\nfn committed_entity(")
                .map(|(body, _)| body)
        })
        .expect("private record graph builder");
    assert!(!builder.starts_with("\n    assignment:"));
    assert!(!builder.starts_with("\n    write_plan:"));
    assert!(!production.contains("pub(crate) fn build_atomic_command_record_set"));
    assert!(!production.contains("pub(super) fn build_atomic_command_record_set"));
    assert!(!production.contains("ProvenanceIdSource"));
    assert_eq!(
        production
            .matches("CheckedCommandCommitResult::StatusUnknown(Box::new(")
            .count(),
        1,
        "only engine commit may construct uncertain command state"
    );
    let stage_transition = production
        .split_once("let (staged, candidate) = match candidate.stage(records)")
        .and_then(|(_, rest)| rest.split_once("\n    Ok((").map(|(body, _)| body))
        .expect("checked stage transition");
    assert!(stage_transition.contains("StorageErrorKind::CommitStatusUnknown"));
    assert!(stage_transition.contains("CheckedCommandStageError::InternalDefect"));
    assert!(!stage_transition.contains("CheckedCommandCommitResult::StatusUnknown"));
    let transition = production
        .split_once("pub(super) fn build_and_stage_checked_candidate")
        .and_then(|(_, rest)| {
            rest.split_once("\n/// Derives the complete successful-command graph")
                .map(|(body, _)| body)
        })
        .expect("checked build-and-stage transition");
    assert_eq!(transition.matches("candidate.stage(records)").count(), 1);
    assert!(!transition.contains("assigned:"));
    assert!(!transition.contains("assigned.stage("));
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
        vec![
            "riffdb-contract-compiler",
            "riffdb-storage-redb",
            "riffdb-testkit",
        ]
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
            "tokio = { version = \"=1.52.0\", default-features = false, features = [\"rt\", \"sync\", \"time\", \"macros\"] }"
        ),
        "Tokio must retain the exact reviewed current-thread channel and select! feature graph"
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
        "AdmissionRequestV1::new(lookup_candidates.clone(), &context)",
        "candidate(lowered, context, true, lookup_candidates)",
        "AdmissionResultV1::Resumed(existing)",
        ".rebind_durable_pending(existing)",
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
    assert_eq!(production_source.matches(".admit_or_resolve(").count(), 0);
    assert_eq!(
        production_source
            .matches(".admit_or_resolve_group(")
            .count(),
        0
    );

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
        .split_once("fn prepare_vacant(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn complete_vacant_admission("))
        .map(|(body, _)| body)
        .expect("bounded vacant-admission implementation");
    let lowering = vacant_body
        .find("lower_preparation(parts, normalized_input, conflict_hasher)")
        .expect("pure lowering");
    let clock = vacant_body.find("clock.now()").expect("one clock sample");
    let request = vacant_body
        .find("AdmissionRequestV1::new")
        .expect("one prepared admission request");
    assert!(
        lowering < clock,
        "pure lowering must finish before clock sampling"
    );
    assert!(
        clock < request,
        "clock sampling must precede admission request construction"
    );
    assert!(
        !production_source.contains("repository.admit_or_resolve_group(requests)"),
        "fresh terminal admission must not persist Pending state"
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
        "snapshot: MaterializedCommandSnapshot",
        "pub(crate) enum ResourceLimitFaultEvidence",
        "Runtime(MaterializedCommandSnapshot)",
        "Materialization(CommandSnapshotResourceLimitEvidence)",
        "pub(crate) enum ExecutionFaultAttempt",
        "Arithmetic {",
        "ResourceLimit {",
        "fn has_exact_semantic_join(&self) -> bool",
        "pub(crate) async fn evaluate_next_command_attempt(",
        "pub(crate) async fn acquire_command_attempt(",
        "pub(crate) fn evaluate_acquired_command_attempt(",
        ".acquire_mut(",
        ".lookup_admission(state.lookup_candidates.clone())",
        ".read_snapshot(state.snapshot_request.clone())",
        "if !snapshot_matches_request(&state.snapshot_request, &raw_snapshot)",
        ".materialize_command_snapshot(raw_snapshot)",
        "state.completed_attempts >= MAX_COMMAND_EVALUATION_ATTEMPTS_V1",
        "state.completed_attempts = state",
        "let execution = catch_unwind(AssertUnwindSafe(|| {",
        "execute_command(",
        ".map_err(|_| CommandAttemptError::EvaluationPanicked)?",
        "snapshot.resolved_plan().bundle().bundle()",
        "snapshot.snapshot()",
        "pending.admission_request_id()",
        "pending.actor().clone()",
        "pending.logical_time()",
        "pending.partition_key().clone()",
        "if !state.terminal_admission && pending == *state.commit_context.pending()",
        "outcome_matches_state(&outcome, &state)",
        "failure_matches_state(&failure, &state)",
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
    assert!(!production_source.contains("pub(super) fn into_parts("));
    assert!(!production_source.contains("code: ExecutionFailureCode"));
    let acquisition = production_source
        .split_once("pub(crate) async fn acquire_command_attempt(")
        .and_then(|(_, remainder)| {
            remainder.split_once("\n/// Rechecks, snapshots, and deterministically evaluates")
        })
        .map(|(body, _)| body)
        .expect("bounded acquisition phase");
    assert_eq!(acquisition.matches(".await").count(), 1);
    let after_acquisition = acquisition
        .split_once(".await")
        .map(|(_, tail)| tail)
        .expect("one acquisition await");
    assert!(
        !after_acquisition.contains(".await"),
        "no await is permitted while a mutation lease may be owned"
    );
    let evaluation = production_source
        .split_once("pub(crate) fn evaluate_acquired_command_attempt(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn transaction_context("))
        .map(|(body, _)| body)
        .expect("synchronous worker evaluation phase");
    assert!(!evaluation.contains(".await"));

    let context = production_source
        .split_once("fn transaction_context(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn snapshot_matches_request("))
        .map(|(body, _)| body)
        .expect("bounded transaction-context lowering");
    assert!(
        !context.contains("invocation_request_id"),
        "runtime context must use the original admitted request identity"
    );

    let target_check = production_source
        .find("if !snapshot_matches_request(&state.snapshot_request, &raw_snapshot)")
        .expect("exact adapter target check");
    let materialization = production_source
        .find(".materialize_command_snapshot(raw_snapshot)")
        .expect("catalog materialization");
    let attempt_counter = production_source
        .find("state.completed_attempts = state")
        .expect("accepted attempt-slot consumption");
    let runtime = production_source
        .find("let execution = catch_unwind(AssertUnwindSafe(|| {")
        .expect("deterministic runtime call");
    assert!(target_check < attempt_counter);
    assert!(attempt_counter < materialization);
    assert!(materialization < runtime);

    let lookup = production_source
        .find(".lookup_admission(state.lookup_candidates.clone())")
        .expect("exact Pending lookup");
    let replay = production_source[lookup..]
        .find("return Ok(CommandAttemptResolution::OutcomeReplay(outcome))")
        .map(|offset| lookup + offset)
        .expect("terminal replay precedence");
    let post_lookup_control = production_source[lookup..]
        .find("check_request_control(state.deadline, &state.cancellation)?;")
        .map(|offset| lookup + offset)
        .expect("post-Pending control point");
    let snapshot_read = production_source
        .find(".read_snapshot(state.snapshot_request.clone())")
        .expect("snapshot read");
    assert!(replay < post_lookup_control);
    assert!(post_lookup_control < snapshot_read);

    for forbidden in [
        "pub mod command_attempt",
        "pub use command_attempt",
        "StorageEngine",
        "StorageWrite",
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
fn command_driver_public_surface_is_closed_and_has_no_storage_or_transport_authority() {
    let production = production_source(COMMAND_EXECUTION_SOURCE);
    assert_eq!(
        top_level_enum_variant_names(production, "pub enum CommandExecutionResult {"),
        [
            "Committed",
            "ExecutionFailed",
            "PreparationChanged",
            "InputMismatch",
        ]
    );
    assert_eq!(
        top_level_enum_variant_names(production, "pub enum CommandExecutionErrorKind {"),
        [
            "AuthorizationDenied",
            "Cancelled",
            "DeadlineExceeded",
            "RetryBudgetExhausted",
            "StorageUnavailable",
            "OutcomeUnknown",
            "InternalDefect",
            "CoordinatorStopped",
            "CoordinatorFenced",
        ]
    );

    let result = braced_item_body(production, "pub enum CommandExecutionResult {");
    assert!(result.contains("Committed(CommittedOutcome)"));
    assert!(result.contains("ExecutionFailed(ExecutionFailedOutcome)"));
    let error_kind = braced_item_body(production, "pub enum CommandExecutionErrorKind {");
    let error = braced_item_body(production, "pub struct CommandExecutionError {");
    assert!(
        !error
            .lines()
            .any(|line| line.trim_start().starts_with("pub ")),
        "executor error detail must remain private"
    );
    let error_impl = braced_item_body(production, "impl CommandExecutionError {");
    let public_error_methods = error_impl
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("pub "))
        .collect::<Vec<_>>();
    assert_eq!(
        public_error_methods,
        ["pub const fn kind(&self) -> CommandExecutionErrorKind {"]
    );

    let public_surface = [result, error_kind, error, public_error_methods[0]].join("\n");
    for forbidden in [
        "StoredExecutionFailedV1",
        "StoredOutcomeV1",
        "StorageError",
        "StorageErrorKind",
        "AdmissionRepository",
        "SnapshotReader",
        "ApplicationCommandTransactionPort",
        "ExecutionFailureTransitionPort",
        "StorageEngine",
        "StorageWrite",
        "tonic::",
        "rmcp::",
        "prost::",
        "redb::",
    ] {
        assert!(
            !public_surface.contains(forbidden),
            "public command result/error surface exposes forbidden authority {forbidden}"
        );
    }

    assert_eq!(
        top_level_enum_variant_names(production, "pub enum CoordinatorDurability {"),
        ["Sync", "Group"]
    );
    let durability = braced_item_body(production, "pub enum CoordinatorDurability {");
    let durability_impl = braced_item_body(production, "impl CoordinatorDurability {");
    let durability_derive = production
        .split_once("pub enum CoordinatorDurability {")
        .expect("coordinator durability declaration")
        .0
        .rsplit_once("#[derive(")
        .expect("coordinator durability derive")
        .1;
    assert!(!durability.contains("Memory"));
    assert!(!durability_impl.contains("DurabilityMode::Memory"));
    assert!(!durability_derive.contains("Default"));
    assert!(!production.contains("impl Default for CoordinatorDurability"));
}

#[test]
fn command_driver_fences_uncertain_admission_before_one_nonexecuting_recovery_read() {
    let production = production_source(COMMAND_EXECUTION_SOURCE);
    let driver = production
        .split_once("pub(super) async fn drive_command_execution<P>(")
        .and_then(|(_, remainder)| {
            remainder.split_once("\nasync fn drive_pending_command_attempts<P>(")
        })
        .map(|(body, _)| body)
        .expect("top-level command driver");

    for required_arm in [
        "Ok(CommandAdmissionResult::Execute(candidate))",
        "Ok(CommandAdmissionResult::Outcome(outcome))",
        "Ok(CommandAdmissionResult::ExecutionFailed(failure))",
        "Ok(CommandAdmissionResult::PreparationChanged)",
        "Ok(CommandAdmissionResult::InputMismatch)",
        "Err(CommandAdmissionError::AdmissionStatusUnknown(uncertain))",
        "Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Storage(error)))",
        "Err(CommandAdmissionError::AdmissionWrite(error))",
        "Err(CommandAdmissionError::Clock(error))",
        "Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Integrity(_)))",
        "Err(CommandAdmissionError::Integrity)",
    ] {
        assert!(
            driver.contains(required_arm),
            "top-level command admission is missing exhaustive arm {required_arm}"
        );
    }
    assert_eq!(driver.matches("reduce_command_admission(").count(), 1);

    let uncertain = driver
        .split_once("Err(CommandAdmissionError::AdmissionStatusUnknown(uncertain)) => {")
        .and_then(|(_, remainder)| {
            remainder.split_once(
                "\n        Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Storage(error)))",
            )
        })
        .map(|(body, _)| body)
        .expect("uncertain admission branch");
    let fence = uncertain
        .find("lifecycle.fence()")
        .expect("uncertain admission fence publication");
    let resolution = uncertain
        .find("resolve_uncertain_command_admission(port, *uncertain)")
        .expect("same-key uncertain admission read");
    assert!(fence < resolution);
    assert_eq!(
        uncertain
            .matches("resolve_uncertain_command_admission(port, *uncertain)")
            .count(),
        1
    );
    for resolution_arm in [
        "UncertainCommandAdmissionResolution::ProvenPending(candidate)",
        "UncertainCommandAdmissionResolution::Outcome(outcome)",
        "UncertainCommandAdmissionResolution::ExecutionFailed(failure)",
        "UncertainCommandAdmissionResolution::OutcomeUnknown(failure)",
        "UncertainCommandAdmissionResolution::Integrity",
    ] {
        assert!(
            uncertain.contains(resolution_arm),
            "uncertain admission is missing exhaustive resolution {resolution_arm}"
        );
    }
    assert!(!uncertain.contains("_ =>"));

    let proven_pending = uncertain
        .split_once("UncertainCommandAdmissionResolution::ProvenPending(candidate) => {")
        .and_then(|(_, remainder)| {
            remainder.split_once(
                "\n                UncertainCommandAdmissionResolution::Outcome(outcome)",
            )
        })
        .map(|(body, _)| body)
        .expect("proven pending resolution");
    assert!(proven_pending.contains("drop(candidate)"));
    assert!(proven_pending.contains("CommandExecutionErrorKind::StorageUnavailable"));
    for forbidden in [
        "evaluate_next_command_attempt",
        "PendingCommandAttempts::from_admission",
        "drive_pending_command_attempts",
        "continue_evaluated_command",
        "next_provenance_id",
        "bind_provenance",
    ] {
        assert!(
            !proven_pending.contains(forbidden),
            "fenced ProvenPending admission must not resume through {forbidden}"
        );
    }
}

#[test]
fn command_driver_fences_every_late_unknown_and_owns_one_bounded_retry_loop() {
    let production = production_source(COMMAND_EXECUTION_SOURCE);
    let evaluated = production
        .split_once("pub(super) fn continue_evaluated_command<P>(")
        .and_then(|(_, remainder)| {
            remainder
                .split_once("\n/// Revalidates and terminalizes one deterministic execution fault.")
        })
        .map(|(body, _)| body)
        .expect("evaluated-command continuation");
    let capacity_refusal =
        "resolve_checked_reserve_decision(indexed.reserve_capacity(), lifecycle)";
    assert!(evaluated.contains(capacity_refusal));
    assert!(
        evaluated
            .find(capacity_refusal)
            .expect("aggregate capacity refusal")
            < evaluated
                .find("let assigned = match reserved.assign_sequence()")
                .expect("sequence assignment")
    );
    let reserve_resolution = production
        .split_once("fn resolve_checked_reserve_decision<C>(")
        .and_then(|(_, remainder)| {
            remainder
                .split_once("\n/// Revalidates and terminalizes one deterministic execution fault.")
        })
        .map(|(body, _)| body)
        .expect("checked reserve-decision resolution");
    assert!(
        reserve_resolution
            .contains("CheckedReserveDecision::CapacityUnavailable => Err(capacity_unavailable())")
    );
    assert!(
        reserve_resolution
            .contains("| CheckedReserveDecision::Integrity => Err(internal_defect(lifecycle))")
    );
    let capacity_helper = production
        .split_once("fn capacity_unavailable() -> CommandDriverContinuation {")
        .and_then(|(_, remainder)| remainder.split_once("\nfn proven_storage_failure("))
        .map(|(body, _)| body)
        .expect("aggregate capacity helper");
    assert!(capacity_helper.contains("CommandExecutionErrorKind::StorageUnavailable"));
    for forbidden in [
        "lifecycle.stop()",
        "lifecycle.fence()",
        "internal_defect",
        "StorageError",
    ] {
        assert!(
            !capacity_helper.contains(forbidden),
            "aggregate capacity refusal must not cross {forbidden}"
        );
    }
    let uncertain_commit = evaluated
        .split_once("CheckedCommandCommitResult::StatusUnknown(uncertain) => {")
        .and_then(|(_, remainder)| {
            remainder.split_once("\n        CheckedCommandCommitResult::Integrity")
        })
        .map(|(body, _)| body)
        .expect("uncertain successful-command commit");
    assert_eq!(
        uncertain_commit
            .matches("resolve_uncertain_command_commit(port, uncertain)")
            .count(),
        1
    );
    assert!(
        uncertain_commit
            .find("lifecycle.fence()")
            .expect("successful-command fence publication")
            < uncertain_commit
                .find("resolve_uncertain_command_commit(port, uncertain)")
                .expect("successful-command same-key read")
    );

    let execution_fault = production
        .split_once("pub(super) fn continue_execution_fault<P>(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn after_rollback<P>("))
        .map(|(body, _)| body)
        .expect("execution-fault continuation");
    let uncertain_failure = execution_fault
        .split_once("ExecutionFailureTerminalizeResult::StatusUnknown(uncertain) => {")
        .and_then(|(_, remainder)| {
            remainder.split_once("\n        ExecutionFailureTerminalizeResult::Integrity")
        })
        .map(|(body, _)| body)
        .expect("uncertain execution-failure terminalization");
    assert_eq!(
        uncertain_failure
            .matches("resolve_uncertain_execution_failure(port, uncertain)")
            .count(),
        1
    );
    assert!(
        uncertain_failure
            .find("lifecycle.fence()")
            .expect("execution-failure fence publication")
            < uncertain_failure
                .find("resolve_uncertain_execution_failure(port, uncertain)")
                .expect("execution-failure same-key read")
    );
    assert_eq!(
        production
            .matches("::StatusUnknown(uncertain) => {")
            .count(),
        3,
        "single and grouped completion plus execution-failure transitions may become status-unknown"
    );

    let attempt_driver = production
        .split_once("async fn drive_pending_command_attempts<P>(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn terminal_continuation("))
        .map(|(body, _)| body)
        .expect("bounded attempt driver");
    assert_eq!(attempt_driver.matches("loop {").count(), 1);
    assert_eq!(
        attempt_driver
            .matches("evaluate_next_command_attempt(")
            .count(),
        1
    );
    assert_eq!(
        attempt_driver
            .matches("continue_evaluated_command(")
            .count(),
        1
    );
    assert_eq!(
        attempt_driver.matches("continue_execution_fault(").count(),
        1
    );
    assert!(attempt_driver.contains("CommandDriverContinuation::Retry(retry) => state = *retry"));
    assert_eq!(attempt_driver.matches(".await").count(), 1);

    let attempt_source = production_source(COMMAND_ATTEMPT_SOURCE);
    assert!(attempt_source.contains("MAX_COMMAND_EVALUATION_ATTEMPTS_V1: usize = 3"));
    assert!(
        attempt_source.contains("state.completed_attempts >= MAX_COMMAND_EVALUATION_ATTEMPTS_V1")
    );
}

#[test]
fn command_driver_maps_caught_runtime_panics_to_the_public_internal_defect() {
    let attempt_source = production_source(COMMAND_ATTEMPT_SOURCE);
    assert!(attempt_source.contains("let execution = catch_unwind(AssertUnwindSafe(|| {"));
    assert!(attempt_source.contains(".map_err(|_| CommandAttemptError::EvaluationPanicked)?"));
    assert!(!attempt_source.contains("resume_unwind"));

    let production = production_source(COMMAND_EXECUTION_SOURCE);
    let mapping = production
        .split_once("pub(super) fn command_attempt_failure(")
        .and_then(|(_, remainder)| remainder.split_once("\nfn storage_error("))
        .map(|(body, _)| body)
        .expect("command-attempt public error mapping");
    assert_eq!(
        mapping
            .matches("CommandAttemptError::EvaluationPanicked")
            .count(),
        1
    );
    let panic_arm = mapping
        .split_once("CommandAttemptError::EvaluationPanicked => {")
        .and_then(|(_, remainder)| {
            remainder.split_once("\n        CommandAttemptError::PendingRecheck(error)")
        })
        .map(|(body, _)| body)
        .expect("caught-runtime-panic mapping");
    assert!(panic_arm.contains("CommandExecutionErrorKind::InternalDefect"));
    for forbidden in [
        "panic!",
        "resume_unwind",
        "StorageError",
        "StoredExecutionFailedV1",
    ] {
        assert!(
            !panic_arm.contains(forbidden),
            "caught runtime panic leaks or resumes through {forbidden}"
        );
    }
}

#[test]
fn reviewed_tokio_owner_and_lock_graph_are_frozen() {
    assert_eq!(
        production_dependency_owners("tokio"),
        [
            "riffdb-api-grpc",
            "riffdb-api-mcp",
            "riffdb-cli",
            // Python extension: bridges Python awaitables onto the same
            // current first-party Tokio runtime as the Rust client.
            "riffdb-client-python-native",
            // Client: `time` only for non-blocking Overloaded backoff (already
            // transitive via tonic; see riffdb-client-rust Cargo.toml comment).
            "riffdb-client-rust",
            "riffdb-commit",
            "riffdb-mcp-stdio",
            "riffdb-server",
        ],
        "a new production Tokio owner requires dependency and feature-unification review"
    );
    for exact_entry in [
        "name = \"tokio\"\nversion = \"1.52.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"a91135f59b1cbf38c91e73cf3386fca9bb77915c45ce2771460c9d92f0f3d776\"\ndependencies = [\n \"bytes\",\n \"libc\",\n \"mio\",\n \"pin-project-lite\",\n \"signal-hook-registry\",\n \"socket2\",\n \"tokio-macros\",\n \"windows-sys 0.61.2\",\n]",
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
        "use std::time::Instant;",
        "Instant::now()",
        "CommitGroupDispatchReason::QueueDrained",
        "receiver.blocking_recv()",
    ] {
        assert!(
            production_source.contains(required),
            "coordinator actor is missing reviewed mechanism {required}"
        );
    }
    for forbidden in [
        "new_multi_thread",
        "tokio::spawn",
        "tokio::net",
        "tokio::fs",
        "tokio::signal",
        "SystemTime",
        "getrandom",
        "rand::",
        "redb::",
        // select! over the intake receiver + writer feedback is required by the
        // pipelined writer (event-driven formation window; no timer driver).
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
    assert!(
        production_source.contains("tokio::select!"),
        "pipelined intake must select over writer feedback and the admission receiver"
    );
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
    let audit_execution = production_source
        .split_once("fn execute_audit(")
        .expect("audit execution implementation")
        .1
        .split_once("async fn execute_command")
        .expect("bounded audit execution implementation")
        .0;
    assert!(
        audit_execution.contains("StorageErrorKind::CommitStatusUnknown")
            && audit_execution.contains("self.lifecycle.fence();")
            && audit_execution.contains("Err(_) => self.lifecycle.stop()"),
        "audit execution must fence only unknown commit status and stop every proven failure"
    );
    let lifecycle_publication = production_source
        .split_once("impl CommandExecutionLifecycle for ActorLifecyclePublisher")
        .expect("actor lifecycle publication implementation")
        .1
        .split_once("struct CommandCoordinatorActor")
        .expect("bounded actor lifecycle publication implementation")
        .0;
    assert!(
        lifecycle_publication
            .find(".store(LIFECYCLE_FENCED, Ordering::Release)")
            .expect("fenced-state publication")
            < lifecycle_publication
                .find("submission_gate.close()")
                .expect("fenced gate close")
    );

    let drop_body = production_source
        .split_once("impl Drop for RunningCommandCoordinator {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("running coordinator Drop implementation");
    // Pipelined writer: Drop must join the intake actor so WriterHandle joins
    // the writer before returning (no orphaned write transaction).
    assert!(
        drop_body.contains(".join()"),
        "coordinator Drop must join the intake actor (and thus the writer)"
    );
}

#[test]
fn first_commit_notification_is_explicit_sequence_only_and_fail_closed() {
    for required in [
        "pub trait ApplicationCommitNotificationSink: Send + Sync",
        "sequence: CommitSequence",
        "Result<(), ApplicationCommitNotificationError>",
    ] {
        assert!(
            NOTIFICATION_SOURCE.contains(required),
            "notification boundary is missing {required}"
        );
    }
    for forbidden in [
        "StoredOutcomeV1",
        "CommitRecord",
        "EntityKey",
        "ActorId",
        "TenantScope",
        "Storage",
        "Repository",
        "riffdb_service",
        "serde::",
        "prost::",
    ] {
        assert!(
            !NOTIFICATION_SOURCE.contains(forbidden),
            "notification sink exposes forbidden authority {forbidden}"
        );
    }
    assert!(!NOTIFICATION_SOURCE.contains("impl Default"));

    let constructor = AUDIT_EXECUTOR_SOURCE
        .split_once("pub fn start<Repository>(")
        .and_then(|(_, remainder)| remainder.split_once(") -> Result<Self, CoordinatorStartError>"))
        .map(|(parameters, _)| parameters)
        .expect("production coordinator constructor");
    assert!(constructor.contains("notifications: Arc<dyn ApplicationCommitNotificationSink>"));

    let execution = AUDIT_EXECUTOR_SOURCE
        .split_once("async fn execute_command(")
        .and_then(|(_, remainder)| remainder.split_once("\n    fn execute_read_only("))
        .map(|(body, _)| body)
        .expect("bounded command completion path");
    for required in [
        "CommittedOutcomeDisposition::FirstCommit",
        "outcome.stored_outcome().commit_sequence()",
        "panic::catch_unwind",
        "self.notifications.publish_first_commit(sequence)",
        "self.lifecycle.stop()",
    ] {
        assert!(
            execution.contains(required),
            "first-commit publication is missing {required}"
        );
    }
    assert!(
        execution
            .find("drive_command(preparation).await")
            .expect("durable completion")
            < execution
                .find("publish_first_commit(sequence)")
                .expect("publication")
    );
    assert!(
        execution
            .find("publish_first_commit(sequence)")
            .expect("publication")
            < execution
                .find("completion.send(result)")
                .expect("caller completion")
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
