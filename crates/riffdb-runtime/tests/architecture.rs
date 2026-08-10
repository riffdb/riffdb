//! Dependency and ambient-authority checks for deterministic runtime crates.

use riffdb_contract_compiler::compile_contract_source;

const RUNTIME_SOURCE: &str = include_str!("../src/lib.rs");
const INVARIANT_SOURCE: &str = include_str!("../../riffdb-invariant/src/lib.rs");
const RUNTIME_MANIFEST: &str = include_str!("../Cargo.toml");
const INVARIANT_MANIFEST: &str = include_str!("../../riffdb-invariant/Cargo.toml");
const CONTRACT_GRAMMAR: &str = include_str!("../../riffdb-contract-syntax/src/grammar.lalrpop");
const CONTRACT_AST: &str = include_str!("../../riffdb-contract-syntax/src/ast.rs");
const CONTRACT_HIR: &str = include_str!("../../riffdb-contract-compiler/src/hir.rs");
const COMMAND_IR: &str = include_str!("../../riffdb-contract-ir/src/plan.rs");
const IR_FORMAT: &str = include_str!("../../riffdb-contract-ir/FORMAT.md");
const DEFERRED_STATE_MACHINE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../contracts/parser-fixtures/invalid/deferred_state_machine.riff"
));

#[test]
fn deterministic_crates_have_no_ambient_or_async_authority() {
    for (crate_name, source) in [
        ("riffdb-runtime", RUNTIME_SOURCE),
        ("riffdb-invariant", INVARIANT_SOURCE),
    ] {
        for forbidden in [
            "std::fs::",
            "std::net::",
            "std::env::",
            "std::process::",
            "std::time::",
            "SystemTime",
            "Instant::now",
            "async fn",
            ".await",
            "tokio::",
            "rand::",
            "getrandom",
            "thread_rng",
        ] {
            assert!(
                !source.contains(forbidden),
                "{crate_name} source contains forbidden authority {forbidden}"
            );
        }
    }
}

#[test]
fn runtime_cannot_assemble_commits_or_observe_capability_and_provenance_state() {
    for forbidden in [
        "CommitIntent",
        "StoredAdmittedProvenanceClaims",
        "ProvenanceId",
        "MutationLease",
        "MutationCapability",
        "ConflictManager",
        "StorageEngine",
        "WriteTransaction",
        "CommandTransaction",
        "riffdb_storage_memory",
        "riffdb_storage_redb",
        "redb::",
    ] {
        assert!(
            !RUNTIME_SOURCE.contains(forbidden),
            "runtime source crosses forbidden boundary {forbidden}"
        );
    }
}

#[test]
fn checked_identifiers_are_not_used_as_clock_entropy_or_order_sources() {
    for forbidden in [
        "request_id.as_bytes",
        "request_id().as_bytes",
        "request_id.into_bytes",
        "request_id().into_bytes",
        "UuidGenerator",
        "UuidSource",
        "AdmissionClock",
        "LogicalTime::new(Timestamp::new",
    ] {
        assert!(
            !RUNTIME_SOURCE.contains(forbidden),
            "runtime derives semantic state from an identifier/source: {forbidden}"
        );
    }
    assert!(INVARIANT_SOURCE.contains(".transaction_time()"));
    assert!(RUNTIME_SOURCE.contains("tx_time: LogicalTime"));
}

#[test]
fn manifests_name_only_the_approved_semantic_layers() {
    for forbidden in [
        "tokio",
        "rand",
        "getrandom",
        "uuid",
        "riffdb-commit",
        "riffdb-conflict",
        "riffdb-service",
        "riffdb-storage-memory",
        "riffdb-storage-redb",
    ] {
        assert!(!RUNTIME_MANIFEST.contains(forbidden));
        assert!(!INVARIANT_MANIFEST.contains(forbidden));
    }
    for required in [
        "riffdb-contract-ir",
        "riffdb-invariant",
        "riffdb-storage-api",
        "riffdb-types",
    ] {
        assert!(RUNTIME_MANIFEST.contains(required));
    }
    assert!(INVARIANT_MANIFEST.contains("riffdb-contract-ir"));
    assert!(INVARIANT_MANIFEST.contains("riffdb-types"));
}

#[test]
fn contract_workflows_do_not_expose_a_generic_state_machine_or_transition_surface() {
    for (layer, source) in [
        ("grammar", CONTRACT_GRAMMAR),
        ("syntax AST", CONTRACT_AST),
        ("compiler HIR", CONTRACT_HIR),
        ("command IR", COMMAND_IR),
        ("IR format", IR_FORMAT),
    ] {
        for forbidden in ["state_machine", "StateMachine"] {
            assert!(
                !source.contains(forbidden),
                "contract {layer} unexpectedly exposes generic {forbidden}"
            );
        }
    }
    for forbidden in [
        "Instruction::Transition",
        "Self::Transition",
        "TransitionInstruction",
    ] {
        assert!(
            !RUNTIME_SOURCE.contains(forbidden),
            "runtime unexpectedly dispatches {forbidden}"
        );
    }
    for (layer, source, required) in [
        ("grammar", CONTRACT_GRAMMAR, "WorkflowTransitionDeclaration"),
        ("syntax AST", CONTRACT_AST, "WorkflowTransition("),
        ("compiler HIR", CONTRACT_HIR, "HirWorkflowTransition"),
        ("command IR", COMMAND_IR, "WorkflowTransition {"),
        ("IR format", IR_FORMAT, "WorkflowTransitionSchema"),
        (
            "deterministic runtime",
            RUNTIME_SOURCE,
            "Instruction::WorkflowTransition",
        ),
    ] {
        assert!(
            source.contains(required),
            "accepted compiled-workflow {layer} lost {required}"
        );
    }
    for required in [
        "Require {",
        "SetField {",
        "EmitEvent(",
        "WorkflowTransition {",
        "Return(",
    ] {
        assert!(
            COMMAND_IR.contains(required),
            "closed command instruction set lost {required}"
        );
    }
    assert!(
        compile_contract_source(DEFERRED_STATE_MACHINE).is_err(),
        "deferred state-machine syntax must fail compilation"
    );
}
