//! Per-assignment evidence and explicit business associations.
// req: CMP-015, CMP-013
use riffdb_contract_compiler::{CompilerDiagnosticCode, compile_contract_source};

const SOURCE: &str = r"
contract Edits version 1 {
  entity State {
    key (tenant: uuid, id: uuid)
    field revision: u64
    field value: u64
  }
  aggregate States { root State partition_by tenant conflict_key (tenant, id) }
  command Replace {
    input request: uuid
    input tenant: uuid
    input id: uuid
    input value: u64
    input expected: u64
    idempotency_key request
    mutate State(tenant, id) as state else Missing {}
    set state.revision = state.revision + 1
    set state.value = value
    return Updated {}
  }
}
";

#[test]
fn revision_increment_does_not_authorize_replacement_set() {
    let error = compile_contract_source(SOURCE).expect_err("replacement requires its own evidence");
    let diagnostic = error
        .semantic()
        .expect("semantic")
        .as_slice()
        .iter()
        .find(|d| d.code() == CompilerDiagnosticCode::UnguardedSet)
        .expect("RDB-C048");
    let span = diagnostic.primary_span();
    assert_eq!(
        &SOURCE[span.start() as usize..span.end() as usize],
        "state.value"
    );
}

fn checked() -> String {
    SOURCE.replace(
        "    set state.revision",
        "    require current: state.revision == expected else Stale {}\n    set state.revision",
    )
}

#[test]
fn fence_covers_root_replacement_and_preserves_all_evidence() {
    use riffdb_contract_ir::{ContractBundle, WriteEvidence};
    let bundle = compile_contract_source(&checked()).expect("fenced");
    assert_eq!(bundle.ir_version(), 25);
    let proofs = bundle.commands()[0].write_proofs().expect("proofs");
    assert!(proofs[0].evidence().contains(&WriteEvidence::StateDerived));
    assert!(proofs.iter().all(|proof| {
        proof
            .evidence()
            .iter()
            .any(|e| matches!(e, WriteEvidence::CallerVersionFence { .. }))
    }));
    assert_eq!(
        ContractBundle::decode(bundle.canonical_bytes()).expect("round trip"),
        bundle
    );
}

#[test]
fn association_is_explicit_and_is_not_a_caller_fence() {
    use riffdb_contract_ir::WriteEvidence;
    let source = checked()
        .replace("    set state.revision = state.revision + 1\n", "")
        .replace(
            "set state.value = value",
            "set state.value = value checked_by current",
        );
    let bundle = compile_contract_source(&source).expect("business check");
    assert_eq!(
        bundle.commands()[0].write_proofs().expect("proofs")[0].evidence(),
        &[WriteEvidence::BusinessCheck { requirement: 0 }]
    );
    assert!(compile_contract_source(&source.replace(" checked_by current", "")).is_err());
    assert_eq!(
        riffdb_contract_ir::ContractBundle::decode(bundle.canonical_bytes()).expect("round trip"),
        bundle
    );
}

#[test]
fn invalid_association_is_rejected_even_with_an_independent_fence() {
    for name in ["missing", "unrelated"] {
        let source = checked()
            .replace(
                "    set state.revision",
                "    require unrelated: value > 0 else Bad {}\n    set state.revision",
            )
            .replace(
                "set state.value = value",
                &format!("set state.value = value checked_by {name}"),
            );
        let error = compile_contract_source(&source).expect_err("invalid association");
        let diagnostic = error
            .semantic()
            .expect("semantic")
            .as_slice()
            .iter()
            .find(|d| d.code() == CompilerDiagnosticCode::InvalidWriteAssociation)
            .expect("association diagnostic");
        assert_eq!(
            &source[diagnostic.primary_span().start() as usize
                ..diagnostic.primary_span().end() as usize],
            name
        );
    }
}

#[test]
fn only_closed_same_field_arithmetic_qualifies() {
    for value in [
        "state.value + value",
        "value + state.value",
        "state.value - value",
    ] {
        compile_contract_source(&SOURCE.replace(
            "set state.value = value",
            &format!("set state.value = {value}"),
        ))
        .expect("delta");
    }
    for value in [
        "state.value * 0 + value",
        "state.revision + value",
        "value - state.value",
        "state.value + state.revision",
    ] {
        assert!(
            compile_contract_source(&SOURCE.replace(
                "set state.value = value",
                &format!("set state.value = {value}")
            ))
            .is_err(),
            "{value}"
        );
    }
}

#[test]
fn unrelated_read_does_not_prevent_an_explicit_overwrite() {
    let source = SOURCE.replace("set state.value = value", "overwrite state.value = value");
    let bundle = compile_contract_source(&source).expect("explicit replacement");
    assert_eq!(
        bundle.commands()[0].write_proofs().expect("proofs")[1].evidence(),
        &[riffdb_contract_ir::WriteEvidence::Overwrite]
    );
}

#[test]
fn historical_artifacts_keep_their_bytes_and_legacy_classification() {
    for bytes in [
        include_bytes!(
            "../../../fixtures/compatibility/write-evidence-legacy/initialized-state-transition.bin"
        )
        .as_slice(),
        include_bytes!(
            "../../../fixtures/compatibility/write-evidence-legacy/command-decision.bin"
        )
        .as_slice(),
        include_bytes!(
            "../../../fixtures/compatibility/write-evidence-legacy/aggregate-collection-budget.bin"
        )
        .as_slice(),
        include_bytes!(
            "../../../fixtures/compatibility/write-evidence-legacy/workflow-initial.bin"
        )
        .as_slice(),
        include_bytes!("../../../fixtures/compatibility/write-evidence-legacy/reimport.bin")
            .as_slice(),
        include_bytes!("../../../fixtures/compatibility/write-evidence-legacy/cascade.bin")
            .as_slice(),
    ] {
        let bundle = riffdb_contract_ir::ContractBundle::decode(bytes).expect("historical decode");
        assert!(bundle.ir_version() < 25);
        assert_eq!(bundle.canonical_bytes(), bytes);
        assert!(
            bundle
                .commands()
                .iter()
                .all(|plan| plan.write_proofs().is_none())
        );
    }
}

#[test]
fn decision_predicates_do_not_authorize_other_arm_assignments() {
    let source = include_str!("../../../fixtures/compiler/command-decision/contract.riff")
        .replace("overwrite state.active", "set state.active");
    let error = compile_contract_source(&source).expect_err("branch read is not evidence");
    assert!(
        error
            .semantic()
            .expect("semantic")
            .as_slice()
            .iter()
            .any(|d| d.code() == CompilerDiagnosticCode::UnguardedSet)
    );
}

#[test]
fn metadata_and_explain_keep_business_associations_separate_from_delta_and_fence() {
    let source = checked().replace(
        "set state.revision = state.revision + 1",
        "set state.revision = state.revision + 1 checked_by current",
    );
    let bundle = compile_contract_source(&source).expect("three independent kinds");
    let proofs = bundle.commands()[0].write_proofs().expect("proofs");
    assert_eq!(proofs[0].evidence().len(), 3);
    let explain = riffdb_contract_ir::CommandExplain::from_plan(&bundle.commands()[0]);
    assert_eq!(explain.write_proofs(), Some(proofs));
    for text in [
        "StateDerived",
        "BusinessCheck",
        "CallerVersionFence",
        "author-asserted",
        "do not prove caller freshness",
    ] {
        assert!(explain.render_text().contains(text), "{text}");
    }
    let without_association = compile_contract_source(&checked()).expect("fence");
    assert_ne!(
        bundle.commands()[0].plan_hash(),
        without_association.commands()[0].plan_hash()
    );
}

#[test]
fn unassociated_checks_and_invariants_cannot_authorize_replacement() {
    for predicate in [
        "state.revision >= 0",
        "state.value == value",
        "state.revision == state.revision",
    ] {
        let source = SOURCE.replace(
            "    set state.revision",
            &format!(
                "    require business: {predicate} else Rejected {{}}\n    set state.revision"
            ),
        );
        assert!(compile_contract_source(&source).is_err());
    }
    let source = SOURCE.replace(
        "field value: u64",
        "field value: u64 invariant nonnegative: value >= 0",
    );
    assert!(compile_contract_source(&source).is_err());
}

#[test]
fn weak_business_predicate_remains_an_explicit_author_claim() {
    let source = SOURCE.replace("    set state.revision", "    require business: state.value == state.value else Rejected {}\n    set state.revision")
        .replace("set state.value = value", "set state.value = value checked_by business");
    let bundle = compile_contract_source(&source).expect("author-associated business check");
    assert_eq!(
        bundle.commands()[0].write_proofs().expect("proofs")[1].evidence(),
        &[riffdb_contract_ir::WriteEvidence::BusinessCheck { requirement: 0 }]
    );
}

fn invalid_association(source: &str, name: &str) {
    let error = compile_contract_source(source).expect_err("invalid association");
    let diagnostic = error
        .semantic()
        .unwrap_or_else(|| panic!("{error:?}"))
        .as_slice()
        .iter()
        .find(|d| d.code() == CompilerDiagnosticCode::InvalidWriteAssociation)
        .unwrap_or_else(|| panic!("{error:?}"));
    assert_eq!(
        &source
            [diagnostic.primary_span().start() as usize..diagnostic.primary_span().end() as usize],
        name
    );
}

#[test]
fn business_associations_refuse_keys_ambiguity_forward_checks_and_creation() {
    let business = SOURCE
        .replace(
            "    set state.revision",
            "    require business: state.value > 0 else Rejected {}\n    set state.revision",
        )
        .replace(
            "set state.value = value",
            "set state.value = value checked_by business",
        );
    compile_contract_source(&business).expect("valid association");
    invalid_association(
        &business.replace("state.value > 0", "state.id == id"),
        "business",
    );
    let duplicate = business.replace(
        "    set state.revision",
        "    require business: state.revision >= 0 else Rejected {}\n    set state.revision",
    );
    let error = compile_contract_source(&duplicate).expect_err("ambiguous duplicate name");
    assert!(
        error
            .semantic()
            .expect("semantic")
            .as_slice()
            .iter()
            .any(|d| d.code() == CompilerDiagnosticCode::DuplicateName)
    );
    let forward = business
        .replace(
            "    require business: state.value > 0 else Rejected {}\n",
            "",
        )
        .replace(
            "    return Updated",
            "    require business: state.value > 0 else Rejected {}\n    return Updated",
        );
    assert!(
        compile_contract_source(&forward)
            .expect_err("requirements precede effects")
            .syntax()
            .is_some()
    );
    let creation = SOURCE
        .replace("mutate State", "create State")
        .replace(
            "set state.revision = state.revision + 1",
            "set state.revision = 0",
        )
        .replace(
            "set state.value = value",
            "set state.value = value checked_by missing",
        );
    invalid_association(&creation, "missing");
}

#[test]
fn common_requirements_do_not_dominate_decision_arm_writes() {
    let source = include_str!("../../../fixtures/compiler/command-decision/contract.riff")
        .replace(
            "    return Applied",
            "    require active: state.active == false else Inactive {}\n    return Applied",
        )
        .replace(
            "overwrite state.active = true",
            "set state.active = true checked_by active",
        );
    invalid_association(&source, "active");
}

#[test]
fn bulk_associations_require_iteration_local_dominance_and_survive_round_trip() {
    use riffdb_contract_ir::{ContractBundle, WriteEvidence};
    let source = include_str!("../../../fixtures/contracts/bulk/indexed-mutations.riff");
    let bundle = compile_contract_source(source).expect("per-element business requirements");
    assert!(
        bundle.commands()[0]
            .write_proofs()
            .expect("proofs")
            .iter()
            .any(|p| p
                .evidence()
                .iter()
                .any(|e| matches!(e, WriteEvidence::BusinessCheck { .. })))
    );
    assert_eq!(
        ContractBundle::decode(bundle.canonical_bytes()).expect("decode"),
        bundle
    );
    invalid_association(
        &source.replace("require state_revision:", "require renamed:"),
        "state_revision",
    );
    let source = source.replace("require state_revision:", "require renamed:").replace("    return IndexedMutationsApplied", "    require state_revision: true else OuterFailure {}\n    return IndexedMutationsApplied");
    invalid_association(&source, "state_revision");
}
