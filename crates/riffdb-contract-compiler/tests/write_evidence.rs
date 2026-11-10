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

// req: DX-004, DX-016
#[test]
fn bulk_loop_revision_inequality_guard_satisfies_rdb_c030() {
    use riffdb_contract_ir::WriteEvidence;

    let base_source = include_str!("../../../fixtures/contracts/bulk/indexed-mutations.riff");
    for predicate in [
        "state.revision == mutation.expected_revision",
        "state.revision >= mutation.expected_revision",
        "state.revision <= mutation.expected_revision",
    ] {
        let source = base_source.replace("state.revision == mutation.expected_revision", predicate);
        let bundle = compile_contract_source(&source).expect("bulk loop revision guard compiles");
        let proofs = bundle.commands()[0].write_proofs().expect("write proofs");
        assert!(
            proofs.iter().any(|p| {
                p.evidence()
                    .iter()
                    .any(|e| matches!(e, WriteEvidence::BusinessCheck { .. }))
            }),
            "expected BusinessCheck write proof for {predicate}"
        );
    }

    let missing_checked_by = base_source.replace(
        "set state.active = mutation.next_active checked_by state_revision",
        "set state.active = mutation.next_active",
    );
    let err = compile_contract_source(&missing_checked_by).expect_err("missing checked_by");
    let diag = err
        .semantic()
        .expect("semantic")
        .as_slice()
        .iter()
        .find(|d| d.code() == CompilerDiagnosticCode::UnguardedSet)
        .expect("RDB-C048");
    let help = diag.code().help().expect("RDB-C048 help");
    assert!(
        help.contains("checked_by"),
        "expected actionable checked_by help, got: {help}",
    );
}

// req: CMP-001, DX-012, DX-050
#[test]
fn bulk_loop_caller_version_fence_guards_element_mutations_without_checked_by() {
    use riffdb_contract_ir::{ContractBundle, WriteEvidence};

    let source = r#"
contract TodoOrganization version 1 {
  entity Todo {
    key (workspace_id: string<64>, todo_id: uuid)
    field title: string<160>
    field category: string<64>
    field sort_rank: i64
    field updated_at: timestamp
    field revision: u64
  }
  entity TodoPlacement {
    key (workspace_id: string<64>, todo_id: uuid)
    field expected_revision: u64
    field category: string<64>
    field sort_rank: i64
  }
  aggregate TodoAggregate {
    root Todo
    child TodoPlacement
    partition_by workspace_id
    conflict_key (workspace_id, todo_id)
  }
  bulk command ApplyOrganization {
    input request_id: uuid
    input workspace_id: string<64>
    input organized_at: timestamp
    input placements: list<TodoPlacement, 1..50>
    idempotency_key request_id
    for placement in placements {
      mutate Todo(workspace_id, placement.todo_id) as todo else TodoMissing { todo_id: placement.todo_id }
      require placement_current: todo.revision == placement.expected_revision else PlacementStale { todo_id: placement.todo_id, current_revision: todo.revision }
      set todo.category = placement.category
      set todo.sort_rank = placement.sort_rank
      set todo.updated_at = organized_at
      set todo.revision = todo.revision + 1
    }
    return OrganizationApplied {}
  }
}
"#;
    let bundle = compile_contract_source(source).expect("bulk loop CallerVersionFence compiles");
    let proofs = bundle.commands()[0].write_proofs().expect("write proofs");
    assert_eq!(proofs.len(), 4);
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

    // Without the revision increment, replacement writes inside the loop still fail closed with RDB-C048.
    let missing_increment = source.replace("      set todo.revision = todo.revision + 1\n", "");
    let err = compile_contract_source(&missing_increment).expect_err("missing revision increment");
    let diag = err
        .semantic()
        .expect("semantic")
        .as_slice()
        .iter()
        .find(|d| d.code() == CompilerDiagnosticCode::UnguardedSet)
        .expect("RDB-C048");
    let help = diag.code().help().expect("help");
    assert!(help.contains("set binding.field = <expr> checked_by <RequirementName>"));
}

// req: CMP-001, DX-012, DX-050
#[test]
fn partial_update_coalesce_preserves_unspecified_fields_under_revision_fence() {
    use riffdb_contract_ir::{
        BinaryOperator, ContractBundle, ExpressionKind, Instruction, WriteEvidence,
    };

    let source = r#"
contract TodoPartialUpdate version 1 {
  entity Todo {
    key (workspace_id: string<64>, todo_id: uuid)
    field title: string<160>
    field notes: optional<string<500>>
    field done: bool
    field priority: i64
    field revision: u64
  }
  aggregate TodoAggregate {
    root Todo
    partition_by workspace_id
    conflict_key (workspace_id, todo_id)
  }
  command UpdateTodo {
    input request_id: uuid
    input workspace_id: string<64>
    input todo_id: uuid
    input expected_revision: u64
    input title: optional<string<160>>
    input notes: optional<string<500>>
    input done: optional<bool>
    input priority: optional<i64>
    idempotency_key request_id
    mutate Todo(workspace_id, todo_id) as todo else TodoMissing {}
    require revision_matches: todo.revision == expected_revision else TodoRevisionConflict {}
    set todo.title = title ?? todo.title
    set todo.notes = coalesce(notes, todo.notes)
    set todo.done = coalesce(done, todo.done)
    set todo.priority = priority ?? todo.priority
    set todo.revision = todo.revision + 1
    return TodoUpdated {}
  }
}
"#;
    let bundle = compile_contract_source(source).expect("partial update contract compiles");
    let plan = &bundle.commands()[0];
    let proofs = plan.write_proofs().expect("write proofs");
    assert_eq!(proofs.len(), 5);
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

    let coalesce_exprs = plan
        .instructions()
        .iter()
        .filter_map(|ins| match ins {
            Instruction::SetField { value, .. } => {
                let node = plan.expressions().get(*value)?;
                matches!(
                    node.kind(),
                    ExpressionKind::Binary {
                        operator: BinaryOperator::Coalesce,
                        ..
                    }
                )
                .then_some(*value)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(coalesce_exprs.len(), 4);

    // Mismatched bounds or non-optional left operand fail closed at compile time.
    let non_optional_left = source.replace("input done: optional<bool>", "input done: bool");
    assert!(compile_contract_source(&non_optional_left).is_err());

    let mismatched_bound = source.replace(
        "input title: optional<string<160>>",
        "input title: optional<string<80>>",
    );
    assert!(compile_contract_source(&mismatched_bound).is_err());
}
