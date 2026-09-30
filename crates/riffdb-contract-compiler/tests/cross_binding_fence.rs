//! Same-root revision fences protect child writes without a child pre-image read.
// req: CMP-015
use riffdb_contract_compiler::{CompilationError, CompilerDiagnosticCode, compile_contract_source};
use riffdb_contract_ir::CommandExplain;

const FENCE: &str = "require current: parent.revision == expected_revision else Stale {}";
const ADVANCE: &str = "set parent.revision = parent.revision + 1";
const SOURCE: &str = include_str!("../../../fixtures/contracts/cross-binding-fence.riff");

fn rejects_child(source: &str, code: CompilerDiagnosticCode) {
    let CompilationError::Semantic(diagnostics) =
        compile_contract_source(source).expect_err("the invalid spelling must refuse")
    else {
        panic!("expected semantic diagnostics");
    };
    let diagnostic = diagnostics
        .as_slice()
        .iter()
        .find(|d| {
            d.code() == code
                && &source[d.primary_span().start() as usize..d.primary_span().end() as usize]
                    == "item.value"
        })
        .expect("expected spelling diagnostic");
    assert_eq!(
        &source
            [diagnostic.primary_span().start() as usize..diagnostic.primary_span().end() as usize],
        "item.value"
    );
}

#[test]
fn root_revision_fence_protects_child_and_classification_is_in_plan() {
    compile_contract_source(&SOURCE.replace(": i64", ": u64")).expect("unsigned revision fence");
    let guarded = compile_contract_source(SOURCE).expect("same-root fence guards the child");
    let plan = &guarded.commands()[0];
    assert!(
        CommandExplain::from_plan(plan)
            .unguarded_writes()
            .is_empty()
    );
    let unguarded = compile_contract_source(
        &SOURCE
            .replace(FENCE, "")
            .replace("set item.value", "overwrite item.value"),
    )
    .expect("explicit overwrite");
    assert_ne!(plan.plan_hash(), unguarded.commands()[0].plan_hash());
    assert_eq!(
        CommandExplain::from_plan(&unguarded.commands()[0])
            .unguarded_writes()
            .len(),
        1
    );
    compile_contract_source(
        &SOURCE
            .replace(
                "parent.revision == expected_revision",
                "expected_revision == parent.revision",
            )
            .replace("parent.revision + 1", "1 + parent.revision"),
    )
    .expect("commuted operands");
}

#[test]
fn removing_either_half_of_root_fence_rejects_child_set() {
    for source in [SOURCE.replace(FENCE, ""), SOURCE.replace(ADVANCE, "")] {
        rejects_child(&source, CompilerDiagnosticCode::UnguardedSet);
    }
}

#[test]
fn root_fenced_child_cannot_claim_to_be_an_overwrite() {
    rejects_child(
        &SOURCE.replace("set item.value", "overwrite item.value"),
        CompilerDiagnosticCode::GuardedOverwrite,
    );
}

#[test]
fn invariant_different_root_and_nonadvancing_writes_are_not_fences() {
    for source in [
        SOURCE.replace(FENCE, "").replace(ADVANCE, ""),
        SOURCE.replace(
            "Child(tenant, parent_id, child_id)",
            "Child(tenant, other_parent_id, child_id)",
        ),
        SOURCE.replace("parent.revision + 1", "parent.revision + 0"),
        SOURCE.replace("parent.revision + 1", "parent.revision"),
        SOURCE.replace("parent.revision + 1", "expected_revision"),
        SOURCE.replace(
            "parent.revision == expected_revision",
            "parent.revision >= expected_revision",
        ),
        SOURCE.replace(
            "parent.revision == expected_revision",
            "parent.revision == 0",
        ),
    ] {
        rejects_child(&source, CompilerDiagnosticCode::UnguardedSet);
    }
}

#[test]
fn unrelated_aggregate_and_child_only_revision_do_not_guard_another_binding() {
    let unrelated = SOURCE.replace("child Child", "")
        .replace("  command UpdateChild", "  aggregate Items { root Child partition_by tenant conflict_key (tenant, parent_id, child_id) }\n  command UpdateChild");
    rejects_child(&unrelated, CompilerDiagnosticCode::UnguardedSet);
    let child_fence = SOURCE
        .replace("field value: i64", "field value: i64 field revision: i64")
        .replace("parent.revision", "item.revision")
        .replace("set item.value = value", "set parent.revision = value");
    let error = compile_contract_source(&child_fence).expect_err("child cannot fence parent");
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
fn initialization_and_observation_are_not_root_fences() {
    for binding in [
        "init_or_mutate Parent(tenant, parent_id) as parent initialize { revision: 0 }",
        "read Parent(tenant, parent_id) as parent else ParentMissing {}",
    ] {
        let source = SOURCE.replace(
            "mutate Parent(tenant, parent_id) as parent else ParentMissing {}",
            binding,
        );
        // A read cannot be assigned; independently it still must not guard the child.
        rejects_child(&source, CompilerDiagnosticCode::UnguardedSet);
    }
}

#[test]
fn decision_predicate_and_conditional_advance_do_not_supply_a_root_fence() {
    let source = SOURCE.replace(
        "mutate Parent(tenant, parent_id) as parent else ParentMissing {}",
        "observe_or_initialize Parent(tenant, parent_id) as parent initialize { revision: 0 }",
    );
    let start = source.find("    mutate Child").expect("child binding");
    let source = format!(
        "{}    decide parent {{\n      when parent.revision == expected_revision => apply {{\n        mutate Child(tenant, parent_id, child_id) as item else ChildMissing {{}}\n        set parent.revision = parent.revision + 1\n        set item.value = value\n      }}\n      else => reject Stale {{}}\n    }}\n    return Updated {{}}\n  }}\n}}\n",
        &source[..start]
    );
    rejects_child(&source, CompilerDiagnosticCode::UnguardedSet);
}
