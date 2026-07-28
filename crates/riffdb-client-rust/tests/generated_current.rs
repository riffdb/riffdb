#![forbid(unsafe_code)]

//! Renderer and currentness checks for the canonical generated command module.
//!
//! Typed compiler and `bundle.bin` consistency remain the prerequisite evidence
//! of `scripts/generate-contract-fixtures --check`.

#[path = "../codegen/legal_spend_codegen.rs"]
mod legal_spend_codegen;

use legal_spend_codegen::{FixtureSet, checked_fixtures, generate_from, generated_source};

const CHECKED_BINDINGS: &str = include_str!("../src/generated/legal_spend.rs");

#[test]
fn checked_bindings_equal_fixture_scoped_emission() {
    let generated = generated_source().expect("checked compiler fixtures must generate");
    assert_eq!(
        generated, CHECKED_BINDINGS,
        "run `./scripts/generate-rust-sdk`"
    );
}

#[test]
fn plan_hash_fixture_text_drives_renderer_output() {
    let fixtures = checked_fixtures();
    // This intentionally isolates renderer behavior. It is not a claim that the
    // mutated text remains consistent with the compiler-owned binary bundle.
    let replacement = "0101010101010101010101010101010101010101010101010101010101010101";
    let changed_plans = fixtures.command_plans.replacen(
        "e128287fe0d5245e6aaf36f3c6045127e24869e52ee40c7e1fdcc76e5a3d74ff",
        replacement,
        1,
    );
    let generated = generate_from(FixtureSet {
        command_plans: &changed_plans,
        ..fixtures
    })
    .expect("a structurally valid hash change must drive emission");

    assert_ne!(generated, CHECKED_BINDINGS);
    assert!(generated.contains(
        "    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,"
    ));
}

#[test]
fn unsupported_command_shape_drift_fails_closed() {
    let fixtures = checked_fixtures();
    let changed_plans = fixtures.command_plans.replacen(
        "input.field.3=name:idempotency_key type:string<128>",
        "input.field.3=name:request_key type:string<128>",
        1,
    );
    let error = generate_from(FixtureSet {
        command_plans: &changed_plans,
        ..fixtures
    })
    .expect_err("fixture-scoped generation must reject unsupported command shapes");

    assert!(error.contains("CreateBudget plan"));
    assert!(error.contains("idempotency_key"));
}

#[test]
fn lineage_identity_drift_fails_closed() {
    let fixtures = checked_fixtures();
    let changed_ledger =
        fixtures
            .lineage_ledger
            .replacen("name:BudgetAlreadyExists", "name:BudgetExists", 1);
    let error = generate_from(FixtureSet {
        lineage_ledger: &changed_ledger,
        ..fixtures
    })
    .expect_err("fixture-scoped generation must reject lineage identity drift");

    assert!(error.contains("allocation 20 entries changed"));
}
