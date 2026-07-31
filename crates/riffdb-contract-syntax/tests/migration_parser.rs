//! Migration grammar, source-span, formatting, and bounds tests.

use riffdb_contract_syntax::{
    MigrationDeclaration, MigrationTransformClause, format_migration, parse_migration,
};

const SOURCE: &str = r#"
migration LegalSpend from 1 to 2 {
  rename entity Budget to SpendingBudget
  rename field Budget.approved_amount to limit
  retire projection LegacySpend
  transform Budget {
    set currency = "USD"
    replace approved_amount with approved_cents using decimal_exact
    require old.approved_amount >= 0
    rekey (old.tenant_id, old.budget_id)
  }
  map enum BudgetState {
    Open -> Active
    Closed -> Closed
  }
  acknowledge repartition BudgetAggregate
  acknowledge aggregate BudgetAggregate
  acknowledge conflict BudgetAggregate
}
"#;

#[test]
fn parses_every_migration_v1_declaration_family_with_spans() {
    let document = parse_migration(SOURCE).expect("migration source");

    assert_eq!(document.migration.value.lineage.value, "LegalSpend");
    assert_eq!(document.migration.value.from.value, "1");
    assert_eq!(document.migration.value.to.value, "2");
    assert_eq!(document.migration.value.declarations.len(), 8);
    assert!(matches!(
        document.migration.value.declarations[0].value,
        MigrationDeclaration::Rename(_)
    ));
    assert!(matches!(
        document.migration.value.declarations[3].value,
        MigrationDeclaration::Transform(ref transform)
            if transform.clauses.iter().any(|clause| matches!(
                clause.value,
                MigrationTransformClause::Rekey(_)
            ))
    ));
    assert_eq!(
        &SOURCE[document.migration.span.start() as usize..document.migration.span.end() as usize]
            [..9],
        "migration"
    );
}

#[test]
fn canonical_formatter_round_trips_semantics() {
    let document = parse_migration(SOURCE).expect("migration source");
    let formatted = format_migration(&document);
    let reparsed = parse_migration(&formatted).expect("formatted migration");

    assert_eq!(formatted, format_migration(&reparsed));
}

#[test]
fn migration_source_obeys_the_one_mibibyte_bound() {
    let oversized = "x".repeat(riffdb_contract_syntax::limits::MAX_MIGRATION_SOURCE_BYTES + 1);
    let error = parse_migration(&oversized).expect_err("oversized source");

    assert_eq!(error.as_slice()[0].code().as_str(), "RDB-S001");
}
