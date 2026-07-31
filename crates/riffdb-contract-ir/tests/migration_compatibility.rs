//! Contract compatibility classes and stable migration-code tests.

use riffdb_contract_ir::{CompatibilityClass, CompatibilityCode};

#[test]
fn migration_class_has_the_frozen_restrictiveness_order() {
    assert!(CompatibilityClass::Compatible < CompatibilityClass::RequiresExplicitVersion);
    assert!(CompatibilityClass::RequiresExplicitVersion < CompatibilityClass::RequiresMigration);
    assert!(CompatibilityClass::RequiresMigration < CompatibilityClass::Incompatible);
}

#[test]
fn gate_a_change_codes_require_migration() {
    let expected = [
        (CompatibilityCode::AddedRequiredField, "RDB-K030"),
        (CompatibilityCode::AddedIndex, "RDB-K031"),
        (CompatibilityCode::AddedRelationship, "RDB-K032"),
        (CompatibilityCode::AddedUniqueConstraint, "RDB-K033"),
        (CompatibilityCode::AddedInvariant, "RDB-K034"),
        (CompatibilityCode::ProjectionBackfill, "RDB-K035"),
    ];

    for (code, stable_code) in expected {
        assert_eq!(code.as_str(), stable_code);
        assert_eq!(code.class(), CompatibilityClass::RequiresMigration);
    }
}
