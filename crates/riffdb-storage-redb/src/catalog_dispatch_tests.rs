use super::*;
use riffdb_storage_api::{AuthoritativeNamespaceV1, ReplicationAuthorityClassV1};
use std::collections::BTreeSet;

// req: STO-012, REP-003, PRJ-004
#[test]
fn native_catalog_dispatch_preserves_old_inventory_and_excludes_derived_only_base() {
    let old: Vec<_> = AuthoritativeNamespaceV1::ALL
        .into_iter()
        .map(N::from)
        .collect();
    for catalog in [
        AuthoritativeStateCatalogV1.digest(),
        AuthoritativeStateCatalogV2.digest(),
    ] {
        let selected: Vec<_> = namespaces(catalog)
            .unwrap()
            .filter(|n| n.as_v1().is_some())
            .collect();
        assert_eq!(selected, old);
        assert!(
            lookup(catalog, "projection_source_rows/v1", b"synthetic-key")
                .unwrap()
                .is_none()
        );
        assert!(
            lookup(catalog, "meta", b"projection_checkpoint_ordinal/v1")
                .unwrap()
                .is_none()
        );
    }
    let current: Vec<_> = namespaces(AuthoritativeStateCatalogV3.digest())
        .unwrap()
        .collect();
    let primary: BTreeSet<_> = current
        .iter()
        .copied()
        .filter(|n| primary_physical_namespace(*n))
        .map(N::table)
        .collect();
    for namespace in [
        N::ProjectionSourceRows,
        N::ProjectionSourceObjects,
        N::ProjectionSourceRoots,
        N::ProjectionSourceMaintenance,
    ] {
        assert!(primary.contains(namespace.table()));
        assert_eq!(
            namespace.class(),
            ReplicationAuthorityClassV1::ReplicatedAuthoritative
        );
        assert!(namespace.as_v1().is_none());
    }
    assert!(!primary.contains(N::GenerationSourceBases.table()));
    assert!(namespaces([0xFF; 32]).is_err());
}
// req: STO-012, REP-003, PRJ-004
#[test]
fn physical_catalog_selection_refuses_source_injection_and_missing_authority() {
    let legacy: BTreeSet<String> = AuthoritativeNamespaceV1::ALL
        .into_iter()
        .filter(|namespace| !namespace.is_retired_locator_table())
        .map(|namespace| namespace.table().to_owned())
        .collect();
    let none = BTreeSet::new();
    for catalog in [
        AuthoritativeStateCatalogV1.digest(),
        AuthoritativeStateCatalogV2.digest(),
    ] {
        assert!(exact_physical_tables(catalog, &legacy, &none).is_ok());
        for source in [
            N::ProjectionSourceRows,
            N::ProjectionSourceObjects,
            N::ProjectionSourceRoots,
            N::ProjectionSourceMaintenance,
        ] {
            let mut injected = legacy.clone();
            injected.insert(source.table().to_owned());
            assert!(exact_physical_tables(catalog, &injected, &none).is_err());
        }
    }
    let current: BTreeSet<String> = namespaces(AuthoritativeStateCatalogV3.digest())
        .unwrap()
        .filter(|namespace| primary_physical_namespace(*namespace))
        .map(|namespace| namespace.table().to_owned())
        .collect();
    assert!(exact_physical_tables(AuthoritativeStateCatalogV3.digest(), &current, &none).is_ok());
    for source in [
        N::ProjectionSourceRows,
        N::ProjectionSourceObjects,
        N::ProjectionSourceRoots,
        N::ProjectionSourceMaintenance,
    ] {
        let mut missing = current.clone();
        assert!(missing.remove(source.table()));
        assert!(
            exact_physical_tables(AuthoritativeStateCatalogV3.digest(), &missing, &none).is_err()
        );
    }
    let mut derived = current.clone();
    derived.insert(N::GenerationSourceBases.table().to_owned());
    assert!(exact_physical_tables(AuthoritativeStateCatalogV3.digest(), &derived, &none).is_err());
    let mut unknown = current.clone();
    unknown.insert("unknown_authority/v1".to_owned());
    assert!(exact_physical_tables(AuthoritativeStateCatalogV3.digest(), &unknown, &none).is_err());
    assert!(exact_physical_tables([0xFF; 32], &legacy, &none).is_err());
    assert!(
        exact_physical_tables(
            AuthoritativeStateCatalogV3.digest(),
            &current,
            &BTreeSet::from(["unexpected_multimap".to_owned()])
        )
        .is_err()
    );
    assert!(current.contains("projection_state") && current.contains("projection_applied"));
}

// req: STO-012, REC-001
#[test]
fn inactive_table_probe_never_creates_metadata_or_accepts_source_authority() {
    use redb::{ReadableTable, TableHandle};
    let scope = crate::test_path::ScopedDirectory::new("source-catalog-table-probe");
    let database = redb::Database::create(scope.join("db.redb")).unwrap();
    let transaction = database.begin_write().unwrap();
    assert!(
        crate::changelog_v3_write::table_inventory(&transaction)
            .unwrap()
            .is_empty()
    );
    assert_eq!(transaction.list_tables().unwrap().count(), 0);
    transaction.open_table(crate::layout::ENTITIES).unwrap();
    let expected = std::collections::BTreeSet::from(["entities"]);
    assert_eq!(
        crate::changelog_v3_write::table_inventory(&transaction).unwrap(),
        expected
    );
    let source: redb::TableDefinition<&[u8], &[u8]> =
        redb::TableDefinition::new(N::ProjectionSourceRows.table());
    transaction.open_table(source).unwrap();
    let before: BTreeSet<_> = transaction
        .list_tables()
        .unwrap()
        .map(|table| table.name().to_owned())
        .collect();
    assert!(crate::changelog_v3_write::table_inventory(&transaction).is_err());
    let after: BTreeSet<_> = transaction
        .list_tables()
        .unwrap()
        .map(|table| table.name().to_owned())
        .collect();
    assert_eq!(before, after);
    assert!(!after.contains("meta"));
    transaction.delete_table(source).unwrap();
    let catalog_key = AuthoritativeNamespaceV1::AuthoritativeStateCatalog
        .metadata_key()
        .unwrap();
    {
        let mut meta = transaction.open_table(crate::layout::META).unwrap();
        let encoded = riffdb_storage_api::proto_codec::encode_authoritative_state_catalog_v1(
            AuthoritativeStateCatalogV1,
        )
        .unwrap();
        meta.insert(catalog_key, encoded.as_bytes()).unwrap();
    }
    assert_eq!(
        crate::changelog_v3_write::table_inventory(&transaction).unwrap(),
        std::collections::BTreeSet::from(["entities", "meta"])
    );
    transaction.open_table(source).unwrap();
    assert!(crate::changelog_v3_write::table_inventory(&transaction).is_err());
    transaction.delete_table(source).unwrap();
    {
        let mut meta = transaction.open_table(crate::layout::META).unwrap();
        meta.insert(catalog_key, [0xff].as_slice()).unwrap();
    }
    assert!(crate::changelog_v3_write::table_inventory(&transaction).is_err());
    {
        let meta = transaction.open_table(crate::layout::META).unwrap();
        assert_eq!(meta.get(catalog_key).unwrap().unwrap().value(), &[0xff]);
    }
    transaction.abort().unwrap();
}

// req: STO-012, REP-005
#[test]
fn successor_catalog_inherits_required_primary_admission_and_unknown_refuses() {
    assert!(!requires_primary_admission(AuthoritativeStateCatalogV1.digest()).unwrap());
    assert!(requires_primary_admission(AuthoritativeStateCatalogV2.digest()).unwrap());
    assert!(requires_primary_admission(AuthoritativeStateCatalogV3.digest()).unwrap());
    assert!(requires_primary_admission([0xFF; 32]).is_err());
}
