#![forbid(unsafe_code)]
//! Closed namespace and authority-class conformance for the V3 substrate.
// req: REP-003, STO-012

use std::collections::BTreeSet;

use riffdb_storage_api::{
    AuthoritativeNamespaceV1, AuthoritativeStateCatalogV1, ReplicationAuthorityClassV1,
    ReplicationTransferV1,
};

#[test]
fn authoritative_catalog_is_closed_unique_and_fail_closed() {
    let catalog = AuthoritativeStateCatalogV1;
    let mut tags = BTreeSet::new();
    let mut domains = BTreeSet::new();
    for namespace in AuthoritativeNamespaceV1::ALL {
        assert!(tags.insert(namespace.tag()));
        assert!(domains.insert((namespace.table(), namespace.metadata_key())));
        assert_eq!(catalog.by_tag(namespace.tag()), Some(namespace));
        let key = namespace.metadata_key().unwrap_or("bounded-test-key");
        assert_eq!(
            catalog.lookup(namespace.table(), key.as_bytes()),
            Some(namespace)
        );
    }
    assert_eq!(catalog.by_tag(0), None);
    assert_eq!(catalog.by_tag(u16::MAX), None);
    assert_eq!(catalog.lookup("foreign", b"key"), None);
    assert_eq!(catalog.lookup("meta", b"foreign/v1"), None);
    assert_eq!(catalog.lookup("meta", b"database_id\0"), None);
    assert_eq!(catalog.lookup("meta", &[0xff]), None);
    assert_eq!(catalog.lookup("meta", b""), None);
}

#[test]
fn generated_authority_inventory_matches_its_single_owner() {
    assert_eq!(
        AuthoritativeStateCatalogV1.canonical_fixture(),
        include_str!("../../../fixtures/replication/authoritative-state-catalog-v1.txt")
    );
    assert!(AuthoritativeStateCatalogV1.canonical_fixture().len() <= 16 * 1024);
}

#[test]
fn projection_lifecycle_and_retention_fence_cannot_be_excluded_from_authority() {
    // ADR-0017's highest generation and published/candidate lifecycle share
    // PROJECTION_FRONTIER. retention::min_projection_durable_frontier consumes
    // that control row. A name suggesting a derived watermark is not a proof
    // that the whole mixed namespace can be reconstructed from commit bytes.
    assert_eq!(
        AuthoritativeNamespaceV1::ProjectionFrontier.class(),
        ReplicationAuthorityClassV1::ReplicatedAuthoritative
    );
}

#[test]
fn catalog_keeps_operational_delivery_and_unproven_indexes_authoritative() {
    use AuthoritativeNamespaceV1 as N;
    use ReplicationAuthorityClassV1 as C;
    for namespace in [
        N::OutboxStatus,
        N::EventConsumerDeliveries,
        N::SecondaryIndexes,
        N::IndexEpochs,
        N::IdempotencyLocators,
        N::ValidatedPrefixEntityHeads,
        N::VectorProjectionControls,
        N::ColumnarProjectionControls,
        N::ProjectionFrontier,
    ] {
        assert_eq!(namespace.class(), C::ReplicatedAuthoritative);
    }
    for namespace in [N::ProjectionState, N::ProjectionApplied] {
        assert_eq!(namespace.class(), C::RebuildableLocal);
    }
    assert_eq!(
        N::CleanCloseLifecycle.class(),
        C::ReplicationControl(ReplicationTransferV1::SourceOnly)
    );
    assert_eq!(
        N::NextChangelogTransaction.class(),
        C::ReplicationControl(ReplicationTransferV1::LineageShared)
    );
    assert_eq!(
        N::ReplicationFollowerState.class(),
        C::ReplicationControl(ReplicationTransferV1::FollowerLocal)
    );
}
