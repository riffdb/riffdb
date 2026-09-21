#![forbid(unsafe_code)]
//! The fencing catalog extends storage inventory without reinterpreting V1.
// req: REP-005, STO-012

use riffdb_storage_api::{
    AuthoritativeNamespaceV1, AuthoritativeNamespaceV2, AuthoritativeStateCatalogV1,
    AuthoritativeStateCatalogV2, ReplicationAuthorityClassV1, ReplicationTransferV1,
};
use std::collections::BTreeSet;

#[test]
fn fencing_catalog_preserves_every_existing_domain_and_refuses_unknowns() {
    let catalog = AuthoritativeStateCatalogV2;
    let mut tags = BTreeSet::new();
    let mut domains = BTreeSet::new();
    for namespace in catalog.namespaces() {
        assert!(tags.insert(namespace.tag()));
        assert!(domains.insert((namespace.table(), namespace.metadata_key())));
        assert_eq!(catalog.by_tag(namespace.tag()), Some(namespace));
        assert_eq!(
            catalog.lookup(
                namespace.table(),
                namespace.metadata_key().unwrap_or("test-key").as_bytes()
            ),
            Some(namespace)
        );
    }
    assert_eq!(tags.len(), AuthoritativeNamespaceV1::ALL.len() + 1);
    for old in AuthoritativeNamespaceV1::ALL {
        let current = catalog.by_tag(old.tag()).unwrap();
        assert_eq!(current, AuthoritativeNamespaceV2::Existing(old));
        assert_eq!(current.table(), old.table());
        assert_eq!(current.metadata_key(), old.metadata_key());
        assert_eq!(current.class(), old.class());
    }
    for tag in [0, 208 + 1, u16::MAX] {
        assert_eq!(catalog.by_tag(tag), None);
    }
    for key in [
        b"replication_primary_admission/v1\0".as_slice(),
        b"replication_primary_admission/v2",
        b"",
        &[0xff],
    ] {
        assert_eq!(catalog.lookup("meta", key), None);
    }
    assert_eq!(
        catalog.lookup("foreign", b"replication_primary_admission/v1"),
        None
    );
}

#[test]
fn primary_admission_is_exact_source_only_domain_and_never_a_v1_mutation() {
    let admission = AuthoritativeNamespaceV2::ReplicationPrimaryAdmission;
    assert_eq!(admission.tag(), 208);
    assert_eq!(admission.table(), "meta");
    assert_eq!(
        admission.metadata_key(),
        Some("replication_primary_admission/v1")
    );
    assert_eq!(
        admission.class(),
        ReplicationAuthorityClassV1::ReplicationControl(ReplicationTransferV1::SourceOnly)
    );
    assert_eq!(
        AuthoritativeStateCatalogV2.lookup("meta", b"replication_primary_admission/v1"),
        Some(admission)
    );
    assert_eq!(AuthoritativeStateCatalogV1.by_tag(admission.tag()), None);
    assert_eq!(
        AuthoritativeStateCatalogV1.lookup(
            admission.table(),
            admission.metadata_key().unwrap().as_bytes()
        ),
        None
    );
    // New catalog identity cannot silently relabel an existing history binding.
    assert_ne!(
        AuthoritativeStateCatalogV2.digest(),
        AuthoritativeStateCatalogV1.digest()
    );
    assert_eq!(
        AuthoritativeStateCatalogV1.canonical_fixture(),
        include_str!("../../../fixtures/replication/authoritative-state-catalog-v1.txt")
    );
}

#[test]
fn fencing_catalog_fixture_is_bounded_and_does_not_enable_production_handshake() {
    use riffdb_storage_api::{
        ChangelogFrameBindingV3, ChangelogFrameV3, ChangelogHistoryPointV3, ChangelogLineageV3,
        ChangelogTransactionSequence, LeadershipEpochV1, MAX_CHANGELOG_FRAME_BYTES,
        MAX_STAGED_COMMANDS, ReplicationHandshakeV3, ReplicationStreamErrorV3,
    };
    use riffdb_types::DatabaseId;
    let catalog = AuthoritativeStateCatalogV2;
    assert_eq!(
        catalog.canonical_fixture(),
        include_str!("../../../fixtures/replication/authoritative-state-catalog-v2.txt")
    );
    assert!(catalog.canonical_fixture().len() <= 16 * 1024);
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap();
    assert!(ChangelogFrameBindingV3::new(database, 1, 1, catalog.digest(), [0; 32]).is_ok());
    let lineage = ChangelogLineageV3::new(database, 1, LeadershipEpochV1::initial()).unwrap();
    let after = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).unwrap(),
        [0; 32],
        riffdb_types::DualFrontier::INITIAL,
    );
    assert_eq!(
        ReplicationHandshakeV3::new(
            lineage,
            after,
            ChangelogFrameV3::IDENTITY,
            catalog.digest(),
            MAX_CHANGELOG_FRAME_BYTES as u64,
            MAX_STAGED_COMMANDS as u64
        ),
        Err(ReplicationStreamErrorV3::UnsupportedCatalog)
    );
    assert!(
        ChangelogFrameBindingV3::new(
            database,
            1,
            1,
            AuthoritativeStateCatalogV1.digest(),
            [0; 32]
        )
        .is_ok()
    );
}
