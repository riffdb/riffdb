//! Release-level durable-format safety tests.

use riffdb_storage_api::{
    AlphaFormatEpoch, CompatibilityFixtureDigest, DurableFormatAction, DurableFormatIdentity,
    DurableFormatMarker, DurableFormatMarkerError, DurableFormatWriter, SafeFormatCommand,
    current_durable_format_manifest, current_durable_format_marker, decode_durable_format_marker,
    encode_durable_format_marker, preflight_durable_format, preflight_durable_format_marker,
};
use riffdb_types::SchemaHash;

#[test]
// req: REP-007, AFC-007
fn pre_prefix_registry_marker_requires_its_matching_binary_before_any_open() {
    let current = current_durable_format_marker();
    // Exact registry frozen at c5ae7c24, before the approved V7/V6 successors.
    let prior = SchemaHash::from_bytes([
        0x78, 0xe1, 0x3c, 0x03, 0x24, 0x88, 0x5b, 0x31, 0x2f, 0x28, 0x2c, 0xbb, 0x75, 0x16, 0x78,
        0x8b, 0x70, 0xb1, 0x13, 0x87, 0xb9, 0x38, 0x52, 0xc4, 0xd6, 0x9b, 0x4d, 0x20, 0x69, 0xfd,
        0xdc, 0xaf,
    ]);
    assert_ne!(prior, current.registry_digest());
    let retained = DurableFormatMarker::new(
        current.identity(),
        prior,
        current.compatibility_fixture_digest(),
    );
    assert_eq!(
        preflight_durable_format_marker(retained),
        Err(DurableFormatMarkerError::ManifestMismatch)
    );
}

#[test]
fn current_manifest_names_every_closed_format_family() {
    let manifest = current_durable_format_manifest();

    assert_eq!(manifest.epoch(), AlphaFormatEpoch::new(1).unwrap());
    assert_eq!(manifest.writer(), DurableFormatWriter::new(3));
    assert_eq!(manifest.release(), env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest.readable_storage_versions(), &[1, 2]);
    assert_eq!(manifest.writable_storage_versions(), &[2]);
    assert_eq!(manifest.readable_redb_layout_versions(), &[1]);
    assert_eq!(manifest.writable_redb_layout_versions(), &[1]);
    assert_eq!(manifest.readable_registry_versions(), &[2]);
    assert_eq!(manifest.writable_registry_versions(), &[2]);
    assert_eq!(manifest.readable_journal_frame_versions(), &[2, 3, 4]);
    assert_eq!(manifest.writable_journal_frame_versions(), &[4]);
    assert_eq!(manifest.readable_journal_extent_versions(), &[3, 4]);
    assert_eq!(manifest.writable_journal_extent_versions(), &[4]);
    assert_eq!(manifest.readable_backup_versions(), &[1]);
    assert_eq!(manifest.writable_backup_versions(), &[1]);
    assert_eq!(manifest.readable_receipt_versions(), &[1, 2, 3, 4]);
    assert_eq!(manifest.writable_receipt_versions(), &[1, 2, 3, 4]);
    assert_eq!(
        manifest.readable_offline_maintenance_receipt_versions(),
        &[1, 2, 3, 4]
    );
    assert_eq!(
        manifest.writable_offline_maintenance_receipt_versions(),
        &[1, 2, 3, 4]
    );
    assert_eq!(
        manifest.readable_contract_migration_check_receipt_versions(),
        &[2]
    );
    assert_eq!(
        manifest.writable_contract_migration_check_receipt_versions(),
        &[2]
    );
    assert_eq!(manifest.readable_format_upgrade_receipt_versions(), &[1]);
    assert_eq!(manifest.writable_format_upgrade_receipt_versions(), &[1]);
    assert_eq!(manifest.readable_format_marker_versions(), &[1]);
    assert_eq!(manifest.writable_format_marker_versions(), &[1]);
    assert_eq!(
        manifest.readable_records().len(),
        riffdb_proto::durable::READABLE_RECORD_SCHEMA_COUNT
    );
    assert_eq!(
        manifest.writable_records().len(),
        riffdb_proto::durable::WRITABLE_RECORD_SCHEMA_COUNT
    );
    assert_ne!(manifest.compatibility_fixture_digest().as_bytes(), &[0; 32]);
    assert_eq!(
        manifest.minimum_supported_source_release(),
        "0.1.0-pre-format-manifest"
    );
    assert_eq!(manifest.maximum_supported_source_release(), "0.1.0");
    assert_eq!(
        manifest.release_edges()[0].source_release(),
        "0.1.0-pre-format-manifest"
    );
    assert_eq!(manifest.release_edges()[0].target_release(), "0.1.0");
    assert!(!manifest.downgrade_supported());
}

#[test]
fn preflight_is_closed_and_never_offers_force_ignore_or_reset() {
    let current = current_durable_format_manifest().identity();
    assert_eq!(
        preflight_durable_format(current),
        Ok(DurableFormatAction::OpenCurrent)
    );

    let predecessor = DurableFormatIdentity::new(
        AlphaFormatEpoch::new(1).unwrap(),
        DurableFormatWriter::new(0),
    );
    assert_eq!(
        preflight_durable_format(predecessor),
        Ok(DurableFormatAction::OfflineInPlace {
            backup_required: true,
            free_space_source_multiples: 2,
            downtime_required: true,
            one_way: true,
            next_command: SafeFormatCommand::Upgrade,
        })
    );

    let newer = DurableFormatIdentity::new(
        AlphaFormatEpoch::new(2).unwrap(),
        DurableFormatWriter::new(1),
    );
    let error = preflight_durable_format(newer).unwrap_err();
    assert_eq!(error.current(), newer);
    assert_eq!(error.binary(), current);
    assert_eq!(error.next_command(), SafeFormatCommand::UseMatchingBinary);
    assert!(!format!("{error:?} {error}").contains("force"));
    assert!(!format!("{error:?} {error}").contains("ignore"));
    assert!(!format!("{error:?} {error}").contains("reset"));
}

#[test]
fn retained_marker_is_fixed_checksummed_and_exact_to_the_manifest() {
    let marker = current_durable_format_marker();
    let encoded = encode_durable_format_marker(marker);
    assert_eq!(decode_durable_format_marker(&encoded), Ok(marker));
    assert_eq!(
        preflight_durable_format_marker(marker),
        Ok(DurableFormatAction::OpenCurrent)
    );

    for end in 0..encoded.len() {
        assert!(decode_durable_format_marker(&encoded[..end]).is_err());
    }
    let mut corrupted = encoded;
    corrupted[50] ^= 1;
    assert_eq!(
        decode_durable_format_marker(&corrupted),
        Err(DurableFormatMarkerError::ChecksumMismatch)
    );

    let wrong_registry = DurableFormatMarker::new(
        marker.identity(),
        SchemaHash::from_bytes([7; 32]),
        marker.compatibility_fixture_digest(),
    );
    assert_eq!(
        preflight_durable_format_marker(wrong_registry),
        Err(DurableFormatMarkerError::ManifestMismatch)
    );
    let wrong_fixtures = DurableFormatMarker::new(
        marker.identity(),
        marker.registry_digest(),
        CompatibilityFixtureDigest::from_bytes([9; 32]),
    );
    assert_eq!(
        preflight_durable_format_marker(wrong_fixtures),
        Err(DurableFormatMarkerError::ManifestMismatch)
    );
}

// req: STO-030, STO-032, REC-005
#[test]
fn prune_writer_requires_backup_bound_offline_upgrade_from_prior_writer() {
    let current = current_durable_format_manifest();
    let prior = DurableFormatIdentity::new(current.epoch(), DurableFormatWriter::new(1));
    assert!(matches!(
        preflight_durable_format(prior),
        Ok(DurableFormatAction::OfflineInPlace {
            backup_required: true,
            downtime_required: true,
            one_way: true,
            ..
        })
    ));
    let wrong_prior_marker = DurableFormatMarker::new(
        prior,
        current.registry_digest(),
        CompatibilityFixtureDigest::from_bytes([9; 32]),
    );
    assert_eq!(
        preflight_durable_format_marker(wrong_prior_marker),
        Err(DurableFormatMarkerError::ManifestMismatch)
    );
    let future = DurableFormatIdentity::new(current.epoch(), DurableFormatWriter::new(4));
    assert!(preflight_durable_format(future).is_err());
}

// req: AFC-001, AFC-002, REC-005, REC-006, REC-007
#[test]
fn registry_completion_marker_requires_exact_backup_bound_writer_edge() {
    let registry = SchemaHash::from_bytes([
        0xa1, 0x41, 0xb7, 0x65, 0xc2, 0xd4, 0x68, 0xa7, 0xc2, 0x70, 0x46, 0x15, 0x53, 0xaa, 0xd6,
        0xa5, 0x6f, 0x9d, 0x2a, 0xf3, 0xa0, 0xe4, 0xf6, 0x30, 0x27, 0x15, 0x12, 0x27, 0xed, 0x4b,
        0x40, 0xa3,
    ]);
    let corpus = CompatibilityFixtureDigest::from_bytes([
        0x09, 0xb3, 0x89, 0xc8, 0x27, 0xda, 0x34, 0xe5, 0x52, 0x73, 0x85, 0xff, 0x74, 0x3b, 0xde,
        0x7a, 0x8a, 0x8b, 0xd6, 0x10, 0x1f, 0xc6, 0x20, 0x19, 0x1c, 0x9b, 0x68, 0x42, 0xb7, 0x7f,
        0xad, 0xc0,
    ]);
    let identity = DurableFormatIdentity::new(
        AlphaFormatEpoch::new(1).unwrap(),
        DurableFormatWriter::new(2),
    );
    let predecessor = DurableFormatMarker::new(identity, registry, corpus);
    assert_eq!(
        preflight_durable_format_marker(predecessor),
        Ok(DurableFormatAction::OfflineInPlace {
            backup_required: true,
            free_space_source_multiples: 2,
            downtime_required: true,
            one_way: true,
            next_command: SafeFormatCommand::Upgrade,
        })
    );
    assert_eq!(
        current_durable_format_manifest().writer(),
        DurableFormatWriter::new(3)
    );
    for (registry, corpus) in [
        (SchemaHash::from_bytes([0; 32]), corpus),
        (registry, CompatibilityFixtureDigest::from_bytes([0; 32])),
    ] {
        assert_eq!(
            preflight_durable_format_marker(DurableFormatMarker::new(identity, registry, corpus)),
            Err(DurableFormatMarkerError::ManifestMismatch)
        );
    }
}
