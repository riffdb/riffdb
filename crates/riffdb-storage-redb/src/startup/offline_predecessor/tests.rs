//! Private qualification uses the unchanged genuine writer-4 artifact set.
// req: REC-001, REC-005, REC-006, STO-032, REP-005
use super::*;
use std::io::Read;

struct Fixture {
    _scope: crate::test_path::ScopedDirectory,
    source: PathBuf,
    backup: PathBuf,
    original_hashes: BTreeMap<PathBuf, [u8; 32]>,
}
fn file_hash(path: &Path) -> [u8; 32] {
    let mut file = std::fs::File::open(path).unwrap();
    let mut digest = Sha256::new();
    let mut bytes = [0; 64 * 1024];
    loop {
        let count = file.read(&mut bytes).unwrap();
        if count == 0 {
            break;
        }
        digest.update(&bytes[..count]);
    }
    digest.finalize().into()
}
impl Fixture {
    fn copy() -> Self {
        // Required by the held private runner, which reconstructs and hashes the
        // frozen artifact set before launching these exact tests. This is not a
        // runtime hook and does not alter/relabel the original writer fixture.
        let original = PathBuf::from(
            std::env::var_os("RIFFDB_PRIVATE_WRITER4_CUT_FIXTURE_ROOT")
                .expect("owned frozen writer-4 fixture root"),
        );
        let scope = crate::test_path::ScopedDirectory::new("readonly-predecessor-owner");
        let source = scope.join("source.redb");
        let backup = scope.join("backup");
        std::fs::create_dir(&backup).unwrap();
        std::fs::copy(original.join("source/db.redb"), &source).unwrap();
        std::fs::copy(
            original.join("source/db.redb.riffjournal"),
            crate::journal::journal_path(&source),
        )
        .unwrap();
        std::fs::copy(
            original.join("source/db.redb.riffdb-format-v1"),
            crate::durable_format_marker_path(&source),
        )
        .unwrap();
        let mut count = 0;
        for entry in std::fs::read_dir(original.join("backup")).unwrap() {
            let entry = entry.unwrap();
            count += 1;
            assert!(count <= 8 && entry.file_type().unwrap().is_file());
            std::fs::copy(entry.path(), backup.join(entry.file_name())).unwrap();
        }
        let mut paths = vec![
            source.clone(),
            crate::journal::journal_path(&source),
            crate::durable_format_marker_path(&source),
        ];
        paths.extend(
            std::fs::read_dir(&backup)
                .unwrap()
                .map(|entry| entry.unwrap().path()),
        );
        let original_hashes = paths
            .into_iter()
            .map(|path| {
                let digest = file_hash(&path);
                (path, digest)
            })
            .collect();
        Self {
            _scope: scope,
            source,
            backup,
            original_hashes,
        }
    }
    fn owner(&self) -> OfflinePredecessorMaintenanceV1 {
        OfflinePredecessorMaintenanceV1::acquire(
            &self.source,
            &self.backup,
            inputs(),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap()
    }
    fn assert_unchanged(&self) {
        for (path, expected) in &self.original_hashes {
            assert_eq!(&file_hash(path), expected);
        }
        let name = self.source.file_name().unwrap().to_str().unwrap();
        assert!(
            !self
                .source
                .with_file_name(format!("{name}.riffdb-format-upgrade-v1"))
                .exists()
        );
    }
}
fn inputs() -> StartupValidationInputs {
    let digest = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![digest]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![digest]).unwrap(),
    )
}

#[test]
fn readonly_historical_identity_requires_the_actual_backup_registry_owner() {
    let fixture = Fixture::copy();
    let owner = fixture.owner();
    let read = owner.artifacts.backup.begin_read().unwrap();
    assert!(
        matches!(crate::store::read_identity_from_read_transaction(&read), Err(error) if error.kind() == StorageErrorKind::IncompatibleFormat)
    );
    let identity = owner.artifacts.backup.history_database_id().unwrap();
    assert!(
        matches!(crate::changelog_v3_roots::read_checkpoint_roots(&read), Err(error) if error.kind() == StorageErrorKind::IncompatibleFormat)
    );
    let mut scope =
        crate::history_stub::begin_immutable_backup_reads(&owner.artifacts.backup).unwrap();
    let history =
        crate::changelog_v3_roots::read_checkpoint_roots_owned_backup_v1(&owner.artifacts.backup)
            .unwrap()
            .unwrap();
    assert_eq!(
        history.lineage().catalog_digest(),
        riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
    );
    assert_eq!(history.lineage().database_id(), identity);
    scope.finish().unwrap();
    drop(scope);
    let retained = super::super::read_retained_metadata_with_admission(
        &read,
        super::super::RetainedRegistryAdmissionV1::OfflineWriter4(&OfflineBackupScanOwnerV1 {
            artifacts: &owner.artifacts,
        }),
    )
    .unwrap();
    assert_eq!(identity, retained.database_id());
    drop(read);
    fixture.assert_unchanged();
}

#[test]
fn readonly_writer4_maintenance_completes_under_primary_custody_without_live_authority() {
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    assert_eq!(
        proof.observation.history.lineage().catalog_digest(),
        riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
    );
    assert_eq!(
        proof.observation.database_id,
        proof.observation.history.lineage().database_id()
    );
    assert_eq!(
        proof.observation.original_registry,
        proof.artifacts.original_registry()
    );
    assert!(proof.observation.active_pointer_digest.is_some());
    assert!(matches!(
        redb::ReadOnlyDatabase::open(&fixture.source),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    fixture.assert_unchanged();
    drop(proof);
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
}

#[test]
fn readonly_primary_path_substitution_refuses_before_accepted_and_preserves_original_inode() {
    let fixture = Fixture::copy();
    let owner = fixture.owner();
    let held = fixture.source.with_file_name("held-original.redb");
    std::fs::rename(&fixture.source, &held).unwrap();
    std::fs::copy(&held, &fixture.source).unwrap();
    assert_eq!(file_hash(&held), file_hash(&fixture.source));
    assert!(
        matches!(owner.artifacts.verify(), Err(error) if error.kind() == StorageErrorKind::CorruptData)
    );
    drop(owner);
    std::fs::remove_file(&fixture.source).unwrap();
    std::fs::rename(&held, &fixture.source).unwrap();
    fixture.assert_unchanged();
}

#[test]
fn readonly_history_scope_accepts_inline_nesting_and_refuses_foreign_same_id_engine() {
    let first = Fixture::copy();
    let second = Fixture::copy();
    let first_owner = first.owner();
    let second_owner = second.owner();
    assert_eq!(
        first_owner.artifacts.backup.history_database_id().unwrap(),
        second_owner.artifacts.backup.history_database_id().unwrap()
    );
    assert!(
        !first_owner
            .artifacts
            .backup
            .engine()
            .same_allocation(second_owner.artifacts.backup.engine())
    );
    let mut outer =
        crate::history_stub::begin_immutable_backup_reads(&first_owner.artifacts.backup).unwrap();
    let mut nested =
        crate::history_stub::begin_immutable_backup_reads(&first_owner.artifacts.backup).unwrap();
    nested.finish().unwrap();
    drop(nested);
    assert!(
        matches!(crate::history_stub::begin_immutable_backup_reads(&second_owner.artifacts.backup), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    outer.finish().unwrap();
    drop(outer);
    // Failure of the foreign scope cannot poison the original or foreign owner.
    first_owner.artifacts.backup.verify().unwrap();
    second_owner.artifacts.backup.verify().unwrap();
    first.assert_unchanged();
    second.assert_unchanged();
}

#[test]
fn readonly_parallel_workers_resolve_genuine_stubs_from_their_exact_owned_backup() {
    let fixture = Fixture::copy();
    let owner = fixture.owner();
    let read = owner.artifacts.backup.begin_read().unwrap();
    let history = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let raw = history
        .iter()
        .unwrap()
        .map(|row| row.unwrap().1.value().to_vec())
        .find(|bytes| {
            crate::history_stub::HistoryStub::decode(bytes)
                .unwrap()
                .is_some()
        })
        .expect("genuine Standard writer fixture retains a history stub");
    drop(history);
    drop(read);
    let mut parent_scope =
        crate::history_stub::begin_immutable_backup_reads(&owner.artifacts.backup).unwrap();
    let expected = crate::history_stub::decode_history_value(&raw)
        .unwrap()
        .encode()
        .unwrap();
    // A conflicting ambient resolver must never supply these worker reads.
    let _ambient = crate::history_stub::HistoryResolver::register_backup(
        owner.artifacts.backup.history_database_id().unwrap(),
        &fixture.backup.join("absent-ambient"),
    );
    let results = super::super::offline_parallel::inspect_rows(16, |_| {
        let mut scope = crate::history_stub::begin_immutable_backup_reads(&owner.artifacts.backup)?;
        let receipt = crate::history_stub::decode_history_value(&raw)?;
        let encoded = receipt.encode().map_err(|_| corrupt())?;
        scope.finish()?;
        Ok(encoded)
    })
    .unwrap();
    for result in results {
        assert_eq!(result.unwrap(), expected);
    }
    parent_scope.finish().unwrap();
    drop(parent_scope);
    fixture.assert_unchanged();
}

#[test]
fn readonly_source_absence_predicate_accepts_empty_containers_and_refuses_rows_or_allocator() {
    // Pure structural predicate only; this scratch engine never becomes a
    // branded offline completion, cut proof or Source usage-zero fact.
    let fixture = Fixture::copy();
    let scratch = fixture.source.with_file_name("layout-predicate.redb");
    std::fs::copy(&fixture.source, &scratch).unwrap();
    let database = redb::Database::open(&scratch).unwrap();
    let domains = [
        riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceRows,
        riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceObjects,
        riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceRoots,
        riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceMaintenance,
    ];
    {
        let write = database.begin_write().unwrap();
        for domain in domains {
            drop(
                write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(domain.table()))
                    .unwrap(),
            );
        }
        write.commit().unwrap();
    }
    let read = database.begin_read().unwrap();
    OfflinePredecessorArtifactsV1::validate_source_absence(&read).unwrap();
    OfflinePredecessorArtifactsV1::validate_v2_layout(&read).unwrap();
    drop(read);
    let definition = TableDefinition::<&[u8], &[u8]>::new(domains[0].table());
    {
        let write = database.begin_write().unwrap();
        write
            .open_table(definition)
            .unwrap()
            .insert(b"unexpected".as_slice(), b"row".as_slice())
            .unwrap();
        write.commit().unwrap();
    }
    let read = database.begin_read().unwrap();
    assert!(
        matches!(OfflinePredecessorArtifactsV1::validate_source_absence(&read), Err(error) if error.kind() == StorageErrorKind::CorruptData)
    );
    drop(read);
    {
        let write = database.begin_write().unwrap();
        write
            .open_table(definition)
            .unwrap()
            .remove(b"unexpected".as_slice())
            .unwrap();
        let allocator = riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceAllocator
            .metadata_key()
            .unwrap();
        write
            .open_table(META)
            .unwrap()
            .insert(allocator, b"unexpected".as_slice())
            .unwrap();
        write.commit().unwrap();
    }
    let read = database.begin_read().unwrap();
    assert!(
        matches!(OfflinePredecessorArtifactsV1::validate_source_absence(&read), Err(error) if error.kind() == StorageErrorKind::CorruptData)
    );
    fixture.assert_unchanged();
}

#[test]
fn readonly_maintenance_handoff_retains_only_fixed_observations_after_the_joined_thread() {
    const _: () = assert!(!std::mem::needs_drop::<FixedMaintenanceObservationV1>());
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    // This result can retain only its actual backup/engine custody and fixed
    // scalars; the complete catalog/command/cache/TLS graphs were destroyed by
    // the joined validation thread before this result could be constructed.
    proof.artifacts.verify().unwrap();
    assert!(proof.observation.open_session_id.get() > 0);
    fixture.assert_unchanged();
}

#[test]
fn readonly_original_runtime_zero_legacy_controls_attests_exact_framing_without_mutation() {
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let history = proof.observation.history;
    let fact = &proof.observation.legacy_controls;
    assert_eq!(
        history.lineage().catalog_digest(),
        riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
    );
    // The physical AUDIT table has one catalog record. The four command
    // service audits are supplied by the existing command-audit index, not
    // additional physical rows in this stream. No legacy control is present.
    assert_eq!(fact.visited_rows, 1);
    assert_eq!(fact.control_count, 0);
    assert_eq!(fact.key_value_bytes, 0);
    // The genuine predecessor is detached with no source policies. This checks
    // only that policy subcondition, not jobs/captured work or whole cut drain.
    assert_eq!(proof.observation.source_policies.retired_policy_count, 0);
    assert_eq!(
        proof
            .observation
            .source_policies
            .retired_policy_native_bytes,
        0
    );
    // Independent accepted D7 zero-control framing, including exact original
    // database, incarnation, epoch and administration head. No production helper.
    let mut framed = Vec::new();
    framed.extend_from_slice(b"riffdb/native-catalog-legacy-controls/v1");
    framed.push(0);
    framed.extend_from_slice(history.lineage().database_id().as_bytes());
    framed.extend_from_slice(&history.lineage().history_incarnation().to_be_bytes());
    framed.extend_from_slice(&history.lineage().leadership_epoch().get().to_be_bytes());
    framed.extend_from_slice(
        &history
            .tail()
            .frontier()
            .administration()
            .map_or(0, |s| s.get())
            .to_be_bytes(),
    );
    framed.extend_from_slice(&history.lineage().catalog_digest());
    framed.extend_from_slice(&0u64.to_be_bytes());
    framed.extend_from_slice(&0u64.to_be_bytes());
    assert_eq!(
        fact.commitment.as_bytes(),
        &<[u8; 32]>::from(Sha256::digest(&framed))
    );
    // Substituting any actual header class changes the commitment. This is
    // framing coverage, not a fabricated foreign control/publication success.
    let database_offset = b"riffdb/native-catalog-legacy-controls/v1".len() + 1;
    for offset in [
        database_offset,
        database_offset + 16,
        database_offset + 24,
        database_offset + 32,
        database_offset + 40,
    ] {
        let mut substituted = framed.clone();
        substituted[offset] ^= 1;
        assert_ne!(
            fact.commitment.as_bytes(),
            &<[u8; 32]>::from(Sha256::digest(&substituted))
        );
    }
    fixture.assert_unchanged();
    drop(proof);
    fixture.assert_unchanged();
}

#[test]
fn immutable_scope_finish_and_join_retire_actual_tls_weak_backing_before_handoff() {
    let fixture = Fixture::copy();
    let mut owner = fixture.owner();
    assert!(
        owner
            .artifacts
            .backup
            .engine_has_no_strong_or_weak_aliases_for_test()
    );
    let owner = std::thread::spawn(move || {
        let mut scope =
            crate::history_stub::begin_immutable_backup_reads(&owner.artifacts.backup).unwrap();
        let mut nested =
            crate::history_stub::begin_immutable_backup_reads(&owner.artifacts.backup).unwrap();
        nested.finish().unwrap();
        drop(nested);
        scope.finish().unwrap();
        drop(scope);
        owner
    })
    .join()
    .unwrap();
    let mut owner = owner;
    assert!(
        owner
            .artifacts
            .backup
            .engine_has_no_strong_or_weak_aliases_for_test(),
        "actual joined worker left no TLS strong OR weak engine alias"
    );
    owner.artifacts.verify().unwrap();
    fixture.assert_unchanged();
}

#[test]
fn immutable_scope_cancellation_clears_actual_tls_before_invalidated_owner_retires() {
    let fixture = Fixture::copy();
    let mut owner = fixture.owner();
    let scope = crate::history_stub::begin_immutable_backup_reads(&owner.artifacts.backup).unwrap();
    drop(scope);
    assert!(
        owner
            .artifacts
            .backup
            .engine_has_no_strong_or_weak_aliases_for_test()
    );
    assert!(matches!(owner.artifacts.verify(), Err(error)
        if error.kind() == StorageErrorKind::Unavailable));
    drop(owner);
    // A fresh actual owner can start after cancellation; no abandoned TLS
    // affinity may adopt it merely because its durable identity is unchanged.
    let mut retry = fixture.owner();
    let mut scope =
        crate::history_stub::begin_immutable_backup_reads(&retry.artifacts.backup).unwrap();
    scope.finish().unwrap();
    drop(scope);
    assert!(
        retry
            .artifacts
            .backup
            .engine_has_no_strong_or_weak_aliases_for_test()
    );
    fixture.assert_unchanged();
}

#[path = "source_phase_tests.rs"]
mod source_phase_tests;

#[path = "source_execution_domain_tests.rs"]
mod source_execution_domain_tests;
