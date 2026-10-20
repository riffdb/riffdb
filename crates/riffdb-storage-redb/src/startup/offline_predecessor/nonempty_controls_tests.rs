//! Genuine old public builder artifacts; maintenance observation only, no cut/drain authority.
// req: REC-001, REC-005, STO-032, REP-005, REP-006
#![allow(clippy::drop_non_drop)]
use super::*;
use riffdb_storage_api::{
    FollowerRegistrationPhaseV1, ReplicationAdministrationActionV1, ReplicationSourceHoldStateV1,
    StoredAdministrationAuditRecordV1, proto_codec::decode_replication_source_hold,
};
use std::io::Read;

struct OriginalControlsFixture {
    _scope: crate::test_path::ScopedDirectory,
    source: PathBuf,
    backup: PathBuf,
    external_repository: PathBuf,
    external_manifest_path: PathBuf,
    external_manifest: riffdb_storage_api::ReplicationBootstrapManifestV1,
    hashes: BTreeMap<PathBuf, [u8; 32]>,
    inventory: BTreeMap<PathBuf, bool>,
    validation_clock: Timestamp,
    generation: u64,
}
fn file_digest(path: &Path) -> [u8; 32] {
    let mut file = std::fs::File::open(path).unwrap();
    let length = file.metadata().unwrap().len();
    assert!(length <= 64 * 1024 * 1024);
    let mut remaining = length;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    while remaining != 0 {
        let bytes = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        file.read_exact(&mut buffer[..bytes]).unwrap();
        digest.update(&buffer[..bytes]);
        remaining -= u64::try_from(bytes).unwrap();
    }
    let mut extra = [0u8; 1];
    assert_eq!(
        file.read(&mut extra).unwrap(),
        0,
        "native artifact grew during measurement"
    );
    assert_eq!(file.metadata().unwrap().len(), length);
    digest.finalize().into()
}
fn native_inventory(roots: &[PathBuf]) -> BTreeMap<PathBuf, bool> {
    fn visit(path: &Path, depth: u8, result: &mut BTreeMap<PathBuf, bool>) {
        assert!(depth <= 8 && result.len() < 256);
        let metadata = std::fs::symlink_metadata(path).unwrap();
        assert!(!metadata.file_type().is_symlink());
        let directory = metadata.is_dir();
        assert!(directory || metadata.is_file() && metadata.len() <= 64 * 1024 * 1024);
        assert!(result.insert(path.to_path_buf(), directory).is_none());
        if directory {
            let mut entries = 0;
            for entry in std::fs::read_dir(path).unwrap() {
                entries += 1;
                assert!(entries <= 128);
                visit(
                    &entry.unwrap().path(),
                    depth.checked_add(1).unwrap(),
                    result,
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    for root in roots {
        visit(root, 0, &mut result);
    }
    result
}
impl OriginalControlsFixture {
    fn copy(kind: &str) -> Self {
        assert!(matches!(kind, "registered" | "retired"));
        let original = PathBuf::from(
            std::env::var_os("RIFFDB_PRIVATE_WRITER4_PUBLIC_CONTROLS_ROOT")
                .expect("actual emitter artifact root"),
        );
        let scope = crate::test_path::ScopedDirectory::new("original-public-writer4-controls");
        let source_directory = scope.join("source");
        let backup = scope.join("backup");
        let mut hashes = BTreeMap::new();
        fn copy_tree(
            from: &Path,
            to: &Path,
            depth: u8,
            files: &mut u16,
            bytes: &mut u64,
            hashes: &mut BTreeMap<PathBuf, [u8; 32]>,
        ) {
            assert!(depth <= 8);
            std::fs::create_dir(to).unwrap();
            let mut entries = 0;
            for entry in std::fs::read_dir(from).unwrap() {
                entries += 1;
                assert!(entries <= 128);
                let entry = entry.unwrap();
                let name = entry.file_name();
                let name = name.to_str().expect("bounded native artifact component");
                assert!(
                    name.len() <= 128
                        && ![".keys", ".key", ".crt", ".pem", ".token", ".toml"]
                            .iter()
                            .any(|suffix| name.ends_with(suffix))
                );
                let metadata = std::fs::symlink_metadata(entry.path()).unwrap();
                assert!(!metadata.file_type().is_symlink());
                let target = to.join(entry.file_name());
                if metadata.is_dir() {
                    copy_tree(
                        &entry.path(),
                        &target,
                        depth.checked_add(1).unwrap(),
                        files,
                        bytes,
                        hashes,
                    );
                } else {
                    assert!(metadata.is_file() && metadata.len() <= 64 * 1024 * 1024);
                    *files = files.checked_add(1).unwrap();
                    *bytes = bytes.checked_add(metadata.len()).unwrap();
                    assert!(*files <= 128 && *bytes <= 512 * 1024 * 1024);
                    assert_eq!(
                        std::fs::copy(entry.path(), &target).unwrap(),
                        metadata.len()
                    );
                    assert!(
                        hashes
                            .insert(target.clone(), file_digest(&target))
                            .is_none()
                    );
                }
            }
        }
        let mut files = 0;
        let mut bytes = 0;
        for (part, target) in [("source", &source_directory), ("backup", &backup)] {
            copy_tree(
                &original.join(kind).join(part),
                target,
                0,
                &mut files,
                &mut bytes,
                &mut hashes,
            );
        }
        // Exact actual1977 custom artifact, outside the canonical native sibling.
        // Copy and preserve it; do not open a repository, register, reap or resume.
        let external_repository = scope.join("custom-bootstrap-repository");
        copy_tree(
            &original.join("custom-bootstrap-repository"),
            &external_repository,
            0,
            &mut files,
            &mut bytes,
            &mut hashes,
        );
        let original_manifest = original.join("builder-manifest.riffbootstrap");
        let metadata = std::fs::symlink_metadata(&original_manifest).unwrap();
        assert!(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= 512);
        let external_manifest_path = scope.join("builder-manifest.riffbootstrap");
        assert_eq!(
            std::fs::copy(&original_manifest, &external_manifest_path).unwrap(),
            metadata.len()
        );
        assert!(
            hashes
                .insert(
                    external_manifest_path.clone(),
                    file_digest(&external_manifest_path)
                )
                .is_none()
        );
        let mut encoded = Vec::new();
        std::fs::File::open(&external_manifest_path)
            .unwrap()
            .take(513)
            .read_to_end(&mut encoded)
            .unwrap();
        assert!(encoded.len() <= 512 && encoded.len() as u64 == metadata.len());
        let external_manifest =
            riffdb_storage_api::ReplicationBootstrapManifestV1::decode(&encoded).unwrap();
        assert_eq!(external_manifest.encode().unwrap(), encoded);
        assert_eq!(
            external_manifest.fence().hold_id(),
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([0x72; 16]).unwrap()
        );
        assert!(external_manifest.page_count() > 0 && external_manifest.row_count() > 0);
        assert_eq!(
            hashes
                .keys()
                .filter(|p| p.starts_with(&external_repository)
                    && p.file_name().is_some_and(|n| n == "transfer.redb"))
                .count(),
            1
        );
        let seconds = std::env::var(if kind == "registered" {
            "RIFFDB_PRIVATE_WRITER4_REGISTERED_CLOCK_SECONDS"
        } else {
            "RIFFDB_PRIVATE_WRITER4_RETIRED_CLOCK_SECONDS"
        })
        .unwrap()
        .parse()
        .unwrap();
        let nanos = std::env::var(if kind == "registered" {
            "RIFFDB_PRIVATE_WRITER4_REGISTERED_CLOCK_NANOSECONDS"
        } else {
            "RIFFDB_PRIVATE_WRITER4_RETIRED_CLOCK_NANOSECONDS"
        })
        .unwrap()
        .parse()
        .unwrap();
        let generation = std::env::var("RIFFDB_PRIVATE_WRITER4_CONTROL_GENERATION")
            .unwrap()
            .parse()
            .unwrap();
        assert!(generation > 0);
        let inventory = native_inventory(&[
            source_directory.clone(),
            backup.clone(),
            external_repository.clone(),
            external_manifest_path.clone(),
        ]);
        Self {
            _scope: scope,
            source: source_directory.join("database.redb"),
            backup,
            external_repository,
            external_manifest_path,
            external_manifest,
            hashes,
            inventory,
            validation_clock: Timestamp::new(seconds, nanos).unwrap(),
            generation,
        }
    }
    fn owner(&self) -> OfflinePredecessorMaintenanceV1 {
        let digest = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        let inputs = StartupValidationInputs::new(
            self.validation_clock,
            ReadableCapabilityDigestInventory::new(vec![digest]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![digest]).unwrap(),
        );
        OfflinePredecessorMaintenanceV1::acquire(
            &self.source,
            &self.backup,
            inputs,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap()
    }
    fn assert_unchanged(&self) {
        assert_eq!(
            native_inventory(&[
                self.source.parent().unwrap().to_path_buf(),
                self.backup.clone(),
                self.external_repository.clone(),
                self.external_manifest_path.clone()
            ]),
            self.inventory
        );
        for (path, hash) in &self.hashes {
            assert_eq!(file_digest(path), *hash);
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

#[test]
fn custom_builder_original_registered_policy_refuses_and_preserves_external_artifact() {
    let fixture = OriginalControlsFixture::copy("registered");
    fixture.assert_unchanged();
    let error = match fixture.owner().validate_complete_maintenance() {
        Err(error) => error,
        Ok(_) => panic!("actual live registered policy cannot satisfy this drain subcondition"),
    };
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    fixture.assert_unchanged();
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
    // The runner additionally requires the existing safe diagnostic for
    // source_policy_drain_observation, after all original validation/D7 stages.
    println!("actual_custom_builder_live_policy_refused_without_source_or_external_mutation");
}

#[test]
fn custom_builder_original_retired_policy_attests_controls_without_bootstrap_hold() {
    let fixture = OriginalControlsFixture::copy("retired");
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    assert_eq!(proof.observation.source_policies.retired_policy_count, 1);
    assert_eq!(proof.observation.legacy_controls.control_count, 2);
    assert!(proof.observation.active_pointer_digest.is_none());
    let history = proof.observation.history;
    // Only after actual complete old validation: bind the unheld old external
    // manifest to the same original lineage. It is not a Source authority token.
    assert_eq!(
        fixture.external_manifest.fence().history().lineage(),
        history.lineage()
    );
    assert!(
        fixture
            .external_manifest
            .fence()
            .history()
            .tail()
            .sequence()
            < history.tail().sequence()
    );
    assert_eq!(
        history.lineage().catalog_digest(),
        riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
    );
    assert!(history.tail().frontier().application().is_none());
    // This read comes solely from the same actual retained original backup
    // owner. Raw envelopes below are measured after complete validation; the
    // sole existing decoder selects records, no second semantic parser exists.
    let read = proof.artifacts.backup.begin_read().unwrap();
    let mut expected = Sha256::new();
    expected.update(b"riffdb/native-catalog-legacy-controls/v1");
    expected.update([0]);
    expected.update(history.lineage().database_id().as_bytes());
    expected.update(history.lineage().history_incarnation().to_be_bytes());
    expected.update(history.lineage().leadership_epoch().get().to_be_bytes());
    expected.update(
        history
            .tail()
            .frontier()
            .administration()
            .map_or(0, |s| s.get())
            .to_be_bytes(),
    );
    expected.update(history.lineage().catalog_digest());
    let audit = read.open_table(AUDIT).unwrap();
    assert!(audit.len().unwrap() <= 64);
    let mut count = 0u64;
    let mut native_bytes = 0u64;
    let mut previous = None;
    for row in audit.iter().unwrap() {
        let (key, value) = row.unwrap();
        let Some(record) = decode_non_command_administration_audit(value.value()).unwrap() else {
            continue;
        };
        let StoredAdministrationAuditRecordV1::Replication(control) = record.value() else {
            continue;
        };
        assert!(count < 2 && value.value().len() <= 8192);
        assert_eq!(
            key.value(),
            keys::encode_audit_key(control.administration_sequence())
        );
        assert_eq!(key.value().len(), 9);
        if let Some(previous) = previous {
            assert!(control.administration_sequence() > previous);
        }
        previous = Some(control.administration_sequence());
        assert_eq!(control.after().generation().get(), fixture.generation);
        assert_eq!(control.after().hold().lineage(), history.lineage());
        if count == 0 {
            assert_eq!(
                control.action(),
                ReplicationAdministrationActionV1::RegisterFollower
            );
            // Public registration returns only after its separately sequenced
            // Succeeded service audit. The actual old builder captures that
            // later full frontier, not the transition receipt's frontier.
            let captured_admin = fixture
                .external_manifest
                .fence()
                .history()
                .tail()
                .frontier()
                .administration()
                .unwrap();
            assert_eq!(
                Some(captured_admin),
                control.administration_sequence().checked_next()
            );
            let terminal_key = keys::encode_audit_key(captured_admin);
            let terminal_value = audit.get(terminal_key.as_slice()).unwrap().unwrap();
            let terminal_record = decode_non_command_administration_audit(terminal_value.value())
                .unwrap()
                .unwrap();
            let StoredAdministrationAuditRecordV1::Service(terminal) = terminal_record.value()
            else {
                panic!("captured registration frontier must be its actual service success")
            };
            assert_eq!(terminal.administration_sequence(), captured_admin);
            assert_eq!(
                terminal.phase(),
                riffdb_types::ServiceAuditPhaseV1::Succeeded
            );
            assert_eq!(
                terminal.operation(),
                riffdb_types::ServiceOperationV1::RegisterFollower
            );
            assert_eq!(
                terminal.link(),
                riffdb_types::ServiceAuditLinkV1::ControlPlane {
                    administration_sequence: control.administration_sequence(),
                }
            );
            assert!(control.matches_service_result(terminal.operation(), terminal.targets()));
            let riffdb_storage_api::ReplicationAdministrationOriginV1::Explicit {
                request_id,
                principal,
                approval_id,
            } = control.origin()
            else {
                panic!("actual public registration must retain its explicit invocation")
            };
            assert_eq!(terminal.request_id(), *request_id);
            assert_eq!(terminal.principal(), Some(principal));
            assert_eq!(terminal.approval_id(), approval_id.as_ref());
            println!(
                "actual_old_manifest_frontier_is_registration_service_success_with_exact_origin_link"
            );
            assert_eq!(
                control.after().phase(),
                FollowerRegistrationPhaseV1::AwaitingBootstrap
            );
        } else {
            assert_eq!(
                control.action(),
                ReplicationAdministrationActionV1::RetireFollower
            );
            assert_eq!(
                control.after().phase(),
                FollowerRegistrationPhaseV1::Retired
            );
        }
        let length = u64::try_from(value.value().len()).unwrap();
        expected.update(key.value());
        expected.update(length.to_be_bytes());
        expected.update(value.value());
        native_bytes = native_bytes
            .checked_add(9)
            .and_then(|n| n.checked_add(length))
            .unwrap();
        count += 1;
    }
    assert_eq!(count, 2);
    drop(audit);
    expected.update(count.to_be_bytes());
    expected.update(native_bytes.to_be_bytes());
    let expected: [u8; 32] = expected.finalize().into();
    assert_eq!(
        proof.observation.legacy_controls.commitment.as_bytes(),
        &expected
    );
    assert_eq!(
        proof.observation.legacy_controls.key_value_bytes,
        native_bytes
    );
    let policies = read
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap();
    assert_eq!(policies.len().unwrap(), 1);
    let mut rows = policies.iter().unwrap();
    let (key, value) = rows.next().unwrap().unwrap();
    let decoded = decode_replication_source_hold(value.value()).unwrap();
    let ReplicationSourceHoldStateV1::Registered(policy) = decoded.value() else {
        panic!("the real retired image retains its registered policy tombstone")
    };
    assert_eq!(policy.phase(), FollowerRegistrationPhaseV1::Retired);
    assert_eq!(policy.generation().get(), fixture.generation);
    assert_eq!(policy.hold().lineage(), history.lineage());
    assert_eq!(key.value(), policy.hold().storage_key().as_slice());
    let policy_bytes =
        u64::try_from(key.value().len().checked_add(value.value().len()).unwrap()).unwrap();
    assert_eq!(
        proof
            .observation
            .source_policies
            .retired_policy_native_bytes,
        policy_bytes
    );
    assert!(rows.next().is_none());
    drop(decoded);
    drop(value);
    drop(key);
    drop(rows);
    drop(policies);
    drop(read);
    proof.artifacts.verify().unwrap();
    fixture.assert_unchanged();
    drop(proof);
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
    println!(
        "actual_custom_builder_retired_policy_two_controls_no_bootstrap_hold_and_exact_bytes_attested"
    );
}
