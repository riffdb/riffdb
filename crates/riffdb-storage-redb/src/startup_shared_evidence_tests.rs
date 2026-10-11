//! Compare shared snapshot facts with the independent physical-row validators.
// req: REC-001, REC-007, STO-030
use super::*;
use riffdb_storage_api::{StoredCommandCapsuleV1, StoredCommandCapsuleV2, StoredCommandSegmentV1};
use riffdb_types::{EventId, EventTypeId};

#[test]
fn exact_bundle_bindings_match_physical_validation_and_do_not_cross_snapshots() {
    let path = TestDatabasePath::new("shared-bundle-proof");
    let store = initialized_store(&path, database_id(0xb3));
    let valid = stored_bundle("shared-valid", 1, b"valid canonical bytes");
    let invalid_hash = StoredContractBundleV1::new(
        ContractLineage::new("shared-invalid").unwrap(),
        ContractVersion::new(1).unwrap(),
        valid.bundle_hash(),
        b"different canonical bytes".to_vec(),
    )
    .unwrap();
    let mismatched_key = stored_bundle("shared-wrong-row", 1, b"wrong physical identity");
    let plan = |lineage: &str, version, hash| {
        ExecutablePlanRef::new(
            ContractLineage::new(lineage).unwrap(),
            ContractVersion::new(version).unwrap(),
            hash,
            CommandId::first(),
            PlanHash::from_bytes([0x73; 32]),
        )
    };
    let plans = [
        plan("shared-valid", 1, valid.bundle_hash()),
        plan("shared-valid", 2, valid.bundle_hash()),
        plan("shared-valid", 1, invalid_hash.bundle_hash()),
        plan(
            "shared-valid",
            1,
            ContractBundleHash::from_bytes([0x99; 32]),
        ),
        plan("shared-invalid", 1, invalid_hash.bundle_hash()),
        plan("shared-miskeyed", 1, mismatched_key.bundle_hash()),
        plan("shared-malformed", 1, valid.bundle_hash()),
        plan("shared-absent", 1, valid.bundle_hash()),
    ];
    let write = store.shared.database.begin_write().unwrap();
    {
        let mut table = write.open_table(CONTRACT_BUNDLES).unwrap();
        for (lineage, bundle) in [
            ("shared-valid", &valid),
            ("shared-invalid", &invalid_hash),
            ("shared-miskeyed", &mismatched_key),
        ] {
            let key = keys::encode_contract_bundle_key(
                &ContractLineage::new(lineage).unwrap(),
                bundle.contract_version(),
            )
            .unwrap();
            let encoded = codec::encode_contract_bundle_v1(bundle).unwrap();
            table.insert(key.as_slice(), encoded.as_bytes()).unwrap();
        }
        let malformed = keys::encode_contract_bundle_key(
            &ContractLineage::new("shared-malformed").unwrap(),
            ContractVersion::new(1).unwrap(),
        )
        .unwrap();
        table
            .insert(malformed.as_slice(), &[0_u8, 1, 2][..])
            .unwrap();
        table
            .insert(
                &[0xff_u8][..],
                codec::encode_contract_bundle_v1(&valid).unwrap().as_bytes(),
            )
            .unwrap();
    }
    write.commit().unwrap();
    let pinned = store.shared.database.begin_read().unwrap();
    let bindings = collect_bundle_bindings(&pinned).unwrap();
    assert_eq!(
        bindings.len(),
        1,
        "only exact key/identity/hash agreement may be cached"
    );
    for candidate in &plans {
        assert_eq!(
            plan_bundle_is_bound(&bindings, candidate),
            plan_bundle_exists(&pinned, candidate).unwrap()
        );
    }
    assert!(plan_bundle_is_bound(&bindings, &plans[0]));
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(CONTRACT_BUNDLES)
        .unwrap()
        .remove(
            keys::encode_contract_bundle_key(valid.lineage(), valid.contract_version())
                .unwrap()
                .as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    assert!(
        plan_bundle_exists(&pinned, &plans[0]).unwrap(),
        "the original pin retains its exact validated bytes"
    );
    assert!(plan_bundle_is_bound(&bindings, &plans[0]));
    let next_pin = store.shared.database.begin_read().unwrap();
    let next_bindings = collect_bundle_bindings(&next_pin).unwrap();
    assert!(!plan_bundle_exists(&next_pin, &plans[0]).unwrap());
    assert!(
        !plan_bundle_is_bound(&next_bindings, &plans[0]),
        "a later pin must build its own proof"
    );
}

fn shared_evidence_capsule() -> StoredCommandCapsuleV2 {
    let bundle = stored_bundle("shared-command", 1, b"command catalog");
    let plan = checkpoint_count_plan(&bundle);
    shared_evidence_capsule_for_plan(plan)
}

fn shared_evidence_capsule_for_plan(plan: ExecutablePlanRef) -> StoredCommandCapsuleV2 {
    let sequence = CommitSequence::first();
    let base = history_commit(sequence, plan, &[]);
    let event_id = EventId::new(sequence, 0);
    let payload = CanonicalRecord::new(Vec::new()).unwrap();
    let event_type = EventTypeId::first();
    let event = riffdb_storage_api::StoredDurableEventV1::new(
        event_id,
        event_type,
        payload.clone(),
        riffdb_storage_api::derive_event_hash_v1(event_id, event_type, &payload).unwrap(),
    )
    .unwrap();
    let commit = StoredCommitRecordV1::new(
        sequence,
        base.admission_request_id(),
        base.plan().clone(),
        base.canonical_input_hash(),
        base.actor().clone(),
        base.logical_time(),
        base.partition_hash(),
        Vec::new(),
        base.read_dependencies().clone(),
        Vec::new(),
        vec![event],
        base.declared_outcome().clone(),
        base.provenance_id(),
        vec![event_id],
        DurabilityMode::Sync,
    )
    .unwrap();
    let identity = IdempotencyIdentity::new(
        database_id(0xb4),
        Environment::new("test").unwrap(),
        TenantScope::Global,
        commit.actor().principal_id().clone(),
        commit.plan().contract_lineage().clone(),
        commit.plan().command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(1).unwrap(), [0x74; 32]),
    );
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_u64(1).unwrap();
    let claims = StoredAdmittedProvenanceClaimsV1::default();
    let outcome = StoredOutcomeV1::new(
        identity.clone(),
        sequence,
        commit.admission_request_id(),
        commit.plan().clone(),
        commit.canonical_input_hash(),
        commit.actor().clone(),
        commit.logical_time(),
        partition.finish().unwrap(),
        commit.partition_hash(),
        Vec::new(),
        commit.declared_outcome().clone(),
        claims.clone(),
        commit.provenance_id(),
        DurabilityMode::Sync,
    )
    .unwrap();
    let provenance = StoredProvenanceRecordV1::new(
        commit.provenance_id(),
        sequence,
        identity,
        commit.admission_request_id(),
        commit.plan().clone(),
        commit.canonical_input_hash(),
        commit.actor().clone(),
        commit.logical_time(),
        commit.partition_hash(),
        Vec::new(),
        commit.declared_outcome().outcome_id(),
        Vec::new(),
        vec![event_id],
        claims,
    )
    .unwrap();
    let audit = |number, phase, link| {
        StoredServiceAuditRecordV1::from_stored_parts(
            AdministrationSequence::new(number).unwrap(),
            commit.admission_request_id(),
            Timestamp::new(1, 0).unwrap(),
            ServiceOperationV1::ExecuteCommand,
            phase,
            Some(audit_principal(0x75)),
            ServiceIngressKindV1::InProcessTestComparison,
            ServiceAuditTargetsV1::empty(),
            None,
            link,
        )
        .unwrap()
    };
    let started = audit(1, ServiceAuditPhaseV1::Started, ServiceAuditLinkV1::None);
    let terminal = audit(
        2,
        ServiceAuditPhaseV1::Succeeded,
        ServiceAuditLinkV1::Command {
            commit_sequence: sequence,
            provenance_id: commit.provenance_id(),
        },
    );
    StoredCommandCapsuleV2::from_base(
        StoredCommandCapsuleV1::new(outcome, provenance, commit, started, terminal).unwrap(),
        Vec::new(),
    )
    .unwrap()
}

#[test]
fn shared_outbox_event_lookup_preserves_legacy_segment_and_checkpoint_domains() {
    let capsule = shared_evidence_capsule();
    let sequence = capsule.commit_sequence();
    let manifest = crate::application::build_command_segment_manifest(
        std::slice::from_ref(&capsule),
        sequence,
    )
    .unwrap();
    let (_, segment) = riffdb_storage_api::seal_and_encode_command_segment_v1(
        StoredCommandSegmentV1::new(
            database_id(0xb4),
            1,
            None,
            vec![capsule.clone()],
            manifest,
            riffdb_storage_api::CommandSegmentDigestV1::from_bytes([0; 32]),
        )
        .unwrap(),
    )
    .unwrap();
    let rows = [
        (
            codec::encode_commit_record_v1(capsule.base().commit())
                .unwrap()
                .as_bytes()
                .to_vec(),
            false,
        ),
        (
            riffdb_storage_api::encode_command_capsule_v1(capsule.base())
                .unwrap()
                .as_bytes()
                .to_vec(),
            false,
        ),
        (
            riffdb_storage_api::encode_command_capsule_v2(&capsule)
                .unwrap()
                .as_bytes()
                .to_vec(),
            true,
        ),
        (segment.as_bytes().to_vec(), true),
    ];
    for (ordinal, (row, embedded)) in rows.into_iter().enumerate() {
        let path = TestDatabasePath::new(&format!("shared-outbox-{ordinal}"));
        let store = initialized_store(&path, database_id(0xb4));
        let write = store.shared.database.begin_write().unwrap();
        write
            .open_table(COMMITS)
            .unwrap()
            .insert(
                keys::encode_application_sequence_key(sequence).as_slice(),
                row.as_slice(),
            )
            .unwrap();
        for event in capsule.events() {
            write
                .open_table(EVENTS)
                .unwrap()
                .insert(
                    keys::encode_event_key(event.event_id()).as_slice(),
                    codec::encode_durable_event_v1(event).unwrap().as_bytes(),
                )
                .unwrap();
        }
        write.commit().unwrap();
        let pin = store.shared.database.begin_read().unwrap();
        for floor in [0, sequence.get()] {
            let commands = command_history::CommandHistory::build(
                &pin,
                floor,
                MAX_STARTUP_EVIDENCE_INDEX_BYTES,
            )
            .unwrap();
            for event_id in [
                EventId::new(sequence, 0),
                EventId::new(sequence, 1),
                EventId::new(CommitSequence::new(2).unwrap(), 0),
            ] {
                let expected = embedded && event_id == EventId::new(sequence, 0);
                assert_eq!(
                    command_segment_event_exists(&pin, event_id).unwrap(),
                    expected
                );
                assert_eq!(
                    command_segment_event_exists_cached(&pin, &commands, event_id).unwrap(),
                    expected,
                    "same event membership must survive cache hits and below-checkpoint misses"
                );
            }
        }
    }
}

#[test]
fn page_catalog_reuse_keeps_every_command_plan_validation_and_evicts_exact_bindings() {
    let source = include_str!("../../../examples/ticketdesk/lifecycle/riffdb/contract.riff");
    let first =
        ValidatedContractBundle::from_compiler_bundle(compile_contract_source(source).unwrap())
            .unwrap();
    let second = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(&source.replacen("version 1", "version 2", 1)).unwrap(),
    )
    .unwrap();
    let plan = |bundle: &ValidatedContractBundle, bad: bool| {
        let command = bundle.bundle().commands().first().unwrap();
        ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            command.command_id(),
            if bad {
                PlanHash::from_bytes([0xff; 32])
            } else {
                command.plan_hash()
            },
        )
    };
    let command = |reference| {
        shared_evidence_capsule_for_plan(reference)
            .with_prefix_evidence(
                riffdb_storage_api::CommandPrefixEvidenceV1::new(
                    riffdb_types::DualFrontier::new(None, None),
                    riffdb_types::DualFrontier::new(
                        Some(CommitSequence::first()),
                        Some(AdministrationSequence::new(2).unwrap()),
                    ),
                    Vec::new(),
                )
                .unwrap(),
            )
            .unwrap()
    };
    let path = TestDatabasePath::new("catalog-page-cache");
    let store = initialized_store(&path, database_id(0xb5));
    let write = store.shared.database.begin_write().unwrap();
    for bundle in [&first, &second] {
        let stored = bundle.to_stored().unwrap();
        write
            .open_table(CONTRACT_BUNDLES)
            .unwrap()
            .insert(
                keys::encode_contract_bundle_key(stored.lineage(), stored.contract_version())
                    .unwrap()
                    .as_slice(),
                codec::encode_contract_bundle_v1(&stored)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
    }
    write.commit().unwrap();
    let pin = store.shared.database.begin_read().unwrap();
    let mut cache = crate::command_prefix::CatalogImageCache::default();
    let first_command = command(plan(&first, false));
    for _ in 0..64 {
        crate::command_prefix::validate_catalog_images_cached(&pin, [&first_command], &mut cache)
            .unwrap();
    }
    assert_eq!(
        cache.loads(),
        1,
        "one direct bundle decode serves all physical groups on the same structural page"
    );
    let invalid = command(plan(&first, true));
    assert!(crate::command_prefix::validate_catalog_images(&pin, [&invalid]).is_err());
    assert!(
        crate::command_prefix::validate_catalog_images_cached(&pin, [&invalid], &mut cache)
            .is_err(),
        "a cached schema must never cache success for a different command plan"
    );
    assert_eq!(cache.loads(), 1);
    let second_command = command(plan(&second, false));
    crate::command_prefix::validate_catalog_images_cached(&pin, [&second_command], &mut cache)
        .unwrap();
    assert_eq!(
        cache.loads(),
        2,
        "a changed version/hash requires full schema validation"
    );
    crate::command_prefix::validate_catalog_images_cached(&pin, [&first_command], &mut cache)
        .unwrap();
    assert_eq!(cache.loads(), 3, "the cache retains only one direct bundle");
    let mut next_page = crate::command_prefix::CatalogImageCache::default();
    crate::command_prefix::validate_catalog_images_cached(&pin, [&first_command], &mut next_page)
        .unwrap();
    assert_eq!(
        next_page.loads(),
        1,
        "even the same pin starts each new page with no retained schema"
    );
}

// req: REC-001, REC-006, REC-007, STO-031
#[test]
fn offline_parallel_audit_pages_match_complete_serial_findings_and_exact_end() {
    for corruption in 0..5 {
        let path = TestDatabasePath::new("offline-parallel-audit-oracle");
        let store = initialized_store(&path, database_id(0xb7));
        let write = store.shared.database.begin_write().unwrap();
        {
            let mut audit = write.open_table(AUDIT).unwrap();
            let mut indexes = write.open_table(AUDIT_BY_REQUEST).unwrap();
            for ordinal in 1..=96_u8 {
                let sequence = AdministrationSequence::new(u64::from(ordinal)).unwrap();
                let request = request_id(if corruption == 4 && ordinal == 16 {
                    15
                } else {
                    ordinal
                });
                let record = StoredServiceAuditRecordV1::from_stored_parts(
                    sequence,
                    request,
                    Timestamp::new(1, 0).unwrap(),
                    ServiceOperationV1::GetHealth,
                    ServiceAuditPhaseV1::Denied,
                    Some(audit_principal(0x72)),
                    ServiceIngressKindV1::Grpc,
                    ServiceAuditTargetsV1::empty(),
                    None,
                    ServiceAuditLinkV1::None,
                )
                .unwrap();
                let encoded = codec::encode_administration_audit_record_v1(
                    &StoredAdministrationAuditRecordV1::Service(record),
                )
                .unwrap();
                let key = keys::encode_audit_key(sequence);
                if corruption == 1 && ordinal == 19 {
                    audit.insert(key.as_slice(), &[0xff_u8][..]).unwrap();
                } else {
                    audit.insert(key.as_slice(), encoded.as_bytes()).unwrap();
                }
                if corruption == 2 && ordinal == 23 {
                    continue;
                }
                let indexed_request = if corruption == 3 && ordinal == 27 {
                    request_id(28)
                } else {
                    request
                };
                let index = codec::encode_service_audit_request_index_v1(
                    riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(
                        indexed_request,
                        sequence,
                    ),
                )
                .unwrap();
                indexes
                    .insert(
                        keys::encode_audit_by_request_key(request, sequence).as_slice(),
                        index.as_bytes(),
                    )
                    .unwrap();
            }
            let allocator = codec::encode_administration_sequence_allocator_v1(
                AdministrationSequenceAllocator::next(AdministrationSequence::new(97).unwrap()),
            )
            .unwrap();
            write
                .open_table(META)
                .unwrap()
                .insert(META_ADMINISTRATION_SEQUENCE, allocator.as_bytes())
                .unwrap();
        }
        write.commit().unwrap();
        let shared = Arc::clone(&store.shared);
        drop(store);
        let inspect = |purpose| {
            // Retain the fixture's original engine owner. Reopening a
            // deliberately corrupt fixture can refuse before either walker.
            let store = RedbStore {
                shared: Arc::clone(&shared),
            };
            let mut session = store
                .begin_structural_evidence_for(inputs(), purpose)
                .unwrap();
            let mut cursor =
                StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
            let mut findings = Vec::new();
            loop {
                match session
                    .read_structural_evidence(cursor, EvidencePageLimit::new(64).unwrap())
                    .unwrap()
                {
                    StructuralEvidencePage::Page {
                        start,
                        next,
                        findings: page,
                    } => {
                        assert_eq!(start, cursor);
                        assert!(next.position() > cursor.position());
                        findings.extend(page);
                        cursor = next;
                    }
                    StructuralEvidencePage::ExactEnd(end) => {
                        assert_eq!(end.cursor(), cursor);
                        return (cursor.position(), findings);
                    }
                }
            }
        };
        let serial = inspect(EvidenceOpenPurpose::BackgroundScrub);
        let parallel = inspect(EvidenceOpenPurpose::OfflineIntegrityScrub);
        assert_eq!(parallel, serial, "corruption case {corruption}");
        assert_eq!(parallel.1.is_empty(), corruption == 0);
    }
}
