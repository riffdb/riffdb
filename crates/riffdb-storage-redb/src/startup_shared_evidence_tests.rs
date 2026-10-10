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
    let sequence = CommitSequence::first();
    let bundle = stored_bundle("shared-command", 1, b"command catalog");
    let plan = checkpoint_count_plan(&bundle);
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
