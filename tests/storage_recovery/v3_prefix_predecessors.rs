//! Real bootstrap and follower receipt checks, including an intra-group predecessor.
// req: REP-007, REP-003, REC-001
use super::*;
use riffdb_storage_api::*;
use riffdb_storage_redb::{
    RedbBootstrapMaterializer, RedbBootstrapStage, RedbFollowerApplier, RedbFollowerStore,
};

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

fn open_follower(path: &Path) -> RedbFollowerApplier {
    let mut session = RedbFollowerStore::open(path)
        .unwrap()
        .begin_structural_evidence(inputs())
        .unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let end = loop {
        match session
            .read_structural_evidence(cursor, EvidencePageLimit::new(64).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "{findings:?}");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (catalog, historical) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = session.finish(end, historical).unwrap() else {
        panic!("current source bootstrap needs no migration");
    };
    opened
        .into_parts()
        .3
        .into_follower_after_catalog_validation()
        .unwrap()
}

fn bootstrap(ports: &RedbOperationalPorts, path: &Path) -> ChangelogHistoryStateV3 {
    let source_path = path.with_extension("source-transfer");
    let mut build = ports
        .begin_replication_bootstrap_v3(
            &source_path,
            ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap(),
        )
        .unwrap();
    while !build.advance().unwrap() {}
    let held = build.finish().unwrap();
    let manifest = held.manifest();
    let mut stage =
        RedbBootstrapStage::create(&path.with_extension("receiver-transfer"), manifest).unwrap();
    for ordinal in 1..=manifest.page_count() {
        stage
            .append(&held.read_page(ordinal).unwrap().encode().unwrap())
            .unwrap();
    }
    let mut materializer = RedbBootstrapMaterializer::create(
        &path.with_extension("candidate"),
        stage.into_materialization_input().unwrap(),
    )
    .unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    materializer
        .finish()
        .unwrap()
        .validate(inputs())
        .unwrap()
        .publish(path)
        .unwrap()
        .release_for_startup()
        .unwrap();
    manifest.fence().history()
}

// Semantic-prior substitutions retain exact receipt/prefix net agreement; a
// real predecessor join is needed to refuse them. Separate cases substitute a
// wrong nested envelope or break a retained raw precondition.
fn contradictory_prefix(
    receipt: &AuthoritativeTransactionV3,
    case: u8,
) -> AuthoritativeTransactionV3 {
    let graph = receipt
        .mutations()
        .iter()
        .find(|m| m.namespace() == N::Commits)
        .unwrap();
    let decoded = decode_command_segment_v1(graph.value().unwrap()).unwrap();
    let segment = decoded.value();
    let last = segment.commands().last().unwrap();
    let mut transitions = last.entity_transitions().to_vec();
    let mut epochs = last.index_generation_transitions().to_vec();
    let prefix = last.prefix_evidence().unwrap();
    let mut mutations = prefix.mutations().to_vec();
    let (namespace, post_image) = if case >= 6 {
        let row = prefix
            .mutations()
            .iter()
            .find(|m| m.namespace() == N::SecondaryIndexes && m.value().is_some())
            .unwrap();
        if case == 6 {
            (
                N::SecondaryIndexes,
                prefix
                    .mutations()
                    .iter()
                    .find(|m| m.namespace() == N::EntityChainHeads)
                    .unwrap()
                    .value()
                    .unwrap()
                    .to_vec(),
            )
        } else {
            let decoded = decode_index_entry_v2(row.value().unwrap()).unwrap();
            let entry = decoded.value();
            let mut partition =
                riffdb_types::PartitionKeyBuilder::new(entry.partition_key().aggregate_type_id());
            partition.push_u64(999).unwrap();
            let entry = StoredIndexEntryV2::new(
                entry.key().clone(),
                entry.schema_binding().clone(),
                entry.covered_values().clone(),
                partition.finish().unwrap(),
            )
            .unwrap();
            (
                N::SecondaryIndexes,
                encode_index_entry_v2(&entry).unwrap().into_bytes(),
            )
        }
    } else if case == 4 {
        let head = prefix
            .mutations()
            .iter()
            .find(|m| m.namespace() == N::EntityChainHeads)
            .unwrap();
        (N::Entities, head.value().unwrap().to_vec())
    } else if case == 5 {
        (
            N::IndexEpochs,
            encode_index_epoch_v1(epochs[0].post_image())
                .unwrap()
                .into_bytes(),
        )
    } else if case == 3 {
        let old = &epochs[0];
        let advance = IndexEpochAdvanceV1::new(
            old.target().clone(),
            old.post_image().schema_binding().clone(),
            IndexEpochPosition::Value(old.next()),
        )
        .unwrap();
        let bytes = encode_index_epoch_v1(advance.post_image())
            .unwrap()
            .into_bytes();
        epochs[0] = advance;
        (N::IndexEpochs, bytes)
    } else {
        let old = &transitions[0];
        let prior = if case == 1 {
            let EntityChainStateV1::Live { version, .. } = old.prior_state() else {
                panic!("live prior");
            };
            EntityChainStateV1::Live {
                version,
                value_hash: riffdb_types::EntityRecordHash::from_bytes([0x77; 32]),
            }
        } else {
            old.prior_state()
        };
        let replacement = CommittedEntityTransitionV1::new(
            old.command_sequence(),
            old.mutation_ordinal(),
            old.target().clone(),
            prior,
            old.prior_chain_revision() + u64::from(case == 2),
            old.prior_transition_hash(),
            old.next_state(),
        )
        .unwrap();
        let head = EntityChainHeadV1::from_stored_parts(
            replacement.target().clone(),
            replacement.prior_chain_revision() + 1,
            replacement.next_state(),
            replacement.command_sequence(),
            replacement.transition_hash(),
        )
        .unwrap();
        transitions[0] = replacement;
        (
            N::EntityChainHeads,
            encode_entity_chain_head_v1(&head).unwrap().into_bytes(),
        )
    };
    let row = mutations
        .iter_mut()
        .find(|m| m.namespace() == namespace && (case < 6 || m.value().is_some()))
        .unwrap();
    let changed_key = row.key().to_vec();
    let expected = if case == 5 {
        Some([0x66; 32])
    } else {
        row.expected_hash()
    };
    *row = AuthoritativeMutationV3::put(namespace, row.key(), expected, &post_image).unwrap();
    let replacement = StoredCommandCapsuleV2::from_base_with_entity_transitions(
        last.base().clone(),
        epochs,
        transitions,
    )
    .unwrap()
    .with_prefix_evidence(
        CommandPrefixEvidenceV1::new(prefix.predecessor(), prefix.covered(), mutations).unwrap(),
    )
    .unwrap();
    let mut commands = segment.commands().to_vec();
    *commands.last_mut().unwrap() = replacement;
    let draft = StoredCommandSegmentV1::new(
        segment.database_id(),
        segment.history_incarnation(),
        segment.predecessor_segment_digest(),
        commands,
        segment.manifest().clone(),
        CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .unwrap();
    let (segment, encoded) = seal_and_encode_command_segment_v1(draft).unwrap();
    let mut net = receipt.mutations().to_vec();
    for row in &mut net {
        let value = if row.namespace() == namespace && row.key() == changed_key {
            Some(post_image.as_slice())
        } else if row.namespace() == N::Commits {
            Some(encoded.as_bytes())
        } else {
            None
        };
        if let Some(value) = value {
            *row = AuthoritativeMutationV3::put(
                row.namespace(),
                row.key(),
                row.expected_hash(),
                value,
            )
            .unwrap();
        }
    }
    let original =
        AuthoritativeTransactionV3::new(receipt.binding(), receipt.attribution(), net).unwrap();
    let prefixes = segment
        .commands()
        .iter()
        .map(|c| c.prefix_evidence().unwrap())
        .collect::<Vec<_>>();
    let net = validate_command_prefix_mutations_v1(&prefixes, &original);
    if case == 5 {
        assert!(net.is_err());
    } else {
        net.unwrap();
    }
    original
}

#[test]
fn follower_joins_prefix_facts_to_physical_and_intra_group_predecessors() {
    for count in [1, 2, 4] {
        for case in [0, 1, 2, 3, 4, 6, 7] {
            // The other terminal steps delete the row or leave its bytes
            // unchanged, so there is no index put to substitute in those cases.
            if count != 1 && case >= 6 {
                continue;
            }
            let source = TestDatabasePath::new("prefix-prior-source");
            install_fixture(&source.0);
            let (ports, _receiver) = observed_ports(&source.0, RedbCommitProfile::Hardened);
            let first = command_fixture_at(1);
            commit_command_fixture(&ports, &first);
            let path = source.0.with_extension("follower.redb");
            let baseline = bootstrap(&ports, &path);
            let mut applier = open_follower(&path);
            let second = superseding_command_fixture_at(2, 1, &first);
            let third = deleting_command_fixture_at(3, &second);
            let fourth =
                build_command_fixture(4, 1, Some(&third), AdmissionShape::VacantTerminal, false);
            let fifth = superseding_command_fixture_at(5, 1, &fourth);
            let commands = [second, third, fourth, fifth];
            commit_command_group(&ports, &commands[..count]);
            let pin = ports.published_changelog_snapshot_v3().unwrap();
            let mut cursor = pin
                .changelog_receipts_v3(baseline.lineage(), baseline.tail())
                .unwrap();
            let mut receipts = Vec::new();
            while let Some(receipt) = cursor.next_receipt().unwrap() {
                assert!(receipts.len() < 16);
                receipts.push(receipt);
            }
            let original = receipts.last_mut().unwrap();
            if case != 0 {
                *original = contradictory_prefix(original, case);
            }
            let expected = ChangelogHistoryPointV3::from_receipt(original).unwrap();
            let frame = ChangelogFrameV3::new(
                ChangelogFrameBindingV3::new(
                    baseline.lineage().database_id(),
                    baseline.lineage().history_incarnation(),
                    baseline.lineage().leadership_epoch().get(),
                    baseline.lineage().catalog_digest(),
                    baseline.tail().history_hash(),
                )
                .unwrap(),
                receipts,
            )
            .unwrap()
            .encode()
            .unwrap();
            if case == 0 {
                assert_eq!(applier.apply_frame(&frame).unwrap(), expected);
                assert_eq!(applier.apply_frame(&frame).unwrap(), expected);
                drop(applier);
                assert_eq!(
                    open_follower(&path).durable_history().unwrap().tail(),
                    expected
                );
            } else {
                assert!(
                    applier.apply_frame(&frame).is_err(),
                    "count {count}, case {case}"
                );
                assert!(
                    applier.durable_history().is_err(),
                    "refusal fences the handle"
                );
                drop(applier);
                assert_eq!(open_follower(&path).durable_history().unwrap(), baseline);
            }
        }
    }
}

#[test]
fn startup_refuses_resealed_epoch_priors_and_secondary_index_images() {
    for case in [3, 5, 6, 7] {
        let source = TestDatabasePath::new("prefix-prior-startup");
        let anchor = install_fixture(&source.0);
        let (ports, _receiver) = observed_ports(&source.0, RedbCommitProfile::Hardened);
        let first = command_fixture_at(1);
        let second = superseding_command_fixture_at(2, 1, &first);
        commit_command_group(&ports, &[first, second]);
        let pin = ports.published_changelog_snapshot_v3().unwrap();
        let mut cursor = pin
            .changelog_receipts_v3(anchor.lineage(), anchor.tail())
            .unwrap();
        let receipt = loop {
            let receipt = cursor.next_receipt().unwrap().unwrap();
            if receipt
                .mutations()
                .iter()
                .any(|m| m.namespace() == N::Commits)
            {
                break receipt;
            }
        };
        let forged = contradictory_prefix(&receipt, case);
        drop(cursor);
        drop(pin);
        drop(_receiver);
        drop(ports);
        let database = Database::open(&source.0).unwrap();
        let write = database.begin_write().unwrap();
        for row in forged
            .mutations()
            .iter()
            .filter(|m| matches!(m.namespace(), N::Commits | N::IndexEpochs))
        {
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(row.namespace().table());
            write
                .open_table(definition)
                .unwrap()
                .insert(row.key(), row.value().unwrap())
                .unwrap();
        }
        write.commit().unwrap();
        drop(database);
        match RedbStore::open(&source.0) {
            Ok(store) => assert!(!collect_structural_findings(store).is_empty()),
            Err(error) => assert_eq!(error.kind(), StorageErrorKind::CorruptData),
        }
    }
}
