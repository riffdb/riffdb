//! Production command owners after the real validated V3 activation handoff.
//! Exact successor receipts, command semantics, checkpoint and recovery evidence.
// req: REP-003, REC-001, STO-012, PERF-007
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3 as A,
    ChangelogHistoryStateV3, PublishedFrontierAdvancement, proto_codec::*,
};
use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};

const HISTORY: TableDefinition<&[u8], &[u8]> = TableDefinition::new(N::ChangelogHistory.table());

// Use the ordinary structural/catalog activation and catalog publication. The
// baseline is its actual retained tail, never a second manually injected root.
fn install_fixture(path: &Path) -> ChangelogHistoryStateV3 {
    prepare_command_database(path);
    let database = Database::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let history = *decode_changelog_history_state_v3(
        meta.get(N::ChangelogHistoryState.metadata_key().unwrap())
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap()
    .value();
    let receipt = AuthoritativeTransactionV3::decode(
        read.open_table(HISTORY)
            .unwrap()
            .get(history.tail().sequence().get().to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    history.validate_terminal_receipt(&receipt).unwrap();
    history
}

#[derive(Debug)]
struct Observer(mpsc::SyncSender<PublishedFrontierAdvancement>);
impl riffdb_storage_api::ChangelogPublicationPort for Observer {
    fn observe_published_advancement(&self, advancement: PublishedFrontierAdvancement) {
        self.0
            .try_send(advancement)
            .expect("bounded test publication channel");
    }
}

fn observed_ports(
    path: &Path,
    profile: RedbCommitProfile,
) -> (
    RedbOperationalPorts,
    mpsc::Receiver<PublishedFrontierAdvancement>,
) {
    let (sender, receiver) = mpsc::sync_channel(32);
    let ports = open_operational(
        RedbStore::open_with_changelog_publication_port(path, profile, Arc::new(Observer(sender)))
            .unwrap(),
    );
    (ports, receiver)
}

fn receipts(
    pin: &PublishedFrontierAdvancement,
    anchor: ChangelogHistoryStateV3,
) -> Vec<AuthoritativeTransactionV3> {
    let mut cursor = pin
        .snapshot()
        .changelog_receipts_v3(anchor.lineage(), anchor.tail())
        .unwrap();
    let mut rows = Vec::new();
    while let Some(receipt) = cursor.next_receipt().unwrap() {
        assert!(rows.len() < 32, "bounded fixture history");
        rows.push(receipt);
    }
    assert!(cursor.next_receipt().unwrap().is_none());
    rows
}

fn latest(receiver: &mpsc::Receiver<PublishedFrontierAdvancement>) -> PublishedFrontierAdvancement {
    receiver
        .try_iter()
        .last()
        .expect("known-durable publication is synchronous with completion")
}

type AuthorityRows = BTreeMap<(N, Vec<u8>), Vec<u8>>;

fn authority_state(pin: &PublishedFrontierAdvancement) -> (ChangelogHistoryStateV3, AuthorityRows) {
    use riffdb_storage_api::{AuthoritativeStateStepV3, ReplicationAuthorityClassV1};
    let mut cursor = pin.snapshot().authoritative_state_v3().unwrap();
    let history = cursor.history();
    let mut namespaces = N::ALL.into_iter().filter(|namespace| {
        namespace.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative
    });
    let mut namespace = namespaces.next();
    let mut rows = AuthorityRows::new();
    let mut previous = None;
    while let Some(step) = cursor.next_item().unwrap() {
        match step {
            AuthoritativeStateStepV3::Row(row) => {
                assert_eq!(Some(row.namespace()), namespace);
                let key = (row.namespace(), row.key().to_vec());
                assert!(previous.as_ref().is_none_or(|prior| prior < &key));
                assert!(rows.len() < 512, "bounded command fixture authority");
                assert!(rows.insert(key.clone(), row.value().to_vec()).is_none());
                previous = Some(key);
            }
            AuthoritativeStateStepV3::EndNamespace(ended) => {
                assert_eq!(Some(ended), namespace);
                namespace = namespaces.next();
            }
        }
    }
    assert!(
        namespace.is_none(),
        "every authority namespace has an exact end"
    );
    assert!(cursor.next_item().unwrap().is_none());
    (history, rows)
}

fn assert_exact_authority_tail(
    before: &PublishedFrontierAdvancement,
    after: &PublishedFrontierAdvancement,
) {
    let (mut history, mut replayed) = authority_state(before);
    let (covered, expected) = authority_state(after);
    for receipt in receipts(after, history) {
        for mutation in receipt.mutations() {
            let key = (mutation.namespace(), mutation.key().to_vec());
            assert!(mutation.matches_prior(replayed.get(&key).map(Vec::as_slice)));
            if let Some(value) = mutation.value() {
                replayed.insert(key, value.to_vec());
            } else {
                assert!(replayed.remove(&key).is_some());
            }
        }
        history = history.advance(&receipt).unwrap();
    }
    assert_eq!(history.tail(), covered.tail());
    assert_eq!(
        replayed, expected,
        "exact comparison includes every authoritative namespace"
    );
}

fn entity_change<'a>(
    receipt: &'a AuthoritativeTransactionV3,
    key: &[u8],
) -> &'a riffdb_storage_api::AuthoritativeMutationV3 {
    receipt
        .mutations()
        .iter()
        .find(|m| m.namespace() == N::Entities && m.key() == key)
        .unwrap()
}

fn assert_command_graph(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    assert_eq!(
        ports
            .read_commit(fixture.records.commit().commit_sequence())
            .unwrap(),
        Some(fixture.records.commit().clone())
    );
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .unwrap(),
        Some(fixture.records.provenance().clone())
    );
    assert_eq!(
        ports
            .read_stored_outcome(fixture.pending.identity())
            .unwrap(),
        Some(fixture.records.stored_outcome().clone())
    );
    let AdmissionLookupResultV1::Found(outcome) =
        ports.lookup_admission(fixture.candidates.clone()).unwrap()
    else {
        panic!("committed identity must be terminal")
    };
    assert_eq!(
        *outcome,
        StoredAdmissionStateV1::StoredOutcome(fixture.records.stored_outcome().clone())
    );
    for event in fixture.records.events() {
        assert_eq!(
            ports.read_durable_event(event.event_id()).unwrap(),
            Some(event.clone())
        );
    }
}

#[test]
fn successor_changelog_receipts_survive_checkpoint_and_recovery() {
    let path = TestDatabasePath::new("v3-real-command-receipts");
    let anchor = install_fixture(&path.0);
    let (mut ports, receiver) = observed_ports(&path.0, RedbCommitProfile::Standard);
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);
    commit_command_group(&ports, &[first.clone(), second.clone()]);
    // This legacy observer has journal wakeups only; WP-746 owns V3-only
    // wakeup integration for direct controls. Its next published pin must still
    // expose the already-materialized direct group without inventing a receipt.
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[standalone_audit_intent(0xa1, 1_700_000_161)])
        .unwrap()
    else {
        panic!("standard audit must use the journal")
    };
    fence.wait().unwrap();
    let group_pin = latest(&receiver);
    let group_rows = receipts(&group_pin, anchor);
    let group = group_rows
        .iter()
        .find(|row| row.attribution() == A::DirectApplicationOrServiceAuditGroup)
        .unwrap();
    assert_eq!(group.attribution(), A::DirectApplicationOrServiceAuditGroup);
    assert_eq!(
        group.binding().covered_frontier.application(),
        CommitSequence::new(2)
    );
    for namespace in [
        N::Entities,
        N::EntityChainHeads,
        N::SecondaryIndexes,
        N::IndexEpochs,
        N::Commits,
        N::IdempotencyLocators,
        N::ProvenanceLocators,
        N::AuditByRequestLocators,
        N::NextApplicationSequence,
        N::NextAdministrationSequence,
    ] {
        assert!(
            group.mutations().iter().any(|m| m.namespace() == namespace),
            "group receipt must carry {namespace:?}"
        );
    }
    let physical_audit_bound = group_rows
        .last()
        .unwrap()
        .binding()
        .covered_frontier
        .administration()
        .unwrap()
        .get();
    for fixture in [&first, &second] {
        assert_command_graph(&ports, fixture);
    }
    let entities = group
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::Entities)
        .collect::<Vec<_>>();
    assert_eq!(entities.len(), 2);
    assert!(
        entities
            .iter()
            .all(|m| m.matches_prior(None) && m.value().is_some())
    );
    let overwritten = superseding_command_fixture_at(3, 1, &first);
    let deleted = deleting_command_fixture_at(4, &overwritten);
    let epoch = ports.begin_deferred_command_epoch().unwrap();
    let epoch = apply_unpublished_command_fixture(epoch, &overwritten);
    assert!(
        receiver.try_recv().is_err(),
        "writer-private commands must not publish"
    );
    let committed = DeferredCommandEpoch::fence(epoch).unwrap();
    assert_eq!(committed.len(), 1);
    let overwrite_pin = latest(&receiver);
    // Separate durability/publication groups are essential here: two staged
    // subgroups in one epoch legitimately form one physical net receipt.
    let epoch = ports.begin_deferred_command_epoch().unwrap();
    let epoch = apply_unpublished_command_fixture(epoch, &deleted);
    assert!(
        receiver.try_recv().is_err(),
        "private delete must not publish"
    );
    assert_eq!(DeferredCommandEpoch::fence(epoch).unwrap().len(), 1);
    let journal_pin = latest(&receiver);
    // Full catalog state, including journal-overlaid allocator metadata, must
    // equal exact receipt replay. A latest-checkpoint-only scan loses these
    // overwritten values and tombstones even though its receipt cursor works.
    assert_exact_authority_tail(&group_pin, &overwrite_pin);
    assert_exact_authority_tail(&overwrite_pin, &journal_pin);
    assert_exact_authority_tail(&group_pin, &journal_pin);
    let rows = receipts(&journal_pin, anchor);
    assert_eq!(rows.len(), group_rows.len() + 2);
    let overwrite = &rows[rows.len() - 2];
    let delete = rows.last().unwrap();
    assert_eq!(overwrite.attribution(), A::JournaledApplicationGroup);
    assert_eq!(delete.attribution(), A::JournaledApplicationGroup);
    assert_eq!(
        overwrite.binding().covered_frontier.application(),
        CommitSequence::new(3)
    );
    assert_eq!(
        delete.binding().covered_frontier.application(),
        CommitSequence::new(4)
    );
    let overwritten_entity = overwrite
        .mutations()
        .iter()
        .find(|m| m.namespace() == N::Entities)
        .unwrap();
    let original = entity_change(group, overwritten_entity.key());
    assert!(overwritten_entity.matches_prior(original.value()));
    assert_ne!(overwritten_entity.value(), original.value());
    let deleted_entity = entity_change(delete, overwritten_entity.key());
    assert!(deleted_entity.matches_prior(overwritten_entity.value()));
    assert!(deleted_entity.value().is_none());
    assert_eq!(ports.read_entity(&first.target).unwrap(), None);
    assert_eq!(
        receipts(&group_pin, anchor),
        group_rows,
        "old pin keeps overwritten values"
    );
    assert_eq!(receipts(&overwrite_pin, anchor).last().unwrap(), overwrite);
    let exact: BTreeMap<_, _> = rows
        .iter()
        .map(|r| (r.binding().sequence.get(), r.encode().unwrap()))
        .collect();
    assert!(ports.write_validated_prefix_checkpoint().unwrap());
    assert_exact_authority_tail(&group_pin, &journal_pin);
    assert_eq!(
        receipts(&journal_pin, anchor),
        rows,
        "checkpoint cannot rederive old values"
    );
    for fixture in [&overwritten, &deleted] {
        assert_command_graph(&ports, fixture);
    }
    drop(group_pin);
    drop(overwrite_pin);
    drop(journal_pin);
    drop(receiver);
    drop(ports);
    for _ in 0..2 {
        let reopened = open_operational(RedbStore::open(&path.0).unwrap());
        assert_eq!(reopened.read_entity(&first.target).unwrap(), None);
        for fixture in [&first, &second, &overwritten, &deleted] {
            assert_command_graph(&reopened, fixture);
        }
        drop(reopened);
        let database = Database::open(&path.0).unwrap();
        let read = database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        let certificate = decode_validated_prefix_checkpoint_v2(
            meta.get(N::ValidatedPrefixCheckpoint.metadata_key().unwrap())
                .unwrap()
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!(
            certificate.value().base().audit_sequence_bound(),
            physical_audit_bound
        );
        let roots = decode_changelog_history_state_v3(
            meta.get(N::ChangelogHistoryState.metadata_key().unwrap())
                .unwrap()
                .unwrap()
                .value(),
        )
        .unwrap();
        assert!(
            roots
                .value()
                .tail()
                .frontier()
                .administration()
                .unwrap()
                .get()
                > physical_audit_bound,
            "command-owned audits advance the logical frontier beyond the physical AUDIT certificate bound"
        );
        let table = read.open_table(HISTORY).unwrap();
        for (sequence, expected) in &exact {
            assert_eq!(
                table
                    .get(sequence.to_be_bytes().as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                expected
            );
        }
    }
    // The obligation's semantic replay proof also executes the real command
    // allocator/journal durability crash matrix, not only orderly reopens.
    real_v3_command_crashes_keep_whole_groups_and_receipts_through_repeated_recovery();
}

#[test]
fn real_v3_journal_subgroups_share_one_physical_receipt_without_losing_command_identity() {
    let path = TestDatabasePath::new("v3-journal-physical-group");
    let anchor = install_fixture(&path.0);
    let (ports, receiver) = observed_ports(&path.0, RedbCommitProfile::Standard);
    let first = command_fixture_at(1);
    let second = superseding_command_fixture_at(2, 1, &first);
    let epoch = ports.begin_deferred_command_epoch().unwrap();
    let epoch = apply_unpublished_command_fixture(epoch, &first);
    let epoch = apply_unpublished_command_fixture(epoch, &second);
    assert!(receiver.try_recv().is_err());
    assert_eq!(ports.read_entity(&first.target).unwrap(), None);
    assert_eq!(DeferredCommandEpoch::fence(epoch).unwrap().len(), 2);
    let pin = latest(&receiver);
    let rows = receipts(&pin, anchor);
    let groups = rows
        .iter()
        .filter(|row| row.attribution() == A::JournaledApplicationGroup)
        .collect::<Vec<_>>();
    assert_eq!(
        groups.len(),
        1,
        "one physical epoch, not one receipt per logical command"
    );
    let receipt = groups[0];
    assert_eq!(receipt.binding().predecessor_frontier.application(), None);
    assert_eq!(
        receipt.binding().covered_frontier.application(),
        CommitSequence::new(2)
    );
    let entities = receipt
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::Entities)
        .collect::<Vec<_>>();
    assert_eq!(
        entities.len(),
        1,
        "same-key transitions are coalesced inside one receipt"
    );
    assert!(entities[0].matches_prior(None));
    let decoded = decode_entity_record_v1(entities[0].value().unwrap()).unwrap();
    assert_eq!(decoded.value(), second.records.entities()[0].post_image());
    for fixture in [&first, &second] {
        assert_command_graph(&ports, fixture);
    }
    assert!(ports.write_validated_prefix_checkpoint().unwrap());
    assert_eq!(receipts(&pin, anchor), rows);
    drop(pin);
    drop(receiver);
    drop(ports);
    let reopened = open_operational(RedbStore::open(&path.0).unwrap());
    for fixture in [&first, &second] {
        assert_command_graph(&reopened, fixture);
    }
}

#[test]
fn v3_real_command_crash_child() {
    let Ok(phase) = std::env::var("RIFFDB_V3_COMMAND_PHASE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("RIFFDB_V3_COMMAND_PATH").unwrap());
    let (operation, committed, direct) = match phase.as_str() {
        "direct-before" => (RedbTestOperation::CommandBatch, false, true),
        "direct-after" => (RedbTestOperation::CommandBatch, true, true),
        "journal-staged-before" => (RedbTestOperation::DeferredCommandBatch, false, false),
        "journal-staged-after" => (RedbTestOperation::DeferredCommandBatch, true, false),
        "journal-tail-after" => (RedbTestOperation::CommandEpochTail, true, false),
        _ => panic!("unknown closed crash phase"),
    };
    let controller = if committed {
        RedbTestController::abort_after_commit(operation)
    } else {
        RedbTestController::abort_before_commit(operation)
    };
    let ports = open_operational(
        RedbStore::open_with_test_controller_and_commit_profile(
            &path,
            if direct {
                RedbCommitProfile::Hardened
            } else {
                RedbCommitProfile::Standard
            },
            controller,
        )
        .unwrap(),
    );
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);
    if direct {
        commit_command_group(&ports, &[first, second]);
    } else {
        let epoch = ports.begin_deferred_command_epoch().unwrap();
        let epoch = apply_unpublished_command_fixture(epoch, &first);
        let epoch = apply_unpublished_command_fixture(epoch, &second);
        DeferredCommandEpoch::fence(epoch).unwrap();
    }
    panic!("armed process edge was not reached");
}

#[test]
fn real_v3_command_crashes_keep_whole_groups_and_receipts_through_repeated_recovery() {
    for phase in [
        "direct-before",
        "direct-after",
        "journal-staged-before",
        "journal-staged-after",
        "journal-tail-after",
    ] {
        let path = TestDatabasePath::new("v3-real-command-crash");
        install_fixture(&path.0);
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("v3_command_receipts::v3_real_command_crash_child")
            .env("RIFFDB_V3_COMMAND_PHASE", phase)
            .env("RIFFDB_V3_COMMAND_PATH", &path.0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_child_aborted(status, phase);
        let committed = matches!(phase, "direct-after" | "journal-tail-after");
        let first = command_fixture_at(1);
        let second = command_fixture_at(2);
        let mut exact = None;
        for _ in 0..2 {
            let ports = open_operational(RedbStore::open(&path.0).unwrap());
            for fixture in [&first, &second] {
                if committed {
                    assert_command_graph(&ports, fixture);
                } else {
                    assert_eq!(
                        ports.lookup_admission(fixture.candidates.clone()).unwrap(),
                        AdmissionLookupResultV1::NotFound
                    );
                    assert_eq!(ports.read_entity(&fixture.target).unwrap(), None);
                    assert_eq!(
                        ports
                            .read_commit(fixture.records.commit().commit_sequence())
                            .unwrap(),
                        None
                    );
                }
            }
            drop(ports);
            let database = Database::open(&path.0).unwrap();
            let read = database.begin_read().unwrap();
            let receipts = read
                .open_table(HISTORY)
                .unwrap()
                .iter()
                .unwrap()
                .map(|row| AuthoritativeTransactionV3::decode(row.unwrap().1.value()).unwrap())
                .filter(|row| {
                    matches!(
                        row.attribution(),
                        A::DirectApplicationOrServiceAuditGroup | A::JournaledApplicationGroup
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                receipts.len(),
                usize::from(committed),
                "{phase}: one physical receipt or no group"
            );
            if committed {
                assert_eq!(
                    receipts[0].binding().covered_frontier.application(),
                    CommitSequence::new(2)
                );
                assert_eq!(
                    receipts[0]
                        .mutations()
                        .iter()
                        .filter(|m| m.namespace() == N::Entities)
                        .count(),
                    2
                );
                let bytes = receipts[0].encode().unwrap();
                if let Some(previous) = &exact {
                    assert_eq!(&bytes, previous);
                } else {
                    exact = Some(bytes);
                }
            }
        }
    }
}

/// Diagnostic evidence for WP-749's intra-transaction stop requirement. This
/// proves the existing V3 contract; it does not claim exact-stop restore support.
#[test]
// req: REP-003
fn grouped_v3_receipt_retains_only_terminal_entity_image() {
    let path = TestDatabasePath::new("v3-grouped-intermediate-image");
    let anchor = install_fixture(&path.0);
    let (ports, _receiver) = observed_ports(&path.0, RedbCommitProfile::Hardened);
    let first = command_fixture_at(1);
    let second = superseding_command_fixture_at(2, 1, &first);
    let first_image = first.records.entities()[0].post_image();
    let second_image = second.records.entities()[0].post_image();
    assert_eq!(first_image.target(), second_image.target());
    assert_ne!(first_image, second_image);
    commit_command_group(&ports, &[first.clone(), second.clone()]);
    assert_eq!(
        ports.read_entity(&first.target).unwrap(),
        Some(second_image.clone())
    );
    assert_eq!(
        ports.read_commit(CommitSequence::new(1).unwrap()).unwrap(),
        Some(first.records.commit().clone())
    );
    assert_eq!(
        ports.read_commit(CommitSequence::new(2).unwrap()).unwrap(),
        Some(second.records.commit().clone())
    );
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let mut cursor = pin
        .changelog_receipts_v3(anchor.lineage(), anchor.tail())
        .unwrap();
    let mut group = None;
    while let Some(receipt) = cursor.next_receipt().unwrap() {
        if receipt.attribution() == A::DirectApplicationOrServiceAuditGroup {
            assert!(group.replace(receipt).is_none());
        }
    }
    let group = group.unwrap();
    assert_eq!(group.binding().predecessor_frontier.application(), None);
    assert_eq!(
        group.binding().covered_frontier.application(),
        CommitSequence::new(2)
    );
    let entities = group
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::Entities)
        .collect::<Vec<_>>();
    assert_eq!(entities.len(), 1);
    let retained = decode_entity_record_v1(entities[0].value().unwrap()).unwrap();
    assert_eq!(retained.value(), second_image);
    assert_ne!(retained.value(), first_image);
    // Command authority preserves a checked reference/hash, not this image.
    let reference = &first.records.commit().entity_references()[0];
    assert_eq!(
        reference.post_image_hash(),
        riffdb_storage_api::derive_entity_record_hash_v1(first_image).unwrap()
    );
    assert_ne!(
        reference.post_image_hash(),
        riffdb_storage_api::derive_entity_record_hash_v1(second_image).unwrap()
    );
}

#[test]
// req: REP-007, REP-003, REC-001
fn command_group_captures_each_put_delete_recreate_and_logical_index_epoch() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        let path = TestDatabasePath::new("v7-grouped-prefix-images");
        let anchor = install_fixture(&path.0);
        let (ports, _receiver) = observed_ports(&path.0, profile);
        let first = command_fixture_at(1);
        let second = superseding_command_fixture_at(2, 1, &first);
        let third = deleting_command_fixture_at(3, &second);
        let fourth =
            build_command_fixture(4, 1, Some(&third), AdmissionShape::VacantTerminal, false);
        let commands = [first, second, third, fourth];
        if profile == RedbCommitProfile::Standard {
            let epoch = ports.begin_deferred_command_epoch().unwrap();
            let empty = DeferredCommandEpoch::begin_empty_batch(epoch).unwrap();
            let batch = stage_command_group(empty, &commands);
            let epoch = batch
                .apply_unpublished_with_service_audit_transitions(
                    DurabilityMode::Sync,
                    commands.iter().map(command_audit_transition).collect(),
                )
                .unwrap();
            DeferredCommandEpoch::fence(epoch).unwrap();
        } else {
            commit_command_group(&ports, &commands);
        }
        let pin = ports.published_changelog_snapshot_v3().unwrap();
        let mut cursor = pin
            .changelog_receipts_v3(anchor.lineage(), anchor.tail())
            .unwrap();
        let mut checked = false;
        let mut stored_segment = None;
        while let Some(receipt) = cursor.next_receipt().unwrap() {
            if receipt.binding().covered_frontier.application() != CommitSequence::new(4) {
                continue;
            }
            let segment_mutation = receipt
                .mutations()
                .iter()
                .find(|m| m.namespace() == N::Commits)
                .unwrap();
            let segment = decode_command_segment_v1(segment_mutation.value().unwrap()).unwrap();
            stored_segment = Some(segment_mutation.value().unwrap().to_vec());
            assert_eq!(segment.value().commands().len(), 4);
            let evidence = segment
                .value()
                .commands()
                .iter()
                .map(|c| {
                    c.prefix_evidence()
                        .expect("newly committed command retains prefix evidence")
                })
                .collect::<Vec<_>>();
            riffdb_storage_api::validate_command_prefix_mutations_v1(&evidence, &receipt).unwrap();
            for (command, prefix) in commands.iter().zip(evidence) {
                let entity = prefix
                    .mutations()
                    .iter()
                    .find(|m| m.namespace() == N::Entities)
                    .unwrap();
                assert_eq!(entity.key(), command.target.key().as_bytes());
                assert_eq!(
                    entity
                        .value()
                        .map(|bytes| decode_entity_record_v1(bytes).unwrap().into_parts().0),
                    command.records.entities()[0].live_post_image().cloned()
                );
                let epoch = prefix
                    .mutations()
                    .iter()
                    .find(|m| m.namespace() == N::IndexEpochs)
                    .unwrap();
                assert_eq!(
                    decode_index_epoch_v1(epoch.value().unwrap())
                        .unwrap()
                        .value(),
                    command.write_plan.index_epochs()[0].post_image()
                );
                assert!(
                    prefix
                        .mutations()
                        .iter()
                        .any(|m| m.namespace() == N::EntityChainHeads)
                );
            }
            checked = true;
        }
        assert!(checked);
        drop(cursor);
        drop(pin);
        drop(_receiver);
        drop(ports);

        // Standard reopen consumes the journal recovery/checkpoint path;
        // hardened reopen reads the already durable Immediate transaction.
        let (reopened, _receiver) = observed_ports(&path.0, profile);
        let pin = reopened.published_changelog_snapshot_v3().unwrap();
        let mut cursor = pin
            .changelog_receipts_v3(anchor.lineage(), anchor.tail())
            .unwrap();
        let mut recovered = None;
        while let Some(receipt) = cursor.next_receipt().unwrap() {
            for mutation in receipt
                .mutations()
                .iter()
                .filter(|m| m.namespace() == N::Commits)
            {
                assert!(
                    recovered
                        .replace(mutation.value().unwrap().to_vec())
                        .is_none()
                );
            }
        }
        assert_eq!(recovered, stored_segment);
        assert_eq!(
            reopened.read_entity(&commands[3].target).unwrap(),
            commands[3].records.entities()[0].live_post_image().cloned()
        );
    }
}

#[test]
// req: REP-007, REC-001
fn startup_refuses_resealed_prefix_images_that_contradict_command_facts() {
    use riffdb_storage_api::{
        AuthoritativeMutationV3 as M, CommandPrefixEvidenceV1, CommandSegmentDigestV1,
        StoredCommandCapsuleV2, StoredCommandSegmentV1,
    };
    const COMMITS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("commits");
    for case in 0..8 {
        let path = TestDatabasePath::new("v7-prefix-row-corruption");
        install_fixture(&path.0);
        let (ports, receiver) = observed_ports(&path.0, RedbCommitProfile::Hardened);
        let first = command_fixture_at(1);
        let second = superseding_command_fixture_at(2, 1, &first);
        commit_command_group(&ports, &[first, second]);
        drop(receiver);
        drop(ports);
        let database = Database::open(&path.0).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut table = write.open_table(COMMITS).unwrap();
            let row = table.get(1_u64.to_be_bytes().as_slice()).unwrap().unwrap();
            let decoded = decode_command_segment_v1(row.value()).unwrap();
            let segment = decoded.value();
            let command = &segment.commands()[0];
            let prefix = command.prefix_evidence().unwrap();
            let mut mutations = prefix.mutations().to_vec();
            match case {
                0 => {} // Valid resealing remains admissible.
                1..=3 => {
                    let namespace = [N::Entities, N::EntityChainHeads, N::IndexEpochs][case - 1];
                    mutations.retain(|m| m.namespace() != namespace);
                }
                4..=6 => {
                    // All bytes and keys are individually valid. Only the join
                    // to command one's facts reveals command two's substitution.
                    let namespace = [N::Entities, N::EntityChainHeads, N::IndexEpochs][case - 4];
                    let later = segment.commands()[1]
                        .prefix_evidence()
                        .unwrap()
                        .mutations()
                        .iter()
                        .find(|m| m.namespace() == namespace)
                        .unwrap();
                    let row = mutations
                        .iter_mut()
                        .find(|m| m.namespace() == namespace)
                        .unwrap();
                    *row = M::put(
                        namespace,
                        row.key(),
                        row.expected_hash(),
                        later.value().unwrap(),
                    )
                    .unwrap();
                }
                7 => {
                    let row = mutations
                        .iter()
                        .find(|m| m.namespace() == N::Entities)
                        .unwrap();
                    mutations
                        .push(M::put(N::Entities, b"extra", None, row.value().unwrap()).unwrap());
                    mutations
                        .sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
                }
                _ => unreachable!(),
            }
            let replacement = StoredCommandCapsuleV2::from_base_with_entity_transitions(
                command.base().clone(),
                command.index_generation_transitions().to_vec(),
                command.entity_transitions().to_vec(),
            )
            .unwrap()
            .with_prefix_evidence(
                CommandPrefixEvidenceV1::new(prefix.predecessor(), prefix.covered(), mutations)
                    .unwrap(),
            )
            .unwrap();
            let mut commands = segment.commands().to_vec();
            commands[0] = replacement;
            let draft = StoredCommandSegmentV1::new(
                segment.database_id(),
                segment.history_incarnation(),
                segment.predecessor_segment_digest(),
                commands,
                segment.manifest().clone(),
                CommandSegmentDigestV1::from_bytes([0; 32]),
            )
            .unwrap();
            let bytes = seal_and_encode_command_segment_v1(draft).unwrap().1;
            // This is a semantic corruption proof, not a checksum failure.
            decode_command_segment_v1(bytes.as_bytes()).unwrap();
            drop(row);
            table
                .insert(1_u64.to_be_bytes().as_slice(), bytes.as_bytes())
                .unwrap();
        }
        write.commit().unwrap();
        drop(database);
        match RedbStore::open(&path.0) {
            Ok(store) if case == 0 => {
                drop(open_operational(store));
            }
            Ok(store) => assert!(
                !collect_structural_findings(store).is_empty(),
                "case {case}"
            ),
            Err(error) => {
                assert_ne!(case, 0);
                assert_eq!(
                    error.kind(),
                    riffdb_storage_api::StorageErrorKind::CorruptData,
                    "case {case}"
                );
            }
        }
    }
}
