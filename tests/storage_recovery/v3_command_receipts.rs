//! Production command owners over an isolated V3 activation fixture. This does
//! not claim production activation or the complete WP-772 crash obligation.
// req: REP-003, REC-001, STO-012, PERF-007
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogAttributionV3 as A, ChangelogHistoryPointV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, ChangelogTransactionAllocator, LeadershipEpochV1,
    PublishedFrontierAdvancement, ReplicationFollowerStateV3, proto_codec::*,
};
use riffdb_types::DualFrontier;
use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};

const HISTORY: TableDefinition<&[u8], &[u8]> = TableDefinition::new(N::ChangelogHistory.table());

// Fixture construction only: the registry already names current V3 but the
// normal activation handoff is still pending. No production activation permit
// is exposed or bypassed by application code.
fn install_fixture(path: &Path) -> ChangelogHistoryStateV3 {
    prepare_command_database(path);
    let database = Database::open(path).unwrap();
    let mut write = database.begin_write().unwrap();
    write.set_two_phase_commit(true);
    let mut meta = write.open_table(META).unwrap();
    let administration = *decode_administration_sequence_allocator_v1(
        meta.get(META_ADMINISTRATION_SEQUENCE)
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap()
    .value();
    let administration = match administration {
        AdministrationSequenceAllocator::Next(next) => AdministrationSequence::new(next.get() - 1),
        AdministrationSequenceAllocator::Exhausted => panic!("empty command fixture allocator"),
    };
    let frontier = DualFrontier::new(None, administration);
    let lineage = ChangelogLineageV3::new(database_id(), 1, LeadershipEpochV1::initial()).unwrap();
    let (sequence, allocator) = ChangelogTransactionAllocator::initial()
        .allocate_one()
        .unwrap();
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: database_id(),
            history_incarnation: 1,
            predecessor: None,
            sequence,
            predecessor_frontier: frontier,
            covered_frontier: frontier,
            prior_history_hash: [0; 32],
        },
        A::V3Activation,
        vec![],
    )
    .unwrap();
    let point = ChangelogHistoryPointV3::from_receipt(&receipt).unwrap();
    let history = ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap();
    for (namespace, value) in [
        (
            N::AuthoritativeStateCatalog,
            encode_authoritative_state_catalog_v1(AuthoritativeStateCatalogV1),
        ),
        (
            N::LeadershipEpoch,
            encode_leadership_epoch_v1(lineage.leadership_epoch()),
        ),
        (
            N::ChangelogHistoryState,
            encode_changelog_history_state_v3(history),
        ),
        (
            N::NextChangelogTransaction,
            encode_changelog_transaction_allocator_v3(allocator),
        ),
        (
            N::ReplicationFollowerState,
            encode_replication_follower_state_v3(ReplicationFollowerStateV3::detached()),
        ),
    ] {
        assert!(
            meta.insert(namespace.metadata_key().unwrap(), value.unwrap().as_bytes())
                .unwrap()
                .is_none()
        );
    }
    drop(meta);
    write
        .open_table(TableDefinition::<&[u8], &[u8]>::new(
            N::ReplicationSourceHolds.table(),
        ))
        .unwrap();
    write
        .open_table(HISTORY)
        .unwrap()
        .insert(
            sequence.get().to_be_bytes().as_slice(),
            receipt.encode().unwrap().as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
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
fn real_v3_command_group_and_journal_overwrite_delete_keep_original_receipts_after_checkpoint() {
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
