//! Exact source fence handoff. These isolated retention fixtures do not replace
//! the complete receiver publication and app-baseline recovery exit gate.
// req: REP-003, REC-001, PERF-007, STO-012
use super::*;
use crate::changelog_v3_activation::SOURCE_HOLDS;
use riffdb_storage_api::{ChangelogCursorErrorV3 as Refusal, proto_codec::*};

fn follower(bootstrap: Hold) -> Hold {
    Hold::new(
        bootstrap.id(),
        Kind::FollowerAcknowledgement,
        bootstrap.lineage(),
        bootstrap.fence(),
    )
}

fn retained(ports: &RedbOperationalPorts, expected: Hold) -> Option<Hold> {
    ports
        .shared
        .database
        .begin_read()
        .unwrap()
        .open_table(SOURCE_HOLDS)
        .unwrap()
        .get(expected.storage_key().as_slice())
        .unwrap()
        .map(|row| {
            *decode_replication_source_hold_v1(row.value())
                .unwrap()
                .value()
        })
}

#[test]
// req: REP-004, REC-002
fn source_progress_observes_only_durable_follower_holds_in_one_immutable_pin() {
    let (_scope, ports, states) = fixture();
    let before = ports.published_changelog_snapshot_v3().unwrap();
    let empty = before.replication_source_progress_v3().unwrap();
    assert_eq!(empty.follower_count(), 0);
    assert_eq!(empty.oldest_acknowledged(), None);
    let mut control = ports.replication_source_control();
    control.register(hold(Kind::Bootstrap, states[1])).unwrap();
    control
        .register(hold(Kind::ArchiveAcknowledgement, states[2]))
        .unwrap();
    let first = hold(Kind::FollowerAcknowledgement, states[3]);
    control.register(first).unwrap();
    let second = Hold::new(
        riffdb_storage_api::ReplicationSourceHoldIdV1::new([0x95; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        first.lineage(),
        states[4].tail(),
    );
    control.register(second).unwrap();
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let observed = pin.replication_source_progress_v3().unwrap();
    assert_eq!(observed.history(), history(&ports));
    assert_eq!(observed.follower_count(), 2);
    assert_eq!(observed.oldest_acknowledged(), Some(first.fence()));
    assert_eq!(before.replication_source_progress_v3().unwrap(), empty);

    let advanced = Hold::new(first.id(), first.kind(), first.lineage(), states[5].tail());
    control.advance_acknowledgement(advanced).unwrap();
    let after = history(&ports);
    assert_eq!(pin.replication_source_progress_v3().unwrap(), observed);
    let latest = ports.published_changelog_snapshot_v3().unwrap();
    let progress = latest.replication_source_progress_v3().unwrap();
    assert_eq!(progress.history(), after);
    assert_eq!(progress.follower_count(), 2);
    assert_eq!(progress.oldest_acknowledged(), Some(second.fence()));
    assert_eq!(
        history(&ports),
        after,
        "observation must create no transaction"
    );
}

#[test]
fn bootstrap_attachment_replaces_its_hold_at_the_exact_same_fence_and_retries_read_only() {
    let (_scope, ports, states) = fixture();
    let bootstrap = hold(Kind::Bootstrap, states[3]);
    let follower = follower(bootstrap);
    let mut control = ports.replication_source_control();
    assert!(control.register(bootstrap).unwrap());
    let before = history(&ports);
    assert!(control.attach_bootstrap(bootstrap).unwrap());
    let after = history(&ports);
    assert_eq!(
        after.tail().sequence().get(),
        before.tail().sequence().get() + 1
    );
    assert_eq!(after.tail().frontier(), before.tail().frontier());
    let read = ports.shared.database.begin_read().unwrap();
    let receipts = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let encoded = receipts
        .get(after.tail().sequence().get().to_be_bytes().as_slice())
        .unwrap()
        .unwrap();
    let receipt = riffdb_storage_api::AuthoritativeTransactionV3::decode(encoded.value()).unwrap();
    assert_eq!(
        receipt.attribution(),
        ChangelogAttributionV3::ReplicationSourceHold
    );
    assert!(receipt.mutations().is_empty());
    assert_eq!(
        receipt.binding().predecessor_frontier,
        receipt.binding().covered_frontier
    );
    drop(encoded);
    drop(receipts);
    drop(read);
    assert_eq!(retained(&ports, bootstrap), None);
    assert_eq!(retained(&ports, follower), Some(follower));
    assert!(!control.attach_bootstrap(bootstrap).unwrap());
    assert_eq!(history(&ports), after);
    assert_eq!(
        control.register(bootstrap),
        Err(Refusal::InvalidPosition),
        "an old artifact cannot resurrect its completed bootstrap hold"
    );
    assert_eq!(history(&ports), after);
    assert!(!control.reclaim_history().unwrap());
    append(&ports, b"later checkpoint after attachment");
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), bootstrap.fence());
    // Only a later durable acknowledgement permits the source floor to move.
    let advanced = Hold::new(
        follower.id(),
        follower.kind(),
        follower.lineage(),
        states[5].tail(),
    );
    assert!(control.advance_acknowledgement(advanced).unwrap());
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), advanced.fence());
}

#[test]
fn bootstrap_attachment_refuses_missing_substituted_and_conflicting_fences_without_writes() {
    let (_scope, ports, states) = fixture();
    let bootstrap = hold(Kind::Bootstrap, states[3]);
    let mut control = ports.replication_source_control();
    let before = history(&ports);
    assert_eq!(
        control.attach_bootstrap(bootstrap),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(history(&ports), before);
    control.register(bootstrap).unwrap();
    let before = history(&ports);
    let epoch = ports.shared.durable_commit_epoch();
    for invalid in [
        follower(bootstrap),
        Hold::new(
            ReplicationSourceHoldIdV1::new([0x91; 16]).unwrap(),
            Kind::Bootstrap,
            bootstrap.lineage(),
            bootstrap.fence(),
        ),
        Hold::new(
            bootstrap.id(),
            Kind::Bootstrap,
            bootstrap.lineage(),
            states[4].tail(),
        ),
        Hold::new(
            bootstrap.id(),
            Kind::Bootstrap,
            bootstrap.lineage(),
            riffdb_storage_api::ChangelogHistoryPointV3::new(
                bootstrap.fence().sequence(),
                [0x95; 32],
                bootstrap.fence().frontier(),
            ),
        ),
    ] {
        assert_eq!(
            control.attach_bootstrap(invalid),
            Err(Refusal::InvalidPosition)
        );
        assert_eq!(history(&ports), before);
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        assert_eq!(retained(&ports, bootstrap), Some(bootstrap));
        assert_eq!(retained(&ports, follower(bootstrap)), None);
    }
    for (incarnation, epoch, expected) in [
        (2, LeadershipEpochV1::initial(), Refusal::ForeignLineage),
        (1, LeadershipEpochV1::new(2).unwrap(), Refusal::StaleEpoch),
    ] {
        let lineage =
            ChangelogLineageV3::new(bootstrap.lineage().database_id(), incarnation, epoch).unwrap();
        assert_eq!(
            control.attach_bootstrap(Hold::new(
                bootstrap.id(),
                Kind::Bootstrap,
                lineage,
                bootstrap.fence()
            )),
            Err(expected)
        );
        assert_eq!(history(&ports), before);
    }
    let conflicting = Hold::new(
        bootstrap.id(),
        Kind::FollowerAcknowledgement,
        bootstrap.lineage(),
        states[5].tail(),
    );
    control.register(conflicting).unwrap();
    let before = history(&ports);
    assert_eq!(
        control.attach_bootstrap(bootstrap),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(retained(&ports, bootstrap), Some(bootstrap));
    assert_eq!(retained(&ports, conflicting), Some(conflicting));
    assert_eq!(history(&ports), before);
}

#[test]
fn bootstrap_attachment_accepts_an_already_registered_identical_follower_without_a_gap() {
    let (_scope, ports, states) = fixture();
    let bootstrap = hold(Kind::Bootstrap, states[3]);
    let mut control = ports.replication_source_control();
    control.register(bootstrap).unwrap();
    control.register(follower(bootstrap)).unwrap();
    assert!(control.attach_bootstrap(bootstrap).unwrap());
    assert_eq!(retained(&ports, bootstrap), None);
    assert_eq!(
        retained(&ports, follower(bootstrap)),
        Some(follower(bootstrap))
    );
}

#[test]
fn bootstrap_attachment_at_the_hold_population_ceiling_does_not_need_an_extra_slot() {
    use redb::ReadableTableMetadata;
    let (_scope, ports, states) = fixture();
    let bootstrap = hold(Kind::Bootstrap, states[3]);
    ports
        .replication_source_control()
        .register(bootstrap)
        .unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    {
        let mut table = write.open_table(SOURCE_HOLDS).unwrap();
        for id in 1..riffdb_storage_api::MAX_REPLICATION_SOURCE_HOLDS_V1 {
            let fence = Hold::new(
                ReplicationSourceHoldIdV1::new(u128::from(id).to_be_bytes()).unwrap(),
                Kind::FollowerAcknowledgement,
                bootstrap.lineage(),
                bootstrap.fence(),
            );
            table
                .insert(
                    fence.storage_key().as_slice(),
                    encode_replication_source_hold_v1(fence).unwrap().as_bytes(),
                )
                .unwrap();
        }
    }
    ports.shared.commit_durable(write).unwrap();
    assert!(
        ports
            .replication_source_control()
            .attach_bootstrap(bootstrap)
            .unwrap()
    );
    assert_eq!(retained(&ports, bootstrap), None);
    assert_eq!(
        retained(&ports, follower(bootstrap)),
        Some(follower(bootstrap))
    );
    assert_eq!(
        ports
            .shared
            .database
            .begin_read()
            .unwrap()
            .open_table(SOURCE_HOLDS)
            .unwrap()
            .len()
            .unwrap(),
        riffdb_storage_api::MAX_REPLICATION_SOURCE_HOLDS_V1
    );
}

#[test]
fn bootstrap_attachment_storage_binding_requires_the_manifest_fence_and_never_resurrects_a_completed_hold()
 {
    let (scope, ports, states) = fixture();
    let id = ReplicationSourceHoldIdV1::new([0x63; 16]).unwrap();
    let path = scope.join("source-artifact");
    let source = ports.prepare_replication_bootstrap_v3(&path, id).unwrap();
    let manifest = source.manifest();
    let before = history(&ports);
    let epoch = ports.shared.durable_commit_epoch();
    assert_eq!(
        ports.attach_replication_bootstrap_v3(manifest, states[3].tail()),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(history(&ports), before);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    // This isolated source test supplies the receiver claim explicitly; real
    // publication/durability evidence remains the authenticated caller's job.
    ports
        .attach_replication_bootstrap_v3(manifest, manifest.fence().history().tail())
        .unwrap();
    drop(source);
    let after = history(&ports);
    ports
        .attach_replication_bootstrap_v3(manifest, manifest.fence().history().tail())
        .unwrap();
    assert_eq!(history(&ports), after);
    assert!(ports.resume_replication_bootstrap_v3(&path, id).is_err());
    assert_eq!(history(&ports), after);
    let bootstrap = Hold::new(
        id,
        Kind::Bootstrap,
        manifest.fence().history().lineage(),
        manifest.fence().history().tail(),
    );
    assert_eq!(retained(&ports, bootstrap), None);
    assert_eq!(
        retained(&ports, follower(bootstrap)),
        Some(follower(bootstrap))
    );
}

#[test]
fn bootstrap_attachment_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_BOOTSTRAP_ATTACHMENT_PATH") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let bytes = std::fs::read(path.parent().unwrap().join("expected-hold.bin")).unwrap();
    let bootstrap = *decode_replication_source_hold_v1(&bytes).unwrap().value();
    let store = RedbStore::open(path).unwrap();
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    assert!(
        !ports
            .replication_source_control()
            .attach_bootstrap(bootstrap)
            .unwrap(),
        "a fresh attachment should reach the requested crash; an exact retry is read-only"
    );
}

#[test]
fn bootstrap_attachment_process_crashes_preserve_one_complete_fence_and_gap_free_tail() {
    for edge in [
        "attachment-follower-staged",
        "attachment-bootstrap-removed",
        "attachment-receipted",
        "attachment-committed",
    ] {
        let (scope, ports, states) = fixture();
        let bootstrap = hold(Kind::Bootstrap, states[3]);
        ports
            .replication_source_control()
            .register(bootstrap)
            .unwrap();
        std::fs::write(
            scope.join("expected-hold.bin"),
            encode_replication_source_hold_v1(bootstrap)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
        let mut before = history(&ports);
        drop(ports);
        for attempt in 0..3 {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "changelog_v3_control_tests::bootstrap_attachment_tests::bootstrap_attachment_crash_child"])
                .env("RIFFDB_BOOTSTRAP_ATTACHMENT_PATH", scope.join("db.redb"))
                .env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge)
                .status().unwrap();
            assert_eq!(
                status.code(),
                Some(if edge == "attachment-committed" && attempt > 0 {
                    0
                } else {
                    93
                }),
                "{edge}, attempt {attempt}"
            );
            let store = RedbStore::open(scope.join("db.redb")).unwrap();
            let ports = RedbOperationalPorts {
                shared: store.shared,
            };
            let committed = edge == "attachment-committed";
            let recovered = history(&ports);
            assert_eq!(
                recovered.tail().sequence().get(),
                before.tail().sequence().get() + u64::from(committed && attempt == 0)
            );
            assert_eq!(
                retained(&ports, bootstrap),
                (!committed).then_some(bootstrap)
            );
            assert_eq!(
                retained(&ports, follower(bootstrap)),
                committed.then_some(follower(bootstrap))
            );
            let mut control = ports.replication_source_control();
            assert!(!control.reclaim_history().unwrap());
            append(&ports, b"checkpoint between attachment retries");
            control.reclaim_history().unwrap();
            assert_eq!(history(&ports).minimum_resume(), bootstrap.fence());
            // Every durable successor remains readable from the identical
            // bootstrap/follower fence, even after reclamation between kills.
            let root = std::sync::Arc::new(crate::checkpoint_root::CheckpointRoot::new(
                ports.shared.database.begin_read().unwrap(),
                1,
            ));
            let mut cursor = crate::changelog_v3_cursor::open(
                &crate::store::RedbReadAccess::Durable(root),
                bootstrap.lineage(),
                bootstrap.fence(),
            )
            .unwrap();
            let mut sequence = bootstrap.fence().sequence().get();
            while let Some(receipt) = cursor.next_receipt().unwrap() {
                sequence += 1;
                assert_eq!(receipt.binding().sequence.get(), sequence);
            }
            assert_eq!(sequence, history(&ports).tail().sequence().get());
            before = history(&ports);
            drop(cursor);
            drop(ports);
        }
        let store = RedbStore::open(scope.join("db.redb")).unwrap();
        let ports = RedbOperationalPorts {
            shared: store.shared,
        };
        let mut control = ports.replication_source_control();
        assert_eq!(
            control.attach_bootstrap(bootstrap).unwrap(),
            edge != "attachment-committed"
        );
        assert!(!control.attach_bootstrap(bootstrap).unwrap());
        assert_eq!(retained(&ports, bootstrap), None);
        assert_eq!(
            retained(&ports, follower(bootstrap)),
            Some(follower(bootstrap))
        );
    }
}

#[test]
fn follower_acknowledgement_port_advances_only_an_existing_follower_hold_and_retries_read_only() {
    let (_scope, ports, states) = fixture();
    let bootstrap = hold(Kind::Bootstrap, states[3]);
    let mut control = ports.replication_source_control();
    control.register(bootstrap).unwrap();
    let before = history(&ports);
    assert!(
        ports
            .acknowledge_replication_follower_v3(
                bootstrap.id(),
                bootstrap.lineage(),
                states[5].tail()
            )
            .is_err()
    );
    assert_eq!(history(&ports), before);
    control.attach_bootstrap(bootstrap).unwrap();
    ports
        .acknowledge_replication_follower_v3(bootstrap.id(), bootstrap.lineage(), states[5].tail())
        .unwrap();
    let updated = Hold::new(
        bootstrap.id(),
        Kind::FollowerAcknowledgement,
        bootstrap.lineage(),
        states[5].tail(),
    );
    assert_eq!(retained(&ports, updated), Some(updated));
    assert_eq!(retained(&ports, bootstrap), None);
    let after = history(&ports);
    ports
        .acknowledge_replication_follower_v3(bootstrap.id(), bootstrap.lineage(), states[5].tail())
        .unwrap();
    assert_eq!(history(&ports), after);
    assert!(
        ports
            .acknowledge_replication_follower_v3(
                bootstrap.id(),
                bootstrap.lineage(),
                states[3].tail()
            )
            .is_err()
    );
    assert_eq!(history(&ports), after);
    assert_eq!(retained(&ports, updated), Some(updated));
}

#[test]
fn follower_acknowledgement_cannot_replace_a_foreign_or_miskeyed_prior_hold() {
    for fault in 0..4 {
        let (_scope, ports, states) = fixture();
        let current = hold(Kind::FollowerAcknowledgement, states[3]);
        let lineage = current.lineage();
        let prior = match fault {
            0 => Hold::new(
                ReplicationSourceHoldIdV1::new([0x94; 16]).unwrap(),
                current.kind(),
                lineage,
                current.fence(),
            ),
            1 => Hold::new(
                current.id(),
                Kind::ArchiveAcknowledgement,
                lineage,
                current.fence(),
            ),
            2 => Hold::new(
                current.id(),
                current.kind(),
                ChangelogLineageV3::new(lineage.database_id(), 2, lineage.leadership_epoch())
                    .unwrap(),
                current.fence(),
            ),
            _ => Hold::new(
                current.id(),
                current.kind(),
                ChangelogLineageV3::new(
                    lineage.database_id(),
                    1,
                    LeadershipEpochV1::new(2).unwrap(),
                )
                .unwrap(),
                current.fence(),
            ),
        };
        // A valid envelope under a substituted key/lineage must never be
        // silently repaired by a later authenticated acknowledgement.
        let write = ports.shared.database.begin_write().unwrap();
        write
            .open_table(SOURCE_HOLDS)
            .unwrap()
            .insert(
                current.storage_key().as_slice(),
                encode_replication_source_hold_v1(prior).unwrap().as_bytes(),
            )
            .unwrap();
        ports.shared.commit_durable(write).unwrap();
        let receipt_bytes = || {
            let read = ports.shared.database.begin_read().unwrap();
            read.open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap()
                .iter()
                .unwrap()
                .map(|row| {
                    let (key, value) = row.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect::<Vec<_>>()
        };
        let before = receipt_bytes();
        let epoch = ports.shared.durable_commit_epoch();
        let error = ports
            .acknowledge_replication_follower_v3(current.id(), lineage, states[5].tail())
            .unwrap_err();
        // The existing drained barrier validates all retained hold bindings
        // before entering the acknowledgement transaction.
        assert!(
            matches!(error, Refusal::Storage(error) if error.kind() == riffdb_storage_api::StorageErrorKind::CorruptData)
        );
        assert_eq!(receipt_bytes(), before);
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        assert_eq!(retained(&ports, current), Some(prior));
    }
}
