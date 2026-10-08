//! Real held artifacts and independent complete source-validation oracles.
// req: REP-003, REP-006, REC-001, END-008, LOG-001
use super::*;
use crate::maintenance::RedbBootstrapCustodyRead;

fn append(ports: &RedbOperationalPorts, ordinal: u64) {
    let _lease = ports.shared.mutation_gate.acquire().unwrap();
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        Source::OutboxTransition,
    )
    .unwrap();
    write
        .open_table(crate::layout::OUTBOX_STATUS)
        .unwrap()
        .insert(
            b"bootstrap-proof-fixture".as_slice(),
            ordinal.to_be_bytes().as_slice(),
        )
        .unwrap();
    write.finish().unwrap().commit(&ports.shared).unwrap();
}

fn ready(read: &mut RedbBootstrapCustodyRead) {
    for _ in 0..64 {
        if read.advance().unwrap() {
            return;
        }
    }
    panic!("bounded custody walk did not reach the exact end");
}

fn check_oracle(ports: &RedbOperationalPorts, control: &mut ReplicationSourceControl) {
    let root = ports.shared.capture_checkpoint_root().unwrap();
    let oracle = crate::changelog_v3_roots::validate_retained_history(&root)
        .unwrap()
        .unwrap();
    let epoch = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    let walk = control.online_validation.as_mut().unwrap();
    assert!(walk.continue_on(root, epoch).unwrap());
    assert_eq!(walk.proof().unwrap().history(), oracle);
    walk.release_completed_pin();
    assert!(walk.validation_pin().is_none());
    assert!(
        control.online_license.is_none(),
        "read proof cannot issue a reclamation license"
    );
    assert!(!ports.shared.mutation_gate.is_held());
}

#[test]
fn bootstrap_custody_continuation_matches_complete_source_oracle_without_population_walk_per_page()
{
    for population in [7_u64, 300, 600] {
        let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        for ordinal in 8..=population {
            append(&ports, ordinal);
        }
        let control = ports.shared_source_control().unwrap();
        let id = riffdb_storage_api::ReplicationSourceHoldIdV1::new([61; 16]).unwrap();
        let source = ports
            .prepare_replication_bootstrap_v3(&scope.join("source"), id)
            .unwrap();
        let manifest = source.manifest();
        let before = ports.shared.durable_commit_epoch();
        let mut read = source.into_custody_read().unwrap();
        assert_eq!(
            read.manifest().unwrap(),
            None,
            "capture cannot release unchecked bytes"
        );
        ready(&mut read);
        assert_eq!(read.manifest().unwrap(), Some(manifest));
        {
            let mut owner = control.lock().unwrap();
            check_oracle(&ports, &mut owner);
            assert_eq!(
                owner.online_validation.as_ref().unwrap().rows_read(),
                [population + 1, 0]
            );
        }
        let mut source = read.into_source().unwrap();
        let mut transcript =
            riffdb_storage_api::ReplicationBootstrapTranscriptV3::new(manifest.fence());
        for ordinal in 1..=manifest.page_count() {
            let mut read = source.into_custody_read().unwrap();
            ready(&mut read);
            transcript
                .observe(&read.read_page(ordinal).unwrap().unwrap())
                .unwrap();
            source = read.into_source().unwrap();
        }
        transcript.verify_manifest(manifest).unwrap();
        assert_eq!(
            ports.shared.durable_commit_epoch(),
            before,
            "custody reads never write or drain"
        );
        assert_eq!(
            control
                .lock()
                .unwrap()
                .online_validation
                .as_ref()
                .unwrap()
                .rows_read(),
            [population + 1, 0]
        );
        for ordinal in 1..=4 {
            append(&ports, population + ordinal);
            let mut read = source.into_custody_read().unwrap();
            ready(&mut read);
            assert_eq!(read.manifest().unwrap(), Some(manifest));
            check_oracle(&ports, &mut control.lock().unwrap());
            source = read.into_source().unwrap();
        }
        assert_eq!(
            control
                .lock()
                .unwrap()
                .online_validation
                .as_ref()
                .unwrap()
                .rows_read(),
            [population + 5, 0]
        );
        drop(source);
    }
}

#[test]
fn bootstrap_custody_refuses_retired_changed_corrupt_and_unclassified_evidence() {
    for scenario in [
        "retired",
        "fenced",
        "attached",
        "missing",
        "changed",
        "corrupt",
        "unclassified",
    ] {
        let (scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let control = ports.shared_source_control().unwrap();
        let id = riffdb_storage_api::ReplicationSourceHoldIdV1::new([62; 16]).unwrap();
        let source = ports
            .prepare_replication_bootstrap_v3(&scope.join("source"), id)
            .unwrap();
        let manifest = source.manifest();
        let hold = Hold::new(
            id,
            Kind::Bootstrap,
            manifest.fence().history().lineage(),
            manifest.fence().history().tail(),
        );
        let mut read = source.into_custody_read().unwrap();
        ready(&mut read);
        match scenario {
            "retired" => ports.online_retention_lane().retire_source_admission(),
            "fenced" => ports.shared.write_fenced.store(true, Ordering::Release),
            "attached" => {
                control.lock().unwrap().attach_bootstrap(hold).unwrap();
            }
            _ => {
                let _lease = ports.shared.mutation_gate.acquire().unwrap();
                let write = ports.shared.database.begin_write().unwrap();
                if scenario == "missing" {
                    write
                        .open_table(SOURCE_HOLDS)
                        .unwrap()
                        .remove(hold.storage_key().as_slice())
                        .unwrap();
                } else if scenario == "changed" {
                    let changed = Hold::new(id, Kind::Bootstrap, hold.lineage(), states[1].tail());
                    let encoded = encode_replication_source_hold_v1(changed).unwrap();
                    write
                        .open_table(SOURCE_HOLDS)
                        .unwrap()
                        .insert(hold.storage_key().as_slice(), encoded.as_bytes())
                        .unwrap();
                } else if scenario == "corrupt" {
                    write
                        .open_table(HISTORY)
                        .unwrap()
                        .insert(7_u64.to_be_bytes().as_slice(), b"corrupt".as_slice())
                        .unwrap();
                }
                ports.shared.commit_durable(write).unwrap();
            }
        }
        if scenario == "unclassified" {
            assert_eq!(
                read.manifest().unwrap(),
                None,
                "unknown publication cannot reuse an old proof"
            );
            ready(&mut read);
            assert_eq!(read.manifest().unwrap(), Some(manifest));
            check_oracle(&ports, &mut control.lock().unwrap());
        } else {
            let mut refused = false;
            for _ in 0..64 {
                match read.manifest() {
                    Ok(None) => {}
                    Err(_) => {
                        refused = true;
                        break;
                    }
                    Ok(Some(_)) => panic!("changed custody released a manifest: {scenario}"),
                }
            }
            assert!(refused, "changed custody must refuse: {scenario}");
            assert!(read.read_page(1).is_err());
        }
    }
}

#[test]
fn bootstrap_custody_cancellation_preserves_shared_candidate_capacity_and_pin_bound() {
    let (scope, ports, states) = crate::changelog_v3_control_tests::fixture();
    let control = ports.shared_source_control().unwrap();
    let id = riffdb_storage_api::ReplicationSourceHoldIdV1::new([63; 16]).unwrap();
    let source = ports
        .prepare_replication_bootstrap_v3(&scope.join("source"), id)
        .unwrap();
    let follower = Hold::new(
        riffdb_storage_api::ReplicationSourceHoldIdV1::new([64; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        states[1].lineage(),
        states[1].tail(),
    );
    control.lock().unwrap().register(follower).unwrap();
    let request = riffdb_storage_api::ReplicationFollowerAcknowledgementV3::new(
        follower.id(),
        follower.lineage(),
        states[5].tail(),
    );
    let ack = ports
        .begin_replication_follower_acknowledgement_v3(request)
        .unwrap();
    {
        let mut owner = control.lock().unwrap();
        let mut complete = false;
        for _ in 0..64 {
            if owner
                .prepare_follower_acknowledgement(request.hold())
                .unwrap()
            {
                complete = true;
                break;
            }
        }
        assert!(complete);
    }
    let before = control.lock().unwrap().prepared_acknowledgement.clone();
    let mut read = source.into_custody_read().unwrap();
    assert!(!read.advance().unwrap());
    let source = read.into_source().unwrap();
    assert_eq!(control.lock().unwrap().prepared_acknowledgement, before);
    assert_eq!(
        control.lock().unwrap().acknowledgement_active,
        Some(request.hold())
    );
    drop(ack);
    let now = std::time::Instant::now();
    {
        let mut owner = control.lock().unwrap();
        owner
            .source_pacing
            .as_mut()
            .unwrap()
            .charge(now, std::time::Duration::from_secs(1));
    }
    // An unknown publication demands a new pin. Cancelling its real owner
    // releases that pin without affecting an acknowledgement that arrived later.
    let write = ports.shared.database.begin_write().unwrap();
    ports.shared.commit_durable(write).unwrap();
    let mut read = source.into_custody_read().unwrap();
    assert!(!read.advance().unwrap());
    let pin = control
        .lock()
        .unwrap()
        .online_validation
        .as_ref()
        .unwrap()
        .validation_pin()
        .unwrap();
    assert!(!control.lock().unwrap().source_background_work_ready());
    ports
        .online_retention_lane()
        .advance_source_validation_off_gate()
        .unwrap();
    assert_eq!(
        control
            .lock()
            .unwrap()
            .source_pacing
            .as_ref()
            .unwrap()
            .wait(now),
        std::time::Duration::from_secs(19)
    );
    let mut queued_ack = ports
        .begin_replication_follower_acknowledgement_v3(request)
        .unwrap();
    assert!(!queued_ack.advance().unwrap());
    drop(queued_ack);
    assert!(
        pin.upgrade().is_some(),
        "another cancelled request cannot revoke this reader's pin"
    );
    drop(read);
    assert!(pin.upgrade().is_none());
    let owner = control.lock().unwrap();
    assert!(!owner.bootstrap_read_active());
    assert!(owner.online_validation.is_none());
    assert!(owner.prepared_acknowledgement.is_none());
    assert_eq!(
        owner.source_pacing.as_ref().unwrap().wait(now),
        std::time::Duration::from_secs(19)
    );
    let bookkeeping = std::mem::size_of::<ReplicationSourceControl>()
        + std::mem::size_of::<RetainedHistoryValidation>()
        + 4096
        + 4 * std::mem::size_of::<RedbBootstrapCustodyRead>()
        + 4 * usize::try_from(MAX_REPLICATION_SOURCE_HOLDS_V1).unwrap()
        + riffdb_types::MAX_KEY_BYTES;
    assert!(bookkeeping <= 64 * 1024);
}

#[test]
fn bootstrap_custody_two_readers_preserve_owner_and_recheck_retirement_after_actual_page_io() {
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let control = ports.shared_source_control().unwrap();
    let first = ports
        .prepare_replication_bootstrap_v3(
            &scope.join("first"),
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([65; 16]).unwrap(),
        )
        .unwrap();
    let second = ports
        .prepare_replication_bootstrap_v3(
            &scope.join("second"),
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([66; 16]).unwrap(),
        )
        .unwrap();
    let manifest = first.manifest();
    let mut first = first.into_custody_read().unwrap();
    assert!(!first.advance().unwrap());
    let pin = control
        .lock()
        .unwrap()
        .online_validation
        .as_ref()
        .unwrap()
        .validation_pin()
        .unwrap();
    let mut second = second.into_custody_read().unwrap();
    assert!(!second.advance().unwrap());
    drop(second);
    assert!(
        pin.upgrade().is_some(),
        "a different reader cannot cancel the active candidate"
    );
    ready(&mut first);
    assert!(
        pin.upgrade().is_none(),
        "a completed proof releases its actual pin"
    );
    assert_eq!(first.manifest().unwrap(), Some(manifest));
    let hold = Hold::new(
        manifest.fence().hold_id(),
        Kind::Bootstrap,
        manifest.fence().history().lineage(),
        manifest.fence().history().tail(),
    );
    let mutation = Arc::clone(&control);
    first.after_page_io(move || {
        mutation.lock().unwrap().attach_bootstrap(hold).unwrap();
    });
    // The hook runs after the real artifact read and before the real current
    // custody check. Old prepared authority cannot release those page bytes.
    assert_eq!(first.read_page(1).unwrap(), None);
    let mut refused = false;
    for _ in 0..64 {
        match first.manifest() {
            Ok(None) => {}
            Err(_) => {
                refused = true;
                break;
            }
            Ok(Some(_)) => panic!("post-I/O attachment released an old manifest"),
        }
    }
    assert!(refused);
    assert!(first.read_page(1).is_err());
    assert!(
        first.into_source().is_err(),
        "a failed closed read stays fused"
    );
    let root = ports.shared.capture_checkpoint_root().unwrap();
    crate::changelog_v3_roots::validate_retained_history(&root)
        .unwrap()
        .unwrap();
    assert!(
        root.open_table(SOURCE_HOLDS)
            .unwrap()
            .get(hold.storage_key().as_slice())
            .unwrap()
            .is_none()
    );
}
