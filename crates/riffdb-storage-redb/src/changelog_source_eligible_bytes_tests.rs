// req: STO-030, STO-031, END-007, END-008
use super::*;
#[test]
fn actual_source_bytes_remain_unknown_until_the_exact_original_ceiling() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let root = ports.shared.capture_checkpoint_root().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&root)
        .unwrap()
        .unwrap();
    let start = history.minimum_resume().sequence().get();
    let end = history.tail().sequence().get();
    let table = root.open_table(HISTORY).unwrap();
    let expected: u64 = (start..end)
        .map(|sequence| {
            let row = table
                .get(sequence.to_be_bytes().as_slice())
                .unwrap()
                .unwrap();
            crate::history_stub::resolve_history_value(row.value())
                .unwrap()
                .len() as u64
        })
        .sum();
    assert!(expected > 0);
    let now = Instant::now();
    let mut probe = Probe::new(root.clone(), history, end, now).unwrap();
    assert_eq!(probe.bytes(now), None, "an unscanned prefix cannot be zero");
    probe.advance(now, 2).unwrap();
    assert_eq!(
        probe.bytes(now),
        None,
        "a bounded deletion-sized prefix is not the backlog"
    );
    for _ in 0..8 {
        probe.advance(now, 2).unwrap();
    }
    assert_eq!(probe.bytes(now), Some(expected));
}
fn append(ports: &RedbOperationalPorts, value: &[u8]) {
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        riffdb_storage_api::ChangelogAttributionV3::OutboxTransition,
    )
    .unwrap();
    write
        .open_table(crate::layout::OUTBOX_STATUS)
        .unwrap()
        .insert(b"opaque-retention-key".as_slice(), value)
        .unwrap();
    write.finish().unwrap().commit(&ports.shared).unwrap();
}

fn original(
    ports: &RedbOperationalPorts,
) -> (Arc<crate::checkpoint_root::CheckpointRoot>, History) {
    let root = ports.shared.capture_checkpoint_root().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&root)
        .unwrap()
        .unwrap();
    (root, history)
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_bytes_cover_the_uncapped_native_ceiling_and_actual_different_sized_records() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    for row in 0..300 {
        append(&ports, &vec![1; if row % 2 == 0 { 17 } else { 4096 }]);
    }
    let (root, history) = original(&ports);
    let first = history.minimum_resume().sequence().get();
    let end = history.tail().sequence().get();
    let (bounded, observation) =
        super::super::retention::floor(&root, history, history.tail(), None).unwrap();
    assert_eq!(bounded.unwrap().sequence().get(), first + 256);
    assert_eq!(observation[5], end);
    let table = root.open_table(HISTORY).unwrap();
    let lengths: Vec<_> = (first..end)
        .map(|seq| {
            let row = table.get(seq.to_be_bytes().as_slice()).unwrap().unwrap();
            u64::try_from(
                crate::history_stub::resolve_history_value(row.value())
                    .unwrap()
                    .len(),
            )
            .unwrap()
        })
        .collect();
    assert!(lengths.iter().max() > lengths.iter().min());
    let now = Instant::now();
    let mut probe = Probe::new(root.clone(), history, observation[5], now).unwrap();
    probe.advance(now, 256).unwrap();
    assert_eq!(probe.bytes(now), None);
    // A concurrent actual publication must not change the original snapshot.
    append(
        &ports,
        b"concurrent publication excluded from original range",
    );
    probe.advance(now, 256).unwrap();
    let sample = probe.sample(now).unwrap();
    assert_eq!(sample[4], end - first);
    assert_eq!(sample[5], lengths.iter().sum::<u64>());
    assert_eq!(sample[6], (end - first) * 8);
    assert_eq!(sample[9], 2);
    assert!(
        probe.root.is_none(),
        "completion must release the journal reader pin"
    );
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_byte_probe_expiry_and_owner_retirement_release_registered_roots() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let (root, history) = original(&ports);
    let weak = Arc::downgrade(&root);
    assert!(
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .any(|registered| registered.ptr_eq(&weak))
    );
    let now = Instant::now();
    let mut probe = Probe::new(root, history, history.tail().sequence().get(), now).unwrap();
    probe.advance(now, 1).unwrap();
    assert!(weak.upgrade().is_some());
    probe.advance(now + MAX_AGE, 256).unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(probe.bytes(now + MAX_AGE), None);
    let (root, history) = original(&ports);
    let weak = Arc::downgrade(&root);
    let mut observer = Observer::default();
    observer
        .capture(root, history, history.tail().sequence().get(), now)
        .unwrap();
    observer.retire();
    assert!(weak.upgrade().is_none());
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn only_a_complete_fresh_empty_source_prefix_is_zero_and_capture_rate_is_bounded() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let (root, history) = original(&ports);
    let now = Instant::now();
    let mut probe = Probe::new(
        root.clone(),
        history,
        history.minimum_resume().sequence().get(),
        now,
    )
    .unwrap();
    assert_eq!(probe.bytes(now), None);
    probe.advance(now, 256).unwrap();
    assert_eq!(probe.bytes(now), Some(0));
    assert!(probe.root.is_none());
    assert_eq!(probe.bytes(now + MAX_AGE), None);
    let mut observer = Observer::default();
    observer
        .capture(root.clone(), history, history.tail().sequence().get(), now)
        .unwrap();
    let captured = observer.probe.as_ref().unwrap().captured;
    observer
        .capture(
            root.clone(),
            history,
            history.tail().sequence().get(),
            now + Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(observer.probe.as_ref().unwrap().captured, captured);
    observer
        .capture(
            root,
            history,
            history.tail().sequence().get(),
            now + CAPTURE_INTERVAL,
        )
        .unwrap();
    assert_eq!(
        observer.probe.as_ref().unwrap().captured,
        now + CAPTURE_INTERVAL
    );
}
// req: STO-030, STO-031, END-007, END-008
#[test]
fn corrupt_or_overflowing_source_observations_never_publish_partial_bytes() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let (root, history) = original(&ports);
    let now = Instant::now();
    let mut observer = Observer::default();
    observer
        .capture(root, history, history.tail().sequence().get(), now)
        .unwrap();
    observer.probe.as_mut().unwrap().bytes = u64::MAX;
    assert_eq!(
        observer.advance(now).unwrap_err().kind(),
        StorageErrorKind::LimitExceeded
    );
    assert!(observer.probe.is_none());
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(HISTORY)
        .unwrap()
        .insert(
            history
                .minimum_resume()
                .sequence()
                .get()
                .to_be_bytes()
                .as_slice(),
            b"malformed canonical receipt".as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    let root = ports.shared.capture_checkpoint_root().unwrap();
    let weak = Arc::downgrade(&root);
    // Corrupt fixtures intentionally give the observation the prior expected
    // identity; a live owner can never convert this mismatch into zero.
    observer
        .capture(
            root,
            history,
            history.tail().sequence().get(),
            now + CAPTURE_INTERVAL,
        )
        .unwrap();
    assert!(observer.advance(now + CAPTURE_INTERVAL).is_err());
    assert!(observer.probe.is_none());
    assert!(weak.upgrade().is_none());
}

// req: STO-030, STO-031, STO-032, END-007, END-008
#[test]
fn online_source_bytes_use_the_actual_barrier_ceiling_without_additional_writer_turns() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = ports.replication_source_control();
    for _ in 0..16 {
        control.reclaim_history_online().unwrap();
        if control.reclamation_yields()[0] == 1 {
            break;
        }
    }
    assert_eq!(control.reclamation_yields()[0], 1);
    append(
        &ports,
        b"independent source progress for original eligibility",
    );
    for _ in 0..16 {
        if control.reclaim_history_online().unwrap().0 > 0 {
            break;
        }
    }
    let plan = control.reclamation_observation().unwrap();
    let probe = control.eligible_bytes.probe.as_ref().unwrap();
    assert_eq!((probe.next, probe.ceiling), (plan[0], plan[5]));
    let root = probe.root.as_ref().unwrap().clone();
    let table = root.open_table(HISTORY).unwrap();
    let expected = (plan[0]..plan[5])
        .map(|seq| {
            let row = table.get(seq.to_be_bytes().as_slice()).unwrap().unwrap();
            u64::try_from(
                crate::history_stub::resolve_history_value(row.value())
                    .unwrap()
                    .len(),
            )
            .unwrap()
        })
        .sum::<u64>();
    let before = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    control.eligible_bytes.advance(Instant::now()).unwrap();
    assert_eq!(
        control
            .eligible_bytes
            .probe
            .as_ref()
            .unwrap()
            .bytes(Instant::now()),
        Some(expected)
    );
    assert!(
        control
            .eligible_bytes
            .probe
            .as_ref()
            .unwrap()
            .root
            .is_none()
    );
    let after = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        before, after,
        "a byte observation cannot write or stimulate reclamation"
    );
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn switching_to_offline_source_control_releases_pending_observation_pins() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let (root, history) = original(&ports);
    let weak = Arc::downgrade(&root);
    let mut control = ports.replication_source_control();
    control
        .eligible_bytes
        .capture(
            root,
            history,
            history.tail().sequence().get(),
            Instant::now(),
        )
        .unwrap();
    assert!(weak.upgrade().is_some());
    assert!(!control.reclaim_history().unwrap());
    assert!(
        weak.upgrade().is_none(),
        "offline ownership must not retain an unadvanced metric pin"
    );
}
