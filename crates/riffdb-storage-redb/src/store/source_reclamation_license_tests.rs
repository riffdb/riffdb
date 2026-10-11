use super::*;

// req: STO-030, STO-031, STO-032, REC-001, END-008
#[test]
fn checkpoint_license_reclaims_its_complete_finite_prefix_without_new_progress() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        finite_prefix(profile);
    }
}

fn finite_prefix(profile: crate::RedbCommitProfile) {
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    Arc::get_mut(&mut ports.shared)
        .unwrap()
        .application_commit_profile = profile;
    for ordinal in 1..=1024 {
        append_for_profile(&mut ports, ordinal, profile);
    }
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    ports
        .shared
        .checkpoint_published_journal_suffix_for_barrier()
        .unwrap();
    drop(writer);
    ports.shared.join_journal_sealer();
    let mut control = ports.replication_source_control();
    for _ in 0..128 {
        assert_eq!(control.reclaim_history_online().unwrap().0, 0);
        if control.reclamation_yields()[0] == 1 {
            break;
        }
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(control.reclamation_yields()[0], 1);
    let history =
        crate::changelog_v3_cursor::history(&ports.begin_composite_read().unwrap()).unwrap();
    let licensed_prefix =
        history.tail().sequence().get() - history.minimum_resume().sequence().get();
    assert!(licensed_prefix >= 1024);
    append_for_profile(&mut ports, 1025, profile);
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    ports
        .shared
        .checkpoint_published_journal_suffix_for_barrier()
        .unwrap();
    drop(writer);
    ports.shared.join_journal_sealer();
    let mut removed = 0;
    for _ in 0..128 {
        removed += control.reclaim_history_online().unwrap().0;
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
        if removed == licensed_prefix {
            break;
        }
    }
    assert_eq!(
        removed, licensed_prefix,
        "a real independently durable checkpoint covers this entire prefix; new cuts must consume only its finite licensed range"
    );
    let completed =
        crate::changelog_v3_cursor::history(&ports.begin_composite_read().unwrap()).unwrap();
    assert_eq!(
        completed.minimum_resume().sequence().get(),
        history.tail().sequence().get()
    );
    // Our five cuts, their checkpoint and repeated observations cannot issue
    // more coverage without an independently committed transaction.
    checkpoint(&ports);
    for _ in 0..16 {
        assert_eq!(control.reclaim_history_online().unwrap().0, 0);
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(
        crate::changelog_v3_cursor::history(&ports.begin_composite_read().unwrap()).unwrap(),
        completed
    );
    append_for_profile(&mut ports, 1026, profile);
    checkpoint(&ports);
    let mut new_removed = 0;
    for _ in 0..16 {
        new_removed += control.reclaim_history_online().unwrap().0;
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(
        new_removed, 1,
        "only the preceding independently observed tail licenses the next prefix"
    );
}

fn checkpoint(ports: &RedbOperationalPorts) {
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    ports
        .shared
        .checkpoint_published_journal_suffix_for_barrier()
        .unwrap();
    drop(writer);
    ports.shared.join_journal_sealer();
}

fn observed_control(ports: &mut RedbOperationalPorts) -> crate::store::ReplicationSourceControl {
    append_acknowledged_audits(ports, 1024);
    checkpoint(ports);
    let mut control = ports.replication_source_control();
    for _ in 0..128 {
        assert_eq!(control.reclaim_history_online().unwrap().0, 0);
        if control.reclamation_yields()[0] == 1 {
            break;
        }
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(control.reclamation_yields()[0], 1);
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    let ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[source_audit_intent(1025)])
        .unwrap()
    else {
        panic!("ordinary fence");
    };
    assert_eq!(fence.wait().unwrap().len(), 1);
    checkpoint(ports);
    control
}

// req: STO-030, STO-031, STO-032, REC-001, END-008
#[test]
fn checkpoint_license_preserves_blocked_interval_until_current_derived_pin_releases() {
    use crate::derived_source_pin::RedbDerivedSourcePin;
    use crate::source_validation_read::ReceiptRead;
    use riffdb_storage_api::{
        ChangelogHistoryPointV3 as Point, ChangelogTransactionSequence as Sequence,
    };
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = observed_control(&mut ports);
    let root = ports.shared.capture_source_validation_pin().unwrap();
    let history = root.history().unwrap().unwrap();
    let sequence = Sequence::new(history.minimum_resume().sequence().get() + 400).unwrap();
    let point = Point::from_receipt(&root.receipt(sequence, None).unwrap().unwrap()).unwrap();
    drop(root);
    let pin = RedbDerivedSourcePin::capture(
        ports.begin_composite_read().unwrap(),
        Arc::clone(&ports.shared.derived_source_pins),
    )
    .unwrap()
    .at(point)
    .unwrap();
    let mut removed = 0;
    for _ in 0..16 {
        let cut = control.reclaim_history_online().unwrap().0;
        assert!(cut <= 256);
        removed += cut;
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(removed, 400);
    assert_eq!(control.reclamation_yields()[2], 1);
    drop(pin);
    for _ in 0..16 {
        removed += control.reclaim_history_online().unwrap().0;
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(
        removed, 1030,
        "a blocking pin does not spend the remaining licensed prefix"
    );
}

// req: STO-030, STO-031, STO-032, REC-001, END-008
#[test]
fn checkpoint_license_discards_unclassified_publication_without_self_renewal() {
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = observed_control(&mut ports);
    let mut removed = 0;
    for _ in 0..16 {
        removed += control.reclaim_history_online().unwrap().0;
        if removed > 0 {
            break;
        }
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(removed, 256);
    ports.shared.invalidate_source_continuation();
    checkpoint(&ports);
    let current =
        crate::changelog_v3_cursor::history(&ports.begin_composite_read().unwrap()).unwrap();
    for _ in 0..32 {
        assert_eq!(control.reclaim_history_online().unwrap().0, 0);
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(
        crate::changelog_v3_cursor::history(&ports.begin_composite_read().unwrap()).unwrap(),
        current
    );
}

fn append_for_profile(
    ports: &mut RedbOperationalPorts,
    ordinal: u64,
    profile: crate::RedbCommitProfile,
) {
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    let values = match ports
        .submit_service_audit_group(&[source_audit_intent(ordinal)])
        .unwrap()
    {
        ServiceAuditGroupAppend::Complete(values) => {
            assert_eq!(profile, crate::RedbCommitProfile::Hardened);
            values
        }
        ServiceAuditGroupAppend::Submitted(fence) => {
            assert_eq!(profile, crate::RedbCommitProfile::Standard);
            fence.wait().unwrap()
        }
    };
    assert_eq!(
        values.len(),
        1,
        "use each profile's actual durable completion"
    );
}

// req: STO-030, STO-031, STO-032, REC-001, END-008
#[test]
fn checkpoint_license_quantum_shrinks_to_exact_canonical_preimage_byte_bound() {
    use crate::source_validation_read::ReceiptRead;
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
        Arc::get_mut(&mut ports.shared)
            .unwrap()
            .application_commit_profile = profile;
        let append = |ports: &RedbOperationalPorts, byte| {
            let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
                &ports.shared.database,
                crate::RedbCommitProfile::Hardened,
                riffdb_storage_api::ChangelogAttributionV3::OutboxTransition,
            )
            .unwrap();
            write
                .open_table(crate::layout::OUTBOX_STATUS)
                .unwrap()
                .insert(
                    b"byte-bound-fixture".as_slice(),
                    vec![byte; 256 * 1024].as_slice(),
                )
                .unwrap();
            write.finish().unwrap().commit(&ports.shared).unwrap();
        };
        for ordinal in 0..256 {
            append(&ports, (ordinal % 2) as u8);
        }
        let mut control = ports.replication_source_control();
        for _ in 0..32 {
            assert_eq!(control.reclaim_history_online().unwrap().0, 0);
            for _ in 0..8 {
                control.advance_validation_off_gate().unwrap();
            }
            if control.reclamation_yields()[0] == 1 {
                break;
            }
        }
        assert_eq!(control.reclamation_yields()[0], 1);
        let source = ports.shared.capture_source_validation_pin().unwrap();
        let history = source.history().unwrap().unwrap();
        let lengths: Vec<_> = (history.minimum_resume().sequence().get()
            ..history.tail().sequence().get())
            .map(|seq| {
                source
                    .receipt(
                        riffdb_storage_api::ChangelogTransactionSequence::new(seq).unwrap(),
                        None,
                    )
                    .unwrap()
                    .unwrap()
                    .encode()
                    .unwrap()
                    .len()
            })
            .collect();
        assert!(lengths.iter().sum::<usize>() > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES);
        drop(source);
        append(&ports, 3);
        let mut removed = 0_usize;
        let mut cuts = 0;
        for _ in 0..32 {
            let count = control.reclaim_history_online().unwrap().0 as usize;
            assert!(count <= 256);
            assert!(
                lengths[removed..removed + count].iter().sum::<usize>()
                    <= riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES
            );
            if count > 0 {
                assert!(count < 256, "canonical bytes must shrink this quantum");
                cuts += 1;
            }
            removed += count;
            for _ in 0..8 {
                control.advance_validation_off_gate().unwrap();
            }
            if removed == lengths.len() {
                break;
            }
        }
        assert_eq!(removed, lengths.len());
        assert!(cuts >= 2);
    }
}
