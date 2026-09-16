//! Archive framing uses the existing wire bounds and exact receipt identities.
// req: REP-007, REP-003
use super::*;

#[test]
fn coalesced_frames_preserve_receipts_checksum_chain_and_exact_resume() {
    let source = snapshot(MAX_STAGED_COMMANDS as u64 + 2);
    let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
    let first = stream.next_coalesced_frame().unwrap().unwrap();
    let decoded = ChangelogFrameV3::decode(first.as_bytes()).unwrap();
    assert_eq!(decoded.receipts(), &source.rows[..MAX_STAGED_COMMANDS]);
    assert_eq!(
        decoded.binding().prior_frame_hash(),
        anchor().history_hash()
    );
    let second = stream.next_coalesced_frame().unwrap().unwrap();
    let decoded = ChangelogFrameV3::decode(second.as_bytes()).unwrap();
    assert_eq!(decoded.receipts(), &source.rows[MAX_STAGED_COMMANDS..]);
    assert_eq!(
        decoded.binding().prior_frame_hash(),
        *first.as_bytes().last_chunk::<32>().unwrap()
    );
    assert_eq!(second.covered(), source.history.tail());
    assert!(stream.next_coalesced_frame().unwrap().is_none());
    let newer = snapshot(MAX_STAGED_COMMANDS as u64 + 5);
    let mut resumed = ChangelogFrameCursorV3::open(&newer, handshake(first.covered())).unwrap();
    let frame = resumed.next_coalesced_frame().unwrap().unwrap();
    assert_eq!(
        ChangelogFrameV3::decode(frame.as_bytes())
            .unwrap()
            .receipts(),
        &newer.rows[MAX_STAGED_COMMANDS..]
    );
    assert_eq!(frame.covered(), newer.history.tail());
}

fn application_snapshot(transitions: &[u64], value_bytes: usize) -> Snapshot {
    let mut history =
        ChangelogHistoryStateV3::new(lineage(), anchor(), anchor(), anchor()).unwrap();
    let mut rows = Vec::new();
    let value = vec![0x51; value_bytes];
    for &count in transitions {
        let before = history.tail();
        let row = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: lineage().database_id(),
                history_incarnation: 1,
                predecessor: Some(before.sequence()),
                sequence: before.sequence().checked_next().unwrap(),
                predecessor_frontier: before.frontier(),
                prior_history_hash: before.history_hash(),
                covered_frontier: DualFrontier::new(
                    CommitSequence::new(
                        before.frontier().application().map_or(0, |v| v.get()) + count,
                    ),
                    None,
                ),
            },
            ChangelogAttributionV3::JournaledApplicationGroup,
            vec![
                AuthoritativeMutationV3::put(
                    AuthoritativeNamespaceV1::Entities,
                    &before.sequence().get().to_be_bytes(),
                    None,
                    &value,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        history = history.advance(&row).unwrap();
        rows.push(row);
    }
    Snapshot { history, rows }
}

#[test]
fn coalesced_frames_charge_command_transitions_and_preserve_lookahead_on_repin() {
    let source = application_snapshot(&[160, 160, 96, 1], 1);
    let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
    let first = stream.next_coalesced_frame().unwrap().unwrap();
    assert_eq!(
        ChangelogFrameV3::decode(first.as_bytes())
            .unwrap()
            .receipts(),
        &source.rows[..1]
    );
    stream.advance_snapshot(&source).unwrap();
    let second = stream.next_coalesced_frame().unwrap().unwrap();
    assert_eq!(
        ChangelogFrameV3::decode(second.as_bytes())
            .unwrap()
            .receipts(),
        &source.rows[1..3]
    );
    let third = stream.next_coalesced_frame().unwrap().unwrap();
    assert_eq!(
        ChangelogFrameV3::decode(third.as_bytes())
            .unwrap()
            .receipts(),
        &source.rows[3..]
    );
    assert_eq!(third.covered(), source.history.tail());
    assert!(stream.next_coalesced_frame().unwrap().is_none());
}

#[test]
fn coalesced_frames_respect_byte_bound_without_splitting_or_losing_receipts() {
    let source = application_snapshot(&[1, 1], MAX_CHANGELOG_FRAME_BYTES / 2);
    let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
    let first = stream.next_coalesced_frame().unwrap().unwrap();
    assert!(first.as_bytes().len() <= MAX_CHANGELOG_FRAME_BYTES);
    assert_eq!(
        ChangelogFrameV3::decode(first.as_bytes())
            .unwrap()
            .receipts(),
        &source.rows[..1]
    );
    // Switching to ordinary one-receipt emission must consume the same lookahead.
    let second = stream.next_frame().unwrap().unwrap();
    assert!(second.as_bytes().len() <= MAX_CHANGELOG_FRAME_BYTES);
    assert_eq!(
        ChangelogFrameV3::decode(second.as_bytes())
            .unwrap()
            .receipts(),
        &source.rows[1..]
    );
    assert_eq!(second.covered(), source.history.tail());
    assert!(stream.next_coalesced_frame().unwrap().is_none());
}

#[test]
fn coalesced_frame_refuses_late_gap_or_truncation_without_partial_emission() {
    for gap in [true, false] {
        let mut source = snapshot(3);
        if gap {
            source.rows.remove(1);
        } else {
            source.rows.pop();
        }
        let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
        assert_eq!(
            stream.next_coalesced_frame().unwrap_err(),
            ReplicationStreamErrorV3::CorruptHistory
        );
        assert_eq!(stream.position(), anchor());
        assert_eq!(
            stream.next_frame().unwrap_err(),
            ReplicationStreamErrorV3::CorruptHistory
        );
        assert_eq!(
            stream.advance_snapshot(&snapshot(3)),
            Err(ReplicationStreamErrorV3::CorruptHistory)
        );
    }
}

#[test]
fn coalesced_lookahead_does_not_bypass_a_pruned_replacement_pin() {
    let mut source = application_snapshot(&[160, 160, 1], 1);
    let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
    let first = stream.next_coalesced_frame().unwrap().unwrap();
    let pruned = ChangelogHistoryPointV3::from_receipt(&source.rows[1]).unwrap();
    source.history =
        ChangelogHistoryStateV3::new(lineage(), anchor(), source.history.tail(), pruned).unwrap();
    assert_eq!(
        stream.advance_snapshot(&source),
        Err(ReplicationStreamErrorV3::HistoryPruned)
    );
    assert_eq!(
        stream.next_coalesced_frame().unwrap_err(),
        ReplicationStreamErrorV3::HistoryPruned
    );
    assert_eq!(stream.position(), first.covered());
}

#[test]
fn coalesced_frame_checks_foreign_lookahead_before_emitting_any_prefix() {
    let mut source = application_snapshot(&[160, 160, 1], 1);
    let row = &source.rows[1];
    let mut binding = row.binding();
    binding.database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x72; 10]).unwrap();
    source.rows[1] =
        AuthoritativeTransactionV3::new(binding, row.attribution(), row.mutations().to_vec())
            .unwrap();
    let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
    assert_eq!(
        stream.next_coalesced_frame().unwrap_err(),
        ReplicationStreamErrorV3::CorruptHistory
    );
    assert_eq!(stream.position(), anchor());
}

struct UncertainArchive(Vec<Vec<u8>>);
impl ArchiveFrameSinkV1 for UncertainArchive {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1> {
        self.0.push(frame.as_bytes().to_vec());
        if self.0.len() == 1 {
            Err(ArchiveConsumerErrorV1::SinkUnavailable)
        } else {
            Ok(())
        }
    }
}

#[test]
fn coalesced_archive_retry_confirms_only_the_exact_complete_pending_frame() {
    let source = snapshot(MAX_STAGED_COMMANDS as u64 + 2);
    let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
    let frame = stream.next_coalesced_frame().unwrap().unwrap();
    let first = frame.covered();
    let mut archive = ArchiveConsumerV1::new(UncertainArchive(vec![]), lineage(), anchor());
    assert_eq!(
        archive.append(frame.into_bytes()),
        Err(ArchiveConsumerErrorV1::SinkUnavailable)
    );
    assert_eq!(archive.position(), anchor());
    assert!(archive.has_pending_frame());
    assert_eq!(archive.retry_pending().unwrap(), first);
    assert!(!archive.has_pending_frame());
    let frame = stream.next_coalesced_frame().unwrap().unwrap();
    assert_eq!(
        archive.append(frame.into_bytes()).unwrap(),
        source.history.tail()
    );
    let attempts = archive.into_sink().0;
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0], attempts[1]);
    let first = ChangelogFrameV3::decode(&attempts[0]).unwrap();
    let second = ChangelogFrameV3::decode(&attempts[2]).unwrap();
    let mut receipts = first.receipts().to_vec();
    receipts.extend_from_slice(second.receipts());
    assert_eq!(receipts, source.rows);
}
