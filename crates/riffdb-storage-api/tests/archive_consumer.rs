#![forbid(unsafe_code)]
//! Archive progress advances only after exact sink durability, never on emission.
// req: REP-007, REP-003
use riffdb_storage_api::*;
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

#[derive(Default)]
struct Sink {
    bytes: Vec<Vec<u8>>,
    uncertain_once: bool,
    fail_before_once: bool,
    attempts: usize,
}
impl ArchiveFrameSinkV1 for Sink {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1> {
        self.attempts += 1;
        assert_eq!(frame.lineage(), lineage());
        assert!(!format!("{frame:?}").contains("private-value"));
        use sha2::{Digest, Sha256};
        assert_eq!(
            frame.digest(),
            <[u8; 32]>::from(Sha256::digest(frame.as_bytes()))
        );
        if std::mem::take(&mut self.fail_before_once) {
            return Err(ArchiveConsumerErrorV1::SinkUnavailable);
        }
        if let Some(prior) = self.bytes.last()
            && prior == frame.as_bytes()
        {
            return Ok(());
        }
        self.bytes.push(frame.as_bytes().to_vec());
        if std::mem::take(&mut self.uncertain_once) {
            Err(ArchiveConsumerErrorV1::SinkUnavailable)
        } else {
            Ok(())
        }
    }
}
fn lineage() -> ChangelogLineageV3 {
    ChangelogLineageV3::new(
        DatabaseId::from_unix_milliseconds_and_random(1000, [1; 10]).unwrap(),
        1,
        LeadershipEpochV1::new(1).unwrap(),
    )
    .unwrap()
}
fn anchor() -> ChangelogHistoryPointV3 {
    ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).unwrap(),
        [0x77; 32],
        DualFrontier::INITIAL,
    )
}
fn frame(
    lineage: ChangelogLineageV3,
    before: ChangelogHistoryPointV3,
    prior_frame: [u8; 32],
    application: u64,
) -> Vec<u8> {
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            predecessor: Some(before.sequence()),
            sequence: before.sequence().checked_next().unwrap(),
            predecessor_frontier: before.frontier(),
            covered_frontier: DualFrontier::new(CommitSequence::new(application), None),
            prior_history_hash: before.history_hash(),
        },
        ChangelogAttributionV3::JournaledApplicationGroup,
        vec![
            AuthoritativeMutationV3::put(
                AuthoritativeNamespaceV1::Entities,
                b"key",
                None,
                b"private-value",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            lineage.database_id(),
            lineage.history_incarnation(),
            lineage.leadership_epoch().get(),
            lineage.catalog_digest(),
            prior_frame,
        )
        .unwrap(),
        vec![receipt],
    )
    .unwrap()
    .encode()
    .unwrap()
}
#[test]
fn archive_sink_uncertainty_retains_exact_frame_without_advancing_progress() {
    for failed_before_write in [false, true] {
        let sink = Sink {
            uncertain_once: !failed_before_write,
            fail_before_once: failed_before_write,
            ..Default::default()
        };
        let mut archive = ArchiveConsumerV1::new(sink, lineage(), anchor());
        let bytes = frame(lineage(), anchor(), anchor().history_hash(), 3);
        assert_eq!(
            archive.append(bytes.clone()),
            Err(ArchiveConsumerErrorV1::SinkUnavailable)
        );
        assert_eq!(archive.position(), anchor());
        assert!(archive.has_pending_frame());
        let confirmed = archive.retry_pending().unwrap();
        assert_eq!(confirmed.frontier().application().unwrap().get(), 3);
        assert_eq!(confirmed.sequence().get(), 2);
        assert!(!archive.has_pending_frame());
        let sink = archive.into_sink();
        assert_eq!(sink.attempts, 2);
        assert_eq!(sink.bytes, vec![bytes]);
    }
}
#[test]
fn archive_overflow_requires_resync_without_skipping_unconfirmed_frame() {
    let mut archive = ArchiveConsumerV1::new(
        Sink {
            uncertain_once: true,
            ..Default::default()
        },
        lineage(),
        anchor(),
    );
    let bytes = frame(lineage(), anchor(), anchor().history_hash(), 1);
    assert_eq!(
        archive.append(bytes.clone()),
        Err(ArchiveConsumerErrorV1::SinkUnavailable)
    );
    assert_eq!(
        archive.append(bytes),
        Err(ArchiveConsumerErrorV1::ResyncRequired)
    );
    assert_eq!(
        archive.retry_pending(),
        Err(ArchiveConsumerErrorV1::ResyncRequired)
    );
    assert_eq!(archive.position(), anchor());
    assert_eq!(archive.into_sink().attempts, 1);
}
#[test]
fn archive_reconnect_resets_only_frame_chain_at_exact_durable_history() {
    let mut archive = ArchiveConsumerV1::new(Sink::default(), lineage(), anchor());
    let first = frame(lineage(), anchor(), anchor().history_hash(), 1);
    let first_checksum = *first.last_chunk::<32>().unwrap();
    let after = archive.append(first).unwrap();
    let next = frame(lineage(), after, first_checksum, 2);
    let after = archive.append(next).unwrap();
    archive.begin_stream().unwrap();
    let after = archive
        .append(frame(lineage(), after, after.history_hash(), 3))
        .unwrap();
    assert_eq!(after.sequence().get(), 4);
    assert_eq!(archive.into_sink().bytes.len(), 3);
}
#[test]
fn archive_rejects_corrupt_foreign_duplicate_and_wrong_chain_before_sink() {
    let foreign =
        ChangelogLineageV3::new(lineage().database_id(), 2, lineage().leadership_epoch()).unwrap();
    let good = frame(lineage(), anchor(), anchor().history_hash(), 1);
    let mut corrupt = good.clone();
    corrupt[100] ^= 1;
    let mut trailing = good.clone();
    trailing.push(0);
    for bytes in [
        corrupt,
        trailing,
        vec![],
        frame(foreign, anchor(), anchor().history_hash(), 1),
        frame(lineage(), anchor(), [0x22; 32], 1),
    ] {
        let mut archive = ArchiveConsumerV1::new(Sink::default(), lineage(), anchor());
        assert!(archive.append(bytes).is_err());
        assert!(
            archive.append(good.clone()).is_err(),
            "terminal frame errors fuse the consumer"
        );
        assert_eq!(archive.position(), anchor());
        assert!(archive.into_sink().bytes.is_empty());
    }
    let mut archive = ArchiveConsumerV1::new(Sink::default(), lineage(), anchor());
    archive.append(good.clone()).unwrap();
    assert!(archive.append(good).is_err());
    assert_eq!(archive.into_sink().bytes.len(), 1);
}

#[test]
fn archive_rejects_gap_wrong_frontier_and_oversize_without_sink_io() {
    let gap = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(4).unwrap(),
        anchor().history_hash(),
        anchor().frontier(),
    );
    let wrong_frontier = ChangelogHistoryPointV3::new(
        anchor().sequence(),
        anchor().history_hash(),
        DualFrontier::new(CommitSequence::new(1), None),
    );
    for bytes in [
        frame(lineage(), gap, anchor().history_hash(), 1),
        frame(lineage(), wrong_frontier, anchor().history_hash(), 2),
        vec![0; MAX_CHANGELOG_FRAME_BYTES + 1],
    ] {
        let mut archive = ArchiveConsumerV1::new(Sink::default(), lineage(), anchor());
        assert!(archive.append(bytes).is_err());
        assert_eq!(archive.position(), anchor());
        assert_eq!(archive.into_sink().attempts, 0);
    }
}
