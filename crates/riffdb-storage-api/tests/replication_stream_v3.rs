#![forbid(unsafe_code)]
//! Receipt-only framing, exact resume, and fail-closed negotiation.
// req: REP-003, PERF-007

use std::collections::VecDeque;

use riffdb_storage_api::*;
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

fn lineage() -> ChangelogLineageV3 {
    ChangelogLineageV3::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap(),
        1,
        LeadershipEpochV1::initial(),
    )
    .unwrap()
}

fn anchor() -> ChangelogHistoryPointV3 {
    ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).unwrap(),
        [0x52; 32],
        DualFrontier::INITIAL,
    )
}

struct Snapshot {
    history: ChangelogHistoryStateV3,
    rows: Vec<AuthoritativeTransactionV3>,
}

fn snapshot(count: u64) -> Snapshot {
    let mut history =
        ChangelogHistoryStateV3::new(lineage(), anchor(), anchor(), anchor()).unwrap();
    let mut rows = Vec::new();
    for _ in 0..count {
        let tail = history.tail();
        let row = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: lineage().database_id(),
                history_incarnation: 1,
                predecessor: Some(tail.sequence()),
                sequence: tail.sequence().checked_next().unwrap(),
                predecessor_frontier: tail.frontier(),
                covered_frontier: tail.frontier(),
                prior_history_hash: tail.history_hash(),
            },
            ChangelogAttributionV3::CleanClose,
            vec![],
        )
        .unwrap();
        history = history.advance(&row).unwrap();
        rows.push(row);
    }
    Snapshot { history, rows }
}

struct Cursor(
    ChangelogHistoryStateV3,
    VecDeque<AuthoritativeTransactionV3>,
);

impl ChangelogReceiptCursorV3 for Cursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.0
    }

    fn next_receipt(
        &mut self,
    ) -> Result<Option<AuthoritativeTransactionV3>, ChangelogCursorErrorV3> {
        Ok(self.1.pop_front())
    }
}

impl PublishedDurableSnapshot for Snapshot {
    fn changelog_receipts_v3(
        &self,
        requested: ChangelogLineageV3,
        after: ChangelogHistoryPointV3,
    ) -> Result<Box<dyn ChangelogReceiptCursorV3>, ChangelogCursorErrorV3> {
        if requested.database_id() != self.history.lineage().database_id()
            || requested.history_incarnation() != self.history.lineage().history_incarnation()
        {
            return Err(ChangelogCursorErrorV3::ForeignLineage);
        }
        if requested.leadership_epoch() != self.history.lineage().leadership_epoch() {
            return Err(ChangelogCursorErrorV3::StaleEpoch);
        }
        if after.sequence() < self.history.minimum_resume().sequence() {
            return Err(StorageError::new(StorageErrorKind::HistoryPruned, None).into());
        }
        Ok(Box::new(Cursor(
            self.history,
            self.rows
                .iter()
                .filter(|row| row.binding().sequence > after.sequence())
                .cloned()
                .collect(),
        )))
    }

    fn read_value(&self, _: CompositeTableV1, _: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        panic!("replication must read receipts, not current values")
    }
    fn read_range(
        &self,
        _: CompositeTableV1,
        _: &[u8],
        _: &[u8],
        _: usize,
    ) -> Result<Vec<CompositeRow>, StorageError> {
        panic!("replication must read receipts, not current rows")
    }
    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        panic!("receipt cursor owns the exact frontier")
    }
    fn administration_frontier(&self) -> Result<Option<AdministrationSequence>, StorageError> {
        panic!("receipt cursor owns the exact frontier")
    }
}

fn handshake(after: ChangelogHistoryPointV3) -> ReplicationHandshakeV3 {
    ReplicationHandshakeV3::new(
        lineage(),
        after,
        ChangelogFrameV3::IDENTITY,
        AuthoritativeStateCatalogV1.digest(),
        MAX_CHANGELOG_FRAME_BYTES as u64,
        MAX_STAGED_COMMANDS as u64,
    )
    .unwrap()
}

#[test]
fn stream_refuses_catalog_substitution_at_open_and_on_later_pins() {
    let v1 = snapshot(3);
    let mut v2 = snapshot(3);
    let lineage = ChangelogLineageV3::new_with_catalog(
        lineage().database_id(),
        1,
        LeadershipEpochV1::initial(),
        AuthoritativeStateCatalogV2.digest(),
    )
    .unwrap();
    v2.history = ChangelogHistoryStateV3::new(
        lineage,
        v2.history.anchor(),
        v2.history.tail(),
        v2.history.minimum_resume(),
    )
    .unwrap();
    let handshake = ReplicationHandshakeV3::new(
        lineage,
        anchor(),
        ChangelogFrameV3::IDENTITY,
        lineage.catalog_digest(),
        MAX_CHANGELOG_FRAME_BYTES as u64,
        MAX_STAGED_COMMANDS as u64,
    )
    .unwrap();
    assert!(matches!(
        ChangelogFrameCursorV3::open(&v1, handshake),
        Err(ReplicationStreamErrorV3::UnsupportedCatalog)
    ));
    let mut stream = ChangelogFrameCursorV3::open(&v2, handshake).unwrap();
    let emitted = stream.next_frame().unwrap().unwrap();
    assert_eq!(
        ChangelogFrameV3::decode(emitted.as_bytes())
            .unwrap()
            .binding()
            .catalog_digest(),
        lineage.catalog_digest()
    );
    let before = stream.position();
    assert_eq!(
        stream.advance_snapshot(&v1),
        Err(ReplicationStreamErrorV3::UnsupportedCatalog)
    );
    assert_eq!(stream.position(), before);
    assert!(matches!(
        stream.next_frame(),
        Err(ReplicationStreamErrorV3::UnsupportedCatalog)
    ));
}

#[test]
// req: REP-004
fn published_head_is_independent_of_emission_and_unavailable_after_stream_failure() {
    let first = snapshot(3);
    let newer = snapshot(5);
    let mut stream = ChangelogFrameCursorV3::open(&first, handshake(anchor())).unwrap();
    assert_eq!(stream.published_head().unwrap(), first.history.tail());
    assert_eq!(stream.position(), anchor());
    let emitted = stream.next_frame().unwrap().unwrap().covered();
    assert_eq!(stream.published_head().unwrap(), first.history.tail());
    stream.advance_snapshot(&newer).unwrap();
    assert_eq!(stream.published_head().unwrap(), newer.history.tail());
    assert_eq!(stream.position(), emitted);

    let mut foreign = snapshot(6);
    foreign.history = ChangelogHistoryStateV3::new(
        ChangelogLineageV3::new(lineage().database_id(), 2, LeadershipEpochV1::initial()).unwrap(),
        foreign.history.anchor(),
        foreign.history.tail(),
        foreign.history.minimum_resume(),
    )
    .unwrap();
    assert_eq!(
        stream.advance_snapshot(&foreign),
        Err(ReplicationStreamErrorV3::ForeignLineage)
    );
    assert_eq!(
        stream.published_head(),
        Err(ReplicationStreamErrorV3::ForeignLineage)
    );
    assert_eq!(stream.position(), emitted);
}

#[test]
fn receipt_stream_resumes_at_the_exact_successor_across_newer_pins() {
    let first = snapshot(2);
    let latest = snapshot(5);
    let mut stream = ChangelogFrameCursorV3::open(&first, handshake(anchor())).unwrap();
    let frame = stream.next_frame().unwrap().unwrap();
    let decoded = ChangelogFrameV3::decode(frame.as_bytes()).unwrap();
    assert_eq!(
        decoded.binding().prior_frame_hash(),
        anchor().history_hash()
    );
    assert_eq!(decoded.receipts(), &first.rows[..1]);
    let acknowledged = frame.covered();
    let mut prior_checksum = *frame.as_bytes().last_chunk::<32>().unwrap();
    stream.advance_snapshot(&latest).unwrap();
    let mut emitted = vec![decoded.receipts()[0].clone()];
    while let Some(frame) = stream.next_frame().unwrap() {
        let decoded = ChangelogFrameV3::decode(frame.as_bytes()).unwrap();
        assert_eq!(decoded.binding().prior_frame_hash(), prior_checksum);
        prior_checksum = *frame.as_bytes().last_chunk::<32>().unwrap();
        emitted.extend_from_slice(decoded.receipts());
    }
    assert_eq!(emitted, latest.rows);
    assert_eq!(stream.position(), latest.history.tail());
    assert!(stream.next_frame().unwrap().is_none());

    let mut resumed = ChangelogFrameCursorV3::open(&latest, handshake(acknowledged)).unwrap();
    let frame = resumed.next_frame().unwrap().unwrap();
    assert_eq!(
        ChangelogFrameV3::decode(frame.as_bytes())
            .unwrap()
            .receipts(),
        &latest.rows[1..2]
    );
}

#[test]
fn stream_rejects_a_gap_and_never_recovers_after_a_refusal() {
    let mut source = snapshot(3);
    source.rows.remove(0);
    let mut stream = ChangelogFrameCursorV3::open(&source, handshake(anchor())).unwrap();
    assert_eq!(
        stream.next_frame().unwrap_err(),
        ReplicationStreamErrorV3::CorruptHistory
    );
    assert_eq!(stream.position(), anchor());
    assert_eq!(
        stream.advance_snapshot(&snapshot(3)),
        Err(ReplicationStreamErrorV3::CorruptHistory)
    );
}

#[test]
fn connected_stream_rechecks_lineage_epoch_and_pruning_on_every_pin() {
    for (incarnation, epoch, pruned, expected) in [
        (2, 1, false, ReplicationStreamErrorV3::ForeignLineage),
        (1, 2, false, ReplicationStreamErrorV3::StaleEpoch),
        (1, 1, true, ReplicationStreamErrorV3::HistoryPruned),
    ] {
        let original = snapshot(2);
        let mut stream = ChangelogFrameCursorV3::open(&original, handshake(anchor())).unwrap();
        let mut changed = snapshot(3);
        changed.history = ChangelogHistoryStateV3::new(
            ChangelogLineageV3::new(
                lineage().database_id(),
                incarnation,
                LeadershipEpochV1::new(epoch).unwrap(),
            )
            .unwrap(),
            changed.history.anchor(),
            changed.history.tail(),
            if pruned {
                ChangelogHistoryPointV3::from_receipt(&changed.rows[0]).unwrap()
            } else {
                anchor()
            },
        )
        .unwrap();
        assert_eq!(stream.advance_snapshot(&changed), Err(expected));
        assert_eq!(stream.next_frame().unwrap_err(), expected);
        assert_eq!(stream.position(), anchor());
    }
}

#[test]
fn truncated_receipt_source_fails_closed_without_advancing_emitted_position() {
    let mut truncated = snapshot(2);
    truncated.rows.clear();
    let mut stream = ChangelogFrameCursorV3::open(&truncated, handshake(anchor())).unwrap();
    assert_eq!(
        stream.next_frame().unwrap_err(),
        ReplicationStreamErrorV3::CorruptHistory
    );
    assert_eq!(stream.position(), anchor());
    assert_eq!(
        stream.next_frame().unwrap_err(),
        ReplicationStreamErrorV3::CorruptHistory
    );
}

#[test]
fn replication_negotiation_never_downgrades_format_catalog_or_bounds() {
    for (format, catalog, bytes, transitions, expected) in [
        (
            "riffdb.changelog-frame/v2",
            AuthoritativeStateCatalogV1.digest(),
            MAX_CHANGELOG_FRAME_BYTES as u64,
            MAX_STAGED_COMMANDS as u64,
            ReplicationStreamErrorV3::UnsupportedFormat,
        ),
        (
            ChangelogFrameV3::IDENTITY,
            [0; 32],
            MAX_CHANGELOG_FRAME_BYTES as u64,
            MAX_STAGED_COMMANDS as u64,
            ReplicationStreamErrorV3::UnsupportedCatalog,
        ),
        (
            ChangelogFrameV3::IDENTITY,
            AuthoritativeStateCatalogV1.digest(),
            1,
            MAX_STAGED_COMMANDS as u64,
            ReplicationStreamErrorV3::UnsupportedBounds,
        ),
    ] {
        assert_eq!(
            ReplicationHandshakeV3::new(lineage(), anchor(), format, catalog, bytes, transitions)
                .unwrap_err(),
            expected
        );
    }
}

#[path = "replication_stream_v3/coalesced.rs"]
mod coalesced;
