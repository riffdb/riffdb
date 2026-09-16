use super::*;
use riffdb_storage_api::{
    EntityTarget, IdempotencyIdentity, StorageError, StoredEntityRecordV1, StoredOutcomeV1,
    StoredProvenanceRecordV1,
};
use std::cell::Cell;

struct Reader {
    commit: StoredCommitRecordV1,
    provenance: StoredProvenanceRecordV1,
    commit_reads: Cell<usize>,
    provenance_reads: Cell<usize>,
    missing_provenance: Cell<bool>,
}

impl Reader {
    fn fixture() -> Self {
        let line =
            include_str!("../../../fixtures/proto/durable-command-prefix-v7-wire-vectors.txt")
                .lines()
                .find_map(|line| line.strip_prefix("riffdb.storage.v1.StoredCommandSegmentV6\t"))
                .unwrap();
        let bytes = (0..line.len())
            .step_by(2)
            .map(|offset| u8::from_str_radix(&line[offset..offset + 2], 16).unwrap())
            .collect::<Vec<_>>();
        let segment = riffdb_storage_api::decode_command_segment_v1(&bytes).unwrap();
        let capsule = segment.value().commands()[0].base();
        Self {
            commit: capsule.commit().clone(),
            provenance: capsule.provenance().clone(),
            commit_reads: Cell::new(0),
            provenance_reads: Cell::new(0),
            missing_provenance: Cell::new(false),
        }
    }
}

impl AuthoritativePointReader for Reader {
    fn read_entity(&self, _: &EntityTarget) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        Ok(None)
    }
    fn read_stored_outcome(
        &self,
        _: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        Ok(None)
    }
    fn read_durable_event(
        &self,
        _: EventId,
    ) -> Result<Option<riffdb_storage_api::StoredDurableEventV1>, StorageError> {
        Ok(None)
    }
    fn read_commit(&self, _: CommitSequence) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        self.commit_reads.set(self.commit_reads.get() + 1);
        Ok(Some(self.commit.clone()))
    }
    fn read_provenance(
        &self,
        _: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        self.provenance_reads.set(self.provenance_reads.get() + 1);
        Ok((!self.missing_provenance.get()).then(|| self.provenance.clone()))
    }
}

#[test]
fn sibling_replay_reuses_only_checked_commit_and_provenance_within_one_page() {
    let reader = Reader::fixture();
    let sequence = reader.commit.commit_sequence();
    let mut cache = ReplayCommitCache::default();
    for _ in 0..20 {
        let checked = cache.get(&reader, sequence).unwrap();
        assert_eq!(&checked.commit, &reader.commit);
    }
    assert_eq!(
        (reader.commit_reads.get(), reader.provenance_reads.get()),
        (1, 1)
    );
    let mut next_page = ReplayCommitCache::default();
    next_page.get(&reader, sequence).unwrap();
    assert_eq!(
        (reader.commit_reads.get(), reader.provenance_reads.get()),
        (2, 2)
    );
    assert_eq!(
        cache
            .get(&reader, sequence.checked_next().unwrap())
            .err()
            .unwrap()
            .kind(),
        EventReplayErrorKind::Integrity
    );
    assert!(cache.current.is_none());
    reader.missing_provenance.set(true);
    assert_eq!(
        cache.get(&reader, sequence).err().unwrap().kind(),
        EventReplayErrorKind::Integrity
    );
    assert!(cache.current.is_none());
    reader.missing_provenance.set(false);
    cache.get(&reader, sequence).unwrap();
    assert_eq!(
        (reader.commit_reads.get(), reader.provenance_reads.get()),
        (5, 4)
    );
}
