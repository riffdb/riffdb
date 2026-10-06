// req: REC-001, REC-005, REC-006, REC-007, END-008
use super::*;
use riffdb_storage_api::{
    ChangelogCursorErrorV3, ChangelogHistoryPointV3, ChangelogLineageV3,
    ChangelogTransactionSequence, CompositeRow, LeadershipEpochV1, StorageError, StorageErrorKind,
    StoredRecordRegistry, encode_record_registry_v3,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};
use std::collections::VecDeque;

fn history() -> ChangelogHistoryStateV3 {
    let id = DatabaseId::from_unix_milliseconds_and_random(1000, [7; 10]).unwrap();
    let lineage = ChangelogLineageV3::new(id, 2, LeadershipEpochV1::initial()).unwrap();
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).unwrap(),
        [1; 32],
        DualFrontier::INITIAL,
    );
    ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap()
}

struct Cursor(VecDeque<Step>);
impl AuthoritativeStateCursorV3 for Cursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        history()
    }
    fn next_item(&mut self) -> Result<Option<Step>, ChangelogCursorErrorV3> {
        Ok(self.0.pop_front())
    }
}
fn row(namespace: N, key: &[u8], value: &[u8]) -> Step {
    Step::Row(AuthoritativeStateRowV3::new(namespace, key, value).unwrap())
}
fn rows(application: &[u8], heads: &[(&[u8], &[u8])]) -> Cursor {
    Cursor(
        [
            vec![
                row(N::Entities, b"application", application),
                Step::EndNamespace(N::Entities),
            ],
            heads
                .iter()
                .map(|(key, value)| row(N::ValidatedPrefixEntityHeads, key, value))
                .collect(),
            vec![Step::EndNamespace(N::ValidatedPrefixEntityHeads)],
        ]
        .concat()
        .into(),
    )
}
fn renewal() -> Vec<AuthoritativeMutationV3> {
    vec![
        AuthoritativeMutationV3::delete_matching(N::ValidatedPrefixEntityHeads, b"a", b"old-a")
            .unwrap(),
        AuthoritativeMutationV3::replace(N::ValidatedPrefixEntityHeads, b"b", b"old-b", b"new-b")
            .unwrap(),
        AuthoritativeMutationV3::put(N::ValidatedPrefixEntityHeads, b"c", None, b"new-c").unwrap(),
    ]
}

#[test]
fn renewal_merges_exact_before_images_inserts_and_deletions_without_omitting_namespace_ends() {
    assert!(
        compare(
            &mut rows(b"unchanged", &[(b"a", b"old-a"), (b"b", b"old-b")]),
            &mut rows(b"unchanged", &[(b"b", b"new-b"), (b"c", b"new-c")]),
            &renewal(),
        )
        .unwrap()
    );
}

#[test]
fn renewed_certificate_cannot_hide_a_different_application_row_or_unconsumed_row() {
    assert!(
        !compare(
            &mut rows(b"expected", &[(b"a", b"old-a"), (b"b", b"old-b")]),
            &mut rows(b"substituted", &[(b"b", b"new-b"), (b"c", b"new-c")]),
            &renewal(),
        )
        .unwrap()
    );
    let mut actual = rows(b"expected", &[(b"b", b"new-b"), (b"c", b"new-c")]);
    actual.0.push_back(Step::EndNamespace(N::RecordRegistry));
    assert!(
        !compare(
            &mut rows(b"expected", &[(b"a", b"old-a"), (b"b", b"old-b")]),
            &mut actual,
            &renewal(),
        )
        .unwrap()
    );
}

#[test]
fn changed_prior_bytes_or_claimed_absence_refuse_even_when_successor_bytes_match() {
    assert!(
        !compare(
            &mut rows(b"unchanged", &[(b"a", b"substituted-a"), (b"b", b"old-b")]),
            &mut rows(b"unchanged", &[(b"b", b"new-b"), (b"c", b"new-c")]),
            &renewal(),
        )
        .unwrap()
    );
    assert!(
        !compare(
            &mut rows(
                b"unchanged",
                &[
                    (b"a", b"old-a"),
                    (b"b", b"old-b"),
                    (b"c", b"already-present")
                ]
            ),
            &mut rows(b"unchanged", &[(b"b", b"new-b"), (b"c", b"new-c")]),
            &renewal(),
        )
        .unwrap()
    );
}

struct Snapshot;
impl PublishedDurableSnapshot for Snapshot {
    fn read_value(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        assert_eq!(table, CompositeTableV1::Meta);
        assert_eq!(key, N::RecordRegistry.metadata_key().unwrap().as_bytes());
        let lineage = history().lineage();
        Ok(Some(
            encode_record_registry_v3(
                StoredRecordRegistry::completed(
                    current_record_registry_digest(),
                    lineage.database_id(),
                    lineage.history_incarnation(),
                )
                .unwrap(),
            )
            .unwrap()
            .as_bytes()
            .to_vec(),
        ))
    }
    fn read_range(
        &self,
        _: CompositeTableV1,
        _: &[u8],
        _: &[u8],
        _: usize,
    ) -> Result<Vec<CompositeRow>, StorageError> {
        Err(StorageError::new(
            StorageErrorKind::InvariantViolation,
            None,
        ))
    }
    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        Ok(None)
    }
    fn administration_frontier(&self) -> Result<Option<AdministrationSequence>, StorageError> {
        Ok(None)
    }
}

#[test]
fn valid_completion_witness_cannot_authorize_an_application_or_allocator_mutation() {
    for change in [
        AuthoritativeMutationV3::replace(N::Entities, b"application", b"old", b"new").unwrap(),
        AuthoritativeMutationV3::replace(
            N::NextApplicationSequence,
            N::NextApplicationSequence
                .metadata_key()
                .unwrap()
                .as_bytes(),
            b"old",
            b"new",
        )
        .unwrap(),
    ] {
        assert!(!valid_completion(&Snapshot, history(), &[change]).unwrap());
    }
}
