// req: REP-001, REP-002, REP-003
use super::*;
use riffdb_storage_api::*;
use riffdb_types::{DatabaseId, DualFrontier};
use std::collections::VecDeque;

struct Input {
    history: ChangelogHistoryStateV3,
    steps: VecDeque<AuthoritativeStateStepV3>,
}
impl AuthoritativeStateCursorV3 for Input {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }
    fn next_item(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
        Ok(self.steps.pop_front())
    }
}
fn input() -> Input {
    let lineage = ChangelogLineageV3::new(
        DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap(),
        1,
        LeadershipEpochV1::new(1).unwrap(),
    )
    .unwrap();
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).unwrap(),
        [1; 32],
        DualFrontier::new(None, None),
    );
    let steps = AuthoritativeNamespaceV1::ALL
        .into_iter()
        .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
        .map(AuthoritativeStateStepV3::EndNamespace)
        .collect();
    Input {
        history: ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap(),
        steps,
    }
}
fn insert(source: &mut Input, key: &[u8], value: &[u8]) {
    let index = source
        .steps
        .iter()
        .position(|step| {
            matches!(
                step,
                AuthoritativeStateStepV3::EndNamespace(AuthoritativeNamespaceV1::Entities)
            )
        })
        .unwrap();
    source.steps.insert(
        index,
        AuthoritativeStateStepV3::Row(
            AuthoritativeStateRowV3::new(AuthoritativeNamespaceV1::Entities, key, value).unwrap(),
        ),
    );
}
fn receipt(
    model: &AuthoritativeNamespaceModel,
    mutations: Vec<AuthoritativeMutationV3>,
) -> AuthoritativeTransactionV3 {
    let point = model.position();
    AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: model.lineage().database_id(),
            history_incarnation: model.lineage().history_incarnation(),
            predecessor: Some(point.sequence()),
            sequence: point.sequence().checked_next().unwrap(),
            predecessor_frontier: point.frontier(),
            covered_frontier: point.frontier(),
            prior_history_hash: point.history_hash(),
        },
        ChangelogAttributionV3::CommandAdmission,
        mutations,
    )
    .unwrap()
}

#[test]
fn namespace_model_requires_all_empty_namespace_ends_and_strict_key_order() {
    let mut missing = input();
    missing.steps.pop_back();
    assert_eq!(
        AuthoritativeNamespaceModel::capture(&mut missing).unwrap_err(),
        NamespaceModelError::Inventory
    );
    let mut unordered = input();
    insert(&mut unordered, b"b", b"two");
    insert(&mut unordered, b"a", b"one");
    assert_eq!(
        AuthoritativeNamespaceModel::capture(&mut unordered).unwrap_err(),
        NamespaceModelError::Order
    );
    let mut duplicate = input();
    duplicate
        .steps
        .push_back(AuthoritativeStateStepV3::EndNamespace(
            AuthoritativeNamespaceV1::ContractBundles,
        ));
    assert_eq!(
        AuthoritativeNamespaceModel::capture(&mut duplicate).unwrap_err(),
        NamespaceModelError::Inventory
    );
}

#[test]
fn namespace_model_checks_every_prior_before_atomic_apply_and_rejects_duplicates() {
    let mut source = input();
    insert(&mut source, b"a", b"one");
    insert(&mut source, b"b", b"two");
    let mut model = AuthoritativeNamespaceModel::capture(&mut source).unwrap();
    let original = model.clone();
    let wrong = receipt(
        &model,
        vec![
            AuthoritativeMutationV3::replace(
                AuthoritativeNamespaceV1::Entities,
                b"a",
                b"one",
                b"changed",
            )
            .unwrap(),
            AuthoritativeMutationV3::replace(
                AuthoritativeNamespaceV1::Entities,
                b"b",
                b"wrong",
                b"changed",
            )
            .unwrap(),
        ],
    );
    assert_eq!(model.apply(&wrong), Err(NamespaceModelError::PriorState));
    assert_eq!(
        model, original,
        "late mismatch must not expose the earlier mutation"
    );
    let valid = receipt(
        &model,
        vec![
            AuthoritativeMutationV3::delete_matching(
                AuthoritativeNamespaceV1::Entities,
                b"a",
                b"one",
            )
            .unwrap(),
            AuthoritativeMutationV3::replace(
                AuthoritativeNamespaceV1::Entities,
                b"b",
                b"two",
                b"changed",
            )
            .unwrap(),
        ],
    );
    model.apply(&valid).unwrap();
    assert_eq!(model.apply(&valid), Err(NamespaceModelError::Position));
    let mut expected = input();
    expected.history = ChangelogHistoryStateV3::new(
        model.lineage(),
        original.position(),
        model.position(),
        original.position(),
    )
    .unwrap();
    insert(&mut expected, b"b", b"changed");
    model.verify(&mut expected).unwrap();
    let mut extra = input();
    extra.history = expected.history;
    insert(&mut extra, b"b", b"changed");
    insert(&mut extra, b"c", b"unexpected");
    assert_eq!(model.verify(&mut extra), Err(NamespaceModelError::Bytes));
}

#[test]
fn namespace_model_budget_failure_does_not_publish_partial_state() {
    let mut source = input();
    insert(&mut source, b"a", b"one");
    let mut model = AuthoritativeNamespaceModel::capture_bounded(&mut source, 1, 4).unwrap();
    let before = model.clone();
    let too_large = receipt(
        &model,
        vec![
            AuthoritativeMutationV3::replace(
                AuthoritativeNamespaceV1::Entities,
                b"a",
                b"one",
                b"oversized",
            )
            .unwrap(),
        ],
    );
    assert_eq!(model.apply(&too_large), Err(NamespaceModelError::Limit));
    assert_eq!(model, before);
    let extra = receipt(
        &model,
        vec![
            AuthoritativeMutationV3::put(AuthoritativeNamespaceV1::Entities, b"b", None, b"")
                .unwrap(),
        ],
    );
    assert_eq!(model.apply(&extra), Err(NamespaceModelError::Limit));
    assert_eq!(model, before);
}

#[test]
fn namespace_model_rejects_foreign_lineage_hash_substitution_and_gaps() {
    let mut model = AuthoritativeNamespaceModel::capture(&mut input()).unwrap();
    let original = model.clone();
    let valid = receipt(
        &model,
        vec![
            AuthoritativeMutationV3::put(AuthoritativeNamespaceV1::Entities, b"a", None, b"one")
                .unwrap(),
        ],
    );
    for choice in 0..3 {
        let mut binding = valid.binding();
        match choice {
            0 => binding.history_incarnation += 1,
            1 => binding.prior_history_hash = [9; 32],
            _ => {
                binding.predecessor = Some(binding.sequence);
                binding.sequence = binding.sequence.checked_next().unwrap();
            }
        }
        let invalid = AuthoritativeTransactionV3::new(
            binding,
            valid.attribution(),
            valid.mutations().to_vec(),
        )
        .unwrap();
        assert_eq!(model.apply(&invalid), Err(NamespaceModelError::Position));
        assert_eq!(model, original);
    }
    model.apply(&valid).unwrap();
    let mut altered = input();
    altered.history = ChangelogHistoryStateV3::new(
        model.lineage(),
        original.position(),
        model.position(),
        original.position(),
    )
    .unwrap();
    insert(&mut altered, b"a", b"different");
    assert_eq!(model.verify(&mut altered), Err(NamespaceModelError::Bytes));
}
