#![forbid(unsafe_code)]
//! Semantic prefix evidence; these tests do not claim durable restore coverage.
// req: REP-007

use riffdb_storage_api::{
    AuthoritativeMutationV3 as M, AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogTransactionSequence,
    ChangelogV3Error as E, CommandPrefixEvidenceV1 as Prefix, MAX_STAGED_WRITE_BYTES,
    validate_command_prefix_mutations_v1,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

fn frontier(app: u64, admin: u64) -> DualFrontier {
    DualFrontier::new(CommitSequence::new(app), AdministrationSequence::new(admin))
}

fn prefix(app: u64, mutations: Vec<M>) -> Prefix {
    Prefix::new(
        frontier(app - 1, (app - 1) * 2),
        frontier(app, app * 2),
        mutations,
    )
    .unwrap()
}

fn receipt(count: u64, mutations: Vec<M>) -> AuthoritativeTransactionV3 {
    // An opaque graph placeholder keeps this a mutation-level proof. Production
    // must additionally validate actual segment bytes and reciprocal command facts.
    let mut all = mutations;
    all.push(M::put(N::Commits, b"segment", None, b"graph-placeholder").unwrap());
    all.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).unwrap(),
            history_incarnation: 1,
            predecessor: None,
            sequence: ChangelogTransactionSequence::new(1).unwrap(),
            predecessor_frontier: DualFrontier::INITIAL,
            covered_frontier: frontier(count, count * 2),
            prior_history_hash: [0; 32],
        },
        ChangelogAttributionV3::JournaledApplicationGroup,
        all,
    )
    .unwrap()
}

#[test]
fn every_put_delete_recreate_prefix_retains_its_exact_intermediate_bytes() {
    let evidence = vec![
        prefix(
            1,
            vec![M::put(N::Entities, b"key", None, b"first").unwrap()],
        ),
        prefix(
            2,
            vec![M::replace(N::Entities, b"key", b"first", b"second").unwrap()],
        ),
        prefix(
            3,
            vec![M::delete_matching(N::Entities, b"key", b"second").unwrap()],
        ),
        prefix(
            4,
            vec![M::put(N::Entities, b"key", None, b"recreated").unwrap()],
        ),
    ];
    let original = receipt(
        4,
        vec![M::put(N::Entities, b"key", None, b"recreated").unwrap()],
    );
    validate_command_prefix_mutations_v1(&evidence, &original).unwrap();
    let expected = [
        Some(b"first".as_slice()),
        Some(b"second".as_slice()),
        None,
        Some(b"recreated".as_slice()),
    ];
    let mut state = None;
    for (step, expected) in evidence.iter().zip(expected) {
        for mutation in step.mutations() {
            assert!(mutation.matches_prior(state));
            state = mutation.value();
        }
        assert_eq!(state, expected);
    }
}

#[test]
fn cancelled_changes_remain_reconstructible_even_without_a_net_receipt_row() {
    let evidence = vec![
        prefix(
            1,
            vec![M::put(N::Entities, b"temporary", None, b"secret").unwrap()],
        ),
        prefix(
            2,
            vec![M::delete_matching(N::Entities, b"temporary", b"secret").unwrap()],
        ),
    ];
    validate_command_prefix_mutations_v1(&evidence, &receipt(2, vec![])).unwrap();
    assert_eq!(
        evidence[0].mutations()[0].value(),
        Some(b"secret".as_slice())
    );
    assert_eq!(evidence[1].mutations()[0].value(), None);
}

#[test]
fn missing_reordered_stale_and_contradictory_evidence_refuses() {
    let first = prefix(
        1,
        vec![M::put(N::Entities, b"key", None, b"first").unwrap()],
    );
    let second = prefix(
        2,
        vec![M::replace(N::Entities, b"key", b"first", b"last").unwrap()],
    );
    let original = receipt(2, vec![M::put(N::Entities, b"key", None, b"last").unwrap()]);
    for bad in [
        vec![],
        vec![first.clone()],
        vec![second.clone(), first.clone()],
        vec![first.clone(), first.clone()],
        vec![
            first.clone(),
            prefix(
                2,
                vec![M::replace(N::Entities, b"key", b"stale", b"last").unwrap()],
            ),
        ],
        vec![
            first.clone(),
            prefix(
                2,
                vec![M::replace(N::Entities, b"key", b"first", b"wrong").unwrap()],
            ),
        ],
    ] {
        assert!(validate_command_prefix_mutations_v1(&bad, &original).is_err());
    }
    let missing = receipt(2, vec![]);
    assert!(validate_command_prefix_mutations_v1(&[first, second], &missing).is_err());
}

#[test]
fn namespace_selection_cannot_hide_missing_independent_authority() {
    for namespace in [
        N::Entities,
        N::EntityChainHeads,
        N::SecondaryIndexes,
        N::IndexEpochs,
        N::IdempotencyPending,
        N::VectorEvidence,
        N::VectorObservations,
        N::VectorEvidenceIndex,
    ] {
        let mutation = M::put(namespace, b"key", None, b"value").unwrap();
        let original = receipt(1, vec![mutation.clone()]);
        assert!(validate_command_prefix_mutations_v1(&[prefix(1, vec![])], &original).is_err());
        validate_command_prefix_mutations_v1(&[prefix(1, vec![mutation])], &original).unwrap();
    }
}

#[test]
fn recursive_graph_control_and_unrelated_authority_cannot_enter_prefix_mutations() {
    for namespace in N::ALL {
        if matches!(
            namespace,
            N::Entities
                | N::EntityChainHeads
                | N::SecondaryIndexes
                | N::IndexEpochs
                | N::IdempotencyPending
                | N::VectorEvidence
                | N::VectorObservations
                | N::VectorEvidenceIndex
        ) {
            continue;
        }
        let key = namespace
            .metadata_key()
            .map_or(b"key".as_slice(), str::as_bytes);
        if let Ok(mutation) = M::put(namespace, key, None, b"secret") {
            assert_eq!(
                Prefix::new(frontier(0, 0), frontier(1, 2), vec![mutation]),
                Err(E::InvalidNamespace)
            );
        }
    }
}

#[test]
fn canonical_order_duplicates_noops_and_invalid_frontiers_refuse() {
    let a = M::put(N::Entities, b"a", None, b"a").unwrap();
    let b = M::put(N::Entities, b"b", None, b"b").unwrap();
    for mutations in [
        vec![b, a.clone()],
        vec![a.clone(), a],
        vec![M::replace(N::Entities, b"same", b"same", b"same").unwrap()],
    ] {
        assert_eq!(
            Prefix::new(frontier(0, 0), frontier(1, 2), mutations),
            Err(E::InvalidEncoding)
        );
    }
    for (prior, next) in [
        (frontier(0, 0), frontier(2, 2)),
        (frontier(1, 2), frontier(1, 4)),
        (frontier(1, 2), frontier(2, 1)),
        (frontier(1, 2), frontier(2, 2)),
        (frontier(0, 0), frontier(1, 3)),
        (frontier(u64::MAX, 2), frontier(1, 4)),
    ] {
        assert_eq!(
            Prefix::new(prior, next, vec![]),
            Err(E::PredecessorMismatch)
        );
    }
    Prefix::new(frontier(0, 1), frontier(1, 2), vec![]).unwrap();
    Prefix::new(
        frontier(u64::MAX - 1, u64::MAX - 1),
        frontier(u64::MAX, u64::MAX),
        vec![],
    )
    .unwrap();
}

#[test]
fn total_evidence_bytes_are_bounded_even_when_the_group_net_is_small() {
    let large = vec![0x55; MAX_STAGED_WRITE_BYTES / 2];
    let first = prefix(1, vec![M::put(N::Entities, b"key", None, &large).unwrap()]);
    let second = prefix(
        2,
        vec![M::replace(N::Entities, b"key", &large, &vec![0x66; large.len()]).unwrap()],
    );
    let third = prefix(
        3,
        vec![M::delete_matching(N::Entities, b"key", &vec![0x66; large.len()]).unwrap()],
    );
    assert_eq!(
        validate_command_prefix_mutations_v1(&[first, second, third], &receipt(3, vec![])),
        Err(E::LimitExceeded)
    );
    assert_eq!(
        Prefix::new(
            frontier(0, 0),
            frontier(1, 2),
            vec![M::put(N::Entities, b"key", None, &vec![0; MAX_STAGED_WRITE_BYTES]).unwrap()]
        ),
        Err(E::LimitExceeded)
    );
}

#[test]
fn evidence_debug_never_discloses_payload_or_keys() {
    let evidence = prefix(
        1,
        vec![M::put(N::Entities, b"private-key", None, b"private-value").unwrap()],
    );
    let debug = format!("{evidence:?}");
    assert!(!debug.contains("private"));
}

#[test]
fn every_five_command_state_history_replays_exactly_at_each_application_stop() {
    for initial in [None, Some(b"initial".as_slice())] {
        for program in 0..3_usize.pow(5) {
            let mut remaining = program;
            let mut state = initial;
            let mut expected = Vec::new();
            let mut evidence = Vec::new();
            for app in 1..=5 {
                let next = match remaining % 3 {
                    0 => None,
                    1 => Some(b"A".as_slice()),
                    _ => Some(b"B".as_slice()),
                };
                remaining /= 3;
                let mutation = match (state, next) {
                    (before, after) if before == after => None,
                    (None, Some(after)) => Some(M::put(N::Entities, b"key", None, after).unwrap()),
                    (Some(before), Some(after)) => {
                        Some(M::replace(N::Entities, b"key", before, after).unwrap())
                    }
                    (Some(before), None) => {
                        Some(M::delete_matching(N::Entities, b"key", before).unwrap())
                    }
                    (None, None) => unreachable!(),
                };
                evidence.push(prefix(app, mutation.into_iter().collect()));
                expected.push(next);
                state = next;
            }
            let net = match (initial, state) {
                (before, after) if before == after => vec![],
                (None, Some(after)) => vec![M::put(N::Entities, b"key", None, after).unwrap()],
                (Some(before), Some(after)) => {
                    vec![M::replace(N::Entities, b"key", before, after).unwrap()]
                }
                (Some(before), None) => {
                    vec![M::delete_matching(N::Entities, b"key", before).unwrap()]
                }
                (None, None) => unreachable!(),
            };
            validate_command_prefix_mutations_v1(&evidence, &receipt(5, net)).unwrap();
            state = initial;
            for (command, expected) in evidence.iter().zip(expected) {
                for mutation in command.mutations() {
                    assert!(mutation.matches_prior(state));
                    state = mutation.value();
                }
                assert_eq!(state, expected, "program {program}");
            }
        }
    }
}
