#![forbid(unsafe_code)]
//! ADR-0186 Amendment 1: explicit unchanged-frontier command lifecycle sources.
// req: REP-003, REC-001, STO-012

use riffdb_storage_api::{
    AuthoritativeMutationV3 as Mutation, AuthoritativeNamespaceV1 as N,
    AuthoritativeTransactionBindingV3 as Binding, AuthoritativeTransactionV3 as Receipt,
    ChangelogAttributionV3 as Source, ChangelogTransactionSequence,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

fn binding() -> Binding {
    Binding {
        database_id: DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap(),
        history_incarnation: 1,
        predecessor: None,
        sequence: ChangelogTransactionSequence::new(1).unwrap(),
        predecessor_frontier: DualFrontier::INITIAL,
        covered_frontier: DualFrontier::INITIAL,
        prior_history_hash: [0; 32],
    }
}

#[test]
fn command_admission_and_execution_failure_have_closed_frontier_checked_sources() {
    assert_eq!(Source::from_tag(32), Some(Source::CommandAdmission));
    assert_eq!(Source::from_tag(33), Some(Source::CommandExecutionFailure));
    assert_eq!(Source::from_tag(34), None);
    assert_eq!(Source::from_tag(0), None);
    assert_eq!(Source::ALL.len(), 33);
    for (index, source) in Source::ALL.into_iter().enumerate() {
        assert_eq!(source as usize, index + 1);
    }
    for source in [Source::CommandAdmission, Source::CommandExecutionFailure] {
        let mutation = Mutation::put(
            if source == Source::CommandAdmission {
                N::IdempotencyPending
            } else {
                N::Idempotency
            },
            b"private-command-key",
            None,
            b"private-command-state",
        )
        .unwrap();
        let receipt = Receipt::new(binding(), source, vec![mutation.clone()]).unwrap();
        assert_eq!(
            Receipt::decode(&receipt.encode().unwrap()).unwrap(),
            receipt
        );
        assert_eq!(
            receipt.binding().predecessor_frontier,
            receipt.binding().covered_frontier
        );
        assert!(!format!("{receipt:?}").contains("private-command"));
        assert!(Receipt::new(binding(), source, vec![]).is_err());
        let advanced_application = Binding {
            covered_frontier: DualFrontier::new(CommitSequence::new(1), None),
            ..binding()
        };
        assert!(Receipt::new(advanced_application, source, vec![mutation.clone()]).is_err());
        for administration in [1, 256, 257] {
            let audited = Binding {
                covered_frontier: DualFrontier::new(
                    None,
                    AdministrationSequence::new(administration),
                ),
                ..binding()
            };
            let observed = Receipt::new(audited, source, vec![mutation.clone()]);
            assert_eq!(
                observed.is_ok(),
                source == Source::CommandExecutionFailure && administration <= 256
            );
            if let Ok(receipt) = observed {
                assert_eq!(
                    Receipt::decode(&receipt.encode().unwrap()).unwrap(),
                    receipt
                );
            }
        }
        // The amendment must not relax the existing advancing-group attribution.
        assert!(
            Receipt::new(
                binding(),
                Source::DirectApplicationOrServiceAuditGroup,
                vec![mutation]
            )
            .is_err()
        );
    }
}
