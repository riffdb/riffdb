//! Stable-ID rename ledger contract tests.

use riffdb_contract_ir::{
    LineageLedgerV1, StableIdNamespace, StableIdNamespaceTag, StableIdentity, StableIdentityRename,
};

fn entity(name: &str) -> StableIdentity {
    StableIdentity::new(
        StableIdNamespace::new(StableIdNamespaceTag::Entity, 0, vec![]).expect("entity namespace"),
        name,
    )
    .expect("entity identity")
}

#[test]
fn rename_retains_numeric_id_and_permanently_reserves_old_identity() {
    let old = entity("Budget");
    let new = entity("SpendingBudget");
    let parent = LineageLedgerV1::genesis(vec![old.clone()]).expect("parent ledger");
    let renamed = LineageLedgerV1::successor_with_renames(
        &parent,
        vec![new.clone()],
        vec![StableIdentityRename::new(old.clone(), new.clone()).expect("rename")],
    )
    .expect("renamed ledger");

    assert_eq!(parent.version(), 1);
    assert_eq!(renamed.version(), 2);
    assert_eq!(parent.active_id(&old), Some(1));
    assert_eq!(renamed.active_id(&new), Some(1));
    assert_eq!(renamed.active_id(&old), None);
    assert_eq!(renamed.aliases().len(), 1);
    assert_eq!(renamed.aliases()[0].identity(), &old);
    assert_eq!(renamed.aliases()[0].id(), 1);
    assert!(LineageLedgerV1::successor(&renamed, vec![old]).is_err());
}

#[test]
fn chained_rename_retains_every_historical_alias() {
    let first = entity("Budget");
    let second = entity("SpendingBudget");
    let third = entity("ApprovedSpend");
    let genesis = LineageLedgerV1::genesis(vec![first.clone()]).expect("genesis");
    let v2 = LineageLedgerV1::successor_with_renames(
        &genesis,
        vec![second.clone()],
        vec![StableIdentityRename::new(first.clone(), second.clone()).expect("first rename")],
    )
    .expect("v2");
    let v3 = LineageLedgerV1::successor_with_renames(
        &v2,
        vec![third.clone()],
        vec![StableIdentityRename::new(second, third.clone()).expect("second rename")],
    )
    .expect("v3");

    assert_eq!(v3.active_id(&third), Some(1));
    assert_eq!(v3.aliases().len(), 2);
    assert_eq!(v3.aliases()[0].id(), 1);
    assert_eq!(v3.aliases()[1].id(), 1);
}

#[test]
fn rename_rejects_rebinding_and_non_active_sources() {
    let first = entity("Budget");
    let second = entity("SpendingBudget");
    let unrelated = entity("Other");
    let parent =
        LineageLedgerV1::genesis(vec![first.clone(), unrelated.clone()]).expect("parent ledger");

    assert!(
        LineageLedgerV1::successor_with_renames(
            &parent,
            vec![second.clone(), unrelated.clone()],
            vec![
                StableIdentityRename::new(first.clone(), second.clone()).expect("first rename"),
                StableIdentityRename::new(unrelated, second).expect("colliding rename"),
            ],
        )
        .is_err()
    );

    let removed = LineageLedgerV1::successor(&parent, vec![]).expect("removed ledger");
    assert!(
        LineageLedgerV1::successor_with_renames(
            &removed,
            vec![entity("Replacement")],
            vec![StableIdentityRename::new(first, entity("Replacement")).expect("rename")],
        )
        .is_err()
    );
}
