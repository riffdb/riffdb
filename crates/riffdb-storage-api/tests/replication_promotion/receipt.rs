// req: REP-005
use super::*;
use riffdb_storage_api::{
    MAX_REPLICATION_PROMOTION_STEPS_V1, ReplicationPromotionFailureV1 as Failure,
    ReplicationPromotionPhaseV1 as Phase, ReplicationPromotionReceiptV1 as Receipt,
    ReplicationPromotionStepV1 as Step,
};
use riffdb_types::ApprovalId;

fn attempted() -> Receipt {
    let (request, _, _) = fixture(2, 7, 11, 3);
    Receipt::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("promotion-operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1234, [8; 10]).unwrap(),
            NonZeroU64::new(3).unwrap(),
        ),
        Some(ApprovalId::new("promotion-approval").unwrap()),
        Timestamp::new(1234, 5).unwrap(),
    )
}
fn selected() -> Selection {
    let (request, follower, evidence) = fixture(2, 7, 11, 3);
    Selection::new(request, follower, evidence).unwrap()
}
fn offline() -> Receipt {
    let mut r = attempted();
    r.advance(Step::Phase(Phase::Draining)).unwrap();
    r.advance(Step::Phase(Phase::Offline)).unwrap();
    r
}
fn copy_parts(
    r: &Receipt,
    selection: Option<Selection>,
    steps: Vec<Step>,
) -> Result<Receipt, riffdb_storage_api::StorageValueError> {
    Receipt::from_canonical_parts(
        r.request(),
        r.request_id(),
        r.principal().clone(),
        r.approval_id().cloned(),
        r.timestamp(),
        selection,
        steps,
    )
}

#[test]
fn promotion_receipt_requires_frozen_offline_selection_before_cutover() {
    let mut r = attempted();
    let original = r.clone();
    assert!(r.record_selection(selected()).is_err());
    assert!(r.advance(Step::Phase(Phase::Offline)).is_err());
    assert_eq!(r, original);
    r.advance(Step::Phase(Phase::Draining)).unwrap();
    r.advance(Step::Phase(Phase::Offline)).unwrap();
    let offline = r.clone();
    assert!(r.advance(Step::Phase(Phase::Selected)).is_err());
    assert!(r.advance(Step::Phase(Phase::CutoverPending)).is_err());
    assert_eq!(r, offline);
    r.record_selection(selected()).unwrap();
    let frozen = r.clone();
    r.record_selection(selected()).unwrap();
    assert_eq!(r, frozen);
    let (request, follower, evidence) = fixture(2, 7, 11, 4);
    assert!(
        r.record_selection(Selection::new(request, follower, evidence).unwrap())
            .is_err()
    );
    assert_eq!(r, frozen);
    for phase in [
        Phase::CutoverPending,
        Phase::CutoverCommitted,
        Phase::Validated,
        Phase::Succeeded,
    ] {
        let before = r.clone();
        r.advance(Step::Phase(phase)).unwrap();
        assert!(r.monotonically_extends(&before));
    }
    assert!(r.is_terminal());
    assert_eq!(r.selection(), Some(&selected()));
    let terminal = r.clone();
    assert!(
        r.advance(Step::Uncertain(Failure::StorageUnavailable))
            .is_err()
    );
    assert_eq!(r, terminal);
    assert!(r.monotonically_extends(&r));
}

#[test]
fn promotion_authenticated_denial_and_failure_are_terminal_without_cutover() {
    for end in [
        Step::Denied(Failure::AuthorizationDenied),
        Step::FailedClosed(Failure::FenceUnavailable),
    ] {
        let mut r = attempted();
        let started = r.clone();
        r.advance(end).unwrap();
        assert!(r.is_terminal());
        assert!(r.monotonically_extends(&started));
        assert!(r.selection().is_none());
        let terminal = r.clone();
        assert!(r.advance(Step::Phase(Phase::Draining)).is_err());
        assert!(r.record_selection(selected()).is_err());
        assert_eq!(r, terminal);
        assert_eq!(copy_parts(&r, None, r.steps().to_vec()).unwrap(), r);
    }
}

#[test]
fn promotion_uncertainty_preserves_progress_and_cannot_reselect_or_skip_validation() {
    let mut r = offline();
    assert!(
        r.advance(Step::Uncertain(Failure::StorageUnavailable))
            .is_err()
    );
    r.record_selection(selected()).unwrap();
    r.advance(Step::Phase(Phase::CutoverPending)).unwrap();
    r.advance(Step::Uncertain(Failure::StorageUnavailable))
        .unwrap();
    assert!(!r.is_terminal());
    assert_eq!(r.phase(), Phase::CutoverPending);
    let uncertain = r.clone();
    r.advance(Step::Uncertain(Failure::StorageUnavailable))
        .unwrap();
    assert_eq!(r, uncertain, "exact receipt durability retry is idempotent");
    assert!(
        r.advance(Step::FailedClosed(Failure::StorageUnavailable))
            .is_err()
    );
    assert!(
        r.advance(Step::Denied(Failure::AuthorizationDenied))
            .is_err()
    );
    assert!(r.advance(Step::Phase(Phase::Succeeded)).is_err());
    r.advance(Step::Phase(Phase::CutoverCommitted)).unwrap();
    r.advance(Step::Uncertain(Failure::ValidationFailed))
        .unwrap();
    r.advance(Step::Phase(Phase::Validated)).unwrap();
    r.advance(Step::Phase(Phase::Succeeded)).unwrap();
    assert!(r.monotonically_extends(&uncertain));
    assert!(
        r.steps()
            .contains(&Step::Uncertain(Failure::StorageUnavailable))
    );
    assert!(
        r.steps()
            .contains(&Step::Uncertain(Failure::ValidationFailed))
    );
}

#[test]
fn promotion_decoded_receipt_refuses_missing_reordered_or_overbound_evidence() {
    let r = offline();
    for steps in [
        vec![],
        vec![Step::Phase(Phase::Offline)],
        vec![Step::Phase(Phase::Attempted), Step::Phase(Phase::Offline)],
        vec![Step::Phase(Phase::Attempted); MAX_REPLICATION_PROMOTION_STEPS_V1 + 1],
        vec![
            Step::Phase(Phase::Attempted),
            Step::Denied(Failure::AuthorizationDenied),
            Step::Phase(Phase::Draining),
        ],
    ] {
        assert!(copy_parts(&r, None, steps).is_err());
    }
    assert!(copy_parts(&r, Some(selected()), r.steps().to_vec()).is_err());
    let mut with_selection = r;
    with_selection.record_selection(selected()).unwrap();
    assert!(copy_parts(&with_selection, None, with_selection.steps().to_vec()).is_err());
    let (request, follower, evidence) = fixture(3, 7, 11, 3);
    assert!(
        copy_parts(
            &with_selection,
            Some(Selection::new(request, follower, evidence).unwrap()),
            with_selection.steps().to_vec()
        )
        .is_err()
    );
    assert_eq!(
        format!("{with_selection:?}"),
        "ReplicationPromotionReceiptV1([redacted])"
    );
}

#[test]
fn promotion_receipt_cannot_rewrite_identity_authority_or_prior_audit_steps() {
    let r = attempted();
    let denied = copy_parts(
        &r,
        None,
        vec![
            Step::Phase(Phase::Attempted),
            Step::Denied(Failure::AuthorizationDenied),
        ],
    )
    .unwrap();
    let failed = copy_parts(
        &r,
        None,
        vec![
            Step::Phase(Phase::Attempted),
            Step::FailedClosed(Failure::FenceUnavailable),
        ],
    )
    .unwrap();
    assert!(!failed.monotonically_extends(&denied));
    assert!(!r.monotonically_extends(&denied));
    let other_request = Request::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [9; 10]).unwrap(),
        r.request().fence_operation_id(),
        r.request().target(),
        r.request().generation(),
    );
    let other_id = RequestId::from_unix_milliseconds_and_random(1234, [9; 10]).unwrap();
    let other_principal = AuditPrincipalV1::new(
        r.principal().principal_id().clone(),
        r.principal().actor_kind(),
        r.principal().capability_id(),
        NonZeroU64::new(4).unwrap(),
    );
    for changed in [
        Receipt::attempted(
            other_request,
            r.request_id(),
            r.principal().clone(),
            r.approval_id().cloned(),
            r.timestamp(),
        ),
        Receipt::attempted(
            r.request(),
            other_id,
            r.principal().clone(),
            r.approval_id().cloned(),
            r.timestamp(),
        ),
        Receipt::attempted(
            r.request(),
            r.request_id(),
            other_principal,
            r.approval_id().cloned(),
            r.timestamp(),
        ),
        Receipt::attempted(
            r.request(),
            r.request_id(),
            r.principal().clone(),
            None,
            r.timestamp(),
        ),
        Receipt::attempted(
            r.request(),
            r.request_id(),
            r.principal().clone(),
            r.approval_id().cloned(),
            Timestamp::new(1235, 5).unwrap(),
        ),
    ] {
        assert!(!changed.monotonically_extends(&r));
    }
}

#[test]
fn promotion_inventory_freezes_selection_across_separately_authenticated_attempts() {
    use riffdb_storage_api::ReplicationPromotionReceiptInventoryV1 as Inventory;
    let mut first = offline();
    first.record_selection(selected()).unwrap();
    first
        .advance(Step::FailedClosed(Failure::StorageUnavailable))
        .unwrap();
    let id = RequestId::from_unix_milliseconds_and_random(1235, [7; 10]).unwrap();
    let mut retry = Receipt::attempted(
        first.request(),
        id,
        first.principal().clone(),
        first.approval_id().cloned(),
        first.timestamp(),
    );
    retry.advance(Step::Phase(Phase::Draining)).unwrap();
    retry.advance(Step::Phase(Phase::Offline)).unwrap();
    let (request, follower, evidence) = fixture(2, 7, 11, 4);
    retry
        .record_selection(Selection::new(request, follower, evidence).unwrap())
        .unwrap();
    assert!(
        Inventory::new(vec![first.clone(), retry]).is_err(),
        "new invocation cannot choose a newer applied frontier"
    );
    let retry = Receipt::from_canonical_parts(
        first.request(),
        id,
        first.principal().clone(),
        first.approval_id().cloned(),
        first.timestamp(),
        first.selection().cloned(),
        first.steps().to_vec(),
    )
    .unwrap();
    let inventory = Inventory::new(vec![first.clone(), retry]).unwrap();
    assert_eq!(
        inventory.selection_for(first.request().operation_id()),
        first.selection()
    );
    assert_eq!(
        inventory.request_for(first.request().operation_id()),
        Some(first.request())
    );
    assert_eq!(inventory.receipts().len(), 2);
    assert_eq!(
        format!("{inventory:?}"),
        "ReplicationPromotionReceiptInventoryV1([redacted])"
    );
}

#[test]
fn promotion_inventory_audits_conflicting_request_without_replacing_operation_choice() {
    use riffdb_storage_api::ReplicationPromotionReceiptInventoryV1 as Inventory;
    let first = attempted();
    let request = Request::new(
        first.request().operation_id(),
        first.request().fence_operation_id(),
        first.request().target(),
        Sequence::new(5).unwrap(),
    );
    let id = RequestId::from_unix_milliseconds_and_random(1235, [7; 10]).unwrap();
    let mut conflict = Receipt::attempted(
        request,
        id,
        first.principal().clone(),
        None,
        first.timestamp(),
    );
    assert!(Inventory::new(vec![first.clone(), conflict.clone()]).is_err());
    conflict
        .advance(Step::Denied(Failure::SelectionConflict))
        .unwrap();
    assert!(
        Inventory::new(vec![conflict.clone()]).is_err(),
        "conflict must reference an existing operation choice"
    );
    let inventory = Inventory::new(vec![first.clone(), conflict]).unwrap();
    assert_eq!(
        inventory.request_for(first.request().operation_id()),
        Some(first.request())
    );
    assert_eq!(
        inventory.selection_for(first.request().operation_id()),
        None
    );
    let mut selected = offline();
    selected
        .record_selection(super::receipt::selected())
        .unwrap();
    assert!(
        selected
            .advance(Step::Denied(Failure::SelectionConflict))
            .is_err(),
        "cannot disguise selected evidence as an excluded conflicting request"
    );
}

#[test]
fn promotion_inventory_requires_unique_canonical_invocations_and_a_bounded_count() {
    use riffdb_storage_api::{
        MAX_REPLICATION_PROMOTION_RECEIPTS_V1, ReplicationPromotionReceiptInventoryV1 as Inventory,
    };
    let first = attempted();
    let later = Receipt::attempted(
        first.request(),
        RequestId::from_unix_milliseconds_and_random(1235, [7; 10]).unwrap(),
        first.principal().clone(),
        None,
        first.timestamp(),
    );
    assert!(Inventory::new(vec![first.clone(), first.clone()]).is_err());
    assert!(Inventory::new(vec![later, first.clone()]).is_err());
    assert!(Inventory::new(vec![first; MAX_REPLICATION_PROMOTION_RECEIPTS_V1 + 1]).is_err());
    assert!(Inventory::new(vec![]).unwrap().receipts().is_empty());
}

#[test]
fn promotion_initial_authorization_denial_does_not_claim_an_operation() {
    use riffdb_storage_api::ReplicationPromotionReceiptInventoryV1 as Inventory;
    let first = attempted();
    let request = Request::new(
        first.request().operation_id(),
        first.request().fence_operation_id(),
        first.request().target(),
        Sequence::new(5).unwrap(),
    );
    let mut denied = Receipt::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1235, [7; 10]).unwrap(),
        first.principal().clone(),
        None,
        first.timestamp(),
    );
    denied
        .advance(Step::Denied(Failure::AuthorizationDenied))
        .unwrap();
    let only_denial = Inventory::new(vec![denied.clone()]).unwrap();
    assert_eq!(only_denial.request_for(request.operation_id()), None);
    let inventory = Inventory::new(vec![first.clone(), denied]).unwrap();
    assert_eq!(
        inventory.request_for(request.operation_id()),
        Some(first.request())
    );
}

#[test]
fn promotion_uncertainty_history_exhaustion_preserves_the_last_exact_receipt() {
    let mut r = offline();
    r.record_selection(selected()).unwrap();
    r.advance(Step::Phase(Phase::CutoverPending)).unwrap();
    while r.steps().len() < MAX_REPLICATION_PROMOTION_STEPS_V1 {
        let failure = if r.steps().last() == Some(&Step::Uncertain(Failure::StorageUnavailable)) {
            Failure::ValidationFailed
        } else {
            Failure::StorageUnavailable
        };
        r.advance(Step::Uncertain(failure)).unwrap();
    }
    let full = r.clone();
    assert!(r.advance(Step::Phase(Phase::CutoverCommitted)).is_err());
    assert_eq!(r, full);
    assert_eq!(r.phase(), Phase::CutoverPending);
    assert_eq!(r.selection(), Some(&selected()));
    r.advance(*r.steps().last().unwrap()).unwrap();
    assert_eq!(r, full);
}
