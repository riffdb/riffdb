//! Closed primary-fence physical transaction; runtime integration remains gated.

use crate::error::{codec_error, storage_error};
use riffdb_storage_api::{
    AdministrationSequenceAllocator, AuditPrincipalV1, AuthoritativeMutationV3,
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV2, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogHistoryStateV3,
    FollowerRegistrationPhaseV1, PrimaryFenceRequestV1, ReplicationPrimaryAdmissionV1,
    ReplicationSourceHoldV2, StorageError, StorageErrorKind, StoredPrimaryFenceAdministrationV1,
    proto_codec::{
        encode_administration_sequence_allocator_v1, encode_primary_fence_administration_v1,
    },
};
use riffdb_types::{DualFrontier, Timestamp};

/// A checked recipe, not authorization, source proof, or a writer permit. The
/// coordinator must freshly authorize under its exclusive drained barrier. This
/// owner never accepts an arbitrary mutation plan and never grants an unfence.
pub(crate) struct PrimaryFencePlan {
    predecessor: ChangelogHistoryStateV3,
    before_admission: ReplicationPrimaryAdmissionV1,
    registration: ReplicationSourceHoldV2,
    before_administration: AdministrationSequenceAllocator,
    record: StoredPrimaryFenceAdministrationV1,
    admission: ReplicationPrimaryAdmissionV1,
    receipt: AuthoritativeTransactionV3,
    successor: ChangelogHistoryStateV3,
    next_administration: AdministrationSequenceAllocator,
}
impl std::fmt::Debug for PrimaryFencePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrimaryFencePlan([REDACTED])")
    }
}
impl PrimaryFencePlan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        history: ChangelogHistoryStateV3,
        admission: ReplicationPrimaryAdmissionV1,
        registration: ReplicationSourceHoldV2,
        request: PrimaryFenceRequestV1,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
        allocator: AdministrationSequenceAllocator,
    ) -> Result<Self, StorageError> {
        let lineage = history.lineage();
        let target = request.target();
        if lineage.catalog_digest() != AuthoritativeStateCatalogV2.digest()
            || admission.lineage() != lineage
            || admission.fence().is_some()
            || registration.hold().lineage() != lineage
            || target.database_id() != lineage.database_id()
            || target.history_incarnation() != lineage.history_incarnation()
            || target.leadership_epoch() != lineage.leadership_epoch()
            || target.hold_id() != registration.hold().id()
            || request.generation() != registration.generation()
            || registration.phase() == FollowerRegistrationPhaseV1::Retired
            || !registration
                .registered_at()
                .precedes_or_equals(history.tail())
            || !registration
                .hold()
                .fence()
                .precedes_or_equals(history.tail())
            || registration.generation() > history.tail().sequence()
            || registration
                .degraded_at()
                .is_some_and(|p| !p.precedes_or_equals(history.tail()))
        {
            return Err(corrupt());
        }
        let allocated = allocator.allocate_one().map_err(|_| corrupt())?;
        // The record constructor checks exact administration succession, physical
        // headroom, and the unchanged application head. It cannot repair a root.
        let record = StoredPrimaryFenceAdministrationV1::new(
            allocated.assigned(),
            timestamp,
            request.operation_id(),
            request.request_id(),
            principal,
            None,
            target,
            request.generation(),
            history.tail(),
        )
        .map_err(|_| corrupt())?;
        let receipt = receipt::reconstruct(&record)?;
        let successor = history.advance(&receipt).map_err(|_| corrupt())?;
        Ok(Self {
            predecessor: history,
            before_admission: admission,
            registration,
            before_administration: allocator,
            admission: ReplicationPrimaryAdmissionV1::fenced(record.clone()),
            record,
            receipt,
            successor,
            next_administration: allocated.next(),
        })
    }
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[path = "primary_fence_transaction.rs"]
mod transaction;

#[path = "primary_fence_authority.rs"]
mod authority;
pub(crate) use authority::{PrimaryFenceAwaiting, PrimaryFenceCandidate, PrimaryFenceCompletion};

#[path = "primary_fence_owner.rs"]
mod owner;

#[path = "primary_fence_proof.rs"]
pub(crate) mod proof;

#[path = "primary_fence_receipt.rs"]
mod receipt;

#[path = "primary_fence_state.rs"]
pub(crate) mod state;

/// Fixture lowering only: production must own the coordinator barrier and policy.
#[cfg(test)]
pub(crate) fn commit_fixture_fence(
    database: &redb::Database,
    request: PrimaryFenceRequestV1,
    principal: AuditPrincipalV1,
    timestamp: Timestamp,
) -> Result<StoredPrimaryFenceAdministrationV1, StorageError> {
    let (awaiting, _) =
        authority::PrimaryFenceCandidate::begin(database, request, principal.clone())?
            .read_transaction_current()?;
    awaiting
        .stage(request, principal, timestamp)?
        .commit_for_test()
}

#[cfg(test)]
#[path = "primary_fence_write_tests.rs"]
mod tests;
