//! Closed Source-only canonical net intent construction outside the writer gate.
//! The same admitted allocation is retained by apply and uncertain reconciliation.
use super::super::maintenance::NativeSourceCheckpointRequest;
use super::*;
use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV3 as N, AuthoritativeTransactionBindingV3,
    ChangelogAttributionV3, MAX_PROJECTION_SOURCE_CONTROL_BYTES, MAX_PROJECTION_SOURCE_STEP_BYTES,
    MAX_PROJECTION_SOURCE_STEP_ROWS, StorageError, StorageErrorKind,
};
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, ProjectionSourceWorkReservationV1,
    ProjectionSourceWorkingBudgetV1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

const MAX_CONTROLS: usize = 6;
const MAX_CHANGES: usize = MAX_PROJECTION_SOURCE_STEP_ROWS as usize + MAX_CONTROLS;

// Constructor accepts only the actual checked native request. This builder is
// not a write/source proof, and only this closed family can append its recipes.
// The expected binding is a proposal; native apply requires the matching opaque
// instruction's commit-assigned binding and transaction-current revalidation.
pub(crate) struct NativeSourceIntentBuilder<'work> {
    budget: &'work ProjectionSourceWorkingBudgetV1,
    operation: SourceCheckpointOperationKind,
    locator: ProjectionSourceLocatorV1,
    predecessor: ChangelogHistoryStateV3,
    expected_binding: AuthoritativeTransactionBindingV3,
    changes: Vec<AuthoritativeMutationV3>,
    failed: bool,
    row_count: usize,
    control_count: usize,
    before_row_bytes: u64,
    after_row_bytes: u64,
    // Last: all constructed data physically drops before prior admission.
    work: ProjectionSourceWorkReservationV1,
}
impl<'work> NativeSourceIntentBuilder<'work> {
    pub(crate) fn new<'request, 'candidate, 'stage, 'slot, 'session>(
        request: &NativeSourceCheckpointRequest<
            'request,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
        predecessor: ChangelogHistoryStateV3,
    ) -> Result<Self, StorageError>
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        use SourceCheckpointOperationKind as K;
        if !matches!(request.operation(), K::Allocate | K::Copy | K::Publish)
            || predecessor.lineage() != request.captured_history().lineage()
            || predecessor.lineage().catalog_digest()
                != riffdb_storage_api::AuthoritativeStateCatalogV3.digest()
            || predecessor.tail().sequence() < request.captured_history().tail().sequence()
        {
            return Err(corrupt());
        }
        let budget = request
            .actual_ports()
            .shared
            .database
            .source_working_budget();
        type Owner =
            riffdb_types::exact_provider_budget_v1::ProviderAllocation<NativeSourceCanonicalIntent>;
        let owner = size_of::<Owner>()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(2 * align_of::<Owner>()))
            .and_then(|bytes| bytes.checked_add(1024))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(limit)?;
        let vector = MAX_CHANGES
            .checked_mul(size_of::<AuthoritativeMutationV3>())
            .ok_or_else(limit)?;
        let work = allocation(vector, align_of::<AuthoritativeMutationV3>())?
            .checked_add(owner)
            .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() as u64))
            .ok_or_else(limit)?;
        let work = budget.reserve(work).map_err(|_| unavailable())?;
        let sequence = predecessor
            .expected_allocator()
            .allocate_one()
            .map_err(|_| limit())?
            .0;
        let binding = AuthoritativeTransactionBindingV3 {
            database_id: predecessor.lineage().database_id(),
            history_incarnation: predecessor.lineage().history_incarnation(),
            predecessor: Some(predecessor.tail().sequence()),
            sequence,
            predecessor_frontier: predecessor.tail().frontier(),
            covered_frontier: predecessor.tail().frontier(),
            prior_history_hash: predecessor.tail().history_hash(),
        };
        Ok(Self {
            budget,
            operation: request.operation(),
            locator: request.captured_locator(),
            predecessor,
            expected_binding: binding,
            changes: Vec::with_capacity(MAX_CHANGES),
            failed: false,
            row_count: 0,
            control_count: 0,
            before_row_bytes: 0,
            after_row_bytes: 0,
            work,
        })
    }
    pub(super) fn expected_publication_binding(&self) -> AuthoritativeTransactionBindingV3 {
        self.expected_binding
    }
    pub(crate) fn put(
        &mut self,
        namespace: N,
        key: &[u8],
        before: Option<&[u8]>,
        after: &[u8],
    ) -> Result<(), StorageError> {
        let result = (|| {
            self.admit_change(namespace, key, before, Some(after))?;
            let change = match before {
                None => AuthoritativeMutationV3::put(namespace, key, None, after),
                Some(before) => AuthoritativeMutationV3::replace(namespace, key, before, after),
            }
            .map_err(|_| corrupt())?;
            self.changes.push(change);
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub(crate) fn remove(
        &mut self,
        namespace: N,
        key: &[u8],
        before: &[u8],
    ) -> Result<(), StorageError> {
        let result = (|| {
            self.admit_change(namespace, key, Some(before), None)?;
            self.changes.push(
                AuthoritativeMutationV3::delete_matching(namespace, key, before)
                    .map_err(|_| corrupt())?,
            );
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn admit_change(
        &mut self,
        namespace: N,
        key: &[u8],
        before: Option<&[u8]>,
        after: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        use SourceCheckpointOperationKind as K;
        let maximum = match (self.operation, namespace) {
            (K::Copy, N::ProjectionSourceRows) if before.is_none() && after.is_some() => {
                if crate::source_keys::source_row_locator_prefix(key).map_err(|_| corrupt())?
                    != self.locator
                {
                    return Err(corrupt());
                }
                self.row_count = self.row_count.checked_add(1).ok_or_else(limit)?;
                if self.row_count > usize::from(MAX_PROJECTION_SOURCE_STEP_ROWS) {
                    return Err(limit());
                }
                MAX_PROJECTION_SOURCE_STEP_BYTES as usize
            }
            (
                K::Allocate,
                N::ProjectionSourceObjects
                | N::ProjectionSourceMaintenance
                | N::ProjectionSourceAllocator,
            )
            | (K::Copy, N::ProjectionSourceMaintenance)
            | (
                K::Publish,
                N::ProjectionSourceObjects
                | N::ProjectionSourceRoots
                | N::ProjectionSourceMaintenance,
            ) => {
                self.control_count = self.control_count.checked_add(1).ok_or_else(limit)?;
                if self.control_count > MAX_CONTROLS {
                    return Err(limit());
                }
                MAX_PROJECTION_SOURCE_CONTROL_BYTES
            }
            _ => return Err(corrupt()),
        };
        if self.changes.len() >= MAX_CHANGES
            || key.is_empty()
            || key.len() > riffdb_types::MAX_KEY_BYTES
            || before.is_some_and(|value| value.is_empty() || value.len() > maximum)
            || after.is_some_and(|value| value.is_empty() || value.len() > maximum)
            || before == after
        {
            return Err(corrupt());
        }
        if namespace == N::ProjectionSourceRows {
            self.before_row_bytes = add_row(self.before_row_bytes, key, before)?;
            self.after_row_bytes = add_row(self.after_row_bytes, key, after)?;
        }
        let work = allocation(key.len(), 1)?
            .checked_add(allocation(after.map_or(0, <[u8]>::len), 1)?)
            .ok_or_else(limit)?;
        self.work
            .absorb(self.budget.reserve(work).map_err(|_| unavailable())?)
            .map_err(|_| unavailable())
    }
    pub(crate) fn finish(mut self) -> Result<NativeSourceStepIntent, StorageError> {
        if self.failed || self.changes.is_empty() {
            return Err(corrupt());
        }
        self.changes.sort_unstable_by(|left, right| {
            (left.namespace(), left.key()).cmp(&(right.namespace(), right.key()))
        });
        let receipt = AuthoritativeTransactionV3::new_for_catalog(
            self.expected_binding,
            ChangelogAttributionV3::ProjectionSourceMaintenance,
            self.changes,
            self.predecessor.lineage().catalog_digest(),
        )
        .map_err(|_| corrupt())?;
        // Receipt construction streams its existing hash without a temporary
        // Vec. The sole retained encoding is separately admitted before encode.
        let encoded_len = receipt.encoded_len().map_err(|_| limit())?;
        self.work
            .absorb(
                self.budget
                    .reserve(allocation(encoded_len, 1)?)
                    .map_err(|_| unavailable())?,
            )
            .map_err(|_| unavailable())?;
        let canonical_receipt = receipt.encode().map_err(|_| corrupt())?;
        if canonical_receipt.len() != encoded_len {
            return Err(corrupt());
        }
        self.work
            .settle_owned_value(NativeSourceCanonicalIntent {
                operation: self.operation,
                locator: self.locator,
                predecessor: self.predecessor,
                receipt,
                canonical_receipt,
                final_abort: false,
            })
            .map_err(|_| unavailable())
    }
}
fn add_row(total: u64, key: &[u8], value: Option<&[u8]>) -> Result<u64, StorageError> {
    let bytes = key
        .len()
        .checked_add(value.map_or(0, <[u8]>::len))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(limit)?;
    let total = total.checked_add(bytes).ok_or_else(limit)?;
    if total > MAX_PROJECTION_SOURCE_STEP_BYTES {
        return Err(limit());
    }
    Ok(total)
}
fn allocation(bytes: usize, alignment: usize) -> Result<u64, StorageError> {
    owned_allocation_capacity_bound_v1(bytes, alignment).map_err(|_| limit())
}
fn corrupt() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}
fn limit() -> StorageError {
    crate::error::storage_error(StorageErrorKind::LimitExceeded)
}
fn unavailable() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}
