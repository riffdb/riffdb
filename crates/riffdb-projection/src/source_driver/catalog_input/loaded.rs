//! Typed final native retention; no new IR execution entry or raw catalog API.
//! Construction is private until the real admitted Catalog/worker producer joins.
#![allow(dead_code)]
use super::*;
use riffdb_catalog::{ActiveCatalogSnapshot, ProjectionSourcePlanCaptureV1};
use riffdb_types::exact_provider_budget_v1::ProviderAllocationOwner;
use riffdb_types::{
    OwnedCapacitySharedV1, ProjectionIdentity, ProjectionSourceWorkAllocationV1,
    ProjectionSourceWorkReservationV1,
};

struct LoadedCatalogData<'session, B: SourceCatalogInputBackend> {
    // All checked catalog allocations/aliases retire before native input owners.
    catalog: Option<ProjectionSourceWorkAllocationV1<ActiveCatalogSnapshot>>,
    // These are concrete actual Native owners, not erased numerical claims or
    // borrowed DTOs. Forgetting ANY strong graph alias leaks them together.
    inputs: SourceCatalogInputs<'session, B>,
    // Existing closed strong protocol frees its Arc block before returning this
    // value for destruction; final header admission remains last here too.
    _root_work: ProjectionSourceWorkReservationV1,
}
/// Closed same-allocation graph aliases; no Weak, raw/Deref/Clone-public escape.
struct LoadedSourceCatalogV1<'session, B: SourceCatalogInputBackend> {
    data: OwnedCapacitySharedV1<LoadedCatalogData<'session, B>>,
}
impl<B: SourceCatalogInputBackend> Drop for LoadedSourceCatalogV1<'_, B> {
    fn drop(&mut self) {}
}

/// Exact rejected data and native owners remain bound until final error drop.
/// The ordinary existing error stays typed; there is no synthetic acceptance.
struct LoadedSourceCatalogFailureV1<'session, B: SourceCatalogInputBackend> {
    catalog: Option<ProjectionSourceWorkAllocationV1<ActiveCatalogSnapshot>>,
    cause: SourceDriverError,
    inputs: SourceCatalogInputs<'session, B>,
}
impl<B: SourceCatalogInputBackend> Drop for LoadedSourceCatalogFailureV1<'_, B> {
    fn drop(&mut self) {}
}

impl<'session, B: SourceCatalogInputBackend> LoadedSourceCatalogV1<'session, B> {
    // The sole future same-body Catalog materializer in this module supplies
    // the already-admitted result. No external/public raw-catalog attachment
    // factory exists. This function neither decodes nor claims stack admission.
    fn from_checked(
        inputs: SourceCatalogInputs<'session, B>,
        catalog: Option<ProjectionSourceWorkAllocationV1<ActiveCatalogSnapshot>>,
    ) -> Result<Self, LoadedSourceCatalogFailureV1<'session, B>> {
        let budget = inputs.budget;
        let valid = match (&catalog, inputs.pointer.owner.pointer(), &inputs.envelope) {
            (Some(catalog), Some(pointer), Some(envelope)) => {
                catalog.belongs_to(budget)
                    && catalog.pointer() == pointer
                    && envelope.belongs_to(budget)
            }
            (None, None, None) => true,
            _ => false,
        };
        if !valid {
            return Err(LoadedSourceCatalogFailureV1 {
                catalog,
                cause: SourceDriverError::ForeignCustody,
                inputs,
            });
        }
        // The actual ProviderAllocation<T> root has strictly more inline fields
        // than closed Arc<T>; its sole existing complete allocator/root formula
        // conservatively prices this State's whole closed shell without another
        // Arc layout implementation or a guessed object/count multiplier.
        let bound=match ProviderAllocationOwner::<LoadedCatalogData<'session,B>>::settlement_root_capacity_upper_bound() {
            Ok(bound)=>bound,
            Err(_)=>return Err(LoadedSourceCatalogFailureV1 {
                catalog,cause:SourceDriverError::SourceUnavailable,inputs,
            }),
        };
        let root_work = match budget.reserve(bound) {
            Ok(work) => work,
            Err(_) => {
                return Err(LoadedSourceCatalogFailureV1 {
                    catalog,
                    cause: SourceDriverError::SourceUnavailable,
                    inputs,
                });
            }
        };
        // No fallible step after the first data/native-owner move. Only the
        // existing closed strong constructor is used; native/control ordering
        // remains intact through allocation abort/unwind and every normal drop.
        Ok(Self {
            data: OwnedCapacitySharedV1::new(LoadedCatalogData {
                catalog,
                inputs,
                _root_work: root_work,
            }),
        })
    }
    fn share(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
    fn catalog(&self) -> Option<&ActiveCatalogSnapshot> {
        self.data.catalog.as_deref()
    }
    fn capture_plan<'catalog>(
        &'catalog self,
        identity: &ProjectionIdentity,
    ) -> Result<LoadedSourcePlanV1<'catalog, 'session, B>, SourceDriverError> {
        let catalog = self.catalog().ok_or(SourceDriverError::InputChanged)?;
        let budget = self.data.inputs.budget;
        // The capture's deep plan/schema copies use the existing Catalog owner
        // and sole preadmitted census. Wrapper headers are prepaid separately.
        let control = budget
            .reserve(std::mem::size_of::<LoadedSourcePlanV1<'catalog, 'session, B>>() as u64)
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
        let retained = self.share();
        let capture = catalog
            .capture_projection_source_admitted_v1(identity, budget)
            .map_err(|error| match error {
                riffdb_catalog::ProjectionSourcePlanCaptureErrorV1::Unavailable => {
                    SourceDriverError::SourceUnavailable
                }
                riffdb_catalog::ProjectionSourcePlanCaptureErrorV1::Integrity => {
                    SourceDriverError::InputChanged
                }
            })?;
        Ok(LoadedSourcePlanV1 {
            capture,
            retained,
            _control: control,
        })
    }
}
struct LoadedSourcePlanV1<'catalog, 'session, B: SourceCatalogInputBackend> {
    capture: ProjectionSourcePlanCaptureV1<'catalog>,
    // Actual whole graph alias: mem::forget(plan) retains EnvelopeOwner,
    // PointerOwner, Source domain/native custody and all original charges.
    retained: LoadedSourceCatalogV1<'session, B>,
    _control: ProjectionSourceWorkReservationV1,
}
impl<B: SourceCatalogInputBackend> Drop for LoadedSourcePlanV1<'_, '_, B> {
    fn drop(&mut self) {}
}
impl<B: SourceCatalogInputBackend> LoadedSourcePlanV1<'_, '_, B> {
    fn driver_plan(&self) -> crate::source_driver::SourceDriverPlan<'_> {
        crate::source_driver::SourceDriverPlan::new(&self.capture)
    }
}
