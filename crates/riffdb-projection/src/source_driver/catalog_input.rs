//! Pre-plan catalog bytes owned by the same actual native Source session.
//! Storage owns opaque envelopes/custody; Projection owns catalog interpretation.
use super::seal::SourceBrand;
use super::{SourceCheckpointBackend, SourceDriverError};
use riffdb_storage_api::ActiveCatalogPointerV1;
use riffdb_types::ProjectionSourceWorkingBudgetV1;

/// Closed native records retain actual base/domain/capacity even when an unused
/// response is forgotten. Implementing these traits for another backend cannot
/// construct the concrete native owner's private fields or its instance brand.
#[doc(hidden)]
pub trait SourceCatalogPointerOwner<B: SourceCheckpointBackend> {
    fn pointer(&self) -> Option<&ActiveCatalogPointerV1>;
    fn belongs_to(&self, budget: &ProjectionSourceWorkingBudgetV1) -> bool;
    fn matches_native(&self, actual: &B::Brand) -> bool;
}
#[doc(hidden)]
pub trait SourceCatalogEnvelopeOwner<B: SourceCheckpointBackend> {
    /// StorageAPI-decoded opaque record; no executable IR or catalog authority.
    fn record(&self) -> &riffdb_storage_api::StoredContractBundleV1;
    fn belongs_to(&self, budget: &ProjectionSourceWorkingBudgetV1) -> bool;
    fn matches_native(&self, actual: &B::Brand) -> bool;
}
/// Actual native constructor fixes the opaque data-owning types, never a naked
/// Vec, semantic caller-selected catalog, callback or numerical DTO authority.
#[doc(hidden)]
pub trait SourceCatalogInputBackend: SourceCheckpointBackend {
    type PointerOwner: SourceCatalogPointerOwner<Self>;
    type EnvelopeOwner: SourceCatalogEnvelopeOwner<Self>;
    type Reader;
    fn read_catalog_pointer<'session>(
        reader: &mut Self::Reader,
        actual: &Self::Brand,
        request: SourceCatalogPointerReadInstruction<'session, Self>,
    ) -> Result<SourceCatalogPointerEvidence<'session, Self>, SourceDriverError>;
    fn read_catalog_bundle<'pointer, 'session>(
        reader: &mut Self::Reader,
        actual: &Self::Brand,
        request: SourceCatalogBundleReadInstruction<'pointer, 'session, Self>,
    ) -> Result<SourceCatalogBundleEvidence<'pointer, 'session, Self>, SourceDriverError>;
}

pub struct SourceCatalogPointerReadInstruction<'session, B: SourceCatalogInputBackend> {
    pub(super) brand: SourceBrand<'session, B>,
    pub(super) budget: &'session ProjectionSourceWorkingBudgetV1,
}
impl<'session, B: SourceCatalogInputBackend> SourceCatalogPointerReadInstruction<'session, B> {
    pub fn check_before_read(&self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        self.brand.check(actual)
    }
    pub fn attach(
        self,
        actual: &B::Brand,
        owner: B::PointerOwner,
    ) -> Result<SourceCatalogPointerEvidence<'session, B>, SourceDriverError> {
        self.brand.check(actual)?;
        if !owner.belongs_to(self.budget) || !owner.matches_native(actual) {
            return Err(SourceDriverError::ForeignCustody);
        }
        Ok(SourceCatalogPointerEvidence {
            owner,
            brand: self.brand,
        })
    }
}
/// Owns native custody even for true pointer absence; no reusable authority or
/// independently extracted Source allocation is available outside this module.
pub struct SourceCatalogPointerEvidence<'session, B: SourceCatalogInputBackend> {
    pub(super) owner: B::PointerOwner,
    pub(super) brand: SourceBrand<'session, B>,
}
impl<B: SourceCatalogInputBackend> Drop for SourceCatalogPointerEvidence<'_, B> {
    fn drop(&mut self) {}
}

pub struct SourceCatalogBundleReadInstruction<'pointer, 'session, B: SourceCatalogInputBackend> {
    pub(super) brand: SourceBrand<'session, B>,
    pub(super) pointer: &'pointer B::PointerOwner,
    pub(super) budget: &'session ProjectionSourceWorkingBudgetV1,
}
impl<'pointer, 'session, B: SourceCatalogInputBackend>
    SourceCatalogBundleReadInstruction<'pointer, 'session, B>
{
    pub fn check_before_read(&self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        self.brand.check(actual)?;
        if !self.pointer.belongs_to(self.budget) || !self.pointer.matches_native(actual) {
            return Err(SourceDriverError::ForeignCustody);
        }
        if self.pointer.pointer().is_none() {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(())
    }
    /// Opaque actual native owner borrowed by this instruction; no allocation,
    /// raw data owner extraction or independent pointer/ledger substitution.
    pub fn pointer_owner(&self) -> &B::PointerOwner {
        self.pointer
    }
    pub fn pointer(&self) -> Result<&ActiveCatalogPointerV1, SourceDriverError> {
        self.pointer
            .pointer()
            .ok_or(SourceDriverError::InputChanged)
    }
    pub fn attach(
        self,
        actual: &B::Brand,
        owner: B::EnvelopeOwner,
    ) -> Result<SourceCatalogBundleEvidence<'pointer, 'session, B>, SourceDriverError> {
        self.check_before_read(actual)?;
        if !owner.belongs_to(self.budget) || !owner.matches_native(actual) {
            return Err(SourceDriverError::ForeignCustody);
        }
        Ok(SourceCatalogBundleEvidence {
            owner: Some(owner),
            pointer: self.pointer,
            brand: self.brand,
        })
    }
}
/// Only the Projection loader interprets the canonical IR in the opaque record. Its native owner
/// survives through the complete loaded graph; Drop/forget cannot detach it.
pub struct SourceCatalogBundleEvidence<'pointer, 'session, B: SourceCatalogInputBackend> {
    pub(super) owner: Option<B::EnvelopeOwner>,
    #[expect(
        dead_code,
        reason = "retains pointer borrow custody for the lifetime of the bundle evidence"
    )]
    pub(super) pointer: &'pointer B::PointerOwner,
    pub(super) brand: SourceBrand<'session, B>,
}
impl<B: SourceCatalogInputBackend> Drop for SourceCatalogBundleEvidence<'_, '_, B> {
    fn drop(&mut self) {}
}

/// Native owns all retained bytes/custody. This is only pre-plan input, not a
/// decoded catalog, full Original completion, stage or publication authority.
pub struct SourceCatalogInputs<'session, B: SourceCatalogInputBackend> {
    #[expect(
        dead_code,
        reason = "retains native pointer custody until SourceCatalogInputs drops"
    )]
    pub(super) pointer: SourceCatalogPointerEvidence<'session, B>,
    #[expect(
        dead_code,
        reason = "retains native envelope custody until SourceCatalogInputs drops"
    )]
    pub(super) envelope: Option<B::EnvelopeOwner>,
    #[expect(
        dead_code,
        reason = "retains working budget borrow custody until SourceCatalogInputs drops"
    )]
    pub(super) budget: &'session ProjectionSourceWorkingBudgetV1,
}
impl<B: SourceCatalogInputBackend> Drop for SourceCatalogInputs<'_, B> {
    fn drop(&mut self) {}
}
/// One fixed instruction chain over the actual private native brand. No caller
/// can provide a catalog subset, naked envelope, semantic closure or fake end.
pub fn read_source_catalog_input<'session, B: SourceCatalogInputBackend>(
    reader: &mut B::Reader,
    actual: &'session B::Brand,
    budget: &'session ProjectionSourceWorkingBudgetV1,
) -> Result<SourceCatalogInputs<'session, B>, SourceDriverError> {
    let pointer = B::read_catalog_pointer(
        reader,
        actual,
        SourceCatalogPointerReadInstruction {
            brand: SourceBrand::new(actual),
            budget,
        },
    )?;
    pointer.brand.check(actual)?;
    let envelope = if pointer.owner.pointer().is_some() {
        let mut evidence = B::read_catalog_bundle(
            reader,
            actual,
            SourceCatalogBundleReadInstruction {
                brand: SourceBrand::new(actual),
                pointer: &pointer.owner,
                budget,
            },
        )?;
        evidence.brand.check(actual)?;
        // Native EnvelopeOwner itself retains an actual pointer-owner alias;
        // remove only the temporary borrow, never detach numerical/native data.
        let owner = evidence
            .owner
            .as_ref()
            .ok_or(SourceDriverError::InputChanged)?;
        let selected = pointer
            .owner
            .pointer()
            .ok_or(SourceDriverError::InputChanged)?;
        let record = owner.record();
        if record.lineage() != selected.lineage()
            || record.contract_version() != selected.contract_version()
            || record.bundle_hash() != selected.bundle_hash()
        {
            return Err(SourceDriverError::InputChanged);
        }
        evidence.owner.take()
    } else {
        None
    };
    if pointer.owner.pointer().is_some() && envelope.is_none() {
        return Err(SourceDriverError::InputChanged);
    }
    Ok(SourceCatalogInputs {
        pointer,
        envelope,
        budget,
    })
}
