//! Immutable administration views retain their actual source-owned allocation.
//! Copying a view never deep-copies original control envelopes or witnesses.
use std::fmt;

use crate::{EncodedContentCharge, NativeCatalogTransitionV1, SourceInheritanceAdministrationV1};
use riffdb_types::{AdministrationSequence, ProjectionSourceWorkAllocationV1};

/// A fully decoded native origin fact owned by the actual engine's source ledger.
pub struct NativeCatalogTransitionAuditV1 {
    value: ProjectionSourceWorkAllocationV1<NativeCatalogTransitionV1>,
    charge: EncodedContentCharge,
    administration: AdministrationSequence,
}
impl NativeCatalogTransitionAuditV1 {
    pub(crate) fn from_admitted(
        value: ProjectionSourceWorkAllocationV1<NativeCatalogTransitionV1>,
        charge: EncodedContentCharge,
    ) -> Self {
        Self {
            administration: value.parts().administration_sequence,
            value,
            charge,
        }
    }
    /// Exact immutable structural fact; this view grants no native authority.
    pub fn value(&self) -> &NativeCatalogTransitionV1 {
        &self.value
    }
    /// Original canonical envelope's exact charge, without a fresh encoding.
    pub const fn encoded_content_charge(&self) -> EncodedContentCharge {
        self.charge
    }
    /// Shared administration ordering position of this origin fact.
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration
    }
}
impl Clone for NativeCatalogTransitionAuditV1 {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            charge: self.charge,
            administration: self.administration,
        }
    }
}
impl PartialEq for NativeCatalogTransitionAuditV1 {
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }
}
impl Eq for NativeCatalogTransitionAuditV1 {}
impl fmt::Debug for NativeCatalogTransitionAuditV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeCatalogTransitionAuditV1([redacted])")
    }
}

/// A fully decoded source-inheritance fact under the originating source ledger.
pub struct SourceInheritanceAuditV1 {
    value: ProjectionSourceWorkAllocationV1<SourceInheritanceAdministrationV1>,
    charge: EncodedContentCharge,
    administration: AdministrationSequence,
}
impl SourceInheritanceAuditV1 {
    pub(crate) fn from_admitted(
        value: ProjectionSourceWorkAllocationV1<SourceInheritanceAdministrationV1>,
        charge: EncodedContentCharge,
    ) -> Self {
        Self {
            administration: value.parts().administration_sequence,
            value,
            charge,
        }
    }
    /// Exact immutable structural fact; this view cannot acquire a source pin.
    pub fn value(&self) -> &SourceInheritanceAdministrationV1 {
        &self.value
    }
    /// Original canonical envelope's exact charge, preserving its framing.
    pub const fn encoded_content_charge(&self) -> EncodedContentCharge {
        self.charge
    }
    /// Shared administration ordering position of this inheritance fact.
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration
    }
}
impl Clone for SourceInheritanceAuditV1 {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            charge: self.charge,
            administration: self.administration,
        }
    }
}
impl PartialEq for SourceInheritanceAuditV1 {
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }
}
impl Eq for SourceInheritanceAuditV1 {}
impl fmt::Debug for SourceInheritanceAuditV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SourceInheritanceAuditV1([redacted])")
    }
}
