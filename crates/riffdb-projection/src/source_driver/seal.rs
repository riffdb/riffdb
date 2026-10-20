//! Non-detachable exact-instance branding for the closed source protocol.
use super::{SourceCheckpointBackend, SourceDriverError};
use std::marker::PhantomData;

// Invariance prevents generic wrappers from substituting their backend type.
// No accessor returns the brand, even to the composition/coordinator owner.
pub(super) struct SourceBrand<'session, B: SourceCheckpointBackend> {
    native: &'session B::Brand,
    _backend: PhantomData<fn(B) -> B>,
}
impl<'session, B: SourceCheckpointBackend> SourceBrand<'session, B> {
    pub(super) fn new(native: &'session B::Brand) -> Self {
        Self {
            native,
            _backend: PhantomData,
        }
    }
    pub(super) fn check(&self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        if std::ptr::eq(self.native, actual) {
            Ok(())
        } else {
            Err(SourceDriverError::ForeignCustody)
        }
    }
    pub(super) fn check_same(&self, other: &Self) -> Result<(), SourceDriverError> {
        self.check(other.native)
    }
    pub(super) fn native(&self) -> &'session B::Brand {
        self.native
    }
    pub(super) fn next(&self) -> Self {
        Self::new(self.native)
    }
}
impl<B: SourceCheckpointBackend> Drop for SourceBrand<'_, B> {
    fn drop(&mut self) {
        // The borrowed real brand survives physical instruction/candidate drop,
        // including an unused value whose final getter was never called.
    }
}

// A second pointer protects stage preimages even within one captured session.
// Native uses a nonzero, non-cloneable private stage brand; never a numeric ID.
pub(super) struct SourceStageBrand<'session, 'stage, B: SourceCheckpointBackend> {
    session: SourceBrand<'session, B>,
    stage: &'stage B::StageBrand,
}
impl<'session, 'stage, B: SourceCheckpointBackend> SourceStageBrand<'session, 'stage, B> {
    pub(super) fn new(session: SourceBrand<'session, B>, stage: &'stage B::StageBrand) -> Self {
        Self { session, stage }
    }
    pub(super) fn check(
        &self,
        session: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.session.check(session)?;
        if std::ptr::eq(self.stage, stage) {
            Ok(())
        } else {
            Err(SourceDriverError::ForeignCustody)
        }
    }
    pub(super) fn native_stage(&self) -> &'stage B::StageBrand {
        self.stage
    }
    pub(super) fn next(&self) -> Self {
        Self::new(self.session.next(), self.stage)
    }
}
impl<B: SourceCheckpointBackend> Drop for SourceStageBrand<'_, '_, B> {
    fn drop(&mut self) {}
}
