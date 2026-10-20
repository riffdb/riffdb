//! Four fixed same-pin observations of an already validated actual Fresh owner.
//! This component neither installs origin evidence nor grants Source closure.
use super::*;
use sha2::{Digest, Sha256};

pub(crate) struct SourceOriginObservationV1 {
    stamps: [[u8; 32]; 3],
    visits: u8,
    checked: Option<CheckedCurrentSourceOriginV1>,
}
#[derive(Clone, Copy)]
pub(crate) struct CheckedCurrentSourceOriginV1 {
    reference: NativeCatalogOriginReferenceV1,
    lineage: riffdb_storage_api::ChangelogLineageV3,
}
impl SourceOriginObservationV1 {
    pub(crate) const fn new() -> Self {
        Self {
            stamps: [[0; 32]; 3],
            visits: 0,
            checked: None,
        }
    }
    pub(crate) const fn complete(&self) -> bool {
        self.visits == 4
    }
    // Only the sealed scan owner calls this on its existing retained observation.
    // An observation never brands/adopts a foreign transaction or mints a hot proof.
    pub(crate) fn step(
        &mut self,
        owner: &crate::engine_owner::EngineOwner<Database>,
        read: &redb::ReadTransaction,
    ) -> Result<(), StorageError> {
        if self.visits < 3 {
            let key = match self.visits {
                0 => metadata_key(AuthoritativeNamespaceV1::AuthoritativeStateCatalog)?,
                1 => META_HISTORY_INCARNATION,
                2 => metadata_key(AuthoritativeNamespaceV1::LeadershipEpoch)?,
                _ => return Err(invariant()),
            };
            let table = read.open_table(META).map_err(table_error)?;
            let value = table
                .get(key)
                .map_err(precommit_storage_error)?
                .ok_or_else(|| value_error(()))?;
            // Same bound as the original fixed stamp checker, before hashing.
            if value.value().is_empty() || value.value().len() > 512 {
                return Err(value_error(()));
            }
            self.stamps[usize::from(self.visits)] = Sha256::digest(value.value()).into();
        } else if self.visits == 3 {
            let key = owner
                .native_origin_context()
                .anchor_key()
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
            let table = read.open_table(AUDIT).map_err(table_error)?;
            let value = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(|| value_error(()))?;
            if value.value().is_empty() || value.value().len() > 2048 {
                return Err(value_error(()));
            }
            let (reference, lineage) = owner.native_origin_context().checked_observed_origin(
                &key,
                value.value().len(),
                Sha256::digest(value.value()).into(),
                context::LineageStampsV1::from_observed_digests(
                    self.stamps[0],
                    self.stamps[1],
                    self.stamps[2],
                ),
            )?;
            self.checked = Some(CheckedCurrentSourceOriginV1 { reference, lineage });
        } else {
            return Err(invariant());
        }
        self.visits += 1;
        Ok(())
    }
    pub(crate) fn checked(&self) -> Result<CheckedCurrentSourceOriginV1, StorageError> {
        if !self.complete() {
            return Err(invariant());
        }
        self.checked.ok_or_else(invariant)
    }
}
impl CheckedCurrentSourceOriginV1 {
    // Origin equality is one local invariant. It does not validate allocation,
    // publication receipt, captured prefix ancestry, base/tail or transition.
    pub(crate) fn matches_descriptor(
        self,
        descriptor: &riffdb_storage_api::ProjectionSourceDescriptorV1,
    ) -> Result<bool, StorageError> {
        let locator = descriptor.reference().locator();
        if locator.allocation_history_incarnation() != self.lineage.history_incarnation() {
            // Inheritance/cut requires its real closed owner, never ID rewriting.
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        if descriptor.captured_source().lineage() != self.lineage {
            return Err(value_error(()));
        }
        riffdb_storage_api::projection_source_origin_commitment_v1(
            locator.database_id(),
            locator.allocation_history_incarnation(),
            self.reference,
        )
        .map(|digest| digest == descriptor.reference().origin())
        .map_err(value_error)
    }
}
