//! Physical ownership of the closed fence recipe. No transaction escapes staging.
use super::owner::FenceTransaction;
use super::*;
use crate::{
    changelog_v3_activation::{HISTORY, SOURCE_HOLDS},
    changelog_v3_write::{apply_mutation, check_predecessor, table_inventory},
    error::{precommit_storage_error, table_error},
    layout::{
        AUDIT, META, META_ADMINISTRATION_SEQUENCE, META_APPLICATION_SEQUENCE, META_DATABASE_ID,
        META_HISTORY_INCARNATION, META_RECORD_REGISTRY,
    },
};
use redb::{ReadableTable, WriteTransaction};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, AuthoritativeNamespaceV2, DurableCodecErrorKind,
    ReplicationFollowerStateV3, proto_codec::*,
};

pub(crate) struct PreparedPrimaryFenceWrite {
    transaction: FenceTransaction,
    record: StoredPrimaryFenceAdministrationV1,
}
impl PrimaryFencePlan {
    /// Consumes the fresh owner held across the current-authority decision.
    /// Only the sibling authority module may provide this transaction; no raw
    /// transaction ingress is exposed outside this private fence implementation.
    pub(super) fn stage_in(
        self,
        owner: FenceTransaction,
    ) -> Result<PreparedPrimaryFenceWrite, StorageError> {
        let transaction = owner.transaction()?;
        let tables = table_inventory(transaction)?;
        for namespace in [
            N::Audit,
            N::ChangelogHistory,
            N::ReplicationSourceHolds,
            N::NextAdministrationSequence,
        ] {
            if !tables.contains(namespace.table()) {
                return Err(corrupt());
            }
        }
        self.check_metadata(transaction, false)?;
        {
            let holds = transaction.open_table(SOURCE_HOLDS).map_err(table_error)?;
            let expected =
                encode_replication_source_hold_v2(self.registration).map_err(codec_error)?;
            let row = holds
                .get(self.registration.hold().storage_key().as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            if row.value() != expected.as_bytes() {
                return Err(corrupt());
            }
            let receipts = transaction.open_table(HISTORY).map_err(table_error)?;
            // Stream the complete retained interval, including every live hold.
            crate::changelog_v3_roots::validate_retained_rows(self.predecessor, &receipts, &holds)?;
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            crate::replication_registration_links::validate(
                &holds,
                &audit,
                self.predecessor,
                &receipts,
            )?;
            self.refuse_retained_fence(&audit)?;
            if receipts
                .get(
                    self.receipt
                        .binding()
                        .sequence
                        .get()
                        .to_be_bytes()
                        .as_slice(),
                )
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(corrupt());
            }
        }
        for mutation in self.receipt.mutations() {
            check_predecessor(transaction, mutation)?;
        }
        // Finish all bounded encoding before any mutation. No arbitrary mutation
        // or post-staging access is exposed by the returned owner.
        let admission =
            encode_replication_primary_admission_v1(&self.admission).map_err(codec_error)?;
        let receipt = self.receipt.encode().map_err(|_| corrupt())?;
        let history = encode_changelog_history_state_v3(self.successor).map_err(codec_error)?;
        let allocator =
            encode_changelog_transaction_allocator_v3(self.successor.expected_allocator())
                .map_err(codec_error)?;
        for mutation in self.receipt.mutations() {
            apply_mutation(transaction, mutation)?;
        }
        #[cfg(test)]
        crash_edge("audit");
        transaction
            .open_table(META)
            .map_err(table_error)?
            .insert(admission_key()?, admission.as_bytes())
            .map_err(precommit_storage_error)?;
        #[cfg(test)]
        crash_edge("admission");
        transaction
            .open_table(HISTORY)
            .map_err(table_error)?
            .insert(
                self.receipt
                    .binding()
                    .sequence
                    .get()
                    .to_be_bytes()
                    .as_slice(),
                receipt.as_slice(),
            )
            .map_err(precommit_storage_error)?;
        #[cfg(test)]
        crash_edge("receipt");
        {
            let mut meta = transaction.open_table(META).map_err(table_error)?;
            meta.insert(key(N::ChangelogHistoryState)?, history.as_bytes())
                .map_err(precommit_storage_error)?;
            meta.insert(key(N::NextChangelogTransaction)?, allocator.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        self.check_metadata(transaction, true)?;
        #[cfg(test)]
        crash_edge("roots");
        Ok(PreparedPrimaryFenceWrite {
            transaction: owner,
            record: self.record,
        })
    }

    fn check_metadata(
        &self,
        transaction: &WriteTransaction,
        after: bool,
    ) -> Result<(), StorageError> {
        let meta = transaction.open_table(META).map_err(table_error)?;
        for (key, expected) in self.metadata(after)? {
            let row = meta
                .get(key)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            if row.value() != expected.as_bytes() {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    pub(super) fn metadata(
        &self,
        after: bool,
    ) -> Result<Vec<(&'static str, CanonicalStoredEnvelopeV1)>, StorageError> {
        let history = if after {
            self.successor
        } else {
            self.predecessor
        };
        let administration = if after {
            self.next_administration
        } else {
            self.before_administration
        };
        let admission = if after {
            &self.admission
        } else {
            &self.before_admission
        };
        source_metadata(history, administration, admission)
    }

    fn refuse_retained_fence(
        &self,
        audit: &impl ReadableTable<&'static [u8], &'static [u8]>,
    ) -> Result<(), StorageError> {
        // Constant-memory scan: an erased or replaced Fenced row cannot turn a
        // retained current-lineage fence record back into Active admission.
        for row in audit.iter().map_err(precommit_storage_error)? {
            let (key, value) = row.map_err(precommit_storage_error)?;
            let decoded = match decode_primary_fence_administration_v1(value.value()) {
                Ok(decoded) => decoded,
                Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                    continue;
                }
                Err(error) => return Err(codec_error(error)),
            };
            let record = decoded.value();
            let lineage = self.predecessor.lineage();
            if key.value() != crate::keys::encode_audit_key(record.administration_sequence())
                || record.lineage().database_id() != lineage.database_id()
                || record.lineage().history_incarnation() >= lineage.history_incarnation()
                || Some(record.administration_sequence())
                    > self.predecessor.tail().frontier().administration()
            {
                return Err(corrupt());
            }
        }
        Ok(())
    }
}
impl PreparedPrimaryFenceWrite {
    /// The retained owner aborts on drop and handles commit uncertainty and
    /// successor publication before releasing its source mutation lease.
    pub(crate) fn commit(self) -> Result<StoredPrimaryFenceAdministrationV1, StorageError> {
        self.transaction.commit()?;
        #[cfg(test)]
        crash_edge("committed");
        Ok(self.record)
    }
}
fn key(namespace: N) -> Result<&'static str, StorageError> {
    namespace
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}
fn admission_key() -> Result<&'static str, StorageError> {
    AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}
#[cfg(test)]
fn crash_edge(edge: &str) {
    if std::env::var("RIFFDB_PRIMARY_FENCE_EDGE").ok().as_deref() == Some(edge) {
        std::process::exit(94);
    }
}

pub(super) fn source_metadata(
    history: ChangelogHistoryStateV3,
    administration: AdministrationSequenceAllocator,
    admission: &ReplicationPrimaryAdmissionV1,
) -> Result<Vec<(&'static str, CanonicalStoredEnvelopeV1)>, StorageError> {
    let lineage = history.lineage();
    let application = history.tail().frontier().application().map_or(
        ApplicationSequenceAllocator::initial(),
        |head| {
            head.checked_next().map_or(
                ApplicationSequenceAllocator::Exhausted,
                ApplicationSequenceAllocator::next,
            )
        },
    );
    Ok(vec![
        (
            META_DATABASE_ID,
            encode_database_identity_v1(lineage.database_id()).map_err(codec_error)?,
        ),
        (
            META_HISTORY_INCARNATION,
            encode_history_incarnation_v1(lineage.history_incarnation()).map_err(codec_error)?,
        ),
        (
            META_RECORD_REGISTRY,
            encode_record_registry_v2(current_record_registry_digest()).map_err(codec_error)?,
        ),
        (
            META_APPLICATION_SEQUENCE,
            encode_application_sequence_allocator_v1(application).map_err(codec_error)?,
        ),
        (
            META_ADMINISTRATION_SEQUENCE,
            encode_administration_sequence_allocator_v1(administration).map_err(codec_error)?,
        ),
        (
            key(N::AuthoritativeStateCatalog)?,
            encode_authoritative_state_catalog_v2(AuthoritativeStateCatalogV2)
                .map_err(codec_error)?,
        ),
        (
            key(N::LeadershipEpoch)?,
            encode_leadership_epoch_v1(lineage.leadership_epoch()).map_err(codec_error)?,
        ),
        (
            key(N::ChangelogHistoryState)?,
            encode_changelog_history_state_v3(history).map_err(codec_error)?,
        ),
        (
            key(N::NextChangelogTransaction)?,
            encode_changelog_transaction_allocator_v3(history.expected_allocator())
                .map_err(codec_error)?,
        ),
        (
            key(N::ReplicationFollowerState)?,
            encode_replication_follower_state_v3(ReplicationFollowerStateV3::detached())
                .map_err(codec_error)?,
        ),
        (
            admission_key()?,
            encode_replication_primary_admission_v1(admission).map_err(codec_error)?,
        ),
    ])
}
