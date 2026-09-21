//! Fresh physical owner retained across the coordinator's pure policy decision.
use super::owner::FenceTransaction;
use super::*;
use crate::{
    changelog_v3_write::table_inventory,
    error::table_error,
    layout::{CAPABILITIES, CAPABILITY_TOKENS, META, META_DATABASE_ID},
};
use redb::ReadableTable;
#[cfg(test)]
use redb::{Database, Durability};
use riffdb_storage_api::{
    CapabilityLifecycleV1, PrimaryFenceRefusalV1,
    TransactionCurrentCapabilityObservationV1 as Current,
};

/// A fresh transaction with no earlier staged mutations or raw transaction escape.
pub(crate) struct PrimaryFenceCandidate {
    transaction: FenceTransaction,
    request: PrimaryFenceRequestV1,
    principal: AuditPrincipalV1,
}

/// The physical writer stays owned while the coordinator reauthorizes the exact
/// request against digest-free current facts and a newly sampled clock value.
pub(crate) struct PrimaryFenceAwaiting {
    candidate: PrimaryFenceCandidate,
    current: Option<Current>,
}

/// A new atomic transition or checked immutable replay; replay holds no writer.
pub(crate) enum PrimaryFenceCompletion {
    Write(Box<transaction::PreparedPrimaryFenceWrite>),
    Replay(Box<StoredPrimaryFenceAdministrationV1>),
    Refused(PrimaryFenceRefusalV1),
}

#[cfg(test)]
impl PrimaryFenceCompletion {
    pub(crate) fn commit(self) -> Result<StoredPrimaryFenceAdministrationV1, StorageError> {
        match self {
            Self::Write(write) => write.commit(),
            Self::Replay(record) => Ok(*record),
            Self::Refused(_) => Err(storage_error(StorageErrorKind::Unavailable)),
        }
    }
}

#[cfg(test)]
impl PrimaryFenceCompletion {
    pub(super) fn commit_for_test(
        self,
    ) -> Result<StoredPrimaryFenceAdministrationV1, StorageError> {
        self.commit()
    }
}

impl PrimaryFencePlan {
    #[cfg(test)]
    pub(crate) fn open(self, database: &Database) -> Result<PrimaryFenceCandidate, StorageError> {
        PrimaryFenceCandidate::begin(
            database,
            request(&self.record),
            self.record.principal().clone(),
        )
    }

    /// Isolated physical fixtures exercise the same current-state binding. Real
    /// callers retain the drained owner and perform pure policy reauthorization.
    #[cfg(test)]
    pub(super) fn stage(self, database: &Database) -> Result<PrimaryFenceCompletion, StorageError> {
        let request = request(&self.record);
        let principal = self.record.principal().clone();
        let timestamp = self.record.timestamp();
        let (awaiting, _) = self.open(database)?.read_transaction_current()?;
        awaiting.stage(request, principal, timestamp)
    }
}

impl PrimaryFenceCandidate {
    pub(crate) fn from_drained(
        write: crate::store::ControlWrite,
        request: PrimaryFenceRequestV1,
        principal: AuditPrincipalV1,
    ) -> Result<Self, StorageError> {
        Self::from_owner(
            FenceTransaction::Drained(Box::new(write)),
            request,
            principal,
        )
    }

    #[cfg(test)]
    pub(crate) fn begin(
        database: &Database,
        request: PrimaryFenceRequestV1,
        principal: AuditPrincipalV1,
    ) -> Result<Self, StorageError> {
        let mut transaction = database
            .begin_write()
            .map_err(crate::error::transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Self::from_owner(
            FenceTransaction::Isolated(Box::new(transaction)),
            request,
            principal,
        )
    }

    fn from_owner(
        transaction: FenceTransaction,
        request: PrimaryFenceRequestV1,
        principal: AuditPrincipalV1,
    ) -> Result<Self, StorageError> {
        let tables = table_inventory(transaction.transaction()?)?;
        for namespace in [N::Capabilities, N::CapabilityTokens] {
            if !tables.contains(namespace.table()) {
                return Err(corrupt());
            }
        }
        Ok(Self {
            transaction,
            request,
            principal,
        })
    }

    pub(crate) fn read_transaction_current(
        self,
    ) -> Result<(PrimaryFenceAwaiting, Option<Current>), StorageError> {
        let current = self.current()?;
        Ok((
            PrimaryFenceAwaiting {
                candidate: self,
                current: current.clone(),
            },
            current,
        ))
    }

    fn current(&self) -> Result<Option<Current>, StorageError> {
        let meta = self
            .transaction
            .transaction()?
            .open_table(META)
            .map_err(table_error)?;
        let database_id = *riffdb_storage_api::proto_codec::decode_database_identity_v1(
            meta.get(META_DATABASE_ID)
                .map_err(crate::error::precommit_storage_error)?
                .ok_or_else(corrupt)?
                .value(),
        )
        .map_err(codec_error)?
        .value();
        let capabilities = self
            .transaction
            .transaction()?
            .open_table(CAPABILITIES)
            .map_err(table_error)?;
        let tokens = self
            .transaction
            .transaction()?
            .open_table(CAPABILITY_TOKENS)
            .map_err(table_error)?;
        Ok(crate::administration::capability_from_tables(
            &capabilities,
            &tokens,
            database_id,
            self.principal.capability_id(),
        )?
        .as_ref()
        .map(Current::from_record))
    }
}

impl PrimaryFenceAwaiting {
    /// Only the coordinator lowers a successful pure policy decision here.
    /// Storage checks binding, lifecycle and time; permission evaluation remains
    /// in policy. Dropping either owner aborts the uncommitted transaction.
    pub(crate) fn stage(
        self,
        selected: PrimaryFenceRequestV1,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
    ) -> Result<PrimaryFenceCompletion, StorageError> {
        if selected != self.candidate.request
            || principal != self.candidate.principal
            || self.candidate.current()? != self.current
            || !self.current.as_ref().is_some_and(|current| {
                current.capability_id() == principal.capability_id()
                    && current.revision() == principal.capability_revision()
                    && current.principal_id() == principal.principal_id()
                    && current.actor_kind() == principal.actor_kind()
                    && current.lifecycle() == &CapabilityLifecycleV1::Active
                    && current.issued_at() <= timestamp
                    && timestamp < current.expires_at()
            })
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let PrimaryFenceCandidate { transaction, .. } = self.candidate;
        let source = match state::SourceState::read(transaction.transaction()?, selected)? {
            std::ops::ControlFlow::Continue(source) => source,
            std::ops::ControlFlow::Break(refusal) => {
                return Ok(PrimaryFenceCompletion::Refused(refusal));
            }
        };
        if let Some(record) = source.admission.fence() {
            if record.operation_id() != selected.operation_id()
                || record.target() != selected.target()
                || record.generation() != selected.generation()
                || record.principal() != &principal
            {
                return Ok(PrimaryFenceCompletion::Refused(
                    PrimaryFenceRefusalV1::FenceConflict,
                ));
            }
            // Current authority was checked before resolving existing evidence.
            // Abandon this read-only writer; never rewrite the original receipt.
            return Ok(PrimaryFenceCompletion::Replay(Box::new(record.clone())));
        }
        // The final sample owns the audit timestamp and checksum. Every planned
        // root and registration beforeimage is rechecked in this same writer.
        PrimaryFencePlan::new(
            source.history,
            source.admission,
            source.registration,
            selected,
            principal,
            timestamp,
            source.allocator,
        )?
        .stage_in(transaction)
        .map(|write| PrimaryFenceCompletion::Write(Box::new(write)))
    }
}

#[cfg(test)]
fn request(record: &StoredPrimaryFenceAdministrationV1) -> PrimaryFenceRequestV1 {
    PrimaryFenceRequestV1::new(
        record.request_id(),
        record.operation_id(),
        record.target(),
        record.generation(),
    )
}
