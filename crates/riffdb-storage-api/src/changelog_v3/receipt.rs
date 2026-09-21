use riffdb_types::{DatabaseId, DualFrontier};

use crate::{MAX_CHANGELOG_FRAME_ENTRIES, MAX_STAGED_COMMANDS};

use super::{AuthoritativeMutationV3, ChangelogTransactionSequence, ChangelogV3Error};

/// Closed source attribution; no user-provided name or diagnostic text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum ChangelogAttributionV3 {
    /// One validated journaled application group.
    JournaledApplicationGroup = 1,
    /// One validated journaled standalone service-audit group.
    JournaledServiceAudit = 2,
    /// One Immediate application or service-audit group.
    DirectApplicationOrServiceAuditGroup = 3,
    /// Contract catalog administration.
    CatalogAdministration = 4,
    /// Query-module activation.
    QueryModuleAdministration = 5,
    /// Reactive-module publication.
    ReactiveModuleAdministration = 6,
    /// Installation campaign transition.
    ApplicationInstallationCampaign = 7,
    /// Application export transition.
    ApplicationExportOperation = 8,
    /// Durable event-consumer transition.
    EventConsumerTransition = 9,
    /// Capability creation or revocation.
    CapabilityAdministration = 10,
    /// Initial capability bootstrap.
    CapabilityBootstrap = 11,
    /// Durable outbox delivery state.
    OutboxTransition = 12,
    /// Columnar lifecycle or retention control.
    ColumnarProjectionControl = 13,
    /// Vector lifecycle or retention control.
    VectorProjectionControl = 14,
    /// One bounded same-lineage contract migration batch.
    ContractMigrationBatch = 15,
    /// Contract migration cutover.
    ContractMigrationCutover = 16,
    /// One bounded index migration batch.
    IndexMigrationBatch = 17,
    /// One bounded same-lineage storage migration transaction.
    StorageFormatMigration = 18,
    /// Authoritative history-retention hold transition.
    RetentionHold = 19,
    /// One bounded authoritative history prune transaction.
    RetentionPrune = 20,
    /// Validated-prefix evidence publication.
    ValidatedPrefixCheckpoint = 21,
    /// Final CLEAN lifecycle transaction.
    CleanClose = 22,
    /// DIRTY activation or CLEAN consumption.
    DirtyActivation = 23,
    /// Atomic initial V3 activation.
    V3Activation = 24,
    /// Same-lineage V3 chain rotation.
    V3Rotation = 25,
    /// Replication-control history reclamation.
    HistoryReclamation = 26,
    /// Source bootstrap/consumer fence transition.
    ReplicationSourceHold = 27,
    /// Locally durable follower apply.
    FollowerApply = 28,
    /// Audited promotion into a fresh fenced lineage.
    Promotion = 29,
    /// Destructive restore's fresh lineage anchor.
    RestoreAnchor = 30,
    /// Mixed projection lifecycle/generation/retention control transition.
    ProjectionControl = 31,
    /// Pending command admission without an application or audit allocation.
    CommandAdmission = 32,
    /// Terminal execution failure, optionally accompanied by service audit.
    CommandExecutionFailure = 33,
    /// One irreversible primary-admission fence and exact administration receipt.
    PrimaryFence = 34,
}

impl ChangelogAttributionV3 {
    /// All source tags in canonical tag order.
    pub const ALL: [Self; 34] = [
        Self::JournaledApplicationGroup,
        Self::JournaledServiceAudit,
        Self::DirectApplicationOrServiceAuditGroup,
        Self::CatalogAdministration,
        Self::QueryModuleAdministration,
        Self::ReactiveModuleAdministration,
        Self::ApplicationInstallationCampaign,
        Self::ApplicationExportOperation,
        Self::EventConsumerTransition,
        Self::CapabilityAdministration,
        Self::CapabilityBootstrap,
        Self::OutboxTransition,
        Self::ColumnarProjectionControl,
        Self::VectorProjectionControl,
        Self::ContractMigrationBatch,
        Self::ContractMigrationCutover,
        Self::IndexMigrationBatch,
        Self::StorageFormatMigration,
        Self::RetentionHold,
        Self::RetentionPrune,
        Self::ValidatedPrefixCheckpoint,
        Self::CleanClose,
        Self::DirtyActivation,
        Self::V3Activation,
        Self::V3Rotation,
        Self::HistoryReclamation,
        Self::ReplicationSourceHold,
        Self::FollowerApply,
        Self::Promotion,
        Self::RestoreAnchor,
        Self::ProjectionControl,
        Self::CommandAdmission,
        Self::CommandExecutionFailure,
        Self::PrimaryFence,
    ];

    /// Decodes only the closed source catalog.
    #[must_use]
    pub fn from_tag(tag: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|source| *source as u16 == tag)
    }

    fn validate(
        self,
        binding: AuthoritativeTransactionBindingV3,
        empty: bool,
    ) -> Result<(), ChangelogV3Error> {
        let previous = binding.predecessor_frontier;
        let covered = binding.covered_frontier;
        if covered != previous && !covered.advances_from(previous) {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        let app = covered.application().map_or(0, |v| v.get())
            - previous.application().map_or(0, |v| v.get());
        let admin = covered.administration().map_or(0, |v| v.get())
            - previous.administration().map_or(0, |v| v.get());
        let valid = match self {
            Self::JournaledApplicationGroup => {
                app > 0 && app <= MAX_STAGED_COMMANDS as u64 && !empty
            }
            Self::JournaledServiceAudit => {
                app == 0 && admin > 0 && admin <= MAX_STAGED_COMMANDS as u64 && !empty
            }
            Self::DirectApplicationOrServiceAuditGroup => (app > 0 || admin > 0) && !empty,
            Self::CommandAdmission => app == 0 && admin == 0 && !empty,
            Self::CommandExecutionFailure => {
                app == 0 && admin <= MAX_STAGED_COMMANDS as u64 && !empty
            }
            Self::PrimaryFence => {
                app == 0
                    && admin == 1
                    && !empty
                    && previous.administration().is_some()
                    && binding.predecessor.is_some()
            }
            Self::CleanClose
            | Self::DirtyActivation
            | Self::V3Rotation
            | Self::HistoryReclamation
            | Self::ReplicationSourceHold => app == 0 && admin == 0 && empty,
            _ => true,
        };
        if valid {
            Ok(())
        } else {
            Err(ChangelogV3Error::InvalidEncoding)
        }
    }
}

/// Inputs checked when sealing a transaction; not mutable fields of a receipt.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AuthoritativeTransactionBindingV3 {
    /// Exact source database identity.
    pub database_id: DatabaseId,
    /// Nonzero history incarnation.
    pub history_incarnation: u64,
    /// Predecessor physical position, absent only before sequence one.
    pub predecessor: Option<ChangelogTransactionSequence>,
    /// Assigned physical position.
    pub sequence: ChangelogTransactionSequence,
    /// Explicit prior application/administration frontier.
    pub predecessor_frontier: DualFrontier,
    /// Explicit covered application/administration frontier.
    pub covered_frontier: DualFrontier,
    /// Exact preceding history hash; never diagnostic data.
    pub prior_history_hash: [u8; 32],
}

impl std::fmt::Debug for AuthoritativeTransactionBindingV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeTransactionBindingV3([redacted])")
    }
}

/// One immutable canonical net transition set with complete source attribution.
/// Receipt construction checks order; it never sorts away duplicates or guesses
/// expected state from the latest database view.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthoritativeTransactionV3 {
    binding: AuthoritativeTransactionBindingV3,
    attribution: ChangelogAttributionV3,
    mutations: Vec<AuthoritativeMutationV3>,
}

impl AuthoritativeTransactionV3 {
    /// Checks the selected closed catalog's complete, unsplit frame budget before
    /// mutation. Production writers must select their validated retained catalog;
    /// generic receipt reconstruction alone does not certify this admission.
    pub fn new_for_catalog(
        binding: AuthoritativeTransactionBindingV3,
        attribution: ChangelogAttributionV3,
        mutations: Vec<AuthoritativeMutationV3>,
        catalog_digest: [u8; 32],
    ) -> Result<Self, ChangelogV3Error> {
        let receipt = Self::new(binding, attribution, mutations)?;
        super::frame::validate_receipt_catalog(&receipt, catalog_digest)?;
        Ok(receipt)
    }

    /// Reconstructs a checked receipt within the original compatibility ceiling.
    /// Admission additionally checks the retained catalog with `new_for_catalog`;
    /// storage proves attribution/frontiers against actual durable command/audit rows.
    pub fn new(
        binding: AuthoritativeTransactionBindingV3,
        attribution: ChangelogAttributionV3,
        mutations: Vec<AuthoritativeMutationV3>,
    ) -> Result<Self, ChangelogV3Error> {
        if binding.history_incarnation == 0 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let expected = match binding.predecessor {
            Some(previous) => previous.checked_next(),
            None => ChangelogTransactionSequence::new(1),
        };
        if expected != Some(binding.sequence) {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        attribution.validate(binding, mutations.is_empty())?;
        if mutations.len() > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if mutations.windows(2).any(|pair| {
            (pair[0].namespace(), pair[0].key()) >= (pair[1].namespace(), pair[1].key())
        }) {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let receipt = Self {
            binding,
            attribution,
            mutations,
        };
        if receipt.encoded_len()? > super::frame::MAX_RECEIPT_BYTES
            || receipt.transition_count() > MAX_STAGED_COMMANDS as u64
        {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        Ok(receipt)
    }

    /// Exact checked immutable transaction binding.
    #[must_use]
    pub const fn binding(&self) -> AuthoritativeTransactionBindingV3 {
        self.binding
    }

    /// Closed attribution tag.
    #[must_use]
    pub const fn attribution(&self) -> ChangelogAttributionV3 {
        self.attribution
    }

    /// Strictly namespace/key-ordered immutable net mutations.
    #[must_use]
    pub fn mutations(&self) -> &[AuthoritativeMutationV3] {
        &self.mutations
    }

    /// Logical transitions charged to the independent unsplit frame ceiling.
    #[must_use]
    pub fn transition_count(&self) -> u64 {
        let row = self.binding;
        let app = row.covered_frontier.application().map_or(0, |v| v.get())
            - row
                .predecessor_frontier
                .application()
                .map_or(0, |v| v.get());
        let admin = row.covered_frontier.administration().map_or(0, |v| v.get())
            - row
                .predecessor_frontier
                .administration()
                .map_or(0, |v| v.get());
        match self.attribution {
            ChangelogAttributionV3::JournaledApplicationGroup => app,
            ChangelogAttributionV3::JournaledServiceAudit => admin,
            ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup => {
                if app > 0 {
                    app
                } else {
                    admin
                }
            }
            _ => 1,
        }
    }

    /// Complete receipt size, including its fixed header and checksum.
    pub fn encoded_len(&self) -> Result<usize, ChangelogV3Error> {
        self.mutations
            .iter()
            .try_fold(160_usize, |bytes, mutation| {
                bytes
                    .checked_add(mutation.encoded_len())
                    .ok_or(ChangelogV3Error::LimitExceeded)
            })
    }
}

impl std::fmt::Debug for AuthoritativeTransactionV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeTransactionV3([redacted])")
    }
}
