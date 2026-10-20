//! Atomic Empty classification and the sole database initialization owner.

use super::*;

impl DatabaseInitializationPort for RedbStore {
    fn initialize_database(
        &mut self,
        candidate: DatabaseId,
    ) -> Result<DatabaseInitializationResult, StorageError> {
        self.initialize_source(candidate, SourceCreation::Current)
    }
}

#[derive(Clone, Copy)]
enum SourceCreation {
    Current,
    #[cfg(any(test, feature = "test-fixtures"))]
    ProjectionSourceFixtureV3,
    #[cfg(any(test, feature = "test-fixtures"))]
    LegacyFixture,
}

/// The exact transaction classified Empty under its existing mutation lease.
/// Fields are private to this owner; a caller cannot supply another transaction.
#[cfg_attr(not(any(test, feature = "test-fixtures")), allow(dead_code))]
pub(crate) struct ProvenEmptyInitializationV1<'engine, 'transaction> {
    owner: &'engine crate::engine_owner::EngineOwner<Database>,
    transaction: &'transaction WriteTransaction,
    database_id: DatabaseId,
}
#[cfg_attr(not(any(test, feature = "test-fixtures")), allow(dead_code))]
impl<'engine, 'transaction> ProvenEmptyInitializationV1<'engine, 'transaction> {
    pub(crate) fn owner(&self) -> &'engine crate::engine_owner::EngineOwner<Database> {
        self.owner
    }
    pub(crate) fn transaction(&self) -> &'transaction WriteTransaction {
        self.transaction
    }
    pub(crate) const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
}

impl RedbStore {
    /// Explicit private genuine V3 fixture; never an existing-source upgrade.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) fn initialize_projection_source_fixture(
        &mut self,
        candidate: DatabaseId,
    ) -> Result<DatabaseInitializationResult, StorageError> {
        self.initialize_source(candidate, SourceCreation::ProjectionSourceFixtureV3)
    }

    /// Explicit frozen pre-V3 fixture construction. Production initialization
    /// has no catalog selector and always creates complete V2 source state.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) fn initialize_legacy_fixture(
        &mut self,
        candidate: DatabaseId,
    ) -> Result<DatabaseInitializationResult, StorageError> {
        self.initialize_source(candidate, SourceCreation::LegacyFixture)
    }

    fn initialize_source(
        &mut self,
        candidate: DatabaseId,
        creation: SourceCreation,
    ) -> Result<DatabaseInitializationResult, StorageError> {
        let _lease = self.acquire_mutation_lease()?;
        self.ensure_writable()?;

        let mut transaction = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;

        match classify_write_layout(&transaction)? {
            LayoutState::Initialized
            | LayoutState::InitializedWithoutValidatedPrefixEntityHeads => {
                #[cfg(any(test, feature = "test-fixtures"))]
                if matches!(creation, SourceCreation::ProjectionSourceFixtureV3) {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
                let winner = read_identity_from_write_transaction(&transaction)?;
                transaction.abort().map_err(precommit_storage_error)?;
                Ok(DatabaseInitializationResult::ConcurrentWinner(winner))
            }
            LayoutState::Empty => {
                let fresh_source: Option<crate::native_source_fresh::StagedEmptyV3<'_>> =
                    match creation {
                        SourceCreation::Current => {
                            create_all_tables(&transaction).map_err(table_error)?;
                            write_initial_metadata(&transaction, candidate)?;
                            crate::changelog_v3_activation::stage_new_source(
                                &mut transaction,
                                candidate,
                            )?;
                            None
                        }
                        #[cfg(any(test, feature = "test-fixtures"))]
                        SourceCreation::LegacyFixture => {
                            create_all_tables(&transaction).map_err(table_error)?;
                            write_initial_metadata(&transaction, candidate)?;
                            None
                        }
                        #[cfg(any(test, feature = "test-fixtures"))]
                        SourceCreation::ProjectionSourceFixtureV3 => {
                            let empty = ProvenEmptyInitializationV1 {
                                owner: &self.shared.database,
                                transaction: &transaction,
                                database_id: candidate,
                            };
                            Some(crate::native_source_fresh::stage_empty_v3(&empty)?)
                        }
                    };
                if let Some(controller) = &self.shared.test_controller {
                    controller.before_commit(RedbTestOperation::Initialization)?;
                }
                self.shared.commit_durable(transaction)?;
                if let Some(fresh_source) = fresh_source
                    && let Err(error) = fresh_source.install_after_durable_commit()
                {
                    self.fence_writes();
                    return Err(error);
                }
                if matches!(creation, SourceCreation::Current) {
                    crate::changelog_v3_activation::activation_edge("committed");
                }
                if let Some(controller) = &self.shared.test_controller
                    && let Err(error) = controller.after_commit(RedbTestOperation::Initialization)
                {
                    self.fence_writes();
                    return Err(error);
                }
                Ok(DatabaseInitializationResult::Installed(candidate))
            }
        }
    }
}

fn write_initial_metadata(
    transaction: &redb::WriteTransaction,
    database_id: DatabaseId,
) -> Result<(), StorageError> {
    let format = encode_storage_format_version_v1(StorageFormatVersion::V2)
        .map_err(crate::error::codec_error)?;
    // This predecessor exists only inside the initial transaction. The Empty
    // owner installs explicit V2 source admission and the current registry before
    // committing; reopening never infers admission from this predecessor.
    let registry = encode_record_registry_v2(crate::changelog_v3_activation::PRE_V3_REGISTRY)
        .map_err(crate::error::codec_error)?;
    let identity = encode_database_identity_v1(database_id).map_err(crate::error::codec_error)?;
    let application =
        encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::initial())
            .map_err(crate::error::codec_error)?;
    let administration = encode_administration_sequence_allocator_v1(
        riffdb_storage_api::AdministrationSequenceAllocator::initial(),
    )
    .map_err(crate::error::codec_error)?;
    let history = encode_history_incarnation_v1(HISTORY_INCARNATION_INITIAL)
        .map_err(crate::error::codec_error)?;
    let rotation = riffdb_storage_api::ChangelogV2RotationReceipt::new(
        database_id,
        HISTORY_INCARNATION_INITIAL,
        DualFrontier::INITIAL,
        [0; 32],
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let rotation = riffdb_storage_api::encode_changelog_v2_rotation_receipt_v1(rotation)
        .map_err(crate::error::codec_error)?;

    let mut table = transaction.open_table(META).map_err(table_error)?;
    table
        .insert(META_FORMAT_VERSION, format.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_DATABASE_ID, identity.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_APPLICATION_SEQUENCE, application.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_ADMINISTRATION_SEQUENCE, administration.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_RECORD_REGISTRY, registry.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_HISTORY_INCARNATION, history.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_CHANGELOG_V2_ROTATION_RECEIPT, rotation.as_bytes())
        .map_err(precommit_storage_error)?;
    // Fresh databases never hold legacy INDEX_EPOCHS rows.
    table
        .insert(META_INDEX_EPOCH_ROWS_REPAIRED, [1u8].as_slice())
        .map_err(precommit_storage_error)?;
    Ok(())
}
