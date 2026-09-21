//! Bounded construction of a private follower file. This owner has no serving,
//! publication, source-writer, or follower-applier capability.
use super::{RedbBootstrapMaterializationInput, path_guard::PinnedDirectory};
use crate::error::storage_error;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition, TableHandle,
};
use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1,
    ReplicationBootstrapManifestV1 as Manifest, ReplicationBootstrapProgressV1 as Progress,
    ReplicationBootstrapTranscriptV3 as Transcript, ReplicationFollowerStateV3,
    StartupValidationInputs, StorageError, StorageErrorKind, proto_codec::*,
};
use std::{
    ffi::OsStr,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

#[path = "bootstrap_catalog.rs"]
mod catalog;
pub use catalog::RedbBootstrapCatalogSession;

#[path = "bootstrap_projection.rs"]
mod projection;

#[path = "bootstrap_publication.rs"]
mod publication;
pub use publication::RedbPublishedBootstrapCandidate;

const PROGRESS: TableDefinition<u8, &[u8]> = TableDefinition::new("bootstrap_receipt_v1");
const FILE: &str = "follower.redb";
const LOCK: &str = "construction.lock";
const CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Exclusive, resumable owner of an unpublished follower construction.
pub struct RedbBootstrapMaterializer {
    database: Database,
    owner: ConstructionOwner,
    transfer: RedbBootstrapMaterializationInput,
    transcript: Transcript,
    progress: Progress,
    sealed: bool,
    failed: bool,
}

struct ConstructionOwner {
    directory: PinnedDirectory,
    file: File,
    lock: File,
    path: PathBuf,
}
impl ConstructionOwner {
    fn verify(&self) -> Result<(), StorageError> {
        self.directory.verify_private()?;
        if !self
            .directory
            .regular_file_matches(OsStr::new(FILE), &self.file)?
            || !self
                .directory
                .regular_file_matches(OsStr::new(LOCK), &self.lock)?
            || self.directory.regular_file_length(OsStr::new(LOCK))? != Some(0)
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

impl RedbBootstrapMaterializer {
    /// Creates a new private construction; no existing path is overwritten.
    pub fn create(
        path: &Path,
        transfer: RedbBootstrapMaterializationInput,
    ) -> Result<Self, StorageError> {
        let parent = PinnedDirectory::open(path.parent().ok_or_else(corrupt)?)?;
        let directory = parent.create_private_child(path.file_name().ok_or_else(corrupt)?)?;
        crash_edge("initial-directory-created");
        let lock = directory.create_new_file(OsStr::new(LOCK))?.into_std();
        lock.try_lock().map_err(unavailable)?;
        lock.sync_all().map_err(unavailable)?;
        let database_path = path.join(FILE);
        let preflight = crate::preflight_durable_format_path(&database_path).map_err(invalid)?;
        crate::format_preflight::publish_initialized_current_marker_with_media(
            &crate::media::RealJournalMedia,
            &database_path,
            preflight,
        )
        .map_err(invalid)?;
        crash_edge("initial-marker-published");
        drop(directory.create_new_file(OsStr::new(FILE))?);
        crash_edge("initial-file-created");
        let file = directory.open_file_read_write(OsStr::new(FILE))?.into_std();
        let database = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create_file(file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)?;
        crash_edge("initial-engine-created");
        let transcript = Transcript::new(transfer.manifest().fence());
        let progress = transcript
            .checkpoint(transfer.manifest())
            .map_err(invalid)?;
        let mut write = database.begin_write().map_err(unavailable)?;
        immediate(&mut write)?;
        crate::layout::create_all_tables(&write).map_err(invalid)?;
        write
            .open_table(crate::changelog_v3_activation::HISTORY)
            .map_err(invalid)?;
        write
            .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .map_err(invalid)?;
        write
            .open_table(PROGRESS)
            .map_err(invalid)?
            .insert(0, progress.encode().map_err(invalid)?.as_slice())
            .map_err(unavailable)?;
        crash_edge("initial-progress-staged");
        write.commit().map_err(unavailable)?;
        crash_edge("initial-progress-committed");
        directory.sync()?;
        let value = Self {
            database,
            owner: ConstructionOwner {
                directory,
                file,
                lock,
                path: database_path,
            },
            transfer,
            transcript,
            progress,
            sealed: false,
            failed: false,
        };
        value.owner.verify()?;
        Ok(value)
    }

    /// Reopens the exact construction using a complete durable
    /// transfer progress boundary. The resume point and inserted authority share one transaction.
    pub fn open(
        path: &Path,
        transfer: RedbBootstrapMaterializationInput,
    ) -> Result<Self, StorageError> {
        Self::open_with_cancellation(path, transfer, None)
    }

    /// Reopens with cancellation checked between pages of a sealed candidate's
    /// complete authority comparison. An interrupted check never grants a candidate.
    pub fn open_cancellable(
        path: &Path,
        transfer: RedbBootstrapMaterializationInput,
        cancellation: &std::sync::atomic::AtomicBool,
    ) -> Result<Self, StorageError> {
        Self::open_with_cancellation(path, transfer, Some(cancellation))
    }

    fn open_with_cancellation(
        path: &Path,
        transfer: RedbBootstrapMaterializationInput,
        cancellation: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Self, StorageError> {
        check_cancellation(cancellation)?;
        let directory = PinnedDirectory::open(path)?;
        directory.verify_private()?;
        if directory.regular_file_length(OsStr::new(LOCK))? != Some(0) {
            return Err(corrupt());
        }
        let lock = directory.open_file_read_write(OsStr::new(LOCK))?.into_std();
        lock.try_lock().map_err(unavailable)?;
        let database_path = path.join(FILE);
        if crate::preflight_durable_format_path(&database_path).map_err(invalid)?
            != crate::RedbDurableFormatPreflight::OpenCurrent
        {
            return Err(corrupt());
        }
        let file = directory.open_file_read_write(OsStr::new(FILE))?.into_std();
        let database = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create_file(file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)?;
        let read = database.begin_read().map_err(unavailable)?;
        let mut tables = std::collections::BTreeSet::new();
        for table in read.list_tables().map_err(invalid)?.take(N::ALL.len() + 2) {
            if table.name() != PROGRESS.name() && !N::ALL.iter().any(|n| n.table() == table.name())
            {
                return Err(corrupt());
            }
            tables.insert(table.name().to_owned());
        }
        if read
            .list_multimap_tables()
            .map_err(invalid)?
            .next()
            .is_some()
        {
            return Err(corrupt());
        }
        let sealed = !tables.remove(PROGRESS.name());
        crate::store::v3_layout::exact_current_tables(&tables, &Default::default())?;
        let (progress, transcript) = if sealed {
            let transcript = verify_authority(&database, &transfer, cancellation)?;
            (
                transcript
                    .checkpoint(transfer.manifest())
                    .map_err(invalid)?,
                transcript,
            )
        } else {
            let table = read.open_table(PROGRESS).map_err(invalid)?;
            if table.len().map_err(unavailable)? != 1 {
                return Err(corrupt());
            }
            let bytes = table.get(0).map_err(unavailable)?.ok_or_else(corrupt)?;
            let progress = Progress::decode(bytes.value()).map_err(invalid)?;
            let page = if progress.page_count() == 0 {
                None
            } else {
                Some(transfer.read_page(progress.page_count())?)
            };
            let transcript =
                Transcript::resume(transfer.manifest(), progress.clone(), page.as_ref())
                    .map_err(invalid)?;
            (progress, transcript)
        };
        drop(read);
        let value = Self {
            database,
            owner: ConstructionOwner {
                directory,
                file,
                lock,
                path: database_path,
            },
            transfer,
            transcript,
            progress,
            sealed,
            failed: false,
        };
        value.owner.verify()?;
        Ok(value)
    }

    /// Exact last locally durable copied page, never a replication acknowledgement.
    #[must_use]
    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    /// Copies at most one bounded page. Cancellation between calls releases all
    /// process-local resources on drop; reopening resumes at the next page.
    pub fn copy_next_page(&mut self) -> Result<Option<Progress>, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.failed = true;
        self.owner.verify()?;
        if self.progress.page_count() == self.transfer.manifest().page_count() {
            self.failed = false;
            return Ok(None);
        }
        let page = self.transfer.read_page(self.progress.page_count() + 1)?;
        self.transcript.observe(&page).map_err(invalid)?;
        let progress = self
            .transcript
            .checkpoint(self.transfer.manifest())
            .map_err(invalid)?;
        let mut write = self.database.begin_write().map_err(unavailable)?;
        immediate(&mut write)?;
        for row in page.rows() {
            let mutation =
                AuthoritativeMutationV3::put(row.namespace(), row.key(), None, row.value())
                    .map_err(invalid)?;
            crate::changelog_v3_write::check_predecessor(&write, &mutation)?;
            crate::changelog_v3_write::apply_mutation(&write, &mutation)?;
        }
        write
            .open_table(PROGRESS)
            .map_err(invalid)?
            .insert(0, progress.encode().map_err(invalid)?.as_slice())
            .map_err(unavailable)?;
        crash_edge("rows-staged");
        self.owner.verify()?;
        write.commit().map_err(unavailable)?;
        crash_edge("rows-committed");
        self.owner.verify()?;
        self.progress = progress;
        self.failed = false;
        Ok(Some(self.progress.clone()))
    }

    /// Seals exact authority and existing follower roots, removes the temporary
    /// table atomically, and compares all authority against the source manifest.
    /// Complete startup validation and derived-state rebuild are still required.
    pub fn finish(self) -> Result<RedbBootstrapCandidate, StorageError> {
        self.finish_with_cancellation(None)
    }

    /// Seals and compares the same authority with cancellation between bounded
    /// pages. Cancellation may leave a sealed private file for exact resume.
    pub fn finish_cancellable(
        self,
        cancellation: &std::sync::atomic::AtomicBool,
    ) -> Result<RedbBootstrapCandidate, StorageError> {
        self.finish_with_cancellation(Some(cancellation))
    }

    fn finish_with_cancellation(
        mut self,
        cancellation: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<RedbBootstrapCandidate, StorageError> {
        check_cancellation(cancellation)?;
        if self.failed {
            return Err(corrupt());
        }
        self.owner.verify()?;
        self.transcript
            .verify_manifest(self.transfer.manifest())
            .map_err(invalid)?;
        if !self.sealed {
            let mut write = self.database.begin_write().map_err(unavailable)?;
            immediate(&mut write)?;
            install_roots(&write, self.transfer.manifest())?;
            write.delete_table(PROGRESS).map_err(invalid)?;
            crate::changelog_v3_roots::validate_retained_history_for_write(&write)?;
            crash_edge("seal-staged");
            write.commit().map_err(unavailable)?;
            crash_edge("seal-committed");
        }
        verify_authority(&self.database, &self.transfer, cancellation)?;
        self.owner.verify()?;
        Ok(RedbBootstrapCandidate {
            catalog_session: None,
            database: self.database,
            owner: self.owner,
            transfer: self.transfer,
            failed: false,
        })
    }
}

/// Complete physical authority in a private file; never a serving capability.
pub struct RedbBootstrapCandidate {
    catalog_session: Option<riffdb_storage_api::OpenSessionId>,
    failed: bool,
    database: Database,
    owner: ConstructionOwner,
    transfer: RedbBootstrapMaterializationInput,
}
impl RedbBootstrapCandidate {
    /// Exact source fence and transfer manifest.
    #[must_use]
    pub fn manifest(&self) -> Manifest {
        self.transfer.manifest()
    }
    /// Runs the unchanged complete structural and catalog scrub. Missing or
    /// corrupt derived state is refused; this method never silently repairs it.
    pub fn validate(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<RedbValidatedBootstrapCandidate, StorageError> {
        self.validate_with_cancellation(inputs, None)
    }

    /// Full validation with cancellation checked between bounded evidence reads.
    pub fn validate_cancellable(
        self,
        inputs: StartupValidationInputs,
        flag: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<RedbValidatedBootstrapCandidate, StorageError> {
        self.validate_with_cancellation(inputs, Some(flag))
    }

    fn validate_with_cancellation(
        self,
        inputs: StartupValidationInputs,
        flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<RedbValidatedBootstrapCandidate, StorageError> {
        self.owner.verify()?;
        if self.failed {
            return Err(corrupt());
        }
        let Self {
            database,
            owner,
            transfer,
            ..
        } = self;
        drop(database);
        let scrub = crate::startup::RedbOfflineIntegrityScrub::from_inputs(&owner.path, inputs);
        let scrub = match flag {
            Some(flag) => scrub.with_cancellation(flag),
            None => scrub,
        };
        scrub.run_follower()?;
        owner.verify()?;
        Ok(RedbValidatedBootstrapCandidate { owner, transfer })
    }
}

/// A scrubbed private candidate retaining construction exclusion. Maintenance
/// publication has a separate owner; no live path or applier is exposed here.
pub struct RedbValidatedBootstrapCandidate {
    owner: ConstructionOwner,
    transfer: RedbBootstrapMaterializationInput,
}
impl RedbValidatedBootstrapCandidate {
    /// Exact immutable manifest. The retained owner prevents concurrent rebuild.
    pub fn manifest(&self) -> Manifest {
        self.transfer.manifest()
    }
    /// Checks that the private candidate still belongs to this construction.
    pub fn verify_private_identity(&self) -> Result<(), StorageError> {
        self.owner.verify()
    }
}

fn install_roots(write: &redb::WriteTransaction, manifest: Manifest) -> Result<(), StorageError> {
    let history = manifest.fence().history();
    let catalog = history.lineage().catalog_digest();
    let encoded_catalog = if catalog == AuthoritativeStateCatalogV1.digest() {
        encode_authoritative_state_catalog_v1(AuthoritativeStateCatalogV1)
    } else if catalog == riffdb_storage_api::AuthoritativeStateCatalogV2.digest() {
        encode_authoritative_state_catalog_v2(riffdb_storage_api::AuthoritativeStateCatalogV2)
    } else {
        return Err(corrupt());
    };
    let roots = [
        (N::AuthoritativeStateCatalog, encoded_catalog),
        (
            N::LeadershipEpoch,
            encode_leadership_epoch_v1(history.lineage().leadership_epoch()),
        ),
        (
            N::ChangelogHistoryState,
            encode_changelog_history_state_v3(history),
        ),
        (
            N::NextChangelogTransaction,
            encode_changelog_transaction_allocator_v3(history.expected_allocator()),
        ),
        (
            N::ReplicationFollowerState,
            encode_replication_follower_state_v3(
                ReplicationFollowerStateV3::attached(history.lineage(), history.tail(), None)
                    .map_err(invalid)?,
            ),
        ),
    ];
    let mut meta = write.open_table(crate::layout::META).map_err(invalid)?;
    for (namespace, value) in roots {
        if meta
            .insert(
                namespace.metadata_key().ok_or_else(corrupt)?,
                value.map_err(invalid)?.as_bytes(),
            )
            .map_err(unavailable)?
            .is_some()
        {
            return Err(corrupt());
        }
    }
    Ok(())
}

fn verify_authority(
    database: &Database,
    transfer: &RedbBootstrapMaterializationInput,
    cancellation: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Transcript, StorageError> {
    check_cancellation(cancellation)?;
    use riffdb_storage_api::AuthoritativeStateStepV3 as Step;
    let root = Arc::new(crate::checkpoint_root::CheckpointRoot::new(
        database.begin_read().map_err(unavailable)?,
        0,
    ));
    let mut input = crate::changelog_v3_cursor::state::open_attached(
        root,
        transfer.manifest().fence().history(),
    )
    .map_err(invalid)?;
    let mut transcript = Transcript::new(transfer.manifest().fence());
    for ordinal in 1..=transfer.manifest().page_count() {
        check_cancellation(cancellation)?;
        let page = transfer.read_page(ordinal)?;
        transcript.observe(&page).map_err(invalid)?;
        for expected in page.rows() {
            match input.next_item().map_err(invalid)? {
                Some(Step::Row(actual)) if actual == *expected => {}
                _ => return Err(corrupt()),
            }
        }
        if page.ends_namespace()
            && input.next_item().map_err(invalid)? != Some(Step::EndNamespace(page.namespace()))
        {
            return Err(corrupt());
        }
    }
    if input.next_item().map_err(invalid)?.is_some() {
        return Err(corrupt());
    }
    transcript
        .verify_manifest(transfer.manifest())
        .map_err(invalid)?;
    check_cancellation(cancellation)?;
    Ok(transcript)
}

fn immediate(write: &mut redb::WriteTransaction) -> Result<(), StorageError> {
    write.set_two_phase_commit(true);
    write
        .set_durability(Durability::Immediate)
        .map_err(unavailable)
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn invalid<T>(_: T) -> StorageError {
    corrupt()
}
fn unavailable<T>(_: T) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn crash_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE")
        .ok()
        .as_deref()
        == Some(_edge)
    {
        std::process::exit(93);
    }
}

fn check_cancellation(flag: Option<&std::sync::atomic::AtomicBool>) -> Result<(), StorageError> {
    if flag.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire)) {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(())
}
