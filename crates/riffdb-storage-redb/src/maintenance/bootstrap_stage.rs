//! Offline transfer database. Pages and progress share one Immediate commit;
//! nothing here grants a live database, application write, or readiness port.
use super::path_guard::PinnedDirectory;
use crate::error::storage_error;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition, TableHandle,
};
use riffdb_storage_api::{
    MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES, MAX_REPLICATION_BOOTSTRAP_PROGRESS_BYTES,
    ReplicationBootstrapManifestV1 as Manifest, ReplicationBootstrapPageV3 as Page,
    ReplicationBootstrapProgressV1 as Progress, ReplicationBootstrapTranscriptV3 as Transcript,
    StorageError, StorageErrorKind,
};
use std::{ffi::OsStr, fs::File, path::Path};

// Scratch tables contain only the admitted external V1 artifact bytes. They
// never enter the serving database's durable metadata registry.
const PAGES: TableDefinition<u32, &[u8]> = TableDefinition::new("bootstrap_pages_v1");
const RECEIPT: TableDefinition<u8, &[u8]> = TableDefinition::new("bootstrap_receipt_v1");
const FILE: &str = "transfer.redb";
const CACHE_BYTES: usize = 64 * 1024 * 1024;

#[path = "bootstrap_build.rs"]
mod build;
pub(super) use build::SourceBuild;

/// Exclusive, move-only owner of a private offline bootstrap transfer.
pub struct RedbBootstrapStage {
    database: Database,
    file: File,
    directory: PinnedDirectory,
    manifest: Manifest,
    progress: Progress,
    transcript: Transcript,
    failed: bool,
    receiver_repository:
        Option<std::sync::Arc<super::bootstrap_receiver_repository::ReceiverRepositoryInner>>,
}

impl RedbBootstrapStage {
    pub(super) fn with_receiver_repository(
        mut self,
        repository: std::sync::Arc<super::bootstrap_receiver_repository::ReceiverRepositoryInner>,
    ) -> Self {
        self.receiver_repository = Some(repository);
        self
    }
    pub(super) fn matches_repository_file(
        &self,
        directory: &PinnedDirectory,
    ) -> Result<bool, StorageError> {
        self.verify_identity()?;
        directory.regular_file_matches(OsStr::new(FILE), &self.file)
    }

    /// Creates a new private stage. Existing directories are never overwritten.
    pub fn create(path: &Path, manifest: Manifest) -> Result<Self, StorageError> {
        let (database, file, directory) = create_database(path)?;
        let transcript = Transcript::new(manifest.fence());
        let progress = transcript.checkpoint(manifest).map_err(invalid)?;
        let encoded = progress.encode().map_err(invalid)?;
        let mut write = database.begin_write().map_err(unavailable)?;
        write.set_two_phase_commit(true);
        write
            .set_durability(Durability::Immediate)
            .map_err(unavailable)?;
        write.open_table(PAGES).map_err(unavailable)?;
        write
            .open_table(RECEIPT)
            .map_err(unavailable)?
            .insert(0, encoded.as_slice())
            .map_err(unavailable)?;
        crash_edge("initial-progress-staged");
        write.commit().map_err(unavailable)?;
        crash_edge("initial-progress-committed");
        directory.sync()?;
        let value = Self {
            database,
            file,
            directory,
            manifest,
            progress,
            transcript,
            failed: false,
            receiver_repository: None,
        };
        value.verify_identity()?;
        Ok(value)
    }

    /// Recovers one durable page boundary, bounded by one page and one receipt.
    /// Full inventory verification remains mandatory before materialization.
    pub fn open(path: &Path, expected: Manifest) -> Result<Self, StorageError> {
        Self::open_checked(path, Some(expected))
    }

    /// Recovers the locally stored manifest and page boundary under engine
    /// exclusion. This does not trust a peer, prove completeness, or grant
    /// publication: composition must check configured lineage before reconnect.
    pub fn recover(path: &Path) -> Result<Self, StorageError> {
        Self::open_checked(path, None)
    }

    fn open_checked(path: &Path, expected: Option<Manifest>) -> Result<Self, StorageError> {
        let directory = PinnedDirectory::open(path)?;
        directory.verify_private()?;
        if directory
            .regular_file_length(OsStr::new(FILE))?
            .is_none_or(|n| n == 0)
        {
            return Err(corrupt());
        }
        let file = directory.open_file_read_write(OsStr::new(FILE))?.into_std();
        let database = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create_file(file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)?;
        let read = database.begin_read().map_err(unavailable)?;
        if read
            .list_multimap_tables()
            .map_err(invalid)?
            .next()
            .is_some()
        {
            return Err(corrupt());
        }
        let mut table_count = 0;
        for table in read.list_tables().map_err(invalid)?.take(3) {
            if table.name() != PAGES.name() && table.name() != RECEIPT.name() {
                return Err(corrupt());
            }
            table_count += 1;
        }
        if table_count != 2 {
            return Err(corrupt());
        }
        let receipt = read.open_table(RECEIPT).map_err(invalid)?;
        if receipt.len().map_err(unavailable)? != 1 {
            return Err(corrupt());
        }
        let encoded = receipt.get(0).map_err(unavailable)?.ok_or_else(corrupt)?;
        if encoded.value().len() > MAX_REPLICATION_BOOTSTRAP_PROGRESS_BYTES {
            return Err(corrupt());
        }
        let progress = Progress::decode(encoded.value()).map_err(invalid)?;
        let manifest = progress.manifest();
        if expected.is_some_and(|expected| expected != manifest) {
            return Err(corrupt());
        }
        let pages = read.open_table(PAGES).map_err(invalid)?;
        if pages.len().map_err(unavailable)? != u64::from(progress.page_count()) {
            return Err(corrupt());
        }
        let last = if progress.page_count() == 0 {
            None
        } else {
            let bytes = pages
                .get(progress.page_count())
                .map_err(unavailable)?
                .ok_or_else(corrupt)?;
            Some(Page::decode(bytes.value()).map_err(invalid)?)
        };
        let transcript =
            Transcript::resume(manifest, progress.clone(), last.as_ref()).map_err(invalid)?;
        drop(pages);
        drop(encoded);
        drop(receipt);
        drop(read);
        let value = Self {
            database,
            file,
            directory,
            manifest,
            progress,
            transcript,
            failed: false,
            receiver_repository: None,
        };
        value.verify_identity()?;
        Ok(value)
    }

    /// Last locally durable transfer boundary. This is not a tail acknowledgement.
    #[must_use]
    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    fn verify_identity(&self) -> Result<(), StorageError> {
        if let Some(repository) = &self.receiver_repository {
            repository.verify()?;
        }
        self.directory.verify_private()?;
        if !self
            .directory
            .regular_file_matches(OsStr::new(FILE), &self.file)?
        {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Atomically persists one exact next page and its progress. An exact retry
    /// of the last durable page is read-only. Any refusal fuses this handle.
    pub fn append(&mut self, encoded: &[u8]) -> Result<Progress, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.failed = true;
        self.verify_identity()?;
        if encoded.len() > MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES {
            return Err(corrupt());
        }
        let page = Page::decode(encoded).map_err(invalid)?;
        if page.ordinal() == self.progress.page_count() {
            let read = self.database.begin_read().map_err(unavailable)?;
            let pages = read.open_table(PAGES).map_err(invalid)?;
            if pages
                .get(page.ordinal())
                .map_err(unavailable)?
                .ok_or_else(corrupt)?
                .value()
                != encoded
            {
                return Err(corrupt());
            }
            self.verify_identity()?;
            self.failed = false;
            return Ok(self.progress.clone());
        }
        self.transcript.observe(&page).map_err(invalid)?;
        let progress = self.transcript.checkpoint(self.manifest).map_err(invalid)?;
        let receipt = progress.encode().map_err(invalid)?;
        let mut write = self.database.begin_write().map_err(unavailable)?;
        write.set_two_phase_commit(true);
        write
            .set_durability(Durability::Immediate)
            .map_err(unavailable)?;
        if write
            .open_table(PAGES)
            .map_err(unavailable)?
            .insert(page.ordinal(), encoded)
            .map_err(unavailable)?
            .is_some()
        {
            return Err(corrupt());
        }
        write
            .open_table(RECEIPT)
            .map_err(unavailable)?
            .insert(0, receipt.as_slice())
            .map_err(unavailable)?;
        crash_edge("before-commit");
        self.verify_identity()?;
        write.commit().map_err(unavailable)?;
        crash_edge("after-commit");
        self.verify_identity()?;
        self.progress = progress;
        self.failed = false;
        Ok(self.progress.clone())
    }

    /// Begins cancellable verification under the retained exclusive engine lock.
    /// Each advance rereads at most one page. Drop releases all local handles.
    pub fn begin_verification(mut self) -> Result<RedbBootstrapVerification, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.verify_identity()?;
        self.transcript
            .verify_manifest(self.manifest)
            .map_err(invalid)?;
        let transcript = Transcript::new(self.manifest.fence());
        Ok(RedbBootstrapVerification {
            stage: self,
            transcript,
            verified_pages: 0,
            complete: false,
            failed: false,
        })
    }

    /// Rereads every page under the retained exclusive engine lock. Async
    /// composition must use begin_verification and schedule bounded advances.
    pub fn verify_complete(self) -> Result<RedbVerifiedBootstrapTransfer, StorageError> {
        let mut verification = self.begin_verification()?;
        while !verification.advance()? {}
        verification.finish()
    }

    /// Releases only a bounded materialization input from a complete durable
    /// progress boundary. This does not assert that earlier pages were reread
    /// after restart. The materializer must verify the complete constructed
    /// authority against the manifest before producing a candidate.
    pub fn into_materialization_input(
        mut self,
    ) -> Result<RedbBootstrapMaterializationInput, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.verify_identity()?;
        self.transcript
            .verify_manifest(self.manifest)
            .map_err(invalid)?;
        Ok(RedbBootstrapMaterializationInput { stage: self })
    }

    fn read_page(&self, ordinal: u32) -> Result<Page, StorageError> {
        self.verify_identity()?;
        if ordinal == 0 || ordinal > self.progress.page_count() {
            return Err(corrupt());
        }
        let read = self.database.begin_read().map_err(unavailable)?;
        let pages = read.open_table(PAGES).map_err(invalid)?;
        let value = pages
            .get(ordinal)
            .map_err(unavailable)?
            .ok_or_else(corrupt)?;
        let page = Page::decode(value.value()).map_err(invalid)?;
        if page.ordinal() != ordinal {
            return Err(corrupt());
        }
        self.verify_identity()?;
        Ok(page)
    }
}

/// Move-only verification owner. No artifact or publication authority can be
/// released before every page and the exact manifest have been checked.
pub struct RedbBootstrapVerification {
    stage: RedbBootstrapStage,
    transcript: Transcript,
    verified_pages: u32,
    complete: bool,
    failed: bool,
}
impl RedbBootstrapVerification {
    /// Reads at most one bounded page. Any refusal fuses this owner.
    pub fn advance(&mut self) -> Result<bool, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.failed = true;
        self.stage.verify_identity()?;
        if !self.complete {
            if self.verified_pages < self.stage.manifest.page_count() {
                let ordinal = self.verified_pages.checked_add(1).ok_or_else(corrupt)?;
                self.transcript
                    .observe(&self.stage.read_page(ordinal)?)
                    .map_err(invalid)?;
                self.verified_pages = ordinal;
            }
            if self.verified_pages == self.stage.manifest.page_count() {
                self.transcript
                    .verify_manifest(self.stage.manifest)
                    .map_err(invalid)?;
                self.complete = true;
            }
        }
        self.stage.verify_identity()?;
        self.failed = false;
        Ok(self.complete)
    }

    /// Consumes a completed verification. Early finish is always refused.
    pub fn finish(self) -> Result<RedbVerifiedBootstrapTransfer, StorageError> {
        if self.failed || !self.complete {
            return Err(corrupt());
        }
        self.stage.verify_identity()?;
        Ok(RedbVerifiedBootstrapTransfer { stage: self.stage })
    }
}

/// A fully checked transfer, retaining its private directory and exclusive lock.
/// Offline materialization and unchanged startup validation must still follow.
pub struct RedbVerifiedBootstrapTransfer {
    stage: RedbBootstrapStage,
}
impl RedbVerifiedBootstrapTransfer {
    pub(super) fn verify_private_identity(&self) -> Result<(), StorageError> {
        self.stage.verify_identity()
    }
    /// Exact immutable source snapshot covered by this transfer.
    #[must_use]
    pub const fn manifest(&self) -> Manifest {
        self.stage.manifest
    }
    /// One bounded checked page for offline materialization.
    pub fn read_page(&self, ordinal: u32) -> Result<Page, StorageError> {
        self.stage.read_page(ordinal)
    }

    /// Moves the checked artifact into the private construction lane.
    #[must_use]
    pub fn into_materialization_input(self) -> RedbBootstrapMaterializationInput {
        RedbBootstrapMaterializationInput { stage: self.stage }
    }
}

/// Complete durable transfer progress, with bounded per-page reads. This type
/// deliberately grants no complete verification or publication authority.
pub struct RedbBootstrapMaterializationInput {
    stage: RedbBootstrapStage,
}
impl RedbBootstrapMaterializationInput {
    /// Exact expected source manifest.
    #[must_use]
    pub const fn manifest(&self) -> Manifest {
        self.stage.manifest
    }
    /// One bounded checked page; full construction verification remains required.
    pub fn read_page(&self, ordinal: u32) -> Result<Page, StorageError> {
        self.stage.read_page(ordinal)
    }
}

fn unavailable<T>(_: T) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn invalid<T>(_: T) -> StorageError {
    corrupt()
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
fn crash_edge(edge: &str) {
    if std::env::var("RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE")
        .ok()
        .as_deref()
        == Some(edge)
    {
        std::process::exit(93);
    }
}
#[cfg(not(test))]
fn crash_edge(_: &str) {}

fn create_database(path: &Path) -> Result<(Database, File, PinnedDirectory), StorageError> {
    let parent = PinnedDirectory::open(path.parent().ok_or_else(corrupt)?)?;
    let directory = parent.create_private_child(path.file_name().ok_or_else(corrupt)?)?;
    crash_edge("initial-directory-created");
    let created = directory.create_new_file(OsStr::new(FILE))?;
    crash_edge("initial-file-created");
    created.sync_all().map_err(unavailable)?;
    drop(created);
    let file = directory.open_file_read_write(OsStr::new(FILE))?.into_std();
    let database = Database::builder()
        .set_cache_size(CACHE_BYTES)
        .create_file(file.try_clone().map_err(unavailable)?)
        .map_err(unavailable)?;
    crash_edge("initial-engine-created");
    Ok((database, file, directory))
}
