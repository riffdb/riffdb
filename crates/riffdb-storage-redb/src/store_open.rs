//! Shared engine construction and mode-specific recovery admission.
use super::*;

#[cfg(test)]
#[path = "store_creation_sync_tests.rs"]
mod creation_sync_tests;

impl RedbStore {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn open_after_format_preflight(
        open_mode: OpenMode,
        path: &Path,
        application_commit_profile: RedbCommitProfile,
        test_controller: Option<RedbTestController>,
        changelog_port: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
        initialize_marker_witness: Option<RedbDurableFormatPreflight>,
        engine_backend: Option<Box<dyn redb::StorageBackend>>,
        journal_media: Arc<dyn JournalMedia>,
    ) -> Result<Self, StorageError> {
        if let Some(witness) = initialize_marker_witness {
            publish_initialized_current_marker_with_media(journal_media.as_ref(), path, witness)
                .map_err(format_preflight_storage_error)?;
        }
        let repair_observed = Arc::new(AtomicBool::new(false));
        let repair_observed_callback = Arc::clone(&repair_observed);
        let refuse_repair = open_mode.is_offline_scrub();
        let mut builder = Builder::new();
        builder.set_cache_size(PRIMARY_REDB_CACHE_SIZE_BYTES);
        builder.set_repair_callback(move |session| {
            repair_observed_callback.store(true, Ordering::Release);
            if refuse_repair {
                session.abort();
            }
            // Bounded progress telemetry only; do not enable quick_repair
            // (quick_repair forces two-phase commit, conflicting with Standard).
            let progress = session.progress();
            let basis_points = ((progress * 10_000.0) as u64).min(10_000);
            LAST_REPAIR_PROGRESS_BPS.store(basis_points, Ordering::Relaxed);
        });
        // The builder configuration above is shared by both arms; only the
        // storage medium differs (ADR-0113 backend-parameterized open).
        // Sidecar durability follows the primary path, not the backend
        // wrapper: a FileBackend kill-barrier still has to observe the
        // sidecar after process death (ADR-0240 two-store crash).
        let derived_in_memory = false;
        let checkpoint_rewrite = Arc::new(crate::checkpoint_rewrite::CheckpointRewrite::default());
        let database = match engine_backend {
            Some(backend) => builder.create_with_backend(
                crate::checkpoint_rewrite::CheckpointRewriteBackend::new(
                    DynStorageBackend(backend),
                    Arc::clone(&checkpoint_rewrite),
                ),
            ),
            None => {
                // Preserve Builder::create's exact opening and exclusive-lock
                // behavior; the wrapper only compares checkpoint commit writes.
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
                let backend = redb::backends::FileBackend::new(file).map_err(database_error)?;
                builder.create_with_backend(
                    crate::checkpoint_rewrite::CheckpointRewriteBackend::new(
                        backend,
                        Arc::clone(&checkpoint_rewrite),
                    ),
                )
            }
        }
        .map_err(database_error)?;
        if refuse_repair && repair_observed.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        if open_mode == OpenMode::Source {
            // redb synchronizes file contents, not first-creation directory
            // publication. Recheck on reopen too: a prior failed sync may have
            // left this otherwise valid file and its CURRENT marker behind.
            journal_media
                .sync_parent_all(path)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        }
        // Source-mode construction cannot reinterpret attached follower state
        // as a writable primary. Refuse before locator installation, journal
        // recovery, or preparation workers. Promotion owns detachment.
        if let OpenMode::PrivateRestore(binding) = &open_mode {
            binding.validate(&database.begin_read().map_err(transaction_error)?)?;
        }
        if let OpenMode::PrivatePromotion(binding) | OpenMode::ReconciledPromotion(binding) =
            &open_mode
        {
            binding.validate(&database.begin_read().map_err(transaction_error)?)?;
        }
        let attached = crate::follower_lifecycle::is_attached(
            &database.begin_read().map_err(transaction_error)?,
        )?;
        if attached != open_mode.is_follower() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if matches!(open_mode, OpenMode::Source | OpenMode::OfflineSource) {
            crate::promotion_cutover::require_reconciled_source_open(
                &database.begin_read().map_err(transaction_error)?,
            )?;
        }
        if open_mode.is_follower() {
            follower::validate_open(&database, path, journal_media.as_ref())?;
        }
        let command_segment_preparation =
            crate::command_segment_preparation::CommandSegmentPreparationPool::new(
                crate::command_segment_preparation::CommandSegmentPreparationPool::production_worker_count(),
            )
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let store = Self {
            shared: Arc::new(SharedRedb {
                extent_roots: Mutex::new(Vec::new()),
                extent_scrubs: std::sync::atomic::AtomicUsize::new(0),
                extent_cursor: Mutex::new(crate::store::extent_reclamation::Cursor::default()),
                derived_source_pins: Arc::default(),
                open_mode: open_mode.clone(),
                follower_namespace: Mutex::new(None),
                database,
                checkpoint_rewrite,
                derived: std::sync::OnceLock::new(),
                derived_open: std::sync::Mutex::new(()),
                derived_in_memory,
                path: path.to_path_buf(),
                journal_media,
                application_commit_profile,
                mutation_gate: ExclusiveGate::default(),
                write_fenced: AtomicBool::new(open_mode.is_private_validation() || refuse_repair),
                engine_repaired_at_open: repair_observed.load(Ordering::Acquire)
                    | (!refuse_repair && take_engine_repaired_by_prior_open(path)),
                engine_initialized_at_open: initialize_marker_witness.is_some(),
                bounded_clean_startup: AtomicBool::new(false),
                clean_certificate_startup: AtomicBool::new(false),
                durable_commit_epoch: AtomicU64::new(0),
                source_continuation_epoch: AtomicU64::new(0),
                durable_root_publication: AtomicU64::new(0),
                durable_root_publication_wait: Mutex::new(()),
                durable_root_publication_changed: Condvar::new(),
                durable_read_frontier: RwLock::new(None),
                current_read_root: RwLock::new(None),
                current_read_root_live: AtomicBool::new(false),
                composite_publication: RwLock::new(None),
                private_composite_frontier: Mutex::new(None),
                publication_queue: Mutex::new(PublicationQueue::default()),
                next_publication_ticket: AtomicU64::new(1),
                journal_runtime: Mutex::new(None),
                journal_checkpoint: Mutex::new(None),
                journal_spare: Arc::new(Mutex::new(())),
                journal_sealer: Mutex::new(None),
                history_resolver: std::sync::OnceLock::new(),
                command_segment_preparation,
                test_controller,
                transient_indexes: RwLock::new(TransientIndexState::Dormant),
                unpublished_command_indexes: Mutex::new(UnpublishedCommandIndexes::default()),
                startup_validation_clean: AtomicBool::new(false),
                checkpoint_write_failures: AtomicU64::new(0),
                checkpoint_ignored: [(); 10].map(|()| AtomicU64::new(0)),
                clean_close_write_failures: AtomicU64::new(0),
                clean_close_declined: [(); 11].map(|()| AtomicU64::new(0)),
                outbox_delivering_proven_absent: AtomicBool::new(false),
                transient_index_rebuilds: AtomicU64::new(0),
                transient_index_commit_rows: AtomicU64::new(0),
                fresh_locator_history_fallback_scans: AtomicU64::new(0),
                fresh_locator_history_fallback_rows: AtomicU64::new(0),
                retention_watermark: AtomicU64::new(0),
                journal_checkpoints_finished: std::sync::Mutex::new(0),
                journal_checkpoint_finished: std::sync::Condvar::new(),
                terminal_execution_failure_rows: AtomicU64::new(0),
                checkpoint_count_rows_walked: AtomicU64::new(0),
                changelog_port,
            }),
        };
        // History stubs (ADR-0263 Amendment 3) must resolve for every open-time
        // reader, so an initialized database registers its extents first.
        {
            let transaction = store
                .shared
                .database
                .begin_read()
                .map_err(transaction_error)?;
            // Layout classification itself reads history, so the identity is
            // read directly; an empty or unreadable one is left to the checks below.
            if let Ok(database_id) = read_identity_from_read_transaction(&transaction) {
                store.shared.register_history_resolver(database_id);
            }
        }
        if open_mode.is_source() {
            store.ensure_current_storage_format()?;
            store.install_command_locator_tables()?;
            store.recover_durability_journal()?;
        }
        if open_mode == OpenMode::OfflineSource {
            store.verify_materialized_journals()?;
        }
        store.cache_verified_retention_watermark()?;
        Ok(store)
    }
}
