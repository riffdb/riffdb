//! One unchanged current-format open path, with optional closed startup funding.
use super::*;

pub(crate) fn open_configured_current(
    path: &Path,
    profile: RedbCommitProfile,
    source_domain: crate::source_working_domain::SourceWorkingDomain,
) -> Result<RedbStore, StorageError> {
    open_with_repair_policy(
        path,
        profile,
        None,
        default_changelog_port(),
        None,
        engine_open::EngineRepairPolicy::Recover,
        Some(source_domain),
    )
}
pub(super) fn open_with_repair_policy(
    path: &Path,
    application_commit_profile: RedbCommitProfile,
    test_controller: Option<RedbTestController>,
    changelog_port: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    media: Option<RedbStorageMedia>,
    repair_policy: engine_open::EngineRepairPolicy,
    source_domain: Option<crate::source_working_domain::SourceWorkingDomain>,
) -> Result<RedbStore, StorageError> {
    let path = path.to_path_buf();
    let (engine_backend, journal_media) = match media {
        Some(media) => (Some(media.engine), media.journal),
        None => (None, Arc::new(RealJournalMedia) as Arc<dyn JournalMedia>),
    };
    let format_preflight = preflight_durable_format_path_with_media(journal_media.as_ref(), &path)
        .map_err(format_preflight_storage_error)?;
    if matches!(
        format_preflight,
        RedbDurableFormatPreflight::OfflineUpgradeRequired { .. }
    ) {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    RedbStore::open_after_format_preflight_with_source_domain(
        OpenMode::Source,
        &path,
        application_commit_profile,
        test_controller,
        changelog_port,
        (format_preflight == RedbDurableFormatPreflight::InitializeCurrent)
            .then_some(format_preflight),
        engine_backend,
        journal_media,
        repair_policy,
        source_domain,
    )
}

#[cfg(test)]
impl RedbStore {
    pub(crate) fn configured_source_budget_for_test(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        self.shared.database.source_working_budget()
    }
}

#[cfg(test)]
mod paired_derived_tests {
    use super::*;
    use crate::source_execution_domain::{
        ConfiguredMaintenanceLockV1, ConfiguredSourceExecutionDomainV1, SourceExecutionBootstrapV1,
    };
    use riffdb_storage_api::DatabaseInitializationPort;
    use riffdb_types::exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES;

    fn open(path: &Path, domain: &ConfiguredSourceExecutionDomainV1) -> RedbStore {
        let source_domain =
            crate::source_working_domain::SourceWorkingDomain::from_configured(domain, None)
                .unwrap();
        open_configured_current(path, RedbCommitProfile::Standard, source_domain).unwrap()
    }

    fn operation_lock(
        path: &Path,
        domain: &ConfiguredSourceExecutionDomainV1,
    ) -> ConfiguredMaintenanceLockV1 {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        let legacy = crate::native_file_custody::LegacyFileCustody::acquire(file).unwrap();
        let mut lock = ConfiguredMaintenanceLockV1::original(legacy);
        drop(lock.retain_for_source(domain).unwrap());
        lock
    }

    // req: STO-020, STO-032, PRJ-004
    #[test]
    fn configured_primary_empty_and_derived_owners_share_ledger_through_reopen_and_final_alias() {
        let directory = crate::test_path::ScopedDirectory::new("paired-derived-ledger");
        let path = directory.join("db.redb");
        let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
        let budget = bootstrap.numerical_budget().clone();
        let domain = bootstrap.into_configured();
        let baseline = budget.observation().unwrap().reserved_bytes;
        let mut store = open(&path, &domain);
        store
            .initialize_database(
                riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [51; 10]).unwrap(),
            )
            .unwrap();
        let ports = store.into_contract_migration_ports().unwrap();
        let empty_read = ports.begin_derived_read().unwrap();
        assert!(
            !derived_store_path(&path).exists(),
            "an absent ordinary read creates no sibling"
        );
        let empty = ports.shared.empty_derived.get().unwrap();
        assert!(empty.source_working_budget().shares_owner_with(&budget));
        let worker = budget.try_acquire_worker().unwrap();
        assert!(empty.source_working_budget().try_acquire_worker().is_err());
        drop(worker);
        ports.shared.derived_database().unwrap();
        let (read, owner) = ports.shared.captured_derived_read().unwrap();
        let derived_read = ports.begin_derived_read().unwrap();
        assert!(owner.source_working_budget().shares_owner_with(&budget));
        let free = {
            let observation = budget.observation().unwrap();
            PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - observation.fixed_owner_bytes
                - observation.owned_bytes
                - observation.reserved_bytes
        };
        let pressure = owner.source_working_budget().reserve(free).unwrap();
        assert!(budget.reserve(1).is_err());
        drop(pressure);
        drop(ports);
        assert!(budget.observation().unwrap().reserved_bytes > baseline);
        assert!(matches!(
            redb::ReadOnlyDatabase::open(derived_store_path(&path)),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        ));
        drop(read);
        assert!(
            budget.observation().unwrap().reserved_bytes > baseline,
            "the final closed engine alias still owns its original domain control"
        );
        drop(owner);
        // Both actual wrappers outlive every ports/shared owner. Each opens
        // its real namespace/table through the fixed bounded semantic method.
        let limit = riffdb_storage_api::ProjectionRecoveryPageLimit::new(
            std::num::NonZeroU16::new(1).unwrap(),
        )
        .unwrap();
        assert!(
            matches!(empty_read.scan_projection_controls(None, limit).unwrap(),
            riffdb_storage_api::ProjectionControlScanV1::ExactEnd { controls } if controls.is_empty())
        );
        assert!(
            matches!(derived_read.scan_projection_controls(None, limit).unwrap(),
            riffdb_storage_api::ProjectionControlScanV1::ExactEnd { controls } if controls.is_empty())
        );
        assert!(budget.observation().unwrap().reserved_bytes > baseline);
        assert!(matches!(
            redb::ReadOnlyDatabase::open(derived_store_path(&path)),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        ));
        drop(derived_read);
        drop(redb::ReadOnlyDatabase::open(derived_store_path(&path)).unwrap());
        assert!(
            budget.observation().unwrap().reserved_bytes > baseline,
            "the surviving empty read independently retains its paired Source domain"
        );
        drop(empty_read);
        assert_eq!(budget.observation().unwrap().reserved_bytes, baseline);
        let reopened = open(&path, &domain);
        let (read, owner) = reopened.shared.captured_derived_read().unwrap();
        assert!(owner.source_working_budget().shares_owner_with(&budget));
        drop(read);
        drop(owner);
        drop(reopened);
        assert_eq!(budget.observation().unwrap().reserved_bytes, baseline);
        drop(domain);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    // req: STO-020, STO-032, PRJ-004
    #[test]
    fn configured_source_inflight_refuses_both_cold_derived_routes_before_native_initialization() {
        let directory = crate::test_path::ScopedDirectory::new("paired-derived-inflight");
        let path = directory.join("db.redb");
        let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
        let domain = bootstrap.into_configured();
        let budget = domain.budget().clone();
        let store = open(&path, &domain);
        let mut lock = operation_lock(&directory.join("maintenance.lock"), &domain);
        let claims = domain
            .begin_operation(lock.retain_for_source(&domain).unwrap())
            .unwrap();
        let before = budget.observation().unwrap();
        assert_eq!(
            store.shared.derived_database().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            store.shared.captured_derived_read().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert!(!derived_store_path(&path).exists());
        assert!(store.shared.derived.get().is_none());
        assert!(store.shared.empty_derived.get().is_none());
        let after = budget.observation().unwrap();
        assert_eq!(after.reserved_bytes, before.reserved_bytes);
        assert_eq!(after.owned_bytes, before.owned_bytes);
        drop(claims.semantic);
        assert!(
            store.shared.derived_database().is_err(),
            "task tail still excludes construction"
        );
        drop(claims.task);
        let live = budget.observation().unwrap();
        let available = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - live.fixed_owner_bytes
            - live.owned_bytes
            - live.reserved_bytes;
        let pressure = budget.reserve(available).unwrap();
        assert_eq!(
            store.shared.captured_derived_read().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            budget.observation().unwrap().reserved_bytes,
            live.reserved_bytes + available
        );
        assert!(store.shared.empty_derived.get().is_none());
        assert!(!derived_store_path(&path).exists());
        assert!(
            budget.try_acquire_worker().is_ok(),
            "failed header admission releases its worker claim"
        );
        assert!(
            domain.require_idle().is_ok(),
            "failed header admission releases configured exclusion"
        );
        drop(pressure);
        let (read, owner) = store.shared.captured_derived_read().unwrap();
        assert!(owner.source_working_budget().shares_owner_with(&budget));
        assert!(!derived_store_path(&path).exists());
        drop(read);
        drop(owner);
        drop(store);
        drop(lock);
        drop(domain);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    // req: STO-020, STO-032, PRJ-004
    #[test]
    fn ordinary_derived_constructor_and_source_entry_exclude_each_other_until_publication() {
        let directory = crate::test_path::ScopedDirectory::new("paired-derived-construction-race");
        let path = directory.join("db.redb");
        let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
        let domain = bootstrap.into_configured();
        let budget = domain.budget().clone();
        let store = open(&path, &domain);
        let mut lock = operation_lock(&directory.join("maintenance.lock"), &domain);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (ordinary_live_worker, ordinary_live_idle, denied_kind, sibling_absent) =
            std::thread::scope(|scope| {
                let shared = &store.shared;
                let thread = scope.spawn(move || {
                    let mut pairing = shared
                        .database
                        .begin_ordinary_derived_construction()
                        .unwrap();
                    ready_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(15))
                        .unwrap();
                    let native = RedbStore::open_derived_database(&shared.path, false).unwrap();
                    let owner = pairing
                        .bind(
                            native,
                            REDB_CACHE_SIZE_BYTES,
                            shared.engine_allocation_class(),
                        )
                        .unwrap();
                    assert!(shared.derived.set(owner).is_ok());
                    // pairing retires only after publication, never while the
                    // ordinary Builder/create/write still owns partial resources.
                });
                ready_rx
                    .recv_timeout(std::time::Duration::from_secs(15))
                    .unwrap();
                let worker_denied = budget.try_acquire_worker().is_err();
                let idle_denied = domain.require_idle().is_err();
                let denied_kind = domain
                    .begin_operation(lock.retain_for_source(&domain).unwrap())
                    .err()
                    .map(|error| error.kind());
                let sibling_absent = !derived_store_path(&path).exists();
                release_tx.send(()).unwrap();
                thread.join().unwrap();
                (worker_denied, idle_denied, denied_kind, sibling_absent)
            });
        assert!(ordinary_live_worker);
        assert!(ordinary_live_idle);
        assert_eq!(denied_kind, Some(StorageErrorKind::Unavailable));
        assert!(sibling_absent);
        assert!(budget.try_acquire_worker().is_ok());
        let claims = domain
            .begin_operation(lock.retain_for_source(&domain).unwrap())
            .unwrap();
        assert!(
            store
                .shared
                .derived
                .get()
                .unwrap()
                .source_working_budget()
                .shares_owner_with(&budget)
        );
        drop(claims);
        drop(store);
        drop(lock);
        drop(domain);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
    // req: STO-020, STO-032, PRJ-004
    #[test]
    fn ordinary_derived_native_open_failure_releases_construction_exclusion_for_same_owner_retry() {
        let directory = crate::test_path::ScopedDirectory::new("paired-derived-error");
        let path = directory.join("db.redb");
        let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
        let domain = bootstrap.into_configured();
        let budget = domain.budget().clone();
        let store = open(&path, &domain);
        let sidecar = derived_store_path(&path);
        std::fs::create_dir(&sidecar).unwrap();
        let before = budget.observation().unwrap();
        assert_eq!(
            store.shared.derived_database().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert!(store.shared.derived.get().is_none());
        assert_eq!(
            budget.observation().unwrap().reserved_bytes,
            before.reserved_bytes
        );
        assert!(budget.try_acquire_worker().is_ok());
        assert!(domain.require_idle().is_ok());
        let mut lock = operation_lock(&directory.join("maintenance.lock"), &domain);
        let claims = domain
            .begin_operation(lock.retain_for_source(&domain).unwrap())
            .unwrap();
        drop(claims);
        std::fs::remove_dir(&sidecar).unwrap();
        store.shared.derived_database().unwrap();
        assert!(
            store
                .shared
                .derived
                .get()
                .unwrap()
                .source_working_budget()
                .shares_owner_with(&budget)
        );
        drop(store);
        drop(lock);
        drop(domain);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
