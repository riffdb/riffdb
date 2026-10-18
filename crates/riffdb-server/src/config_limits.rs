//! Per-database storage, request-rate, and concurrency resource enforcement.

use super::*;
use std::fs;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use riffdb_errors::PublicError;
use riffdb_service::{
    DatabaseResourceAdmissionPort, operation_grows_authoritative_storage,
    operation_is_exempt_from_resource_limits, operation_uses_write_rate_bucket,
};
use riffdb_types::ServiceOperationV1;

/// Minimum configurable per-database storage quota (`64 KiB`).
pub(crate) const MIN_STORAGE_QUOTA_BYTES: u64 = 64 * 1024;
/// Maximum configurable per-database request rate (`1,000,000` RPS).
pub(crate) const MAX_RATE_LIMIT_RPS: u32 = 1_000_000;
/// Maximum configurable blocking worker threads per database.
pub(crate) const MAX_BLOCKING_WORKER_THREADS: u16 = 8;
/// Maximum configurable blocking port operations per database.
pub(crate) const MAX_BLOCKING_PORT_OPERATIONS: u16 = 256;
/// Maximum configurable commit coordinator workload capacity per database.
pub(crate) const MAX_COORDINATOR_WORKLOAD_CAPACITY: u16 = 4_096;

const TOKENS_PER_OPERATION: u128 = 1_000_000_000;
const MAX_QUOTA_DIRECTORY_DEPTH: usize = 8;
const MAX_QUOTA_DIRECTORY_ENTRIES: usize = 4_096;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LimitsDocument {
    max_storage_bytes: Option<u64>,
    max_write_rps: Option<u32>,
    write_burst: Option<u32>,
    max_read_rps: Option<u32>,
    read_burst: Option<u32>,
    max_blocking_operations: Option<u16>,
    blocking_worker_threads: Option<u16>,
    max_coordinator_capacity: Option<u16>,
    extent_profile: Option<String>,
}

impl fmt::Debug for LimitsDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LimitsDocument([CONFIGURED])")
    }
}

/// Validated token-bucket rate and burst ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RateBucketConfig {
    rate_per_second: NonZeroU32,
    burst: NonZeroU32,
}

impl RateBucketConfig {
    pub(crate) const fn rate_per_second(self) -> NonZeroU32 {
        self.rate_per_second
    }

    pub(crate) const fn burst(self) -> NonZeroU32 {
        self.burst
    }
}

/// Validated per-database storage, request-rate, and concurrency bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseLimitsConfig {
    max_storage_bytes: Option<NonZeroU64>,
    write_rate: Option<RateBucketConfig>,
    read_rate: Option<RateBucketConfig>,
    max_blocking_operations: NonZeroU16,
    blocking_worker_threads: NonZeroU16,
    max_coordinator_capacity: Option<NonZeroU16>,
    extent_profile: riffdb_storage_redb::JournalExtentProfile,
}

impl Default for DatabaseLimitsConfig {
    fn default() -> Self {
        Self {
            max_storage_bytes: None,
            write_rate: None,
            read_rate: None,
            max_blocking_operations: NonZeroU16::new(MAX_BLOCKING_PORT_OPERATIONS)
                .unwrap_or(NonZeroU16::MIN),
            blocking_worker_threads: NonZeroU16::new(MAX_BLOCKING_WORKER_THREADS)
                .unwrap_or(NonZeroU16::MIN),
            max_coordinator_capacity: None,
            extent_profile: riffdb_storage_redb::JournalExtentProfile::Standard,
        }
    }
}

impl DatabaseLimitsConfig {
    pub(crate) const fn max_storage_bytes(self) -> Option<NonZeroU64> {
        self.max_storage_bytes
    }

    pub(crate) const fn write_rate(self) -> Option<RateBucketConfig> {
        self.write_rate
    }

    pub(crate) const fn read_rate(self) -> Option<RateBucketConfig> {
        self.read_rate
    }

    pub(crate) const fn max_blocking_operations(self) -> NonZeroU16 {
        self.max_blocking_operations
    }

    pub(crate) const fn blocking_worker_threads(self) -> NonZeroU16 {
        self.blocking_worker_threads
    }

    pub(crate) const fn max_coordinator_capacity(self) -> Option<NonZeroU16> {
        self.max_coordinator_capacity
    }

    pub(crate) const fn extent_profile(self) -> riffdb_storage_redb::JournalExtentProfile {
        self.extent_profile
    }

    pub(crate) const fn has_admission_governor(self) -> bool {
        self.max_storage_bytes.is_some() || self.write_rate.is_some() || self.read_rate.is_some()
    }
}

pub(super) fn parse_limits(
    document: Option<LimitsDocument>,
) -> Result<DatabaseLimitsConfig, ServerConfigError> {
    let Some(document) = document else {
        return Ok(DatabaseLimitsConfig::default());
    };
    let max_storage_bytes = match document.max_storage_bytes {
        Some(bytes) => {
            let nonzero =
                NonZeroU64::new(bytes).ok_or(ServerConfigError::InvalidLimitsConfiguration)?;
            if nonzero.get() < MIN_STORAGE_QUOTA_BYTES {
                return Err(ServerConfigError::InvalidLimitsConfiguration);
            }
            Some(nonzero)
        }
        None => None,
    };
    let write_rate = parse_rate_bucket(document.max_write_rps, document.write_burst)?;
    let read_rate = parse_rate_bucket(document.max_read_rps, document.read_burst)?;
    let max_blocking_operations = match document.max_blocking_operations {
        Some(operations) => {
            let nonzero =
                NonZeroU16::new(operations).ok_or(ServerConfigError::InvalidLimitsConfiguration)?;
            if nonzero.get() > MAX_BLOCKING_PORT_OPERATIONS {
                return Err(ServerConfigError::InvalidLimitsConfiguration);
            }
            nonzero
        }
        None => NonZeroU16::new(MAX_BLOCKING_PORT_OPERATIONS).unwrap_or(NonZeroU16::MIN),
    };
    let blocking_worker_threads = match document.blocking_worker_threads {
        Some(threads) => {
            let nonzero =
                NonZeroU16::new(threads).ok_or(ServerConfigError::InvalidLimitsConfiguration)?;
            if nonzero.get() > MAX_BLOCKING_WORKER_THREADS
                || nonzero.get() > max_blocking_operations.get()
            {
                return Err(ServerConfigError::InvalidLimitsConfiguration);
            }
            nonzero
        }
        None => {
            let default_threads = MAX_BLOCKING_WORKER_THREADS.min(max_blocking_operations.get());
            NonZeroU16::new(default_threads).unwrap_or(NonZeroU16::MIN)
        }
    };
    let max_coordinator_capacity = match document.max_coordinator_capacity {
        Some(capacity) => {
            let nonzero =
                NonZeroU16::new(capacity).ok_or(ServerConfigError::InvalidLimitsConfiguration)?;
            if nonzero.get() > MAX_COORDINATOR_WORKLOAD_CAPACITY {
                return Err(ServerConfigError::InvalidLimitsConfiguration);
            }
            Some(nonzero)
        }
        None => None,
    };
    let extent_profile = match document.extent_profile.as_deref() {
        None | Some("standard") => riffdb_storage_redb::JournalExtentProfile::Standard,
        Some("compact") => riffdb_storage_redb::JournalExtentProfile::Compact,
        Some(_) => return Err(ServerConfigError::InvalidLimitsConfiguration),
    };
    Ok(DatabaseLimitsConfig {
        max_storage_bytes,
        write_rate,
        read_rate,
        max_blocking_operations,
        blocking_worker_threads,
        max_coordinator_capacity,
        extent_profile,
    })
}

fn parse_rate_bucket(
    rate_per_second: Option<u32>,
    burst: Option<u32>,
) -> Result<Option<RateBucketConfig>, ServerConfigError> {
    match (rate_per_second, burst) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ServerConfigError::InvalidLimitsConfiguration),
        (Some(rate), burst) => {
            let rate_per_second =
                NonZeroU32::new(rate).ok_or(ServerConfigError::InvalidLimitsConfiguration)?;
            if rate_per_second.get() > MAX_RATE_LIMIT_RPS {
                return Err(ServerConfigError::InvalidLimitsConfiguration);
            }
            let burst = match burst {
                Some(raw_burst) => NonZeroU32::new(raw_burst)
                    .ok_or(ServerConfigError::InvalidLimitsConfiguration)?,
                None => rate_per_second,
            };
            if burst.get() > MAX_RATE_LIMIT_RPS {
                return Err(ServerConfigError::InvalidLimitsConfiguration);
            }
            Ok(Some(RateBucketConfig {
                rate_per_second,
                burst,
            }))
        }
    }
}

struct TokenBucketState {
    tokens: u128,
    last_refill: Instant,
}

struct TokenBucket {
    rate_per_second: u128,
    max_tokens: u128,
    state: Mutex<TokenBucketState>,
}

impl TokenBucket {
    fn new(config: RateBucketConfig, now: Instant) -> Self {
        let max_tokens = u128::from(config.burst.get()).saturating_mul(TOKENS_PER_OPERATION);
        Self {
            rate_per_second: u128::from(config.rate_per_second.get()),
            max_tokens,
            state: Mutex::new(TokenBucketState {
                tokens: max_tokens,
                last_refill: now,
            }),
        }
    }

    fn try_acquire(&self, now: Instant) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed_ns = now.saturating_duration_since(state.last_refill).as_nanos();
        if elapsed_ns > 0 {
            let added = elapsed_ns.saturating_mul(self.rate_per_second);
            state.tokens = state.tokens.saturating_add(added).min(self.max_tokens);
            state.last_refill = now;
        }
        if state.tokens >= TOKENS_PER_OPERATION {
            state.tokens -= TOKENS_PER_OPERATION;
            true
        } else {
            false
        }
    }
}

struct StorageQuotaPaths {
    max_storage_bytes: NonZeroU64,
    database_path: PathBuf,
    projections_root: PathBuf,
    outbox_root: Option<PathBuf>,
}

impl StorageQuotaPaths {
    fn is_within_quota(&self) -> bool {
        self.observed_storage_bytes() < self.max_storage_bytes.get()
    }

    fn observed_storage_bytes(&self) -> u64 {
        let mut total = 0_u64;
        for path in owned_database_files(&self.database_path) {
            if let Ok(metadata) = fs::symlink_metadata(&path)
                && metadata.is_file()
            {
                total = total.saturating_add(metadata.len());
            }
        }
        total = total.saturating_add(sealed_extent_bytes(&self.database_path));
        total = total.saturating_add(directory_tree_bytes(&self.projections_root));
        if let Some(outbox_root) = &self.outbox_root {
            total = total.saturating_add(directory_tree_bytes(outbox_root));
        }
        total
    }
}

fn owned_database_files(database_path: &Path) -> [PathBuf; 4] {
    let mut journal = database_path.as_os_str().to_os_string();
    journal.push(".journal");
    let mut format = database_path.as_os_str().to_os_string();
    format.push(".format");
    let mut derived = database_path.as_os_str().to_os_string();
    derived.push(".derived");
    [
        database_path.to_path_buf(),
        PathBuf::from(journal),
        PathBuf::from(format),
        PathBuf::from(derived),
    ]
}

fn sealed_extent_bytes(database_path: &Path) -> u64 {
    let Some(parent) = database_path.parent() else {
        return 0;
    };
    let Some(file_name) = database_path.file_name().and_then(OsStr::to_str) else {
        return 0;
    };
    let prefix = format!("{file_name}.journal.ext.");
    let Ok(entries) = fs::read_dir(parent) else {
        return 0;
    };
    let mut total = 0_u64;
    for entry in entries.take(MAX_QUOTA_DIRECTORY_ENTRIES).flatten() {
        let name = entry.file_name();
        if name.to_str().is_some_and(|text| text.starts_with(&prefix))
            && let Ok(metadata) = entry.metadata()
            && metadata.is_file()
        {
            total = total.saturating_add(metadata.len());
        }
    }
    total
}

fn directory_tree_bytes(root: &Path) -> u64 {
    let mut total = 0_u64;
    let mut visited_entries = 0_usize;
    let mut stack = vec![(root.to_path_buf(), 0_usize)];
    while let Some((current, depth)) = stack.pop() {
        if depth > MAX_QUOTA_DIRECTORY_DEPTH || visited_entries >= MAX_QUOTA_DIRECTORY_ENTRIES {
            break;
        }
        let Ok(entries) = fs::read_dir(&current) else {
            continue;
        };
        for entry in entries {
            if visited_entries >= MAX_QUOTA_DIRECTORY_ENTRIES {
                break;
            }
            visited_entries += 1;
            let Ok(entry) = entry else {
                continue;
            };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_file() {
                if let Ok(metadata) = entry.metadata() {
                    total = total.saturating_add(metadata.len());
                }
            } else if file_type.is_dir() && depth < MAX_QUOTA_DIRECTORY_DEPTH {
                stack.push((entry.path(), depth + 1));
            }
        }
    }
    total
}

/// Server-owned runtime enforcer for per-database rate and storage quotas.
pub(crate) struct ServerDatabaseResourceGovernor {
    clock: Arc<dyn Fn() -> Instant + Send + Sync>,
    storage_quota: Option<StorageQuotaPaths>,
    write_bucket: Option<TokenBucket>,
    read_bucket: Option<TokenBucket>,
}

impl ServerDatabaseResourceGovernor {
    pub(crate) fn new(
        limits: DatabaseLimitsConfig,
        database_path: PathBuf,
        projections_root: PathBuf,
        outbox_root: Option<PathBuf>,
    ) -> Self {
        Self::with_clock(
            limits,
            database_path,
            projections_root,
            outbox_root,
            Arc::new(Instant::now),
        )
    }

    pub(crate) fn with_clock(
        limits: DatabaseLimitsConfig,
        database_path: PathBuf,
        projections_root: PathBuf,
        outbox_root: Option<PathBuf>,
        clock: Arc<dyn Fn() -> Instant + Send + Sync>,
    ) -> Self {
        let now = clock();
        Self {
            clock,
            storage_quota: limits
                .max_storage_bytes
                .map(|max_storage_bytes| StorageQuotaPaths {
                    max_storage_bytes,
                    database_path,
                    projections_root,
                    outbox_root,
                }),
            write_bucket: limits
                .write_rate
                .map(|config| TokenBucket::new(config, now)),
            read_bucket: limits.read_rate.map(|config| TokenBucket::new(config, now)),
        }
    }
}

impl DatabaseResourceAdmissionPort for ServerDatabaseResourceGovernor {
    fn admit_operation(&self, operation: ServiceOperationV1) -> Result<(), PublicError> {
        if operation_is_exempt_from_resource_limits(operation) {
            return Ok(());
        }
        if operation_grows_authoritative_storage(operation)
            && let Some(storage_quota) = &self.storage_quota
            && !storage_quota.is_within_quota()
        {
            return Err(PublicError::overloaded());
        }
        let now = (self.clock)();
        if operation_uses_write_rate_bucket(operation) {
            if let Some(bucket) = &self.write_bucket
                && !bucket.try_acquire(now)
            {
                return Err(PublicError::overloaded());
            }
        } else if let Some(bucket) = &self.read_bucket
            && !bucket.try_acquire(now)
        {
            return Err(PublicError::overloaded());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // req: NET-005, PERF-007
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn limits_configuration_validates_bounds_and_defaults_worker_threads_to_blocking_cap() {
        let root = tempfile::tempdir().unwrap();
        let config_path = root.path().join("config.toml");

        let valid = format!(
            "[databases.dev]\npath = '{0}/dev.redb'\nbackup_root = '{0}/dev-backups'\nprojections_root = '{0}/dev-projections'\nenvironment = 'development'\n[databases.dev.limits]\nmax_storage_bytes = 536870912\nmax_write_rps = 50\nwrite_burst = 100\nmax_read_rps = 250\nread_burst = 500\nmax_blocking_operations = 4\nmax_coordinator_capacity = 64\nextent_profile = 'compact'\n",
            root.path().display()
        );
        fs::write(&config_path, &valid).unwrap();
        let resolved = ServerConfig::resolve(
            ["--config".into(), config_path.clone().into_os_string()],
            &EmptyEnvironment,
            root.path(),
        )
        .expect("valid developer database limits");
        let limits = resolved.databases()[0].limits();
        assert_eq!(limits.max_storage_bytes().unwrap().get(), 536_870_912);
        assert_eq!(limits.write_rate().unwrap().rate_per_second().get(), 50);
        assert_eq!(limits.write_rate().unwrap().burst().get(), 100);
        assert_eq!(limits.read_rate().unwrap().rate_per_second().get(), 250);
        assert_eq!(limits.read_rate().unwrap().burst().get(), 500);
        assert_eq!(limits.max_blocking_operations().get(), 4);
        assert_eq!(limits.blocking_worker_threads().get(), 4);
        assert_eq!(limits.max_coordinator_capacity().unwrap().get(), 64);
        assert_eq!(
            limits.extent_profile(),
            riffdb_storage_redb::JournalExtentProfile::Compact
        );

        for invalid in [
            valid.replace("536870912", "1024"),
            valid.replace("max_write_rps = 50\n", ""),
            valid.replace("max_blocking_operations = 4", "max_blocking_operations = 0"),
            valid.replace(
                "max_blocking_operations = 4",
                "max_blocking_operations = 4\nblocking_worker_threads = 8",
            ),
            valid.replace(
                "max_coordinator_capacity = 64",
                "max_coordinator_capacity = 8192",
            ),
            valid.replace("extent_profile = 'compact'", "extent_profile = 'tiny'"),
        ] {
            fs::write(&config_path, invalid).unwrap();
            assert_eq!(
                ServerConfig::resolve(
                    ["--config".into(), config_path.clone().into_os_string()],
                    &EmptyEnvironment,
                    root.path(),
                )
                .unwrap_err(),
                ServerConfigError::InvalidLimitsConfiguration
            );
        }
    }

    #[test]
    fn governor_enforces_token_buckets_and_preserves_reads_and_exports_at_storage_ceiling() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("dev.redb");
        let projections_root = root.path().join("projections");
        fs::create_dir_all(&projections_root).unwrap();
        fs::write(&db_path, vec![0_u8; 40 * 1024]).unwrap();

        let now = Arc::new(Mutex::new(Instant::now()));
        let clock_now = Arc::clone(&now);
        let limits = parse_limits(Some(LimitsDocument {
            max_storage_bytes: Some(64 * 1024),
            max_write_rps: Some(2),
            write_burst: Some(2),
            max_read_rps: Some(3),
            read_burst: Some(3),
            max_blocking_operations: Some(16),
            blocking_worker_threads: Some(2),
            max_coordinator_capacity: Some(32),
            extent_profile: Some("compact".to_owned()),
        }))
        .unwrap();
        let governor = ServerDatabaseResourceGovernor::with_clock(
            limits,
            db_path.clone(),
            projections_root.clone(),
            None,
            Arc::new(move || *clock_now.lock().unwrap()),
        );

        // Two writes succeed within the burst of 2; third write is rate-limited.
        assert!(
            governor
                .admit_operation(ServiceOperationV1::ExecuteCommand)
                .is_ok()
        );
        assert!(
            governor
                .admit_operation(ServiceOperationV1::ExecuteCommand)
                .is_ok()
        );
        assert_eq!(
            governor
                .admit_operation(ServiceOperationV1::ExecuteCommand)
                .unwrap_err()
                .kind(),
            riffdb_errors::PublicErrorKind::Overloaded
        );

        // Reads use their own independent bucket and remain available while writes are throttled.
        for _ in 0..3 {
            assert!(
                governor
                    .admit_operation(ServiceOperationV1::ExecuteQuery)
                    .is_ok()
            );
        }
        assert_eq!(
            governor
                .admit_operation(ServiceOperationV1::ExecuteQuery)
                .unwrap_err()
                .kind(),
            riffdb_errors::PublicErrorKind::Overloaded
        );
        // Health and statistics probes remain exempt even when both buckets are empty.
        assert!(
            governor
                .admit_operation(ServiceOperationV1::GetHealth)
                .is_ok()
        );
        assert!(
            governor
                .admit_operation(ServiceOperationV1::GetStatistics)
                .is_ok()
        );

        // Advance clock by 2 seconds to refill both token buckets to full burst capacity.
        *now.lock().unwrap() += Duration::from_secs(2);

        // Grow projection storage so total database footprint exceeds the 64 KiB ceiling.
        fs::write(projections_root.join("segment.bin"), vec![0_u8; 32 * 1024]).unwrap();

        // Storage-growing writes fail closed with Overloaded.
        assert_eq!(
            governor
                .admit_operation(ServiceOperationV1::ExecuteCommand)
                .unwrap_err()
                .kind(),
            riffdb_errors::PublicErrorKind::Overloaded
        );
        assert_eq!(
            governor
                .admit_operation(ServiceOperationV1::DeployContract)
                .unwrap_err()
                .kind(),
            riffdb_errors::PublicErrorKind::Overloaded
        );

        // Reads, uncertainty recovery, application exports, and capability revocation remain admitted!
        for safe_operation in [
            ServiceOperationV1::ExecuteQuery,
            ServiceOperationV1::ResolveCommandOutcome,
            ServiceOperationV1::StartApplicationExport,
            ServiceOperationV1::GetApplicationExportPage,
            ServiceOperationV1::RevokeCapability,
        ] {
            *now.lock().unwrap() += Duration::from_secs(1);
            assert!(
                governor.admit_operation(safe_operation).is_ok(),
                "{safe_operation:?} must remain admissible at storage ceiling"
            );
        }
    }
}
