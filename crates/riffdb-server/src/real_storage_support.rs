//! Real on-disk redb compositions shared by this crate's unit tests.
//!
//! Every helper here builds the *production* storage bridge over a real redb
//! file so tests exercise `SharedRedbOperationalPorts`, its current views, and
//! the adapters layered on it rather than a stand-in.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_api::{
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs,
};
use riffdb_types::{DatabaseId, DigestKeyId, Timestamp};

use crate::identifiers::ProductionIdentifierSources;
use crate::startup::open_redb_startup;
use crate::storage::SharedRedbOperationalPorts;

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

/// Returns a unique real-filesystem path for one test database.
///
/// `CARGO_TARGET_TMPDIR` is only defined for integration-test and benchmark
/// targets, so unit tests fall back to the platform temporary directory used
/// by the crate's existing on-disk startup tests. Either way the database is a
/// real file that redb opens, maps, and fsyncs.
#[must_use]
pub(crate) fn temporary_database_path(label: &str) -> PathBuf {
    let base = option_env!("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join(format!(
        "riffdb-server-{label}-{}-{}.redb",
        std::process::id(),
        NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
    ))
}

/// One real redb database plus the production sharing bridge over it.
pub(crate) struct RealStorage {
    path: PathBuf,
    pub(crate) storage: SharedRedbOperationalPorts,
    pub(crate) database_id: DatabaseId,
}

impl RealStorage {
    /// Initializes, validates, and activates a real redb database on disk.
    pub(crate) fn open(label: &str) -> Self {
        let path = temporary_database_path(label);
        let digest = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
        let inputs = StartupValidationInputs::new(
            Timestamp::new(0, 0).expect("startup timestamp"),
            ReadableCapabilityDigestInventory::new(vec![digest]).expect("capability inventory"),
            ReadableIdempotencyDigestInventory::new(vec![digest]).expect("idempotency inventory"),
        );
        let startup = open_redb_startup(
            &path,
            inputs,
            &ProductionIdentifierSources::new().database_ids(),
        )
        .expect("open and validate a real redb database");
        let database_id = startup.database_id();
        let (_id, _retained, _history, _lifecycle, _capacity, ports) = startup.into_parts();
        let storage = SharedRedbOperationalPorts::new(ports, None)
            .expect("build the production storage bridge");
        Self {
            path,
            storage,
            database_id,
        }
    }
}

impl Drop for RealStorage {
    fn drop(&mut self) {
        let _removed = std::fs::remove_file(&self.path);
    }
}
