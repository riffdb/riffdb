//! Real on-disk redb compositions shared by this crate's unit tests.
//!
//! Every helper here builds the *production* storage bridge over a real redb
//! file so tests exercise `SharedRedbOperationalPorts`, its current views, and
//! the adapters layered on it rather than a stand-in.

use std::path::PathBuf;

use riffdb_storage_api::{
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs,
};
use riffdb_types::{DatabaseId, DigestKeyId, Timestamp};

use crate::identifiers::ProductionIdentifierSources;
use crate::startup::open_redb_startup;
use crate::storage::SharedRedbOperationalPorts;

/// Returns a whole-directory scope plus a unique real-filesystem database
/// path inside it for one test database.
///
/// `CARGO_TARGET_TMPDIR` is only defined for integration-test and benchmark
/// targets, so unit tests fall back to the platform temporary directory used
/// by the crate's existing on-disk startup tests. Either way the database is a
/// real file that redb opens, maps, and fsyncs, and the scope's `Drop`
/// removes the database together with every side file it grows (journal,
/// checkpoint, spare, durable-format marker, …) on pass, fail, or panic.
#[must_use]
pub(crate) fn temporary_database_scope(label: &str) -> (tempfile::TempDir, PathBuf) {
    let base = option_env!("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let scope = tempfile::Builder::new()
        .prefix(&format!("riffdb-server-{label}-"))
        .tempdir_in(base)
        .expect("create test database scope directory");
    let path = scope.path().join("db.redb");
    (scope, path)
}

/// One real redb database plus the production sharing bridge over it.
pub(crate) struct RealStorage {
    pub(crate) storage: SharedRedbOperationalPorts,
    pub(crate) database_id: DatabaseId,
    /// Declared last so the scope directory outlives the open database.
    _scope: tempfile::TempDir,
}

impl RealStorage {
    /// Initializes, validates, and activates a real redb database on disk.
    pub(crate) fn open(label: &str) -> Self {
        let (scope, path) = temporary_database_scope(label);
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
            storage,
            database_id,
            _scope: scope,
        }
    }
}
