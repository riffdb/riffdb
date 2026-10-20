//! Physical engine lifetime for independent archive work; no read/write authority.
use crate::{RedbOperationalPorts, engine_owner::EngineOwner};
use riffdb_types::OwnedCapacitySharedV1;

/// Internal archive composition keeps the actual engine alive until its thread
/// ends. This owns no SharedRedb, read transaction, snapshot or mutation port.
#[doc(hidden)]
#[derive(Clone)]
pub struct RedbArchiveJobEngineCustodyV1 {
    _engine: OwnedCapacitySharedV1<EngineOwner<redb::Database>>,
}

impl RedbOperationalPorts {
    /// Retains this exact already-open engine before spawning archive work.
    /// Arc cloning creates no new native transaction, cache or worker budget.
    #[doc(hidden)]
    #[must_use]
    pub fn archive_job_engine_custody_v1(&self) -> RedbArchiveJobEngineCustodyV1 {
        RedbArchiveJobEngineCustodyV1 {
            _engine: self.shared.database.clone(),
        }
    }
}

impl Drop for RedbArchiveJobEngineCustodyV1 {
    fn drop(&mut self) {}
}

#[cfg(test)]
#[path = "archive_job_custody_tests.rs"]
mod tests;
