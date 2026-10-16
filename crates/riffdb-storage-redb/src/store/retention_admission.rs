//! Closed pre-admission yield for the background retention writer.

use super::*;

#[expect(
    clippy::large_enum_variant,
    reason = "The existing write and its lease stay inline; boxing adds fallible allocation after admission"
)]
pub(crate) enum RetentionWriteBegin {
    Ready(RedbWriteAccess),
    CheckpointInFlight,
    AwaitingNativeDependency,
}

impl SharedRedb {
    /// This observation grants no write authority and never restarts a builder.
    /// Retention repeats it under its mutation lease before opening a write.
    pub(crate) fn retention_native_dependency_pending(
        self: &Arc<Self>,
    ) -> Result<bool, StorageError> {
        if self.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let access = self.begin_composite_operational_read()?;
        let capability = access.locator_capability();
        if capability.is_pending() {
            capability.source_binding()?;
            Ok(true)
        } else {
            capability.require_complete().map(|_| false)
        }
    }
}

impl RedbOperationalPorts {
    /// A retention-only closed yield; other direct writers keep their
    /// original checkpoint Option semantics and strict locator admission.
    pub(crate) fn begin_retention_write_unless_checkpointing(
        &self,
        checkpoint_first: bool,
        timing: &mut WriteBeginTiming,
    ) -> Result<RetentionWriteBegin, StorageError> {
        let mut pending = false;
        let access = self.begin_attributed_write_inner(
            riffdb_storage_api::ChangelogAttributionV3::RetentionPrune,
            checkpoint_first,
            true,
            Some(timing),
            Some(&mut pending),
        )?;
        Ok(match access {
            Some(access) => RetentionWriteBegin::Ready(access),
            None if pending => RetentionWriteBegin::AwaitingNativeDependency,
            None => RetentionWriteBegin::CheckpointInFlight,
        })
    }

    pub(crate) fn begin_deferred_retention_write(
        &self,
        timing: &mut WriteBeginTiming,
    ) -> Result<Option<RedbWriteAccess>, StorageError> {
        self.begin_deferred_maintenance_write_inner(timing, true)
    }
}
