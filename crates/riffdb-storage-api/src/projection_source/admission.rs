//! Compiled ADR-0316 bounds and checked accounting, independent of native locators/providers.

use crate::StorageValueError;

/// Maximum selected exact projection identities in one database.
pub const MAX_PROJECTION_SOURCE_ROOTS: u16 = 64;
/// Maximum canonical rows in one authoritative source object.
pub const MAX_PROJECTION_SOURCE_ROWS: u32 = 65_536;
/// Maximum complete canonical source key/value bytes per object.
pub const MAX_PROJECTION_SOURCE_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum selected authoritative source bytes per database.
pub const MAX_PROJECTION_SELECTED_SOURCE_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum all source-table bytes, including candidates, pins and retirement.
pub const MAX_PROJECTION_SOURCE_TABLE_BYTES: u64 = 512 * 1024 * 1024;
/// Maximum complete encoded descriptor/root/maintenance record.
pub use riffdb_types::MAX_PROJECTION_SOURCE_CONTROL_BYTES;
/// Aggregate incremental worker memory, including staging cache and all scratch.
pub const MAX_PROJECTION_SOURCE_WORKING_BYTES: u64 =
    riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES;
/// Maximum encoded private staging bytes.
pub const MAX_PROJECTION_SOURCE_STAGING_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum physically allocated private staging disk bytes.
pub const MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES: u64 = 128 * 1024 * 1024;
/// Maximum row mutations in one source copy or collection step.
pub const MAX_PROJECTION_SOURCE_STEP_ROWS: u16 = 128;
/// Maximum complete row bytes per step, before tighter existing V3/write ceilings.
pub const MAX_PROJECTION_SOURCE_STEP_BYTES: u64 = 4 * 1024 * 1024;
/// A source-seeded generation begins rollover by this individually applied tail.
pub const PROJECTION_SOURCE_ROLLOVER_TAIL: u64 = 32_768;
/// Further derived apply refuses at this individually applied tail length.
pub const MAX_PROJECTION_GENERATION_TAIL: u64 = 131_072;

/// Structural native source-table accounting observation, not a reservation owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionSourceTableUsageV1 {
    selected_roots: u16,
    selected_bytes: u64,
    total_bytes: u64,
    candidate_reserved_bytes: Option<u64>,
}

impl ProjectionSourceTableUsageV1 {
    /// Checks decoded accounting. Unreadable required inputs must refuse upstream.
    pub fn new(
        selected_roots: u16,
        selected_bytes: u64,
        total_bytes: u64,
        candidate_reserved_bytes: Option<u64>,
    ) -> Result<Self, StorageValueError> {
        if selected_roots > MAX_PROJECTION_SOURCE_ROOTS
            || selected_bytes > MAX_PROJECTION_SELECTED_SOURCE_BYTES
            || total_bytes > MAX_PROJECTION_SOURCE_TABLE_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if selected_bytes > total_bytes
            || selected_bytes < u64::from(selected_roots)
            || candidate_reserved_bytes.is_some_and(|bytes| {
                bytes == 0
                    || bytes
                        > MAX_PROJECTION_SOURCE_OBJECT_BYTES
                            + 3 * MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64
                    || bytes > total_bytes - selected_bytes
            })
            || (selected_roots == 0 && selected_bytes != 0)
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            selected_roots,
            selected_bytes,
            total_bytes,
            candidate_reserved_bytes,
        })
    }

    /// Checked empty source-table observation.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            selected_roots: 0,
            selected_bytes: 0,
            total_bytes: 0,
            candidate_reserved_bytes: None,
        }
    }

    /// Selected roots counted independently from retained superseded objects.
    #[must_use]
    pub const fn selected_roots(self) -> u16 {
        self.selected_roots
    }
    /// Selected source charge, including its complete native metadata.
    #[must_use]
    pub const fn selected_bytes(self) -> u64 {
        self.selected_bytes
    }
    /// All physical source-table canonical charge, including retiring rows.
    #[must_use]
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }
    /// Entire new-object reservation that must exist before the first copy page.
    #[must_use]
    pub const fn candidate_reserved_bytes(self) -> Option<u64> {
        self.candidate_reserved_bytes
    }

    /// Preflights complete new-object charge. This pure post-image does not acquire
    /// a reservation: the coordinator/native owner must atomically reserve it
    /// before copying. Existing selected/superseded bytes are never subtracted.
    pub fn after_candidate_allocation(
        self,
        rows: u32,
        canonical_row_bytes: u64,
        object_control_bytes: u64,
        root_maintenance_allocator_bytes: u64,
    ) -> Result<Self, StorageValueError> {
        if self.candidate_reserved_bytes.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        if rows > MAX_PROJECTION_SOURCE_ROWS
            || canonical_row_bytes > MAX_PROJECTION_SOURCE_OBJECT_BYTES
            || object_control_bytes > MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64
            || root_maintenance_allocator_bytes > 3 * MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if (rows == 0) != (canonical_row_bytes == 0)
            || canonical_row_bytes < u64::from(rows)
            || object_control_bytes == 0
            || root_maintenance_allocator_bytes == 0
        {
            return Err(StorageValueError::InvalidShape);
        }
        // The per-object cap includes that object's complete descriptor/control;
        // it is never a 64-MiB rows allowance plus uncounted object metadata.
        let object_charge = canonical_row_bytes
            .checked_add(object_control_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        if object_charge > MAX_PROJECTION_SOURCE_OBJECT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let charge = object_charge
            .checked_add(root_maintenance_allocator_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        let total = self
            .total_bytes
            .checked_add(charge)
            .ok_or(StorageValueError::SizeOverflow)?;
        Self::new(
            self.selected_roots,
            self.selected_bytes,
            total,
            Some(charge),
        )
    }
}

/// Independently measured worker resource observation, not permission to allocate.
/// Actual capacities must be reserved before construction and remain held until
/// final physical release; these limits cannot borrow locator/provider budgets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionSourceWorkingSetV1 {
    memory_bytes: u64,
    staging_bytes: u64,
    allocated_staging_bytes: u64,
}

impl ProjectionSourceWorkingSetV1 {
    /// Checks the complete simultaneous worker/staging inventory.
    pub fn new(
        memory_bytes: u64,
        staging_bytes: u64,
        allocated_staging_bytes: u64,
    ) -> Result<Self, StorageValueError> {
        if memory_bytes > MAX_PROJECTION_SOURCE_WORKING_BYTES
            || staging_bytes > MAX_PROJECTION_SOURCE_STAGING_BYTES
            || allocated_staging_bytes > MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if staging_bytes > allocated_staging_bytes {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            memory_bytes,
            staging_bytes,
            allocated_staging_bytes,
        })
    }

    /// Complete simultaneously reserved worker memory.
    #[must_use]
    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }
    /// Canonical private staging charge.
    #[must_use]
    pub const fn staging_bytes(self) -> u64 {
        self.staging_bytes
    }
    /// Actual allocated staging disk charge.
    #[must_use]
    pub const fn allocated_staging_bytes(self) -> u64 {
        self.allocated_staging_bytes
    }
}
