//! Source and provider admission share the retained-frame sizing and canonical
//! reader. Their numerical ownership types and ledgers remain separate.
use super::*;

pub(super) fn physical_work_bound(placement: FramePlacement) -> Result<u64, JournalIoError> {
    let maximum = extent_frame_bytes(MAX_JOURNAL_FRAME_BYTES).ok_or(JournalIoError::Corrupt)?;
    if placement.padded_len > maximum {
        return Err(JournalIoError::Corrupt);
    }
    u64::try_from(placement.padded_len)
        .ok()
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(32 * 1024))
        .ok_or(JournalIoError::Corrupt)
}
fn logical_frame_layout(logical: &[u8]) -> Result<(usize, usize), JournalIoError> {
    if logical.len() < FRAME_HEADER_BYTES_V2 + FRAME_FOOTER_BYTES
        || logical.len() > MAX_JOURNAL_FRAME_BYTES
    {
        return Err(JournalIoError::Corrupt);
    }
    let count = usize::try_from(read_u32(logical, 68).map_err(|_| JournalIoError::Corrupt)?)
        .map_err(|_| JournalIoError::Corrupt)?;
    let payload = usize::try_from(read_u32(logical, 72).map_err(|_| JournalIoError::Corrupt)?)
        .map_err(|_| JournalIoError::Corrupt)?;
    if count > payload / MUTATION_HEADER_BYTES || payload > logical.len() {
        return Err(JournalIoError::Corrupt);
    }
    Ok((count, payload))
}

pub(super) fn logical_work_bound(logical: &[u8]) -> Result<u64, JournalIoError> {
    let (count, _) = logical_frame_layout(logical)?;
    let fold = riffdb_storage_api::provider_receipt_work_bound_v1(count, logical.len())
        .map_err(|_| JournalIoError::Capacity)?;
    count
        .checked_add(4)
        .and_then(|n| n.checked_mul(std::mem::size_of::<JournalMutation>()))
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| u64::try_from(n).ok())
        .and_then(|n| fold.checked_add(n))
        .ok_or(JournalIoError::Capacity)
}
pub(crate) fn read_source_retained_frame_v1(
    media: &dyn JournalMedia,
    candidates: &[PathBuf],
    database_id: DatabaseId,
    placement: FramePlacement,
    generation: u64,
    budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
) -> Result<(JournalFrame, crate::store::SourceReadWork), JournalIoError> {
    let _physical_work =
        crate::store::SourceReadWork::admit(budget, physical_work_bound(placement)?)
            .map_err(|_| JournalIoError::Capacity)?;
    read_retained_frame_from_with_admission(
        media,
        candidates,
        database_id,
        placement,
        generation,
        |logical| {
            crate::store::SourceReadWork::admit(budget, logical_work_bound(logical)?)
                .map_err(|_| JournalIoError::Capacity)
        },
    )
}

/// Exact allocation classes of Command-kind proven frame decoding. This does
/// not construct a receipt or include its independent indexed fold workspace.
fn command_logical_work_bound(logical: &[u8]) -> Result<u64, JournalIoError> {
    let (count, payload) = logical_frame_layout(logical)?;
    if logical[26] != JournalFrameKind::Command as u8 {
        return Err(JournalIoError::Corrupt);
    }
    let charge = |bytes, alignment| {
        riffdb_types::owned_allocation_capacity_bound_v1(bytes, alignment)
            .map_err(|_| JournalIoError::Capacity)
    };
    let vector = count
        .checked_mul(std::mem::size_of::<JournalMutation>())
        .ok_or(JournalIoError::Capacity)?;
    let vector = charge(vector, std::mem::align_of::<JournalMutation>())?;
    // Each key/value becomes one Box<[u8]> via an exact-capacity Vec. Sum their
    // requested payload and the shared allocator overhead for at most2N buffers.
    let overhead = charge(1, 1)?
        .checked_sub(2)
        .ok_or(JournalIoError::Capacity)?;
    let buffers = u64::try_from(count)
        .ok()
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_mul(overhead))
        .ok_or(JournalIoError::Capacity)?;
    let payload = u64::try_from(payload)
        .ok()
        .and_then(|n| n.checked_mul(2))
        .ok_or(JournalIoError::Capacity)?;
    vector
        .checked_add(buffers)
        .and_then(|n| n.checked_add(payload))
        .and_then(|n| n.checked_add(std::mem::size_of::<JournalFrame>() as u64))
        .ok_or(JournalIoError::Capacity)
}
pub(crate) fn read_source_command_retained_frame_v1(
    media: &dyn JournalMedia,
    candidates: &[PathBuf],
    database_id: DatabaseId,
    placement: FramePlacement,
    generation: u64,
    budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
) -> Result<(JournalFrame, crate::store::SourceReadWork), JournalIoError> {
    let _physical_work =
        crate::store::SourceReadWork::admit(budget, physical_work_bound(placement)?)
            .map_err(|_| JournalIoError::Capacity)?;
    read_retained_frame_from_with_admission(
        media,
        candidates,
        database_id,
        placement,
        generation,
        |logical| {
            crate::store::SourceReadWork::admit(budget, command_logical_work_bound(logical)?)
                .map_err(|_| JournalIoError::Capacity)
        },
    )
}
#[cfg(test)]
type SourceFramePause = (
    PathBuf,
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);
#[cfg(test)]
std::thread_local! {static SOURCE_FRAME_PAUSE:std::cell::RefCell<Option<SourceFramePause>>=
const {std::cell::RefCell::new(None)};}
#[cfg(test)]
pub(crate) fn install_source_frame_pause_for_test(
    path: PathBuf,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
) -> bool {
    SOURCE_FRAME_PAUSE.with(|slot| slot.replace(Some((path, entered, release))).is_none())
}
#[cfg(test)]
pub(super) fn before_source_candidate_open_for_test(path: &Path) -> Result<(), JournalIoError> {
    let pause = SOURCE_FRAME_PAUSE.with(|slot| {
        let matches = slot
            .borrow()
            .as_ref()
            .is_some_and(|(selected, _, _)| selected == path);
        matches.then(|| slot.take()).flatten()
    });
    if let Some((_, entered, release)) = pause {
        entered.send(()).map_err(|_| JournalIoError::Io)?;
        release.recv().map_err(|_| JournalIoError::Io)?;
    }
    Ok(())
}
