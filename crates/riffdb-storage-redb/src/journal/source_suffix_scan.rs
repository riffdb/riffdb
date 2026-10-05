//! ADR-0271's small closed source drain; ordinary recovery limits are unchanged.
use super::*;

pub(crate) const SOURCE_SUFFIX_TRANSITIONS: usize = 256;
pub(crate) const SOURCE_SUFFIX_BYTES: usize = 1024 * 1024;

pub(crate) fn scan_online_source_suffix_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected_database: DatabaseId,
    mut visit: impl FnMut(&JournalFrame, Option<StubLocator>) -> Result<(), JournalIoError>,
) -> Result<Option<(JournalFileHeader, JournalScanTail)>, JournalIoError> {
    let mut file = media.open_read(path).map_err(|_| JournalIoError::Io)?;
    // Active native lanes have already upgraded any empty legacy journal.
    // Refuse an unknown header before allocating or reading any frame payload.
    let extent = read_selected_extent_header(&mut file, true)?;
    if extent.logical.database_id() != expected_database {
        return Err(JournalIoError::Corrupt);
    }
    let (database_id, generation) = (extent.logical.database_id(), extent.generation);
    let walk = walk_extent_frames_with_limits(
        &mut file,
        &extent,
        ExtentWalk::FromAnchor,
        extent_file_bytes(extent.data_bytes),
        true,
        |frame, placement| {
            visit(
                frame,
                Some(StubLocator {
                    database_id,
                    generation,
                    placement,
                }),
            )
        },
    )?;
    if walk.footer {
        return Err(JournalIoError::Corrupt);
    }
    Ok(Some((extent.logical, walk.tail)))
}

#[cfg(test)]
#[path = "source_suffix_scan_tests.rs"]
mod tests;
