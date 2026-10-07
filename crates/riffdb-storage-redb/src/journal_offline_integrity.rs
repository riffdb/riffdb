//! Complete journal planning shared by recovery and read-only offline verification.
use super::*;

pub(super) fn inspect_journal_with_media(
    media: &dyn JournalMedia,
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
    recover: bool,
) -> Result<(), JournalIoError> {
    let snapshot = database.begin_read().map_err(|_| JournalIoError::Io)?;
    if crate::changelog_v3_journal::has_recovery_roots(&snapshot)
        .map_err(|_| JournalIoError::Corrupt)?
    {
        drop(snapshot);
        return super::recovery_stream::inspect(media, database, path, database_id, recover);
    }
    drop(snapshot);
    let retained_limit = super::recovery_memory::legacy_retained_limit()?;
    let mut retained_bytes = 0_usize;
    let mut frames = Vec::new();
    let mut locators = Vec::new();
    let Some((header, tail)) =
        scan_located_journal_with_media(media, path, database_id, |frame, locator| {
            if crate::changelog_v3_journal::has_receipt_source(frame) {
                return Err(JournalIoError::Corrupt);
            }
            let bytes = super::recovery_memory::frame_owned_bound(frame)?
                .checked_add(
                    2 * std::mem::size_of::<JournalFrame>()
                        + 3 * std::mem::size_of::<Option<StubLocator>>(),
                )
                .ok_or(JournalIoError::Capacity)?;
            // Reserve descriptor growth before either push; boxed payloads
            // themselves are cloned once and have exact capacities.
            retained_bytes = retained_bytes
                .checked_add(bytes)
                .filter(|bytes| *bytes <= retained_limit)
                .ok_or(JournalIoError::Capacity)?;
            frames.push(frame.clone());
            locators.push(locator);
            Ok(())
        })?
    else {
        return Ok(());
    };
    let redb_frontier = read_redb_frontier(database)?;
    let checkpoint_frontier = (
        header.checkpoint_sequence(),
        header.checkpoint_administration_sequence(),
    );
    let tail_frontier = (tail.last_sequence, tail.last_administration_sequence);
    // An inactive or partial root cannot borrow the bounded V3 recovery
    // owner. The legacy path admits no receipt allocator or source cut.
    if frames
        .iter()
        .any(crate::changelog_v3_journal::has_receipt_source)
    {
        return Err(JournalIoError::Corrupt);
    }
    // An exact empty recovered extent needs no mutation. Preserving its
    // selected canonical header is required by ADR-0157 clean-close binding;
    // recycling an already-empty exact extent would manufacture a new header
    // generation on every otherwise read-only open.
    if frames.is_empty()
        && !tail.incomplete_tail
        && tail_frontier == checkpoint_frontier
        && redb_frontier == checkpoint_frontier
    {
        return Ok(());
    }
    if frames.is_empty()
        && !tail.incomplete_tail
        && tail_frontier == checkpoint_frontier
        && checkpoint_frontier.0 <= redb_frontier.0
        && checkpoint_frontier.1 <= redb_frontier.1
        && checkpoint_frontier != redb_frontier
    {
        if !recover {
            return Ok(());
        }
        return advance_journal_anchor_with_media(
            media,
            path,
            &JournalFileHeader::with_frontiers(
                database_id,
                redb_frontier.0,
                redb_frontier.1,
                tail.last_hash,
            ),
        );
    }
    if redb_frontier == checkpoint_frontier {
        if !frames.is_empty() {
            if !recover {
                return Err(JournalIoError::Io);
            }
            replay_frames(database, &frames, &locators)?;
        }
    } else if redb_frontier == tail_frontier && !frames.is_empty() {
        super::recovery_memory::verify_legacy_tail(database, frames)?;
    } else {
        return Err(JournalIoError::Corrupt);
    }
    if !recover {
        return if tail.incomplete_tail {
            Err(JournalIoError::Io)
        } else {
            Ok(())
        };
    }
    advance_journal_anchor_with_media(
        media,
        path,
        &JournalFileHeader::with_frontiers(
            database_id,
            tail.last_sequence,
            tail.last_administration_sequence,
            tail.last_hash,
        ),
    )
}
