//! Bounded-root V3 recovery: one decoded frame at a time, two exact passes.
use super::*;
use riffdb_storage_api::{StorageError, StorageErrorKind};

pub(super) fn inspect(
    media: &dyn JournalMedia,
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
    recover: bool,
) -> Result<(), JournalIoError> {
    let frontier = read_redb_frontier(database)?;
    let snapshot = database.begin_read().map_err(|_| JournalIoError::Io)?;
    // Explicit offline verification retains its complete integrity walk.
    // Startup/replay validates only fixed roots and referenced original rows;
    // first-access validators and the complete scrub own other populations.
    if !recover {
        crate::changelog_v3_roots::validate_retained_history(&snapshot)
            .map_err(|_| JournalIoError::Corrupt)?;
    }
    let mut walk = crate::changelog_v3_journal::RecoveryWalk::new(
        &snapshot,
        riffdb_types::DualFrontier::new(frontier.0, frontier.1),
    )
    .map_err(recovery_error)?;
    let mut frames = 0_usize;
    let Some((header, tail)) =
        scan_located_journal_with_media(media, path, database_id, |frame, _| {
            walk.observe(frame).map_err(recovery_error)?;
            frames = frames.checked_add(1).ok_or(JournalIoError::Corrupt)?;
            Ok(())
        })?
    else {
        return Ok(());
    };
    let plan = walk.finish();
    drop(snapshot);
    let checkpoint = (
        header.checkpoint_sequence(),
        header.checkpoint_administration_sequence(),
    );
    if frames == 0 && !tail.incomplete_tail && frontier == checkpoint {
        return Ok(());
    }
    if frames == 0 && (checkpoint.0 > frontier.0 || checkpoint.1 > frontier.1) {
        return Err(JournalIoError::Corrupt);
    }
    if !recover {
        return if plan.replay_from == frames && !tail.incomplete_tail {
            Ok(())
        } else {
            Err(JournalIoError::Io)
        };
    }
    if plan.replay_from < frames {
        replay(
            media,
            database,
            path,
            database_id,
            plan.replay_from,
            frames,
            &header,
            &tail,
        )?;
    }
    #[cfg(test)]
    crate::changelog_v3_journal::recovery_crash_edge("before-reclaim");
    advance_journal_anchor_with_media(
        media,
        path,
        &JournalFileHeader::with_frontiers(
            database_id,
            plan.covered.application(),
            plan.covered.administration(),
            tail.last_hash,
        ),
    )?;
    #[cfg(test)]
    crate::changelog_v3_journal::recovery_crash_edge("reclaimed");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn replay(
    media: &dyn JournalMedia,
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
    replay_from: usize,
    frames: usize,
    header: &JournalFileHeader,
    tail: &JournalScanTail,
) -> Result<(), JournalIoError> {
    let mut tx = database.begin_write().map_err(|_| JournalIoError::Io)?;
    tx.set_two_phase_commit(true);
    tx.set_durability(Durability::Immediate)
        .map_err(|_| JournalIoError::Io)?;
    let mut index = 0_usize;
    let mut chain = crate::changelog_v3_journal::ChainedFrameMaterialization::default();
    let observed = scan_located_journal_with_media(media, path, database_id, |frame, locator| {
        if index >= replay_from {
            chain
                .materialize(&tx, frame, locator)
                .map_err(recovery_error)?;
        }
        index = index.checked_add(1).ok_or(JournalIoError::Corrupt)?;
        Ok(())
    })?;
    // Re-read bytes have no authority merely because a prior pass checked them.
    // Any substitution or changed terminal boundary aborts the whole transaction.
    if observed.as_ref() != Some(&(header.clone(), *tail)) || index != frames {
        return Err(JournalIoError::Corrupt);
    }
    chain.finish(&tx).map_err(recovery_error)?;
    #[cfg(test)]
    crate::changelog_v3_journal::recovery_crash_edge("staged");
    tx.commit().map_err(|_| JournalIoError::Io)?;
    #[cfg(test)]
    crate::changelog_v3_journal::recovery_crash_edge("committed");
    Ok(())
}

fn recovery_error(error: StorageError) -> JournalIoError {
    match error.kind() {
        StorageErrorKind::LimitExceeded => JournalIoError::Capacity,
        _ => JournalIoError::Corrupt,
    }
}
