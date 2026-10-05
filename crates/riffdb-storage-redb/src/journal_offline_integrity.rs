//! Complete journal planning shared by recovery and read-only offline verification.
use super::*;

pub(super) fn inspect_journal_with_media(
    media: &dyn JournalMedia,
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
    recover: bool,
) -> Result<(), JournalIoError> {
    let mut frames = Vec::new();
    let mut locators = Vec::new();
    let Some((header, tail)) =
        scan_located_journal_with_media(media, path, database_id, |frame, locator| {
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
    let snapshot = database.begin_read().map_err(|_| JournalIoError::Io)?;
    let v3 = crate::changelog_v3_journal::has_recovery_roots(&snapshot)
        .map_err(|_| JournalIoError::Corrupt)?
        || frames
            .iter()
            .any(crate::changelog_v3_journal::has_receipt_source);
    if v3 {
        let plan = crate::changelog_v3_journal::plan_recovery(
            &snapshot,
            &frames,
            riffdb_types::DualFrontier::new(redb_frontier.0, redb_frontier.1),
        )
        .map_err(|_| JournalIoError::Corrupt)?;
        drop(snapshot);
        if frames.is_empty() && !tail.incomplete_tail && redb_frontier == checkpoint_frontier {
            return Ok(());
        }
        if frames.is_empty()
            && (checkpoint_frontier.0 > redb_frontier.0 || checkpoint_frontier.1 > redb_frontier.1)
        {
            return Err(JournalIoError::Corrupt);
        }
        if !recover {
            return if plan.replay_from == frames.len() && !tail.incomplete_tail {
                Ok(())
            } else {
                Err(JournalIoError::Io)
            };
        }
        if plan.replay_from < frames.len() {
            replay_frames(
                database,
                &frames[plan.replay_from..],
                &locators[plan.replay_from..],
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
        return Ok(());
    }
    drop(snapshot);
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
        verify_replayed_tail(database, &frames)?;
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
