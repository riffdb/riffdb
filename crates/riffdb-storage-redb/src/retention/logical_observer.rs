//! Complete original eligible prefix, independent of the next deletion batch.
use super::*;
use crate::store::elapsed_us;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

const CAPTURE_INTERVAL: Duration = Duration::from_secs(60);
const MAX_AGE: Duration = Duration::from_secs(30);
const MAX_ROWS: usize = 256;

#[derive(Default)]
pub(in crate::retention) struct Observer {
    probe: Option<Probe>,
    next_capture: Option<Instant>,
}

impl Observer {
    // Diagnostic failure invalidates this measurement, never changes the
    // authoritative lane's own pruning plan or admission result.
    pub(in crate::retention) fn report_refusal(&mut self, error: &StorageError) {
        self.probe = None;
        eprintln!(
            "riffdb-retention-logical-bytes-refusal-v1\tkind={:?}",
            error.kind()
        );
    }
    pub(in crate::retention) fn retire(&mut self) {
        self.probe = None;
    }

    pub(in crate::retention) fn tick(
        &mut self,
        ports: &crate::store::RedbOperationalPorts,
        retain: NonZeroU64,
    ) -> Result<(), StorageError> {
        let now = Instant::now();
        if self.next_capture.is_none_or(|next| now >= next) {
            self.probe = Some(Probe::capture(ports, retain, now)?);
            self.next_capture = Some(
                now.checked_add(CAPTURE_INTERVAL)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
            );
        }
        self.advance(ports, now)
    }

    pub(in crate::retention) fn advance(
        &mut self,
        ports: &crate::store::RedbOperationalPorts,
        now: Instant,
    ) -> Result<(), StorageError> {
        let Some(probe) = self.probe.as_mut() else {
            return Ok(());
        };
        if let Err(error) = probe.advance(ports, now) {
            self.probe = None;
            return Err(error);
        }
        let observed = Instant::now();
        if observed.saturating_duration_since(probe.captured) >= MAX_AGE {
            self.probe = None;
        } else if probe.completed {
            // Only complete original-prefix values are emitted. This is one
            // component; source receipts and physical extents remain separate.
            let root = probe.root;
            let incarnation = probe.incarnation;
            let watermark = probe.watermark;
            let head = probe.head;
            let ceiling = probe.ceiling;
            let through = probe.next - 1;
            let rows = probe.rows;
            let values = probe.bytes.values;
            let keys = probe.bytes.keys;
            let age_us = elapsed_us(probe.captured, observed);
            let pages = probe.pages;
            eprintln!(
                "riffdb-retention-logical-bytes-v1\troot={root}\tincarnation={incarnation}\twatermark={watermark}\thead={head}\tceiling={ceiling}\tthrough={through}\trows={rows}\tcanonical_history_value_bytes={values}\tremoved_key_bytes={keys}\tcapture_age_us={age_us}\tpages={pages}"
            );
            self.probe = None;
        }
        Ok(())
    }
}

struct Probe {
    read: Option<RedbReadAccess>,
    root: u64,
    database_id: riffdb_types::DatabaseId,
    incarnation: u64,
    watermark: u64,
    head: u64,
    ceiling: u64,
    next: u64,
    rows: u64,
    bytes: Bytes,
    pages: u64,
    captured: Instant,
    completed: bool,
}

impl Probe {
    fn capture(
        ports: &crate::store::RedbOperationalPorts,
        retain: NonZeroU64,
        now: Instant,
    ) -> Result<Self, StorageError> {
        let read = ports.begin_composite_read()?;
        let derived = ports.begin_derived_read()?;
        let incarnation = load_history_incarnation(&read)?;
        let watermark = read
            .read_value(JournalTable::Meta, META_RETENTION_WATERMARK.as_bytes())?
            .map(|bytes| {
                decode_retention_watermark_v1(&bytes)
                    .map_err(crate::error::codec_error)
                    .map(|value| value.into_parts().0)
            })
            .transpose()?;
        if watermark
            .as_ref()
            .is_some_and(|watermark| watermark.history_incarnation() != incarnation)
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let watermark = watermark.map_or(0, |value| value.watermark_sequence());
        let head = prepared::access_head(&read)?;
        let mut fences =
            online::collect_online_fencing_inputs(&read, &derived, &load_holds(&read)?)?;
        fences.durable_application_head = Some(head);
        let ceiling = compute_max_permissible_watermark(&fences)
            .0
            .unwrap_or(0)
            .min(head.saturating_sub(retain.get()));
        let database_id = crate::store::read_identity_from_read_transaction(&read)?;
        let root = match &read {
            RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => root.identity(),
            RedbReadAccess::Composite(view) => view.checkpoint_root().identity(),
        };
        let next = watermark
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        for (key, value) in
            read.read_range_reverse(JournalTable::Commits, &[], &next.to_be_bytes(), 1)?
        {
            let physical = decode_application_sequence_key(&key)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if online::row_last(&value, physical)? >= next {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
        Ok(Self {
            read: Some(read),
            root,
            database_id,
            incarnation,
            watermark,
            head,
            ceiling,
            next,
            rows: 0,
            bytes: Bytes::default(),
            pages: 0,
            captured: now,
            completed: false,
        })
    }

    fn finish(&mut self) {
        self.read = None;
        self.completed = true;
    }

    fn advance(
        &mut self,
        ports: &crate::store::RedbOperationalPorts,
        now: Instant,
    ) -> Result<(), StorageError> {
        if now.saturating_duration_since(self.captured) >= MAX_AGE {
            self.read = None;
            self.completed = false;
            return Ok(());
        }
        if self.read.is_none() {
            return Ok(());
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let mut page_bytes = 0_usize;
        for _ in 0..MAX_ROWS {
            if Instant::now().saturating_duration_since(self.captured) >= MAX_AGE {
                self.read = None;
                self.completed = false;
                return Ok(());
            }
            if self.next > self.ceiling {
                self.finish();
                break;
            }
            let read = self
                .read
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let (key, value) = read
                .read_range_to(JournalTable::Commits, &self.next.to_be_bytes(), None, 1)?
                .pop()
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let physical = decode_application_sequence_key(&key)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if physical.get() != self.next {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if !ports.shared.segment_extent_is_sealed(&value)? {
                self.finish();
                break;
            }
            let canonical = crate::history_stub::resolve_commits_value(&value)?;
            let segment = online::decode_segment_row(&canonical)?;
            if segment.as_ref().is_some_and(|segment| {
                segment.first_commit_sequence() != physical
                    || segment.database_id() != self.database_id
                    || segment.history_incarnation() != self.incarnation
            }) {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let end = segment.as_ref().map_or(physical.get(), |segment| {
                segment.last_commit_sequence().get()
            });
            if end > self.ceiling {
                self.finish();
                break;
            }
            let next_bytes = page_bytes
                .checked_add(canonical.len())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if next_bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES {
                break;
            }
            let mut views = CountingViews::new(read);
            if let Some(segment) = &segment {
                for event in segment
                    .commands()
                    .iter()
                    .flat_map(|command| command.events())
                {
                    if !delivered(read, event.event_id())? {
                        self.finish();
                        return Ok(());
                    }
                }
            }
            let (lower, upper) = event_keyed_range_bounds(self.next, end);
            let mut row_bytes = Bytes::default();
            let mut workspace_bytes = canonical.len();
            for table in [
                JournalTable::Events,
                JournalTable::Outbox,
                JournalTable::OutboxStatus,
            ] {
                let mut next_key = lower.to_vec();
                let exclusive = successor(upper.to_vec())?;
                let mut count = 0;
                while let Some((key, value)) =
                    read.read_range(table, &next_key, &exclusive, 1)?.pop()
                {
                    count += 1;
                    workspace_bytes = workspace_bytes
                        .checked_add(key.len())
                        .and_then(|n| n.checked_add(value.len()))
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                    if workspace_bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES
                        || count > riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES
                    {
                        return Err(storage_error(StorageErrorKind::LimitExceeded));
                    }
                    if table == JournalTable::Outbox
                        && !delivered(
                            read,
                            decode_event_key(&key)
                                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?,
                        )?
                    {
                        self.finish();
                        return Ok(());
                    }
                    row_bytes.add(value.len(), key.len())?;
                    next_key = successor(key.into_vec())?;
                }
            }
            match segment {
                Some(segment) => {
                    for command in segment.commands() {
                        materialize_retained_capsule_views(
                            &mut views,
                            command.base(),
                            command.events(),
                        )?;
                    }
                }
                None => materialize_single(&mut views, &canonical, physical)?,
            }
            row_bytes.add(canonical.len(), key.len())?;
            row_bytes.extend(views.bytes()?)?;
            self.bytes.extend(row_bytes)?;
            self.next = end
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            self.rows = self
                .rows
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            page_bytes = next_bytes;
        }
        if self.next > self.ceiling {
            self.finish();
        }
        Ok(())
    }
}

fn successor(mut key: Vec<u8>) -> Result<Vec<u8>, StorageError> {
    for byte in key.iter_mut().rev() {
        if *byte < u8::MAX {
            *byte += 1;
            return Ok(key);
        }
        *byte = 0;
    }
    Err(storage_error(StorageErrorKind::LimitExceeded))
}

fn delivered(read: &RedbReadAccess, event: riffdb_types::EventId) -> Result<bool, StorageError> {
    read.read_value(JournalTable::OutboxStatus, &event.to_be_bytes())?
        .map(|bytes| {
            codec::decode_outbox_status_v1(&bytes).map(|status| {
                if status.value().event_id() == event {
                    Ok(matches!(
                        status.value().state(),
                        riffdb_storage_api::OutboxDeliveryStateV1::Delivered { .. }
                    ))
                } else {
                    Err(storage_error(StorageErrorKind::CorruptData))
                }
            })
        })
        .transpose()
        .and_then(|value| value.unwrap_or(Ok(false)))
}

#[cfg(test)]
#[path = "logical_observer_tests.rs"]
mod tests;

fn materialize_single(
    views: &mut CountingViews<'_>,
    canonical: &[u8],
    physical: CommitSequence,
) -> Result<(), StorageError> {
    match riffdb_storage_api::decode_command_capsule_v2(canonical) {
        Ok(capsule) => {
            if capsule.value().commit_sequence() != physical {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            return materialize_retained_capsule_views(
                views,
                capsule.value().base(),
                capsule.value().events(),
            );
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(crate::error::codec_error(error)),
    }
    let (is_capsule, references) =
        match riffdb_storage_api::decode_command_capsule_event_references(canonical) {
            Ok(references) => (true, references.into_parts().0),
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                let references =
                    riffdb_storage_api::decode_commit_event_references_with_revision(canonical)
                        .map_err(crate::error::codec_error)?;
                (false, references.value().references().to_vec())
            }
            Err(error) => return Err(crate::error::codec_error(error)),
        };
    let mut events = Vec::with_capacity(references.len());
    for reference in references {
        let bytes = views
            .read
            .read_value(
                JournalTable::Events,
                &crate::keys::encode_event_key(reference.event_id()),
            )?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let event = codec::decode_durable_event_v1(&bytes)?.into_parts().0;
        if !reference.matches(&event) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        events.push(event);
    }
    if !is_capsule {
        let record = riffdb_storage_api::decode_commit_record_with_events(canonical, events)
            .map_err(crate::error::codec_error)?;
        if record.value().commit_sequence() != physical {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(());
    }
    let capsule = riffdb_storage_api::decode_command_capsule_v1(canonical, events)
        .map_err(crate::error::codec_error)?;
    if capsule.value().commit_sequence() != physical {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    materialize_retained_capsule_views(views, capsule.value(), &[])
}
