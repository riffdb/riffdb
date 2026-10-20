//! Streaming one object/prefix at a time; no retained primary snapshot or map.
use super::*;
use riffdb_storage_api::ProjectionSourceLifecycleV1 as Life;

pub(super) fn step_objects(
    census: &mut SourceUsageCensus<'_>,
    read: &CensusRead<'_>,
    page: &mut PageBudget,
) -> Result<(), StorageError> {
    if census.current.is_none() {
        // Start at most one object per step. Its fixed metadata lookups therefore
        // begin with an empty128/4MiB page; later steps only stream this prefix.
        let encoded = match read.next(N::ProjectionSourceObjects, &census.after, None, page)? {
            ReadRow::PageFull => return Ok(()),
            ReadRow::End => {
                census.after.clear();
                census.phase = Phase::Rows;
                return Ok(());
            }
            ReadRow::Row(row) => row,
        };
        let object = decode_projection_source_object_admitted_v1(encoded.value(), read.budget())
            .map_err(decode_error)?
            .into_parts()
            .0;
        let locator = decode_source_object_key(encoded.key()).map_err(|_| integrity())?;
        if locator != object.descriptor().reference().locator() {
            return Err(integrity());
        }
        check::validate_object_location(
            &object,
            census.lineage.ok_or_else(integrity)?,
            census.allocator.ok_or_else(integrity)?,
            read.source_through(),
        )?;
        if add(object.descriptor().canonical_row_bytes(), encoded.bytes())?
            > riffdb_storage_api::MAX_PROJECTION_SOURCE_OBJECT_BYTES
        {
            return Err(limit());
        }
        let selected = root_for_identity(read, object.descriptor().reference().identity(), page)?;
        if object.lifecycle() == Life::Published {
            if selected
                .as_ref()
                .is_none_or(|root| !root.matches_published_object(&object))
            {
                return Err(integrity());
            }
        } else if selected
            .as_ref()
            .is_some_and(|root| root.reference().locator() == locator)
        {
            return Err(integrity());
        }
        let (maintenance, maintenance_bytes) = match read.point(
            N::ProjectionSourceMaintenance,
            &source_maintenance_key(locator),
            page,
        )? {
            ReadRow::End => (None, 0),
            ReadRow::PageFull => return Err(integrity()),
            ReadRow::Row(row) => {
                let maintenance =
                    decode_projection_source_maintenance_admitted_v1(row.value(), read.budget())
                        .map_err(decode_error)?
                        .into_parts()
                        .0;
                if maintenance.descriptor() != object.descriptor() {
                    return Err(integrity());
                }
                if maintenance
                    .last_copied_key()
                    .is_some_and(|key| source_row_locator_prefix(key).ok() != Some(locator))
                {
                    return Err(integrity());
                }
                let bytes = row.bytes();
                (Some(maintenance), bytes)
            }
        };
        if matches!(object.lifecycle(), Life::Copying | Life::Aborting) && maintenance.is_none() {
            return Err(integrity());
        }
        if object.lifecycle() == Life::Copying
            && maintenance
                .as_ref()
                .is_some_and(|m| m.expected_prior_root() != selected.as_deref())
        {
            return Err(integrity());
        }
        census.totals.total = add(census.totals.total, encoded.bytes())?;
        save_key(&mut census.after, encoded.key());
        census.row_after.clear();
        census.current = Some(CurrentObject {
            object,
            maintenance,
            object_bytes: encoded.bytes(),
            maintenance_bytes,
            rows: 0,
            bytes: 0,
        });
    }
    let current = census.current.as_mut().ok_or_else(integrity)?;
    let prefix = source_row_prefix(current.object.descriptor().reference().locator());
    while !page.full() {
        match read.next(
            N::ProjectionSourceRows,
            &census.row_after,
            Some(&prefix),
            page,
        )? {
            ReadRow::PageFull => return Ok(()),
            ReadRow::Row(row) => {
                validate_row(read, &row, &current.object)?;
                current.rows = current.rows.checked_add(1).ok_or_else(limit)?;
                current.bytes = add(current.bytes, row.bytes())?;
                if current.rows > current.object.descriptor().row_count()
                    || current.bytes > current.object.descriptor().canonical_row_bytes()
                {
                    return Err(integrity());
                }
                save_key(&mut census.row_after, row.key());
            }
            ReadRow::End => {
                check::finish_object(current, &census.row_after)?;
                if matches!(current.object.lifecycle(), Life::Copying | Life::Aborting) {
                    if census.totals.candidate.is_some() {
                        return Err(limit());
                    }
                    // Source7 intentionally does not persist volatile quota.
                    // Reconstruct a conservative complete candidate reservation
                    // from its exact descriptor, finite root widths, independent
                    // Maintenance envelope ceiling, and assigned allocator KV.
                    let descriptor = current.object.descriptor();
                    let future_root_value =
                        projection_source_root_future_encoded_bound_v1(descriptor.reference())
                            .map_err(crate::error::codec_error)?
                            .get() as u64;
                    let future_root = add(
                        source_root_key_len(descriptor.reference().identity()) as u64,
                        future_root_value,
                    )?;
                    let maximum_maintenance = add(
                        SOURCE_LOCATOR_KEY_BYTES as u64,
                        riffdb_storage_api::MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64,
                    )?;
                    let full = add(
                        add(
                            add(
                                add(descriptor.canonical_row_bytes(), current.object_bytes)?,
                                future_root,
                            )?,
                            maximum_maintenance,
                        )?,
                        census.totals.allocator_bytes,
                    )?;
                    let actual = add(
                        add(
                            add(current.bytes, current.object_bytes)?,
                            current.maintenance_bytes,
                        )?,
                        census.totals.allocator_bytes,
                    )?;
                    // Existing usage construction rechecks the whole compiled
                    // candidate ceiling; no smaller group/object limits are made.
                    census.totals.candidate = Some(Candidate {
                        locator: descriptor.reference().locator(),
                        full,
                        actual,
                        maintenance_seen: false,
                    });
                }
                census.totals.object_rows =
                    add(census.totals.object_rows, u64::from(current.rows))?;
                census.totals.object_row_bytes =
                    add(census.totals.object_row_bytes, current.bytes)?;
                census.current = None;
                census.row_after.clear();
                return Ok(());
            }
        }
    }
    Ok(())
}
