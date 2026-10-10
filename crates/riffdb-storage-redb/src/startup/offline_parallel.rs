//! Bounded same-pin offline row inspection. No writer or evidence authority.
use super::*;
use crate::error::storage_error;
use riffdb_storage_api::{StorageError, StorageErrorKind};

pub(super) const MAX_ROWS: usize = 64;
const WORKERS: usize = 4;

pub(super) fn inspect_rows<T: Send>(
    count: usize,
    inspect: impl Fn(usize) -> Result<T, StorageError> + Sync,
) -> Result<Vec<Result<T, StorageError>>, StorageError> {
    if count > MAX_ROWS {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    if count < WORKERS * 2 {
        return Ok((0..count).map(inspect).collect());
    }
    std::thread::scope(|scope| {
        let chunk = count.div_ceil(WORKERS);
        let mut handles = Vec::with_capacity(WORKERS);
        let mut spawn_failure = None;
        for start in (0..count).step_by(chunk) {
            let end = (start + chunk).min(count);
            let inspect = &inspect;
            match std::thread::Builder::new()
                .name("riffdb-offline-check".to_owned())
                .spawn_scoped(scope, move || (start..end).map(inspect).collect::<Vec<_>>())
            {
                Ok(handle) => handles.push(handle),
                Err(_) => {
                    spawn_failure = Some(storage_error(StorageErrorKind::Unavailable));
                    break;
                }
            }
        }
        // Join every started worker, even after spawn failure or a panic. No
        // worker can outlive the immutable pin or the owner's mutation lease.
        let mut results = Vec::with_capacity(count);
        let mut join_failure = None;
        for handle in handles {
            match handle.join() {
                Ok(chunk) => results.extend(chunk),
                Err(_) => {
                    join_failure = Some(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
        }
        if let Some(error) = spawn_failure.or(join_failure) {
            return Err(error);
        }
        if results.len() != count {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        // Results retain physical row order, irrespective of completion order.
        Ok(results)
    })
}

/// Original row-inspector input, or its existing command-cache result. The
/// stateful command cache stays on the owner thread; workers share only the
/// immutable engine pin and immutable publication facts.
type RowInspection = Result<Option<StructuralFinding>, StorageError>;
type AuditPage = Option<Vec<RowInspection>>;

enum AuditInspection {
    Ready(RowInspection),
    Row {
        index: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

impl RedbStructuralEvidenceSession {
    pub(super) fn inspect_offline_audit_page(
        &mut self,
        position: u64,
        requested: u64,
    ) -> Result<AuditPage, StorageError> {
        if self.purpose != EvidenceOpenPurpose::OfflineIntegrityScrub
            || self.clean_close_fast
            || self.bounded_dirty_startup
            || self.checkpoint.is_some()
        {
            return Ok(None);
        }
        let Some(cursors) = self.structural_cursors.as_ref() else {
            return Ok(None);
        };
        let phase = cursors.phase;
        if !matches!(phase, 21 | 22) {
            return Ok(None);
        }
        let remaining = self.structural_counts[phase]
            .checked_sub(cursors.consumed_in_phase)
            .ok_or_else(invariant)?;
        if remaining == 0 {
            return Ok(None);
        }
        let count = requested.min(remaining).min(MAX_ROWS as u64) as usize;
        let mut rows = Vec::with_capacity(count);
        let mut bytes = 0usize;
        // Each copied key/value is bounded before allocation by the common
        // structural reader. Admit its maximum combined charge before reading
        // another row. Thus this page owns at most 64 MiB of input, not 64
        // maximum-sized envelopes. Worker outputs remain at most 64 findings.
        const MAX_ONE_ROW: usize = 2 * riffdb_storage_api::MAX_DURABLE_ENCODED_CONTENT_BYTES;
        const MAX_PAGE_INPUT: usize = 2 * MAX_ONE_ROW;
        for offset in 0..count {
            if bytes > MAX_PAGE_INPUT - MAX_ONE_ROW {
                break;
            }
            let row = self.next_structural_row_raw();
            let (actual_phase, index, key, value) = match row {
                Ok(row) => row,
                Err(error) => {
                    rows.push(AuditInspection::Ready(Err(error)));
                    break;
                }
            };
            let relative = position
                .checked_add(offset as u64)
                .and_then(|v| v.checked_sub(1))
                .ok_or_else(invariant)?;
            let before = self.structural_counts[..phase]
                .iter()
                .try_fold(0_u64, |sum, count| {
                    sum.checked_add(*count).ok_or_else(invariant)
                })?;
            if actual_phase != phase
                || index != relative.checked_sub(before).ok_or_else(invariant)?
            {
                rows.push(AuditInspection::Ready(Err(invariant())));
                break;
            }
            bytes = bytes
                .checked_add(key.len())
                .and_then(|v| v.checked_add(value.len()))
                .ok_or_else(limit_exceeded)?;
            if phase == 21 {
                let inspect_index = keys::decode_audit_key(&key)
                    .map(|sequence| sequence.get().saturating_sub(1))
                    .unwrap_or(index);
                match inspect_cached_command_audit_row(
                    self.validation_read.as_ref().ok_or_else(invariant)?,
                    &self.command_history,
                    inspect_index,
                    &key,
                    &value,
                ) {
                    Ok(Some(finding)) => rows.push(AuditInspection::Ready(Ok(finding))),
                    Ok(None) => rows.push(AuditInspection::Row {
                        index: inspect_index,
                        key,
                        value,
                    }),
                    Err(error) => {
                        rows.push(AuditInspection::Ready(Err(error)));
                        break;
                    }
                }
            } else {
                rows.push(AuditInspection::Row { index, key, value });
            }
        }
        let read = self.validation_read.as_ref().ok_or_else(invariant)?;
        let context = StructuralInspectContext {
            inputs: &self.inputs,
            segments: &self.segments,
            binding_bundles: &self.binding_bundles,
            publication_audits: self.publication_audits.as_ref().ok_or_else(invariant)?,
            database_id: self.database_id,
        };
        let cancellation = self.cancellation.as_deref();
        let results = inspect_rows(rows.len(), |index| {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
            match &rows[index] {
                AuditInspection::Ready(result) => result.clone(),
                AuditInspection::Row { index, key, value } => {
                    inspect_table_row_from_bytes(read, &context, phase, *index, key, value)
                }
            }
        })?;
        if phase == 21 {
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase]
                .checked_add(results.len() as u64)
                .ok_or_else(limit_exceeded)?;
        }
        Ok(Some(results))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Barrier, Condvar, Mutex};

    // req: REC-006, STO-031
    #[test]
    fn offline_parallel_joins_all_inspections_before_reporting_first_failure() {
        let visited = AtomicU64::new(0);
        let results = inspect_rows(16, |index| {
            visited.fetch_or(1 << index, Ordering::SeqCst);
            if index == 0 {
                Err(storage_error(StorageErrorKind::CorruptData))
            } else if index == 12 {
                Err(storage_error(StorageErrorKind::Unavailable))
            } else {
                Ok(index)
            }
        });
        assert_eq!(
            visited.load(Ordering::SeqCst),
            (1 << 16) - 1,
            "all bounded workers must join before reporting an ordered row failure"
        );
        let results = results.expect("workers joined");
        assert_eq!(
            results[0].as_ref().unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            results[12].as_ref().unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(*results[15].as_ref().unwrap(), 15);
    }

    // req: REC-006, STO-031
    #[test]
    fn offline_parallel_preserves_row_order_when_later_workers_finish_first() {
        let barrier = Barrier::new(WORKERS);
        let completed = (Mutex::new(false), Condvar::new());
        let results = inspect_rows(16, |index| {
            if index.is_multiple_of(4) {
                barrier.wait();
            }
            if index == 0 {
                let (lock, ready) = &completed;
                let mut later = lock.lock().unwrap();
                while !*later {
                    later = ready.wait(later).unwrap();
                }
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if index == 15 {
                let (lock, ready) = &completed;
                *lock.lock().unwrap() = true;
                ready.notify_one();
            }
            Ok(index)
        })
        .expect("bounded workers joined");
        assert_eq!(results.len(), 16);
        for (index, result) in results.into_iter().enumerate() {
            if index == 0 {
                assert_eq!(result.unwrap_err().kind(), StorageErrorKind::CorruptData);
            } else {
                assert_eq!(result.unwrap(), index);
            }
        }
    }

    // req: REC-006, STO-031
    #[test]
    fn offline_parallel_refuses_oversized_batches_without_starting_work() {
        let visited = AtomicU64::new(0);
        let result = inspect_rows(MAX_ROWS + 1, |_| {
            visited.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::LimitExceeded);
        assert_eq!(visited.load(Ordering::SeqCst), 0);
    }
}
