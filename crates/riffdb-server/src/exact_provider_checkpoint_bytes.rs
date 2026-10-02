//! Admitted immutable encoded checkpoint input; semantic decoding remains separate.
// req: OQ-040
use super::RebuildFailure;
use riffdb_types::exact_provider_budget_v1::{ProviderAllocation, ProviderBudget};
use std::io::Read;
use std::sync::Arc;

pub(super) type CheckpointBytes = Arc<ProviderAllocation<Vec<u8>>>;

/// Length comes from the already opened file. The allocation never grows; a
/// changed length refuses rather than permitting an unadmitted resize.
pub(super) fn admit_checkpoint_bytes(
    reader: &mut impl Read,
    length: u64,
    maximum: usize,
    budget: &ProviderBudget,
) -> Result<CheckpointBytes, RebuildFailure> {
    let length = usize::try_from(length).map_err(|_| RebuildFailure::Integrity)?;
    if length > maximum {
        return Err(RebuildFailure::Integrity);
    }
    // Two times exact capacity plus root/Arc/ledger/allocator overhead. There
    // is no collection census or decoded provider allocation in this helper.
    let allowance = u64::try_from(length)
        .ok()
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(2048))
        .ok_or(RebuildFailure::Integrity)?;
    let work = budget
        .reserve(allowance)
        .map_err(|_| RebuildFailure::Transient)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| RebuildFailure::Transient)?;
    bytes.resize(length, 0);
    reader
        .read_exact(&mut bytes)
        .map_err(|_| RebuildFailure::Transient)?;
    let mut extra = [0_u8; 1];
    if reader
        .read(&mut extra)
        .map_err(|_| RebuildFailure::Transient)?
        != 0
    {
        return Err(RebuildFailure::Integrity);
    }
    work.commit_value(bytes)
        .map_err(|_| RebuildFailure::Transient)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
    use std::cell::Cell;
    use std::io::{Cursor, Error, ErrorKind, Result as IoResult};

    struct Reader<'a> {
        bytes: Cursor<&'a [u8]>,
        reads: &'a Cell<u32>,
        fail: bool,
    }
    impl Read for Reader<'_> {
        fn read(&mut self, output: &mut [u8]) -> IoResult<usize> {
            self.reads.set(self.reads.get() + 1);
            if self.fail {
                return Err(Error::from(ErrorKind::Other));
            }
            self.bytes.read(output)
        }
    }

    #[test]
    fn refused_admission_does_not_read_or_materialize_checkpoint_bytes() {
        let budget = ProviderBudget::new();
        let occupied = budget.reserve(EXACT_PROVIDER_BUDGET_BYTES - 128).unwrap();
        let reads = Cell::new(0);
        let mut reader = Reader {
            bytes: Cursor::new(b"checkpoint"),
            reads: &reads,
            fail: false,
        };
        assert!(matches!(
            admit_checkpoint_bytes(&mut reader, 10, 1024, &budget),
            Err(RebuildFailure::Transient)
        ));
        assert_eq!(reads.get(), 0);
        drop(occupied);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn actual_buffer_capture_keeps_its_charge_until_final_reference() {
        let budget = ProviderBudget::new();
        let bytes =
            admit_checkpoint_bytes(&mut Cursor::new(b"checkpoint"), 10, 1024, &budget).unwrap();
        assert_eq!(bytes.as_slice(), b"checkpoint");
        let capture = bytes.clone();
        drop(bytes);
        let counters = budget.observation().unwrap();
        assert_eq!(counters.reserved_bytes, 0);
        assert_eq!(counters.owned_bytes, 2068);
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn oversized_input_is_refused_before_reading() {
        let budget = ProviderBudget::new();
        let reads = Cell::new(0);
        let mut reader = Reader {
            bytes: Cursor::new(b"checkpoint"),
            reads: &reads,
            fail: false,
        };
        assert!(matches!(
            admit_checkpoint_bytes(&mut reader, 10, 9, &budget),
            Err(RebuildFailure::Integrity)
        ));
        assert_eq!(reads.get(), 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn short_growing_and_failed_reads_release_only_their_allowance() {
        let budget = ProviderBudget::new();
        let unrelated = budget.reserve(4096).unwrap();
        for (declared, fail) in [(11, false), (9, false), (10, true)] {
            let reads = Cell::new(0);
            let mut reader = Reader {
                bytes: Cursor::new(b"checkpoint"),
                reads: &reads,
                fail,
            };
            assert!(admit_checkpoint_bytes(&mut reader, declared, 1024, &budget).is_err());
            let counters = budget.observation().unwrap();
            assert_eq!(counters.reserved_bytes, 4096);
            assert_eq!(counters.owned_bytes, 0);
        }
        drop(unrelated);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
