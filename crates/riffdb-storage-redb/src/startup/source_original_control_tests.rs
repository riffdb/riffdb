//! Exact persistent control ownership on the real captured Original lease.
use super::*;

// req: PRJ-004, STO-020, REC-001
#[test]
fn actual_original_prefix_lease_control_remains_admitted_through_drop_and_partial_capture_refusal()
{
    let (_root, ports, _) = fixture();
    let budget = ports.shared.database.source_working_budget();
    let before = budget.observation().unwrap();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let live = budget.observation().unwrap();
    assert!(prefix.belongs_to_ports(&ports));
    assert_eq!(prefix.captured_through(), CommitSequence::first());
    assert!(prefix.captured_active_catalog_pointer().is_some());
    eprintln!("actual_original_capture_complete_before_header_check");
    let total_delta = live
        .reserved_bytes
        .checked_sub(before.reserved_bytes)
        .unwrap();
    let pin_control = prefix.retained_pin_control_bytes_for_test();
    let header_delta = total_delta.checked_sub(pin_control).unwrap();
    eprintln!(
        "actual_original_lease_control_total_delta={} pin_bytes={} header_delta={} required_header_bytes={}",
        total_delta,
        pin_control,
        header_delta,
        std::mem::size_of::<crate::store::OriginalPrefixRetentionLease>()
    );
    assert_eq!(
        header_delta,
        u64::try_from(std::mem::size_of::<
            crate::store::OriginalPrefixRetentionLease,
        >())
        .unwrap(),
        "persistent lease header must be admitted separately from its actual retained pin"
    );
    assert!(
        live.owned_bytes > before.owned_bytes,
        "actual captured pointer retains its separate owned graph"
    );
    assert!(budget.try_acquire_worker().is_err());
    drop(prefix);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_ok());
    // Permit the header but not the existing512KiB metadata scratch. The real
    // capture must unwind its partially prepared control and its worker claim.
    let before = budget.observation().unwrap();
    let header = u64::try_from(std::mem::size_of::<
        crate::store::OriginalPrefixRetentionLease,
    >())
    .unwrap();
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let pressure = budget.reserve(available - header).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(
        matches!(ports.capture_original_prefix_retention(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert_live_gauges_equal(budget.observation().unwrap(), saturated);
    assert!(budget.try_acquire_worker().is_ok());
    drop(pressure);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
}
