//! Real retained-command regression: a receipt/source-object pin alone does not
//! fence the complete original application prefix needed by ADR-0316.
//! The native logical lease retains the needed commands without keeping a
//! whole primary redb read snapshot throughout evaluation.
use super::*;

// req: PRJ-004, STO-012, REC-001
#[test]
fn captured_original_prefix_requires_an_application_prune_fence() {
    let (_directory, mut ports) = super::native_source_admission::fixture();
    super::native_source_admission::complete_first_event(&mut ports);
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap().reserved_bytes;
    let pin = ports.capture_original_prefix_retention().unwrap();
    assert_eq!(pin.captured_through().get(), 2);
    assert!(budget.try_acquire_worker().is_err());
    assert!(ports.capture_original_prefix_retention().is_err());
    assert!(budget.observation().unwrap().reserved_bytes > before);
    let result = ports
        .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
        .unwrap();
    assert!(
        matches!(
            result,
            crate::OnlineRetentionStep::Held(crate::OnlineRetentionHold::Fenced(_))
        ),
        "a captured Original1..F source must prevent primary pruning its needed commit"
    );
    drop(pin);
    assert!(budget.try_acquire_worker().is_ok());
    assert_eq!(budget.observation().unwrap().reserved_bytes, before);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    ));
}

// req: PRJ-004, STO-012, REC-001
#[test]
fn original_capture_retains_same_history_and_actual_engine_identity() {
    let (_directory, ports) = super::native_source_admission::fixture();
    let (_other_directory, other_ports) = super::native_source_admission::fixture();
    let original = ports.capture_original_prefix_retention().unwrap();
    let other = other_ports.capture_original_prefix_retention().unwrap();
    // Both came from the same genuine durable fixture. Matching durable lineage
    // cannot transfer retention/worker custody to a different physical engine.
    assert_eq!(
        original.captured_history().lineage(),
        other.captured_history().lineage()
    );
    assert_eq!(
        original.captured_history().tail().frontier().application(),
        Some(original.captured_through())
    );
    assert!(original.tentative_source_locator().is_none());
    let alias = crate::RedbOperationalPorts {
        shared: Arc::clone(&ports.shared),
    };
    let before = other.originating_budget().observation().unwrap();
    assert!(original.belongs_to_ports(&alias));
    assert!(!original.belongs_to_ports(&other_ports));
    assert_eq!(other.originating_budget().observation().unwrap(), before);
}
