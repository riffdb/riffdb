#![forbid(unsafe_code)]

//! Boundary evidence for the accepted independent migration ledgers.

use riffdb_storage_fjall_comparison::{
    MigrationCharge, MigrationLedgerError, paginate_migration_charges,
};

const FOUR_MIB: usize = 4 * 1024 * 1024;

#[test]
fn five_hundred_rows_fit_and_five_hundred_first_is_the_exact_continuation() {
    let rows = vec![
        MigrationCharge {
            evidence_bytes: 1,
            instruction_bytes: 1,
        };
        501
    ];
    let first = paginate_migration_charges(&rows, 0).expect("first bounded page");
    assert_eq!(first.range(), 0..500);
    assert_eq!(first.next(), Some(500));
    let second = paginate_migration_charges(&rows, 500).expect("strict continuation");
    assert_eq!(second.range(), 500..501);
    assert_eq!(second.next(), None);
}

#[test]
fn equal_four_mib_fits_and_one_byte_over_stays_unconsumed() {
    let rows = [
        MigrationCharge {
            evidence_bytes: FOUR_MIB,
            instruction_bytes: FOUR_MIB,
        },
        MigrationCharge {
            evidence_bytes: 1,
            instruction_bytes: 1,
        },
    ];
    let first = paginate_migration_charges(&rows, 0).expect("equal-bound row");
    assert_eq!(first.range(), 0..1);
    assert_eq!(first.evidence_bytes(), FOUR_MIB);
    assert_eq!(first.instruction_bytes(), FOUR_MIB);
    assert_eq!(first.next(), Some(1));
}

#[test]
fn either_independent_ledger_can_limit_the_page() {
    let evidence_limited = [
        MigrationCharge {
            evidence_bytes: FOUR_MIB,
            instruction_bytes: 1,
        },
        MigrationCharge {
            evidence_bytes: 1,
            instruction_bytes: 1,
        },
    ];
    let instruction_limited = [
        MigrationCharge {
            evidence_bytes: 1,
            instruction_bytes: FOUR_MIB,
        },
        MigrationCharge {
            evidence_bytes: 1,
            instruction_bytes: 1,
        },
    ];
    assert_eq!(
        paginate_migration_charges(&evidence_limited, 0)
            .expect("evidence-limited page")
            .range(),
        0..1
    );
    assert_eq!(
        paginate_migration_charges(&instruction_limited, 0)
            .expect("instruction-limited page")
            .range(),
        0..1
    );
}

#[test]
fn an_oversized_complete_row_fails_instead_of_splitting_or_looping() {
    let rows = [MigrationCharge {
        evidence_bytes: FOUR_MIB + 1,
        instruction_bytes: 1,
    }];
    assert_eq!(
        paginate_migration_charges(&rows, 0),
        Err(MigrationLedgerError::BoundExceeded)
    );
}
