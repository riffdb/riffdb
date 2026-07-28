//! Dependency-free dual-ledger pagination probe for the accepted migration bounds.

use std::ops::Range;

/// Accepted maximum complete rows in one migration page.
pub(crate) const MAX_MIGRATION_ROWS: usize = 500;
/// Accepted maximum bytes in each independent migration page ledger.
pub(crate) const MAX_MIGRATION_BYTES: usize = 4 * 1024 * 1024;

/// One physical-order row's already checked independent charges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCharge {
    /// Exact observed key-plus-envelope evidence charge.
    pub evidence_bytes: usize,
    /// Conservative expected/replacement instruction charge.
    pub instruction_bytes: usize,
}

/// Closed dual-ledger pagination failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationLedgerError {
    /// The requested starting position is beyond the input.
    InvalidStart,
    /// A charge overflowed or one complete row cannot fit without splitting.
    BoundExceeded,
}

/// One nonempty bounded migration page or an explicit exact-end page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationLedgerPage {
    range: Range<usize>,
    evidence_bytes: usize,
    instruction_bytes: usize,
    next: Option<usize>,
}

impl MigrationLedgerPage {
    /// Returns the complete input-row range admitted to this page.
    #[must_use]
    pub fn range(&self) -> Range<usize> {
        self.range.clone()
    }

    /// Returns the exact evidence-ledger charge.
    #[must_use]
    pub const fn evidence_bytes(&self) -> usize {
        self.evidence_bytes
    }

    /// Returns the exact conservative instruction-ledger charge.
    #[must_use]
    pub const fn instruction_bytes(&self) -> usize {
        self.instruction_bytes
    }

    /// Returns the first unconsumed row, or `None` at exact end.
    #[must_use]
    pub const fn next(&self) -> Option<usize> {
        self.next
    }

    /// Returns whether this page is the explicit exact end.
    #[must_use]
    pub fn exact_end(&self) -> bool {
        self.range.is_empty() && self.next.is_none()
    }
}

/// Applies the unchanged 500-row/two-independent-4-MiB page rule.
///
/// A nonterminal page is always nonempty. The first row that would exceed
/// either ledger is left as the exact continuation.
pub fn paginate_migration_charges(
    rows: &[MigrationCharge],
    start: usize,
) -> Result<MigrationLedgerPage, MigrationLedgerError> {
    if start > rows.len() {
        return Err(MigrationLedgerError::InvalidStart);
    }
    if start == rows.len() {
        return Ok(MigrationLedgerPage {
            range: start..start,
            evidence_bytes: 0,
            instruction_bytes: 0,
            next: None,
        });
    }

    let mut end = start;
    let mut evidence = 0usize;
    let mut instructions = 0usize;
    while end < rows.len() && end - start < MAX_MIGRATION_ROWS {
        let row = rows[end];
        let next_evidence = evidence
            .checked_add(row.evidence_bytes)
            .ok_or(MigrationLedgerError::BoundExceeded)?;
        let next_instructions = instructions
            .checked_add(row.instruction_bytes)
            .ok_or(MigrationLedgerError::BoundExceeded)?;
        if next_evidence > MAX_MIGRATION_BYTES || next_instructions > MAX_MIGRATION_BYTES {
            break;
        }
        evidence = next_evidence;
        instructions = next_instructions;
        end += 1;
    }
    if end == start {
        return Err(MigrationLedgerError::BoundExceeded);
    }
    Ok(MigrationLedgerPage {
        range: start..end,
        evidence_bytes: evidence,
        instruction_bytes: instructions,
        next: (end < rows.len()).then_some(end),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instruction_ledger_can_stop_before_smaller_evidence_ledger() {
        let rows = [
            MigrationCharge {
                evidence_bytes: 1,
                instruction_bytes: MAX_MIGRATION_BYTES,
            },
            MigrationCharge {
                evidence_bytes: 1,
                instruction_bytes: 1,
            },
        ];
        let first = paginate_migration_charges(&rows, 0).expect("first row fits exactly");
        assert_eq!(first.range(), 0..1);
        assert_eq!(first.next(), Some(1));
        let second = paginate_migration_charges(&rows, 1).expect("continuation advances");
        assert_eq!(second.range(), 1..2);
        assert_eq!(second.next(), None);
    }

    #[test]
    fn one_complete_row_is_never_split() {
        let rows = [MigrationCharge {
            evidence_bytes: MAX_MIGRATION_BYTES + 1,
            instruction_bytes: 1,
        }];
        assert_eq!(
            paginate_migration_charges(&rows, 0),
            Err(MigrationLedgerError::BoundExceeded)
        );
    }
}
