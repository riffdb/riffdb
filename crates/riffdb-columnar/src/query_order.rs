//! Resolved row ordering and bounded retention for scan-query limits.
use super::*;
use std::collections::BinaryHeap;

pub(super) type MatchedRow = (PrimaryKeyBytes, Vec<CanonicalValue>, MergedRow);

pub(super) struct ResolvedOrder {
    index: usize,
    primary_key: bool,
    direction: SortDirection,
}

pub(super) fn resolve(
    definition: &RegisteredDefinition,
    order: &[OrderSpec],
) -> Result<Vec<ResolvedOrder>, QueryError> {
    order
        .iter()
        .map(|order| {
            validate_order_field(definition, order.field)?;
            let (index, primary_key) = match projected_field_index(definition, order.field) {
                Ok(index) => (index, false),
                Err(_) => (primary_key_field_index(definition, order.field)?, true),
            };
            Ok(ResolvedOrder {
                index,
                primary_key,
                direction: order.direction,
            })
        })
        .collect()
}

pub(super) fn compare(order: &[ResolvedOrder], left: &MatchedRow, right: &MatchedRow) -> Ordering {
    for term in order {
        let (left, right) = if term.primary_key {
            (&left.1[term.index], &right.1[term.index])
        } else {
            (&left.2.cells[term.index], &right.2.cells[term.index])
        };
        let cmp = compare_values(left, right);
        let cmp = match term.direction {
            SortDirection::Asc => cmp,
            SortDirection::Desc => cmp.reverse(),
        };
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    left.0.cmp(&right.0)
}

struct RankedRow<'a> {
    order: &'a [ResolvedOrder],
    row: MatchedRow,
}
impl PartialEq for RankedRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for RankedRow<'_> {}
impl PartialOrd for RankedRow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankedRow<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(self.order, &self.row, &other.row)
    }
}

pub(super) struct TopRows<'a> {
    order: &'a [ResolvedOrder],
    limit: usize,
    heap: BinaryHeap<RankedRow<'a>>,
}
impl<'a> TopRows<'a> {
    pub(super) fn new(order: &'a [ResolvedOrder], limit: usize) -> Self {
        Self {
            order,
            limit,
            heap: BinaryHeap::new(),
        }
    }

    pub(super) fn push(&mut self, row: MatchedRow) {
        if self.limit == 0 {
            return;
        }
        let candidate = RankedRow {
            order: self.order,
            row,
        };
        if self.heap.len() < self.limit {
            self.heap.push(candidate);
        } else if let Some(mut worst) = self.heap.peek_mut()
            && candidate < *worst
        {
            *worst = candidate;
        }
    }

    pub(super) fn into_rows(self) -> Vec<MatchedRow> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|item| item.row)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: OQ-020
    #[test]
    fn bounded_rows_match_full_order_with_nulls_mixed_directions_and_key_ties() {
        let rows = (0_u64..200)
            .map(|key| {
                (
                    PrimaryKeyBytes::from_entity_key_bytes(key.to_be_bytes().to_vec()),
                    vec![CanonicalValue::U64(key)],
                    MergedRow {
                        entity_version: EntityVersion::new(1).unwrap(),
                        cells: vec![if key % 5 == 0 {
                            CanonicalValue::Null
                        } else {
                            CanonicalValue::U64(key % 7)
                        }],
                    },
                )
            })
            .collect::<Vec<_>>();
        for direction in [SortDirection::Asc, SortDirection::Desc] {
            for primary_key in [false, true] {
                let order = [
                    ResolvedOrder {
                        index: 0,
                        primary_key: false,
                        direction,
                    },
                    ResolvedOrder {
                        index: 0,
                        primary_key,
                        direction: SortDirection::Desc,
                    },
                ];
                for order in [&order[..], &order[..1], &order[..0]] {
                    let mut expected = rows.clone();
                    expected.sort_by(|a, b| compare(order, a, b));
                    for limit in [0, 1, 10, 200, 300] {
                        let mut top = TopRows::new(order, limit);
                        for row in rows.iter().rev() {
                            top.push(row.clone());
                            assert!(top.heap.len() <= limit);
                        }
                        assert_eq!(top.into_rows(), expected[..limit.min(rows.len())]);
                    }
                }
            }
        }
    }
}
