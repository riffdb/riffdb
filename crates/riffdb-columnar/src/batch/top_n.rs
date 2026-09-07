//! Private bounded Top-N mechanics for a compiler-sealed batch program.

use riffdb_types::{
    CanonicalValue, Date, EnumVariantId, MAX_APPLICATION_QUERY_PAGE_ROWS,
    MAX_APPLICATION_QUERY_RESULT_BYTES, MAX_KEY_BYTES, Timestamp,
};
use std::cmp::Ordering;

use crate::PrimaryKeyBytes;
use crate::segment_v2::{MAX_SEGMENT_V2_COLUMNS, SegmentV2Cell, SegmentV2LogicalType};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNError {
    InvalidLimit,
    InvalidOrder,
    InvalidPrimaryKey,
    CellCount,
    UnexpectedNoValue,
    TypeMismatch,
    ArithmeticOverflow,
    StateBoundExceeded,
    InvalidHeapState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNValueDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNNoValuePlacement {
    PresentOnly,
    First,
    Last,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TopNOrderTerm {
    logical_type: SegmentV2LogicalType,
    direction: TopNValueDirection,
    no_value: TopNNoValuePlacement,
}

impl TopNOrderTerm {
    pub(crate) const fn new(
        logical_type: SegmentV2LogicalType,
        direction: TopNValueDirection,
        no_value: TopNNoValuePlacement,
    ) -> Self {
        Self {
            logical_type,
            direction,
            no_value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TopNCandidate {
    primary_key: PrimaryKeyBytes,
    order_cells: Vec<SegmentV2Cell>,
}

impl TopNCandidate {
    pub(crate) const fn new(primary_key: PrimaryKeyBytes, order_cells: Vec<SegmentV2Cell>) -> Self {
        Self {
            primary_key,
            order_cells,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OrderedScalar {
    Bool(bool),
    I64(i64),
    U64(u64),
    Decimal(i128),
    Money(i128),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(Timestamp),
    Date(Date),
    Uuid([u8; 16]),
    Enum(EnumVariantId),
}

impl OrderedScalar {
    const fn tag(&self) -> u8 {
        match self {
            Self::Bool(_) => 1,
            Self::I64(_) => 2,
            Self::U64(_) => 3,
            Self::Decimal(_) => 4,
            Self::Money(_) => 5,
            Self::String(_) => 6,
            Self::Bytes(_) => 7,
            Self::Timestamp(_) => 8,
            Self::Date(_) => 9,
            Self::Uuid(_) => 10,
            Self::Enum(_) => 11,
        }
    }
}

impl Ord for OrderedScalar {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => left.cmp(right),
            (Self::I64(left), Self::I64(right)) => left.cmp(right),
            (Self::U64(left), Self::U64(right)) => left.cmp(right),
            (Self::Decimal(left), Self::Decimal(right)) => left.cmp(right),
            (Self::Money(left), Self::Money(right)) => left.cmp(right),
            (Self::String(left), Self::String(right)) => left.cmp(right),
            (Self::Bytes(left), Self::Bytes(right)) => left.cmp(right),
            (Self::Timestamp(left), Self::Timestamp(right)) => left.cmp(right),
            (Self::Date(left), Self::Date(right)) => left.cmp(right),
            (Self::Uuid(left), Self::Uuid(right)) => left.cmp(right),
            (Self::Enum(left), Self::Enum(right)) => left.cmp(right),
            (left, right) => left.tag().cmp(&right.tag()),
        }
    }
}

impl PartialOrd for OrderedScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RankedCell {
    NoValue,
    Value(OrderedScalar),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TopNPosition {
    order: Vec<RankedCell>,
    primary_key: PrimaryKeyBytes,
    payload_bytes: usize,
}

impl TopNPosition {
    pub(crate) const fn primary_key(&self) -> &PrimaryKeyBytes {
        &self.primary_key
    }
}

#[derive(Debug)]
pub(crate) struct BoundedTopN {
    order: Vec<TopNOrderTerm>,
    limit: usize,
    capacity: usize,
    state_byte_ceiling: usize,
    retained_payload_bytes: usize,
    retained: Vec<TopNPosition>,
    failure: Option<TopNError>,
}

impl BoundedTopN {
    pub(crate) fn new(order: Vec<TopNOrderTerm>, limit: usize) -> Result<Self, TopNError> {
        let maximum = usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS)
            .map_err(|_| TopNError::ArithmeticOverflow)?;
        if limit == 0 || limit > maximum {
            return Err(TopNError::InvalidLimit);
        }
        if order.len() > MAX_SEGMENT_V2_COLUMNS {
            return Err(TopNError::InvalidOrder);
        }
        let capacity = limit.checked_add(1).ok_or(TopNError::ArithmeticOverflow)?;
        let state_byte_ceiling = usize::try_from(MAX_APPLICATION_QUERY_RESULT_BYTES)
            .map_err(|_| TopNError::ArithmeticOverflow)?;
        Ok(Self {
            order,
            limit,
            capacity,
            state_byte_ceiling,
            retained_payload_bytes: 0,
            retained: Vec::with_capacity(capacity),
            failure: None,
        })
    }

    pub(crate) fn push(&mut self, candidate: TopNCandidate) -> Result<(), TopNError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let position = match position(&self.order, candidate) {
            Ok(position) => position,
            Err(error) => return self.poison(error),
        };
        if self.retained.len() < self.capacity {
            let next_bytes = match self
                .retained_payload_bytes
                .checked_add(position.payload_bytes)
            {
                Some(bytes) if bytes <= self.state_byte_ceiling => bytes,
                Some(_) => return self.poison(TopNError::StateBoundExceeded),
                None => return self.poison(TopNError::ArithmeticOverflow),
            };
            self.push_heap(position);
            self.retained_payload_bytes = next_bytes;
            return Ok(());
        }

        let Some(worst) = self.retained.first() else {
            return self.poison(TopNError::InvalidHeapState);
        };
        if compare_positions(&self.order, &position, worst) != Ordering::Less {
            return Ok(());
        }
        let next_bytes = match self
            .retained_payload_bytes
            .checked_sub(worst.payload_bytes)
            .and_then(|bytes| bytes.checked_add(position.payload_bytes))
        {
            Some(bytes) if bytes <= self.state_byte_ceiling => bytes,
            Some(_) => return self.poison(TopNError::StateBoundExceeded),
            None => return self.poison(TopNError::ArithmeticOverflow),
        };
        self.retained[0] = position;
        if let Err(error) = self.sift_down(0) {
            return self.poison(error);
        }
        self.retained_payload_bytes = next_bytes;
        Ok(())
    }

    pub(crate) fn retained_len(&self) -> usize {
        self.retained.len()
    }

    pub(crate) fn finish(self) -> Result<TopNPage, TopNError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let mut rows = self.retained;
        rows.sort_unstable_by(|left, right| compare_positions(&self.order, left, right));
        let has_more = rows.len() > self.limit;
        if has_more {
            rows.truncate(self.limit);
        }
        let cursor = has_more.then(|| rows.last().cloned()).flatten();
        Ok(TopNPage {
            order: self.order,
            rows,
            cursor,
        })
    }

    fn poison(&mut self, error: TopNError) -> Result<(), TopNError> {
        self.retained.clear();
        self.retained_payload_bytes = 0;
        self.failure = Some(error);
        Err(error)
    }

    fn push_heap(&mut self, position: TopNPosition) {
        self.retained.push(position);
        let mut child = self.retained.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if compare_positions(&self.order, &self.retained[child], &self.retained[parent])
                != Ordering::Greater
            {
                break;
            }
            self.retained.swap(child, parent);
            child = parent;
        }
    }

    fn sift_down(&mut self, mut parent: usize) -> Result<(), TopNError> {
        loop {
            let left = parent
                .checked_mul(2)
                .and_then(|value| value.checked_add(1))
                .ok_or(TopNError::ArithmeticOverflow)?;
            if left >= self.retained.len() {
                break;
            }
            let right = left.checked_add(1).ok_or(TopNError::ArithmeticOverflow)?;
            let larger = if right < self.retained.len()
                && compare_positions(&self.order, &self.retained[right], &self.retained[left])
                    == Ordering::Greater
            {
                right
            } else {
                left
            };
            if compare_positions(&self.order, &self.retained[larger], &self.retained[parent])
                != Ordering::Greater
            {
                break;
            }
            self.retained.swap(parent, larger);
            parent = larger;
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TopNPage {
    order: Vec<TopNOrderTerm>,
    rows: Vec<TopNPosition>,
    cursor: Option<TopNPosition>,
}

impl TopNPage {
    pub(crate) const fn has_more(&self) -> bool {
        self.cursor.is_some()
    }

    pub(crate) const fn cursor(&self) -> Option<&TopNPosition> {
        self.cursor.as_ref()
    }

    pub(crate) fn primary_key_bytes(&self) -> Vec<Vec<u8>> {
        self.rows
            .iter()
            .map(|row| row.primary_key.as_bytes().to_vec())
            .collect()
    }

    pub(crate) fn compare_row_to_cursor(&self, row: usize) -> Option<Ordering> {
        Some(compare_positions(
            &self.order,
            self.rows.get(row)?,
            self.cursor.as_ref()?,
        ))
    }
}

fn compare_positions(
    order: &[TopNOrderTerm],
    left: &TopNPosition,
    right: &TopNPosition,
) -> Ordering {
    for ((term, left), right) in order.iter().zip(&left.order).zip(&right.order) {
        let compared = match (left, right) {
            (RankedCell::NoValue, RankedCell::NoValue) => Ordering::Equal,
            (RankedCell::NoValue, RankedCell::Value(_)) => match term.no_value {
                TopNNoValuePlacement::First => Ordering::Less,
                TopNNoValuePlacement::Last => Ordering::Greater,
                TopNNoValuePlacement::PresentOnly => Ordering::Equal,
            },
            (RankedCell::Value(_), RankedCell::NoValue) => match term.no_value {
                TopNNoValuePlacement::First => Ordering::Greater,
                TopNNoValuePlacement::Last => Ordering::Less,
                TopNNoValuePlacement::PresentOnly => Ordering::Equal,
            },
            (RankedCell::Value(left), RankedCell::Value(right)) => match term.direction {
                TopNValueDirection::Ascending => left.cmp(right),
                TopNValueDirection::Descending => left.cmp(right).reverse(),
            },
        };
        if compared != Ordering::Equal {
            return compared;
        }
    }
    left.primary_key.cmp(&right.primary_key)
}

fn position(order: &[TopNOrderTerm], candidate: TopNCandidate) -> Result<TopNPosition, TopNError> {
    if candidate.primary_key.as_bytes().is_empty()
        || candidate.primary_key.as_bytes().len() > MAX_KEY_BYTES
    {
        return Err(TopNError::InvalidPrimaryKey);
    }
    if candidate.order_cells.len() != order.len() {
        return Err(TopNError::CellCount);
    }
    let mut payload_bytes = candidate.primary_key.as_bytes().len();
    let mut ranked = Vec::with_capacity(order.len());
    for (term, cell) in order.iter().zip(candidate.order_cells) {
        payload_bytes = payload_bytes
            .checked_add(cell_payload_bytes(&cell)?)
            .ok_or(TopNError::ArithmeticOverflow)?;
        ranked.push(rank_cell(term, cell)?);
    }
    Ok(TopNPosition {
        order: ranked,
        primary_key: candidate.primary_key,
        payload_bytes,
    })
}

fn cell_payload_bytes(cell: &SegmentV2Cell) -> Result<usize, TopNError> {
    match cell {
        SegmentV2Cell::Missing | SegmentV2Cell::Null => Ok(1),
        SegmentV2Cell::Value(value) => match value {
            CanonicalValue::Bool(_) => Ok(2),
            CanonicalValue::I64(_) | CanonicalValue::U64(_) => Ok(9),
            CanonicalValue::Decimal(_) => Ok(17),
            CanonicalValue::Money(_) => Ok(20),
            CanonicalValue::String(value) => value
                .len()
                .checked_add(1)
                .ok_or(TopNError::ArithmeticOverflow),
            CanonicalValue::Bytes(value) => value
                .len()
                .checked_add(1)
                .ok_or(TopNError::ArithmeticOverflow),
            CanonicalValue::Timestamp(_) => Ok(13),
            CanonicalValue::Date(_) => Ok(5),
            CanonicalValue::Uuid(_) => Ok(17),
            CanonicalValue::Enum { .. } => Ok(9),
            CanonicalValue::Null
            | CanonicalValue::List(_)
            | CanonicalValue::Record(_)
            | CanonicalValue::Vector(_) => Err(TopNError::TypeMismatch),
        },
    }
}

fn rank_cell(term: &TopNOrderTerm, cell: SegmentV2Cell) -> Result<RankedCell, TopNError> {
    match cell {
        SegmentV2Cell::Missing | SegmentV2Cell::Null => match term.no_value {
            TopNNoValuePlacement::PresentOnly => Err(TopNError::UnexpectedNoValue),
            TopNNoValuePlacement::First | TopNNoValuePlacement::Last => Ok(RankedCell::NoValue),
        },
        SegmentV2Cell::Value(value) => Ok(RankedCell::Value(ordered_scalar(
            &term.logical_type,
            value,
        )?)),
    }
}

fn ordered_scalar(
    logical_type: &SegmentV2LogicalType,
    value: CanonicalValue,
) -> Result<OrderedScalar, TopNError> {
    match (logical_type, value) {
        (SegmentV2LogicalType::Bool, CanonicalValue::Bool(value)) => Ok(OrderedScalar::Bool(value)),
        (SegmentV2LogicalType::I64, CanonicalValue::I64(value)) => Ok(OrderedScalar::I64(value)),
        (SegmentV2LogicalType::U64, CanonicalValue::U64(value)) => Ok(OrderedScalar::U64(value)),
        (SegmentV2LogicalType::String, CanonicalValue::String(value)) => {
            Ok(OrderedScalar::String(value.into_string()))
        }
        (SegmentV2LogicalType::Bytes, CanonicalValue::Bytes(value)) => {
            Ok(OrderedScalar::Bytes(value.into_vec()))
        }
        (SegmentV2LogicalType::Timestamp, CanonicalValue::Timestamp(value)) => {
            Ok(OrderedScalar::Timestamp(value))
        }
        (SegmentV2LogicalType::Date, CanonicalValue::Date(value)) => Ok(OrderedScalar::Date(value)),
        (SegmentV2LogicalType::Uuid, CanonicalValue::Uuid(value)) => Ok(OrderedScalar::Uuid(value)),
        (
            SegmentV2LogicalType::Enum(expected),
            CanonicalValue::Enum {
                type_id,
                variant_id,
            },
        ) if *expected == type_id => Ok(OrderedScalar::Enum(variant_id)),
        (SegmentV2LogicalType::Decimal(expected), CanonicalValue::Decimal(value))
            if *expected == value.spec() =>
        {
            Ok(OrderedScalar::Decimal(value.coefficient()))
        }
        (SegmentV2LogicalType::Money { currency, amount }, CanonicalValue::Money(value))
            if *currency == value.currency() && *amount == value.amount().spec() =>
        {
            Ok(OrderedScalar::Money(value.amount().coefficient()))
        }
        _ => Err(TopNError::TypeMismatch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PrimaryKeyBytes;
    use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType};
    use riffdb_types::{
        CanonicalValue, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId, EnumVariantId, Money,
        Timestamp,
    };

    fn key(value: u8) -> PrimaryKeyBytes {
        PrimaryKeyBytes::from_entity_key_bytes(vec![value])
    }

    fn candidate(primary_key: u8, cells: Vec<SegmentV2Cell>) -> TopNCandidate {
        TopNCandidate::new(key(primary_key), cells)
    }

    fn assert_scalar_order(
        logical_type: SegmentV2LogicalType,
        left: CanonicalValue,
        right: CanonicalValue,
    ) {
        for (direction, expected) in [
            (TopNValueDirection::Ascending, vec![vec![2], vec![1]]),
            (TopNValueDirection::Descending, vec![vec![1], vec![2]]),
        ] {
            let mut top = BoundedTopN::new(
                vec![TopNOrderTerm::new(
                    logical_type.clone(),
                    direction,
                    TopNNoValuePlacement::PresentOnly,
                )],
                2,
            )
            .expect("bounded scalar order");
            top.push(candidate(2, vec![SegmentV2Cell::Value(left.clone())]))
                .expect("left scalar");
            top.push(candidate(1, vec![SegmentV2Cell::Value(right.clone())]))
                .expect("right scalar");
            assert_eq!(top.finish().expect("page").primary_key_bytes(), expected);
        }
    }

    // Inert mechanics checkpoint only. This deliberately does not claim an
    // ADR-0161 obligation or select the batch path for production execution.
    #[test]
    fn bounded_top_n_orders_no_value_terms_and_uses_the_unique_key_tie_break() {
        let order = vec![
            TopNOrderTerm::new(
                SegmentV2LogicalType::U64,
                TopNValueDirection::Descending,
                TopNNoValuePlacement::Last,
            ),
            TopNOrderTerm::new(
                SegmentV2LogicalType::U64,
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::First,
            ),
        ];
        let mut top = BoundedTopN::new(order, 5).expect("bounded program");
        for row in [
            candidate(
                8,
                vec![
                    SegmentV2Cell::Null,
                    SegmentV2Cell::Value(CanonicalValue::U64(1)),
                ],
            ),
            candidate(
                5,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::U64(9)),
                    SegmentV2Cell::Value(CanonicalValue::U64(2)),
                ],
            ),
            candidate(
                2,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::U64(9)),
                    SegmentV2Cell::Value(CanonicalValue::U64(1)),
                ],
            ),
            candidate(
                1,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::U64(9)),
                    SegmentV2Cell::Value(CanonicalValue::U64(1)),
                ],
            ),
            candidate(
                7,
                vec![
                    SegmentV2Cell::Missing,
                    SegmentV2Cell::Value(CanonicalValue::U64(0)),
                ],
            ),
            candidate(
                4,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::U64(8)),
                    SegmentV2Cell::Null,
                ],
            ),
            candidate(
                3,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::U64(8)),
                    SegmentV2Cell::Missing,
                ],
            ),
        ] {
            top.push(row).expect("valid row");
        }
        let page = top.finish().expect("complete page");
        assert_eq!(
            page.primary_key_bytes(),
            vec![vec![1], vec![2], vec![5], vec![3], vec![4]]
        );
        assert!(page.has_more());
        assert_eq!(
            page.cursor()
                .expect("continuation")
                .primary_key()
                .as_bytes(),
            &[4]
        );

        for (direction, placement) in [
            (TopNValueDirection::Ascending, TopNNoValuePlacement::First),
            (TopNValueDirection::Descending, TopNNoValuePlacement::Last),
        ] {
            let mut top = BoundedTopN::new(
                vec![TopNOrderTerm::new(
                    SegmentV2LogicalType::U64,
                    direction,
                    placement,
                )],
                3,
            )
            .expect("state-aware order");
            top.push(candidate(2, vec![SegmentV2Cell::Missing]))
                .expect("missing is NoValue");
            top.push(candidate(1, vec![SegmentV2Cell::Null]))
                .expect("null is NoValue");
            top.push(candidate(
                3,
                vec![SegmentV2Cell::Value(CanonicalValue::U64(7))],
            ))
            .expect("present value");
            let expected = match placement {
                TopNNoValuePlacement::First => vec![vec![1], vec![2], vec![3]],
                TopNNoValuePlacement::Last => vec![vec![3], vec![1], vec![2]],
                TopNNoValuePlacement::PresentOnly => unreachable!(),
            };
            assert_eq!(top.finish().expect("page").primary_key_bytes(), expected);
        }
    }

    #[test]
    fn bounded_top_n_covers_the_complete_v2_scalar_total_order() {
        assert_scalar_order(
            SegmentV2LogicalType::Bool,
            CanonicalValue::Bool(false),
            CanonicalValue::Bool(true),
        );
        assert_scalar_order(
            SegmentV2LogicalType::I64,
            CanonicalValue::I64(-1),
            CanonicalValue::I64(1),
        );
        assert_scalar_order(
            SegmentV2LogicalType::U64,
            CanonicalValue::U64(1),
            CanonicalValue::U64(2),
        );
        assert_scalar_order(
            SegmentV2LogicalType::String,
            CanonicalValue::string("alpha").expect("string"),
            CanonicalValue::string("beta").expect("string"),
        );
        assert_scalar_order(
            SegmentV2LogicalType::Bytes,
            CanonicalValue::bytes([0x01]).expect("bytes"),
            CanonicalValue::bytes([0x02]).expect("bytes"),
        );
        assert_scalar_order(
            SegmentV2LogicalType::Timestamp,
            CanonicalValue::Timestamp(Timestamp::new(-1, 999_999_999).expect("timestamp")),
            CanonicalValue::Timestamp(Timestamp::new(0, 0).expect("timestamp")),
        );
        assert_scalar_order(
            SegmentV2LogicalType::Date,
            CanonicalValue::Date(Date::new(-1)),
            CanonicalValue::Date(Date::new(1)),
        );
        assert_scalar_order(
            SegmentV2LogicalType::Uuid,
            CanonicalValue::Uuid([0x01; 16]),
            CanonicalValue::Uuid([0x02; 16]),
        );
        let enum_type = EnumTypeId::new(7).expect("enum type");
        assert_scalar_order(
            SegmentV2LogicalType::Enum(enum_type),
            CanonicalValue::Enum {
                type_id: enum_type,
                variant_id: EnumVariantId::new(1).expect("variant"),
            },
            CanonicalValue::Enum {
                type_id: enum_type,
                variant_id: EnumVariantId::new(2).expect("variant"),
            },
        );
        let decimal = DecimalSpec::new(8, 2).expect("decimal type");
        assert_scalar_order(
            SegmentV2LogicalType::Decimal(decimal),
            CanonicalValue::Decimal(Decimal::new(decimal, -1).expect("decimal")),
            CanonicalValue::Decimal(Decimal::new(decimal, 1).expect("decimal")),
        );
        let currency = CurrencyCode::new("USD").expect("currency");
        assert_scalar_order(
            SegmentV2LogicalType::Money {
                currency,
                amount: decimal,
            },
            CanonicalValue::Money(Money::new(
                currency,
                Decimal::new(decimal, -1).expect("amount"),
            )),
            CanonicalValue::Money(Money::new(
                currency,
                Decimal::new(decimal, 1).expect("amount"),
            )),
        );
    }

    #[test]
    fn bounded_top_n_refuses_bounds_states_and_types_without_partial_output() {
        assert_eq!(
            BoundedTopN::new(Vec::new(), 0).unwrap_err(),
            TopNError::InvalidLimit
        );
        assert_eq!(
            BoundedTopN::new(
                Vec::new(),
                riffdb_types::MAX_APPLICATION_QUERY_PAGE_ROWS as usize + 1
            )
            .unwrap_err(),
            TopNError::InvalidLimit
        );
        let order = vec![TopNOrderTerm::new(
            SegmentV2LogicalType::U64,
            TopNValueDirection::Ascending,
            TopNNoValuePlacement::PresentOnly,
        )];
        let mut top = BoundedTopN::new(order, 1).expect("bounded program");
        assert_eq!(
            top.push(candidate(
                1,
                vec![SegmentV2Cell::Value(CanonicalValue::U64(1))]
            )),
            Ok(())
        );
        assert_eq!(
            top.push(candidate(2, vec![SegmentV2Cell::Null])),
            Err(TopNError::UnexpectedNoValue)
        );
        assert_eq!(top.finish(), Err(TopNError::UnexpectedNoValue));

        let order = vec![TopNOrderTerm::new(
            SegmentV2LogicalType::U64,
            TopNValueDirection::Ascending,
            TopNNoValuePlacement::Last,
        )];
        let mut top = BoundedTopN::new(order, 1).expect("bounded program");
        assert_eq!(
            top.push(candidate(
                1,
                vec![SegmentV2Cell::Value(CanonicalValue::I64(1))]
            )),
            Err(TopNError::TypeMismatch)
        );
        assert_eq!(top.finish(), Err(TopNError::TypeMismatch));

        let term = TopNOrderTerm::new(
            SegmentV2LogicalType::U64,
            TopNValueDirection::Ascending,
            TopNNoValuePlacement::Last,
        );
        assert_eq!(
            BoundedTopN::new(vec![term.clone(); MAX_SEGMENT_V2_COLUMNS + 1], 1).unwrap_err(),
            TopNError::InvalidOrder
        );
        let maximum = BoundedTopN::new(
            Vec::new(),
            usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS).expect("page bound"),
        )
        .expect("accepted maximum");
        assert_eq!(maximum.capacity, 65_535);

        let mut wrong_count = BoundedTopN::new(vec![term.clone()], 1).expect("bounded program");
        assert_eq!(
            wrong_count.push(candidate(1, Vec::new())),
            Err(TopNError::CellCount)
        );
        assert_eq!(wrong_count.finish(), Err(TopNError::CellCount));

        let mut bad_key = BoundedTopN::new(vec![term.clone()], 1).expect("bounded program");
        assert_eq!(
            bad_key.push(TopNCandidate::new(
                PrimaryKeyBytes::from_entity_key_bytes(Vec::new()),
                vec![SegmentV2Cell::Value(CanonicalValue::U64(1))],
            )),
            Err(TopNError::InvalidPrimaryKey)
        );
        assert_eq!(bad_key.finish(), Err(TopNError::InvalidPrimaryKey));

        let mut oversized_key = BoundedTopN::new(vec![term.clone()], 1).expect("bounded program");
        assert_eq!(
            oversized_key.push(TopNCandidate::new(
                PrimaryKeyBytes::from_entity_key_bytes(vec![0; MAX_KEY_BYTES + 1]),
                vec![SegmentV2Cell::Value(CanonicalValue::U64(1))],
            )),
            Err(TopNError::InvalidPrimaryKey)
        );
        assert_eq!(oversized_key.finish(), Err(TopNError::InvalidPrimaryKey));

        let mut wrapped_null = BoundedTopN::new(vec![term], 1).expect("bounded program");
        assert_eq!(
            wrapped_null.push(candidate(
                1,
                vec![SegmentV2Cell::Value(CanonicalValue::Null)]
            )),
            Err(TopNError::TypeMismatch)
        );
        assert_eq!(wrapped_null.finish(), Err(TopNError::TypeMismatch));

        let enum_type = EnumTypeId::new(1).expect("enum type");
        let other_enum_type = EnumTypeId::new(2).expect("other enum type");
        let decimal = DecimalSpec::new(8, 2).expect("decimal type");
        let other_decimal = DecimalSpec::new(8, 3).expect("other decimal type");
        let usd = CurrencyCode::new("USD").expect("USD");
        let eur = CurrencyCode::new("EUR").expect("EUR");
        for (logical_type, value) in [
            (
                SegmentV2LogicalType::Enum(enum_type),
                CanonicalValue::Enum {
                    type_id: other_enum_type,
                    variant_id: EnumVariantId::first(),
                },
            ),
            (
                SegmentV2LogicalType::Decimal(decimal),
                CanonicalValue::Decimal(Decimal::new(other_decimal, 1).expect("decimal")),
            ),
            (
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                CanonicalValue::Money(Money::new(eur, Decimal::new(decimal, 1).expect("amount"))),
            ),
        ] {
            let mut exact_identity = BoundedTopN::new(
                vec![TopNOrderTerm::new(
                    logical_type,
                    TopNValueDirection::Ascending,
                    TopNNoValuePlacement::PresentOnly,
                )],
                1,
            )
            .expect("bounded program");
            assert_eq!(
                exact_identity.push(candidate(1, vec![SegmentV2Cell::Value(value)])),
                Err(TopNError::TypeMismatch)
            );
            assert_eq!(exact_identity.finish(), Err(TopNError::TypeMismatch));
        }
    }

    #[test]
    fn bounded_top_n_enforces_its_independent_retained_state_byte_ceiling() {
        let mut top = BoundedTopN::new(
            vec![TopNOrderTerm::new(
                SegmentV2LogicalType::String,
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::PresentOnly,
            )],
            5,
        )
        .expect("bounded program");
        let bounded_text = "x".repeat(riffdb_types::MAX_STRING_BYTES - 64);
        for primary_key in 1..=4 {
            top.push(candidate(
                primary_key,
                vec![SegmentV2Cell::Value(
                    CanonicalValue::string(bounded_text.clone()).expect("bounded string"),
                )],
            ))
            .expect("within retained byte ceiling");
        }
        assert_eq!(
            top.push(candidate(
                5,
                vec![SegmentV2Cell::Value(
                    CanonicalValue::string(bounded_text).expect("bounded string"),
                )],
            )),
            Err(TopNError::StateBoundExceeded)
        );
        assert_eq!(top.retained_len(), 0);
        assert_eq!(top.finish(), Err(TopNError::StateBoundExceeded));
    }

    #[test]
    fn bounded_top_n_retains_only_limit_plus_probe_and_cursor_uses_final_order() {
        let order = vec![TopNOrderTerm::new(
            SegmentV2LogicalType::U64,
            TopNValueDirection::Ascending,
            TopNNoValuePlacement::Last,
        )];
        let mut top = BoundedTopN::new(order, 2).expect("bounded program");
        for value in (0_u8..32).rev() {
            top.push(candidate(
                value + 1,
                vec![SegmentV2Cell::Value(CanonicalValue::U64(u64::from(value)))],
            ))
            .expect("valid row");
            assert!(top.retained_len() <= 3);
        }
        let page = top.finish().expect("complete page");
        assert_eq!(page.primary_key_bytes(), vec![vec![1], vec![2]]);
        let cursor = page.cursor().expect("continuation");
        assert_eq!(cursor.primary_key().as_bytes(), &[2]);
        assert_eq!(
            page.compare_row_to_cursor(1),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            page.compare_row_to_cursor(0),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(page.compare_row_to_cursor(2), None);

        let empty = BoundedTopN::new(Vec::new(), 1)
            .expect("primary-key-only order")
            .finish()
            .expect("empty page");
        assert_eq!(empty.primary_key_bytes(), Vec::<Vec<u8>>::new());
        assert!(!empty.has_more());

        let mut exact = BoundedTopN::new(Vec::new(), 2).expect("primary-key-only order");
        exact.push(candidate(2, Vec::new())).expect("second key");
        exact.push(candidate(1, Vec::new())).expect("first key");
        let exact = exact.finish().expect("exact page");
        assert_eq!(exact.primary_key_bytes(), vec![vec![1], vec![2]]);
        assert!(!exact.has_more());
    }

    #[test]
    fn bounded_top_n_heap_matches_an_independent_full_sort_for_every_input_order() {
        let rows = (0_u16..257)
            .map(|key| (((u32::from(key) * 37) % 101) as u64, key))
            .collect::<Vec<_>>();
        let mut expected = rows.clone();
        expected.sort_unstable_by(|(left_value, left_key), (right_value, right_key)| {
            left_value
                .cmp(right_value)
                .then_with(|| left_key.cmp(right_key))
        });
        let expected = expected
            .iter()
            .take(17)
            .map(|(_, key)| key.to_be_bytes().to_vec())
            .collect::<Vec<_>>();

        for input in [rows.clone(), rows.into_iter().rev().collect()] {
            let mut top = BoundedTopN::new(
                vec![TopNOrderTerm::new(
                    SegmentV2LogicalType::U64,
                    TopNValueDirection::Ascending,
                    TopNNoValuePlacement::PresentOnly,
                )],
                17,
            )
            .expect("bounded program");
            for (value, key) in input {
                top.push(TopNCandidate::new(
                    PrimaryKeyBytes::from_entity_key_bytes(key.to_be_bytes()),
                    vec![SegmentV2Cell::Value(CanonicalValue::U64(value))],
                ))
                .expect("valid candidate");
                assert!(top.retained_len() <= 18);
            }
            let page = top.finish().expect("complete page");
            assert_eq!(page.primary_key_bytes(), expected);
            assert!(page.has_more());
            assert_eq!(
                page.compare_row_to_cursor(16),
                Some(std::cmp::Ordering::Equal)
            );
        }
    }

    #[test]
    fn bounded_top_n_refuses_a_duplicate_primary_key() {
        let mut top = BoundedTopN::new(Vec::new(), 2).expect("bounded program");
        top.push(candidate(1, Vec::new())).expect("first key");
        assert_eq!(
            top.push(candidate(1, Vec::new())),
            Err(TopNError::InvalidPrimaryKey)
        );
        assert_eq!(top.finish(), Err(TopNError::InvalidPrimaryKey));
    }

    #[test]
    fn bounded_top_n_decimal_order_remains_the_scalar_canonical_byte_order() {
        let decimal = DecimalSpec::new(8, 2).expect("decimal type");
        let mut top = BoundedTopN::new(
            vec![TopNOrderTerm::new(
                SegmentV2LogicalType::Decimal(decimal),
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::PresentOnly,
            )],
            2,
        )
        .expect("bounded program");
        top.push(candidate(
            1,
            vec![SegmentV2Cell::Value(CanonicalValue::Decimal(
                Decimal::new(decimal, -1).expect("negative decimal"),
            ))],
        ))
        .expect("negative decimal");
        top.push(candidate(
            2,
            vec![SegmentV2Cell::Value(CanonicalValue::Decimal(
                Decimal::new(decimal, 1).expect("positive decimal"),
            ))],
        ))
        .expect("positive decimal");

        assert_eq!(
            top.finish().expect("page").primary_key_bytes(),
            vec![vec![2], vec![1]]
        );
    }
}
