use super::*;

/// One normalized half-open physical index interval.
///
/// This is a transient compiler/executor witness. It is never serialized,
/// hashed independently, or exposed through an application surface.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundIndexRangeV1 {
    start_inclusive: Vec<u8>,
    end_exclusive: Vec<u8>,
}

impl BoundIndexRangeV1 {
    fn checked(start_inclusive: Vec<u8>, end_exclusive: Vec<u8>) -> Option<Self> {
        (start_inclusive < end_exclusive).then_some(Self {
            start_inclusive,
            end_exclusive,
        })
    }

    /// Inclusive physical start.
    #[doc(hidden)]
    #[must_use]
    pub fn start_inclusive(&self) -> &[u8] {
        &self.start_inclusive
    }

    /// Exclusive physical end.
    #[doc(hidden)]
    #[must_use]
    pub fn end_exclusive(&self) -> &[u8] {
        &self.end_exclusive
    }

    /// Applies the shared forward/reverse continuation truth table.
    #[doc(hidden)]
    #[must_use]
    pub fn resume_window<'a>(
        &'a self,
        direction: AccessDirection,
        after: Option<&'a [u8]>,
        after_inclusive: bool,
    ) -> Option<BoundIndexRangeWindowV1<'a>> {
        let Some(after) = after else {
            return Some(BoundIndexRangeWindowV1 {
                start_inclusive: &self.start_inclusive,
                end_exclusive: &self.end_exclusive,
                skip_start_equal: false,
                include_end_equal: false,
            });
        };
        match direction {
            AccessDirection::Forward if after >= self.end_exclusive.as_slice() => None,
            AccessDirection::Forward if after < self.start_inclusive.as_slice() => {
                Some(BoundIndexRangeWindowV1 {
                    start_inclusive: &self.start_inclusive,
                    end_exclusive: &self.end_exclusive,
                    skip_start_equal: false,
                    include_end_equal: false,
                })
            }
            AccessDirection::Forward => Some(BoundIndexRangeWindowV1 {
                start_inclusive: after,
                end_exclusive: &self.end_exclusive,
                skip_start_equal: !after_inclusive,
                include_end_equal: false,
            }),
            AccessDirection::Reverse
                if after < self.start_inclusive.as_slice()
                    || (!after_inclusive && after == self.start_inclusive.as_slice()) =>
            {
                None
            }
            AccessDirection::Reverse if after >= self.end_exclusive.as_slice() => {
                Some(BoundIndexRangeWindowV1 {
                    start_inclusive: &self.start_inclusive,
                    end_exclusive: &self.end_exclusive,
                    skip_start_equal: false,
                    include_end_equal: false,
                })
            }
            AccessDirection::Reverse => Some(BoundIndexRangeWindowV1 {
                start_inclusive: &self.start_inclusive,
                end_exclusive: after,
                skip_start_equal: false,
                include_end_equal: after_inclusive,
            }),
        }
    }
}

/// One resumed half-open range window shared by memory and redb.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundIndexRangeWindowV1<'a> {
    start_inclusive: &'a [u8],
    end_exclusive: &'a [u8],
    skip_start_equal: bool,
    include_end_equal: bool,
}

impl<'a> BoundIndexRangeWindowV1<'a> {
    /// Inclusive physical start supplied to storage.
    #[doc(hidden)]
    #[must_use]
    pub const fn start_inclusive(self) -> &'a [u8] {
        self.start_inclusive
    }

    /// Exclusive physical end supplied to storage.
    #[doc(hidden)]
    #[must_use]
    pub const fn end_exclusive(self) -> &'a [u8] {
        self.end_exclusive
    }

    /// Whether a forward adapter must exclude the exact continuation key.
    #[doc(hidden)]
    #[must_use]
    pub const fn skip_start_equal(self) -> bool {
        self.skip_start_equal
    }

    /// Whether a reverse adapter includes the exact supplied upper key.
    #[doc(hidden)]
    #[must_use]
    pub const fn include_end_equal(self) -> bool {
        self.include_end_equal
    }
}

/// One bounded immutable range schedule in physical traversal order.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundIndexRangeScheduleV1 {
    ranges: Vec<BoundIndexRangeV1>,
}

impl BoundIndexRangeScheduleV1 {
    /// Disjoint physical ranges in forward or reverse traversal order.
    #[doc(hidden)]
    #[must_use]
    pub fn ranges(&self) -> &[BoundIndexRangeV1] {
        &self.ranges
    }
}

/// Lowers every ordinary predicate into one finite physical range schedule.
#[doc(hidden)]
pub fn bound_index_range_schedule_v1(
    step: &QueryAccessStep,
    predicates: &[BoundPredicate],
) -> Result<BoundIndexRangeScheduleV1, QueryExecutionError> {
    let (fields, direction) = match step.access() {
        QueryAccessKind::Index {
            fields, direction, ..
        }
        | QueryAccessKind::PartitionSetIndex {
            fields, direction, ..
        }
        | QueryAccessKind::ExpansionIndex {
            fields, direction, ..
        } => (fields, direction),
        _ => return Err(QueryExecutionError::InvalidProgram),
    };
    let schema = step
        .internal_index_key_schema()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let descriptor = step
        .internal_operational_index_descriptor()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let mut leading = vec![Vec::<CanonicalValue>::with_capacity(fields.len())];
    let mut consumed = 0usize;
    for (logical_position, field) in fields.iter().enumerate() {
        let component = descriptor
            .component(logical_position, field)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let mut match_first: Option<&BoundPredicate> = None;
        let mut match_second: Option<&BoundPredicate> = None;
        let mut match_count = 0usize;
        for predicate in predicates {
            if predicate.field() == field {
                match match_count {
                    0 => match_first = Some(predicate),
                    1 => match_second = Some(predicate),
                    _ => {}
                }
                match_count += 1;
            }
        }
        if match_count == 0 {
            if consumed != predicates.len() {
                return Err(QueryExecutionError::InvalidProgram);
            }
            return encode_prefix_schedule(schema, leading, *direction);
        }
        let is_interval_op = |op: QueryPredicateOperator| {
            matches!(
                op,
                QueryPredicateOperator::NotEqual
                    | QueryPredicateOperator::Less
                    | QueryPredicateOperator::LessEqual
                    | QueryPredicateOperator::Greater
                    | QueryPredicateOperator::GreaterEqual
            )
        };
        let all_interval = match (match_count, match_first, match_second) {
            (1, Some(p1), None) => is_interval_op(p1.operator()),
            (2, Some(p1), Some(p2)) => {
                is_interval_op(p1.operator()) && is_interval_op(p2.operator())
            }
            _ => predicates
                .iter()
                .filter(|predicate| predicate.field() == field)
                .all(|predicate| is_interval_op(predicate.operator())),
        };
        if all_interval {
            if leading.len() != 1 || consumed + match_count != predicates.len() || match_count > 2 {
                return Err(QueryExecutionError::InvalidProgram);
            }
            let leading_values = leading.pop().ok_or(QueryExecutionError::InvalidProgram)?;
            return match (match_first, match_second) {
                (Some(p1), None) => {
                    encode_interval_schedule(schema, component, leading_values, &[p1], *direction)
                }
                (Some(p1), Some(p2)) => encode_interval_schedule(
                    schema,
                    component,
                    leading_values,
                    &[p1, p2],
                    *direction,
                ),
                _ => Err(QueryExecutionError::InvalidProgram),
            };
        }
        let Some(predicate) = match_first.filter(|_| match_count == 1) else {
            return Err(QueryExecutionError::InvalidProgram);
        };
        match predicate.operator() {
            QueryPredicateOperator::Equal => {
                for values in &mut leading {
                    push_exact_index_prefix_value(schema, component, values, predicate.value())?;
                }
                consumed += 1;
            }
            QueryPredicateOperator::In => {
                let CanonicalValue::List(items) = predicate.value() else {
                    return Err(QueryExecutionError::InvalidProgram);
                };
                if items.values().len() > MAX_QUERY_PARAMETERS {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                let prior = std::mem::take(&mut leading);
                for values in prior {
                    for item in items.values() {
                        let mut expanded = values.clone();
                        push_exact_index_prefix_value(schema, component, &mut expanded, item)?;
                        leading.push(expanded);
                    }
                }
                consumed += 1;
            }
            QueryPredicateOperator::IsNull => {
                for values in &mut leading {
                    if component.encoding() != riffdb_contract_ir::IndexFieldEncodingV1::Presence
                        || component.physical_start() != values.len()
                        || component.physical().len() != 2
                    {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                    let payload = schema
                        .components()
                        .get(values.len() + 1)
                        .ok_or(QueryExecutionError::InvalidProgram)
                        .and_then(|component| {
                            riffdb_contract_ir::presence_placeholder_v1(component)
                                .map_err(|_| QueryExecutionError::InvalidProgram)
                        })?;
                    values.push(CanonicalValue::U64(riffdb_contract_ir::PRESENCE_NULL_V1));
                    values.push(payload);
                }
                consumed += 1;
            }
            QueryPredicateOperator::IsNotNull => {
                for values in &mut leading {
                    if component.encoding() != riffdb_contract_ir::IndexFieldEncodingV1::Presence
                        || component.physical_start() != values.len()
                        || component.physical().len() != 2
                    {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                    values.push(CanonicalValue::U64(riffdb_contract_ir::PRESENCE_VALUE_V1));
                }
                if consumed + 1 != predicates.len() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                return encode_prefix_schedule(schema, leading, *direction);
            }
            QueryPredicateOperator::Exists => {
                if component.encoding() != riffdb_contract_ir::IndexFieldEncodingV1::Presence
                    || component.physical().len() != 2
                {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                let prior = std::mem::take(&mut leading);
                for values in prior {
                    if component.physical_start() != values.len() {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                    for state in [
                        riffdb_contract_ir::PRESENCE_NULL_V1,
                        riffdb_contract_ir::PRESENCE_VALUE_V1,
                    ] {
                        let mut expanded = values.clone();
                        expanded.push(CanonicalValue::U64(state));
                        leading.push(expanded);
                    }
                }
                if consumed + 1 != predicates.len() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                return encode_prefix_schedule(schema, leading, *direction);
            }
            QueryPredicateOperator::Prefix => {
                let CanonicalValue::String(prefix) = predicate.value() else {
                    return Err(QueryExecutionError::InvalidProgram);
                };
                if consumed + 1 != predicates.len() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                let prefixes = leading
                    .into_iter()
                    .map(|values| {
                        let transformed =
                            transform_text_key(component.encoding(), prefix.as_str())?;
                        if component.physical_start() != values.len()
                            || component.physical().len() != 1
                        {
                            return Err(QueryExecutionError::InvalidProgram);
                        }
                        schema
                            .encode_index_ordered_prefix(&values, &transformed)
                            .map(|prefix| prefix.as_bytes().to_vec())
                            .map_err(|_| QueryExecutionError::InvalidProgram)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                return encode_physical_prefix_schedule(prefixes, *direction);
            }
            QueryPredicateOperator::NotEqual
            | QueryPredicateOperator::Less
            | QueryPredicateOperator::LessEqual
            | QueryPredicateOperator::Greater
            | QueryPredicateOperator::GreaterEqual => {
                return Err(QueryExecutionError::InvalidProgram);
            }
        }
    }
    if consumed != predicates.len() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    encode_prefix_schedule(schema, leading, *direction)
}

pub(super) fn push_exact_index_prefix_value(
    schema: &riffdb_contract_ir::KeySchema,
    descriptor: &riffdb_query_ir::OperationalIndexComponentV1,
    values: &mut Vec<CanonicalValue>,
    logical: &CanonicalValue,
) -> Result<(), QueryExecutionError> {
    if descriptor.physical_start() != values.len() || descriptor.physical().len() != 1 {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let component = schema
        .components()
        .get(values.len())
        .ok_or(QueryExecutionError::InvalidProgram)?;
    if descriptor.physical() != std::slice::from_ref(component) {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let physical = match (descriptor.encoding(), component.codec(), logical) {
        (
            riffdb_contract_ir::IndexFieldEncodingV1::Canonical,
            KeyComponentCodecV1::Canonical,
            value,
        ) => value.clone(),
        (
            riffdb_contract_ir::IndexFieldEncodingV1::TextKey(profile),
            KeyComponentCodecV1::OrderedBytes,
            CanonicalValue::String(value),
        ) => {
            let bytes = transform_text_key(
                riffdb_contract_ir::IndexFieldEncodingV1::TextKey(profile),
                value.as_str(),
            )?;
            CanonicalValue::bytes(bytes).map_err(|_| QueryExecutionError::InvalidProgram)?
        }
        _ => return Err(QueryExecutionError::InvalidProgram),
    };
    values.push(physical);
    Ok(())
}

fn encode_prefix_schedule(
    schema: &riffdb_contract_ir::KeySchema,
    prefixes: Vec<Vec<CanonicalValue>>,
    direction: AccessDirection,
) -> Result<BoundIndexRangeScheduleV1, QueryExecutionError> {
    let ranges = prefixes
        .into_iter()
        .map(|values| {
            let prefix = schema
                .encode_index_prefix(&values)
                .map(|prefix| prefix.as_bytes().to_vec())
                .map_err(|_| QueryExecutionError::InvalidProgram)?;
            let end =
                exclusive_prefix_end_v1(&prefix).ok_or(QueryExecutionError::InvalidProgram)?;
            BoundIndexRangeV1::checked(prefix, end).ok_or(QueryExecutionError::InvalidProgram)
        })
        .collect::<Result<Vec<_>, _>>()?;
    finish_range_schedule(ranges, direction)
}

fn encode_physical_prefix_schedule(
    prefixes: Vec<Vec<u8>>,
    direction: AccessDirection,
) -> Result<BoundIndexRangeScheduleV1, QueryExecutionError> {
    let ranges = prefixes
        .into_iter()
        .map(|prefix| {
            let end =
                exclusive_prefix_end_v1(&prefix).ok_or(QueryExecutionError::InvalidProgram)?;
            BoundIndexRangeV1::checked(prefix, end).ok_or(QueryExecutionError::InvalidProgram)
        })
        .collect::<Result<Vec<_>, _>>()?;
    finish_range_schedule(ranges, direction)
}

fn encode_interval_schedule(
    schema: &riffdb_contract_ir::KeySchema,
    descriptor: &riffdb_query_ir::OperationalIndexComponentV1,
    mut leading: Vec<CanonicalValue>,
    predicates: &[&BoundPredicate],
    direction: AccessDirection,
) -> Result<BoundIndexRangeScheduleV1, QueryExecutionError> {
    let component = schema
        .components()
        .get(leading.len())
        .ok_or(QueryExecutionError::InvalidProgram)?;
    if predicates.is_empty()
        || predicates.len() > 2
        || predicates
            .iter()
            .any(|predicate| !interval_value_supported(descriptor.encoding(), predicate.value()))
        || descriptor.physical_start() != leading.len()
        || descriptor.physical() != std::slice::from_ref(component)
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let leading_start = schema
        .encode_index_prefix(&leading)
        .map(|prefix| prefix.as_bytes().to_vec())
        .map_err(|_| QueryExecutionError::InvalidProgram)?;
    let leading_end =
        exclusive_prefix_end_v1(&leading_start).ok_or(QueryExecutionError::InvalidProgram)?;

    if let [predicate] = predicates
        && predicate.operator() == QueryPredicateOperator::NotEqual
    {
        push_exact_index_prefix_value(schema, descriptor, &mut leading, predicate.value())?;
        let exact_start = schema
            .encode_index_prefix(&leading)
            .map(|prefix| prefix.as_bytes().to_vec())
            .map_err(|_| QueryExecutionError::InvalidProgram)?;
        let exact_end =
            exclusive_prefix_end_v1(&exact_start).ok_or(QueryExecutionError::InvalidProgram)?;
        let ranges = [
            BoundIndexRangeV1::checked(leading_start, exact_start),
            BoundIndexRangeV1::checked(exact_end, leading_end),
        ]
        .into_iter()
        .flatten()
        .collect();
        return finish_range_schedule(ranges, direction);
    }
    if predicates
        .iter()
        .any(|predicate| predicate.operator() == QueryPredicateOperator::NotEqual)
    {
        return Err(QueryExecutionError::InvalidProgram);
    }

    let mut start = leading_start;
    let mut end = leading_end;
    let mut lower_seen = false;
    let mut upper_seen = false;
    for predicate in predicates {
        push_exact_index_prefix_value(schema, descriptor, &mut leading, predicate.value())?;
        let exact_start = schema
            .encode_index_prefix(&leading)
            .map(|prefix| prefix.as_bytes().to_vec())
            .map_err(|_| QueryExecutionError::InvalidProgram)?;
        leading.pop();
        let exact_end =
            exclusive_prefix_end_v1(&exact_start).ok_or(QueryExecutionError::InvalidProgram)?;
        match predicate.operator() {
            QueryPredicateOperator::Greater if !lower_seen => {
                start = exact_end;
                lower_seen = true;
            }
            QueryPredicateOperator::GreaterEqual if !lower_seen => {
                start = exact_start;
                lower_seen = true;
            }
            QueryPredicateOperator::Less if !upper_seen => {
                end = exact_start;
                upper_seen = true;
            }
            QueryPredicateOperator::LessEqual if !upper_seen => {
                end = exact_end;
                upper_seen = true;
            }
            _ => return Err(QueryExecutionError::InvalidProgram),
        }
    }
    let ranges = BoundIndexRangeV1::checked(start, end).into_iter().collect();
    finish_range_schedule(ranges, direction)
}

fn interval_value_supported(
    encoding: riffdb_contract_ir::IndexFieldEncodingV1,
    value: &CanonicalValue,
) -> bool {
    match encoding {
        riffdb_contract_ir::IndexFieldEncodingV1::Canonical => matches!(
            value,
            CanonicalValue::I64(_)
                | CanonicalValue::U64(_)
                | CanonicalValue::Timestamp(_)
                | CanonicalValue::Date(_)
                | CanonicalValue::Uuid(_)
                | CanonicalValue::Enum { .. }
        ),
        riffdb_contract_ir::IndexFieldEncodingV1::TextKey(_) => {
            matches!(value, CanonicalValue::String(_))
        }
        riffdb_contract_ir::IndexFieldEncodingV1::Presence => false,
    }
}

fn transform_text_key(
    encoding: riffdb_contract_ir::IndexFieldEncodingV1,
    value: &str,
) -> Result<Vec<u8>, QueryExecutionError> {
    match encoding {
        riffdb_contract_ir::IndexFieldEncodingV1::TextKey(
            riffdb_contract_ir::TextKeyProfileV1::BinaryUtf8,
        ) => Ok(value.as_bytes().to_vec()),
        riffdb_contract_ir::IndexFieldEncodingV1::TextKey(
            riffdb_contract_ir::TextKeyProfileV1::UnicodeFold,
        ) => Ok(riffdb_types::unicode_fold_v1(value).into_bytes()),
        _ => Err(QueryExecutionError::InvalidProgram),
    }
}

fn finish_range_schedule(
    mut ranges: Vec<BoundIndexRangeV1>,
    direction: AccessDirection,
) -> Result<BoundIndexRangeScheduleV1, QueryExecutionError> {
    if ranges.len() > 1 {
        ranges.sort_unstable_by(|left, right| {
            left.start_inclusive
                .cmp(&right.start_inclusive)
                .then_with(|| left.end_exclusive.cmp(&right.end_exclusive))
        });
        if ranges
            .windows(2)
            .any(|pair| pair[0].end_exclusive > pair[1].start_inclusive)
        {
            return Err(QueryExecutionError::InvalidProgram);
        }
    }
    let schedule_bytes = ranges.iter().try_fold(0usize, |total, range| {
        total
            .checked_add(range.start_inclusive.len())?
            .checked_add(range.end_exclusive.len())
    });
    if schedule_bytes.is_none_or(|bytes| bytes > MAX_OPERATIONAL_PREFIX_SCHEDULE_BYTES) {
        return Err(QueryExecutionError::BoundExceeded);
    }
    if ranges.len() > 1 && direction == AccessDirection::Reverse {
        ranges.reverse();
    }
    Ok(BoundIndexRangeScheduleV1 { ranges })
}

pub(super) fn exclusive_prefix_end_v1(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}
