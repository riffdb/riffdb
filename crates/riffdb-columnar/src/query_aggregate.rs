//! Streaming state for the scan executor's existing exact aggregate semantics.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtraAggregateKind {
    None,
    Quantile(u8),
    Stddev,
}

pub(super) struct Accumulator {
    semantic: AggregateSemanticIdentityV1,
    extra: ExtraAggregateKind,
    index: Option<usize>,
    filter: Option<(usize, CanonicalValue)>,
    numeric_domain: Option<NumericFoldDomain>,
    count: u64,
    total: i128,
    scalar: Option<CanonicalValue>,
    boolean: bool,
    distinct: BTreeSet<Vec<u8>>,
    samples: Vec<(i128, CanonicalValue)>,
    scratch: Vec<u8>,
}

impl Accumulator {
    pub(super) fn new(
        definition: &RegisteredDefinition,
        op: &AggregateOp,
    ) -> Result<Self, QueryError> {
        let mut current = op;
        let mut filter = None;
        while let AggregateOp::Filtered {
            op: inner,
            filter_field,
            filter_value,
        } = current
        {
            let filter_idx = projected_field_index(definition, *filter_field)?;
            filter = Some((filter_idx, filter_value.clone()));
            current = inner;
        }
        let semantic = current.semantic_identity();
        let extra = match current {
            AggregateOp::Median { .. } => ExtraAggregateKind::Quantile(50),
            AggregateOp::P90 { .. } => ExtraAggregateKind::Quantile(90),
            AggregateOp::P95 { .. } => ExtraAggregateKind::Quantile(95),
            AggregateOp::P99 { .. } => ExtraAggregateKind::Quantile(99),
            AggregateOp::Stddev { .. } => ExtraAggregateKind::Stddev,
            _ => ExtraAggregateKind::None,
        };
        let index = if semantic == AggregateSemanticIdentityV1::Count {
            None
        } else {
            Some(projected_field_index(
                definition,
                aggregate_input_field(current)?,
            )?)
        };
        Ok(Self {
            semantic,
            extra,
            index,
            filter,
            numeric_domain: None,
            count: 0,
            total: 0,
            scalar: None,
            boolean: semantic == AggregateSemanticIdentityV1::All,
            distinct: BTreeSet::new(),
            samples: Vec::new(),
            scratch: Vec::new(),
        })
    }

    pub(super) fn push(
        &mut self,
        row: &MergedRowRef<'_>,
        fuel: &mut ColumnarAggregateFuel,
    ) -> Result<(), QueryError> {
        fuel.consume_operations(1)?;
        if let Some((filter_idx, expected_val)) = &self.filter
            && &row.cells[*filter_idx] != expected_val
        {
            return Ok(());
        }
        let value = self.index.map(|index| &row.cells[index]);
        match self.extra {
            ExtraAggregateKind::Quantile(_) | ExtraAggregateKind::Stddev => {
                let value = value.expect("resolved input");
                if matches!(value, CanonicalValue::Null) {
                    return Ok(());
                }
                let (domain, term) = numeric_as_i128(value)?;
                if let Some(expected) = self.numeric_domain {
                    if expected != domain {
                        return Err(QueryError::InvalidAggregate(
                            "quantile/stddev requires uniform numeric domain and scale",
                        ));
                    }
                } else {
                    self.numeric_domain = Some(domain);
                }
                if self.samples.len() >= usize::from(MAX_COLUMNAR_DISTINCT_VALUES_V1) {
                    return Err(QueryError::AggregateBudgetExceeded {
                        resource: "distinct values",
                        max: usize::from(MAX_COLUMNAR_DISTINCT_VALUES_V1),
                    });
                }
                fuel.consume_state(32)?;
                self.total = self
                    .total
                    .checked_add(term)
                    .ok_or(QueryError::InvalidAggregate("stddev total overflow"))?;
                self.samples.push((term, value.clone()));
                return Ok(());
            }
            ExtraAggregateKind::None => {}
        }
        use AggregateSemanticIdentityV1 as S;
        match self.semantic {
            S::Count | S::CountPresent => {
                if self.semantic == S::Count || value != Some(&CanonicalValue::Null) {
                    self.count = self
                        .count
                        .checked_add(1)
                        .ok_or(QueryError::InvalidAggregate("count overflow"))?;
                }
            }
            S::Sum | S::Mean => {
                let (domain, term) = numeric_as_i128(value.expect("resolved input"))?;
                if let Some(expected) = self.numeric_domain {
                    if expected != domain {
                        return Err(QueryError::InvalidAggregate(
                            "sum/mean requires uniform numeric domain and scale",
                        ));
                    }
                } else {
                    self.numeric_domain = Some(domain);
                }
                self.total = self
                    .total
                    .checked_add(term)
                    .ok_or(QueryError::InvalidAggregate(if self.semantic == S::Sum {
                        "sum overflow"
                    } else {
                        "mean total overflow"
                    }))?;
                if self.semantic == S::Mean {
                    self.count = self
                        .count
                        .checked_add(1)
                        .ok_or(QueryError::InvalidAggregate("mean count overflow"))?;
                }
            }
            S::Min | S::Max => {
                let value = value.expect("resolved input");
                let preferred = if self.semantic == S::Min {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                if self
                    .scalar
                    .as_ref()
                    .is_none_or(|current| compare_values(value, current) == preferred)
                {
                    self.scalar = Some(value.clone());
                }
            }
            S::Any | S::All => {
                let CanonicalValue::Bool(value) = value.expect("resolved input") else {
                    return Err(QueryError::InvalidAggregate("any/all requires bool"));
                };
                if self.semantic == S::Any {
                    self.boolean |= value;
                } else {
                    self.boolean &= value;
                }
            }
            S::CountDistinct | S::CountDistinctPresent => {
                let value = value.expect("resolved input");
                if self.semantic == S::CountDistinctPresent && matches!(value, CanonicalValue::Null)
                {
                    return Ok(());
                }
                self.scratch.clear();
                riffdb_types::encode_canonical_value_into(&mut self.scratch, value)
                    .map_err(|_| QueryError::InvalidAggregate("distinct encoding"))?;
                if !self.distinct.contains(&self.scratch) {
                    if self.distinct.len() >= usize::from(MAX_COLUMNAR_DISTINCT_VALUES_V1) {
                        return Err(QueryError::AggregateBudgetExceeded {
                            resource: "distinct values",
                            max: usize::from(MAX_COLUMNAR_DISTINCT_VALUES_V1),
                        });
                    }
                    fuel.consume_state(self.scratch.len().checked_add(32).ok_or(
                        QueryError::AggregateBudgetExceeded {
                            resource: "state bytes",
                            max: MAX_AGGREGATE_STATE_BYTES_V1 as usize,
                        },
                    )?)?;
                    self.distinct.insert(std::mem::take(&mut self.scratch));
                }
            }
            S::ExactCount => {
                return Err(QueryError::InvalidAggregate(
                    "unsupported aggregate semantic",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        fuel: &mut ColumnarAggregateFuel,
    ) -> Result<AggregateValue, QueryError> {
        match self.extra {
            ExtraAggregateKind::Quantile(q) => {
                if self.samples.is_empty() {
                    return Ok(AggregateValue::Scalar(None));
                }
                #[allow(clippy::unnecessary_sort_by)]
                self.samples.sort_by(|left, right| left.0.cmp(&right.0));
                let n = self.samples.len();
                let rank = (usize::from(q).saturating_mul(n).saturating_add(99)) / 100;
                let idx = rank.saturating_sub(1).min(n - 1);
                let chosen = self.samples[idx].1.clone();
                fuel.consume_canonical_state(&chosen)?;
                return Ok(AggregateValue::Scalar(Some(chosen)));
            }
            ExtraAggregateKind::Stddev => {
                fuel.consume_state(std::mem::size_of::<i128>())?;
                if self.samples.len() <= 1 {
                    return Ok(AggregateValue::Sum(0));
                }
                let n = i128::try_from(self.samples.len())
                    .map_err(|_| QueryError::InvalidAggregate("stddev count overflow"))?;
                let sum = self.total;
                let mut numer: u128 = 0;
                for (term, _) in &self.samples {
                    let diff = n
                        .checked_mul(*term)
                        .and_then(|v| v.checked_sub(sum))
                        .ok_or(QueryError::InvalidAggregate("stddev diff overflow"))?;
                    let sq = diff
                        .unsigned_abs()
                        .checked_mul(diff.unsigned_abs())
                        .ok_or(QueryError::InvalidAggregate("stddev square overflow"))?;
                    numer = numer
                        .checked_add(sq)
                        .ok_or(QueryError::InvalidAggregate("stddev sum overflow"))?;
                }
                let denom = (n.unsigned_abs())
                    .checked_mul(n.unsigned_abs())
                    .ok_or(QueryError::InvalidAggregate("stddev denom overflow"))?;
                let stddev = (numer / denom).isqrt();
                let stddev_i128 = i128::try_from(stddev)
                    .map_err(|_| QueryError::InvalidAggregate("stddev result overflow"))?;
                return Ok(AggregateValue::Sum(stddev_i128));
            }
            ExtraAggregateKind::None => {}
        }
        use AggregateSemanticIdentityV1 as S;
        Ok(match self.semantic {
            S::Count | S::CountPresent => {
                fuel.consume_state(std::mem::size_of::<u64>())?;
                AggregateValue::Count(self.count)
            }
            S::Sum => {
                fuel.consume_state(std::mem::size_of::<i128>())?;
                AggregateValue::Sum(self.total)
            }
            S::Mean => {
                fuel.consume_state(std::mem::size_of::<i128>() + std::mem::size_of::<u64>())?;
                AggregateValue::ExactMean {
                    total: self.total,
                    count: self.count,
                }
            }
            S::Min | S::Max => {
                if let Some(value) = self.scalar.as_ref() {
                    fuel.consume_canonical_state(value)?;
                }
                AggregateValue::Scalar(self.scalar)
            }
            S::Any | S::All => {
                fuel.consume_state(std::mem::size_of::<bool>())?;
                AggregateValue::Bool(self.boolean)
            }
            S::CountDistinct | S::CountDistinctPresent => AggregateValue::Count(
                u64::try_from(self.distinct.len())
                    .map_err(|_| QueryError::InvalidAggregate("count overflow"))?,
            ),
            S::ExactCount => {
                return Err(QueryError::InvalidAggregate(
                    "unsupported aggregate semantic",
                ));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CurrencyCode, Decimal, DecimalSpec, Money};

    fn fold_checked(
        semantic: AggregateSemanticIdentityV1,
        values: &[CanonicalValue],
    ) -> Result<AggregateValue, QueryError> {
        let mut state = Accumulator {
            semantic,
            extra: ExtraAggregateKind::None,
            index: Some(0),
            filter: None,
            numeric_domain: None,
            count: 0,
            total: 0,
            scalar: None,
            boolean: semantic == AggregateSemanticIdentityV1::All,
            distinct: BTreeSet::new(),
            samples: Vec::new(),
            scratch: Vec::new(),
        };
        let mut fuel = ColumnarAggregateFuel::new();
        for value in values {
            let cells = [value.clone()];
            state.push(
                &MergedRowRef {
                    entity_version: EntityVersion::new(1).unwrap(),
                    cells: &cells,
                },
                &mut fuel,
            )?;
        }
        state.finish(&mut fuel)
    }

    fn fold(semantic: AggregateSemanticIdentityV1, values: &[CanonicalValue]) -> AggregateValue {
        fold_checked(semantic, values).unwrap()
    }

    // req: OQ-045, OQ-046, OQ-047, OQ-048
    #[test]
    fn streaming_folds_preserve_null_empty_typed_distinct_and_exact_mean_semantics() {
        use AggregateSemanticIdentityV1 as S;
        let values = [
            CanonicalValue::I64(7),
            CanonicalValue::Null,
            CanonicalValue::I64(3),
        ];
        assert_eq!(
            fold(S::Min, &values),
            AggregateValue::Scalar(Some(CanonicalValue::Null))
        );
        assert_eq!(
            fold(S::Max, &values),
            AggregateValue::Scalar(Some(CanonicalValue::I64(7)))
        );
        for semantic in [S::Min, S::Max] {
            assert_eq!(fold(semantic, &[]), AggregateValue::Scalar(None));
            assert_eq!(
                fold(semantic, &[CanonicalValue::Null]),
                AggregateValue::Scalar(Some(CanonicalValue::Null))
            );
        }
        let distinct = [
            CanonicalValue::Null,
            CanonicalValue::U64(1),
            CanonicalValue::I64(1),
            CanonicalValue::string("1").unwrap(),
            CanonicalValue::U64(1),
            CanonicalValue::Null,
        ];
        assert_eq!(fold(S::CountDistinct, &distinct), AggregateValue::Count(4));
        assert_eq!(
            fold(S::CountDistinctPresent, &distinct),
            AggregateValue::Count(3)
        );
        assert_eq!(fold(S::CountPresent, &distinct), AggregateValue::Count(4));
        assert_eq!(
            fold(S::Mean, &[CanonicalValue::I64(3), CanonicalValue::I64(4)]),
            AggregateValue::ExactMean { total: 7, count: 2 }
        );
        assert_eq!(
            fold(S::Mean, &[]),
            AggregateValue::ExactMean { total: 0, count: 0 }
        );
        assert_eq!(fold(S::Any, &[]), AggregateValue::Bool(false));
        assert_eq!(fold(S::All, &[]), AggregateValue::Bool(true));

        let spec2 = DecimalSpec::new(10, 2).unwrap();
        let spec4 = DecimalSpec::new(10, 4).unwrap();
        let d1 = CanonicalValue::Decimal(Decimal::new(spec2, 125).unwrap());
        let d2 = CanonicalValue::Decimal(Decimal::new(spec2, 275).unwrap());
        let d_mixed = CanonicalValue::Decimal(Decimal::new(spec4, 12500).unwrap());
        assert_eq!(
            fold(S::Sum, &[d1.clone(), d2.clone()]),
            AggregateValue::Sum(400)
        );
        assert_eq!(
            fold(S::Mean, &[d1.clone(), d2]),
            AggregateValue::ExactMean {
                total: 400,
                count: 2
            }
        );
        assert!(fold_checked(S::Sum, &[d1, d_mixed]).is_err());

        let usd = CurrencyCode::new("USD").unwrap();
        let eur = CurrencyCode::new("EUR").unwrap();
        let m1 = CanonicalValue::Money(Money::new(usd, Decimal::new(spec4, 10_0000).unwrap()));
        let m2 = CanonicalValue::Money(Money::new(usd, Decimal::new(spec4, 25_5000).unwrap()));
        let m_eur = CanonicalValue::Money(Money::new(eur, Decimal::new(spec4, 10_0000).unwrap()));
        assert_eq!(
            fold(S::Sum, &[m1.clone(), m2.clone()]),
            AggregateValue::Sum(355_000)
        );
        assert_eq!(
            fold(S::Mean, &[m1.clone(), m2]),
            AggregateValue::ExactMean {
                total: 355_000,
                count: 2
            }
        );
        assert!(fold_checked(S::Sum, &[m1, m_eur]).is_err());
    }
}
