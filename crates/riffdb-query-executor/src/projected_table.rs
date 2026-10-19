use super::*;

/// Executes one compiled `source projected` query program over materialized
/// projection rows supplied by the event or scalar columnar projection ports.
pub fn execute_projected_table_rows(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    mut candidate_rows: Vec<QueryRow>,
    application_head: u64,
    epoch_key: String,
    epoch: u64,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    if parameters.0.len() > MAX_QUERY_PARAMETERS {
        return Err(QueryExecutionError::BoundExceeded);
    }
    let [step] = program.steps() else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    let default_outcome = program
        .surface()
        .schemas()
        .results()
        .first()
        .ok_or(QueryExecutionError::InvalidProgram)?
        .name();
    let prepared_plan = prepared_executor_program_plan(program);
    let prepared_step = prepared_plan.steps.first();
    let mut fuel = QueryExecutionFuel::from_cost(program.cost());
    fuel.step()?;
    let limit = resolve_row_limit(step, parameters)?;
    let bound_predicates = bind_predicates(step, parameters, &BTreeMap::new(), &[])?;
    let scanned_candidates =
        u64::try_from(candidate_rows.len()).map_err(|_| QueryExecutionError::BoundExceeded)?;
    if scanned_candidates > MAX_QUERY_SCANNED_ROWS {
        return Err(QueryExecutionError::BoundExceeded);
    }

    let mut matched_rows = Vec::new();
    for row in candidate_rows.drain(..) {
        if row.entity() != step.entity() {
            return Err(QueryExecutionError::BackendIntegrity);
        }
        if predicates_match_at_positions(&row, &bound_predicates, None)? {
            matched_rows.push(row);
        }
    }

    if let QueryAccessKind::Index {
        fields, direction, ..
    } = step.access()
    {
        let mut sort_err = None;
        matched_rows.sort_by(|left, right| {
            for field in fields {
                let l_val = left.field(field).unwrap_or(&CanonicalValue::Null);
                let r_val = right.field(field).unwrap_or(&CanonicalValue::Null);
                match aggregate_scalar_order(l_val, r_val) {
                    Ok(Ordering::Equal) => continue,
                    Ok(ord) => {
                        return if *direction == AccessDirection::Reverse {
                            ord.reverse()
                        } else {
                            ord
                        };
                    }
                    Err(err) => {
                        if sort_err.is_none() {
                            sort_err = Some(err);
                        }
                        return Ordering::Equal;
                    }
                }
            }
            Ordering::Equal
        });
        if let Some(err) = sort_err {
            return Err(err);
        }
    }

    let limit_usize = usize::try_from(limit).map_err(|_| QueryExecutionError::BoundExceeded)?;
    if matched_rows.len() > limit_usize {
        matched_rows.truncate(limit_usize);
    }
    let intermediate_count =
        u64::try_from(matched_rows.len()).map_err(|_| QueryExecutionError::BoundExceeded)?;
    fuel.scans(intermediate_count)?;
    fuel.intermediates(intermediate_count)?;

    if matched_rows.is_empty()
        && let Some(outcome) = step.absence_outcome()
    {
        let mut index_epochs = BTreeMap::new();
        index_epochs.insert(epoch_key, epoch);
        return finish_snapshot(
            &mut fuel,
            QueryOwnedSnapshot {
                application_head,
                index_epochs,
                outcome: outcome.to_owned(),
                fields: Arc::new(BTreeMap::new()),
                covered_result: None,
                continuation_binding: None,
                continuation: None,
                cache_pinned: false,
            },
        );
    }

    let mut rows = QueryStepRows::Owned(matched_rows);
    let bindings = BTreeMap::new();
    let mut result_fields = BTreeMap::new();
    let aggregates = program.surface().aggregates();
    for (agg_idx, aggregate) in aggregates.iter().enumerate() {
        if aggregate.source_binding() != step.binding() || aggregate.parent_binding().is_some() {
            continue;
        }
        let prepared_agg = prepared_plan
            .aggregates
            .get(agg_idx)
            .and_then(Option::as_ref);
        let fallback_selected;
        let selected = if let Some(agg_plan) = prepared_agg {
            &agg_plan.selected
        } else {
            fallback_selected = selected_aggregate_fields(program, aggregate)?;
            &fallback_selected
        };
        if selected.is_empty() {
            continue;
        }
        let agg_rows = evaluate_operational_aggregate(
            program,
            Some(&prepared_plan),
            aggregate,
            selected,
            prepared_agg,
            &rows,
            None,
            parameters,
            &bindings,
            None,
        )?;
        let groups = agg_rows.len();
        let value = if aggregate.group_keys().is_empty() {
            QueryResultValue::AggregateOne(
                agg_rows
                    .into_iter()
                    .next()
                    .ok_or(QueryExecutionError::InvalidProgram)?,
            )
        } else {
            QueryResultValue::AggregateMany(agg_rows)
        };
        let projected = u64::try_from(groups)
            .ok()
            .and_then(|groups| groups.checked_mul(selected.len() as u64))
            .ok_or(QueryExecutionError::BoundExceeded)?;
        fuel.projected_values(projected)?;
        if result_fields
            .insert(aggregate.name().to_owned(), value)
            .is_some()
        {
            return Err(QueryExecutionError::InvalidProgram);
        }
    }

    let fallback_selected: Vec<Arc<str>>;
    let selected_names: &[Arc<str>] = if let Some(s) = prepared_step {
        &s.selected_names
    } else {
        fallback_selected = step
            .selected_fields()
            .iter()
            .map(|name| Arc::<str>::from(name.as_str()))
            .collect();
        &fallback_selected
    };
    if step.result_names().is_empty() {
        for row in &rows {
            row.validate_selected(selected_names)?;
        }
        fuel.projected_values(0)?;
        if step.cardinality() == Cardinality::One && rows.is_empty() {
            return Err(QueryExecutionError::InvalidProgram);
        }
    } else {
        let output_names = prepared_step
            .and_then(|s| s.output_selected_names.as_deref())
            .unwrap_or(selected_names);
        if output_names.len() != selected_names.len() {
            for row in &rows {
                row.validate_selected(selected_names)?;
            }
        }
        let QueryStepRows::Owned(mut owned) =
            std::mem::replace(&mut rows, QueryStepRows::Owned(Vec::new()))
        else {
            unreachable!()
        };
        for row in &mut owned {
            row.project_in_place(output_names)?;
        }
        let projected_count = u64::try_from(owned.len())
            .ok()
            .and_then(|rows| rows.checked_mul(step.selected_fields().len() as u64))
            .and_then(|values| values.checked_mul(step.result_names().len() as u64))
            .ok_or(QueryExecutionError::BoundExceeded)?;
        fuel.projected_values(projected_count)?;
        let value = match step.cardinality() {
            Cardinality::One => QueryResultValue::One(
                owned
                    .into_iter()
                    .next()
                    .ok_or(QueryExecutionError::InvalidProgram)?,
            ),
            Cardinality::Maybe => QueryResultValue::Maybe(owned.into_iter().next()),
            Cardinality::Many => QueryResultValue::Many(owned),
        };
        for name in step.result_names() {
            if result_fields.insert(name.clone(), value.clone()).is_some() {
                return Err(QueryExecutionError::InvalidProgram);
            }
        }
    }

    let mut index_epochs = BTreeMap::new();
    index_epochs.insert(epoch_key, epoch);
    finish_snapshot(
        &mut fuel,
        QueryOwnedSnapshot {
            application_head,
            index_epochs,
            outcome: default_outcome.to_owned(),
            fields: Arc::new(result_fields),
            covered_result: None,
            continuation_binding: None,
            continuation: None,
            cache_pinned: false,
        },
    )
}
