use std::fmt::Write;

use crate::{
    AggregateFunction, BinaryOperator, CandidateSetExpression, CandidateSource, Cardinality,
    Direction, Document, Expression, FieldSelection, Literal, NullPlacement, Path,
    ProjectedFreshness, Selection, TokenizedMatchKind, TokenizedRanking, TypeReference,
    UnaryOperator,
};

/// Emits the canonical, idempotent RiffQL source spelling for the document version.
#[must_use]
pub fn format_query(document: &Document) -> String {
    let mut output = String::new();
    if let Some(name) = &document.name {
        write!(output, "query {}(", name.value.as_str()).expect("String writes cannot fail");
        for (index, parameter) in document.parameters.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            write!(
                output,
                "${}: {}",
                parameter.name.value.as_str(),
                format_type(&parameter.ty.value)
            )
            .expect("String writes cannot fail");
            if let Some(default) = &parameter.default {
                write!(output, " = {}", format_literal(&default.value))
                    .expect("String writes cannot fail");
            }
        }
        output.push_str(") {\n");
    } else {
        output.push_str("{\n");
    }
    if let Some(source) = &document.projected_source {
        writeln!(
            output,
            "    source projected {}",
            format_path(&source.path.value)
        )
        .expect("String writes cannot fail");
        match source.freshness.value {
            ProjectedFreshness::Available => output.push_str("    freshness available\n\n"),
            ProjectedFreshness::Causal {
                inherit_session_commit,
                max_wait_ms,
            } => {
                writeln!(
                    output,
                    "    freshness causal inherit_session_commit {inherit_session_commit} max_wait_ms {max_wait_ms}\n"
                )
                .expect("String writes cannot fail");
            }
            ProjectedFreshness::Bounded { max_lag_ms } => {
                writeln!(output, "    freshness bounded max_lag_ms {max_lag_ms}\n")
                    .expect("String writes cannot fail");
            }
        }
    }
    for candidate in &document.body.candidates {
        writeln!(
            output,
            "    candidates {}: {}",
            candidate.name.value.as_str(),
            format_path(&candidate.root_key.value)
        )
        .expect("String writes cannot fail");
        output.push_str("        from ");
        match &candidate.expression {
            CandidateSetExpression::Single(source) => {
                format_candidate_source(&mut output, source, 0);
                output.push('\n');
            }
            CandidateSetExpression::Intersection(sources) => {
                format_candidate_sources(&mut output, "intersect", sources);
            }
            CandidateSetExpression::Union(sources) => {
                format_candidate_sources(&mut output, "union", sources);
            }
            CandidateSetExpression::Difference { positive, negative } => {
                output.push_str("difference {\n            ");
                format_candidate_source(&mut output, positive, 12);
                output.push_str(";\n");
                for (index, source) in negative.iter().enumerate() {
                    output.push_str("            ");
                    format_candidate_source(&mut output, source, 12);
                    if index + 1 != negative.len() {
                        output.push(',');
                    }
                    output.push('\n');
                }
                output.push_str("        }\n");
            }
        }
        writeln!(output, "        within {}", candidate.within).expect("String writes cannot fail");
        writeln!(
            output,
            "        else {}\n",
            candidate.refusal_outcome.value.as_str()
        )
        .expect("String writes cannot fail");
    }
    for binding in &document.body.bindings {
        let cardinality = match binding.cardinality.value {
            Cardinality::One => "one",
            Cardinality::Maybe => "maybe",
            Cardinality::Many => "many",
        };
        writeln!(
            output,
            "    {cardinality} {} from {}",
            binding.name.value.as_str(),
            binding.entity.value.as_str()
        )
        .expect("String writes cannot fail");
        writeln!(
            output,
            "        where {}",
            format_expression(&binding.predicate.value, 0)
        )
        .expect("String writes cannot fail");
        if let Some(matching) = &binding.tokenized_match {
            write!(
                output,
                "        matching({}, ",
                matching.index.value.as_str()
            )
            .expect("String writes cannot fail");
            match matching.kind.value {
                TokenizedMatchKind::Conjunction => output.push_str("conjunction"),
                TokenizedMatchKind::Disjunction => output.push_str("disjunction"),
                TokenizedMatchKind::Phrase => output.push_str("phrase"),
                TokenizedMatchKind::Proximity(distance) => {
                    write!(output, "proximity, {distance}").expect("String writes cannot fail");
                }
            }
            write!(output, ", ${}", matching.query.value.as_str())
                .expect("String writes cannot fail");
            if matching.ranking == TokenizedRanking::RiffBm25V1 {
                output.push_str(", riff_bm25_v1");
            }
            output.push_str(")\n");
        }
        if !binding.order.is_empty() {
            output.push_str("        order by ");
            for (index, term) in binding.order.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format_path(&term.path.value));
                output.push(' ');
                output.push_str(match term.direction.value {
                    Direction::Ascending => "asc",
                    Direction::Descending => "desc",
                });
                if let Some(placement) = &term.null_placement {
                    output.push_str(match placement.value {
                        NullPlacement::First => " nulls first",
                        NullPlacement::Last => " nulls last",
                    });
                }
            }
            output.push('\n');
        }
        if let Some(take) = &binding.take {
            write!(
                output,
                "        take {}",
                format_expression(&take.limit.value, 0)
            )
            .expect("String writes cannot fail");
            if let Some(after) = &take.after {
                write!(output, " after ${}", after.value.as_str())
                    .expect("String writes cannot fail");
            }
            if let Some(offset) = &take.offset {
                write!(output, " offset {}", format_expression(&offset.value, 0))
                    .expect("String writes cannot fail");
            }
            output.push('\n');
        }
        if let Some(nearest) = &binding.nearest {
            write!(
                output,
                "        nearest({}, ${}, {})",
                nearest.field.value.as_str(),
                nearest.vector.value.as_str(),
                format_expression(&nearest.k.value, 0)
            )
            .expect("String writes cannot fail");
            output.push('\n');
        }
        if let Some(outcome) = &binding.absence_outcome {
            writeln!(output, "        else {}", outcome.value.as_str())
                .expect("String writes cannot fail");
        }
        output.push('\n');
    }
    for aggregate in &document.body.aggregates {
        writeln!(
            output,
            "    aggregate {} from {} {{",
            aggregate.name.value.as_str(),
            aggregate.source.value.as_str()
        )
        .expect("String writes cannot fail");
        if !aggregate.group_by.is_empty() {
            output.push_str("        group by ");
            for (index, field) in aggregate.group_by.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format_path(&field.value));
            }
            output.push('\n');
        }
        for measure in &aggregate.measures {
            output.push_str("        ");
            output.push_str(match measure.function.value {
                AggregateFunction::Count => "count(",
                AggregateFunction::ExactCount => "exact_count(",
                AggregateFunction::Sum => "sum(",
                AggregateFunction::Min => "min(",
                AggregateFunction::Max => "max(",
                AggregateFunction::CountPresent => "count_present(",
                AggregateFunction::CountDistinct => "count_distinct(",
                AggregateFunction::CountDistinctPresent => "count_distinct_present(",
                AggregateFunction::Mean => "mean(",
                AggregateFunction::Any => "any(",
                AggregateFunction::All => "all(",
            });
            if let Some(field) = &measure.field {
                output.push_str(&format_path(&field.value));
            }
            writeln!(output, ") as {}", measure.alias.value.as_str())
                .expect("String writes cannot fail");
        }
        output.push_str("    }\n\n");
    }
    output.push_str("    return");
    if let Some(outcome) = &document.body.outcome {
        write!(output, " {}", outcome.value.as_str()).expect("String writes cannot fail");
    }
    output.push(' ');
    format_selection(&mut output, &document.body.selection, 1);
    output.push('\n');
    if !document.body.outcomes.is_empty() {
        output.push_str("\n    outcomes ");
        for (index, outcome) in document.body.outcomes.iter().enumerate() {
            if index > 0 {
                output.push_str(" | ");
            }
            output.push_str(outcome.value.as_str());
        }
        output.push('\n');
    }
    output.push_str("}\n");
    output
}

fn format_candidate_sources(output: &mut String, operator: &str, sources: &[CandidateSource]) {
    writeln!(output, "{operator} {{").expect("String writes cannot fail");
    for (index, source) in sources.iter().enumerate() {
        output.push_str("            ");
        format_candidate_source(output, source, 12);
        if index + 1 != sources.len() {
            output.push(',');
        }
        output.push('\n');
    }
    output.push_str("        }\n");
}

fn format_candidate_source(output: &mut String, source: &CandidateSource, indent: usize) {
    write!(
        output,
        "{} using {}\n{}where {}",
        format_path(&source.projected_key.value),
        source.access.value.as_str(),
        " ".repeat(indent + 4),
        format_expression(&source.predicate.value, 0)
    )
    .expect("String writes cannot fail");
}

fn format_selection(output: &mut String, selection: &Selection, indentation: usize) {
    output.push_str("{\n");
    for (index, field) in selection.fields.iter().enumerate() {
        output.push_str(&"    ".repeat(indentation + 1));
        format_field(output, field, indentation + 1);
        if index + 1 != selection.fields.len() && is_bare_reveals_identifier(field) {
            output.push(',');
        }
        output.push('\n');
    }
    output.push_str(&"    ".repeat(indentation));
    output.push('}');
}

fn is_bare_reveals_identifier(field: &FieldSelection) -> bool {
    field.alias.is_none()
        && field.source.value.0.len() == 1
        && field.source.value.0[0].value.as_str() == "reveals"
}

fn format_field(output: &mut String, field: &FieldSelection, indentation: usize) {
    if let Some(alias) = &field.alias {
        write!(
            output,
            "{}: {}",
            alias.value.as_str(),
            format_path(&field.source.value)
        )
        .expect("String writes cannot fail");
    } else {
        output.push_str(&format_path(&field.source.value));
    }
    for reveals in &field.reveals {
        output.push_str(" reveals ");
        output.push_str(&format_path(&reveals.value));
    }
    if let Some(nested) = &field.nested {
        output.push(' ');
        format_selection(output, nested, indentation);
    }
}

fn format_type(value: &TypeReference) -> String {
    match value {
        TypeReference::Named(path) => format_path(path),
        TypeReference::Optional(inner) => format!("{}?", format_type(&inner.value)),
        TypeReference::Set(inner) => format!("Set<{}>", format_type(&inner.value)),
        TypeReference::Cursor => "Cursor".to_owned(),
        TypeReference::Limit => "Limit".to_owned(),
        TypeReference::BoundedLimit(maximum) => format!("Limit<{maximum}>"),
    }
}

fn format_expression(value: &Expression, parent_precedence: u8) -> String {
    match value {
        Expression::Parameter(name) => format!("${}", name.value.as_str()),
        Expression::Path(path) => format_path(path),
        Expression::Literal(literal) => format_literal(literal),
        Expression::PresenceGuard {
            parameter,
            predicate,
        } => format!(
            "when ${} {{ {} }}",
            parameter.value.as_str(),
            format_expression(&predicate.value, 0)
        ),
        Expression::Unary { operator, operand } => match operator.value {
            UnaryOperator::IsNull => {
                format!("{} is null", format_expression(&operand.value, 3))
            }
            UnaryOperator::IsNotNull => {
                format!("{} is not null", format_expression(&operand.value, 3))
            }
            UnaryOperator::Exists => {
                format!("exists {}", format_expression(&operand.value, 3))
            }
        },
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            let (text, precedence) = match operator.value {
                BinaryOperator::Or => ("||", 1),
                BinaryOperator::And => ("&&", 2),
                BinaryOperator::Equal => ("==", 3),
                BinaryOperator::NotEqual => ("!=", 3),
                BinaryOperator::Less => ("<", 3),
                BinaryOperator::LessEqual => ("<=", 3),
                BinaryOperator::Greater => (">", 3),
                BinaryOperator::GreaterEqual => (">=", 3),
                BinaryOperator::In => ("in", 3),
                BinaryOperator::NotIn => ("not_in", 3),
                BinaryOperator::Prefix => ("prefix", 3),
                BinaryOperator::StartsWith => ("starts_with", 3),
                BinaryOperator::EndsWith => ("ends_with", 3),
                BinaryOperator::Contains => ("contains", 3),
            };
            let rendered = format!(
                "{} {text} {}",
                format_expression(&left.value, precedence),
                format_expression(&right.value, precedence + 1)
            );
            if precedence < parent_precedence {
                format!("({rendered})")
            } else {
                rendered
            }
        }
    }
}

fn format_literal(value: &Literal) -> String {
    match value {
        Literal::Unsigned(value) => value.clone(),
        Literal::String(value) => format!("\"{}\"", escape_string(value)),
        Literal::Boolean(true) => "true".to_owned(),
        Literal::Boolean(false) => "false".to_owned(),
        Literal::Null => "null".to_owned(),
    }
}

fn escape_string(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character < '\u{0020}' => {
                write!(output, "\\u{:04x}", character as u32).expect("String writes cannot fail");
            }
            character => output.push(character),
        }
    }
    output
}

fn format_path(path: &Path) -> String {
    path.0
        .iter()
        .map(|segment| segment.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use crate::{format_query, parse_query};

    #[test]
    fn canonical_format_is_parse_stable_and_idempotent() {
        let source = r#"query Open($tenant: TenantId,$limit: Limit<499>=25){
many tickets from Ticket where tenant_id==$tenant order by updated_at desc,ticket_id desc take $limit
return Found{tickets: tickets{ticket_id title}}
outcomes Found
}"#;
        let first = parse_query(source).expect("parse source");
        let formatted = format_query(&first);
        let second = parse_query(&formatted).expect("parse formatted source");
        assert_eq!(format_query(&second), formatted);
    }
}
