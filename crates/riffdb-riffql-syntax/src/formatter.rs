use std::fmt::Write;

use crate::{
    BinaryOperator, Cardinality, Direction, Document, Expression, FieldSelection, Literal, Path,
    Selection, TypeReference,
};

/// Emits the canonical, idempotent RiffQL v1 source spelling.
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
            output.push('\n');
        }
        if let Some(outcome) = &binding.absence_outcome {
            writeln!(output, "        else {}", outcome.value.as_str())
                .expect("String writes cannot fail");
        }
        output.push('\n');
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

fn format_selection(output: &mut String, selection: &Selection, indentation: usize) {
    output.push_str("{\n");
    for field in &selection.fields {
        output.push_str(&"    ".repeat(indentation + 1));
        format_field(output, field, indentation + 1);
        output.push('\n');
    }
    output.push_str(&"    ".repeat(indentation));
    output.push('}');
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
    }
}

fn format_expression(value: &Expression, parent_precedence: u8) -> String {
    match value {
        Expression::Parameter(name) => format!("${}", name.value.as_str()),
        Expression::Path(path) => format_path(path),
        Expression::Literal(literal) => format_literal(literal),
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
        let source = r#"query Open($tenant: TenantId,$limit: Limit=25){
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
