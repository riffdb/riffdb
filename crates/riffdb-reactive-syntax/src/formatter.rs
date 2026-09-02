//! Canonical reactive-source formatter.

use std::fmt::Write as _;

use crate::{
    Argument, BinaryOperator, Expression, Literal, Module, Operand, Parameter, UpdateMode,
};

/// Renders canonical grammar-v1 source.
#[must_use]
pub fn format_module(module: &Module) -> String {
    let mut output = String::new();
    writeln!(
        output,
        "reactive {} version {} {{",
        module.name(),
        module.version()
    )
    .expect("String writes cannot fail");
    for stream in module.streams() {
        write!(output, "  stream {}", stream.name()).expect("String writes cannot fail");
        parameters(&mut output, stream.parameters());
        writeln!(output, " {{").expect("String writes cannot fail");
        output.push_str("    partition (");
        for (index, binding) in stream.partition().iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            write!(output, "{} = ${}", binding.field(), binding.parameter())
                .expect("String writes cannot fail");
        }
        output.push_str(");\n");
        for event in stream.events() {
            write!(output, "    event {} select (", event.event())
                .expect("String writes cannot fail");
            for (index, field) in event.fields().iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str(field);
            }
            output.push_str(");\n");
        }
        if let Some(predicate) = stream.predicate() {
            output.push_str("    where ");
            expression(&mut output, predicate, 0);
            output.push_str(";\n");
        }
        output.push_str("  }\n");
    }
    for watch in module.watches() {
        write!(output, "  watch {}", watch.name()).expect("String writes cannot fail");
        parameters(&mut output, watch.parameters());
        writeln!(
            output,
            " query {} updates {};",
            watch.query(),
            match watch.update_mode() {
                UpdateMode::Patch => "patch",
                UpdateMode::Reset => "reset",
            }
        )
        .expect("String writes cannot fail");
    }
    for subscription in module.subscriptions() {
        write!(output, "  subscription {}", subscription.name())
            .expect("String writes cannot fail");
        parameters(&mut output, subscription.parameters());
        output.push_str(" {\n    stream ");
        output.push_str(subscription.stream().stream());
        arguments(&mut output, subscription.stream().arguments());
        output.push_str(";\n");
        for hydration in subscription.hydrations() {
            write!(
                output,
                "    hydrate {} query {}",
                hydration.name(),
                hydration.query()
            )
            .expect("String writes cannot fail");
            arguments(&mut output, hydration.arguments());
            output.push_str(";\n");
        }
        for reaction in subscription.reactions() {
            writeln!(
                output,
                "    reaction {} command {};",
                reaction.name(),
                reaction.command()
            )
            .expect("String writes cannot fail");
        }
        let limits = subscription.limits();
        writeln!(
            output,
            "    limits {{\n      batch {};\n      in_flight {};\n      lease_seconds {};\n    }}",
            limits.batch(),
            limits.in_flight(),
            limits.lease_seconds()
        )
        .expect("String writes cannot fail");
        output.push_str("  }\n");
    }
    output.push_str("}\n");
    output
}

fn parameters(output: &mut String, values: &[Parameter]) {
    output.push('(');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        write!(output, "${}: {}", value.name(), value.type_name())
            .expect("String writes cannot fail");
    }
    output.push(')');
}

fn arguments(output: &mut String, values: &[Argument]) {
    output.push('(');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        write!(output, "{} = ", value.name()).expect("String writes cannot fail");
        operand(output, value.value());
    }
    output.push(')');
}

fn expression(output: &mut String, value: &Expression, parent_precedence: u8) {
    match value {
        Expression::Operand(value) => operand(output, value),
        Expression::Binary {
            left,
            operator,
            right,
            ..
        } => {
            let precedence = match operator {
                BinaryOperator::Or => 1,
                BinaryOperator::And => 2,
                _ => 3,
            };
            let parenthesized = precedence < parent_precedence;
            if parenthesized {
                output.push('(');
            }
            expression(output, left, precedence);
            write!(output, " {} ", operator_text(*operator)).expect("String writes cannot fail");
            expression(output, right, precedence + 1);
            if parenthesized {
                output.push(')');
            }
        }
    }
}

fn operand(output: &mut String, value: &Operand) {
    match value {
        Operand::EventField(value, _) => {
            output.push_str("event.");
            output.push_str(value);
        }
        Operand::Parameter(value, _) => {
            output.push('$');
            output.push_str(value);
        }
        Operand::Literal(Literal::Integer(value), _) => {
            write!(output, "{value}").expect("String writes cannot fail")
        }
        Operand::Literal(Literal::Boolean(value), _) => {
            output.push_str(if *value { "true" } else { "false" })
        }
        Operand::Literal(Literal::String(value), _) => {
            output.push('"');
            for character in value.chars() {
                match character {
                    '"' => output.push_str("\\\""),
                    '\\' => output.push_str("\\\\"),
                    '\n' => output.push_str("\\n"),
                    '\r' => output.push_str("\\r"),
                    '\t' => output.push_str("\\t"),
                    value => output.push(value),
                }
            }
            output.push('"');
        }
        Operand::Literal(Literal::Symbol(value), _) => output.push_str(value),
    }
}

const fn operator_text(value: BinaryOperator) -> &'static str {
    match value {
        BinaryOperator::Equal => "==",
        BinaryOperator::NotEqual => "!=",
        BinaryOperator::Less => "<",
        BinaryOperator::LessEqual => "<=",
        BinaryOperator::Greater => ">",
        BinaryOperator::GreaterEqual => ">=",
        BinaryOperator::And => "&&",
        BinaryOperator::Or => "||",
    }
}
