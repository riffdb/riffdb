//! Deterministic bounded renderers over checked diagnostic and plan DTOs.

use std::error::Error;
use std::fmt;
use std::fmt::Write;

use riffdb_contract_compiler::CompilationError;
use riffdb_contract_ir::{CommandExplain, ExecutionClass, KeyPurpose};

/// Maximum bytes in one compiler diagnostic report.
pub const MAX_DIAGNOSTIC_REPORT_BYTES: usize = 65_536;

/// Maximum bytes in one command explain report.
pub const MAX_EXPLAIN_REPORT_BYTES: usize = 1_048_576;

/// A bounded rendered operator report.
#[derive(Clone, Eq, PartialEq)]
pub struct OperatorReport {
    text: String,
}

impl OperatorReport {
    /// Borrows the complete deterministic report.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Returns its checked UTF-8 byte length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.text.len()
    }

    /// Reports whether the checked report is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }
}

impl fmt::Debug for OperatorReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OperatorReport([REDACTED])")
    }
}

/// Renders compiler diagnostics without source snippets or caller-controlled text.
pub fn render_compilation_diagnostics(
    error: &CompilationError,
) -> Result<OperatorReport, DiagnosticRenderError> {
    let mut output = BoundedText::new(MAX_DIAGNOSTIC_REPORT_BYTES);
    writeln!(output, "format=riffdb-compiler-diagnostics-v1")?;
    match error {
        CompilationError::Syntax(diagnostics) => {
            for diagnostic in diagnostics.as_slice() {
                let span = diagnostic.span();
                writeln!(
                    output,
                    "{} span={}..{} summary={}",
                    diagnostic.code().as_str(),
                    span.start(),
                    span.end(),
                    diagnostic.code().summary()
                )?;
                if let Some(help) = diagnostic.code().help() {
                    writeln!(output, "help={help}")?;
                }
                if !diagnostic.expected().is_empty() {
                    write!(output, "expected=")?;
                    for (index, expected) in diagnostic.expected().iter().enumerate() {
                        if index != 0 {
                            write!(output, ",")?;
                        }
                        write!(output, "{expected}")?;
                    }
                    writeln!(output)?;
                }
            }
        }
        CompilationError::Semantic(diagnostics) => {
            for diagnostic in diagnostics.as_slice() {
                let span = diagnostic.primary_span();
                writeln!(
                    output,
                    "{} span={}..{} summary={}",
                    diagnostic.code().as_str(),
                    span.start(),
                    span.end(),
                    diagnostic.summary()
                )?;
                if let Some(related) = diagnostic.related_span() {
                    writeln!(
                        output,
                        "related_span={}..{}",
                        related.start(),
                        related.end()
                    )?;
                }
                if let Some(help) = diagnostic.code().help() {
                    writeln!(output, "help={help}")?;
                }
            }
        }
    }
    Ok(output.finish())
}

/// Renders the value-free structural command explanation required by `CMP-013`.
pub fn render_command_explain(
    explain: &CommandExplain,
) -> Result<OperatorReport, DiagnosticRenderError> {
    let mut output = BoundedText::new(MAX_EXPLAIN_REPORT_BYTES);
    writeln!(output, "format=riffdb-operator-command-explain-v1")?;
    writeln!(output, "command_id={}", explain.command_id().get())?;
    writeln!(
        output,
        "execution_class={}",
        execution_class(explain.execution_class())
    )?;
    write!(
        output,
        "partition=expression:{} components:{} purpose:",
        explain.partition_expression().get(),
        explain.partition_component_count()
    )?;
    write_key_purpose(&mut output, explain.partition_schema().purpose())?;
    writeln!(output)?;
    writeln!(
        output,
        "conflict_key_count={}",
        explain.conflict_key_count()
    )?;
    for (index, conflict) in explain.conflict_derivations().iter().enumerate() {
        write!(output, "conflict.{index}=purpose:")?;
        write_key_purpose(&mut output, conflict.schema().purpose())?;
        write!(output, " expressions:")?;
        write_ids(
            &mut output,
            conflict
                .expressions()
                .iter()
                .map(|expression| expression.get()),
        )?;
        writeln!(output)?;
    }
    write!(output, "bindings=")?;
    write_ids(
        &mut output,
        explain.bindings().iter().map(|binding| binding.get()),
    )?;
    writeln!(output)?;
    write!(output, "reads=")?;
    write_pairs(
        &mut output,
        explain
            .read_fields()
            .iter()
            .map(|(binding, field)| (binding.get(), field.get())),
    )?;
    writeln!(output)?;
    write!(output, "writes=")?;
    write_pairs(
        &mut output,
        explain
            .write_fields()
            .iter()
            .map(|(binding, field)| (binding.get(), field.get())),
    )?;
    writeln!(output)?;
    write!(output, "invariants=")?;
    write_ids(
        &mut output,
        explain.invariants().iter().map(|invariant| invariant.get()),
    )?;
    writeln!(output)?;
    write!(output, "events=")?;
    write_ids(
        &mut output,
        explain.events().iter().map(|event| event.get()),
    )?;
    writeln!(output)?;
    write!(output, "outcomes=")?;
    write_ids(
        &mut output,
        explain.outcomes().iter().map(|outcome| outcome.get()),
    )?;
    writeln!(output)?;
    writeln!(
        output,
        "root_validation_reads={}",
        explain.root_validation_reads().len()
    )?;
    writeln!(output, "commit_checks={}", explain.commit_checks().len())?;
    Ok(output.finish())
}

fn write_ids(
    output: &mut BoundedText,
    values: impl IntoIterator<Item = u32>,
) -> Result<(), fmt::Error> {
    for (index, value) in values.into_iter().enumerate() {
        if index != 0 {
            write!(output, ",")?;
        }
        write!(output, "{value}")?;
    }
    Ok(())
}

fn write_pairs(
    output: &mut BoundedText,
    values: impl IntoIterator<Item = (u32, u32)>,
) -> Result<(), fmt::Error> {
    for (index, (left, right)) in values.into_iter().enumerate() {
        if index != 0 {
            write!(output, ",")?;
        }
        write!(output, "{left}:{right}")?;
    }
    Ok(())
}

fn write_key_purpose(output: &mut BoundedText, purpose: KeyPurpose) -> Result<(), fmt::Error> {
    match purpose {
        KeyPurpose::Entity(entity) => write!(output, "entity:{}", entity.get()),
        KeyPurpose::Partition(aggregate) => write!(output, "partition:{}", aggregate.get()),
        KeyPurpose::Conflict(aggregate) => write!(output, "conflict:{}", aggregate.get()),
        KeyPurpose::Index {
            index_id,
            entity_type,
        } => write!(
            output,
            "index:{}:entity:{}",
            index_id.get(),
            entity_type.get()
        ),
    }
}

const fn execution_class(class: ExecutionClass) -> &'static str {
    match class {
        ExecutionClass::ReadOnly => "read_only",
        ExecutionClass::IdempotentMutation => "idempotent_mutation",
    }
}

struct BoundedText {
    text: String,
    limit: usize,
}

impl BoundedText {
    fn new(limit: usize) -> Self {
        Self {
            text: String::new(),
            limit,
        }
    }

    fn finish(self) -> OperatorReport {
        OperatorReport { text: self.text }
    }
}

impl Write for BoundedText {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let Some(new_len) = self.text.len().checked_add(text.len()) else {
            return Err(fmt::Error);
        };
        if new_len > self.limit {
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

/// A bounded renderer refused to produce output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticRenderError;

impl fmt::Display for DiagnosticRenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("operator diagnostic output exceeded its fixed bound")
    }
}

impl Error for DiagnosticRenderError {}

impl From<fmt::Error> for DiagnosticRenderError {
    fn from(_: fmt::Error) -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use riffdb_contract_compiler::{
        CompilationError, CompilerBoundResource, CompilerDiagnostic, CompilerDiagnostics,
        compile_contract_source,
    };
    use riffdb_contract_ir::KeyPurpose;
    use riffdb_contract_syntax::Span;
    use riffdb_types::{AggregateTypeId, EntityTypeId, IndexId};

    use super::*;

    const BUDGET: &str = include_str!("../../../contracts/examples/budget.riff");

    #[test]
    fn budget_first_command_explain_matches_snapshot() {
        let bundle = compile_contract_source(BUDGET).expect("budget contract compiles");
        let report = render_command_explain(&CommandExplain::from_plan(&bundle.commands()[0]))
            .expect("bounded explain");
        assert_eq!(
            report.as_str(),
            include_str!("../fixtures/budget-first-command-explain-v1.txt")
        );
        assert!(report.as_str().contains("partition=expression:"));
        assert!(report.as_str().contains("conflict_key_count="));
        assert!(report.as_str().contains("reads="));
        assert!(report.as_str().contains("writes="));
        assert!(report.as_str().contains("invariants="));
        assert!(report.as_str().contains("events="));
        assert!(report.as_str().contains("outcomes="));
    }

    #[test]
    fn compiler_diagnostic_renderer_never_echoes_source() {
        let canary = "riffdb-secret-token-canary";
        let source = format!("not_a_contract {canary}");
        let error = compile_contract_source(&source).expect_err("invalid contract");
        let report = render_compilation_diagnostics(&error).expect("bounded report");
        assert!(!report.as_str().contains(canary));
        assert!(matches!(error, CompilationError::Syntax(_)));
    }

    #[test]
    fn compiler_bound_renderer_preserves_closed_actual_and_maximum() {
        let error = CompilationError::Semantic(CompilerDiagnostics::single(
            CompilerDiagnostic::bound_exceeded(
                CompilerBoundResource::CommandCorrelatedIndexWork,
                65_536,
                65_535,
                Span::new(12, 20).expect("span"),
            ),
        ));
        let report = render_compilation_diagnostics(&error).expect("bounded report");

        assert!(report.as_str().contains(
            "RDB-C020 span=12..20 summary=command_correlated_index_work is 65536; maximum is 65535"
        ));
    }

    #[test]
    fn every_key_purpose_has_frozen_stable_text() {
        let purposes = [
            KeyPurpose::Entity(EntityTypeId::first()),
            KeyPurpose::Partition(AggregateTypeId::first()),
            KeyPurpose::Conflict(AggregateTypeId::new(2).expect("nonzero aggregate")),
            KeyPurpose::Index {
                index_id: IndexId::first(),
                entity_type: EntityTypeId::new(2).expect("nonzero entity"),
            },
        ];
        let mut output = BoundedText::new(1_024);
        for purpose in purposes {
            write_key_purpose(&mut output, purpose).expect("bounded key purpose");
            writeln!(output).expect("bounded line");
        }
        assert_eq!(
            output.finish().as_str(),
            include_str!("../fixtures/key-purpose-v1.txt")
        );
    }
}
