//! Same-parser Source bundle construction; private staged implementation.
//! No incomplete bundle, callback, schema/budget pair or Source authority escapes.
use super::source_decode_sink::SourceDecodeSinkV1;
use super::*;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};

// This is execution state over the one existing semantic IR, not a second AST.
// Every accumulated data field dies before its actual originating allowance.
struct SourceBundleConstructionV1<'source> {
    bundle: Option<ContractBundle>,
    ledger: Option<LineageLedgerV1>,
    schema: Option<SchemaIr>,
    workflows: Option<WorkflowCatalog>,
    row_policies: Option<RowPolicyCatalogV1>,
    mcp_command_names: Option<McpCommandNameRegistryV2>,
    compatibility: Option<CompatibilityReport>,
    sink: SourceDecodeSinkV1<'source>,
}

// Every failure is scalar existing IrValidationError or admission refusal. The
// original allowance stays last even if future bounded error data gains an owner.
enum SourceBundleFailureV1 {
    Unavailable,
    Invalid(IrValidationError),
}
impl From<BudgetUnavailable> for SourceBundleFailureV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::Unavailable
    }
}
impl From<IrValidationError> for SourceBundleFailureV1 {
    fn from(error: IrValidationError) -> Self {
        Self::Invalid(error)
    }
}
impl From<ProviderBundleEncodingErrorV1> for SourceBundleFailureV1 {
    fn from(error: ProviderBundleEncodingErrorV1) -> Self {
        match error {
            ProviderBundleEncodingErrorV1::Unavailable => Self::Unavailable,
            ProviderBundleEncodingErrorV1::Invalid(error) => Self::Invalid(error),
        }
    }
}

/// Source admission refusal remains distinct from unchanged canonical rejection.
/// No raw partially constructed semantic value or numerical ownership escapes.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct SourceBundleDecodeErrorV1 {
    failure: SourceBundleFailureV1,
    _control: Option<ProjectionSourceWorkReservationV1>,
}
#[cfg_attr(not(test), allow(dead_code))]
impl SourceBundleDecodeErrorV1 {
    pub(super) fn is_admission_unavailable(&self) -> bool {
        matches!(self.failure, SourceBundleFailureV1::Unavailable)
    }
    pub(super) fn validation_error(&self) -> Option<&IrValidationError> {
        match &self.failure {
            SourceBundleFailureV1::Invalid(error) => Some(error),
            SourceBundleFailureV1::Unavailable => None,
        }
    }
}
impl std::fmt::Debug for SourceBundleDecodeErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.is_admission_unavailable() {
            "SourceBundleDecodeErrorV1::Unavailable"
        } else {
            "SourceBundleDecodeErrorV1::Invalid"
        })
    }
}
impl std::fmt::Display for SourceBundleDecodeErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.is_admission_unavailable() {
            "source bundle decode unavailable"
        } else {
            "source bundle rejected"
        })
    }
}
impl std::error::Error for SourceBundleDecodeErrorV1 {}

#[cfg_attr(not(test), allow(dead_code))]
impl<'source> SourceBundleConstructionV1<'source> {
    fn new(
        budget: &'source ProjectionSourceWorkingBudgetV1,
    ) -> Result<Self, SourceBundleDecodeErrorV1> {
        // Compiled inline owner/fork/walk headers precede the first borrowed
        // family walk. This is NOT a compiler stack bound: the actual fixed
        // execution stack must independently be preadmitted by its closed owner. SourceDecodeSink additionally reserves the
        // existing full4MiB physical ownership inspection scratch.
        let controls = std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of::<SourceBundleDecodeErrorV1>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Reader<'_>>()))
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<ledger_admission::LedgerSectionBound>())
            })
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<
                    schema_decode_admission::SchemaSectionBound,
                >())
            })
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<workflow_admission::WorkflowSectionBound>())
            })
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<
                    row_policy_admission::RowPolicySectionBound,
                >())
            })
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<
                    compatibility_admission::CompatibilitySectionBound,
                >())
            })
            .and_then(|n| n.checked_add(std::mem::size_of::<mcp_admission::McpSectionBound>()))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(SourceBundleDecodeErrorV1 {
                failure: SourceBundleFailureV1::Unavailable,
                _control: None,
            })?;
        let controls = controls.checked_add(
            riffdb_types::exact_provider_budget_v1::ProviderAllocationOwner::<ContractBundle>::settlement_root_capacity_upper_bound()
                .map_err(|_| SourceBundleDecodeErrorV1 { failure: SourceBundleFailureV1::Unavailable, _control: None })?,
        ).ok_or(SourceBundleDecodeErrorV1 { failure: SourceBundleFailureV1::Unavailable, _control: None })?;
        let sink =
            SourceDecodeSinkV1::new(budget, controls).map_err(|_| SourceBundleDecodeErrorV1 {
                failure: SourceBundleFailureV1::Unavailable,
                _control: None,
            })?;
        Ok(Self {
            bundle: None,
            ledger: None,
            schema: None,
            workflows: None,
            row_policies: None,
            mcp_command_names: None,
            compatibility: None,
            sink,
        })
    }

    // Private full-heap kernel only. The production Source entry must compose
    // the actual preadmitted/joined fixed-stack owner before exposing this path.
    // Neither the budget nor this numerical result establishes native Source
    // identity, completed Original input, D4 or publication authority.
    fn decode_all(
        mut self,
        bytes: &[u8],
    ) -> Result<
        riffdb_types::ProjectionSourceWorkAllocationV1<ContractBundle>,
        SourceBundleDecodeErrorV1,
    > {
        if self.bundle.is_some()
            || self.ledger.is_some()
            || self.schema.is_some()
            || self.workflows.is_some()
            || self.row_policies.is_some()
            || self.mcp_command_names.is_some()
            || self.compatibility.is_some()
        {
            return Err(self.into_failure(SourceBundleFailureV1::Unavailable));
        }
        match decode_bundle_with_admission(bytes, &mut self.sink) {
            Ok(bundle) => self.bundle = Some(bundle),
            Err(error) => return Err(self.into_failure(error.into())),
        }
        // No fallible loose-local transfer can refund the sink before the
        // complete value. Types settlement itself owns value FIRST/work LAST.
        match self {
            Self {
                bundle: Some(value),
                ledger: None,
                schema: None,
                workflows: None,
                row_policies: None,
                mcp_command_names: None,
                compatibility: None,
                sink,
            } => sink.settle(value).map_err(|_| SourceBundleDecodeErrorV1 {
                failure: SourceBundleFailureV1::Unavailable,
                _control: None,
            }),
            remaining => Err(remaining.into_failure(SourceBundleFailureV1::Unavailable)),
        }
    }

    fn into_failure(self, failure: SourceBundleFailureV1) -> SourceBundleDecodeErrorV1 {
        let Self {
            bundle,
            ledger,
            schema,
            workflows,
            row_policies,
            mcp_command_names,
            compatibility,
            sink,
        } = self;
        // Data retirement precedes allowance transfer/refund on every rejection.
        drop(bundle);
        drop(compatibility);
        drop(mcp_command_names);
        drop(row_policies);
        drop(workflows);
        drop(schema);
        drop(ledger);
        SourceBundleDecodeErrorV1 {
            failure,
            _control: Some(sink.into_retained()),
        }
    }
}

// There is deliberately no public Source decode entry while the actual fixed
// stack owner is pending. Full heap parsing/final validation is now one closed
// kernel above; no helper-only qualification or Source authority is claimed.
// Test-only heap/parser diagnostics deliberately run on the ordinary test
// harness. They do not prove a preadmitted physical Source execution stack,
// native resource factory or complete aggregate Source64 working set.
#[cfg(test)]
#[path = "source_decode_admission_tests.rs"]
mod tests;
