//! Exact-contract compiler for grammar-v1 reactive modules.

use std::collections::BTreeMap;

use riffdb_contract_ir::{ContractBundle, ValueType, ValueTypeTag};
use riffdb_query_ir::{
    CompiledReactiveOperationV1, ReactiveCommandDependencyV1, ReactiveDeliveryLimitsV1,
    ReactiveEventFieldV1, ReactiveEventV1, ReactiveModulePlanV1, ReactiveOperationPlanV1,
    ReactiveParameterV1, ReactivePartitionBindingV1, ReactivePredicateNodeV1,
    ReactiveQueryDependencyV1, ReactiveUpdateModeV1,
};
use riffdb_query_syntax::{
    Argument, BinaryOperator, Expression, Literal, Module, Operand, Span, UpdateMode, format_module,
};
use riffdb_types::{
    CanonicalString, CanonicalValue, QueryCostVectorV1, QueryModuleHash, QueryPlanHash,
    ReactiveOperationName, encode_canonical_value, hash_reactive_source,
};

/// One exact named-query fact supplied by immutable query-module compilation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveQueryCatalogEntry {
    module_hash: QueryModuleHash,
    query_name: String,
    plan_hash: QueryPlanHash,
    parameters: Vec<(String, String)>,
    cost: QueryCostVectorV1,
    patch_key: Vec<String>,
}

impl ReactiveQueryCatalogEntry {
    /// Constructs one exact query fact in canonical parameter order.
    #[doc(hidden)]
    pub fn checked(
        module_hash: QueryModuleHash,
        query_name: String,
        plan_hash: QueryPlanHash,
        parameters: Vec<(String, String)>,
        cost: QueryCostVectorV1,
        patch_key: Vec<String>,
    ) -> Option<Self> {
        if query_name.is_empty()
            || parameters.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || patch_key.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return None;
        }
        Some(Self {
            module_hash,
            query_name,
            plan_hash,
            parameters,
            cost,
            patch_key,
        })
    }
    /// Exact named query.
    #[must_use]
    pub fn query_name(&self) -> &str {
        &self.query_name
    }
}

/// Stable reactive compiler diagnostic code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReactiveCompileDiagnosticCode {
    /// A referenced contract, query, stream, or command symbol is absent.
    UnknownSymbol,
    /// Two exact static types disagree.
    TypeMismatch,
    /// Event selections do not prove one identical complete partition.
    CrossPartition,
    /// An event field is absent, unselected, or inconsistent across variants.
    InvalidEventField,
    /// Patch mode lacks one complete explicitly selected primary key.
    UnkeyedPatch,
    /// Aggregate query work exceeds a fixed application ceiling.
    ExcessiveCost,
    /// A reaction command lacks supported direct idempotency.
    InvalidReaction,
    /// A source declaration does not reproduce its exact dependency definition.
    DefinitionMismatch,
    /// A compiler-owned artifact bound was exceeded.
    LimitExceeded,
}

impl ReactiveCompileDiagnosticCode {
    /// Stable public diagnostic code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownSymbol => "RDB-RC001",
            Self::TypeMismatch => "RDB-RC002",
            Self::CrossPartition => "RDB-RC003",
            Self::InvalidEventField => "RDB-RC004",
            Self::UnkeyedPatch => "RDB-RC005",
            Self::ExcessiveCost => "RDB-RC006",
            Self::InvalidReaction => "RDB-RC007",
            Self::DefinitionMismatch => "RDB-RC008",
            Self::LimitExceeded => "RDB-RC009",
        }
    }
}

/// One value-free source-spanned semantic diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReactiveCompileDiagnostic {
    code: ReactiveCompileDiagnosticCode,
    span: Span,
}

impl ReactiveCompileDiagnostic {
    fn new(code: ReactiveCompileDiagnosticCode, span: Span) -> Self {
        Self { code, span }
    }
    /// Stable classification.
    #[must_use]
    pub const fn code(self) -> ReactiveCompileDiagnosticCode {
        self.code
    }
    /// Exact source span.
    #[must_use]
    pub const fn span(self) -> Span {
        self.span
    }
}

/// Resolves, proves, canonically encodes, and hashes one reactive module.
pub fn compile_reactive_module(
    source: &Module,
    contract: &ContractBundle,
    query_catalog: &[ReactiveQueryCatalogEntry],
) -> Result<ReactiveModulePlanV1, Vec<ReactiveCompileDiagnostic>> {
    Compiler::new(contract, query_catalog)
        .compile(source)
        .map_err(|error| vec![error])
}

struct Compiler<'a> {
    contract: &'a ContractBundle,
    queries: BTreeMap<&'a str, &'a ReactiveQueryCatalogEntry>,
    streams: BTreeMap<String, CompiledReactiveOperationV1>,
}

impl<'a> Compiler<'a> {
    fn new(contract: &'a ContractBundle, queries: &'a [ReactiveQueryCatalogEntry]) -> Self {
        Self {
            contract,
            queries: queries
                .iter()
                .map(|query| (query.query_name.as_str(), query))
                .collect(),
            streams: BTreeMap::new(),
        }
    }

    fn compile(
        mut self,
        source: &Module,
    ) -> Result<ReactiveModulePlanV1, ReactiveCompileDiagnostic> {
        let mut operations = Vec::new();
        for stream in source.streams() {
            let operation = self.compile_stream(stream)?;
            self.streams
                .insert(stream.name().to_owned(), operation.clone());
            operations.push(operation);
        }
        for watch in source.watches() {
            operations.push(self.compile_watch(watch)?);
        }
        for subscription in source.subscriptions() {
            operations.push(self.compile_subscription(subscription)?);
        }
        let canonical_source = format_module(source);
        ReactiveModulePlanV1::checked(
            source.name().to_owned(),
            source.version(),
            self.contract.lineage().clone(),
            self.contract.contract_version(),
            self.contract.bundle_hash(),
            hash_reactive_source(canonical_source.as_bytes()),
            operations,
        )
        .ok_or_else(|| {
            ReactiveCompileDiagnostic::new(
                ReactiveCompileDiagnosticCode::LimitExceeded,
                source.span(),
            )
        })
    }

    fn compile_stream(
        &self,
        source: &riffdb_query_syntax::Stream,
    ) -> Result<CompiledReactiveOperationV1, ReactiveCompileDiagnostic> {
        let parameters = self.parameters(source.parameters())?;
        let parameter_types = parameters
            .iter()
            .map(|value| (value.name().to_owned(), value.type_name().to_owned()))
            .collect::<BTreeMap<_, _>>();
        let mut events = Vec::new();
        let mut common_fields: Option<BTreeMap<String, String>> = None;
        let mut partition_signature: Option<Vec<(String, String)>> = None;
        for selected in source.events() {
            let event = self
                .contract
                .schema()
                .events()
                .iter()
                .find(|event| event.name() == selected.event())
                .ok_or_else(|| {
                    diagnostic(
                        ReactiveCompileDiagnosticCode::UnknownSymbol,
                        selected.span(),
                    )
                })?;
            let event_partition = event.partition().ok_or_else(|| {
                diagnostic(
                    ReactiveCompileDiagnosticCode::CrossPartition,
                    selected.span(),
                )
            })?;
            let signature = event_partition
                .fields()
                .iter()
                .map(|id| {
                    let field = event.payload().field(*id)?;
                    Some((
                        field.name().to_owned(),
                        type_name(field.value_type(), self.contract)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    diagnostic(
                        ReactiveCompileDiagnosticCode::CrossPartition,
                        selected.span(),
                    )
                })?;
            if partition_signature
                .as_ref()
                .is_some_and(|existing| existing != &signature)
            {
                return Err(diagnostic(
                    ReactiveCompileDiagnosticCode::CrossPartition,
                    selected.span(),
                ));
            }
            partition_signature.get_or_insert(signature);
            let all_fields = event
                .payload()
                .fields()
                .iter()
                .map(|field| {
                    Some((
                        field.name().to_owned(),
                        type_name(field.value_type(), self.contract)?,
                    ))
                })
                .collect::<Option<BTreeMap<_, _>>>()
                .ok_or_else(|| {
                    diagnostic(
                        ReactiveCompileDiagnosticCode::InvalidEventField,
                        selected.span(),
                    )
                })?;
            common_fields = Some(match common_fields {
                None => all_fields,
                Some(existing) => existing
                    .into_iter()
                    .filter(|(name, ty)| all_fields.get(name) == Some(ty))
                    .collect(),
            });
            let mut fields = Vec::new();
            for name in selected.fields() {
                let field = event
                    .payload()
                    .fields()
                    .iter()
                    .find(|field| field.name() == name)
                    .ok_or_else(|| {
                        diagnostic(
                            ReactiveCompileDiagnosticCode::InvalidEventField,
                            selected.span(),
                        )
                    })?;
                fields.push(
                    ReactiveEventFieldV1::checked(
                        name.clone(),
                        field.id(),
                        type_name(field.value_type(), self.contract).ok_or_else(|| {
                            diagnostic(ReactiveCompileDiagnosticCode::TypeMismatch, selected.span())
                        })?,
                    )
                    .ok_or_else(|| {
                        diagnostic(
                            ReactiveCompileDiagnosticCode::LimitExceeded,
                            selected.span(),
                        )
                    })?,
                );
            }
            events.push(
                ReactiveEventV1::checked(event.name().to_owned(), event.id(), fields).ok_or_else(
                    || {
                        diagnostic(
                            ReactiveCompileDiagnosticCode::LimitExceeded,
                            selected.span(),
                        )
                    },
                )?,
            );
        }
        let signature = partition_signature.ok_or_else(|| {
            diagnostic(ReactiveCompileDiagnosticCode::CrossPartition, source.span())
        })?;
        if source.partition().len() != signature.len() {
            return Err(diagnostic(
                ReactiveCompileDiagnosticCode::CrossPartition,
                source.span(),
            ));
        }
        let mut partition = Vec::new();
        for (binding, (field, ty)) in source.partition().iter().zip(signature) {
            if binding.field() != field || parameter_types.get(binding.parameter()) != Some(&ty) {
                return Err(diagnostic(
                    ReactiveCompileDiagnosticCode::CrossPartition,
                    binding.span(),
                ));
            }
            partition.push(
                ReactivePartitionBindingV1::checked(field, binding.parameter().to_owned(), ty)
                    .ok_or_else(|| {
                        diagnostic(ReactiveCompileDiagnosticCode::LimitExceeded, binding.span())
                    })?,
            );
        }
        let selected_common = events
            .iter()
            .map(|event| {
                event
                    .fields()
                    .iter()
                    .map(|field| (field.name().to_owned(), field.type_name().to_owned()))
                    .collect::<BTreeMap<_, _>>()
            })
            .reduce(|left, right| {
                left.into_iter()
                    .filter(|(name, ty)| right.get(name) == Some(ty))
                    .collect()
            })
            .unwrap_or_default();
        let available = common_fields
            .unwrap_or_default()
            .into_iter()
            .filter(|(name, ty)| selected_common.get(name) == Some(ty))
            .collect::<BTreeMap<_, _>>();
        let predicate = source
            .predicate()
            .map(|expression| self.predicate(expression, &parameter_types, &available))
            .transpose()?
            .unwrap_or_default();
        self.operation(
            source.name(),
            source.span(),
            ReactiveOperationPlanV1::Stream {
                parameters,
                partition,
                events,
                predicate,
            },
        )
    }

    fn compile_watch(
        &self,
        source: &riffdb_query_syntax::Watch,
    ) -> Result<CompiledReactiveOperationV1, ReactiveCompileDiagnostic> {
        let parameters = self.parameters(source.parameters())?;
        let query = self.query_dependency(source.query(), source.name(), &[], source.span())?;
        if parameters
            .iter()
            .map(|value| (value.name().to_owned(), value.type_name().to_owned()))
            .collect::<Vec<_>>()
            != self.queries[source.query()].parameters
        {
            return Err(diagnostic(
                ReactiveCompileDiagnosticCode::DefinitionMismatch,
                source.span(),
            ));
        }
        let entry = self.queries[source.query()];
        let (update_mode, patch_key) = match source.update_mode() {
            UpdateMode::Patch if entry.patch_key.is_empty() => {
                return Err(diagnostic(
                    ReactiveCompileDiagnosticCode::UnkeyedPatch,
                    source.span(),
                ));
            }
            UpdateMode::Patch => (ReactiveUpdateModeV1::Patch, entry.patch_key.clone()),
            UpdateMode::Reset => (ReactiveUpdateModeV1::Reset, Vec::new()),
        };
        self.operation(
            source.name(),
            source.span(),
            ReactiveOperationPlanV1::Watch {
                parameters,
                query,
                update_mode,
                patch_key,
            },
        )
    }

    fn compile_subscription(
        &self,
        source: &riffdb_query_syntax::Subscription,
    ) -> Result<CompiledReactiveOperationV1, ReactiveCompileDiagnostic> {
        let parameters = self.parameters(source.parameters())?;
        let parameter_types = parameters
            .iter()
            .map(|value| (value.name().to_owned(), value.type_name().to_owned()))
            .collect::<BTreeMap<_, _>>();
        let stream = self.streams.get(source.stream().stream()).ok_or_else(|| {
            diagnostic(
                ReactiveCompileDiagnosticCode::UnknownSymbol,
                source.stream().span(),
            )
        })?;
        let (stream_parameters, stream_events) = match stream.plan() {
            ReactiveOperationPlanV1::Stream {
                parameters, events, ..
            } => (parameters, events),
            _ => {
                return Err(diagnostic(
                    ReactiveCompileDiagnosticCode::DefinitionMismatch,
                    source.stream().span(),
                ));
            }
        };
        let stream_targets = stream_parameters
            .iter()
            .map(|value| (value.name().to_owned(), value.type_name().to_owned()))
            .collect::<Vec<_>>();
        let stream_arguments = self.arguments(
            source.stream().arguments(),
            &stream_targets,
            &parameter_types,
            &BTreeMap::new(),
            source.stream().span(),
        )?;
        let event_fields = stream_events
            .iter()
            .map(|event| {
                event
                    .fields()
                    .iter()
                    .map(|field| (field.name().to_owned(), field.type_name().to_owned()))
                    .collect::<BTreeMap<_, _>>()
            })
            .reduce(|left, right| {
                left.into_iter()
                    .filter(|(name, ty)| right.get(name) == Some(ty))
                    .collect()
            })
            .unwrap_or_default();
        let mut hydrations = Vec::new();
        let mut aggregate = QueryCostVectorV1::zero();
        for hydration in source.hydrations() {
            let query = self
                .queries
                .get(hydration.query())
                .copied()
                .ok_or_else(|| {
                    diagnostic(
                        ReactiveCompileDiagnosticCode::UnknownSymbol,
                        hydration.span(),
                    )
                })?;
            let arguments = self.arguments(
                hydration.arguments(),
                &query.parameters,
                &parameter_types,
                &event_fields,
                hydration.span(),
            )?;
            let dependency = self.query_dependency(
                hydration.query(),
                hydration.name(),
                &arguments,
                hydration.span(),
            )?;
            aggregate = add_cost(aggregate, dependency.cost()).ok_or_else(|| {
                diagnostic(
                    ReactiveCompileDiagnosticCode::ExcessiveCost,
                    hydration.span(),
                )
            })?;
            hydrations.push(dependency);
        }
        if aggregate.scanned_index_rows() > 500 || aggregate.encoded_result_bytes() > 4_194_304 {
            return Err(diagnostic(
                ReactiveCompileDiagnosticCode::ExcessiveCost,
                source.span(),
            ));
        }
        let mut reactions = Vec::new();
        for reaction in source.reactions() {
            let command = self
                .contract
                .commands()
                .iter()
                .find(|command| command.name() == reaction.command())
                .ok_or_else(|| {
                    diagnostic(
                        ReactiveCompileDiagnosticCode::UnknownSymbol,
                        reaction.span(),
                    )
                })?;
            if command.idempotency_input().is_none() {
                return Err(diagnostic(
                    ReactiveCompileDiagnosticCode::InvalidReaction,
                    reaction.span(),
                ));
            }
            reactions.push(
                ReactiveCommandDependencyV1::checked(
                    reaction.name().to_owned(),
                    command.name().to_owned(),
                    command.command_id(),
                )
                .ok_or_else(|| {
                    diagnostic(
                        ReactiveCompileDiagnosticCode::LimitExceeded,
                        reaction.span(),
                    )
                })?,
            );
        }
        let limits = source.limits();
        let limits = ReactiveDeliveryLimitsV1::new(
            limits.batch(),
            limits.in_flight(),
            limits.lease_seconds(),
        )
        .ok_or_else(|| diagnostic(ReactiveCompileDiagnosticCode::LimitExceeded, limits.span()))?;
        self.operation(
            source.name(),
            source.span(),
            ReactiveOperationPlanV1::Subscription {
                parameters,
                stream_name: stream.name().clone(),
                stream_hash: stream.identity(),
                stream_arguments,
                hydrations,
                reactions,
                limits,
            },
        )
    }

    fn parameters(
        &self,
        values: &[riffdb_query_syntax::Parameter],
    ) -> Result<Vec<ReactiveParameterV1>, ReactiveCompileDiagnostic> {
        values
            .iter()
            .map(|value| {
                let (_, name) =
                    resolve_type(value.type_name(), self.contract).ok_or_else(|| {
                        diagnostic(ReactiveCompileDiagnosticCode::UnknownSymbol, value.span())
                    })?;
                ReactiveParameterV1::checked(value.name().to_owned(), name).ok_or_else(|| {
                    diagnostic(ReactiveCompileDiagnosticCode::LimitExceeded, value.span())
                })
            })
            .collect()
    }

    fn query_dependency(
        &self,
        query_name: &str,
        local_name: &str,
        arguments: &[(String, String)],
        span: Span,
    ) -> Result<ReactiveQueryDependencyV1, ReactiveCompileDiagnostic> {
        let query = self
            .queries
            .get(query_name)
            .copied()
            .ok_or_else(|| diagnostic(ReactiveCompileDiagnosticCode::UnknownSymbol, span))?;
        ReactiveQueryDependencyV1::checked(
            local_name.to_owned(),
            query.module_hash,
            query.query_name.clone(),
            query.plan_hash,
            arguments.to_vec(),
            query.cost,
        )
        .ok_or_else(|| diagnostic(ReactiveCompileDiagnosticCode::LimitExceeded, span))
    }

    fn arguments(
        &self,
        values: &[Argument],
        targets: &[(String, String)],
        parameters: &BTreeMap<String, String>,
        event_fields: &BTreeMap<String, String>,
        span: Span,
    ) -> Result<Vec<(String, String)>, ReactiveCompileDiagnostic> {
        if values.len() != targets.len() {
            return Err(diagnostic(
                ReactiveCompileDiagnosticCode::DefinitionMismatch,
                span,
            ));
        }
        let by_name = values
            .iter()
            .map(|value| (value.name(), value))
            .collect::<BTreeMap<_, _>>();
        targets
            .iter()
            .map(|(name, ty)| {
                let value = by_name.get(name.as_str()).ok_or_else(|| {
                    diagnostic(ReactiveCompileDiagnosticCode::DefinitionMismatch, span)
                })?;
                let (actual, canonical) = operand_type(
                    value.value(),
                    Some(ty),
                    parameters,
                    event_fields,
                    self.contract,
                )
                .ok_or_else(|| {
                    diagnostic(ReactiveCompileDiagnosticCode::TypeMismatch, value.span())
                })?;
                if &actual != ty {
                    return Err(diagnostic(
                        ReactiveCompileDiagnosticCode::TypeMismatch,
                        value.span(),
                    ));
                }
                Ok((name.clone(), canonical))
            })
            .collect()
    }

    fn predicate(
        &self,
        value: &Expression,
        parameters: &BTreeMap<String, String>,
        event_fields: &BTreeMap<String, String>,
    ) -> Result<Vec<ReactivePredicateNodeV1>, ReactiveCompileDiagnostic> {
        let mut nodes = Vec::new();
        let result =
            compile_expression(value, parameters, event_fields, self.contract, &mut nodes)?;
        if result != "bool" {
            return Err(diagnostic(
                ReactiveCompileDiagnosticCode::TypeMismatch,
                value.span(),
            ));
        }
        Ok(nodes)
    }

    fn operation(
        &self,
        name: &str,
        span: Span,
        plan: ReactiveOperationPlanV1,
    ) -> Result<CompiledReactiveOperationV1, ReactiveCompileDiagnostic> {
        let name = ReactiveOperationName::new(name.to_owned())
            .map_err(|_| diagnostic(ReactiveCompileDiagnosticCode::LimitExceeded, span))?;
        CompiledReactiveOperationV1::checked(name, plan)
            .ok_or_else(|| diagnostic(ReactiveCompileDiagnosticCode::LimitExceeded, span))
    }
}

fn compile_expression(
    value: &Expression,
    parameters: &BTreeMap<String, String>,
    event_fields: &BTreeMap<String, String>,
    contract: &ContractBundle,
    nodes: &mut Vec<ReactivePredicateNodeV1>,
) -> Result<String, ReactiveCompileDiagnostic> {
    match value {
        Expression::Operand(_) => Err(diagnostic(
            ReactiveCompileDiagnosticCode::TypeMismatch,
            value.span(),
        )),
        Expression::Binary {
            left,
            operator: BinaryOperator::And | BinaryOperator::Or,
            right,
            ..
        } => {
            if compile_expression(left, parameters, event_fields, contract, nodes)? != "bool"
                || compile_expression(right, parameters, event_fields, contract, nodes)? != "bool"
            {
                return Err(diagnostic(
                    ReactiveCompileDiagnosticCode::TypeMismatch,
                    value.span(),
                ));
            }
            nodes.push(
                if matches!(
                    value,
                    Expression::Binary {
                        operator: BinaryOperator::And,
                        ..
                    }
                ) {
                    ReactivePredicateNodeV1::And
                } else {
                    ReactivePredicateNodeV1::Or
                },
            );
            Ok("bool".to_owned())
        }
        Expression::Binary {
            left,
            operator,
            right,
            ..
        } => {
            let (left_operand, right_operand) = match (&**left, &**right) {
                (Expression::Operand(left), Expression::Operand(right)) => (left, right),
                _ => {
                    return Err(diagnostic(
                        ReactiveCompileDiagnosticCode::TypeMismatch,
                        value.span(),
                    ));
                }
            };
            let left_known = known_operand_type(left_operand, parameters, event_fields);
            let right_known = known_operand_type(right_operand, parameters, event_fields);
            let expected = left_known.as_deref().or(right_known.as_deref());
            let (left_type, left_node) =
                predicate_operand(left_operand, expected, parameters, event_fields, contract)
                    .ok_or_else(|| {
                        diagnostic(ReactiveCompileDiagnosticCode::TypeMismatch, left.span())
                    })?;
            let (right_type, right_node) = predicate_operand(
                right_operand,
                Some(&left_type),
                parameters,
                event_fields,
                contract,
            )
            .ok_or_else(|| diagnostic(ReactiveCompileDiagnosticCode::TypeMismatch, right.span()))?;
            if left_type != right_type {
                return Err(diagnostic(
                    ReactiveCompileDiagnosticCode::TypeMismatch,
                    value.span(),
                ));
            }
            nodes.push(left_node);
            nodes.push(right_node);
            nodes.push(match operator {
                BinaryOperator::Equal => ReactivePredicateNodeV1::Equal,
                BinaryOperator::NotEqual => ReactivePredicateNodeV1::NotEqual,
                BinaryOperator::Less => ReactivePredicateNodeV1::Less,
                BinaryOperator::LessEqual => ReactivePredicateNodeV1::LessEqual,
                BinaryOperator::Greater => ReactivePredicateNodeV1::Greater,
                BinaryOperator::GreaterEqual => ReactivePredicateNodeV1::GreaterEqual,
                BinaryOperator::And | BinaryOperator::Or => unreachable!(),
            });
            Ok("bool".to_owned())
        }
    }
}

fn predicate_operand(
    value: &Operand,
    expected: Option<&str>,
    parameters: &BTreeMap<String, String>,
    event_fields: &BTreeMap<String, String>,
    contract: &ContractBundle,
) -> Option<(String, ReactivePredicateNodeV1)> {
    match value {
        Operand::EventField(name, _) => event_fields.get(name).cloned().map(|ty| {
            (
                ty.clone(),
                ReactivePredicateNodeV1::EventField(name.clone(), ty),
            )
        }),
        Operand::Parameter(name, _) => parameters.get(name).cloned().map(|ty| {
            (
                ty.clone(),
                ReactivePredicateNodeV1::Parameter(name.clone(), ty),
            )
        }),
        Operand::Literal(literal, _) => {
            let expected = expected?;
            let canonical = literal_value(literal, expected, contract)?;
            Some((
                expected.to_owned(),
                ReactivePredicateNodeV1::Literal(
                    expected.to_owned(),
                    encode_canonical_value(&canonical).ok()?,
                ),
            ))
        }
    }
}

fn operand_type(
    value: &Operand,
    expected: Option<&str>,
    parameters: &BTreeMap<String, String>,
    event_fields: &BTreeMap<String, String>,
    contract: &ContractBundle,
) -> Option<(String, String)> {
    match value {
        Operand::Parameter(name, _) => parameters
            .get(name)
            .cloned()
            .map(|ty| (ty, format!("${name}"))),
        Operand::EventField(name, _) => event_fields
            .get(name)
            .cloned()
            .map(|ty| (ty, format!("event.{name}"))),
        Operand::Literal(value, _) => {
            let expected = expected?;
            let encoded =
                encode_canonical_value(&literal_value(value, expected, contract)?).ok()?;
            Some((
                expected.to_owned(),
                format!("literal:{}:{}", expected, hex(&encoded)),
            ))
        }
    }
}

fn known_operand_type(
    value: &Operand,
    parameters: &BTreeMap<String, String>,
    event_fields: &BTreeMap<String, String>,
) -> Option<String> {
    match value {
        Operand::Parameter(name, _) => parameters.get(name).cloned(),
        Operand::EventField(name, _) => event_fields.get(name).cloned(),
        Operand::Literal(_, _) => None,
    }
}

fn literal_value(
    value: &Literal,
    expected: &str,
    contract: &ContractBundle,
) -> Option<CanonicalValue> {
    match (value, expected) {
        (Literal::Integer(value), "i64") => Some(CanonicalValue::I64(*value)),
        (Literal::Integer(value), "u64") => u64::try_from(*value).ok().map(CanonicalValue::U64),
        (Literal::Boolean(value), "bool") => Some(CanonicalValue::Bool(*value)),
        (Literal::String(value), ty) if ty.starts_with("string<") => {
            CanonicalString::new(value.clone())
                .ok()
                .map(CanonicalValue::String)
        }
        (Literal::Symbol(symbol), expected) => {
            let (enumeration_name, variant_name) = symbol.split_once('.')?;
            let enumeration = contract
                .schema()
                .enums()
                .iter()
                .find(|value| value.name() == enumeration_name)?;
            if type_name(&ValueType::enumeration(enumeration.id()), contract)? != expected {
                return None;
            }
            let variant = enumeration
                .variants()
                .iter()
                .find(|value| value.name() == variant_name)?;
            Some(CanonicalValue::Enum {
                type_id: enumeration.id(),
                variant_id: variant.id(),
            })
        }
        _ => None,
    }
}

fn resolve_type(source: &str, contract: &ContractBundle) -> Option<(Option<ValueType>, String)> {
    if source == "Limit" {
        return Some((None, "limit".to_owned()));
    }
    if source == "Cursor" {
        return Some((None, "cursor".to_owned()));
    }
    if let Some((entity_name, field_name)) = source.split_once('.') {
        let entity = contract
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == entity_name)?;
        let field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == field_name)?;
        return Some((
            Some(field.value_type().clone()),
            type_name(field.value_type(), contract)?,
        ));
    }
    let enumeration = contract
        .schema()
        .enums()
        .iter()
        .find(|enumeration| enumeration.name() == source)?;
    let ty = ValueType::enumeration(enumeration.id());
    Some((Some(ty.clone()), type_name(&ty, contract)?))
}

fn type_name(value: &ValueType, contract: &ContractBundle) -> Option<String> {
    Some(match value.tag() {
        ValueTypeTag::Bool => "bool".to_owned(),
        ValueTypeTag::I64 => "i64".to_owned(),
        ValueTypeTag::U64 => "u64".to_owned(),
        ValueTypeTag::Decimal => {
            let spec = value.decimal_spec()?;
            format!("decimal<{},{}>", spec.precision(), spec.scale())
        }
        ValueTypeTag::Money => format!("money<{}>", value.currency()?),
        ValueTypeTag::String => format!("string<{}>", value.byte_bound()?),
        ValueTypeTag::Bytes => format!("bytes<{}>", value.byte_bound()?),
        ValueTypeTag::Timestamp => "timestamp".to_owned(),
        ValueTypeTag::Date => "date".to_owned(),
        ValueTypeTag::Uuid => "uuid".to_owned(),
        ValueTypeTag::Enum => contract
            .schema()
            .enums()
            .iter()
            .find(|enumeration| Some(enumeration.id()) == value.enum_type_id())
            .map(|value| value.name().to_owned())?,
        ValueTypeTag::Optional | ValueTypeTag::List | ValueTypeTag::Record => return None,
        ValueTypeTag::Vector => {
            format!("vector<{}>", value.vector_dimension()?.get())
        }
    })
}

fn add_cost(left: QueryCostVectorV1, right: QueryCostVectorV1) -> Option<QueryCostVectorV1> {
    QueryCostVectorV1::new(
        left.access_steps().checked_add(right.access_steps())?,
        left.scanned_index_rows()
            .checked_add(right.scanned_index_rows())?,
        left.point_reads().checked_add(right.point_reads())?,
        left.dependent_keys().checked_add(right.dependent_keys())?,
        left.intermediate_rows()
            .checked_add(right.intermediate_rows())?,
        left.projected_values()
            .checked_add(right.projected_values())?,
        left.encoded_result_bytes()
            .checked_add(right.encoded_result_bytes())?,
    )
}
fn diagnostic(code: ReactiveCompileDiagnosticCode, span: Span) -> ReactiveCompileDiagnostic {
    ReactiveCompileDiagnostic::new(code, span)
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("String writes cannot fail");
    }
    output
}
