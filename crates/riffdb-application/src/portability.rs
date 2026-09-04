//! Compiler and adapter validation for foundational portability values.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{ContractBundle, RecordSchema};
use riffdb_types::EntityTypeId;

use crate::{AdapterConformanceManifest, RoleOperationKind};

pub use riffdb_types::application_portability::*;

/// Compiler- and adapter-owned checks over the serialization-neutral value.
pub trait ApplicationPortabilityValidation {
    /// Verifies every mapping against one exact compiler-owned contract bundle.
    fn validate_compiled_contract(
        &self,
        bundle: &ContractBundle,
    ) -> Result<(), ApplicationPortabilityError>;

    /// Derives the only legal entity reconstitution order.
    fn compiled_reimport_entity_schedule(
        &self,
        bundle: &ContractBundle,
    ) -> Result<Vec<EntityTypeId>, ApplicationPortabilityError>;

    /// Binds mappings and observations to one exact adapter-owned public surface.
    fn validate_adapter_conformance(
        &self,
        adapter: &AdapterConformanceManifest,
    ) -> Result<(), ApplicationPortabilityError>;
}

impl ApplicationPortabilityValidation for ApplicationPortabilityManifest {
    fn validate_compiled_contract(
        &self,
        bundle: &ContractBundle,
    ) -> Result<(), ApplicationPortabilityError> {
        let input = self.input();
        if bundle.lineage() != &input.contract_lineage
            || bundle.contract_version() != input.contract_version
            || bundle.bundle_hash() != input.contract_bundle_hash
        {
            return Err(error(
                ApplicationPortabilityErrorKind::ReconciliationMismatch,
            ));
        }
        for mapping in &input.mappings {
            let source = match mapping.class() {
                PortableRecordClass::Entity => bundle
                    .schema()
                    .entities()
                    .iter()
                    .find(|entity| entity.name() == mapping.symbol().as_str())
                    .map(|entity| entity.record()),
                PortableRecordClass::Event => bundle
                    .schema()
                    .events()
                    .iter()
                    .find(|event| event.name() == mapping.symbol().as_str())
                    .map(|event| event.payload()),
            }
            .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
            match mapping.strategy() {
                PortableReimportStrategy::ReimportCommand { command } => {
                    if mapping.class() != PortableRecordClass::Entity {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    }
                    let command = bundle
                        .commands()
                        .iter()
                        .find(|plan| plan.name() == command.as_str())
                        .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
                    let target = command.input().record();
                    let [target_field] = target.fields() else {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    };
                    let Some((element, maximum)) = target_field.value_type().list_parts() else {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    };
                    if !command.is_reimport()
                        || command.idempotency_input().is_some()
                        || maximum == 0
                        || element.record_ref() != Some(source.owner())
                    {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    }
                }
                PortableReimportStrategy::LegacyApplicationCommand {
                    command,
                    idempotency_input,
                    record_input,
                    fields,
                } => {
                    let command = bundle
                        .commands()
                        .iter()
                        .find(|plan| plan.name() == command.as_str())
                        .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
                    let target = command.input().record();
                    if let Some(record_input) = record_input {
                        let target_field = field_by_name(target, record_input.as_str())
                            .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
                        let Some((element, maximum)) = target_field.value_type().list_parts()
                        else {
                            return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                        };
                        if maximum == 0 || element.record_ref() != Some(source.owner()) {
                            return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                        }
                    } else {
                        for binding in fields {
                            let source_field =
                                field_by_name(source, binding.source_field().as_str()).ok_or_else(
                                    || error(ApplicationPortabilityErrorKind::InvalidShape),
                                )?;
                            let target_field =
                                field_by_name(target, binding.command_input().as_str())
                                    .ok_or_else(|| {
                                        error(ApplicationPortabilityErrorKind::InvalidShape)
                                    })?;
                            if source_field.value_type() != target_field.value_type() {
                                return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                            }
                        }
                    }
                    let idempotency_field = field_by_name(target, idempotency_input.as_str());
                    if idempotency_field.is_none()
                        || command.idempotency_input() != idempotency_field.map(|field| field.id())
                        || target.fields().iter().any(|target_field| {
                            target_field.name() != idempotency_input.as_str()
                                && record_input.as_ref().is_none_or(|record_input| {
                                    record_input.as_str() != target_field.name()
                                })
                                && !fields.iter().any(|binding| {
                                    binding.command_input().as_str() == target_field.name()
                                })
                        })
                    {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    }
                }
                PortableReimportStrategy::Migration { .. } => {}
            }
        }
        Ok(())
    }

    fn compiled_reimport_entity_schedule(
        &self,
        bundle: &ContractBundle,
    ) -> Result<Vec<EntityTypeId>, ApplicationPortabilityError> {
        self.validate_compiled_contract(bundle)?;
        let selected = self
            .input()
            .mappings
            .iter()
            .filter(|mapping| mapping.class() == PortableRecordClass::Entity)
            .map(|mapping| {
                if !matches!(
                    mapping.strategy(),
                    PortableReimportStrategy::ReimportCommand { .. }
                ) {
                    return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                }
                let entity = bundle
                    .schema()
                    .entities()
                    .iter()
                    .find(|entity| entity.name() == mapping.symbol().as_str())
                    .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
                Ok((entity.id(), entity.name()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if selected.is_empty() {
            return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
        }
        let selected_ids = selected
            .iter()
            .map(|(entity, _)| *entity)
            .collect::<BTreeSet<_>>();
        let mut indegree = selected_ids
            .iter()
            .map(|entity| (*entity, 0_usize))
            .collect::<BTreeMap<_, _>>();
        let mut dependents = selected_ids
            .iter()
            .map(|entity| (*entity, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        for relationship in bundle.schema().relationships() {
            if !selected_ids.contains(&relationship.source_entity()) {
                continue;
            }
            if !selected_ids.contains(&relationship.target_entity()) {
                return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
            }
            if dependents
                .get_mut(&relationship.target_entity())
                .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?
                .insert(relationship.source_entity())
            {
                let count = indegree
                    .get_mut(&relationship.source_entity())
                    .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| error(ApplicationPortabilityErrorKind::LimitExceeded))?;
            }
        }
        let mut ready = indegree
            .iter()
            .filter_map(|(entity, count)| (*count == 0).then_some(*entity))
            .collect::<BTreeSet<_>>();
        let mut schedule = Vec::with_capacity(selected_ids.len());
        while let Some(entity) = ready.pop_first() {
            schedule.push(entity);
            for dependent in dependents.get(&entity).into_iter().flatten() {
                let count = indegree
                    .get_mut(dependent)
                    .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
                *count = count
                    .checked_sub(1)
                    .ok_or_else(|| error(ApplicationPortabilityErrorKind::InvalidShape))?;
                if *count == 0 {
                    ready.insert(*dependent);
                }
            }
        }
        if schedule.len() != selected_ids.len() {
            return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
        }
        Ok(schedule)
    }

    fn validate_adapter_conformance(
        &self,
        adapter: &AdapterConformanceManifest,
    ) -> Result<(), ApplicationPortabilityError> {
        let input = self.input();
        if adapter.identity() != input.adapter_manifest_hash
            || adapter.input().contract_lineage != input.contract_lineage
            || !adapter.input().evolution.iter().any(|evolution| {
                evolution.successor().version() == input.contract_version
                    && evolution.successor().bundle_hash() == input.contract_bundle_hash
            })
        {
            return Err(error(
                ApplicationPortabilityErrorKind::ReconciliationMismatch,
            ));
        }
        for mapping in &input.mappings {
            match mapping.strategy() {
                PortableReimportStrategy::ReimportCommand { command } => {
                    if adapter.input().roles.iter().any(|role| {
                        role.operations().iter().any(|operation| {
                            operation.kind() == RoleOperationKind::Command
                                && operation.name().as_str() == command.as_str()
                        })
                    }) {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    }
                }
                PortableReimportStrategy::LegacyApplicationCommand { command, .. } => {
                    if !adapter.input().roles.iter().any(|role| {
                        role.operations().iter().any(|operation| {
                            operation.kind() == RoleOperationKind::Command
                                && operation.name().as_str() == command.as_str()
                        })
                    }) {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    }
                }
                PortableReimportStrategy::Migration { migration_hash } => {
                    if !adapter
                        .input()
                        .evolution
                        .iter()
                        .any(|evolution| evolution.migration_hash() == Some(*migration_hash))
                    {
                        return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
                    }
                }
            }
        }
        if input.observations.iter().any(|observation| {
            !adapter.input().roles.iter().any(|role| {
                role.operations().iter().any(|operation| {
                    operation.kind() == RoleOperationKind::Query
                        && operation.name().as_str() == observation.query().as_str()
                })
            })
        }) {
            return Err(error(ApplicationPortabilityErrorKind::InvalidShape));
        }
        Ok(())
    }
}

const fn error(kind: ApplicationPortabilityErrorKind) -> ApplicationPortabilityError {
    ApplicationPortabilityError::new(kind)
}

fn field_by_name<'a>(
    record: &'a RecordSchema,
    name: &str,
) -> Option<&'a riffdb_contract_ir::FieldSchema> {
    record.fields().iter().find(|field| field.name() == name)
}
