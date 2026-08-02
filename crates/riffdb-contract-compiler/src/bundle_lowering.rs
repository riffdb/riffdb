//! Final assembly of checked plans into one immutable contract bundle.

use riffdb_contract_ir::{
    CommandPlan, CompatibilityReport, ContractBundle, GeneratedSchemaArtifact, LineageLedgerV1,
    McpCommandNameRegistryV2, ParentBundleRef, ProjectionPlan, SchemaIr, StableIdNamespace,
    StableIdNamespaceTag, StableIdentity, required_lineage_allocation_namespaces,
};
use riffdb_types::{ContractLineage, SourceHash};

use crate::symbols::GenesisSymbols;

const COMPILER_SEMANTIC_VERSION: &str = "0.1.0";

pub(crate) struct BundleParts {
    pub(crate) schema: SchemaIr,
    pub(crate) commands: Vec<CommandPlan>,
    pub(crate) projections: Vec<ProjectionPlan>,
    pub(crate) mcp_names: McpCommandNameRegistryV2,
}

pub(crate) fn assemble_bundle(
    symbols: &GenesisSymbols,
    source_hash: SourceHash,
    parent: Option<&ContractBundle>,
    parts: BundleParts,
    compatibility: CompatibilityReport,
) -> Result<ContractBundle, riffdb_contract_ir::IrValidationError> {
    let lineage =
        ContractLineage::new(parts.mcp_names.lineage().as_str().to_owned()).map_err(|_| {
            riffdb_contract_ir::IrValidationError::InvalidText {
                kind: "contract lineage",
            }
        })?;
    let artifacts = generated_schema_artifacts(&parts.schema, &parts.commands, &parts.projections)?;
    let identities = stable_identities(&parts.schema, &parts.commands, &parts.projections)?;
    let required =
        required_lineage_allocation_namespaces(&parts.schema, &parts.commands, &parts.projections)?;
    let ledger = match parent {
        Some(parent) if !symbols.renames.is_empty() => {
            LineageLedgerV1::successor_complete_with_renames(
                parent.ledger(),
                identities,
                required,
                symbols.renames.clone(),
            )?
        }
        Some(parent) => LineageLedgerV1::successor_complete(parent.ledger(), identities, required)?,
        None => LineageLedgerV1::genesis_complete(identities, required)?,
    };
    let parent_ref =
        parent.map(|parent| ParentBundleRef::new(parent.contract_version(), parent.bundle_hash()));
    ContractBundle::new(
        COMPILER_SEMANTIC_VERSION,
        lineage,
        symbols.contract_version,
        parent_ref,
        source_hash,
        ledger,
        parts.schema,
        parts.commands,
        parts.projections,
        artifacts,
        parts.mcp_names,
        compatibility,
    )
}

fn generated_schema_artifacts(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<Vec<GeneratedSchemaArtifact>, riffdb_contract_ir::IrValidationError> {
    let mut artifacts = Vec::new();
    for entity in schema.entities() {
        artifacts.push(GeneratedSchemaArtifact::entity(
            entity.id(),
            entity.record(),
            schema,
        )?);
    }
    for event in schema.events() {
        artifacts.push(GeneratedSchemaArtifact::event(
            event.id(),
            event.payload(),
            schema,
        )?);
    }
    for command in commands {
        artifacts.push(GeneratedSchemaArtifact::command_input(
            command.command_id(),
            command.input().record(),
            schema,
            command.idempotency_input(),
        )?);
        artifacts.push(GeneratedSchemaArtifact::command_outcomes(
            command.command_id(),
            command.outcomes(),
            schema,
        )?);
    }
    for projection in projections {
        let group_types = projection
            .group_schema()
            .group_components()
            .iter()
            .map(|component| component.value_type().clone())
            .collect::<Vec<_>>();
        artifacts.push(GeneratedSchemaArtifact::projection_result(
            projection.projection_id(),
            &group_types,
            projection.group_schema().measures(),
            schema,
        )?);
    }
    Ok(artifacts)
}

pub(crate) fn stable_identities(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<Vec<StableIdentity>, riffdb_contract_ir::IrValidationError> {
    let mut identities = Vec::new();
    let mut push = |tag, owner_kind, owner_ids, name: &str| {
        let namespace = StableIdNamespace::new(tag, owner_kind, owner_ids)?;
        identities.push(StableIdentity::new(namespace, name)?);
        Ok::<_, riffdb_contract_ir::IrValidationError>(())
    };
    for entity in schema.entities() {
        push(StableIdNamespaceTag::Entity, 0, vec![], entity.name())?;
        for field in entity.record().fields() {
            push(
                StableIdNamespaceTag::Field,
                0x01,
                vec![entity.id().get()],
                field.name(),
            )?;
        }
        for invariant in entity.invariants() {
            push(
                StableIdNamespaceTag::Invariant,
                0x01,
                vec![entity.id().get()],
                invariant.name(),
            )?;
        }
        for index in entity.indexes() {
            push(
                StableIdNamespaceTag::Index,
                0x01,
                vec![entity.id().get()],
                index.name(),
            )?;
        }
    }
    for event in schema.events() {
        push(StableIdNamespaceTag::Event, 0, vec![], event.name())?;
        for field in event.payload().fields() {
            push(
                StableIdNamespaceTag::Field,
                0x02,
                vec![event.id().get()],
                field.name(),
            )?;
        }
    }
    for enumeration in schema.enums() {
        push(StableIdNamespaceTag::Enum, 0, vec![], enumeration.name())?;
        for variant in enumeration.variants() {
            push(
                StableIdNamespaceTag::EnumVariant,
                0x01,
                vec![enumeration.id().get()],
                variant.name(),
            )?;
        }
    }
    for aggregate in schema.aggregates() {
        push(StableIdNamespaceTag::Aggregate, 0, vec![], aggregate.name())?;
        for invariant in aggregate.invariants() {
            push(
                StableIdNamespaceTag::Invariant,
                0x02,
                vec![aggregate.id().get()],
                invariant.name(),
            )?;
        }
    }
    for command in commands {
        push(StableIdNamespaceTag::Command, 0, vec![], command.name())?;
        for field in command.input().record().fields() {
            push(
                StableIdNamespaceTag::Field,
                0x03,
                vec![command.command_id().get()],
                field.name(),
            )?;
        }
        for outcome in command.outcomes() {
            push(
                StableIdNamespaceTag::Outcome,
                0x01,
                vec![command.command_id().get()],
                outcome.name(),
            )?;
            for field in outcome.payload().fields() {
                push(
                    StableIdNamespaceTag::Field,
                    0x04,
                    vec![command.command_id().get(), outcome.id().get()],
                    field.name(),
                )?;
            }
        }
    }
    for projection in projections {
        push(
            StableIdNamespaceTag::Projection,
            0,
            vec![],
            projection.name(),
        )?;
        for field in projection.group_schema().measures().fields() {
            push(
                StableIdNamespaceTag::Field,
                0x05,
                vec![projection.projection_id().get()],
                field.name(),
            )?;
        }
    }
    Ok(identities)
}
