//! Versioned binding codec, including bounded selector descriptors.
use super::*;

pub(super) fn encode_binding(
    writer: &mut Writer,
    binding: &BindingPlan,
    include_display_names: bool,
    ir_version: u32,
) -> Result<(), IrValidationError> {
    writer.u32(binding.id().get())?;
    if include_display_names {
        writer.string(binding.name())?;
    }
    writer.u8(binding.mode() as u8)?;
    writer.u32(binding.entity_type().get())?;
    encode_key_schema(writer, binding.key_schema())?;
    writer.u32(binding.key_expressions().len() as u32)?;
    for expression in binding.key_expressions() {
        writer.u32(expression.get())?;
    }
    writer.u32(binding.accessed_fields().len() as u32)?;
    for field in binding.accessed_fields() {
        writer.u32(field.get())?;
    }
    writer.bool(binding.complete_record_access())?;
    if ir_version >= EXECUTABLE_IR_VERSION_V17 {
        writer.u32(binding.initializer().len() as u32)?;
        for field in binding.initializer() {
            writer.u32(field.field_id().get())?;
            writer.u32(field.expression().get())?;
        }
        writer.bool(binding.failure().is_some())?;
        if let Some(failure) = binding.failure() {
            encode_outcome_construction(writer, failure)?;
        }
    } else {
        let failure = binding
            .failure()
            .ok_or(IrValidationError::UnsupportedVersion {
                kind: "initialized mutable binding executable IR",
                value: ir_version,
            })?;
        encode_outcome_construction(writer, failure)?;
    }
    if ir_version >= EXECUTABLE_IR_VERSION_V6 {
        writer.bool(binding.restriction_failure().is_some())?;
        if let Some(failure) = binding.restriction_failure() {
            encode_outcome_construction(writer, failure)?;
        }
    }
    if ir_version >= EXECUTABLE_IR_VERSION_V13 {
        writer.bool(binding.cascade_failure().is_some())?;
        if let Some(failure) = binding.cascade_failure() {
            encode_outcome_construction(writer, failure)?;
        }
    }
    if ir_version >= EXECUTABLE_IR_VERSION_V26 {
        writer.bool(binding.indexed_selection().is_some())?;
        if let Some(selection) = binding.indexed_selection() {
            writer.u32(selection.index().get())?;
            writer.u32(selection.prefix_expressions().len() as u32)?;
            for expression in selection.prefix_expressions() {
                writer.u32(expression.get())?;
            }
        }
    }
    Ok(())
}

pub(super) fn decode_binding(
    reader: &mut Reader<'_>,
    outcomes: &[OutcomeSchema],
    arena: &ExpressionArena,
    ir_version: u32,
) -> Result<BindingPlan, IrValidationError> {
    let id = BindingId::new(reader.u32()?);
    let name = reader.string(256)?;
    let mode = decode_binding_mode(reader.u8()?, ir_version)?;
    let entity_type = decode_entity_id(reader)?;
    let key_schema = decode_key_schema(reader, 0)?;
    let key_count = decode_len(reader, "binding key expressions", 1_024)?;
    let mut key_expressions = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        key_expressions.push(ExprId::new(reader.u32()?));
    }
    let field_count = decode_len(reader, "binding accessed fields", crate::MAX_COMMAND_ITEMS)?;
    let mut accessed_fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        accessed_fields.push(decode_field_id(reader)?);
    }
    let complete_record_access = reader.bool()?;
    let initializer = if ir_version >= EXECUTABLE_IR_VERSION_V17 {
        let count = decode_len(
            reader,
            "binding initializer fields",
            crate::MAX_COMMAND_ITEMS,
        )?;
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            fields.push(FieldExpression::new(
                decode_field_id(reader)?,
                ExprId::new(reader.u32()?),
            ));
        }
        fields
    } else {
        Vec::new()
    };
    let failure = if ir_version >= EXECUTABLE_IR_VERSION_V17 {
        if reader.bool()? {
            Some(decode_outcome_construction(reader, outcomes, arena)?)
        } else {
            None
        }
    } else {
        Some(decode_outcome_construction(reader, outcomes, arena)?)
    };
    let restriction_failure = if ir_version >= EXECUTABLE_IR_VERSION_V6 && reader.bool()? {
        Some(decode_outcome_construction(reader, outcomes, arena)?)
    } else {
        None
    };
    let cascade_failure = if ir_version >= EXECUTABLE_IR_VERSION_V13 && reader.bool()? {
        Some(decode_outcome_construction(reader, outcomes, arena)?)
    } else {
        None
    };
    let selection = if ir_version >= EXECUTABLE_IR_VERSION_V26 && reader.bool()? {
        let index = decode_index_id(reader)?;
        let count = decode_len(
            reader,
            "selection prefix expressions",
            crate::MAX_OBJECT_FIELDS,
        )?;
        let mut prefix = Vec::with_capacity(count);
        for _ in 0..count {
            prefix.push(ExprId::new(reader.u32()?));
        }
        Some(crate::IndexedSelectionPlan::new(index, prefix)?)
    } else {
        None
    };
    let binding = if matches!(
        mode,
        BindingMode::InitOrMutate | BindingMode::ObserveOrInitialize
    ) {
        if failure.is_some() || restriction_failure.is_some() || cascade_failure.is_some() {
            return Err(IrValidationError::InvalidDependency {
                reason: "initialized mutable binding carries a failure outcome",
            });
        }
        if mode == BindingMode::InitOrMutate {
            BindingPlan::new_initialized(
                id,
                name,
                entity_type,
                key_schema,
                key_expressions,
                accessed_fields,
                complete_record_access,
                initializer,
            )
        } else {
            BindingPlan::new_deferred_initialized(
                id,
                name,
                entity_type,
                key_schema,
                key_expressions,
                accessed_fields,
                complete_record_access,
                initializer,
            )
        }
    } else {
        BindingPlan::new_with_delete_failures(
            id,
            name,
            mode,
            entity_type,
            key_schema,
            key_expressions,
            accessed_fields,
            complete_record_access,
            failure.ok_or(IrValidationError::InvalidDependency {
                reason: "ordinary binding omits its failure outcome",
            })?,
            restriction_failure,
            cascade_failure,
        )
    }?;
    match selection {
        Some(selection) => binding.with_indexed_selection(selection),
        None => Ok(binding),
    }
}

pub(super) fn decode_binding_mode(
    tag: u8,
    ir_version: u32,
) -> Result<BindingMode, IrValidationError> {
    match tag {
        binding_tag::READ => Ok(BindingMode::Read),
        binding_tag::MUTATE => Ok(BindingMode::Mutate),
        binding_tag::CREATE => Ok(BindingMode::Create),
        binding_tag::DELETE if ir_version >= EXECUTABLE_IR_VERSION_V5 => Ok(BindingMode::Delete),
        binding_tag::INIT_OR_MUTATE if ir_version >= EXECUTABLE_IR_VERSION_V17 => {
            Ok(BindingMode::InitOrMutate)
        }
        binding_tag::OBSERVE_OR_INITIALIZE if ir_version >= EXECUTABLE_IR_VERSION_V18 => {
            Ok(BindingMode::ObserveOrInitialize)
        }
        tag => Err(IrValidationError::UnknownTag {
            kind: "binding mode",
            tag,
        }),
    }
}
