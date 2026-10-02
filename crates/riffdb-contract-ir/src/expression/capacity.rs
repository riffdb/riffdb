//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_copy_v1!(BinaryOperator);

riffdb_types::inspect_owned_capacity_copy_v1!(BindingId);

riffdb_types::inspect_owned_capacity_copy_v1!(ExprId);

riffdb_types::inspect_owned_capacity_struct_v1!(ExpressionArena {
    owned: [nodes],
    inline: []
});

impl riffdb_types::InspectOwnedCapacityV1 for ExpressionKind {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::Constant(field_0) => {
                inspection.visit(field_0)?;
            }
            Self::InputField(field_0) => {
                inspection.visit(field_0)?;
            }
            Self::ServiceValue(field_0) => {
                inspection.visit(field_0)?;
            }
            Self::CollectionElement => {}
            Self::CollectionElementField(field_0) => {
                inspection.visit(field_0)?;
            }
            Self::CompleteBinding(field_0) => {
                inspection.visit(field_0)?;
            }
            Self::BoundField { binding, field } => {
                inspection.visit(binding)?;
                inspection.visit(field)?;
            }
            Self::SchemaField { entity_type, field } => {
                inspection.visit(entity_type)?;
                inspection.visit(field)?;
            }
            Self::RootValidationField { read, field } => {
                inspection.visit(read)?;
                inspection.visit(field)?;
            }
            Self::SourceEventField(field_0) => {
                inspection.visit(field_0)?;
            }
            Self::TransactionTime => {}
            Self::TransactionDate => {}
            Self::Unary { operator, operand } => {
                inspection.visit(operator)?;
                inspection.visit(operand)?;
            }
            Self::Binary {
                operator,
                left,
                right,
            } => {
                inspection.visit(operator)?;
                inspection.visit(left)?;
                inspection.visit(right)?;
            }
        }
        Ok(())
    }
}

riffdb_types::inspect_owned_capacity_copy_v1!(RootValidationReadId);

riffdb_types::inspect_owned_capacity_struct_v1!(TypedExpression {
    owned: [kind, result_type],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(UnaryOperator);
