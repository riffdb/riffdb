//! Closed service-owned command values and their non-durable ownership.

use super::*;

/// Closed service-owned command-value kinds introduced by executable IR v2.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ServiceValueKind {
    /// UUID version 7 observed and sealed by the application service.
    UuidV7 = crate::format_registry::service_value_kind::UUID_V7,
    /// Trusted transaction time observed and sealed before deterministic evaluation.
    TransactionTime = crate::format_registry::service_value_kind::TRANSACTION_TIME,
}

/// One compiler-declared value that callers cannot supply or override.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceValueSchema {
    pub(super) field: FieldSchema,
    pub(super) kind: ServiceValueKind,
}

impl ServiceValueSchema {
    /// Creates one checked service-value declaration.
    pub fn new(field: FieldSchema, kind: ServiceValueKind) -> Result<Self, IrValidationError> {
        let valid_type = match kind {
            ServiceValueKind::UuidV7 => field.value_type().tag() == ValueTypeTag::Uuid,
            ServiceValueKind::TransactionTime => {
                field.value_type().tag() == ValueTypeTag::Timestamp
            }
        };
        if !valid_type {
            return Err(IrValidationError::TypeMismatch {
                context: "service-owned command value",
            });
        }
        Ok(Self { field, kind })
    }

    /// Stable command-scoped field identity.
    #[must_use]
    pub const fn field(&self) -> &FieldSchema {
        &self.field
    }

    /// Closed observation kind.
    #[must_use]
    pub const fn kind(&self) -> ServiceValueKind {
        self.kind
    }
}

riffdb_types::inspect_owned_capacity_copy_v1!(ServiceValueKind);

riffdb_types::inspect_owned_capacity_struct_v1!(ServiceValueSchema {
    owned: [field, kind],
    inline: []
});
