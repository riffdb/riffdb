//! Projection definition registration, validation, and fingerprinting (D2).

use std::fmt;

use riffdb_contract_ir::{ContractBundle, ValueType, ValueTypeTag};
use riffdb_types::{EntityTypeId, FieldId, HashDomain, hash};

/// Layout version frozen into definition fingerprints and manifests.
pub const LAYOUT_VERSION: u32 = 1;

/// Author-facing columnar projection definition before contract validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarProjectionDefinition {
    /// Stable projection name (engine-local; not a public catalog ID in CP1).
    pub name: String,
    /// Entity type name from the contract schema.
    pub entity_name: String,
    /// Ordered projected field IDs (must exist on the entity).
    pub projected_fields: Vec<FieldId>,
    /// Organization scope field (must exist on the entity).
    pub org_scope_field: FieldId,
}

/// Stable fingerprint over entity id, ordered fields + types, org field, layout.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DefinitionFingerprint([u8; 32]);

impl DefinitionFingerprint {
    /// Returns the 32 digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Reconstructs from exact digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for DefinitionFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Definition validated against a contract bundle and ready for engine open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredDefinition {
    name: String,
    entity_type_id: EntityTypeId,
    entity_name: String,
    projected_fields: Vec<FieldId>,
    projected_types: Vec<ValueType>,
    org_scope_field: FieldId,
    org_scope_type: ValueType,
    primary_key_fields: Vec<FieldId>,
    fingerprint: DefinitionFingerprint,
}

impl RegisteredDefinition {
    /// Validates `definition` against `bundle` and computes the fingerprint.
    pub fn register(
        definition: ColumnarProjectionDefinition,
        bundle: &ContractBundle,
    ) -> Result<Self, DefinitionError> {
        if definition.name.is_empty() || definition.name.len() > 256 {
            return Err(DefinitionError::InvalidName);
        }
        if definition.projected_fields.is_empty() {
            return Err(DefinitionError::EmptyProjectedFields);
        }
        let schema = bundle.schema();
        let entity = schema
            .entities()
            .iter()
            .find(|entity| entity.name() == definition.entity_name)
            .ok_or(DefinitionError::UnknownEntity {
                name: definition.entity_name.clone(),
            })?;
        let record = entity.record();
        let mut projected_types = Vec::with_capacity(definition.projected_fields.len());
        let mut seen = std::collections::BTreeSet::new();
        for field_id in &definition.projected_fields {
            if !seen.insert(*field_id) {
                return Err(DefinitionError::DuplicateProjectedField {
                    field_id: *field_id,
                });
            }
            let field = record
                .field(*field_id)
                .ok_or(DefinitionError::UnknownField {
                    field_id: *field_id,
                })?;
            if !is_supported_column_type(field.value_type()) {
                return Err(DefinitionError::UnsupportedColumnType {
                    field_id: *field_id,
                    tag: field.value_type().tag(),
                });
            }
            projected_types.push(field.value_type().clone());
        }
        let org_field = record.field(definition.org_scope_field).ok_or(
            DefinitionError::OrgFieldNotOnEntity {
                field_id: definition.org_scope_field,
            },
        )?;
        if org_field.value_type().tag() == ValueTypeTag::Optional {
            // Every row must carry a definite org partition; an absent org
            // scope would make the row unreachable from any query.
            return Err(DefinitionError::OptionalOrgScope {
                field_id: definition.org_scope_field,
            });
        }
        if !is_supported_column_type(org_field.value_type()) {
            return Err(DefinitionError::UnsupportedColumnType {
                field_id: definition.org_scope_field,
                tag: org_field.value_type().tag(),
            });
        }
        let fingerprint = compute_fingerprint(
            entity.id(),
            &definition.projected_fields,
            &projected_types,
            definition.org_scope_field,
            org_field.value_type(),
        );
        Ok(Self {
            name: definition.name,
            entity_type_id: entity.id(),
            entity_name: definition.entity_name,
            projected_fields: definition.projected_fields,
            projected_types,
            org_scope_field: definition.org_scope_field,
            org_scope_type: org_field.value_type().clone(),
            primary_key_fields: entity.primary_key_fields().to_vec(),
            fingerprint,
        })
    }

    /// Projection name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Projected entity type id.
    #[must_use]
    pub const fn entity_type_id(&self) -> EntityTypeId {
        self.entity_type_id
    }

    /// Entity type name.
    #[must_use]
    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    /// Ordered projected field ids.
    #[must_use]
    pub fn projected_fields(&self) -> &[FieldId] {
        &self.projected_fields
    }

    /// Ordered projected value types (aligned with [`Self::projected_fields`]).
    #[must_use]
    pub fn projected_types(&self) -> &[ValueType] {
        &self.projected_types
    }

    /// Organization scope field id.
    #[must_use]
    pub const fn org_scope_field(&self) -> FieldId {
        self.org_scope_field
    }

    /// Organization scope value type (never `Optional` — rejected at registration).
    #[must_use]
    pub const fn org_scope_type(&self) -> &ValueType {
        &self.org_scope_type
    }

    /// Primary key field ids on the entity.
    #[must_use]
    pub fn primary_key_fields(&self) -> &[FieldId] {
        &self.primary_key_fields
    }

    /// Definition fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> DefinitionFingerprint {
        self.fingerprint
    }
}

/// Registration rejection classes (D2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefinitionError {
    /// Empty or oversized projection name.
    InvalidName,
    /// No projected fields were supplied.
    EmptyProjectedFields,
    /// Entity name is not in the contract schema.
    UnknownEntity {
        /// Requested entity name.
        name: String,
    },
    /// Field id is not on the entity record.
    UnknownField {
        /// Missing field.
        field_id: FieldId,
    },
    /// Projected field list contained a duplicate id.
    DuplicateProjectedField {
        /// Duplicate field.
        field_id: FieldId,
    },
    /// Column type is outside the CP1 scalar set (List/Record nested, etc.).
    UnsupportedColumnType {
        /// Field with unsupported type.
        field_id: FieldId,
        /// Observed type tag.
        tag: ValueTypeTag,
    },
    /// Org scope field is not present on the entity.
    OrgFieldNotOnEntity {
        /// Missing org field.
        field_id: FieldId,
    },
    /// Org scope field is `Optional`; every row must carry a definite org.
    OptionalOrgScope {
        /// Optional org field.
        field_id: FieldId,
    },
}

impl fmt::Display for DefinitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName => f.write_str("invalid projection name"),
            Self::EmptyProjectedFields => f.write_str("projected field list is empty"),
            Self::UnknownEntity { name } => write!(f, "unknown entity {name}"),
            Self::UnknownField { field_id } => write!(f, "unknown field {}", field_id.get()),
            Self::DuplicateProjectedField { field_id } => {
                write!(f, "duplicate projected field {}", field_id.get())
            }
            Self::UnsupportedColumnType { field_id, tag } => {
                write!(
                    f,
                    "unsupported column type {:?} for field {}",
                    tag,
                    field_id.get()
                )
            }
            Self::OrgFieldNotOnEntity { field_id } => {
                write!(f, "org scope field {} is not on entity", field_id.get())
            }
            Self::OptionalOrgScope { field_id } => {
                write!(f, "org scope field {} must not be optional", field_id.get())
            }
        }
    }
}

impl std::error::Error for DefinitionError {}

fn is_supported_column_type(value_type: &ValueType) -> bool {
    match value_type.tag() {
        ValueTypeTag::Bool
        | ValueTypeTag::I64
        | ValueTypeTag::U64
        | ValueTypeTag::String
        | ValueTypeTag::Uuid
        | ValueTypeTag::Enum
        | ValueTypeTag::Timestamp
        | ValueTypeTag::Date
        | ValueTypeTag::Decimal
        | ValueTypeTag::Money
        | ValueTypeTag::Bytes => true,
        ValueTypeTag::Optional => value_type
            .optional_inner()
            .is_some_and(is_supported_column_type),
        ValueTypeTag::List | ValueTypeTag::Record => false,
    }
}

fn compute_fingerprint(
    entity_type_id: EntityTypeId,
    projected_fields: &[FieldId],
    projected_types: &[ValueType],
    org_scope_field: FieldId,
    org_type: &ValueType,
) -> DefinitionFingerprint {
    let mut payload = Vec::new();
    payload.extend_from_slice(&LAYOUT_VERSION.to_be_bytes());
    payload.extend_from_slice(&entity_type_id.to_be_bytes());
    payload.extend_from_slice(&(projected_fields.len() as u32).to_be_bytes());
    for (field_id, value_type) in projected_fields.iter().zip(projected_types.iter()) {
        payload.extend_from_slice(&field_id.get().to_be_bytes());
        encode_type_fingerprint(value_type, &mut payload);
    }
    payload.extend_from_slice(&org_scope_field.get().to_be_bytes());
    encode_type_fingerprint(org_type, &mut payload);
    // Domain-separated SHA-256; CanonicalValue domain is acceptable for a
    // private engine fingerprint (not a public wire identity).
    let digest = hash(HashDomain::CanonicalValue, &payload);
    DefinitionFingerprint::from_bytes(*digest.as_bytes())
}

fn encode_type_fingerprint(value_type: &ValueType, out: &mut Vec<u8>) {
    out.push(value_type.tag() as u8);
    match value_type.tag() {
        ValueTypeTag::Optional => {
            if let Some(inner) = value_type.optional_inner() {
                encode_type_fingerprint(inner, out);
            }
        }
        ValueTypeTag::String | ValueTypeTag::Bytes => {
            let bound = value_type.byte_bound().unwrap_or(0) as u32;
            out.extend_from_slice(&bound.to_be_bytes());
        }
        ValueTypeTag::Decimal => {
            if let Some(spec) = value_type.decimal_spec() {
                out.push(spec.precision());
                out.push(spec.scale());
            }
        }
        ValueTypeTag::Money => {
            if let Some(currency) = value_type.currency() {
                out.extend_from_slice(currency.as_bytes());
            }
        }
        ValueTypeTag::Enum => {
            if let Some(type_id) = value_type.enum_type_id() {
                out.extend_from_slice(&type_id.get().to_be_bytes());
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_contract_compiler::compile_contract_source;

    const CONTRACT: &str = r#"
contract ColumnarDef version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: u64)
    field status: u64
    field title: string<200>
    field nested: optional<list<i64,4>>
    field alt_org: optional<uuid>
  }

  event TicketCreated {
    organization_id: uuid
    ticket_id: u64
  }

  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }

  command CreateTicket {
    input idempotency_key: string<128>
    input organization_id: uuid
    input ticket_id: u64
    input status: u64
    input title: string<200>

    idempotency_key idempotency_key
    create Ticket(organization_id, ticket_id) as ticket
      else AlreadyExists { ticket_id: ticket_id }

    set ticket.status = status
    set ticket.title = title

    emit TicketCreated { organization_id: organization_id, ticket_id: ticket_id }
    return Created { ticket: ticket }
  }
}
"#;

    fn field_id(bundle: &ContractBundle, entity: &str, name: &str) -> FieldId {
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|e| e.name() == entity)
            .expect("entity");
        entity
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == name)
            .map(|f| f.id())
            .expect("field")
    }

    #[test]
    fn registers_supported_scalar_projection() {
        let bundle = compile_contract_source(CONTRACT).expect("compile");
        let org = field_id(&bundle, "Ticket", "organization_id");
        let status = field_id(&bundle, "Ticket", "status");
        let title = field_id(&bundle, "Ticket", "title");
        let registered = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "board".into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![status, title],
                org_scope_field: org,
            },
            &bundle,
        )
        .expect("register");
        assert_eq!(registered.name(), "board");
        assert_eq!(registered.projected_fields(), &[status, title]);
        let again = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "board".into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![status, title],
                org_scope_field: org,
            },
            &bundle,
        )
        .expect("register again");
        assert_eq!(registered.fingerprint(), again.fingerprint());
    }

    #[test]
    fn rejects_unknown_entity() {
        let bundle = compile_contract_source(CONTRACT).expect("compile");
        let org = field_id(&bundle, "Ticket", "organization_id");
        let status = field_id(&bundle, "Ticket", "status");
        let err = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "board".into(),
                entity_name: "Missing".into(),
                projected_fields: vec![status],
                org_scope_field: org,
            },
            &bundle,
        )
        .expect_err("unknown entity");
        assert!(matches!(err, DefinitionError::UnknownEntity { .. }));
    }

    #[test]
    fn rejects_unknown_field() {
        let bundle = compile_contract_source(CONTRACT).expect("compile");
        let org = field_id(&bundle, "Ticket", "organization_id");
        let bogus = FieldId::new(9_999).expect("id");
        let err = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "board".into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![bogus],
                org_scope_field: org,
            },
            &bundle,
        )
        .expect_err("unknown field");
        assert!(matches!(err, DefinitionError::UnknownField { .. }));
    }

    #[test]
    fn rejects_list_column_type() {
        let bundle = compile_contract_source(CONTRACT).expect("compile");
        let org = field_id(&bundle, "Ticket", "organization_id");
        let nested = field_id(&bundle, "Ticket", "nested");
        let err = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "board".into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![nested],
                org_scope_field: org,
            },
            &bundle,
        )
        .expect_err("list unsupported");
        assert!(matches!(err, DefinitionError::UnsupportedColumnType { .. }));
    }

    #[test]
    fn rejects_optional_org_scope_field() {
        let bundle = compile_contract_source(CONTRACT).expect("compile");
        let status = field_id(&bundle, "Ticket", "status");
        let alt_org = field_id(&bundle, "Ticket", "alt_org");
        let err = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "board".into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![status],
                org_scope_field: alt_org,
            },
            &bundle,
        )
        .expect_err("optional org scope rejected");
        assert!(matches!(err, DefinitionError::OptionalOrgScope { .. }));
    }

    #[test]
    fn rejects_org_field_not_on_entity() {
        let bundle = compile_contract_source(CONTRACT).expect("compile");
        let status = field_id(&bundle, "Ticket", "status");
        let bogus = FieldId::new(9_999).expect("id");
        let err = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "board".into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![status],
                org_scope_field: bogus,
            },
            &bundle,
        )
        .expect_err("org missing");
        assert!(matches!(err, DefinitionError::OrgFieldNotOnEntity { .. }));
    }
}
