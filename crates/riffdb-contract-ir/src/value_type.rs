//! Closed transactional type IR.

use riffdb_types::{
    CanonicalValue, CommandId, CurrencyCode, DecimalSpec, EntityTypeId, EnumTypeId, EventTypeId,
    FieldId, MAX_BYTES_VALUE_BYTES, MAX_DECIMAL_PRECISION, MAX_LIST_ENTRIES, MAX_NESTING_DEPTH,
    MAX_STRING_BYTES, OutcomeId, ProjectionId,
};

use crate::{IrValidationError, checked_len};

/// Immutable executable-IR type tags.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ValueTypeTag {
    /// Boolean.
    Bool = crate::format_registry::value_type::BOOL,
    /// Signed 64-bit integer.
    I64 = crate::format_registry::value_type::I64,
    /// Unsigned 64-bit integer.
    U64 = crate::format_registry::value_type::U64,
    /// Fixed precision and scale decimal.
    Decimal = crate::format_registry::value_type::DECIMAL,
    /// Fixed-currency money.
    Money = crate::format_registry::value_type::MONEY,
    /// Bounded exact UTF-8 text.
    String = crate::format_registry::value_type::STRING,
    /// Bounded bytes.
    Bytes = crate::format_registry::value_type::BYTES,
    /// UTC timestamp.
    Timestamp = crate::format_registry::value_type::TIMESTAMP,
    /// Calendar date.
    Date = crate::format_registry::value_type::DATE,
    /// UUID network bytes.
    Uuid = crate::format_registry::value_type::UUID,
    /// Declared enum reference.
    Enum = crate::format_registry::value_type::ENUM,
    /// Nullable inner value.
    Optional = crate::format_registry::value_type::OPTIONAL,
    /// Bounded list.
    List = crate::format_registry::value_type::LIST,
    /// Complete typed record reference.
    Record = crate::format_registry::value_type::RECORD,
    /// Fixed-dimension f32 vector for nearest-neighbor search.
    Vector = crate::format_registry::value_type::VECTOR,
}

/// The stable owner of one record schema.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RecordTypeRef {
    /// Entity record.
    Entity(EntityTypeId),
    /// Durable-event payload.
    Event(EventTypeId),
    /// Command input record.
    CommandInput(CommandId),
    /// One declared command outcome payload.
    CommandOutcome {
        /// Owning command.
        command_id: CommandId,
        /// Declared outcome.
        outcome_id: OutcomeId,
    },
    /// Projection result row.
    ProjectionResult(ProjectionId),
}

impl RecordTypeRef {
    pub(crate) fn tag(&self) -> u8 {
        match self {
            Self::Entity(_) => crate::format_registry::record_reference::ENTITY,
            Self::Event(_) => crate::format_registry::record_reference::EVENT,
            Self::CommandInput(_) => crate::format_registry::record_reference::COMMAND_INPUT,
            Self::CommandOutcome { .. } => {
                crate::format_registry::record_reference::COMMAND_OUTCOME
            }
            Self::ProjectionResult(_) => {
                crate::format_registry::record_reference::PROJECTION_RESULT
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ValueTypeKind {
    Bool,
    I64,
    U64,
    Decimal(DecimalSpec),
    Money(CurrencyCode),
    String(usize),
    Bytes(usize),
    Timestamp,
    Date,
    Uuid,
    Enum(EnumTypeId),
    Optional(Box<ValueType>),
    List {
        element: Box<ValueType>,
        maximum: usize,
    },
    Record(RecordTypeRef),
    Vector(riffdb_types::VectorDimension),
}

/// One fully validated transactional value type.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ValueType(ValueTypeKind);

impl ValueType {
    /// Whether a checked expression may be injected into this destination type.
    ///
    /// Grammar v1 permits a nonoptional `T` expression in an `optional<T>`
    /// construction context; all other conversions remain explicit and exact.
    #[must_use]
    pub fn accepts_contextual(&self, actual: &Self) -> bool {
        self == actual || self.optional_inner().is_some_and(|inner| inner == actual)
    }

    /// Boolean type.
    #[must_use]
    pub const fn bool() -> Self {
        Self(ValueTypeKind::Bool)
    }

    /// Signed 64-bit integer type.
    #[must_use]
    pub const fn i64() -> Self {
        Self(ValueTypeKind::I64)
    }

    /// Unsigned 64-bit integer type.
    #[must_use]
    pub const fn u64() -> Self {
        Self(ValueTypeKind::U64)
    }

    /// Fixed decimal type.
    #[must_use]
    pub const fn decimal(spec: DecimalSpec) -> Self {
        Self(ValueTypeKind::Decimal(spec))
    }

    /// Fixed-currency money type. Money v1 is always decimal `<38,2>`.
    #[must_use]
    pub const fn money(currency: CurrencyCode) -> Self {
        Self(ValueTypeKind::Money(currency))
    }

    /// Bounded UTF-8 type.
    pub fn string(maximum_bytes: usize) -> Result<Self, IrValidationError> {
        if maximum_bytes == 0 {
            return Err(IrValidationError::Empty {
                kind: "string bound",
            });
        }
        checked_len("string bound", maximum_bytes, MAX_STRING_BYTES)?;
        Ok(Self(ValueTypeKind::String(maximum_bytes)))
    }

    /// Bounded bytes type.
    pub fn bytes(maximum_bytes: usize) -> Result<Self, IrValidationError> {
        if maximum_bytes == 0 {
            return Err(IrValidationError::Empty {
                kind: "bytes bound",
            });
        }
        checked_len("bytes bound", maximum_bytes, MAX_BYTES_VALUE_BYTES)?;
        Ok(Self(ValueTypeKind::Bytes(maximum_bytes)))
    }

    /// UTC timestamp type.
    #[must_use]
    pub const fn timestamp() -> Self {
        Self(ValueTypeKind::Timestamp)
    }

    /// Calendar date type.
    #[must_use]
    pub const fn date() -> Self {
        Self(ValueTypeKind::Date)
    }

    /// UUID type.
    #[must_use]
    pub const fn uuid() -> Self {
        Self(ValueTypeKind::Uuid)
    }

    /// Declared enum type.
    #[must_use]
    pub const fn enumeration(type_id: EnumTypeId) -> Self {
        Self(ValueTypeKind::Enum(type_id))
    }

    /// Nullable type. Nested optionals are not representable in v1.
    pub fn optional(inner: Self) -> Result<Self, IrValidationError> {
        if inner.is_optional() {
            return Err(IrValidationError::TypeMismatch {
                context: "nested optional",
            });
        }
        if inner.nesting_depth() >= MAX_NESTING_DEPTH {
            return Err(IrValidationError::LimitExceeded {
                kind: "type nesting",
                actual: inner.nesting_depth() + 1,
                maximum: MAX_NESTING_DEPTH,
            });
        }
        Ok(Self(ValueTypeKind::Optional(Box::new(inner))))
    }

    /// Bounded list type.
    pub fn list(element: Self, maximum: usize) -> Result<Self, IrValidationError> {
        if maximum == 0 {
            return Err(IrValidationError::Empty { kind: "list bound" });
        }
        checked_len("list bound", maximum, MAX_LIST_ENTRIES)?;
        if element.nesting_depth() >= MAX_NESTING_DEPTH {
            return Err(IrValidationError::LimitExceeded {
                kind: "type nesting",
                actual: element.nesting_depth() + 1,
                maximum: MAX_NESTING_DEPTH,
            });
        }
        Ok(Self(ValueTypeKind::List {
            element: Box::new(element),
            maximum,
        }))
    }

    /// Complete record reference type.
    #[must_use]
    pub const fn record(owner: RecordTypeRef) -> Self {
        Self(ValueTypeKind::Record(owner))
    }

    /// Fixed-dimension f32 vector type for nearest-neighbor search.
    #[must_use]
    pub const fn vector(dimension: riffdb_types::VectorDimension) -> Self {
        Self(ValueTypeKind::Vector(dimension))
    }

    /// Returns the vector dimension when this is a vector type.
    #[must_use]
    pub const fn vector_dimension(&self) -> Option<riffdb_types::VectorDimension> {
        match self.0 {
            ValueTypeKind::Vector(dim) => Some(dim),
            _ => None,
        }
    }

    /// Returns the immutable v1 type tag.
    #[must_use]
    pub const fn tag(&self) -> ValueTypeTag {
        match self.0 {
            ValueTypeKind::Bool => ValueTypeTag::Bool,
            ValueTypeKind::I64 => ValueTypeTag::I64,
            ValueTypeKind::U64 => ValueTypeTag::U64,
            ValueTypeKind::Decimal(_) => ValueTypeTag::Decimal,
            ValueTypeKind::Money(_) => ValueTypeTag::Money,
            ValueTypeKind::String(_) => ValueTypeTag::String,
            ValueTypeKind::Bytes(_) => ValueTypeTag::Bytes,
            ValueTypeKind::Timestamp => ValueTypeTag::Timestamp,
            ValueTypeKind::Date => ValueTypeTag::Date,
            ValueTypeKind::Uuid => ValueTypeTag::Uuid,
            ValueTypeKind::Enum(_) => ValueTypeTag::Enum,
            ValueTypeKind::Optional(_) => ValueTypeTag::Optional,
            ValueTypeKind::List { .. } => ValueTypeTag::List,
            ValueTypeKind::Record(_) => ValueTypeTag::Record,
            ValueTypeKind::Vector(_) => ValueTypeTag::Vector,
        }
    }

    /// Returns the decimal specification, when this is a decimal type.
    #[must_use]
    pub const fn decimal_spec(&self) -> Option<DecimalSpec> {
        match self.0 {
            ValueTypeKind::Decimal(spec) => Some(spec),
            _ => None,
        }
    }

    /// Returns the money currency, when this is a money type.
    #[must_use]
    pub const fn currency(&self) -> Option<CurrencyCode> {
        match self.0 {
            ValueTypeKind::Money(currency) => Some(currency),
            _ => None,
        }
    }

    /// Returns the string or bytes bound.
    #[must_use]
    pub const fn byte_bound(&self) -> Option<usize> {
        match self.0 {
            ValueTypeKind::String(maximum) | ValueTypeKind::Bytes(maximum) => Some(maximum),
            _ => None,
        }
    }

    /// Returns the enum type identity.
    #[must_use]
    pub const fn enum_type_id(&self) -> Option<EnumTypeId> {
        match self.0 {
            ValueTypeKind::Enum(id) => Some(id),
            _ => None,
        }
    }

    /// Returns the optional inner type.
    #[must_use]
    pub fn optional_inner(&self) -> Option<&Self> {
        match &self.0 {
            ValueTypeKind::Optional(inner) => Some(inner),
            _ => None,
        }
    }

    /// Returns the list element and maximum count.
    #[must_use]
    pub fn list_parts(&self) -> Option<(&Self, usize)> {
        match &self.0 {
            ValueTypeKind::List { element, maximum } => Some((element, *maximum)),
            _ => None,
        }
    }

    /// Returns the complete record reference.
    #[must_use]
    pub fn record_ref(&self) -> Option<&RecordTypeRef> {
        match &self.0 {
            ValueTypeKind::Record(owner) => Some(owner),
            _ => None,
        }
    }

    /// Whether canonical null is legal under this type.
    #[must_use]
    pub const fn is_optional(&self) -> bool {
        matches!(self.0, ValueTypeKind::Optional(_))
    }

    /// Whether this is a nonoptional scalar accepted as a projection group.
    #[must_use]
    pub const fn is_projection_group_scalar(&self) -> bool {
        matches!(
            self.0,
            ValueTypeKind::Bool
                | ValueTypeKind::I64
                | ValueTypeKind::U64
                | ValueTypeKind::Decimal(_)
                | ValueTypeKind::Money(_)
                | ValueTypeKind::String(_)
                | ValueTypeKind::Bytes(_)
                | ValueTypeKind::Timestamp
                | ValueTypeKind::Date
                | ValueTypeKind::Uuid
                | ValueTypeKind::Enum(_)
        )
    }

    /// Whether this is a closed v1 authoritative key component.
    #[must_use]
    pub const fn is_authoritative_key_scalar(&self) -> bool {
        self.is_projection_group_scalar()
            && !matches!(self.0, ValueTypeKind::Decimal(_) | ValueTypeKind::Money(_))
    }

    /// Whether values of this type support v1 equality operators.
    #[must_use]
    pub fn supports_equality(&self) -> bool {
        match &self.0 {
            ValueTypeKind::Bool
            | ValueTypeKind::I64
            | ValueTypeKind::U64
            | ValueTypeKind::Decimal(_)
            | ValueTypeKind::Money(_)
            | ValueTypeKind::String(_)
            | ValueTypeKind::Bytes(_)
            | ValueTypeKind::Timestamp
            | ValueTypeKind::Date
            | ValueTypeKind::Uuid
            | ValueTypeKind::Enum(_) => true,
            ValueTypeKind::Optional(inner) => inner.supports_equality(),
            ValueTypeKind::List { .. } | ValueTypeKind::Record(_) | ValueTypeKind::Vector(_) => {
                false
            }
        }
    }

    /// Returns the maximum canonical value-document length when statically known.
    pub fn maximum_canonical_bytes(&self) -> Result<Option<usize>, IrValidationError> {
        let value = match &self.0 {
            ValueTypeKind::Bool => 3,
            ValueTypeKind::I64 | ValueTypeKind::U64 => 10,
            ValueTypeKind::Decimal(_) => 20,
            ValueTypeKind::Money(_) => 23,
            ValueTypeKind::String(maximum) | ValueTypeKind::Bytes(maximum) => maximum
                .checked_add(6)
                .ok_or(IrValidationError::SizeOverflow { kind: "value type" })?,
            ValueTypeKind::Timestamp => 14,
            ValueTypeKind::Date => 6,
            ValueTypeKind::Uuid => 18,
            ValueTypeKind::Enum(_) => 10,
            ValueTypeKind::Optional(inner) => inner.maximum_canonical_bytes()?.unwrap_or(2).max(2),
            ValueTypeKind::List { element, maximum } => {
                let Some(element) = element.maximum_canonical_bytes()? else {
                    return Ok(None);
                };
                6usize
                    .checked_add(
                        element
                            .checked_mul(*maximum)
                            .ok_or(IrValidationError::SizeOverflow { kind: "list type" })?,
                    )
                    .ok_or(IrValidationError::SizeOverflow { kind: "list type" })?
            }
            ValueTypeKind::Record(_) => return Ok(None),
            ValueTypeKind::Vector(dim) => {
                // tag + 4 bytes dimension + 4 bytes per f32 component
                (dim.get() as usize)
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(6))
                    .ok_or(IrValidationError::SizeOverflow { kind: "vector type" })?
            }
        };
        Ok(Some(value))
    }

    /// Checks a canonical value against this complete static type.
    pub fn validate_value(&self, value: &CanonicalValue) -> Result<(), IrValidationError> {
        if matches!(value, CanonicalValue::Null) {
            return if self.is_optional() {
                Ok(())
            } else {
                Err(IrValidationError::TypeMismatch {
                    context: "canonical value",
                })
            };
        }
        if let ValueTypeKind::Optional(inner) = &self.0 {
            return inner.validate_value(value);
        }
        let valid = match (&self.0, value) {
            (ValueTypeKind::Bool, CanonicalValue::Bool(_))
            | (ValueTypeKind::I64, CanonicalValue::I64(_))
            | (ValueTypeKind::U64, CanonicalValue::U64(_))
            | (ValueTypeKind::Timestamp, CanonicalValue::Timestamp(_))
            | (ValueTypeKind::Date, CanonicalValue::Date(_))
            | (ValueTypeKind::Uuid, CanonicalValue::Uuid(_)) => true,
            (ValueTypeKind::Decimal(spec), CanonicalValue::Decimal(value)) => value.spec() == *spec,
            (ValueTypeKind::Money(currency), CanonicalValue::Money(value)) => {
                value.currency() == *currency
                    && value.amount().spec()
                        == DecimalSpec::new(MAX_DECIMAL_PRECISION, 2)
                            .expect("accepted fixed money spec")
            }
            (ValueTypeKind::String(maximum), CanonicalValue::String(value)) => {
                value.len() <= *maximum
            }
            (ValueTypeKind::Bytes(maximum), CanonicalValue::Bytes(value)) => {
                value.len() <= *maximum
            }
            (
                ValueTypeKind::Enum(type_id),
                CanonicalValue::Enum {
                    type_id: actual, ..
                },
            ) => type_id == actual,
            (ValueTypeKind::List { element, maximum }, CanonicalValue::List(values)) => {
                values.len() <= *maximum
                    && values
                        .values()
                        .iter()
                        .all(|value| element.validate_value(value).is_ok())
            }
            (ValueTypeKind::Record(_), CanonicalValue::Record(_)) => true,
            (ValueTypeKind::Vector(dim), CanonicalValue::Vector(vec)) => {
                vec.dimension() == dim.get()
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(IrValidationError::TypeMismatch {
                context: "canonical value",
            })
        }
    }

    fn nesting_depth(&self) -> usize {
        match &self.0 {
            ValueTypeKind::Optional(inner) => 1 + inner.nesting_depth(),
            ValueTypeKind::List { element, .. } => 1 + element.nesting_depth(),
            _ => 1,
        }
    }
}

/// One stable-ID field in a record schema.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FieldSchema {
    id: FieldId,
    name: String,
    value_type: ValueType,
}

impl FieldSchema {
    /// Creates a checked field declaration.
    pub fn new(
        id: FieldId,
        name: impl Into<String>,
        value_type: ValueType,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        crate::validate_source_name(&name, "field")?;
        Ok(Self {
            id,
            name,
            value_type,
        })
    }

    /// Stable field ID.
    #[must_use]
    pub const fn id(&self) -> FieldId {
        self.id
    }

    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Complete static type.
    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CanonicalString, Decimal, Money};

    #[test]
    fn rejects_nested_optional_and_zero_bounds() {
        assert!(ValueType::string(0).is_err());
        let optional = ValueType::optional(ValueType::i64()).expect("optional");
        assert!(ValueType::optional(optional).is_err());
    }

    #[test]
    fn contextual_optional_injection_is_one_way() {
        let scalar = ValueType::i64();
        let optional = ValueType::optional(scalar.clone()).expect("optional");
        assert!(optional.accepts_contextual(&scalar));
        assert!(optional.accepts_contextual(&optional));
        assert!(!scalar.accepts_contextual(&optional));
    }

    #[test]
    fn validates_exact_decimal_and_utf8_bounds() {
        let spec = DecimalSpec::new(4, 2).expect("spec");
        let ty = ValueType::decimal(spec);
        assert!(
            ty.validate_value(&CanonicalValue::Decimal(
                Decimal::new(spec, 123).expect("decimal")
            ))
            .is_ok()
        );
        let string = ValueType::string(2).expect("type");
        assert!(
            string
                .validate_value(&CanonicalValue::String(
                    CanonicalString::new("é").expect("value")
                ))
                .is_ok()
        );
    }

    #[test]
    fn money_type_uses_the_fixed_v1_spec() {
        let currency = CurrencyCode::new("USD").expect("currency");
        let amount = riffdb_types::Decimal::new(DecimalSpec::new(38, 2).expect("spec"), 100)
            .expect("amount");
        assert!(
            ValueType::money(currency)
                .validate_value(&CanonicalValue::Money(Money::new(currency, amount)))
                .is_ok()
        );
    }
}
