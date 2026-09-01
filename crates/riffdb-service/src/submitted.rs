//! Checked, pre-schema command values accepted by every application transport.

use std::fmt;

use riffdb_types::{
    CanonicalBytes, CanonicalRecord, CanonicalString, CanonicalValue, CurrencyCode, Date, Decimal,
    DecimalSpec, EnumTypeId, EnumVariantId, FieldId, MAX_ATOMIC_COMMAND_REQUEST_BYTES_V2,
    MAX_CANONICAL_DOCUMENT_BYTES, MAX_DECIMAL_PRECISION, MAX_LIST_ENTRIES, MAX_NESTING_DEPTH,
    MAX_RECORD_FIELDS, Money, Timestamp,
};

use crate::{ServiceDtoError, SourceName};

const LENGTH_BYTES: usize = 4;
const COLLECTION_COUNT_BYTES: usize = 4;
const OPTION_BYTES: usize = 1;
const TAG_BYTES: usize = 1;

/// A decimal submitted before the selected command schema confirms its type.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SubmittedDecimal {
    coefficient: i128,
    scale: u8,
    precision: Option<u8>,
}

impl SubmittedDecimal {
    /// Creates a structurally valid legacy decimal without inventing precision.
    pub const fn new(coefficient: i128, scale: u8) -> Result<Self, ServiceDtoError> {
        if scale > MAX_DECIMAL_PRECISION {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            coefficient,
            scale,
            precision: None,
        })
    }

    /// Creates a decimal with optional caller-supplied precision evidence.
    pub fn with_precision(
        coefficient: i128,
        scale: u8,
        precision: Option<u8>,
    ) -> Result<Self, ServiceDtoError> {
        let value = Self::new(coefficient, scale)?;
        let Some(precision) = precision else {
            return Ok(value);
        };
        let spec = DecimalSpec::new(precision, scale).map_err(|_| ServiceDtoError::OutOfRange)?;
        Decimal::new(spec, coefficient).map_err(|_| ServiceDtoError::OutOfRange)?;
        Ok(Self {
            precision: Some(precision),
            ..value
        })
    }

    /// Decodes a minimal big-endian two's-complement coefficient without schema context.
    ///
    /// This is the transport-neutral bridge for protocols that preserve the signed
    /// coefficient as bytes but cannot select the compiled decimal precision.
    pub fn from_minimal_twos_complement(
        coefficient: &[u8],
        scale: u8,
    ) -> Result<Self, ServiceDtoError> {
        Self::from_minimal_twos_complement_with_precision(coefficient, scale, None)
    }

    /// Decodes a minimal coefficient and retains optional precision evidence.
    pub fn from_minimal_twos_complement_with_precision(
        coefficient: &[u8],
        scale: u8,
        precision: Option<u8>,
    ) -> Result<Self, ServiceDtoError> {
        if coefficient.is_empty() {
            return Err(ServiceDtoError::Empty);
        }
        if coefficient.len() > size_of::<i128>() {
            return Err(ServiceDtoError::TooLong);
        }
        if coefficient.len() > 1
            && ((coefficient[0] == 0 && coefficient[1] & 0x80 == 0)
                || (coefficient[0] == 0xff && coefficient[1] & 0x80 != 0))
        {
            return Err(ServiceDtoError::InvalidShape);
        }

        let fill = if coefficient[0] & 0x80 == 0 { 0 } else { 0xff };
        let mut decoded = [fill; size_of::<i128>()];
        let offset = decoded.len() - coefficient.len();
        decoded[offset..].copy_from_slice(coefficient);
        Self::with_precision(i128::from_be_bytes(decoded), scale, precision)
    }

    /// Returns the signed fixed-scale coefficient.
    #[must_use]
    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }

    /// Returns the submitted fractional scale.
    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Returns optional caller-supplied precision evidence.
    #[must_use]
    pub const fn precision(self) -> Option<u8> {
        self.precision
    }
}

impl fmt::Debug for SubmittedDecimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SubmittedDecimal([REDACTED])")
    }
}

/// A money value submitted before the selected command schema fixes currency.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SubmittedMoney {
    currency: CurrencyCode,
    amount: SubmittedDecimal,
}

impl SubmittedMoney {
    /// Preserves the caller's currency and decimal representation structurally.
    #[must_use]
    pub const fn new(currency: CurrencyCode, amount: SubmittedDecimal) -> Self {
        Self { currency, amount }
    }

    /// Returns the submitted currency.
    #[must_use]
    pub const fn currency(self) -> CurrencyCode {
        self.currency
    }

    /// Returns the submitted coefficient and scale.
    #[must_use]
    pub const fn amount(self) -> SubmittedDecimal {
        self.amount
    }
}

impl fmt::Debug for SubmittedMoney {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SubmittedMoney([REDACTED])")
    }
}

/// An enum identity submitted before membership and display-name resolution.
#[derive(Clone, Eq, PartialEq)]
pub struct SubmittedEnum {
    type_id: Option<EnumTypeId>,
    variant_id: Option<EnumVariantId>,
    name: Option<SourceName>,
}

impl SubmittedEnum {
    /// Creates a structurally checked enum identity.
    #[must_use]
    pub const fn new(
        type_id: EnumTypeId,
        variant_id: EnumVariantId,
        name: Option<SourceName>,
    ) -> Self {
        Self {
            type_id: Some(type_id),
            variant_id: Some(variant_id),
            name,
        }
    }

    /// Creates the exact name-only form resolved under the selected schema.
    #[must_use]
    pub const fn name_only(name: SourceName) -> Self {
        Self {
            type_id: None,
            variant_id: None,
            name: Some(name),
        }
    }

    /// Returns the submitted enum type identity.
    #[must_use]
    pub const fn type_id(&self) -> Option<EnumTypeId> {
        self.type_id
    }

    /// Returns the submitted enum variant identity.
    #[must_use]
    pub const fn variant_id(&self) -> Option<EnumVariantId> {
        self.variant_id
    }

    /// Borrows the optional submitted display name.
    #[must_use]
    pub const fn name(&self) -> Option<&SourceName> {
        self.name.as_ref()
    }
}

impl fmt::Debug for SubmittedEnum {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SubmittedEnum([REDACTED])")
    }
}

/// Stable, source, or redundant field identity retained until schema resolution.
#[derive(Clone, Eq, PartialEq)]
pub enum SubmittedFieldIdentity {
    /// Stable field identity only.
    Id(FieldId),
    /// Exact source field name only.
    Name(SourceName),
    /// Redundant identity whose components must resolve to the same field.
    IdAndName {
        /// Stable field identity.
        id: FieldId,
        /// Exact source field name.
        name: SourceName,
    },
}

impl SubmittedFieldIdentity {
    /// Returns the stable field identity when supplied.
    #[must_use]
    pub const fn field_id(&self) -> Option<FieldId> {
        match self {
            Self::Id(id) | Self::IdAndName { id, .. } => Some(*id),
            Self::Name(_) => None,
        }
    }

    /// Borrows the source field name when supplied.
    #[must_use]
    pub const fn name(&self) -> Option<&SourceName> {
        match self {
            Self::Name(name) | Self::IdAndName { name, .. } => Some(name),
            Self::Id(_) => None,
        }
    }
}

impl fmt::Debug for SubmittedFieldIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SubmittedFieldIdentity([REDACTED])")
    }
}

/// One pre-schema record field.
#[derive(Clone, Eq, PartialEq)]
pub struct SubmittedField {
    identity: SubmittedFieldIdentity,
    value: SubmittedValue,
}

impl SubmittedField {
    /// Creates one field without resolving its identity against a schema.
    #[must_use]
    pub const fn new(identity: SubmittedFieldIdentity, value: SubmittedValue) -> Self {
        Self { identity, value }
    }

    /// Borrows the structurally meaningful field identity.
    #[must_use]
    pub const fn identity(&self) -> &SubmittedFieldIdentity {
        &self.identity
    }

    /// Borrows the submitted value.
    #[must_use]
    pub const fn value(&self) -> &SubmittedValue {
        &self.value
    }
}

impl fmt::Debug for SubmittedField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SubmittedField([REDACTED])")
    }
}

/// A bounded ordered pre-schema list.
#[derive(Clone, Eq, PartialEq)]
pub struct SubmittedList(Vec<SubmittedValue>);

impl SubmittedList {
    /// Creates a list while enforcing process collection, recursion, and document limits.
    pub fn new(values: Vec<SubmittedValue>) -> Result<Self, ServiceDtoError> {
        if values.len() > MAX_LIST_ENTRIES {
            return Err(ServiceDtoError::TooManyItems);
        }
        ensure_child_depth(values.iter())?;
        let list = Self(values);
        ensure_value_document_bound(list.structural_size()?)?;
        Ok(list)
    }

    /// Borrows values in submitted order.
    #[must_use]
    pub fn values(&self) -> &[SubmittedValue] {
        &self.0
    }

    /// Returns the submitted item count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the list is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn structural_size(&self) -> Result<usize, ServiceDtoError> {
        let mut size = COLLECTION_COUNT_BYTES;
        for value in &self.0 {
            checked_add(&mut size, value.structural_size()?)?;
        }
        Ok(size)
    }
}

impl fmt::Debug for SubmittedList {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubmittedList")
            .field("length", &self.len())
            .finish()
    }
}

/// A bounded pre-schema record that preserves field identity representations.
#[derive(Clone, Eq, PartialEq)]
pub struct SubmittedRecord(Vec<SubmittedField>);

impl SubmittedRecord {
    /// Creates a record without sorting or resolving names and IDs.
    pub fn new(fields: Vec<SubmittedField>) -> Result<Self, ServiceDtoError> {
        Self::new_with_bound(fields, MAX_CANONICAL_DOCUMENT_BYTES)
    }

    /// Creates the complete root record of one atomic command invocation.
    ///
    /// Every child value has already passed the unchanged individual-value
    /// bound. This constructor alone admits the wider command-input envelope.
    pub fn new_command_input(fields: Vec<SubmittedField>) -> Result<Self, ServiceDtoError> {
        Self::new_with_bound(fields, MAX_ATOMIC_COMMAND_REQUEST_BYTES_V2)
    }

    fn new_with_bound(
        fields: Vec<SubmittedField>,
        maximum_bytes: usize,
    ) -> Result<Self, ServiceDtoError> {
        if fields.len() > MAX_RECORD_FIELDS {
            return Err(ServiceDtoError::TooManyItems);
        }
        ensure_child_depth(fields.iter().map(SubmittedField::value))?;
        let record = Self(fields);
        ensure_document_bound(record.structural_size()?, maximum_bytes)?;
        Ok(record)
    }

    /// Borrows fields in submitted order.
    #[must_use]
    pub fn fields(&self) -> &[SubmittedField] {
        &self.0
    }

    /// Returns the submitted field count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the record is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn structural_size(&self) -> Result<usize, ServiceDtoError> {
        let mut size = COLLECTION_COUNT_BYTES;
        for field in &self.0 {
            checked_add(&mut size, field.identity.structural_size()?)?;
            checked_add(&mut size, field.value.structural_size()?)?;
        }
        Ok(size)
    }
}

impl TryFrom<CanonicalRecord> for SubmittedRecord {
    type Error = ServiceDtoError;

    fn try_from(record: CanonicalRecord) -> Result<Self, Self::Error> {
        let fields = record
            .fields()
            .iter()
            .map(|(field_id, value)| {
                Ok(SubmittedField::new(
                    SubmittedFieldIdentity::Id(*field_id),
                    SubmittedValue::try_from(value.clone())?,
                ))
            })
            .collect::<Result<Vec<_>, ServiceDtoError>>()?;
        Self::new(fields)
    }
}

impl fmt::Debug for SubmittedRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubmittedRecord")
            .field("field_count", &self.len())
            .finish()
    }
}

/// The closed, bounded value algebra before compiled-schema materialization.
#[derive(Clone, Eq, PartialEq)]
pub enum SubmittedValue {
    /// Explicit optional absence.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// Decimal coefficient and scale without caller-selected precision.
    Decimal(SubmittedDecimal),
    /// Currency-qualified submitted decimal.
    Money(SubmittedMoney),
    /// Exact bounded UTF-8 text.
    String(CanonicalString),
    /// Exact bounded opaque bytes.
    Bytes(CanonicalBytes),
    /// Caller-supplied UTC timestamp.
    Timestamp(Timestamp),
    /// Calendar date.
    Date(Date),
    /// UUID network-order bytes.
    Uuid([u8; 16]),
    /// Stable enum identity and optional source display name.
    Enum(SubmittedEnum),
    /// Bounded ordered values.
    List(SubmittedList),
    /// Bounded fields retaining submitted identities.
    Record(SubmittedRecord),
    /// A fixed-dimension f32 vector for nearest-neighbor search.
    Vector(riffdb_types::CanonicalVector),
}

impl SubmittedValue {
    /// Constructs bounded exact UTF-8 text.
    pub fn string(value: impl Into<String>) -> Result<Self, ServiceDtoError> {
        let value = CanonicalString::new(value)
            .map(Self::String)
            .map_err(|_| ServiceDtoError::TooLong)?;
        ensure_value_document_bound(value.structural_size()?)?;
        Ok(value)
    }

    /// Constructs bounded exact bytes.
    pub fn bytes(value: impl Into<Vec<u8>>) -> Result<Self, ServiceDtoError> {
        let value = CanonicalBytes::new(value)
            .map(Self::Bytes)
            .map_err(|_| ServiceDtoError::TooLong)?;
        ensure_value_document_bound(value.structural_size()?)?;
        Ok(value)
    }

    /// Constructs a bounded ordered list.
    pub fn list(values: Vec<Self>) -> Result<Self, ServiceDtoError> {
        SubmittedList::new(values).map(Self::List)
    }

    /// Constructs a bounded pre-schema record.
    pub fn record(fields: Vec<SubmittedField>) -> Result<Self, ServiceDtoError> {
        SubmittedRecord::new(fields).map(Self::Record)
    }

    fn nesting_depth(&self) -> usize {
        match self {
            Self::List(list) => list
                .values()
                .iter()
                .map(Self::nesting_depth)
                .max()
                .map_or(0, |depth| depth + 1),
            Self::Record(record) => record
                .fields()
                .iter()
                .map(|field| field.value.nesting_depth())
                .max()
                .map_or(0, |depth| depth + 1),
            _ => 0,
        }
    }

    pub(crate) fn structural_size(&self) -> Result<usize, ServiceDtoError> {
        let payload = match self {
            Self::Null => 0,
            Self::Bool(_) => 1,
            Self::I64(_) | Self::U64(_) => 8,
            Self::Decimal(value) => 18 + usize::from(value.precision().is_some()),
            Self::Money(value) => 21 + usize::from(value.amount().precision().is_some()),
            Self::String(value) => framed_size(value.len())?,
            Self::Bytes(value) => framed_size(value.len())?,
            Self::Timestamp(_) => 12,
            Self::Date(_) => 4,
            Self::Uuid(_) => 16,
            Self::Enum(value) => {
                let mut size = 2 * OPTION_BYTES;
                if value.type_id().is_some() {
                    checked_add(&mut size, 4)?;
                }
                if value.variant_id().is_some() {
                    checked_add(&mut size, 4)?;
                }
                checked_add(&mut size, OPTION_BYTES)?;
                if let Some(name) = value.name() {
                    checked_add(&mut size, framed_size(name.as_str().len())?)?;
                }
                size
            }
            Self::List(list) => list.structural_size()?,
            Self::Record(record) => record.structural_size()?,
            Self::Vector(vector) => 4 + vector.byte_size(),
        };
        payload
            .checked_add(TAG_BYTES)
            .ok_or(ServiceDtoError::TooLong)
    }
}

impl TryFrom<CanonicalValue> for SubmittedValue {
    type Error = ServiceDtoError;

    fn try_from(value: CanonicalValue) -> Result<Self, Self::Error> {
        let submitted = match value {
            CanonicalValue::Null => Self::Null,
            CanonicalValue::Bool(value) => Self::Bool(value),
            CanonicalValue::I64(value) => Self::I64(value),
            CanonicalValue::U64(value) => Self::U64(value),
            CanonicalValue::Decimal(value) => Self::Decimal(SubmittedDecimal {
                coefficient: value.coefficient(),
                scale: value.spec().scale(),
                precision: Some(value.spec().precision()),
            }),
            CanonicalValue::Money(value) => Self::Money(submitted_money_from_canonical(value)),
            CanonicalValue::String(value) => Self::String(value),
            CanonicalValue::Bytes(value) => Self::Bytes(value),
            CanonicalValue::Timestamp(value) => Self::Timestamp(value),
            CanonicalValue::Date(value) => Self::Date(value),
            CanonicalValue::Uuid(value) => Self::Uuid(value),
            CanonicalValue::Enum {
                type_id,
                variant_id,
            } => Self::Enum(SubmittedEnum::new(type_id, variant_id, None)),
            CanonicalValue::List(values) => Self::List(SubmittedList::new(
                values
                    .values()
                    .iter()
                    .cloned()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>, ServiceDtoError>>()?,
            )?),
            CanonicalValue::Record(record) => Self::Record(SubmittedRecord::try_from(record)?),
            CanonicalValue::Vector(vector) => Self::Vector(vector),
        };
        ensure_value_document_bound(submitted.structural_size()?)?;
        Ok(submitted)
    }
}

impl fmt::Debug for SubmittedValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("SubmittedValue::Null"),
            Self::List(value) => formatter
                .debug_struct("SubmittedValue::List")
                .field("length", &value.len())
                .finish(),
            Self::Record(value) => formatter
                .debug_struct("SubmittedValue::Record")
                .field("field_count", &value.len())
                .finish(),
            _ => formatter.write_str("SubmittedValue([REDACTED])"),
        }
    }
}

impl SubmittedFieldIdentity {
    fn structural_size(&self) -> Result<usize, ServiceDtoError> {
        let mut size = TAG_BYTES;
        if self.field_id().is_some() {
            checked_add(&mut size, 4)?;
        }
        if let Some(name) = self.name() {
            checked_add(&mut size, framed_size(name.as_str().len())?)?;
        }
        Ok(size)
    }
}

fn submitted_money_from_canonical(value: Money) -> SubmittedMoney {
    SubmittedMoney::new(
        value.currency(),
        SubmittedDecimal {
            coefficient: value.amount().coefficient(),
            scale: value.amount().spec().scale(),
            precision: Some(value.amount().spec().precision()),
        },
    )
}

fn ensure_child_depth<'a>(
    children: impl IntoIterator<Item = &'a SubmittedValue>,
) -> Result<(), ServiceDtoError> {
    let depth = children
        .into_iter()
        .map(SubmittedValue::nesting_depth)
        .max()
        .map_or(0, |depth| depth + 1);
    if depth > MAX_NESTING_DEPTH {
        return Err(ServiceDtoError::OutOfRange);
    }
    Ok(())
}

fn ensure_value_document_bound(size: usize) -> Result<(), ServiceDtoError> {
    ensure_document_bound(size, MAX_CANONICAL_DOCUMENT_BYTES)
}

fn ensure_document_bound(size: usize, maximum: usize) -> Result<(), ServiceDtoError> {
    if size > maximum {
        Err(ServiceDtoError::TooLong)
    } else {
        Ok(())
    }
}

fn checked_add(total: &mut usize, value: usize) -> Result<(), ServiceDtoError> {
    *total = total.checked_add(value).ok_or(ServiceDtoError::TooLong)?;
    Ok(())
}

fn framed_size(payload: usize) -> Result<usize, ServiceDtoError> {
    LENGTH_BYTES
        .checked_add(payload)
        .ok_or(ServiceDtoError::TooLong)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{Decimal, DecimalSpec};

    #[test]
    fn debug_output_redacts_submitted_business_values_and_names() {
        let field = SubmittedField::new(
            SubmittedFieldIdentity::Name(SourceName::new("secret_field").expect("source name")),
            SubmittedValue::string("secret-value").expect("bounded string"),
        );
        let record = SubmittedRecord::new(vec![field]).expect("bounded record");

        let rendered = format!("{record:?}");
        assert!(!rendered.contains("secret_field"));
        assert!(!rendered.contains("secret-value"));
    }

    #[test]
    fn checked_canonical_conversion_preserves_scale_and_field_ids() {
        let spec = DecimalSpec::new(28, 2).expect("decimal spec");
        let record = CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::Decimal(Decimal::new(spec, 1234).expect("decimal")),
        )])
        .expect("canonical record");

        let submitted = SubmittedRecord::try_from(record).expect("bounded submitted record");
        let field = &submitted.fields()[0];
        assert_eq!(field.identity().field_id(), Some(FieldId::first()));
        let SubmittedValue::Decimal(decimal) = field.value() else {
            panic!("decimal remains decimal");
        };
        assert_eq!(decimal.coefficient(), 1234);
        assert_eq!(decimal.scale(), 2);
    }

    #[test]
    fn submitted_decimal_rejects_scale_outside_the_value_algebra() {
        assert_eq!(
            SubmittedDecimal::new(1, MAX_DECIMAL_PRECISION + 1),
            Err(ServiceDtoError::OutOfRange)
        );
    }

    #[test]
    fn submitted_decimal_decodes_minimal_twos_complement_without_a_precision() {
        for (encoded, expected) in [
            (vec![0x00], 0),
            (vec![0x7f], 127),
            (vec![0x00, 0x80], 128),
            (vec![0x80], -128),
            (vec![0xff, 0x7f], -129),
            (i128::MIN.to_be_bytes().to_vec(), i128::MIN),
            (i128::MAX.to_be_bytes().to_vec(), i128::MAX),
        ] {
            let value = SubmittedDecimal::from_minimal_twos_complement(&encoded, 2)
                .expect("minimal coefficient");
            assert_eq!(value.coefficient(), expected);
            assert_eq!(value.scale(), 2);
        }

        assert_eq!(
            SubmittedDecimal::from_minimal_twos_complement(&[], 0),
            Err(ServiceDtoError::Empty)
        );
        assert_eq!(
            SubmittedDecimal::from_minimal_twos_complement(&[0; 17], 0),
            Err(ServiceDtoError::TooLong)
        );
        for nonminimal in [&[0x00, 0x7f][..], &[0xff, 0x80][..]] {
            assert_eq!(
                SubmittedDecimal::from_minimal_twos_complement(nonminimal, 0),
                Err(ServiceDtoError::InvalidShape)
            );
        }
    }

    #[test]
    fn submitted_scalar_constructors_retain_the_individual_value_bound() {
        let exact_payload = MAX_CANONICAL_DOCUMENT_BYTES - TAG_BYTES - LENGTH_BYTES;
        assert!(SubmittedValue::string("x".repeat(exact_payload)).is_ok());
        assert_eq!(
            SubmittedValue::string("x".repeat(exact_payload + 1)),
            Err(ServiceDtoError::TooLong)
        );
        assert!(SubmittedValue::bytes(vec![0; exact_payload]).is_ok());
        assert_eq!(
            SubmittedValue::bytes(vec![0; exact_payload + 1]),
            Err(ServiceDtoError::TooLong)
        );
    }

    #[test]
    fn only_the_complete_command_input_may_cross_the_value_document_bound() {
        let fields = || {
            [1_u32, 2]
                .into_iter()
                .map(|id| {
                    SubmittedField::new(
                        SubmittedFieldIdentity::Id(FieldId::new(id).expect("field ID")),
                        SubmittedValue::bytes(vec![0xa5; 600_000]).expect("bounded child value"),
                    )
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(
            SubmittedRecord::new(fields()),
            Err(ServiceDtoError::TooLong)
        );
        assert!(SubmittedRecord::new_command_input(fields()).is_ok());
        assert_eq!(
            SubmittedValue::record(fields()),
            Err(ServiceDtoError::TooLong)
        );
    }

    #[test]
    fn submitted_collections_enforce_exact_count_and_depth_boundaries() {
        assert!(SubmittedList::new(vec![SubmittedValue::Null; MAX_LIST_ENTRIES]).is_ok());
        assert_eq!(
            SubmittedList::new(vec![SubmittedValue::Null; MAX_LIST_ENTRIES + 1]),
            Err(ServiceDtoError::TooManyItems)
        );

        let field = SubmittedField::new(
            SubmittedFieldIdentity::Id(FieldId::first()),
            SubmittedValue::Null,
        );
        assert!(SubmittedRecord::new(vec![field.clone(); MAX_RECORD_FIELDS]).is_ok());
        assert_eq!(
            SubmittedRecord::new(vec![field; MAX_RECORD_FIELDS + 1]),
            Err(ServiceDtoError::TooManyItems)
        );

        let mut nested = SubmittedValue::Null;
        for _ in 0..MAX_NESTING_DEPTH {
            nested = SubmittedValue::list(vec![nested]).expect("depth at the limit");
        }
        assert_eq!(
            SubmittedValue::list(vec![nested]),
            Err(ServiceDtoError::OutOfRange)
        );
    }
}
