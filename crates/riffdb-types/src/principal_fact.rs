//! Canonical bounded principal facts for compiler-owned row policy.

use std::{error::Error, fmt, sync::Arc};

use crate::{CanonicalValue, decode_canonical_value, encode_canonical_value};

/// Immutable principal-fact encoding version.
pub const PRINCIPAL_FACT_SET_VERSION_V1: u32 = 1;
/// Maximum fact schemas and values carried by one capability.
pub const MAX_PRINCIPAL_FACTS_V1: usize = 32;
/// Maximum scalar members of one list-valued fact.
pub const MAX_PRINCIPAL_FACT_VALUES_V1: usize = 64;
/// Maximum canonical bytes across one complete fact set.
pub const MAX_PRINCIPAL_FACT_BYTES_V1: usize = 64 * 1024;
const MAX_PRINCIPAL_FACT_NAME_BYTES_V1: usize = 256;
const FACT_SET_MAGIC: &[u8] = b"RIFFDB-PRINCIPAL-FACTS\0";

/// Safe failure to construct a bounded principal-fact set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalFactError {
    /// A fact name is not a compiler-visible source identifier.
    InvalidName,
    /// A fact value is not one scalar or one bounded scalar set.
    InvalidValue,
    /// A count or encoded byte ceiling was exceeded.
    LimitExceeded,
    /// A fact or set member occurs more than once.
    Duplicate,
    /// Canonical encoding failed or overflowed.
    InvalidEncoding,
}

impl fmt::Display for PrincipalFactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidName => "principal fact name is invalid",
            Self::InvalidValue => "principal fact value has an unsupported shape",
            Self::LimitExceeded => "principal fact value exceeds a hard limit",
            Self::Duplicate => "principal fact value contains a duplicate identity",
            Self::InvalidEncoding => "principal fact canonical encoding failed",
        })
    }
}

impl Error for PrincipalFactError {}

/// One canonical named authorization fact.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityPrincipalFactV1 {
    name: String,
    value: CanonicalValue,
    encoded_value: Arc<[u8]>,
}

impl CapabilityPrincipalFactV1 {
    /// Constructs one scalar fact or one canonical bounded scalar set.
    pub fn new(name: impl Into<String>, value: CanonicalValue) -> Result<Self, PrincipalFactError> {
        let name = name.into();
        validate_name(&name)?;
        let value = canonicalize_value(value)?;
        let encoded_value =
            encode_canonical_value(&value).map_err(|_| PrincipalFactError::InvalidEncoding)?;
        if encoded_value.len() > MAX_PRINCIPAL_FACT_BYTES_V1 {
            return Err(PrincipalFactError::LimitExceeded);
        }
        Ok(Self {
            name,
            value,
            encoded_value: encoded_value.into(),
        })
    }

    /// Compiler-visible symbolic fact name.
    #[doc(hidden)]
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Exact value for trusted policy evaluation. Formatting remains redacted.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_value(&self) -> &CanonicalValue {
        &self.value
    }

    /// Exact canonical value bytes for persistence and narrowing comparison.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_encoded_value(&self) -> &[u8] {
        &self.encoded_value
    }

    fn is_narrowing_of(&self, parent: &Self) -> bool {
        if self.name != parent.name {
            return false;
        }
        match (&self.value, &parent.value) {
            (CanonicalValue::List(child), CanonicalValue::List(parent)) => child
                .values()
                .iter()
                .all(|candidate| parent.values().iter().any(|value| value == candidate)),
            _ => self.encoded_value == parent.encoded_value,
        }
    }
}

impl fmt::Debug for CapabilityPrincipalFactV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityPrincipalFactV1([REDACTED])")
    }
}

/// Complete canonical fact set bound to one current capability revision.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityPrincipalFactsV1 {
    facts: Arc<[CapabilityPrincipalFactV1]>,
    canonical_bytes: Arc<[u8]>,
}

impl CapabilityPrincipalFactsV1 {
    /// Sorts facts by name, rejects duplicates, and enforces the complete byte ceiling.
    pub fn new(mut facts: Vec<CapabilityPrincipalFactV1>) -> Result<Self, PrincipalFactError> {
        if facts.len() > MAX_PRINCIPAL_FACTS_V1 {
            return Err(PrincipalFactError::LimitExceeded);
        }
        facts.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if facts.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(PrincipalFactError::Duplicate);
        }
        let canonical_bytes = encode_set(&facts)?;
        if canonical_bytes.len() > MAX_PRINCIPAL_FACT_BYTES_V1 {
            return Err(PrincipalFactError::LimitExceeded);
        }
        Ok(Self {
            facts: facts.into(),
            canonical_bytes: canonical_bytes.into(),
        })
    }

    /// Empty facts for roles whose policies require no principal attributes.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(Vec::new()).expect("the empty principal fact set is bounded")
    }

    /// Compiler-visible fact names without values.
    #[doc(hidden)]
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.facts.iter().map(|fact| fact.name.as_str())
    }

    /// Strictly decodes one canonical bounded capability fact set.
    #[doc(hidden)]
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, PrincipalFactError> {
        if bytes.len() > MAX_PRINCIPAL_FACT_BYTES_V1 {
            return Err(PrincipalFactError::LimitExceeded);
        }
        let mut reader = FactReader::new(bytes);
        if reader.take(FACT_SET_MAGIC.len())? != FACT_SET_MAGIC
            || reader.u32()? != PRINCIPAL_FACT_SET_VERSION_V1
        {
            return Err(PrincipalFactError::InvalidEncoding);
        }
        let count = reader.len(MAX_PRINCIPAL_FACTS_V1)?;
        let mut facts = Vec::with_capacity(count);
        for _ in 0..count {
            let name = std::str::from_utf8(reader.bytes(MAX_PRINCIPAL_FACT_NAME_BYTES_V1)?)
                .map_err(|_| PrincipalFactError::InvalidEncoding)?;
            let value = decode_canonical_value(reader.bytes(MAX_PRINCIPAL_FACT_BYTES_V1)?)
                .map_err(|_| PrincipalFactError::InvalidEncoding)?;
            facts.push(CapabilityPrincipalFactV1::new(name, value)?);
        }
        if !reader.is_empty() {
            return Err(PrincipalFactError::InvalidEncoding);
        }
        let decoded = Self::new(facts)?;
        if decoded.canonical_bytes.as_ref() != bytes {
            return Err(PrincipalFactError::InvalidEncoding);
        }
        Ok(decoded)
    }

    /// Looks up one trusted fact for authoritative policy evaluation.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_fact(&self, name: &str) -> Option<&CapabilityPrincipalFactV1> {
        self.facts
            .binary_search_by(|fact| fact.name.as_str().cmp(name))
            .ok()
            .map(|index| &self.facts[index])
    }

    /// Complete versioned canonical bytes for storage and capability identity.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Returns true only when every child fact is equal to or a set-subset of its parent fact.
    /// Dropping facts is narrowing; adding, changing, or widening facts is not.
    #[must_use]
    pub fn is_narrowing_of(&self, parent: &Self) -> bool {
        self.facts.iter().all(|child| {
            parent
                .facts
                .binary_search_by(|fact| fact.name.cmp(&child.name))
                .ok()
                .is_some_and(|index| child.is_narrowing_of(&parent.facts[index]))
        })
    }
}

impl fmt::Debug for CapabilityPrincipalFactsV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityPrincipalFactsV1")
            .field("facts", &"[REDACTED]")
            .field("fact_count", &self.facts.len())
            .finish()
    }
}

fn canonicalize_value(value: CanonicalValue) -> Result<CanonicalValue, PrincipalFactError> {
    match value {
        CanonicalValue::List(list) => {
            if list.len() > MAX_PRINCIPAL_FACT_VALUES_V1 {
                return Err(PrincipalFactError::LimitExceeded);
            }
            let mut members = list
                .values()
                .iter()
                .cloned()
                .map(|member| {
                    if !is_scalar(&member) {
                        return Err(PrincipalFactError::InvalidValue);
                    }
                    let bytes = encode_canonical_value(&member)
                        .map_err(|_| PrincipalFactError::InvalidEncoding)?;
                    Ok((bytes, member))
                })
                .collect::<Result<Vec<_>, PrincipalFactError>>()?;
            members.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            if members.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(PrincipalFactError::Duplicate);
            }
            CanonicalValue::list(members.into_iter().map(|(_, member)| member).collect())
                .map_err(|_| PrincipalFactError::InvalidValue)
        }
        scalar if is_scalar(&scalar) => Ok(scalar),
        _ => Err(PrincipalFactError::InvalidValue),
    }
}

fn is_scalar(value: &CanonicalValue) -> bool {
    matches!(
        value,
        CanonicalValue::Bool(_)
            | CanonicalValue::I64(_)
            | CanonicalValue::U64(_)
            | CanonicalValue::Decimal(_)
            | CanonicalValue::Money(_)
            | CanonicalValue::String(_)
            | CanonicalValue::Bytes(_)
            | CanonicalValue::Timestamp(_)
            | CanonicalValue::Date(_)
            | CanonicalValue::Uuid(_)
            | CanonicalValue::Enum { .. }
    )
}

fn validate_name(name: &str) -> Result<(), PrincipalFactError> {
    let mut bytes = name.bytes();
    let first = bytes.next().ok_or(PrincipalFactError::InvalidName)?;
    if name.len() > MAX_PRINCIPAL_FACT_NAME_BYTES_V1
        || !(first == b'_' || first.is_ascii_alphabetic())
        || bytes.any(|byte| !(byte == b'_' || byte.is_ascii_alphanumeric()))
    {
        return Err(PrincipalFactError::InvalidName);
    }
    Ok(())
}

fn encode_set(facts: &[CapabilityPrincipalFactV1]) -> Result<Vec<u8>, PrincipalFactError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(FACT_SET_MAGIC);
    bytes.extend_from_slice(&PRINCIPAL_FACT_SET_VERSION_V1.to_be_bytes());
    push_len(&mut bytes, facts.len())?;
    for fact in facts {
        push_bytes(&mut bytes, fact.name.as_bytes())?;
        push_bytes(&mut bytes, &fact.encoded_value)?;
        if bytes.len() > MAX_PRINCIPAL_FACT_BYTES_V1 {
            return Err(PrincipalFactError::LimitExceeded);
        }
    }
    Ok(bytes)
}

fn push_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), PrincipalFactError> {
    push_len(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn push_len(bytes: &mut Vec<u8>, value: usize) -> Result<(), PrincipalFactError> {
    let value = u32::try_from(value).map_err(|_| PrincipalFactError::LimitExceeded)?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

struct FactReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FactReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], PrincipalFactError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(PrincipalFactError::InvalidEncoding)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(PrincipalFactError::InvalidEncoding)?;
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, PrincipalFactError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| PrincipalFactError::InvalidEncoding)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn len(&mut self, maximum: usize) -> Result<usize, PrincipalFactError> {
        let len = usize::try_from(self.u32()?).map_err(|_| PrincipalFactError::LimitExceeded)?;
        if len > maximum {
            return Err(PrincipalFactError::LimitExceeded);
        }
        Ok(len)
    }

    fn bytes(&mut self, maximum: usize) -> Result<&'a [u8], PrincipalFactError> {
        let len = self.len(maximum)?;
        self.take(len)
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> CanonicalValue {
        CanonicalValue::string(value).expect("bounded text")
    }

    #[test]
    fn fact_sets_are_canonical_redacted_and_narrowing_only() {
        let parent = CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new(
                "groups",
                CanonicalValue::list(vec![text("operators"), text("authors")])
                    .expect("bounded list"),
            )
            .expect("fact"),
            CapabilityPrincipalFactV1::new("region", text("us-central")).expect("fact"),
        ])
        .expect("parent facts");
        let child = CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new(
                "groups",
                CanonicalValue::list(vec![text("authors")]).expect("bounded list"),
            )
            .expect("fact"),
        ])
        .expect("child facts");
        let widened = CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new(
                "groups",
                CanonicalValue::list(vec![text("authors"), text("owners")]).expect("bounded list"),
            )
            .expect("fact"),
        ])
        .expect("widened facts");

        assert!(child.is_narrowing_of(&parent));
        assert!(!widened.is_narrowing_of(&parent));
        assert_eq!(parent.names().collect::<Vec<_>>(), ["groups", "region"]);
        assert_eq!(
            format!("{parent:?}"),
            "CapabilityPrincipalFactsV1 { facts: \"[REDACTED]\", fact_count: 2 }"
        );
        assert!(!format!("{parent:?}").contains("authors"));
        assert_eq!(
            CapabilityPrincipalFactsV1::decode_canonical(parent.internal_canonical_bytes())
                .expect("strict round trip"),
            parent
        );
    }

    #[test]
    fn ordering_duplicates_and_limits_fail_closed() {
        let first = CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new("zeta", CanonicalValue::Bool(true)).expect("fact"),
            CapabilityPrincipalFactV1::new("alpha", CanonicalValue::U64(7)).expect("fact"),
        ])
        .expect("facts");
        let second = CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new("alpha", CanonicalValue::U64(7)).expect("fact"),
            CapabilityPrincipalFactV1::new("zeta", CanonicalValue::Bool(true)).expect("fact"),
        ])
        .expect("facts");
        assert_eq!(
            first.internal_canonical_bytes(),
            second.internal_canonical_bytes()
        );
        assert_eq!(
            CapabilityPrincipalFactsV1::new(vec![
                CapabilityPrincipalFactV1::new("same", CanonicalValue::Bool(true)).expect("fact"),
                CapabilityPrincipalFactV1::new("same", CanonicalValue::Bool(false)).expect("fact"),
            ])
            .expect_err("duplicate name"),
            PrincipalFactError::Duplicate
        );
        assert_eq!(
            CapabilityPrincipalFactV1::new("bad-name", CanonicalValue::Bool(true))
                .expect_err("invalid name"),
            PrincipalFactError::InvalidName
        );
        let mut trailing = first.internal_canonical_bytes().to_vec();
        trailing.push(0);
        assert_eq!(
            CapabilityPrincipalFactsV1::decode_canonical(&trailing).expect_err("trailing bytes"),
            PrincipalFactError::InvalidEncoding
        );
        for end in 0..first.internal_canonical_bytes().len() {
            assert!(
                CapabilityPrincipalFactsV1::decode_canonical(
                    &first.internal_canonical_bytes()[..end]
                )
                .is_err()
            );
        }
    }
}
