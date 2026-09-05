//! Canonical schema-bound identities for columnar projection state.

use std::fmt;

use crate::{ContractLineage, EntityTypeId, FieldId};

/// Maximum canonical bytes in one schema-bound columnar source identity.
pub const MAX_COLUMNAR_PROJECTION_SOURCE_V1_BYTES: usize = 291;

/// Positive replay ceilings bound into one columnar specification and control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnarProjectionReplayLimitsV1 {
    age_seconds: u64,
    bytes: u64,
    backlog: u64,
}

impl ColumnarProjectionReplayLimitsV1 {
    /// Constructs exact positive replay ceilings.
    #[must_use]
    pub const fn new(age_seconds: u64, bytes: u64, backlog: u64) -> Option<Self> {
        if age_seconds == 0 || bytes == 0 || backlog == 0 {
            return None;
        }
        Some(Self {
            age_seconds,
            bytes,
            backlog,
        })
    }

    /// Maximum replay age in seconds.
    #[must_use]
    pub const fn age_seconds(self) -> u64 {
        self.age_seconds
    }

    /// Maximum retained replay bytes.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    /// Maximum retained replay sequence backlog.
    #[must_use]
    pub const fn backlog(self) -> u64 {
        self.backlog
    }
}

/// Stable legacy columnar-definition fingerprint, preserved byte-for-byte.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DefinitionFingerprint([u8; 32]);

impl DefinitionFingerprint {
    /// Returns the 32 digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Reconstructs the accepted fingerprint from exact digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for DefinitionFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Stable logical identity of one compiler-declared vector projection.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VectorProjectionSourceV1 {
    lineage: ContractLineage,
    entity_type: EntityTypeId,
    vector_field: FieldId,
}

impl VectorProjectionSourceV1 {
    /// Constructs a source identity from compiler-owned stable IDs.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        entity_type: EntityTypeId,
        vector_field: FieldId,
    ) -> Self {
        Self {
            lineage,
            entity_type,
            vector_field,
        }
    }

    /// Contract lineage owning this projection.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Compiler-assigned entity identity.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Compiler-assigned vector field identity.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        self.vector_field
    }
}

/// One canonical schema-bound scalar or vector source identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarProjectionSourceV1 {
    /// Scalar columnar state bound to the accepted physical definition fingerprint.
    Scalar {
        /// Contract lineage owning the schema.
        lineage: ContractLineage,
        /// Accepted legacy definition fingerprint.
        definition_fingerprint: DefinitionFingerprint,
    },
    /// Vector columnar state retaining the accepted lineage/entity/field tuple.
    Vector(VectorProjectionSourceV1),
}

impl ColumnarProjectionSourceV1 {
    /// Constructs one scalar source identity.
    #[must_use]
    pub const fn scalar(
        lineage: ContractLineage,
        definition_fingerprint: DefinitionFingerprint,
    ) -> Self {
        Self::Scalar {
            lineage,
            definition_fingerprint,
        }
    }

    /// Constructs one vector source identity.
    #[must_use]
    pub const fn vector(source: VectorProjectionSourceV1) -> Self {
        Self::Vector(source)
    }

    /// Returns the owning contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        match self {
            Self::Scalar { lineage, .. } => lineage,
            Self::Vector(source) => source.lineage(),
        }
    }

    /// Encodes the exact ADR-0192 canonical source bytes.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let lineage = self.lineage().as_str().as_bytes();
        let mut bytes = Vec::with_capacity(1 + 2 + lineage.len() + 32);
        match self {
            Self::Scalar {
                definition_fingerprint,
                ..
            } => {
                bytes.push(0x01);
                bytes.extend_from_slice(&(lineage.len() as u16).to_be_bytes());
                bytes.extend_from_slice(lineage);
                bytes.extend_from_slice(definition_fingerprint.as_bytes());
            }
            Self::Vector(source) => {
                bytes.push(0x02);
                bytes.extend_from_slice(&(lineage.len() as u16).to_be_bytes());
                bytes.extend_from_slice(lineage);
                bytes.extend_from_slice(&source.entity_type().to_be_bytes());
                bytes.extend_from_slice(&source.vector_field().to_be_bytes());
            }
        }
        bytes
    }

    /// Decodes, fully consumes, and revalidates exact canonical source bytes.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ColumnarProjectionSourceError> {
        if bytes.len() < 4 || bytes.len() > MAX_COLUMNAR_PROJECTION_SOURCE_V1_BYTES {
            return Err(ColumnarProjectionSourceError);
        }
        let tag = bytes[0];
        let lineage_len = usize::from(u16::from_be_bytes([bytes[1], bytes[2]]));
        let suffix = match tag {
            0x01 => 32,
            0x02 => 8,
            _ => return Err(ColumnarProjectionSourceError),
        };
        let expected = 3_usize
            .checked_add(lineage_len)
            .and_then(|value| value.checked_add(suffix))
            .ok_or(ColumnarProjectionSourceError)?;
        if expected != bytes.len() {
            return Err(ColumnarProjectionSourceError);
        }
        let lineage_text = std::str::from_utf8(&bytes[3..3 + lineage_len])
            .map_err(|_| ColumnarProjectionSourceError)?;
        let lineage =
            ContractLineage::new(lineage_text).map_err(|_| ColumnarProjectionSourceError)?;
        let tail = &bytes[3 + lineage_len..];
        let source = if tag == 0x01 {
            let fingerprint: [u8; 32] =
                tail.try_into().map_err(|_| ColumnarProjectionSourceError)?;
            Self::scalar(lineage, DefinitionFingerprint::from_bytes(fingerprint))
        } else {
            let entity = EntityTypeId::new(u32::from_be_bytes(
                tail[..4]
                    .try_into()
                    .map_err(|_| ColumnarProjectionSourceError)?,
            ))
            .ok_or(ColumnarProjectionSourceError)?;
            let field = FieldId::new(u32::from_be_bytes(
                tail[4..]
                    .try_into()
                    .map_err(|_| ColumnarProjectionSourceError)?,
            ))
            .ok_or(ColumnarProjectionSourceError)?;
            Self::vector(VectorProjectionSourceV1::new(lineage, entity, field))
        };
        if source.to_canonical_bytes() != bytes {
            return Err(ColumnarProjectionSourceError);
        }
        Ok(source)
    }
}

/// Closed rejection of malformed canonical columnar source bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnarProjectionSourceError;

impl fmt::Display for ColumnarProjectionSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid canonical columnar projection source")
    }
}

impl std::error::Error for ColumnarProjectionSourceError {}
