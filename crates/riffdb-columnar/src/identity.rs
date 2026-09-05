//! Schema-bound columnar definition and specification identities.

use std::fmt;

use riffdb_contract_ir::{ContractBundle, KeyComponentCodecV1, ValueType, ValueTypeTag};
use riffdb_types::{
    ColumnarDefinitionSemanticsHashV1, ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1,
    DefinitionFingerprint, ProjectionProviderDescriptorV1, ProjectionProviderPolicyModeV1,
    ProjectionProviderStaticBoundsV1, VectorProjectionSourceV1, hash_columnar_definition_semantics,
    hash_columnar_projection_spec,
};

use crate::{RegisteredDefinition, VectorProviderProfileV1};

/// Maximum bytes in the canonical definition-semantics document.
pub const MAX_COLUMNAR_DEFINITION_SEMANTICS_V1_BYTES: usize = 1_048_576;
/// Maximum bytes in a complete spec-hash payload, excluding the hash-domain frame.
pub const MAX_COLUMNAR_PROJECTION_SPEC_PAYLOAD_V1_BYTES: usize = 5_225;
/// Maximum bytes in the vector-only spec extension.
pub const MAX_COLUMNAR_VECTOR_EXTENSION_V1_BYTES: usize = 4_636;

/// Exact replay limits bound into one columnar specification.
pub use riffdb_types::ColumnarProjectionReplayLimitsV1 as ColumnarSpecReplayLimitsV1;

/// Exact vector-only portion of a complete columnar specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarVectorSpecExtensionV1(Vec<u8>);

impl ColumnarVectorSpecExtensionV1 {
    /// Resolves and encodes complete vector semantics from one checked bundle.
    pub fn from_registered(
        definition: &RegisteredDefinition,
        vector_field: riffdb_types::FieldId,
        bundle: &ContractBundle,
    ) -> Result<Self, ColumnarIdentityError> {
        let position = definition
            .projected_fields()
            .iter()
            .position(|field| *field == vector_field)
            .ok_or(ColumnarIdentityError)?;
        let dimension = definition.projected_types()[position]
            .vector_dimension()
            .ok_or(ColumnarIdentityError)?;
        let vector = bundle
            .schema()
            .vector_field_spec(definition.entity_type_id(), vector_field)
            .ok_or(ColumnarIdentityError)?;
        let production = bundle
            .schema()
            .vector_production_spec(definition.entity_type_id(), vector_field)
            .ok_or(ColumnarIdentityError)?;
        let source_count =
            u16::try_from(vector.source_fields().len()).map_err(|_| ColumnarIdentityError)?;
        let metadata = production.metadata();
        let model_identity = metadata.model_identity().as_bytes();
        let model_version = metadata.model_version().as_bytes();
        let mut bytes = Vec::new();
        push_u32(&mut bytes, dimension.get());
        bytes.push(vector.metric().tag());
        push_u16(&mut bytes, source_count);
        for source in vector.source_fields() {
            push_u32(&mut bytes, source.get());
        }
        bytes.extend_from_slice(&vector.stale_entity_count_threshold().to_be_bytes());
        push_u16(
            &mut bytes,
            u16::try_from(model_identity.len()).map_err(|_| ColumnarIdentityError)?,
        );
        bytes.extend_from_slice(model_identity);
        push_u16(
            &mut bytes,
            u16::try_from(model_version.len()).map_err(|_| ColumnarIdentityError)?,
        );
        bytes.extend_from_slice(model_version);
        match definition.vector_ann_config(vector_field) {
            Some(ann) => {
                bytes.push(1);
                push_u32(&mut bytes, ann.row_threshold());
                push_u32(&mut bytes, ann.recall_target_bps());
            }
            None => bytes.push(0),
        }
        if bytes.len() > MAX_COLUMNAR_VECTOR_EXTENSION_V1_BYTES {
            return Err(ColumnarIdentityError);
        }
        Ok(Self(bytes))
    }

    /// Canonical extension bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One exact canonical definition-semantics document and typed digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarDefinitionSemanticsV1 {
    bytes: Vec<u8>,
    payload: Vec<u8>,
    hash: ColumnarDefinitionSemanticsHashV1,
}

impl ColumnarDefinitionSemanticsV1 {
    /// Captures primary-key schema and all projected/organization types.
    pub fn from_registered(
        definition: &RegisteredDefinition,
    ) -> Result<Self, ColumnarIdentityError> {
        let key_schema = definition.primary_key_schema();
        if definition.primary_key_fields().len() != key_schema.components().len()
            || definition.primary_key_fields().len() > 4_096
            || definition.projected_fields().len() != definition.projected_types().len()
            || definition.projected_fields().len() > 4_096
            || definition.vector_ann_configs().len() > 4_096
        {
            return Err(ColumnarIdentityError);
        }
        let mut bytes = Vec::new();
        push_u16(&mut bytes, 1);
        push_u32(&mut bytes, definition.entity_type_id().get());
        push_u32(
            &mut bytes,
            u32::try_from(definition.primary_key_fields().len())
                .map_err(|_| ColumnarIdentityError)?,
        );
        push_u32(&mut bytes, key_schema.codec_version());
        push_u32(
            &mut bytes,
            u32::try_from(key_schema.maximum_encoded_bytes()).map_err(|_| ColumnarIdentityError)?,
        );
        for (field, component) in definition
            .primary_key_fields()
            .iter()
            .zip(key_schema.components())
        {
            push_u32(&mut bytes, field.get());
            push_type(&mut bytes, component.value_type())?;
            bytes.push(match component.codec() {
                KeyComponentCodecV1::Canonical => 1,
                KeyComponentCodecV1::OrderedBytes => 2,
            });
            push_u32(
                &mut bytes,
                u32::try_from(component.maximum_payload_bytes())
                    .map_err(|_| ColumnarIdentityError)?,
            );
            push_u32(
                &mut bytes,
                u32::try_from(component.enum_variants().len())
                    .map_err(|_| ColumnarIdentityError)?,
            );
            for variant in component.enum_variants() {
                push_u32(&mut bytes, variant.get());
            }
        }
        push_u32(
            &mut bytes,
            u32::try_from(definition.projected_fields().len())
                .map_err(|_| ColumnarIdentityError)?,
        );
        for (field, value_type) in definition
            .projected_fields()
            .iter()
            .zip(definition.projected_types())
        {
            push_u32(&mut bytes, field.get());
            push_type(&mut bytes, value_type)?;
        }
        push_u32(&mut bytes, definition.org_scope_field().get());
        push_type(&mut bytes, definition.org_scope_type())?;
        push_u32(
            &mut bytes,
            u32::try_from(definition.vector_ann_configs().len())
                .map_err(|_| ColumnarIdentityError)?,
        );
        for (field, config) in definition.vector_ann_configs() {
            push_u32(&mut bytes, field.get());
            push_u32(&mut bytes, config.row_threshold());
            push_u32(&mut bytes, config.recall_target_bps());
        }
        if bytes.len() > MAX_COLUMNAR_DEFINITION_SEMANTICS_V1_BYTES {
            return Err(ColumnarIdentityError);
        }
        let length = u32::try_from(bytes.len()).map_err(|_| ColumnarIdentityError)?;
        let mut payload = Vec::with_capacity(5 + bytes.len());
        payload.push(0x01);
        push_u32(&mut payload, length);
        payload.extend_from_slice(&bytes);
        let hash = hash_columnar_definition_semantics(&payload);
        Ok(Self {
            bytes,
            payload,
            hash,
        })
    }

    /// Canonical definition-semantics bytes, without the hash purpose framing.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Exact purpose-framed hash payload, excluding the ADR-0011 domain frame.
    #[must_use]
    pub fn hash_payload(&self) -> &[u8] {
        &self.payload
    }

    /// Purpose- and domain-separated digest.
    #[must_use]
    pub const fn hash(&self) -> ColumnarDefinitionSemanticsHashV1 {
        self.hash
    }
}

/// One complete schema-bound columnar specification and typed digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarProjectionSpecV1 {
    source: ColumnarProjectionSourceV1,
    definition_fingerprint: DefinitionFingerprint,
    definition_semantics_hash: ColumnarDefinitionSemanticsHashV1,
    descriptors: Vec<ProjectionProviderDescriptorV1>,
    replay: ColumnarSpecReplayLimitsV1,
    vector_extension: Vec<u8>,
    payload: Vec<u8>,
    hash: ColumnarProjectionSpecHashV1,
}

impl ColumnarProjectionSpecV1 {
    /// Resolves and hashes the exact scalar specification from one checked bundle.
    pub fn for_scalar(
        definition: &RegisteredDefinition,
        bundle: &ContractBundle,
    ) -> Result<Self, ColumnarIdentityError> {
        let semantics = ColumnarDefinitionSemanticsV1::from_registered(definition)?;
        let descriptor = definition
            .columnar_provider_descriptor_v1(
                ProjectionProviderPolicyModeV1::BoundedRowAdmission,
                scalar_bounds(),
            )
            .map_err(|_| ColumnarIdentityError)?;
        Self::new_checked(
            ColumnarProjectionSourceV1::scalar(bundle.lineage().clone(), definition.fingerprint()),
            definition.fingerprint(),
            semantics.hash(),
            vec![descriptor],
            ColumnarSpecReplayLimitsV1::new(86_400, 1_073_741_824, 100_000)
                .ok_or(ColumnarIdentityError)?,
            Vec::new(),
        )
    }

    /// Resolves and hashes the exact vector specification from one checked bundle.
    pub fn for_vector(
        definition: &RegisteredDefinition,
        vector_field: riffdb_types::FieldId,
        bundle: &ContractBundle,
    ) -> Result<Self, ColumnarIdentityError> {
        let production = bundle
            .schema()
            .vector_production_spec(definition.entity_type_id(), vector_field)
            .ok_or(ColumnarIdentityError)?;
        let semantics = ColumnarDefinitionSemanticsV1::from_registered(definition)?;
        let mut descriptors = vec![
            definition
                .vector_provider_descriptor_v1(
                    vector_field,
                    VectorProviderProfileV1::Exact,
                    ProjectionProviderPolicyModeV1::BoundedRowAdmission,
                    vector_bounds(),
                )
                .map_err(|_| ColumnarIdentityError)?,
        ];
        if definition.vector_ann_config(vector_field).is_some() {
            descriptors.push(
                definition
                    .vector_provider_descriptor_v1(
                        vector_field,
                        VectorProviderProfileV1::Approximate,
                        ProjectionProviderPolicyModeV1::BoundedRowAdmission,
                        vector_bounds(),
                    )
                    .map_err(|_| ColumnarIdentityError)?,
            );
        }
        let extension =
            ColumnarVectorSpecExtensionV1::from_registered(definition, vector_field, bundle)?;
        Self::new_checked(
            ColumnarProjectionSourceV1::vector(VectorProjectionSourceV1::new(
                bundle.lineage().clone(),
                definition.entity_type_id(),
                vector_field,
            )),
            definition.fingerprint(),
            semantics.hash(),
            descriptors,
            ColumnarSpecReplayLimitsV1::new(
                production.replay_age_seconds(),
                production.replay_bytes(),
                production.replay_backlog(),
            )
            .ok_or(ColumnarIdentityError)?,
            extension.0,
        )
    }

    fn new_checked(
        source: ColumnarProjectionSourceV1,
        definition_fingerprint: DefinitionFingerprint,
        definition_semantics_hash: ColumnarDefinitionSemanticsHashV1,
        descriptors: Vec<ProjectionProviderDescriptorV1>,
        replay: ColumnarSpecReplayLimitsV1,
        vector_extension: Vec<u8>,
    ) -> Result<Self, ColumnarIdentityError> {
        if descriptors.is_empty()
            || descriptors.len() > u8::MAX as usize
            || vector_extension.len() > MAX_COLUMNAR_VECTOR_EXTENSION_V1_BYTES
        {
            return Err(ColumnarIdentityError);
        }
        let descriptor_bytes = descriptors
            .iter()
            .map(ProjectionProviderDescriptorV1::to_canonical_bytes)
            .collect::<Vec<_>>();
        if descriptor_bytes
            .windows(2)
            .any(|pair| (pair[0][6], pair[0][7]) >= (pair[1][6], pair[1][7]))
        {
            return Err(ColumnarIdentityError);
        }
        if let ColumnarProjectionSourceV1::Scalar {
            definition_fingerprint: source_fingerprint,
            ..
        } = &source
            && source_fingerprint != &definition_fingerprint
        {
            return Err(ColumnarIdentityError);
        }
        let source_bytes = source.to_canonical_bytes();
        let mut payload = Vec::new();
        payload.push(0x02);
        push_u16(&mut payload, 1);
        push_u16(
            &mut payload,
            u16::try_from(source_bytes.len()).map_err(|_| ColumnarIdentityError)?,
        );
        payload.extend_from_slice(&source_bytes);
        payload.extend_from_slice(definition_fingerprint.as_bytes());
        payload.extend_from_slice(definition_semantics_hash.as_bytes());
        payload.push(u8::try_from(descriptors.len()).map_err(|_| ColumnarIdentityError)?);
        for descriptor in &descriptor_bytes {
            payload.extend_from_slice(descriptor);
        }
        payload.extend_from_slice(&replay.age_seconds().to_be_bytes());
        payload.extend_from_slice(&replay.bytes().to_be_bytes());
        payload.extend_from_slice(&replay.backlog().to_be_bytes());
        push_u32(
            &mut payload,
            u32::try_from(vector_extension.len()).map_err(|_| ColumnarIdentityError)?,
        );
        payload.extend_from_slice(&vector_extension);
        if payload.len() > MAX_COLUMNAR_PROJECTION_SPEC_PAYLOAD_V1_BYTES {
            return Err(ColumnarIdentityError);
        }
        let hash = hash_columnar_projection_spec(&payload);
        Ok(Self {
            source,
            definition_fingerprint,
            definition_semantics_hash,
            descriptors,
            replay,
            vector_extension,
            payload,
            hash,
        })
    }

    /// Stable schema-bound source.
    #[must_use]
    pub const fn source(&self) -> &ColumnarProjectionSourceV1 {
        &self.source
    }

    /// Exact hash preimage payload, excluding the ADR-0011 domain frame.
    #[must_use]
    pub fn hash_payload(&self) -> &[u8] {
        &self.payload
    }

    /// Complete typed specification digest.
    #[must_use]
    pub const fn hash(&self) -> ColumnarProjectionSpecHashV1 {
        self.hash
    }

    /// Accepted legacy definition fingerprint.
    #[must_use]
    pub const fn definition_fingerprint(&self) -> DefinitionFingerprint {
        self.definition_fingerprint
    }

    /// Separate complete definition-semantics digest.
    #[must_use]
    pub const fn definition_semantics_hash(&self) -> ColumnarDefinitionSemanticsHashV1 {
        self.definition_semantics_hash
    }

    /// Canonically ordered provider descriptors.
    #[must_use]
    pub fn descriptors(&self) -> &[ProjectionProviderDescriptorV1] {
        &self.descriptors
    }

    /// Bound replay limits.
    #[must_use]
    pub const fn replay_limits(&self) -> ColumnarSpecReplayLimitsV1 {
        self.replay
    }

    /// Exact vector extension; empty for Scalar.
    #[must_use]
    pub fn vector_extension(&self) -> &[u8] {
        &self.vector_extension
    }
}

const fn scalar_bounds() -> ProjectionProviderStaticBoundsV1 {
    ProjectionProviderStaticBoundsV1 {
        max_candidates: 100_000,
        max_output_rows: 500,
        max_measures: 16,
        max_input_bytes: 16_384,
        max_work_units: 1_000_000,
        max_state_bytes_per_row: 16_384,
        max_diagnostic_bytes: 4_096,
        retained_epochs: 8_192,
        max_catchup_lag: 100,
        max_epoch_lease_steps: 1_000,
    }
}

const fn vector_bounds() -> ProjectionProviderStaticBoundsV1 {
    ProjectionProviderStaticBoundsV1 {
        max_candidates: 500,
        max_output_rows: 499,
        max_measures: 0,
        max_input_bytes: 16_384,
        max_work_units: 2_048_000,
        max_state_bytes_per_row: 16_384,
        max_diagnostic_bytes: 4_096,
        retained_epochs: 8_192,
        max_catchup_lag: 100,
        max_epoch_lease_steps: 1_000,
    }
}

fn push_type(bytes: &mut Vec<u8>, value_type: &ValueType) -> Result<(), ColumnarIdentityError> {
    let mut encoded = Vec::new();
    encode_type(value_type, &mut encoded)?;
    let length = u16::try_from(encoded.len()).map_err(|_| ColumnarIdentityError)?;
    push_u16(bytes, length);
    bytes.extend_from_slice(&encoded);
    Ok(())
}

fn encode_type(value_type: &ValueType, bytes: &mut Vec<u8>) -> Result<(), ColumnarIdentityError> {
    bytes.push(value_type.tag() as u8);
    match value_type.tag() {
        ValueTypeTag::Decimal => {
            let spec = value_type.decimal_spec().ok_or(ColumnarIdentityError)?;
            bytes.extend_from_slice(&[spec.precision(), spec.scale()]);
        }
        ValueTypeTag::Money => bytes.extend_from_slice(
            value_type
                .currency()
                .ok_or(ColumnarIdentityError)?
                .as_bytes(),
        ),
        ValueTypeTag::String | ValueTypeTag::Bytes => push_u32(
            bytes,
            u32::try_from(value_type.byte_bound().ok_or(ColumnarIdentityError)?)
                .map_err(|_| ColumnarIdentityError)?,
        ),
        ValueTypeTag::Enum => push_u32(
            bytes,
            value_type
                .enum_type_id()
                .ok_or(ColumnarIdentityError)?
                .get(),
        ),
        ValueTypeTag::Optional => {
            let inner = value_type.optional_inner().ok_or(ColumnarIdentityError)?;
            if inner.tag() == ValueTypeTag::Optional {
                return Err(ColumnarIdentityError);
            }
            encode_type(inner, bytes)?;
        }
        ValueTypeTag::Vector => push_u32(
            bytes,
            value_type
                .vector_dimension()
                .ok_or(ColumnarIdentityError)?
                .get(),
        ),
        ValueTypeTag::List | ValueTypeTag::Record => return Err(ColumnarIdentityError),
        ValueTypeTag::Bool
        | ValueTypeTag::I64
        | ValueTypeTag::U64
        | ValueTypeTag::Timestamp
        | ValueTypeTag::Date
        | ValueTypeTag::Uuid => {}
    }
    Ok(())
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

/// Closed rejection of invalid or over-bound identity input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnarIdentityError;

impl fmt::Display for ColumnarIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid schema-bound columnar identity")
    }
}

impl std::error::Error for ColumnarIdentityError {}
