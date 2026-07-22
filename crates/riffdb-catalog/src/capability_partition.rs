//! Shared schema validation for capability partition scopes.

use std::error::Error;
use std::fmt;

use riffdb_types::{MAX_CAPABILITY_PARTITIONS, PartitionScopeV1, ScopedPartitionV1};

use crate::ValidatedContractBundle;

/// Opaque process-local failure from capability partition schema validation.
///
/// The value deliberately retains no key bytes, lineage, aggregate, schema,
/// component, enum, or decoder source. Callers may classify it only as a
/// capability partition validation failure.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CapabilityPartitionValidationError {
    _private: (),
}

const CAPABILITY_PARTITION_INVALID: CapabilityPartitionValidationError =
    CapabilityPartitionValidationError { _private: () };

impl fmt::Debug for CapabilityPartitionValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityPartitionValidationError")
    }
}

impl fmt::Display for CapabilityPartitionValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("capability partition is invalid")
    }
}

impl Error for CapabilityPartitionValidationError {}

/// Validates one envelope-checked scoped partition against a checked bundle.
///
/// This function is pure and bounded by the existing partition-key and schema
/// limits. Success is process-local evidence only; it is not a policy decision
/// or a serializable validation certificate.
pub fn validate_capability_partition(
    bundle: &ValidatedContractBundle,
    partition: &ScopedPartitionV1,
) -> Result<(), CapabilityPartitionValidationError> {
    let contract = bundle.bundle();
    if partition.lineage() != contract.lineage() {
        return Err(CAPABILITY_PARTITION_INVALID);
    }

    let aggregate = contract
        .schema()
        .aggregate(partition.partition_key().aggregate_type_id())
        .ok_or(CAPABILITY_PARTITION_INVALID)?;
    aggregate
        .keys()
        .partition_schema()
        .decode_partition(partition.partition_key())
        .map(|_| ())
        .map_err(|_| CAPABILITY_PARTITION_INVALID)
}

/// Validates every explicit partition in a complete capability scope.
///
/// `All` contains no schema-directed key material. Explicit scopes are checked
/// for the public v1 collection bound before any entry is decoded, including
/// values assembled directly through the public enum representation.
pub fn validate_capability_partition_scope(
    bundle: &ValidatedContractBundle,
    scope: &PartitionScopeV1,
) -> Result<(), CapabilityPartitionValidationError> {
    let PartitionScopeV1::Explicit(entries) = scope else {
        return Ok(());
    };
    if entries.is_empty() || entries.len() > MAX_CAPABILITY_PARTITIONS {
        return Err(CAPABILITY_PARTITION_INVALID);
    }
    entries
        .iter()
        .try_for_each(|entry| validate_capability_partition(bundle, entry))
}

#[cfg(test)]
mod tests {
    use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
    use riffdb_contract_ir::AggregateSchema;
    use riffdb_types::{
        AggregateTypeId, CanonicalValue, ContractLineage, EnumVariantId, PartitionKey,
        PartitionKeyBuilder,
    };

    use super::*;

    const KEY_VALIDATION_SOURCE: &str = r#"
contract CapabilityKeys version 1 {
  enum Mode { Alpha }
  entity TextRow { key (id: string<4>) }
  entity BytesRow { key (id: bytes<4>) }
  entity EnumRow { key (id: Mode) }
  entity BoolRow { key (id: bool) }
  aggregate TextRows { root TextRow partition_by id conflict_key (id) }
  aggregate BytesRows { root BytesRow partition_by id conflict_key (id) }
  aggregate EnumRows { root EnumRow partition_by id conflict_key (id) }
  aggregate BoolRows { root BoolRow partition_by id conflict_key (id) }
}
"#;

    fn checked_bundle(source: &str) -> ValidatedContractBundle {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(source).expect("key-validation contract compiles"),
        )
        .expect("compiler bundle is catalog-valid")
    }

    fn aggregate<'a>(bundle: &'a ValidatedContractBundle, name: &str) -> &'a AggregateSchema {
        bundle
            .bundle()
            .schema()
            .aggregates()
            .iter()
            .find(|aggregate| aggregate.name() == name)
            .expect("named aggregate")
    }

    fn scoped(bundle: &ValidatedContractBundle, key: PartitionKey) -> ScopedPartitionV1 {
        ScopedPartitionV1::new(bundle.lineage().clone(), key)
    }

    fn explicit(bundle: &ValidatedContractBundle, key: PartitionKey) -> PartitionScopeV1 {
        PartitionScopeV1::explicit(vec![scoped(bundle, key)]).expect("one explicit partition")
    }

    #[test]
    fn accepts_all_and_complete_keys_for_the_active_lineage() {
        let bundle = checked_bundle(KEY_VALIDATION_SOURCE);
        let aggregate = aggregate(&bundle, "TextRows");
        let key = aggregate
            .keys()
            .partition_schema()
            .encode_partition(&[CanonicalValue::string("key").expect("bounded string")])
            .expect("complete partition key");

        assert!(validate_capability_partition_scope(&bundle, &PartitionScopeV1::All).is_ok());
        assert!(validate_capability_partition(&bundle, &scoped(&bundle, key.clone())).is_ok());
        assert!(validate_capability_partition_scope(&bundle, &explicit(&bundle, key)).is_ok());
    }

    #[test]
    fn rejects_wrong_lineage_unknown_owner_incomplete_and_trailing_keys() {
        let bundle = checked_bundle(KEY_VALIDATION_SOURCE);
        let aggregate = aggregate(&bundle, "TextRows");
        let valid = aggregate
            .keys()
            .partition_schema()
            .encode_partition(&[CanonicalValue::string("key").expect("bounded string")])
            .expect("complete partition key");
        let foreign =
            ScopedPartitionV1::new(ContractLineage::new("Foreign").expect("lineage"), valid);
        assert!(validate_capability_partition(&bundle, &foreign).is_err());

        let unknown_owner = AggregateTypeId::new(u32::MAX).expect("nonzero aggregate ID");
        let mut unknown = PartitionKeyBuilder::new(unknown_owner);
        unknown.push_str("key").expect("bounded component");
        assert!(
            validate_capability_partition(
                &bundle,
                &scoped(&bundle, unknown.finish().expect("envelope-checked key")),
            )
            .is_err()
        );

        let incomplete = PartitionKeyBuilder::new(aggregate.id())
            .finish()
            .expect("envelope-only key");
        assert!(validate_capability_partition(&bundle, &scoped(&bundle, incomplete)).is_err());

        let mut trailing = PartitionKeyBuilder::new(aggregate.id());
        trailing.push_str("key").expect("bounded component");
        trailing
            .push_bool(true)
            .expect("bounded trailing component");
        assert!(
            validate_capability_partition(
                &bundle,
                &scoped(&bundle, trailing.finish().expect("envelope-checked key")),
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_schema_invalid_component_payloads() {
        let bundle = checked_bundle(KEY_VALIDATION_SOURCE);

        let text = aggregate(&bundle, "TextRows");
        let mut invalid_utf8 = PartitionKeyBuilder::new(text.id());
        invalid_utf8
            .push_bytes(&[0xff])
            .expect("structurally bounded bytes");
        let mut oversized_text = PartitionKeyBuilder::new(text.id());
        oversized_text
            .push_str("abcde")
            .expect("structurally bounded text");

        let bytes = aggregate(&bundle, "BytesRows");
        let mut oversized_bytes = PartitionKeyBuilder::new(bytes.id());
        oversized_bytes
            .push_bytes(&[0; 5])
            .expect("structurally bounded bytes");

        let enumeration = aggregate(&bundle, "EnumRows");
        let mut unknown_variant = PartitionKeyBuilder::new(enumeration.id());
        unknown_variant
            .push_enum_variant(EnumVariantId::new(2).expect("second variant ID"))
            .expect("structurally bounded enum");

        let boolean = aggregate(&bundle, "BoolRows");
        let mut invalid_bool = vec![0x50, 0x01];
        invalid_bool.extend_from_slice(&boolean.id().to_be_bytes());
        invalid_bool.push(2);
        let invalid_bool = PartitionKey::from_bytes(invalid_bool)
            .expect("envelope validation does not inspect Boolean payloads");

        for key in [
            invalid_utf8.finish().expect("structural key"),
            oversized_text.finish().expect("structural key"),
            oversized_bytes.finish().expect("structural key"),
            unknown_variant.finish().expect("structural key"),
            invalid_bool,
        ] {
            assert!(validate_capability_partition(&bundle, &scoped(&bundle, key)).is_err());
        }
    }

    #[test]
    fn complete_scope_is_bounded_before_entry_validation() {
        let bundle = checked_bundle(KEY_VALIDATION_SOURCE);
        assert!(
            validate_capability_partition_scope(&bundle, &PartitionScopeV1::Explicit(Vec::new()))
                .is_err()
        );

        let aggregate = aggregate(&bundle, "BoolRows");
        let key = aggregate
            .keys()
            .partition_schema()
            .encode_partition(&[CanonicalValue::Bool(true)])
            .expect("complete partition key");
        let over_limit = vec![scoped(&bundle, key); MAX_CAPABILITY_PARTITIONS + 1];
        assert!(
            validate_capability_partition_scope(&bundle, &PartitionScopeV1::Explicit(over_limit),)
                .is_err()
        );
    }

    #[test]
    fn compatible_successor_accepts_the_same_checked_partition_schema() {
        let genesis_compiled =
            compile_contract_source(KEY_VALIDATION_SOURCE).expect("genesis compiles");
        let successor_source = KEY_VALIDATION_SOURCE.replacen("version 1", "version 2", 1);
        let successor_compiled = compile_contract_successor(&successor_source, &genesis_compiled)
            .expect("unchanged successor is compatible");
        let genesis = ValidatedContractBundle::from_compiler_bundle(genesis_compiled)
            .expect("genesis is catalog-valid");
        let successor = ValidatedContractBundle::from_compiler_bundle(successor_compiled)
            .expect("successor is catalog-valid");
        let key = aggregate(&genesis, "TextRows")
            .keys()
            .partition_schema()
            .encode_partition(&[CanonicalValue::string("key").expect("bounded string")])
            .expect("complete partition key");
        let partition = scoped(&genesis, key);

        assert!(validate_capability_partition(&genesis, &partition).is_ok());
        assert!(validate_capability_partition(&successor, &partition).is_ok());
    }

    #[test]
    fn failure_debug_and_display_are_fixed_and_redacted() {
        let error = CAPABILITY_PARTITION_INVALID;
        assert_eq!(format!("{error:?}"), "CapabilityPartitionValidationError");
        assert_eq!(error.to_string(), "capability partition is invalid");
    }
}
