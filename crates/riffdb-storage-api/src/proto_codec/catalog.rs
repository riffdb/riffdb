use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, ApprovalId, ContractBundleHash, ContractLineage, ContractVersion,
    QueryModuleHash, QueryModuleName, QueryModuleVersion, RequestId,
};

use crate::{
    ActiveCatalogPointerV1, ActiveQueryModulePointerV1, EncodedPageItem,
    StoredCatalogAdministrationV1, StoredContractBundleV1, StoredQueryModuleAdministrationV1,
    StoredQueryModuleV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, audit_principal_from_proto,
    audit_principal_to_proto, decode_message, encode_message, fixed, require, storage_result,
    timestamp_from_proto, timestamp_to_proto,
};

const BUNDLE: &str = "riffdb.storage.v1.StoredContractBundleV1";
const ACTIVE: &str = "riffdb.storage.v1.ActiveCatalogPointerV1";
const ADMINISTRATION: &str = "riffdb.storage.v1.StoredCatalogAdministrationV1";
const QUERY_MODULE: &str = "riffdb.storage.v1.StoredQueryModuleV1";
const ACTIVE_QUERY_MODULE: &str = "riffdb.storage.v1.ActiveQueryModulePointerV1";
const QUERY_MODULE_ADMINISTRATION: &str = "riffdb.storage.v1.StoredQueryModuleAdministrationV1";

fn active_to_proto(value: &ActiveCatalogPointerV1) -> wire::ActiveCatalogPointerV1 {
    wire::ActiveCatalogPointerV1 {
        contract_lineage: value.lineage().as_str().to_owned(),
        contract_version: value.contract_version().get(),
        contract_bundle_hash: value.bundle_hash().as_bytes().to_vec(),
    }
}

fn active_query_module_to_proto(
    value: &ActiveQueryModulePointerV1,
) -> wire::ActiveQueryModulePointerV1 {
    wire::ActiveQueryModulePointerV1 {
        module_name: value.module_name().as_str().to_owned(),
        module_version: value.module_version().get(),
        module_hash: value.module_hash().as_bytes().to_vec(),
        contract_lineage: value.contract_lineage().as_str().to_owned(),
        contract_version: value.contract_version().get(),
        contract_bundle_hash: value.contract_bundle_hash().as_bytes().to_vec(),
    }
}

fn active_query_module_from_proto(
    value: wire::ActiveQueryModulePointerV1,
) -> Result<ActiveQueryModulePointerV1, DurableCodecError> {
    Ok(ActiveQueryModulePointerV1::new(
        QueryModuleName::new(value.module_name).map_err(|_| DurableCodecError::corrupt())?,
        QueryModuleVersion::new(value.module_version).ok_or_else(DurableCodecError::corrupt)?,
        QueryModuleHash::from_bytes(fixed(value.module_hash)?),
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        ContractVersion::new(value.contract_version).ok_or_else(DurableCodecError::corrupt)?,
        ContractBundleHash::from_bytes(fixed(value.contract_bundle_hash)?),
    ))
}

fn active_from_proto(
    value: wire::ActiveCatalogPointerV1,
) -> Result<ActiveCatalogPointerV1, DurableCodecError> {
    Ok(ActiveCatalogPointerV1::new(
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        ContractVersion::new(value.contract_version).ok_or_else(DurableCodecError::corrupt)?,
        ContractBundleHash::from_bytes(fixed(value.contract_bundle_hash)?),
    ))
}

/// Encodes one immutable compiled contract bundle.
pub fn encode_contract_bundle_v1(
    value: &StoredContractBundleV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        BUNDLE,
        &wire::StoredContractBundleV1 {
            contract_lineage: value.lineage().as_str().to_owned(),
            contract_version: value.contract_version().get(),
            contract_bundle_hash: value.bundle_hash().as_bytes().to_vec(),
            canonical_bundle: value.canonical_bytes().to_vec(),
        },
    )
}

/// Decodes one immutable compiled contract bundle without interpreting its IR.
pub fn decode_contract_bundle_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredContractBundleV1>, DurableCodecError> {
    decode_message::<wire::StoredContractBundleV1, _, _>(BUNDLE, encoded, |value| {
        storage_result(StoredContractBundleV1::new(
            ContractLineage::new(value.contract_lineage)
                .map_err(|_| DurableCodecError::corrupt())?,
            ContractVersion::new(value.contract_version).ok_or_else(DurableCodecError::corrupt)?,
            ContractBundleHash::from_bytes(fixed(value.contract_bundle_hash)?),
            value.canonical_bundle,
        ))
    })
}

/// Encodes the one active-catalog pointer.
pub fn encode_active_catalog_pointer_v1(
    value: &ActiveCatalogPointerV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(ACTIVE, &active_to_proto(value))
}

/// Decodes the one active-catalog pointer.
pub fn decode_active_catalog_pointer_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<ActiveCatalogPointerV1>, DurableCodecError> {
    decode_message::<wire::ActiveCatalogPointerV1, _, _>(ACTIVE, encoded, active_from_proto)
}

/// Encodes one catalog activation administration record.
pub fn encode_catalog_administration_v1(
    value: &StoredCatalogAdministrationV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        ADMINISTRATION,
        &wire::StoredCatalogAdministrationV1 {
            administration_sequence: value.administration_sequence().get(),
            request_id: value.request_id().as_bytes().to_vec(),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
            principal: Some(audit_principal_to_proto(value.principal())),
            previous_active: value.previous_active().map(active_to_proto),
            activated: Some(active_to_proto(value.activated())),
            approval_id: value.approval_id().map(|value| value.as_str().to_owned()),
        },
    )
}

/// Decodes one catalog activation administration record.
pub fn decode_catalog_administration_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCatalogAdministrationV1>, DurableCodecError> {
    decode_message::<wire::StoredCatalogAdministrationV1, _, _>(ADMINISTRATION, encoded, |value| {
        Ok(StoredCatalogAdministrationV1::from_stored_parts(
            AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            RequestId::from_bytes(fixed(value.request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            timestamp_from_proto(require(value.timestamp)?)?,
            audit_principal_from_proto(require(value.principal)?)?,
            value.previous_active.map(active_from_proto).transpose()?,
            active_from_proto(require(value.activated)?)?,
            value
                .approval_id
                .map(ApprovalId::new)
                .transpose()
                .map_err(|_| DurableCodecError::corrupt())?,
        ))
    })
}

/// Encodes one immutable canonical query module.
pub fn encode_query_module_v1(
    value: &StoredQueryModuleV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        QUERY_MODULE,
        &wire::StoredQueryModuleV1 {
            module_name: value.module_name().as_str().to_owned(),
            module_version: value.module_version().get(),
            module_hash: value.module_hash().as_bytes().to_vec(),
            contract_lineage: value.contract_lineage().as_str().to_owned(),
            contract_version: value.contract_version().get(),
            contract_bundle_hash: value.contract_bundle_hash().as_bytes().to_vec(),
            canonical_module: value.canonical_bytes().to_vec(),
        },
    )
}

/// Decodes one immutable query module without interpreting its compiler IR.
pub fn decode_query_module_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredQueryModuleV1>, DurableCodecError> {
    decode_message::<wire::StoredQueryModuleV1, _, _>(QUERY_MODULE, encoded, |value| {
        storage_result(StoredQueryModuleV1::new(
            QueryModuleName::new(value.module_name).map_err(|_| DurableCodecError::corrupt())?,
            QueryModuleVersion::new(value.module_version).ok_or_else(DurableCodecError::corrupt)?,
            QueryModuleHash::from_bytes(fixed(value.module_hash)?),
            ContractLineage::new(value.contract_lineage)
                .map_err(|_| DurableCodecError::corrupt())?,
            ContractVersion::new(value.contract_version).ok_or_else(DurableCodecError::corrupt)?,
            ContractBundleHash::from_bytes(fixed(value.contract_bundle_hash)?),
            value.canonical_module,
        ))
    })
}

/// Encodes one exact active query-module pointer.
pub fn encode_active_query_module_pointer_v1(
    value: &ActiveQueryModulePointerV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(ACTIVE_QUERY_MODULE, &active_query_module_to_proto(value))
}

/// Decodes one exact active query-module pointer.
pub fn decode_active_query_module_pointer_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<ActiveQueryModulePointerV1>, DurableCodecError> {
    decode_message::<wire::ActiveQueryModulePointerV1, _, _>(
        ACTIVE_QUERY_MODULE,
        encoded,
        active_query_module_from_proto,
    )
}

/// Encodes one query-module activation administration record.
pub fn encode_query_module_administration_v1(
    value: &StoredQueryModuleAdministrationV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        QUERY_MODULE_ADMINISTRATION,
        &wire::StoredQueryModuleAdministrationV1 {
            administration_sequence: value.administration_sequence().get(),
            request_id: value.request_id().as_bytes().to_vec(),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
            principal: Some(audit_principal_to_proto(value.principal())),
            previous_active: value.previous_active().map(active_query_module_to_proto),
            activated: Some(active_query_module_to_proto(value.activated())),
            approval_id: value.approval_id().map(|value| value.as_str().to_owned()),
        },
    )
}

/// Decodes one query-module activation administration record.
pub fn decode_query_module_administration_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredQueryModuleAdministrationV1>, DurableCodecError> {
    decode_message::<wire::StoredQueryModuleAdministrationV1, _, _>(
        QUERY_MODULE_ADMINISTRATION,
        encoded,
        |value| {
            Ok(StoredQueryModuleAdministrationV1::from_stored_parts(
                AdministrationSequence::new(value.administration_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?,
                RequestId::from_bytes(fixed(value.request_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                timestamp_from_proto(require(value.timestamp)?)?,
                audit_principal_from_proto(require(value.principal)?)?,
                value
                    .previous_active
                    .map(active_query_module_from_proto)
                    .transpose()?,
                active_query_module_from_proto(require(value.activated)?)?,
                value
                    .approval_id
                    .map(ApprovalId::new)
                    .transpose()
                    .map_err(|_| DurableCodecError::corrupt())?,
            ))
        },
    )
}
