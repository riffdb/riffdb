//! Current-V7 authorization and bounded application-reimport orchestration.

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use riffdb_application::{
    ApplicationPortabilityManifest, InstallationSymbol, PortableRecordClass,
    PortableReimportStrategy, ReimportPageMappingOutcomeV1,
};
use riffdb_contract_ir::{RecordSchema, RecordTypeRef, SchemaIr, ValueType, ValueTypeTag};
use riffdb_errors::PublicError;
use riffdb_policy::{
    ApplicationReimportAuthorizationRequestV1, ApplicationReimportDecisionV1,
    ApplicationReimportPolicyOperationV1, AuthorizedApplicationReimportV1,
};
use riffdb_types::{
    CanonicalBytes, CanonicalList, CanonicalRecord, CanonicalString, CanonicalValue,
    CanonicalVector, CurrencyCode, Date, Decimal, DecimalSpec, IdempotencyKey, Money, RequestId,
    ServiceAuditPhaseV1, ServiceOperationV1, Timestamp, encode_canonical_record,
    hash_generated_artifact,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::audit::ServiceAuditTargetMap;
use crate::service::{MaintenanceInternalDefect, RiffDbServiceInner};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ApplicationReimportApplication, ApplicationReimportCoordinatorPort,
    ApplicationReimportMutationPortErrorV1, ApplicationReimportObservationPortErrorV1,
    ApplicationReimportOperationRequestV1, ApplicationReimportOperationResultV1,
    ApplicationReimportPagePreparationV1, ApplyApplicationReimportPageRequestV1,
    AuthorizedApplicationReimportOperationV1, AuthorizedApplicationReimportPageV1,
    AuthorizedApplicationReimportStartV1, GetApplicationReimportResultV1, PortAdmissionError,
    PortDriverStopped, RequestContext, RiffDbService, ServiceFailure, ServiceFuture, ServiceResult,
    StartApplicationReimportRequestV1, ensure_response_budget,
};

impl ApplicationReimportApplication for RiffDbService {
    fn start_application_reimport(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: StartApplicationReimportRequestV1,
    ) -> ServiceFuture<'_, ApplicationReimportOperationResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::StartApplicationReimport,
            ingress,
            async move { start_reimport(service, context, request_id, request).await },
        )
    }

    fn apply_application_reimport_page(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplyApplicationReimportPageRequestV1,
    ) -> ServiceFuture<'_, ApplicationReimportOperationResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::ApplyApplicationReimportPage,
            ingress,
            async move { apply_page(service, context, request_id, request).await },
        )
    }

    fn get_application_reimport(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationReimportOperationRequestV1,
    ) -> ServiceFuture<'_, GetApplicationReimportResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetApplicationReimport,
            ingress,
            async move { observe_or_cancel(service, context, request_id, request, false).await },
        )
    }

    fn cancel_application_reimport(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationReimportOperationRequestV1,
    ) -> ServiceFuture<'_, GetApplicationReimportResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::CancelApplicationReimport,
            ingress,
            async move { observe_or_cancel(service, context, request_id, request, true).await },
        )
    }
}

async fn start_reimport(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: StartApplicationReimportRequestV1,
) -> ServiceResult<ApplicationReimportOperationResultV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let policy_request = ApplicationReimportAuthorizationRequestV1::new(
        request.campaign_id(),
        request.lineage().clone(),
        request.portability_manifest().identity(),
        request.scope(),
        ApplicationReimportPolicyOperationV1::Start,
    );
    let begun = service
        .begin_application_reimport_invocation(
            &context,
            policy_request.clone(),
            ServiceOperationV1::StartApplicationReimport,
            ServiceAuditTargetMap::application_reimport(request.lineage().clone())
                .map_err(|_| integrity(&service))?,
        )
        .await?;
    validate_authorization(
        &service,
        &context,
        &policy_request,
        begun.initial_authorization(),
    )?;
    let result = async {
        let permit = reserve(
            &service,
            &context,
            coordinator.reserve_application_reimport_start(context.control()),
        )
        .await?;
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let expected_lineage = request.lineage().clone();
        let expected_manifest = request.portability_manifest().identity();
        let receipt = permit
            .submit(AuthorizedApplicationReimportStartV1::new(
                context.request_id(),
                context.ingress(),
                request,
                authorization,
            ))
            .map_err(pre_submit_failure)?;
        let result = wait_mutation(&service, &context, receipt).await?;
        if result.lineage() != &expected_lineage
            || result.campaign().source().portability_manifest_hash() != expected_manifest
            || result.campaign().source().page_hashes().is_empty()
            || result.campaign().authority().capability_id() != context.principal().capability_id()
        {
            return Err(integrity(&service));
        }
        ensure_response_budget(&result)?;
        Ok(result)
    }
    .await;
    finish(&service, &context, &begun, &result).await?;
    result
}

async fn apply_page(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: ApplyApplicationReimportPageRequestV1,
) -> ServiceResult<ApplicationReimportOperationResultV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let binding = resolve_binding(&service, &context, &coordinator, request.campaign_id()).await?;
    let policy_request = policy_request(
        request.campaign_id(),
        &binding,
        ApplicationReimportPolicyOperationV1::Page,
    );
    let begun = service
        .begin_application_reimport_invocation(
            &context,
            policy_request.clone(),
            ServiceOperationV1::ApplyApplicationReimportPage,
            ServiceAuditTargetMap::application_reimport(binding.lineage().clone())
                .map_err(|_| integrity(&service))?,
        )
        .await?;
    validate_authorization(
        &service,
        &context,
        &policy_request,
        begun.initial_authorization(),
    )?;
    let result = async {
        let preparation =
            prepare_page(&service, &context, &coordinator, request.campaign_id()).await?;
        if preparation.lineage() != binding.lineage()
            || preparation.portability_manifest().identity() != binding.portability_manifest_hash()
            || preparation.expected_page() != request.page().page_number()
            || preparation.expected_hash() != request.page().page_hash()
        {
            return Err(integrity(&service));
        }
        let outcomes =
            execute_page_commands(&service, &context, &request, &preparation, &policy_request)
                .await?;
        let permit = reserve(
            &service,
            &context,
            coordinator.reserve_application_reimport_page(context.control()),
        )
        .await?;
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let page_hash = request.page().page_hash();
        let receipt = permit
            .submit(AuthorizedApplicationReimportPageV1::new(
                request,
                authorization,
                outcomes,
            ))
            .map_err(pre_submit_failure)?;
        let result = wait_mutation(&service, &context, receipt).await?;
        if result.lineage() != binding.lineage()
            || result.campaign().source().portability_manifest_hash()
                != binding.portability_manifest_hash()
            || !result
                .campaign()
                .source()
                .page_hashes()
                .contains(&page_hash)
        {
            return Err(integrity(&service));
        }
        ensure_response_budget(&result)?;
        Ok(result)
    }
    .await;
    finish(&service, &context, &begun, &result).await?;
    result
}

async fn observe_or_cancel(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: ApplicationReimportOperationRequestV1,
    cancel: bool,
) -> ServiceResult<GetApplicationReimportResultV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let binding = resolve_binding(&service, &context, &coordinator, request.campaign_id()).await?;
    let (policy_operation, operation) = if cancel {
        (
            ApplicationReimportPolicyOperationV1::Cancel,
            ServiceOperationV1::CancelApplicationReimport,
        )
    } else {
        (
            ApplicationReimportPolicyOperationV1::Status,
            ServiceOperationV1::GetApplicationReimport,
        )
    };
    let policy_request = policy_request(request.campaign_id(), &binding, policy_operation);
    let begun = service
        .begin_application_reimport_invocation(
            &context,
            policy_request.clone(),
            operation,
            ServiceAuditTargetMap::application_reimport_operation(),
        )
        .await?;
    validate_authorization(
        &service,
        &context,
        &policy_request,
        begun.initial_authorization(),
    )?;
    let result = async {
        let permit = if cancel {
            EitherPermit::Cancel(
                reserve(
                    &service,
                    &context,
                    coordinator.reserve_application_reimport_cancel(context.control()),
                )
                .await?,
            )
        } else {
            EitherPermit::Observe(
                reserve(
                    &service,
                    &context,
                    coordinator.reserve_application_reimport_observation(context.control()),
                )
                .await?,
            )
        };
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let authorized = AuthorizedApplicationReimportOperationV1::new(request, authorization);
        let observation = match permit {
            EitherPermit::Observe(permit) => {
                let receipt = permit.submit(authorized).map_err(pre_submit_failure)?;
                wait_observation(&service, &context, receipt).await?
            }
            EitherPermit::Cancel(permit) => {
                let receipt = permit.submit(authorized).map_err(pre_submit_failure)?;
                wait_mutation(&service, &context, receipt).await?
            }
        };
        let result = match observation {
            None => GetApplicationReimportResultV1::NotFound,
            Some(observation)
                if observation.lineage() == binding.lineage()
                    && observation.campaign().source().portability_manifest_hash()
                        == binding.portability_manifest_hash() =>
            {
                GetApplicationReimportResultV1::Found(Box::new(observation))
            }
            Some(_) => return Err(integrity(&service)),
        };
        ensure_response_budget(&result)?;
        Ok(result)
    }
    .await;
    finish(&service, &context, &begun, &result).await?;
    result
}

enum EitherPermit {
    Observe(crate::ApplicationReimportObservationPermitV1),
    Cancel(crate::ApplicationReimportCancelPermitV1),
}

async fn resolve_binding(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    coordinator: &Arc<dyn ApplicationReimportCoordinatorPort>,
    campaign_id: riffdb_types::ApplicationInstallationCampaignId,
) -> ServiceResult<crate::ApplicationReimportPolicyBindingV1> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.resolve_application_reimport_binding(campaign_id, context.control()),
    )
    .await
    {
        Ok(Ok(Some(binding))) => Ok(binding),
        Ok(Ok(None)) => Err(PublicError::authorization_denied().into()),
        Ok(Err(error)) => Err(observation_failure(service, error)),
        Err(error) => Err(controlled_failure(error)),
    }
}

async fn prepare_page(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    coordinator: &Arc<dyn ApplicationReimportCoordinatorPort>,
    campaign_id: riffdb_types::ApplicationInstallationCampaignId,
) -> ServiceResult<ApplicationReimportPagePreparationV1> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.prepare_application_reimport_page(campaign_id, context.control()),
    )
    .await
    {
        Ok(Ok(Some(preparation))) => Ok(preparation),
        Ok(Ok(None)) => Err(PublicError::authorization_denied().into()),
        Ok(Err(error)) => Err(observation_failure(service, error)),
        Err(error) => Err(controlled_failure(error)),
    }
}

async fn execute_page_commands(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: &ApplyApplicationReimportPageRequestV1,
    preparation: &ApplicationReimportPagePreparationV1,
    policy_request: &ApplicationReimportAuthorizationRequestV1,
) -> ServiceResult<Vec<ReimportPageMappingOutcomeV1>> {
    if request.page().class() != riffdb_types::ApplicationExportClassV1::Entity {
        return Err(invalid_reimport_page());
    }
    let snapshot = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(|_| integrity(service))?
    .ok_or_else(|| integrity(service))?;
    let bundle = snapshot.bundle().bundle();
    preparation
        .portability_manifest()
        .validate_compiled_contract(bundle)
        .map_err(|_| integrity(service))?;
    let mut parsed = request
        .page()
        .lines()
        .iter()
        .map(|line| parse_entity_line(line.as_bytes(), bundle))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|()| invalid_reimport_page())?;
    if parsed.is_empty() {
        return Ok(Vec::new());
    }
    let entity_name = parsed[0].entity.clone();
    if parsed.iter().any(|record| record.entity != entity_name) {
        return Err(invalid_reimport_page());
    }
    let (mapping_symbol, command_name) =
        resolve_page_mapping(preparation.portability_manifest(), &entity_name)
            .ok_or_else(invalid_reimport_page)?;
    let command = bundle
        .commands()
        .iter()
        .find(|command| command.name() == command_name.as_str() && command.is_reimport())
        .ok_or_else(|| integrity(service))?;
    let resolved = snapshot
        .resolve_active_command(command.command_id(), command.plan_hash())
        .map_err(|_| integrity(service))?;
    let input_field = resolved
        .plan()
        .input()
        .record()
        .fields()
        .first()
        .ok_or_else(|| integrity(service))?;
    let mut outcomes = Vec::with_capacity(parsed.len());
    for record in parsed.drain(..) {
        let normalized = CanonicalRecord::new(vec![(
            input_field.id(),
            CanonicalValue::List(
                CanonicalList::new(vec![CanonicalValue::Record(record.fields.clone())])
                    .map_err(|_| integrity(service))?,
            ),
        )])
        .map_err(|_| integrity(service))?;
        validate_record(&normalized, resolved.plan().input().record())
            .map_err(|()| invalid_reimport_page())?;
        let server_key = derive_reimport_idempotency(
            preparation.portability_manifest(),
            &mapping_symbol,
            &record.key,
        )
        .map_err(|()| integrity(service))?;
        ensure_control_open(context)?;
        let authorization = authorize_current(service, context, policy_request.clone())?;
        let (replayed, outcome_hash) = crate::command_operations::execute_reimport_record(
            service,
            context,
            resolved.clone(),
            normalized,
            authorization,
            server_key,
        )
        .await?;
        outcomes.push(
            ReimportPageMappingOutcomeV1::new(
                PortableRecordClass::Entity,
                mapping_symbol.clone(),
                1,
                replayed,
                outcome_hash,
            )
            .map_err(|_| integrity(service))?,
        );
    }
    Ok(outcomes)
}

fn resolve_page_mapping(
    manifest: &ApplicationPortabilityManifest,
    entity_name: &str,
) -> Option<(InstallationSymbol, InstallationSymbol)> {
    manifest.input().mappings.iter().find_map(|mapping| {
        if mapping.class() != PortableRecordClass::Entity
            || mapping.symbol().as_str() != entity_name
        {
            return None;
        }
        match mapping.strategy() {
            PortableReimportStrategy::ReimportCommand { command } => {
                Some((mapping.symbol().clone(), command.clone()))
            }
            PortableReimportStrategy::LegacyApplicationCommand { .. }
            | PortableReimportStrategy::Migration { .. } => None,
        }
    })
}

fn derive_reimport_idempotency(
    manifest: &ApplicationPortabilityManifest,
    symbol: &InstallationSymbol,
    key: &CanonicalRecord,
) -> Result<IdempotencyKey, ()> {
    let key_bytes = encode_canonical_record(key).map_err(|_| ())?;
    let mut preimage = Vec::with_capacity(32 + symbol.as_str().len() + key_bytes.len() + 2);
    preimage.extend_from_slice(manifest.identity().as_bytes());
    preimage.push(PortableRecordClass::Entity as u8);
    preimage.extend_from_slice(symbol.as_str().as_bytes());
    preimage.push(0);
    preimage.extend_from_slice(&key_bytes);
    let digest = hash_generated_artifact(&preimage);
    let text = format!("reimport/{}", lower_hex(digest.as_bytes()));
    IdempotencyKey::new(text).map_err(|_| ())
}

struct ParsedEntityLine {
    entity: String,
    fields: CanonicalRecord,
    key: CanonicalRecord,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EntityLineWire {
    class: String,
    entity: String,
    fields: BTreeMap<String, Value>,
    key: BTreeMap<String, Value>,
    version: String,
    written_by_contract_version: String,
}

fn parse_entity_line(
    bytes: &[u8],
    bundle: &riffdb_contract_ir::ContractBundle,
) -> Result<ParsedEntityLine, ()> {
    let wire: EntityLineWire = serde_json::from_slice(bytes).map_err(|_| ())?;
    if serde_json::to_vec(&wire).map_err(|_| ())? != bytes
        || wire.class != "entity"
        || parse_canonical_u64(&wire.version).is_none()
        || parse_canonical_u64(&wire.written_by_contract_version).is_none()
    {
        return Err(());
    }
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == wire.entity)
        .ok_or(())?;
    let fields = materialize_record(&wire.fields, entity.record(), bundle.schema())?;
    let key = entity
        .primary_key_fields()
        .iter()
        .map(|field_id| {
            let field = entity.record().field(*field_id).ok_or(())?;
            let raw = wire.key.get(field.name()).ok_or(())?;
            let value = materialize_export_value(raw, field.value_type(), bundle.schema())?;
            let position = fields
                .fields()
                .binary_search_by_key(field_id, |(id, _)| *id)
                .map_err(|_| ())?;
            if fields.fields()[position].1 != value {
                return Err(());
            }
            Ok((*field_id, value))
        })
        .collect::<Result<Vec<_>, ()>>()?;
    if wire.key.len() != key.len() {
        return Err(());
    }
    Ok(ParsedEntityLine {
        entity: wire.entity,
        fields,
        key: CanonicalRecord::new(key).map_err(|_| ())?,
    })
}

fn materialize_record(
    input: &BTreeMap<String, Value>,
    record: &RecordSchema,
    schema: &SchemaIr,
) -> Result<CanonicalRecord, ()> {
    if input.len() != record.fields().len() {
        return Err(());
    }
    let fields = record
        .fields()
        .iter()
        .map(|field| {
            let raw = input.get(field.name()).ok_or(())?;
            Ok((
                field.id(),
                materialize_export_value(raw, field.value_type(), schema)?,
            ))
        })
        .collect::<Result<Vec<_>, ()>>()?;
    let value = CanonicalRecord::new(fields).map_err(|_| ())?;
    validate_record(&value, record)?;
    Ok(value)
}

fn validate_record(value: &CanonicalRecord, schema: &RecordSchema) -> Result<(), ()> {
    if value.fields().len() != schema.fields().len() {
        return Err(());
    }
    for ((actual_id, actual_value), expected_field) in value.fields().iter().zip(schema.fields()) {
        if *actual_id != expected_field.id()
            || expected_field
                .value_type()
                .validate_value(actual_value)
                .is_err()
        {
            return Err(());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecimalWire {
    coefficient: String,
    precision: u8,
    scale: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MoneyWire {
    coefficient: String,
    currency: String,
    precision: u8,
    scale: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimestampWire {
    nanos: u32,
    seconds: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DateWire {
    days_since_unix_epoch: i32,
}

fn materialize_export_value(
    value: &Value,
    expected: &ValueType,
    schema: &SchemaIr,
) -> Result<CanonicalValue, ()> {
    if value.is_null() {
        return expected
            .is_optional()
            .then_some(CanonicalValue::Null)
            .ok_or(());
    }
    let concrete = expected.optional_inner().unwrap_or(expected);
    let result = match concrete.tag() {
        ValueTypeTag::Bool => CanonicalValue::Bool(value.as_bool().ok_or(())?),
        ValueTypeTag::I64 => CanonicalValue::I64(parse_canonical_i64(value.as_str().ok_or(())?)?),
        ValueTypeTag::U64 => {
            CanonicalValue::U64(parse_canonical_u64(value.as_str().ok_or(())?).ok_or(())?)
        }
        ValueTypeTag::Decimal => {
            let wire: DecimalWire = serde_json::from_value(value.clone()).map_err(|_| ())?;
            let spec = DecimalSpec::new(wire.precision, wire.scale).map_err(|_| ())?;
            if concrete.decimal_spec() != Some(spec) {
                return Err(());
            }
            CanonicalValue::Decimal(
                Decimal::new(spec, parse_canonical_i128(&wire.coefficient)?).map_err(|_| ())?,
            )
        }
        ValueTypeTag::Money => {
            let wire: MoneyWire = serde_json::from_value(value.clone()).map_err(|_| ())?;
            let spec = DecimalSpec::new(wire.precision, wire.scale).map_err(|_| ())?;
            let currency = CurrencyCode::new(&wire.currency).map_err(|_| ())?;
            if concrete.currency() != Some(currency) {
                return Err(());
            }
            CanonicalValue::Money(Money::new(
                currency,
                Decimal::new(spec, parse_canonical_i128(&wire.coefficient)?).map_err(|_| ())?,
            ))
        }
        ValueTypeTag::String => CanonicalValue::String(
            CanonicalString::new(value.as_str().ok_or(())?.to_owned()).map_err(|_| ())?,
        ),
        ValueTypeTag::Bytes => CanonicalValue::Bytes(
            CanonicalBytes::new(
                base64::engine::general_purpose::STANDARD
                    .decode(value.as_str().ok_or(())?)
                    .map_err(|_| ())?,
            )
            .map_err(|_| ())?,
        ),
        ValueTypeTag::Timestamp => {
            let wire: TimestampWire = serde_json::from_value(value.clone()).map_err(|_| ())?;
            CanonicalValue::Timestamp(
                Timestamp::new(parse_canonical_i64(&wire.seconds)?, wire.nanos).map_err(|_| ())?,
            )
        }
        ValueTypeTag::Date => {
            let wire: DateWire = serde_json::from_value(value.clone()).map_err(|_| ())?;
            CanonicalValue::Date(Date::new(wire.days_since_unix_epoch))
        }
        ValueTypeTag::Uuid => CanonicalValue::Uuid(parse_uuid(value.as_str().ok_or(())?)?),
        ValueTypeTag::Enum => {
            let enum_type = concrete.enum_type_id().ok_or(())?;
            let variant_name = value.as_str().ok_or(())?;
            let enumeration = schema
                .enums()
                .iter()
                .find(|enumeration| enumeration.id() == enum_type)
                .ok_or(())?;
            let variant = enumeration
                .variants()
                .iter()
                .find(|variant| variant.name() == variant_name)
                .ok_or(())?;
            CanonicalValue::Enum {
                type_id: enum_type,
                variant_id: variant.id(),
            }
        }
        ValueTypeTag::List => {
            let (element, maximum) = concrete.list_parts().ok_or(())?;
            let values = value.as_array().ok_or(())?;
            if values.len() > maximum {
                return Err(());
            }
            CanonicalValue::List(
                CanonicalList::new(
                    values
                        .iter()
                        .map(|value| materialize_export_value(value, element, schema))
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|_| ())?,
            )
        }
        ValueTypeTag::Record => {
            let record = resolve_record_schema(concrete.record_ref().ok_or(())?, schema)?;
            let input =
                serde_json::from_value::<BTreeMap<String, Value>>(value.clone()).map_err(|_| ())?;
            CanonicalValue::Record(materialize_record(&input, record, schema)?)
        }
        ValueTypeTag::Vector => {
            let values = value
                .as_array()
                .ok_or(())?
                .iter()
                .map(|value| value.as_f64().map(|value| value as f32).ok_or(()))
                .collect::<Result<Vec<_>, _>>()?;
            CanonicalValue::Vector(CanonicalVector::new(values).map_err(|_| ())?)
        }
        ValueTypeTag::Optional => return Err(()),
    };
    concrete.validate_value(&result).map_err(|_| ())?;
    Ok(result)
}

fn resolve_record_schema<'a>(
    owner: &RecordTypeRef,
    schema: &'a SchemaIr,
) -> Result<&'a RecordSchema, ()> {
    match owner {
        RecordTypeRef::Entity(entity) => schema
            .entity(*entity)
            .map(riffdb_contract_ir::EntitySchema::record),
        RecordTypeRef::Event(event) => schema
            .event(*event)
            .map(riffdb_contract_ir::EventSchema::payload),
        RecordTypeRef::CommandInput(_)
        | RecordTypeRef::CommandOutcome { .. }
        | RecordTypeRef::ProjectionResult(_) => None,
    }
    .ok_or(())
}

fn parse_canonical_u64(value: &str) -> Option<u64> {
    canonical_digits(value)
        .then(|| value.parse().ok())
        .flatten()
}

fn parse_canonical_i64(value: &str) -> Result<i64, ()> {
    if value == "0"
        || value
            .strip_prefix('-')
            .is_some_and(canonical_unsigned_digits)
        || canonical_unsigned_digits(value)
    {
        value.parse().map_err(|_| ())
    } else {
        Err(())
    }
}

fn parse_canonical_i128(value: &str) -> Result<i128, ()> {
    if value == "0"
        || value
            .strip_prefix('-')
            .is_some_and(canonical_unsigned_digits)
        || canonical_unsigned_digits(value)
    {
        value.parse().map_err(|_| ())
    } else {
        Err(())
    }
}

fn canonical_digits(value: &str) -> bool {
    value == "0" || canonical_unsigned_digits(value)
}

fn canonical_unsigned_digits(value: &str) -> bool {
    !value.is_empty() && !value.starts_with('0') && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_uuid(value: &str) -> Result<[u8; 16], ()> {
    if value.len() != 36 {
        return Err(());
    }
    let compact = value
        .bytes()
        .enumerate()
        .filter_map(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                (byte != b'-').then_some(Err(()))
            } else {
                Some(Ok(byte))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if compact.len() != 32 {
        return Err(());
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in compact.chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Result<u8, ()> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(()),
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn invalid_reimport_page() -> ServiceFailure {
    PublicError::validation(riffdb_errors::ValidationIssues::one(
        riffdb_errors::ValidationIssue::new(
            riffdb_errors::ValidationCode::InvalidValue,
            riffdb_errors::ValidationPath::root(),
        ),
    ))
    .into()
}

fn policy_request(
    campaign_id: riffdb_types::ApplicationInstallationCampaignId,
    binding: &crate::ApplicationReimportPolicyBindingV1,
    operation: ApplicationReimportPolicyOperationV1,
) -> ApplicationReimportAuthorizationRequestV1 {
    ApplicationReimportAuthorizationRequestV1::new(
        campaign_id,
        binding.lineage().clone(),
        binding.portability_manifest_hash(),
        binding.scope(),
        operation,
    )
}

fn authorize_current(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: ApplicationReimportAuthorizationRequestV1,
) -> ServiceResult<Box<AuthorizedApplicationReimportV1>> {
    let authorization = match service
        .providers
        .policy
        .authorize_application_reimport(context.principal(), request.clone())
    {
        Ok(ApplicationReimportDecisionV1::Allow(authorization)) => authorization,
        Ok(ApplicationReimportDecisionV1::Deny(_)) => {
            return Err(PublicError::authorization_denied().into());
        }
        Err(_) => return Err(PublicError::storage_unavailable().into()),
    };
    validate_authorization(service, context, &request, &authorization)?;
    Ok(authorization)
}

fn validate_authorization(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: &ApplicationReimportAuthorizationRequestV1,
    authorization: &AuthorizedApplicationReimportV1,
) -> ServiceResult<()> {
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.request() != request
        || authorization.authority().capability_id() != context.principal().capability_id()
        || authorization.authority().capability_revision()
            != context.principal().capability_revision()
        || authorization.principal_id() != context.principal().principal_id()
        || authorization.actor_kind() != context.principal().actor_kind()
    {
        return Err(integrity(service));
    }
    Ok(())
}

async fn reserve<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    future: crate::PortFuture<'_, T, PortAdmissionError>,
) -> ServiceResult<T> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        future,
    )
    .await
    {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(error)) => Err(pre_submit_failure(error)),
        Err(error) => Err(controlled_failure(error)),
    }
}

async fn wait_mutation<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    receipt: crate::PortReceipt<T, ApplicationReimportMutationPortErrorV1>,
) -> ServiceResult<T> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(error))) => Err(mutation_failure(service, error)),
        Ok(Err(PortDriverStopped)) | Err(_) => Err(PublicError::outcome_unknown().into()),
    }
}

async fn wait_observation<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    receipt: crate::PortReceipt<T, ApplicationReimportObservationPortErrorV1>,
) -> ServiceResult<T> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(error))) => Err(observation_failure(service, error)),
        Ok(Err(PortDriverStopped)) => Err(PublicError::storage_unavailable().into()),
        Err(error) => Err(controlled_failure(error)),
    }
}

async fn finish<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunApplicationReimportInvocation,
    result: &ServiceResult<T>,
) -> ServiceResult<()> {
    let phase = match result {
        Ok(_) => ServiceAuditPhaseV1::Succeeded,
        Err(ServiceFailure::Cancelled | ServiceFailure::DeadlineExceeded) => {
            ServiceAuditPhaseV1::Cancelled
        }
        Err(ServiceFailure::Public(error))
            if error.kind() == riffdb_errors::PublicErrorKind::OutcomeUnknown =>
        {
            ServiceAuditPhaseV1::OutcomeUncertain
        }
        Err(_) => ServiceAuditPhaseV1::Failed,
    };
    begun.finish(service, context, phase).await.map_err(|_| {
        service.note_audit_failure(begun.operation());
        PublicError::storage_unavailable().into()
    })
}

fn exact_request_id(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request_id: RequestId,
) -> ServiceResult<()> {
    if request_id != context.request_id() {
        return Err(integrity(service));
    }
    Ok(())
}

fn coordinator(
    service: &RiffDbServiceInner,
) -> ServiceResult<Arc<dyn ApplicationReimportCoordinatorPort>> {
    service
        .providers
        .application_reimport
        .as_ref()
        .map(Arc::clone)
        .ok_or_else(|| PublicError::storage_unavailable().into())
}

fn ensure_control_open(context: &RequestContext) -> ServiceResult<()> {
    if context.control().is_cancelled() {
        Err(ServiceFailure::Cancelled)
    } else if context.control().is_deadline_exceeded() {
        Err(ServiceFailure::DeadlineExceeded)
    } else {
        Ok(())
    }
}

const fn pre_submit_failure(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            ServiceFailure::Public(PublicError::storage_unavailable())
        }
    }
}

const fn controlled_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn mutation_failure(
    service: &RiffDbServiceInner,
    error: ApplicationReimportMutationPortErrorV1,
) -> ServiceFailure {
    match error {
        ApplicationReimportMutationPortErrorV1::IdentityMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        ApplicationReimportMutationPortErrorV1::AuthorityChanged => {
            PublicError::authorization_denied().into()
        }
        ApplicationReimportMutationPortErrorV1::StorageUnavailable => {
            PublicError::storage_unavailable().into()
        }
        ApplicationReimportMutationPortErrorV1::Integrity => integrity(service),
        ApplicationReimportMutationPortErrorV1::SourceMismatch
        | ApplicationReimportMutationPortErrorV1::InvalidPhase
        | ApplicationReimportMutationPortErrorV1::CommandFailed => PublicError::validation(
            riffdb_errors::ValidationIssues::one(riffdb_errors::ValidationIssue::new(
                riffdb_errors::ValidationCode::InvalidValue,
                riffdb_errors::ValidationPath::root(),
            )),
        )
        .into(),
    }
}

fn observation_failure(
    service: &RiffDbServiceInner,
    error: ApplicationReimportObservationPortErrorV1,
) -> ServiceFailure {
    match error {
        ApplicationReimportObservationPortErrorV1::StorageUnavailable => {
            PublicError::storage_unavailable().into()
        }
        ApplicationReimportObservationPortErrorV1::Integrity => integrity(service),
    }
}

fn integrity(service: &RiffDbServiceInner) -> ServiceFailure {
    service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity)
}
