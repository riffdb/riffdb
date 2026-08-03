#![forbid(unsafe_code)]

//! WP-420 contextual causation, retry identity, and crash-recovery acceptance.

use riffdb_service::{
    ContextualCausationClaimsV1, ContextualCausationKey, ContextualCausationToken,
    ContextualCausationTokenCodec, ExecuteCommandRequest, ReactionIdempotencyValue, SourceName,
    SubmittedField, SubmittedFieldIdentity, SubmittedRecord, SubmittedValue,
    bind_reaction_idempotency_fixture, derive_reaction_idempotency,
};
use riffdb_types::{
    ActorId, CommandId, ContractVersion, DatabaseId, EventConsumerName, EventDeliveryAttempt,
    EventId, EventLeaseToken, QueryParameterHash, ReactiveModuleHash, ReactiveOperationHash,
    ReactiveOperationName, RequestId, Timestamp,
};

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

fn claims(command_id: CommandId) -> ContextualCausationClaimsV1 {
    claims_for_attempt(command_id, 2, 0x05)
}

fn claims_for_attempt(
    command_id: CommandId,
    attempt: u8,
    lease_token: u8,
) -> ContextualCausationClaimsV1 {
    ContextualCausationClaimsV1::new(
        DatabaseId::from_bytes(uuid_bytes(0x01)).expect("database"),
        7,
        ReactiveModuleHash::from_bytes([0x02; 32]),
        ReactiveOperationHash::from_bytes([0x03; 32]),
        ReactiveOperationName::new("TriageTicket").expect("operation"),
        QueryParameterHash::from_bytes([0x04; 32]),
        EventConsumerName::new("triage-worker").expect("consumer"),
        EventId::new(42_u64.try_into().expect("sequence"), 1),
        EventDeliveryAttempt::new(attempt).expect("attempt"),
        EventLeaseToken::from_bytes([lease_token; 32]),
        ActorId::new("agent:test").expect("principal"),
        9_u64.try_into().expect("revision"),
        command_id,
        Timestamp::new(1_900_000_000, 0).expect("expiry"),
        RequestId::from_bytes(uuid_bytes(0x07)).expect("request"),
    )
    .expect("causation claims")
}

#[test]
fn commit_before_ack_redelivery_reuses_one_business_identity() {
    let command_id = CommandId::new(3).expect("command");
    let first = claims_for_attempt(command_id, 1, 0x31);
    let redelivery = claims_for_attempt(command_id, 2, 0x32);
    let first_identity =
        derive_reaction_idempotency(&first, "assign", false).expect("first reaction identity");
    let retry_identity = derive_reaction_idempotency(&redelivery, "assign", false)
        .expect("redelivery reaction identity");
    assert_eq!(first_identity, retry_identity);

    let mut authoritative_business_state = vec![first_identity];
    if !authoritative_business_state.contains(&retry_identity) {
        authoritative_business_state.push(retry_identity);
    }
    assert_eq!(authoritative_business_state.len(), 1);
}

#[test]
fn causation_token_is_sealed_exact_and_value_free() {
    let codec = ContextualCausationTokenCodec::new(ContextualCausationKey::from_bytes([0x11; 32]));
    let claims = claims(CommandId::new(3).expect("command"));
    let token = codec.seal(&claims).expect("sealed token");
    assert_eq!(codec.open(&token), Ok(claims.clone()));

    let mut tampered = token.as_bytes().to_vec();
    let last = tampered.len() - 1;
    tampered[last] ^= 1;
    let tampered = ContextualCausationToken::checked(tampered).expect("bounded token");
    assert!(codec.open(&tampered).is_err());
    assert!(!format!("{token:?}").contains("TriageTicket"));
}

#[test]
fn reaction_identity_is_retry_stable_and_domain_separated() {
    let claims = claims(CommandId::new(3).expect("command"));
    let first = derive_reaction_idempotency(&claims, "assign", true).expect("UUID identity");
    let retry = derive_reaction_idempotency(&claims, "assign", true).expect("UUID identity");
    assert_eq!(first, retry);
    let ReactionIdempotencyValue::Uuid(uuid) = first else {
        panic!("expected UUID identity");
    };
    assert_eq!(uuid[6] >> 4, 8, "RFC 9562 UUIDv8 version bits");
    assert_eq!(uuid[8] >> 6, 2, "RFC 9562 variant bits");

    assert_ne!(
        derive_reaction_idempotency(&claims, "assign", false),
        derive_reaction_idempotency(&claims, "close", false),
    );
}

#[test]
fn reaction_binder_replaces_caller_identity_for_string_and_uuid_commands() {
    for (type_name, submitted) in [
        (
            "string<128>",
            SubmittedValue::string("caller-selected").expect("string input"),
        ),
        ("uuid", SubmittedValue::Uuid([0x99; 16])),
    ] {
        let source = format!(
            "contract Reaction version 1 {{\n  entity Row {{\n    key (id: uuid)\n    field value: i64\n  }}\n  aggregate Rows {{ root Row partition_by id conflict_key (id) }}\n  command Change {{\n    input request_key: {type_name}\n    input id: uuid\n    idempotency_key request_key\n    mutate Row(id) as row else Missing {{}}\n    set row.value = 1\n    return Changed {{ row: row }}\n  }}\n}}\n"
        );
        let bundle = riffdb_contract_compiler::compile_contract_source(&source)
            .expect("reaction contract compiles");
        let command = bundle
            .commands()
            .iter()
            .find(|command| command.name() == "Change")
            .expect("command");
        let claims = claims(command.command_id());
        let request = ExecuteCommandRequest::new(
            SourceName::new("Change").expect("command name"),
            Some(ContractVersion::new(1).expect("version")),
            SubmittedRecord::new(vec![
                SubmittedField::new(
                    SubmittedFieldIdentity::Name(
                        SourceName::new("request_key").expect("field name"),
                    ),
                    submitted,
                ),
                SubmittedField::new(
                    SubmittedFieldIdentity::Name(SourceName::new("id").expect("field name")),
                    SubmittedValue::Uuid([0x22; 16]),
                ),
            ])
            .expect("submitted input"),
        )
        .expect("request");
        let bound = bind_reaction_idempotency_fixture(&claims, "change", command, request)
            .expect("bind reaction identity");
        let value = bound
            .input()
            .fields()
            .iter()
            .find(|field| {
                field
                    .identity()
                    .name()
                    .is_some_and(|name| name.as_str() == "request_key")
            })
            .expect("idempotency field")
            .value();
        match (type_name, value) {
            ("uuid", SubmittedValue::Uuid(value)) => {
                assert_eq!(
                    *value,
                    match derive_reaction_idempotency(&claims, "change", true)
                        .expect("derived UUID")
                    {
                        ReactionIdempotencyValue::Uuid(value) => value,
                        ReactionIdempotencyValue::String(_) => panic!("expected UUID"),
                    }
                );
            }
            ("string<128>", SubmittedValue::String(value)) => {
                assert_eq!(
                    value.as_str(),
                    match derive_reaction_idempotency(&claims, "change", false)
                        .expect("derived string")
                    {
                        ReactionIdempotencyValue::String(value) => value,
                        ReactionIdempotencyValue::Uuid(_) => panic!("expected string"),
                    }
                );
            }
            _ => panic!("reaction binder produced the wrong idempotency type"),
        }
    }
}
