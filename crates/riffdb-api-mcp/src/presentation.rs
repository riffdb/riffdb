use std::error::Error;
use std::fmt;

use riffdb_errors::{
    ApplicationError, ErrorClass, PublicError, PublicErrorDetails, RecoveryAction,
    ValidationPathSegment,
};
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Map, Value, json};

use crate::{MCP_OUTBOUND_MESSAGE_MAX_BYTES, SchemaDocument, bounded_json};

/// Escapes one untrusted string for use as the contents of a generated
/// Markdown block.
///
/// Newlines and directional/control characters cannot create a second block,
/// and Markdown/HTML metacharacters cannot create links, raw HTML, emphasis,
/// or instruction-like headings. The caller owns the static block structure.
pub(crate) fn escape_markdown_block_text(input: &str) -> Result<String, McpPresentationError> {
    if input.is_empty() || input.len() > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
        return Err(McpPresentationError);
    }
    let mut output = String::with_capacity(input.len().min(MCP_OUTBOUND_MESSAGE_MAX_BYTES));
    let mut at_block_start = true;
    let mut leading_decimal = false;

    for character in input.chars() {
        if output.len() > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpPresentationError);
        }
        if character == '\n' || character == '\r' || character.is_control() {
            push_markdown_fragment(&mut output, " ")?;
            continue;
        }
        if is_directional_control(character) {
            push_markdown_fragment(&mut output, "\u{fffd}")?;
            at_block_start = false;
            leading_decimal = false;
            continue;
        }

        if at_block_start {
            if character.is_whitespace() {
                push_markdown_character(&mut output, character)?;
                continue;
            }
            leading_decimal = character.is_ascii_digit();
            if matches!(character, '#' | '+' | '-') {
                push_markdown_fragment(&mut output, "\\")?;
            }
            at_block_start = false;
        } else if leading_decimal {
            if character == '.' {
                push_markdown_fragment(&mut output, "\\")?;
                leading_decimal = false;
            } else if !character.is_ascii_digit() {
                leading_decimal = false;
            }
        }

        match character {
            '&' => push_markdown_fragment(&mut output, "&amp;")?,
            '<' => push_markdown_fragment(&mut output, "&lt;")?,
            '>' => push_markdown_fragment(&mut output, "&gt;")?,
            '\\' | '`' | '*' | '_' | '{' | '}' | '[' | ']' | '(' | ')' | '!' | '|' => {
                push_markdown_fragment(&mut output, "\\")?;
                push_markdown_character(&mut output, character)?;
            }
            _ => push_markdown_character(&mut output, character)?,
        }
    }
    Ok(output)
}

fn push_markdown_fragment(output: &mut String, fragment: &str) -> Result<(), McpPresentationError> {
    let next = output
        .len()
        .checked_add(fragment.len())
        .ok_or(McpPresentationError)?;
    if next > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
        return Err(McpPresentationError);
    }
    output.push_str(fragment);
    Ok(())
}

fn push_markdown_character(
    output: &mut String,
    character: char,
) -> Result<(), McpPresentationError> {
    let next = output
        .len()
        .checked_add(character.len_utf8())
        .ok_or(McpPresentationError)?;
    if next > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
        return Err(McpPresentationError);
    }
    output.push(character);
    Ok(())
}

pub(crate) const fn is_directional_control(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{2069}'
            | '\u{feff}'
    )
}

/// Validation boundary shared by request arguments and structured results.
///
/// Implementations must evaluate the supplied instance against the exact
/// advertised Draft 2020-12 schema. The common renderer never treats converter
/// success as schema-validation evidence.
pub(crate) trait StructuredContentValidator: Send + Sync {
    /// Returns success only when `instance` conforms to `schema`.
    fn validate(
        &self,
        schema: &SchemaDocument,
        instance: &Value,
    ) -> Result<(), McpPresentationError>;
}

/// One schema-validated, bounded JSON object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundedStructuredContent {
    value: Value,
    compact_json: String,
}

impl BoundedStructuredContent {
    /// Validates and bounds one request or result object.
    pub(crate) fn validate(
        schema: &SchemaDocument,
        value: Value,
        validator: &dyn StructuredContentValidator,
    ) -> Result<Self, McpPresentationError> {
        if !value.is_object() {
            return Err(McpPresentationError);
        }
        validator.validate(schema, &value)?;
        let compact_json = bounded_json::to_string(&value, MCP_OUTBOUND_MESSAGE_MAX_BYTES)
            .map_err(|_| McpPresentationError)?;
        Ok(Self {
            value,
            compact_json,
        })
    }
}

/// Renders a schema-validated business result with `isError=false`.
#[must_use]
pub(crate) fn render_business_result(content: BoundedStructuredContent) -> CallToolResult {
    let mut result = CallToolResult::success(vec![ContentBlock::text(content.compact_json)]);
    result.structured_content = Some(content.value);
    result
}

/// Converts the single public-safe error owner into one common MCP tool error.
pub(crate) fn render_public_error(
    error: &PublicError,
) -> Result<CallToolResult, McpPresentationError> {
    let value = public_error_value(error);
    let compact_json = bounded_json::to_string(&value, MCP_OUTBOUND_MESSAGE_MAX_BYTES)
        .map_err(|_| McpPresentationError)?;
    Ok(CallToolResult::error(vec![ContentBlock::text(
        compact_json,
    )]))
}

/// Renders the shared symbolic application error as bounded machine JSON.
pub(crate) fn render_application_error(
    error: &ApplicationError,
) -> Result<CallToolResult, McpPresentationError> {
    let value = application_error_value(error);
    let compact_json = bounded_json::to_string(&value, MCP_OUTBOUND_MESSAGE_MAX_BYTES)
        .map_err(|_| McpPresentationError)?;
    Ok(CallToolResult::error(vec![ContentBlock::text(
        compact_json,
    )]))
}

fn application_error_value(error: &ApplicationError) -> Value {
    let mut object = Map::new();
    object.insert("type".to_owned(), Value::String("application".to_owned()));
    object.insert(
        "code".to_owned(),
        Value::String(error.code().as_str().to_owned()),
    );
    object.insert(
        "message".to_owned(),
        Value::String(error.safe_message().to_owned()),
    );
    object.insert(
        "category".to_owned(),
        Value::String(error.category().as_str().to_owned()),
    );
    object.insert(
        "recovery_action".to_owned(),
        Value::String(error.recovery_action().as_str().to_owned()),
    );
    object.insert(
        "operation".to_owned(),
        Value::String(error.operation().as_str().to_owned()),
    );
    if let Some((lineage, version)) = error.context().contract() {
        object.insert(
            "contract_lineage".to_owned(),
            Value::String(lineage.as_str().to_owned()),
        );
        object.insert(
            "contract_version".to_owned(),
            Value::String(version.to_string()),
        );
    }
    if let Some(symbol) = error.context().operation_symbol() {
        object.insert(
            "operation_symbol".to_owned(),
            Value::String(symbol.to_owned()),
        );
    }
    if !error.context().symbol_path().is_empty() {
        object.insert(
            "symbol_path".to_owned(),
            Value::Array(
                error
                    .context()
                    .symbol_path()
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if let Some(span) = error.context().source_span() {
        object.insert(
            "source_span".to_owned(),
            json!({"start": span.start().to_string(), "end": span.end().to_string()}),
        );
    }
    object.insert(
        "fixes".to_owned(),
        Value::Array(
            error
                .fixes()
                .iter()
                .map(|fix| Value::String(fix.as_str().to_owned()))
                .collect(),
        ),
    );
    if let Some(trace_id) = error.context().trace_id() {
        object.insert("trace_id".to_owned(), Value::String(trace_id.to_string()));
    }
    if let Some(incident_id) = error.incident_id() {
        object.insert(
            "incident_id".to_owned(),
            Value::String(incident_id.to_string()),
        );
    }
    Value::Object(object)
}

fn public_error_value(error: &PublicError) -> Value {
    let mut object = Map::new();
    object.insert("code".to_owned(), Value::String(error.code().to_owned()));
    object.insert(
        "message".to_owned(),
        Value::String(error.safe_message().to_owned()),
    );
    object.insert(
        "class".to_owned(),
        Value::String(error_class(error.class()).to_owned()),
    );
    object.insert(
        "recovery_action".to_owned(),
        Value::String(recovery_action(error.recovery_action()).to_owned()),
    );
    match error.details() {
        PublicErrorDetails::None => {}
        PublicErrorDetails::Validation(issues) => {
            let values = issues
                .as_slice()
                .iter()
                .map(|issue| {
                    let path: Vec<Value> = issue
                        .path()
                        .segments()
                        .iter()
                        .map(|segment| match segment {
                            ValidationPathSegment::Field(field_id) => {
                                json!({"field_id": field_id.get()})
                            }
                            ValidationPathSegment::ListIndex(index) => {
                                json!({"list_index": index})
                            }
                        })
                        .collect();
                    json!({"code": issue.code().code(), "path": path})
                })
                .collect();
            object.insert("validation_issues".to_owned(), Value::Array(values));
        }
        PublicErrorDetails::ContractMismatch {
            active_contract_version,
        } => {
            object.insert(
                "active_contract_version".to_owned(),
                Value::String(active_contract_version.to_string()),
            );
        }
        PublicErrorDetails::CommandExecutionFailed { code } => {
            object.insert(
                "execution_failure".to_owned(),
                Value::Number(code.code().into()),
            );
        }
    }
    if let Some(incident_id) = error.incident_id() {
        object.insert(
            "incident_id".to_owned(),
            Value::String(incident_id.to_string()),
        );
    }
    Value::Object(object)
}

const fn error_class(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::InvalidArgument => "invalid_argument",
        ErrorClass::Conflict => "conflict",
        ErrorClass::PermissionDenied => "permission_denied",
        ErrorClass::DeadlineExceeded => "deadline_exceeded",
        ErrorClass::FailedPrecondition => "failed_precondition",
        ErrorClass::Unavailable => "unavailable",
        ErrorClass::Uncertain => "uncertain",
        ErrorClass::Internal => "internal",
    }
}

const fn recovery_action(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::CorrectRequest => "correct_request",
        RecoveryAction::Retry => "retry",
        RecoveryAction::ResolveWithSameIdempotencyKey => "resolve_with_same_idempotency_key",
        RecoveryAction::ObtainPermission => "obtain_permission",
        RecoveryAction::RefreshContract => "refresh_contract",
        RecoveryAction::ContactOperator => "contact_operator",
    }
}

/// A schema, serialization, or presentation bound was not satisfied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpPresentationError;

impl fmt::Display for McpPresentationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP presentation failed")
    }
}

impl Error for McpPresentationError {}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;

    use riffdb_errors::{
        ApplicationError, ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation,
        ApplicationSourceSpan, InternalError, PublicError, PublicErrorKind, ValidationCode,
        ValidationIssue, ValidationIssues, ValidationPath, ValidationPathSegment,
    };
    use riffdb_types::{
        ContractLineage, ContractVersion, ExecutionFailureCode, FieldId, IncidentId, RequestId,
    };
    use serde_json::{Value, json};

    use super::{
        BoundedStructuredContent, McpPresentationError, StructuredContentValidator,
        escape_markdown_block_text, render_application_error, render_business_result,
        render_public_error,
    };
    use crate::{
        MCP_OUTBOUND_MESSAGE_MAX_BYTES, SchemaDocument, bounded_json, fixed_tool_registry,
    };

    const PUBLIC_ERROR_KINDS: [PublicErrorKind; 9] = [
        PublicErrorKind::Validation,
        PublicErrorKind::IdempotencyKeyReuse,
        PublicErrorKind::AuthorizationDenied,
        PublicErrorKind::ConcurrencyDeadlineExceeded,
        PublicErrorKind::ContractMismatch,
        PublicErrorKind::StorageUnavailable,
        PublicErrorKind::OutcomeUnknown,
        PublicErrorKind::InternalDefect,
        PublicErrorKind::CommandExecutionFailed,
    ];

    #[test]
    fn application_error_matches_the_cross_surface_fixture() {
        let trace_id = RequestId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x23,
        ])
        .expect("trace ID");
        let context = ApplicationErrorContext::empty()
            .with_contract(
                ContractLineage::new("ticketdesk").expect("lineage"),
                ContractVersion::new(18).expect("version"),
            )
            .with_operation_symbol("TicketPage".to_owned())
            .expect("symbol")
            .with_symbol_path(vec![
                "Ticket".to_owned(),
                "requester".to_owned(),
                "User.email".to_owned(),
            ])
            .expect("path")
            .with_source_span(ApplicationSourceSpan::new(20, 44).expect("span"))
            .with_trace_id(trace_id);
        let error = ApplicationError::new(
            ApplicationErrorCode::AuthorizationDenied,
            ApplicationOperation::ExecuteQuery,
            context,
            None,
        );
        let result = render_application_error(&error).expect("render");
        let value = serde_json::to_value(result).expect("SDK result");
        let text = value["content"][0]["text"].as_str().expect("text");
        let actual: Value = serde_json::from_str(text).expect("JSON");
        let expected: Value = serde_json::from_slice(include_bytes!(
            "../../../fixtures/application-errors/authorization-ticket-page-v1.json"
        ))
        .expect("fixture");
        assert_eq!(actual, expected);
    }

    const PUBLIC_ERROR_TEXT_GOLDENS: [&str; 9] = [
        "{\"class\":\"invalid_argument\",\"code\":\"validation_failed\",\"message\":\"request validation failed\",\"recovery_action\":\"correct_request\",\"validation_issues\":[{\"code\":\"type_mismatch\",\"path\":[{\"field_id\":7},{\"list_index\":3}]}]}",
        "{\"class\":\"conflict\",\"code\":\"idempotency_key_reuse\",\"message\":\"idempotency key was reused with different input\",\"recovery_action\":\"correct_request\"}",
        "{\"class\":\"permission_denied\",\"code\":\"authorization_denied\",\"message\":\"operation is not authorized\",\"recovery_action\":\"obtain_permission\"}",
        "{\"class\":\"deadline_exceeded\",\"code\":\"concurrency_deadline_exceeded\",\"message\":\"concurrency deadline exceeded\",\"recovery_action\":\"retry\"}",
        "{\"active_contract_version\":\"41\",\"class\":\"failed_precondition\",\"code\":\"contract_mismatch\",\"message\":\"contract version or plan does not match\",\"recovery_action\":\"refresh_contract\"}",
        "{\"class\":\"unavailable\",\"code\":\"storage_unavailable\",\"message\":\"storage is temporarily unavailable\",\"recovery_action\":\"retry\"}",
        "{\"class\":\"uncertain\",\"code\":\"outcome_unknown\",\"message\":\"command outcome is not yet known\",\"recovery_action\":\"resolve_with_same_idempotency_key\"}",
        "{\"class\":\"internal\",\"code\":\"internal_defect\",\"incident_id\":\"42424242-4242-7242-8242-424242424242\",\"message\":\"an internal error occurred\",\"recovery_action\":\"contact_operator\"}",
        "{\"class\":\"failed_precondition\",\"code\":\"command_execution_failed\",\"execution_failure\":1,\"message\":\"command execution failed\",\"recovery_action\":\"contact_operator\"}",
    ];

    struct AcceptingValidator;

    impl StructuredContentValidator for AcceptingValidator {
        fn validate(&self, _: &SchemaDocument, _: &Value) -> Result<(), McpPresentationError> {
            Ok(())
        }
    }

    struct RejectingValidator;

    impl StructuredContentValidator for RejectingValidator {
        fn validate(&self, _: &SchemaDocument, _: &Value) -> Result<(), McpPresentationError> {
            Err(McpPresentationError)
        }
    }

    struct PanickingValidator;

    impl StructuredContentValidator for PanickingValidator {
        fn validate(&self, _: &SchemaDocument, _: &Value) -> Result<(), McpPresentationError> {
            panic!("non-object input reached the schema validator")
        }
    }

    fn one_schema() -> &'static SchemaDocument {
        fixed_tool_registry()
            .expect("accepted fixed tool registry")
            .tools()
            .first()
            .expect("fixed tool")
            .input_schema()
    }

    #[test]
    fn business_results_require_a_schema_validated_object_witness() {
        let value = json!({"contract_lineage": "orders", "version": 7});
        let bounded =
            BoundedStructuredContent::validate(one_schema(), value.clone(), &AcceptingValidator)
                .expect("bounded result");

        assert_eq!(bounded.value, value);
        assert_eq!(
            serde_json::from_str::<Value>(&bounded.compact_json).expect("compact JSON"),
            value
        );

        let result = render_business_result(bounded);
        assert_eq!(
            serde_json::to_value(result).expect("SDK result"),
            json!({
                "content": [{
                    "type": "text",
                    "text": "{\"contract_lineage\":\"orders\",\"version\":7}"
                }],
                "structuredContent": {
                    "contract_lineage": "orders",
                    "version": 7
                },
                "isError": false
            })
        );
    }

    #[test]
    fn validation_fails_closed_before_a_business_result_can_be_rendered() {
        assert_eq!(
            BoundedStructuredContent::validate(
                one_schema(),
                json!({"contract_lineage": "orders"}),
                &RejectingValidator,
            ),
            Err(McpPresentationError)
        );
        assert_eq!(
            BoundedStructuredContent::validate(
                one_schema(),
                Value::String("not an object".to_owned()),
                &PanickingValidator,
            ),
            Err(McpPresentationError)
        );
    }

    fn incident_id() -> IncidentId {
        let mut bytes = [0x42; 16];
        bytes[6] = 0x72;
        bytes[8] = 0x82;
        IncidentId::from_bytes(bytes).expect("fixture is a valid UUIDv7")
    }

    fn public_error_fixture(kind: PublicErrorKind) -> PublicError {
        match kind {
            PublicErrorKind::Validation => {
                PublicError::validation(ValidationIssues::one(ValidationIssue::new(
                    ValidationCode::TypeMismatch,
                    ValidationPath::new(vec![
                        ValidationPathSegment::Field(FieldId::new(7).expect("field ID is nonzero")),
                        ValidationPathSegment::ListIndex(3),
                    ])
                    .expect("fixture path is bounded"),
                )))
            }
            PublicErrorKind::IdempotencyKeyReuse => PublicError::idempotency_key_reuse(),
            PublicErrorKind::AuthorizationDenied => PublicError::authorization_denied(),
            PublicErrorKind::ConcurrencyDeadlineExceeded => {
                PublicError::concurrency_deadline_exceeded()
            }
            PublicErrorKind::ContractMismatch => PublicError::contract_mismatch(
                ContractVersion::new(41).expect("contract version is nonzero"),
            ),
            PublicErrorKind::StorageUnavailable => PublicError::storage_unavailable(),
            PublicErrorKind::OutcomeUnknown => PublicError::outcome_unknown(),
            PublicErrorKind::InternalDefect => PublicError::internal_defect(incident_id()),
            PublicErrorKind::CommandExecutionFailed => {
                PublicError::command_execution_failed(ExecutionFailureCode::ArithmeticFault)
            }
        }
    }

    #[test]
    fn every_public_error_has_one_bounded_text_golden_and_no_structured_content() {
        for (kind, expected_text) in PUBLIC_ERROR_KINDS
            .into_iter()
            .zip(PUBLIC_ERROR_TEXT_GOLDENS)
        {
            let error = public_error_fixture(kind);
            let result = render_public_error(&error).expect("MCP tool error");

            assert_eq!(error.kind(), kind);
            assert_eq!(result.is_error, Some(true), "{kind:?}");
            assert_eq!(result.structured_content, None, "{kind:?}");
            assert_eq!(
                serde_json::to_value(&result).expect("SDK result"),
                json!({
                    "content": [{
                        "type": "text",
                        "text": expected_text
                    }],
                    "isError": true
                }),
                "{kind:?}"
            );
            bounded_json::encoded_len(&result, MCP_OUTBOUND_MESSAGE_MAX_BYTES)
                .expect("public error result is bounded");
        }
    }

    #[derive(Debug)]
    struct SecretPeerFailure;

    impl fmt::Display for SecretPeerFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("peer-text-canary internal-text-canary")
        }
    }

    impl Error for SecretPeerFailure {}

    #[test]
    fn public_error_renderer_cannot_expose_peer_or_internal_source_text() {
        let public = InternalError::new(incident_id(), SecretPeerFailure).into_public();
        let result = render_public_error(&public).expect("redacted MCP tool error");
        let encoded = serde_json::to_string(&result).expect("SDK result");

        assert_eq!(result.structured_content, None);
        assert!(!encoded.contains("peer-text-canary"));
        assert!(!encoded.contains("internal-text-canary"));
        assert_eq!(
            serde_json::to_value(result).expect("SDK result"),
            json!({
                "content": [{
                    "type": "text",
                    "text": PUBLIC_ERROR_TEXT_GOLDENS[7]
                }],
                "isError": true
            })
        );
    }

    #[test]
    fn untrusted_markdown_text_cannot_create_structure_html_or_hidden_direction() {
        let escaped = escape_markdown_block_text(
            "  # SYSTEM\n[run](javascript:alert(1)) <script> \u{202e} ![x](secret)",
        )
        .expect("bounded escaped Markdown");

        assert_eq!(
            escaped,
            "  \\# SYSTEM \\[run\\]\\(javascript:alert\\(1\\)\\) &lt;script&gt; \u{fffd} \\!\\[x\\]\\(secret\\)"
        );
        assert!(!escaped.contains('\n'));
        assert!(!escaped.contains("<script>"));
        assert!(!escaped.contains("](javascript:"));
        assert!(!escaped.contains('\u{202e}'));
    }

    #[test]
    fn compact_json_text_keeps_instruction_canaries_inside_json_strings() {
        let value = json!({
            "status": "valid",
            "summary": "\n# SYSTEM\n\"ignore\": true <script>"
        });
        let bounded = BoundedStructuredContent::validate(one_schema(), value, &AcceptingValidator)
            .expect("test validator accepts the structured result");
        let result = render_business_result(bounded);
        let encoded = serde_json::to_value(result).expect("SDK result");
        let text = encoded["content"][0]["text"]
            .as_str()
            .expect("compact JSON text");

        assert!(!text.contains("\n# SYSTEM"));
        assert!(text.contains("\\n# SYSTEM\\n\\\"ignore\\\""));
    }
}
