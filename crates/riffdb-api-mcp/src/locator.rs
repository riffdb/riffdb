use std::error::Error;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use riffdb_types::{
    ActorId, CommandId, CommitSequence, ContractLineage, ContractVersion, DigestKeyId,
    EntityTypeId, ProjectionId, ProvenanceId,
};

/// Maximum encoded length of every non-provenance MCP resource locator.
pub const MAX_MCP_RESOURCE_LOCATOR_BYTES: usize = 2_048;

/// Exact tighter maximum for a persisted-outcome resource locator.
pub const MAX_OUTCOME_RESOURCE_LOCATOR_BYTES: usize = 1_745;

const PROVENANCE_PREFIX: &str = "riffdb://provenance/";
const PROVENANCE_LOCATOR_BYTES: usize = 56;
const OUTCOME_KEY_HASH_BYTES: usize = 37;
const OUTCOME_KEY_HASH_TEXT_BYTES: usize = 50;
const DIGEST_SCHEME_V1: u8 = 1;

/// The checked structural components of an outcome key-hash segment.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutcomeKeyHash {
    key_id: DigestKeyId,
    digest: [u8; 32],
}

impl OutcomeKeyHash {
    /// Creates a v1 presentation tuple from an already checked digest identity.
    #[must_use]
    pub const fn new(key_id: DigestKeyId, digest: [u8; 32]) -> Self {
        Self { key_id, digest }
    }

    /// Returns the non-secret digest-key identifier.
    #[must_use]
    pub const fn key_id(self) -> DigestKeyId {
        self.key_id
    }

    /// Borrows the sensitive-correlation digest bytes.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    fn encode(self) -> String {
        let mut tuple = [0_u8; OUTCOME_KEY_HASH_BYTES];
        tuple[0] = DIGEST_SCHEME_V1;
        tuple[1..5].copy_from_slice(&self.key_id.to_be_bytes());
        tuple[5..].copy_from_slice(&self.digest);
        URL_SAFE_NO_PAD.encode(tuple)
    }

    fn decode(text: &str) -> Result<Self, ResourceLocatorError> {
        if text.len() != OUTCOME_KEY_HASH_TEXT_BYTES {
            return Err(ResourceLocatorError);
        }

        let mut tuple = [0_u8; OUTCOME_KEY_HASH_BYTES];
        let decoded = URL_SAFE_NO_PAD
            .decode_slice(text, &mut tuple)
            .map_err(|_| ResourceLocatorError)?;
        if decoded != OUTCOME_KEY_HASH_BYTES
            || tuple[0] != DIGEST_SCHEME_V1
            || URL_SAFE_NO_PAD.encode(tuple) != text
        {
            return Err(ResourceLocatorError);
        }

        let key_id = DigestKeyId::new(u32::from_be_bytes([tuple[1], tuple[2], tuple[3], tuple[4]]))
            .ok_or(ResourceLocatorError)?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&tuple[5..]);
        Ok(Self::new(key_id, digest))
    }
}

impl fmt::Debug for OutcomeKeyHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OutcomeKeyHash([REDACTED])")
    }
}

/// One completely parsed canonical RiffDB MCP resource locator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpResourceLocator {
    /// The active contract.
    ActiveContract,
    /// One immutable contract version.
    ContractVersion {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Nonzero contract version.
        version: ContractVersion,
    },
    /// One entity type's schema.
    EntitySchema {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Stable entity type identifier.
        entity_id: EntityTypeId,
    },
    /// One stable command plan.
    CommandPlan {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Stable command identifier.
        command_id: CommandId,
    },
    /// One stable command's generated documentation.
    CommandDocumentation {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Stable command identifier.
        command_id: CommandId,
    },
    /// One persisted command outcome.
    Outcome {
        /// Stable authenticated principal identity carried in the URI.
        principal: ActorId,
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Stable command identifier.
        command_id: CommandId,
        /// Exact compiler-owned tool-name bytes.
        tool_name: String,
        /// Checked v1 keyed-digest presentation.
        key_hash: OutcomeKeyHash,
    },
    /// One authoritative commit.
    Commit(CommitSequence),
    /// One provenance record.
    Provenance(ProvenanceId),
    /// One projection's status.
    ProjectionStatus {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Stable projection identifier.
        projection_id: ProjectionId,
    },
    /// Server health and readiness.
    ServerHealth,
}

/// Returns the exact active-contract locator.
#[must_use]
pub const fn format_active_contract_locator() -> &'static str {
    "riffdb://contract/active"
}

/// Formats one canonical contract-version locator.
#[must_use]
pub fn format_contract_version_locator(
    lineage: &ContractLineage,
    version: ContractVersion,
) -> String {
    format!(
        "riffdb://contract/{}/{}",
        encode_identity(lineage.as_bytes()),
        version
    )
}

/// Formats one canonical entity-schema locator.
#[must_use]
pub fn format_entity_schema_locator(lineage: &ContractLineage, entity_id: EntityTypeId) -> String {
    format!(
        "riffdb://entity/{}/{entity_id}/schema",
        encode_identity(lineage.as_bytes())
    )
}

/// Formats one canonical command-plan locator.
#[must_use]
pub fn format_command_plan_locator(lineage: &ContractLineage, command_id: CommandId) -> String {
    format!(
        "riffdb://command/{}/{command_id}/plan",
        encode_identity(lineage.as_bytes())
    )
}

/// Formats one canonical command-documentation locator.
#[must_use]
pub fn format_command_documentation_locator(
    lineage: &ContractLineage,
    command_id: CommandId,
) -> String {
    format!(
        "riffdb://command/{}/{command_id}/docs",
        encode_identity(lineage.as_bytes())
    )
}

/// Formats one canonical persisted-outcome locator.
pub fn format_outcome_locator(
    principal: &ActorId,
    lineage: &ContractLineage,
    command_id: CommandId,
    tool_name: &str,
    key_hash: OutcomeKeyHash,
) -> Result<String, ResourceLocatorError> {
    validate_command_tool_name(tool_name)?;
    let locator = format!(
        "riffdb://outcome/{}/{}/{command_id}/{tool_name}/{}",
        encode_identity(principal.as_str().as_bytes()),
        encode_identity(lineage.as_bytes()),
        key_hash.encode()
    );
    if locator.len() > MAX_OUTCOME_RESOURCE_LOCATOR_BYTES {
        return Err(ResourceLocatorError);
    }
    Ok(locator)
}

/// Formats one canonical commit locator.
#[must_use]
pub fn format_commit_locator(sequence: CommitSequence) -> String {
    format!("riffdb://commit/{sequence}")
}

/// Formats the exact ADR-0024 provenance locator.
#[must_use]
pub fn format_provenance_locator(provenance_id: ProvenanceId) -> String {
    format!("{PROVENANCE_PREFIX}{provenance_id}")
}

/// Formats one canonical projection-status locator.
#[must_use]
pub fn format_projection_status_locator(
    lineage: &ContractLineage,
    projection_id: ProjectionId,
) -> String {
    format!(
        "riffdb://projection/{}/{projection_id}/status",
        encode_identity(lineage.as_bytes())
    )
}

/// Returns the exact server-health locator.
#[must_use]
pub const fn format_server_health_locator() -> &'static str {
    "riffdb://server/health"
}

/// Formats a contract-version locator from checked public-wire scalar parts.
pub fn format_contract_version_locator_from_public(
    lineage: &str,
    version: u64,
) -> Result<String, ResourceLocatorError> {
    Ok(format_contract_version_locator(
        &lineage_from_public(lineage)?,
        ContractVersion::new(version).ok_or(ResourceLocatorError)?,
    ))
}

/// Formats an entity-schema locator from checked public-wire scalar parts.
pub fn format_entity_schema_locator_from_public(
    lineage: &str,
    entity_id: u32,
) -> Result<String, ResourceLocatorError> {
    Ok(format_entity_schema_locator(
        &lineage_from_public(lineage)?,
        EntityTypeId::new(entity_id).ok_or(ResourceLocatorError)?,
    ))
}

/// Formats a command-plan locator from checked public-wire scalar parts.
pub fn format_command_plan_locator_from_public(
    lineage: &str,
    command_id: u32,
) -> Result<String, ResourceLocatorError> {
    Ok(format_command_plan_locator(
        &lineage_from_public(lineage)?,
        CommandId::new(command_id).ok_or(ResourceLocatorError)?,
    ))
}

/// Formats a command-documentation locator from checked public-wire scalar parts.
pub fn format_command_documentation_locator_from_public(
    lineage: &str,
    command_id: u32,
) -> Result<String, ResourceLocatorError> {
    Ok(format_command_documentation_locator(
        &lineage_from_public(lineage)?,
        CommandId::new(command_id).ok_or(ResourceLocatorError)?,
    ))
}

/// Formats an exact commit locator from one checked public-wire sequence.
pub fn format_commit_locator_from_public(sequence: u64) -> Result<String, ResourceLocatorError> {
    Ok(format_commit_locator(
        CommitSequence::new(sequence).ok_or(ResourceLocatorError)?,
    ))
}

/// Returns the exact commit class-template locator.
#[must_use]
pub const fn format_commit_template_locator() -> &'static str {
    "riffdb://commit/{sequence}"
}

/// Formats an exact provenance locator from checked public UUIDv7 bytes.
pub fn format_provenance_locator_from_public(
    provenance_id: &[u8],
) -> Result<String, ResourceLocatorError> {
    let provenance_id: [u8; 16] = provenance_id.try_into().map_err(|_| ResourceLocatorError)?;
    Ok(format_provenance_locator(
        ProvenanceId::from_bytes(provenance_id).map_err(|_| ResourceLocatorError)?,
    ))
}

/// Returns the exact provenance class-template locator.
#[must_use]
pub const fn format_provenance_template_locator() -> &'static str {
    "riffdb://provenance/{provenance_id}"
}

/// Formats a projection-status locator from checked public-wire scalar parts.
pub fn format_projection_status_locator_from_public(
    lineage: &str,
    projection_id: u32,
) -> Result<String, ResourceLocatorError> {
    Ok(format_projection_status_locator(
        &lineage_from_public(lineage)?,
        ProjectionId::new(projection_id).ok_or(ResourceLocatorError)?,
    ))
}

/// Formats the persisted-outcome URI template advertised by discovery.
pub fn format_outcome_template_locator_from_public(
    lineage: &str,
    command_id: u32,
    tool_name: &str,
) -> Result<String, ResourceLocatorError> {
    validate_command_tool_name(tool_name)?;
    let lineage = lineage_from_public(lineage)?;
    let command_id = CommandId::new(command_id).ok_or(ResourceLocatorError)?;
    let locator = format!(
        "riffdb://outcome/{{principal}}/{}/{command_id}/{tool_name}/{{key_hash}}",
        encode_identity(lineage.as_bytes()),
    );
    if locator.len() > MAX_OUTCOME_RESOURCE_LOCATOR_BYTES {
        return Err(ResourceLocatorError);
    }
    Ok(locator)
}

/// Formats one exact persisted-outcome locator from public-wire parts.
pub fn format_outcome_locator_from_public(
    principal: &str,
    lineage: &str,
    command_id: u32,
    tool_name: &str,
    digest_key_id: u32,
    digest: [u8; 32],
) -> Result<String, ResourceLocatorError> {
    format_outcome_locator(
        &ActorId::new(principal.to_owned()).map_err(|_| ResourceLocatorError)?,
        &lineage_from_public(lineage)?,
        CommandId::new(command_id).ok_or(ResourceLocatorError)?,
        tool_name,
        OutcomeKeyHash::new(
            DigestKeyId::new(digest_key_id).ok_or(ResourceLocatorError)?,
            digest,
        ),
    )
}

/// Parses a resource locator without normalizing any alternate spelling.
pub fn parse_resource_locator(text: &str) -> Result<McpResourceLocator, ResourceLocatorError> {
    if text.is_empty() || text.len() > MAX_MCP_RESOURCE_LOCATOR_BYTES {
        return Err(ResourceLocatorError);
    }
    if text.len() == PROVENANCE_LOCATOR_BYTES && text.starts_with(PROVENANCE_PREFIX) {
        return parse_provenance(text);
    }

    let remainder = text.strip_prefix("riffdb://").ok_or(ResourceLocatorError)?;
    let mut segments = remainder.split('/');
    let authority = segments.next().ok_or(ResourceLocatorError)?;
    let path: Vec<&str> = segments.collect();
    if authority.is_empty() || path.iter().any(|segment| segment.is_empty()) {
        return Err(ResourceLocatorError);
    }

    let locator = match (authority, path.as_slice()) {
        ("contract", ["active"]) => McpResourceLocator::ActiveContract,
        ("contract", [lineage, version]) => McpResourceLocator::ContractVersion {
            lineage: parse_lineage(lineage)?,
            version: ContractVersion::new(parse_u64(version)?).ok_or(ResourceLocatorError)?,
        },
        ("entity", [lineage, entity_id, "schema"]) => McpResourceLocator::EntitySchema {
            lineage: parse_lineage(lineage)?,
            entity_id: EntityTypeId::new(parse_u32(entity_id)?).ok_or(ResourceLocatorError)?,
        },
        ("command", [lineage, command_id, "plan"]) => McpResourceLocator::CommandPlan {
            lineage: parse_lineage(lineage)?,
            command_id: CommandId::new(parse_u32(command_id)?).ok_or(ResourceLocatorError)?,
        },
        ("command", [lineage, command_id, "docs"]) => McpResourceLocator::CommandDocumentation {
            lineage: parse_lineage(lineage)?,
            command_id: CommandId::new(parse_u32(command_id)?).ok_or(ResourceLocatorError)?,
        },
        ("outcome", [principal, lineage, command_id, tool_name, key_hash]) => {
            if text.len() > MAX_OUTCOME_RESOURCE_LOCATOR_BYTES {
                return Err(ResourceLocatorError);
            }
            validate_command_tool_name(tool_name)?;
            McpResourceLocator::Outcome {
                principal: ActorId::new(decode_identity(principal)?)
                    .map_err(|_| ResourceLocatorError)?,
                lineage: parse_lineage(lineage)?,
                command_id: CommandId::new(parse_u32(command_id)?).ok_or(ResourceLocatorError)?,
                tool_name: (*tool_name).to_owned(),
                key_hash: OutcomeKeyHash::decode(key_hash)?,
            }
        }
        ("commit", [sequence]) => McpResourceLocator::Commit(
            CommitSequence::new(parse_u64(sequence)?).ok_or(ResourceLocatorError)?,
        ),
        ("projection", [lineage, projection_id, "status"]) => {
            McpResourceLocator::ProjectionStatus {
                lineage: parse_lineage(lineage)?,
                projection_id: ProjectionId::new(parse_u32(projection_id)?)
                    .ok_or(ResourceLocatorError)?,
            }
        }
        ("server", ["health"]) => McpResourceLocator::ServerHealth,
        _ => return Err(ResourceLocatorError),
    };

    if format_parsed_locator(&locator)? != text {
        return Err(ResourceLocatorError);
    }
    Ok(locator)
}

fn format_parsed_locator(locator: &McpResourceLocator) -> Result<String, ResourceLocatorError> {
    match locator {
        McpResourceLocator::ActiveContract => Ok(format_active_contract_locator().to_owned()),
        McpResourceLocator::ContractVersion { lineage, version } => {
            Ok(format_contract_version_locator(lineage, *version))
        }
        McpResourceLocator::EntitySchema { lineage, entity_id } => {
            Ok(format_entity_schema_locator(lineage, *entity_id))
        }
        McpResourceLocator::CommandPlan {
            lineage,
            command_id,
        } => Ok(format_command_plan_locator(lineage, *command_id)),
        McpResourceLocator::CommandDocumentation {
            lineage,
            command_id,
        } => Ok(format_command_documentation_locator(lineage, *command_id)),
        McpResourceLocator::Outcome {
            principal,
            lineage,
            command_id,
            tool_name,
            key_hash,
        } => format_outcome_locator(principal, lineage, *command_id, tool_name, *key_hash),
        McpResourceLocator::Commit(sequence) => Ok(format_commit_locator(*sequence)),
        McpResourceLocator::Provenance(provenance_id) => {
            Ok(format_provenance_locator(*provenance_id))
        }
        McpResourceLocator::ProjectionStatus {
            lineage,
            projection_id,
        } => Ok(format_projection_status_locator(lineage, *projection_id)),
        McpResourceLocator::ServerHealth => Ok(format_server_health_locator().to_owned()),
    }
}

fn parse_provenance(text: &str) -> Result<McpResourceLocator, ResourceLocatorError> {
    let uuid = text
        .strip_prefix(PROVENANCE_PREFIX)
        .ok_or(ResourceLocatorError)?;
    if uuid.len() != 36
        || uuid.as_bytes()[8] != b'-'
        || uuid.as_bytes()[13] != b'-'
        || uuid.as_bytes()[18] != b'-'
        || uuid.as_bytes()[23] != b'-'
    {
        return Err(ResourceLocatorError);
    }

    let mut bytes = [0_u8; 16];
    let mut source_index = 0;
    let mut target_index = 0;
    while source_index < uuid.len() {
        if matches!(source_index, 8 | 13 | 18 | 23) {
            source_index += 1;
            continue;
        }
        let source = uuid.as_bytes();
        let high = lowercase_hex(source[source_index]).ok_or(ResourceLocatorError)?;
        let low = lowercase_hex(source[source_index + 1]).ok_or(ResourceLocatorError)?;
        bytes[target_index] = (high << 4) | low;
        source_index += 2;
        target_index += 1;
    }

    let provenance_id = ProvenanceId::from_bytes(bytes).map_err(|_| ResourceLocatorError)?;
    if format_provenance_locator(provenance_id) != text {
        return Err(ResourceLocatorError);
    }
    Ok(McpResourceLocator::Provenance(provenance_id))
}

fn parse_lineage(text: &str) -> Result<ContractLineage, ResourceLocatorError> {
    ContractLineage::new(decode_identity(text)?).map_err(|_| ResourceLocatorError)
}

fn lineage_from_public(text: &str) -> Result<ContractLineage, ResourceLocatorError> {
    ContractLineage::new(text.to_owned()).map_err(|_| ResourceLocatorError)
}

fn encode_identity(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(bytes.len());
    for &byte in bytes {
        if is_unreserved(byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}

fn decode_identity(segment: &str) -> Result<String, ResourceLocatorError> {
    if segment.is_empty() {
        return Err(ResourceLocatorError);
    }

    let source = segment.as_bytes();
    let mut decoded = Vec::with_capacity(source.len());
    let mut index = 0;
    while index < source.len() {
        if source[index] == b'%' {
            if index + 2 >= source.len() {
                return Err(ResourceLocatorError);
            }
            let high = uppercase_hex(source[index + 1]).ok_or(ResourceLocatorError)?;
            let low = uppercase_hex(source[index + 2]).ok_or(ResourceLocatorError)?;
            let byte = (high << 4) | low;
            if is_unreserved(byte) {
                return Err(ResourceLocatorError);
            }
            decoded.push(byte);
            index += 3;
        } else {
            if !is_unreserved(source[index]) {
                return Err(ResourceLocatorError);
            }
            decoded.push(source[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| ResourceLocatorError)
}

const fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn parse_u32(text: &str) -> Result<u32, ResourceLocatorError> {
    validate_decimal(text)?;
    text.parse().map_err(|_| ResourceLocatorError)
}

fn parse_u64(text: &str) -> Result<u64, ResourceLocatorError> {
    validate_decimal(text)?;
    text.parse().map_err(|_| ResourceLocatorError)
}

fn validate_decimal(text: &str) -> Result<(), ResourceLocatorError> {
    if text.is_empty()
        || text == "0"
        || text.starts_with('0')
        || !text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ResourceLocatorError);
    }
    Ok(())
}

/// Validates the exact compiler-owned ADR-0020 command tool-name grammar.
///
/// The check never normalizes, derives, or suffixes a name.
pub fn validate_command_tool_name(tool_name: &str) -> Result<(), ResourceLocatorError> {
    if tool_name.len() > 128 {
        return Err(ResourceLocatorError);
    }
    let remainder = tool_name
        .strip_prefix("riffdb.cmd.")
        .ok_or(ResourceLocatorError)?;
    let (contract, command) = remainder.split_once('.').ok_or(ResourceLocatorError)?;
    if command.contains('.') || !valid_tool_segment(contract) || !valid_tool_segment(command) {
        return Err(ResourceLocatorError);
    }
    Ok(())
}

fn valid_tool_segment(segment: &str) -> bool {
    let mut bytes = segment.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

const fn lowercase_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

const fn uppercase_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// A resource locator was malformed, noncanonical, oversized, or structurally invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceLocatorError;

impl fmt::Display for ResourceLocatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RiffDB resource locator is invalid")
    }
}

impl Error for ResourceLocatorError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn lineage() -> ContractLineage {
        ContractLineage::new("Legal Spend/POC").expect("fixture lineage is bounded")
    }

    fn actor() -> ActorId {
        ActorId::new("operator@example.com").expect("fixture actor is bounded")
    }

    fn provenance() -> ProvenanceId {
        ProvenanceId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x23,
        ])
        .expect("fixture is UUIDv7")
    }

    #[test]
    fn all_concrete_locator_kinds_round_trip() {
        let lineage = lineage();
        let key_hash =
            OutcomeKeyHash::new(DigestKeyId::new(7).expect("key id is nonzero"), [0x42; 32]);
        let locators = [
            format_active_contract_locator().to_owned(),
            format_contract_version_locator(
                &lineage,
                ContractVersion::new(3).expect("version is nonzero"),
            ),
            format_entity_schema_locator(
                &lineage,
                EntityTypeId::new(4).expect("entity ID is nonzero"),
            ),
            format_command_plan_locator(
                &lineage,
                CommandId::new(5).expect("command ID is nonzero"),
            ),
            format_command_documentation_locator(
                &lineage,
                CommandId::new(5).expect("command ID is nonzero"),
            ),
            format_outcome_locator(
                &actor(),
                &lineage,
                CommandId::new(5).expect("command ID is nonzero"),
                "riffdb.cmd.legalspend.allocatebudget",
                key_hash,
            )
            .expect("outcome locator is valid"),
            format_commit_locator(CommitSequence::new(8).expect("sequence is nonzero")),
            format_provenance_locator(provenance()),
            format_projection_status_locator(
                &lineage,
                ProjectionId::new(9).expect("projection ID is nonzero"),
            ),
            format_server_health_locator().to_owned(),
        ];

        for locator in locators {
            let parsed = parse_resource_locator(&locator).expect("producer emits canonical text");
            assert_eq!(
                format_parsed_locator(&parsed).expect("parsed locator formats"),
                locator
            );
        }
    }

    #[test]
    fn public_part_formatters_share_the_canonical_locator_owner() {
        assert_eq!(
            format_contract_version_locator_from_public("LegalSpend", 3),
            Ok("riffdb://contract/LegalSpend/3".to_owned())
        );
        assert_eq!(
            format_entity_schema_locator_from_public("LegalSpend", 4),
            Ok("riffdb://entity/LegalSpend/4/schema".to_owned())
        );
        assert_eq!(
            format_command_plan_locator_from_public("LegalSpend", 5),
            Ok("riffdb://command/LegalSpend/5/plan".to_owned())
        );
        assert_eq!(
            format_command_documentation_locator_from_public("LegalSpend", 5),
            Ok("riffdb://command/LegalSpend/5/docs".to_owned())
        );
        assert_eq!(
            format_commit_locator_from_public(8),
            Ok("riffdb://commit/8".to_owned())
        );
        assert_eq!(
            format_provenance_locator_from_public(provenance().as_bytes()),
            Ok(format_provenance_locator(provenance()))
        );
        assert_eq!(
            format_projection_status_locator_from_public("LegalSpend", 6),
            Ok("riffdb://projection/LegalSpend/6/status".to_owned())
        );
        assert_eq!(
            format_outcome_template_locator_from_public(
                "LegalSpend",
                5,
                "riffdb.cmd.legalspend.allocatebudget",
            ),
            Ok(
                "riffdb://outcome/{principal}/LegalSpend/5/riffdb.cmd.legalspend.allocatebudget/{key_hash}"
                    .to_owned()
            )
        );
        assert_eq!(
            format_outcome_locator_from_public(
                "operator",
                "LegalSpend",
                5,
                "riffdb.cmd.legalspend.allocatebudget",
                7,
                [0x42; 32],
            ),
            format_outcome_locator(
                &ActorId::new("operator").expect("actor"),
                &ContractLineage::new("LegalSpend").expect("lineage"),
                CommandId::new(5).expect("command"),
                "riffdb.cmd.legalspend.allocatebudget",
                OutcomeKeyHash::new(DigestKeyId::new(7).expect("key"), [0x42; 32]),
            )
        );
        assert_eq!(
            format_commit_template_locator(),
            "riffdb://commit/{sequence}"
        );
        assert_eq!(
            format_provenance_template_locator(),
            "riffdb://provenance/{provenance_id}"
        );

        assert_eq!(
            format_command_plan_locator_from_public("LegalSpend", 0),
            Err(ResourceLocatorError)
        );
        assert_eq!(
            format_provenance_locator_from_public(&[0_u8; 16]),
            Err(ResourceLocatorError)
        );
    }

    #[test]
    fn provenance_golden_is_exact() {
        let text = format_provenance_locator(provenance());
        assert_eq!(
            text,
            "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23"
        );
        assert_eq!(text.len(), PROVENANCE_LOCATOR_BYTES);
    }

    #[test]
    fn outcome_key_hash_golden_is_exact() {
        let hash = OutcomeKeyHash::new(DigestKeyId::new(1).expect("key ID is nonzero"), [0; 32]);
        assert_eq!(
            hash.encode(),
            "AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(
            OutcomeKeyHash::decode(&hash.encode()).expect("golden decodes"),
            hash
        );
        assert_eq!(format!("{hash:?}"), "OutcomeKeyHash([REDACTED])");
    }

    #[test]
    fn noncanonical_locators_fail_closed() {
        for invalid in [
            "RIFFDB://server/health",
            "riffdb://server/health/",
            "riffdb://server/health?full=true",
            "riffdb://contract//1",
            "riffdb://contract/LegalSpend/01",
            "riffdb://contract/Legal%53pend/1",
            "riffdb://contract/Legal%2fSpend/1",
            "riffdb://contract/Legal Spend/1",
            "riffdb://commit/+1",
            "riffdb://commit/0",
            "riffdb://provenance/019BF6AA-A640-7DE6-89C9-8A7F70BBBD23",
            "riffdb://provenance/019bf6aa-a640-6de6-89c9-8a7f70bbbd23",
            "riffdb://command/LegalSpend/1/unknown",
            "riffdb://outcome/operator/LegalSpend/2/RIFFDB.cmd.a.b/AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "riffdb://outcome/operator/LegalSpend/2/riffdb.cmd.a.b/AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        ] {
            assert_eq!(
                parse_resource_locator(invalid),
                Err(ResourceLocatorError),
                "{invalid}"
            );
        }
    }
}
