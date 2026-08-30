//! Bounded client-side scheduling and resumable receipts for ordinary commands.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use riffdb_client_rust::{
    ApplicationErrorCode, AttemptBudget, CallMetadata, ClientError, IdempotentCommand,
    IdempotentTransportBatchError, PublicErrorKind, RiffDbClient, generate_agent_session_id, v1,
};
use riffdb_types::hash_command_batch_document;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::app::natural_command_record;
use crate::input::{InputError, validate_path};

pub(crate) const MAX_BATCH_SOURCE_BYTES: usize = 64 * 1_048_576;
pub(crate) const MAX_BATCH_ITEM_BYTES: usize = 1_048_576;
pub(crate) const MAX_BATCH_ITEMS: usize = 4_096;
pub(crate) const MAX_BATCH_CONCURRENCY: usize = 32;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 1_024;
const MAX_ERROR_MESSAGE_BYTES: usize = 1_024;
const MAX_REPORTED_REJECTIONS: usize = 16;
const MAX_UNCHECKPOINTED_TERMINALS: usize = 16;
const MAX_TRANSPORT_BATCH_ITEMS: usize = 16;
const CHECKPOINT_SCHEMA: &str = "riffdb.command-batch-checkpoint/v1";

type BatchExecution = (BatchItem, Result<v1::ExecuteCommandResponse, ClientError>);
type TransportBatchExecution = Result<Vec<BatchExecution>, IdempotentTransportBatchError>;

#[derive(Debug)]
pub(crate) enum BatchError {
    Input(InputError),
    CheckpointInvalid,
    CheckpointContractVersionMismatch {
        checkpoint_file: String,
        checkpoint_contract_version: Option<u64>,
        requested_contract_version: Option<u64>,
    },
    CheckpointWriteFailed,
    IdentifierUnavailable,
}

#[derive(Clone, Debug)]
struct BatchItem {
    ordinal: u32,
    digest: String,
    command: IdempotentCommand,
}

#[derive(Debug)]
pub(crate) struct BatchSource {
    source_digest: String,
    items: Vec<BatchItem>,
}

/// One compiler-owned collection count constraint applied before transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CollectionInputConstraint {
    pub(crate) field: String,
    pub(crate) minimum: usize,
    pub(crate) maximum: usize,
}

impl CollectionInputConstraint {
    pub(crate) fn validate(&self, input: &serde_json::Map<String, serde_json::Value>) -> bool {
        input
            .get(&self.field)
            .and_then(serde_json::Value::as_array)
            .is_some_and(|values| (self.minimum..=self.maximum).contains(&values.len()))
    }
}

impl BatchSource {
    pub(crate) fn item_count(&self) -> usize {
        self.items.len()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BatchOptions {
    pub(crate) command_name: String,
    pub(crate) expected_contract_version: Option<u64>,
    pub(crate) concurrency: usize,
    pub(crate) idempotency_field: String,
    pub(crate) error_outcomes: BTreeSet<String>,
    pub(crate) checkpoint_path: Option<PathBuf>,
    pub(crate) progress: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct BatchReport {
    pub(crate) status: &'static str,
    pub(crate) session_id: String,
    pub(crate) command: String,
    pub(crate) total: usize,
    pub(crate) succeeded: usize,
    pub(crate) rejected: usize,
    pub(crate) pending: usize,
    pub(crate) resumed: usize,
    pub(crate) checkpoint: Option<String>,
    pub(crate) rejected_items: Vec<BatchRejectedItem>,
    pub(crate) rejected_items_truncated: usize,
}

/// One bounded, value-free terminal rejection included in the public batch summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct BatchRejectedItem {
    pub(crate) ordinal: u32,
    pub(crate) outcome: Option<String>,
    pub(crate) error_code: String,
    pub(crate) error_message: String,
}

impl BatchReport {
    pub(crate) const fn is_complete(&self) -> bool {
        self.pending == 0
    }

    pub(crate) const fn has_rejections(&self) -> bool {
        self.rejected != 0
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Checkpoint {
    schema: String,
    session_id: String,
    command: String,
    expected_contract_version: Option<u64>,
    idempotency_field: String,
    source_sha256: String,
    entries: BTreeMap<u32, ReceiptEntry>,
    checksum_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReceiptEntry {
    item_sha256: String,
    disposition: ReceiptDisposition,
    outcome: Option<String>,
    commit_sequence: Option<u64>,
    contract_version: Option<u64>,
    plan_hash: Option<String>,
    outcome_uri: Option<String>,
    provenance_uri: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReceiptDisposition {
    Succeeded,
    Rejected,
}

struct UniqueCommandInput(serde_json::Map<String, serde_json::Value>);

impl<'de> Deserialize<'de> for UniqueCommandInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueObjectVisitor;

        impl<'de> Visitor<'de> for UniqueObjectVisitor {
            type Value = UniqueCommandInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one JSON object with unique field names")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = serde_json::Map::new();
                while let Some((name, value)) = access.next_entry::<String, serde_json::Value>()? {
                    if fields.insert(name, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate command input field"));
                    }
                }
                Ok(UniqueCommandInput(fields))
            }
        }

        deserializer.deserialize_map(UniqueObjectVisitor)
    }
}

pub(crate) fn parse_source(
    source: &[u8],
    command_name: &str,
    expected_contract_version: Option<u64>,
    idempotency_field: &str,
) -> Result<BatchSource, BatchError> {
    parse_source_with_constraint(
        source,
        command_name,
        expected_contract_version,
        idempotency_field,
        None,
    )
}

pub(crate) fn parse_source_with_constraint(
    source: &[u8],
    command_name: &str,
    expected_contract_version: Option<u64>,
    idempotency_field: &str,
    collection_constraint: Option<&CollectionInputConstraint>,
) -> Result<BatchSource, BatchError> {
    if source.is_empty()
        || source.len() > MAX_BATCH_SOURCE_BYTES
        || idempotency_field.is_empty()
        || idempotency_field.len() > 256
    {
        return Err(BatchError::Input(InputError::Invalid));
    }
    let text = std::str::from_utf8(source).map_err(|_| BatchError::Input(InputError::Invalid))?;
    let mut items = Vec::new();
    let mut keys = BTreeSet::new();
    for raw_line in text.lines() {
        if raw_line.trim().is_empty() {
            continue;
        }
        if raw_line.len() > MAX_BATCH_ITEM_BYTES || items.len() == MAX_BATCH_ITEMS {
            return Err(BatchError::Input(InputError::TooLarge));
        }
        let UniqueCommandInput(input) =
            serde_json::from_str(raw_line).map_err(|_| BatchError::Input(InputError::Invalid))?;
        if collection_constraint.is_some_and(|constraint| !constraint.validate(&input)) {
            return Err(BatchError::Input(InputError::Invalid));
        }
        let key = input
            .get(idempotency_field)
            .and_then(batch_idempotency_key)
            .filter(|key| !key.is_empty() && key.len() <= MAX_IDEMPOTENCY_KEY_BYTES)
            .ok_or(BatchError::Input(InputError::Invalid))?;
        if !keys.insert(key.to_owned()) {
            return Err(BatchError::Input(InputError::Invalid));
        }
        let input =
            natural_command_record(input).map_err(|()| BatchError::Input(InputError::Invalid))?;
        let command = IdempotentCommand::new(command_name, expected_contract_version, input)
            .map_err(|_| BatchError::Input(InputError::Invalid))?;
        let ordinal =
            u32::try_from(items.len() + 1).map_err(|_| BatchError::Input(InputError::TooLarge))?;
        items.push(BatchItem {
            ordinal,
            digest: digest_hex(raw_line.as_bytes()),
            command,
        });
    }
    if items.is_empty() {
        return Err(BatchError::Input(InputError::Invalid));
    }
    Ok(BatchSource {
        source_digest: digest_hex(source),
        items,
    })
}

pub(crate) async fn execute(
    source: BatchSource,
    options: BatchOptions,
    client: RiffDbClient,
    attempts: AttemptBudget,
    metadata: CallMetadata,
) -> Result<BatchReport, BatchError> {
    if options.concurrency == 0
        || options.concurrency > MAX_BATCH_CONCURRENCY
        || options.command_name.is_empty()
        || options.command_name.len() > 256
        || options
            .error_outcomes
            .iter()
            .any(|value| value.is_empty() || value.len() > 256)
        || options.error_outcomes.len() > 64
    {
        return Err(BatchError::Input(InputError::Invalid));
    }
    let (mut checkpoint, resumed) = load_or_create_checkpoint(&source, &options)?;
    persist_checkpoint(options.checkpoint_path.as_deref(), &mut checkpoint)?;
    let already_terminal = checkpoint.entries.len();
    let mut pending = VecDeque::new();
    for item in source.items.iter().cloned() {
        match checkpoint.entries.get(&item.ordinal) {
            Some(entry) if entry.item_sha256 == item.digest => {}
            Some(_) => return Err(BatchError::CheckpointInvalid),
            None => pending.push_back(item),
        }
    }

    let mut tasks = JoinSet::new();
    fill_tasks(
        &mut tasks,
        &mut pending,
        options.concurrency,
        &client,
        attempts,
        &metadata,
    );
    let mut interrupted = false;
    let mut uncheckpointed_terminals = 0_usize;
    while !tasks.is_empty() {
        let joined = tokio::select! {
            result = tasks.join_next() => result,
            signal = tokio::signal::ctrl_c() => {
                if signal.is_ok() {
                    interrupted = true;
                    tasks.abort_all();
                    break;
                }
                tasks.join_next().await
            }
        };
        let Some(Ok(Ok(completed))) = joined else {
            interrupted = true;
            tasks.abort_all();
            break;
        };
        for (item, result) in completed {
            if let Some(entry) = receipt_entry(&item, result, &options.error_outcomes) {
                if options.progress {
                    eprintln!(
                        "batch {}/{} {}",
                        item.ordinal,
                        source.items.len(),
                        match entry.disposition {
                            ReceiptDisposition::Succeeded => "succeeded",
                            ReceiptDisposition::Rejected => "rejected",
                        }
                    );
                }
                checkpoint.entries.insert(item.ordinal, entry);
                uncheckpointed_terminals = uncheckpointed_terminals.saturating_add(1);
                if checkpoint_wave_is_full(uncheckpointed_terminals) {
                    persist_checkpoint(options.checkpoint_path.as_deref(), &mut checkpoint)?;
                    uncheckpointed_terminals = 0;
                }
            } else if options.progress {
                eprintln!("batch {}/{} pending", item.ordinal, source.items.len());
            }
        }
        fill_tasks(
            &mut tasks,
            &mut pending,
            options.concurrency,
            &client,
            attempts,
            &metadata,
        );
    }
    if interrupted {
        tasks.abort_all();
    }
    if uncheckpointed_terminals != 0 {
        persist_checkpoint(options.checkpoint_path.as_deref(), &mut checkpoint)?;
    }

    let succeeded = checkpoint
        .entries
        .values()
        .filter(|entry| entry.disposition == ReceiptDisposition::Succeeded)
        .count();
    let rejected = checkpoint
        .entries
        .values()
        .filter(|entry| entry.disposition == ReceiptDisposition::Rejected)
        .count();
    let pending_count = source
        .items
        .len()
        .saturating_sub(succeeded.saturating_add(rejected));
    let (rejected_items, rejected_items_truncated) = rejection_summary(&checkpoint);
    Ok(BatchReport {
        status: if pending_count != 0 {
            "pending"
        } else if rejected != 0 {
            "completed_with_errors"
        } else {
            "completed"
        },
        session_id: checkpoint.session_id,
        command: options.command_name,
        total: source.items.len(),
        succeeded,
        rejected,
        pending: pending_count,
        resumed: if resumed { already_terminal } else { 0 },
        checkpoint: options
            .checkpoint_path
            .as_deref()
            .map(|path| path.to_string_lossy().into_owned()),
        rejected_items,
        rejected_items_truncated,
    })
}

const fn checkpoint_wave_is_full(uncheckpointed_terminals: usize) -> bool {
    uncheckpointed_terminals >= MAX_UNCHECKPOINTED_TERMINALS
}

fn rejection_summary(checkpoint: &Checkpoint) -> (Vec<BatchRejectedItem>, usize) {
    let mut total = 0_usize;
    let mut items = Vec::new();
    for (ordinal, entry) in &checkpoint.entries {
        if entry.disposition != ReceiptDisposition::Rejected {
            continue;
        }
        total = total.saturating_add(1);
        if items.len() == MAX_REPORTED_REJECTIONS {
            continue;
        }
        items.push(BatchRejectedItem {
            ordinal: *ordinal,
            outcome: entry.outcome.clone(),
            error_code: entry
                .error_code
                .clone()
                .unwrap_or_else(|| "batch_item_rejected".to_owned()),
            error_message: entry
                .error_message
                .clone()
                .unwrap_or_else(|| "the command reached a terminal rejected result".to_owned()),
        });
    }
    (items, total.saturating_sub(MAX_REPORTED_REJECTIONS))
}

fn fill_tasks(
    tasks: &mut JoinSet<TransportBatchExecution>,
    pending: &mut VecDeque<BatchItem>,
    concurrency: usize,
    client: &RiffDbClient,
    attempts: AttemptBudget,
    metadata: &CallMetadata,
) {
    let (transport_batch_size, transport_concurrency) = transport_batch_policy(concurrency);
    while tasks.len() < transport_concurrency {
        let mut batch = Vec::with_capacity(transport_batch_size);
        while batch.len() < transport_batch_size {
            let Some(item) = pending.pop_front() else {
                break;
            };
            batch.push(item);
        }
        if batch.is_empty() {
            break;
        }
        let item_client = client.clone();
        let item_metadata = metadata.clone();
        let commands = batch.iter().map(|item| item.command.clone()).collect();
        tasks.spawn(async move {
            let results = item_client
                .execute_idempotent_transport_batch_with_retry(commands, attempts, &item_metadata)
                .await;
            results.map(|results| batch.into_iter().zip(results).collect())
        });
    }
}

fn transport_batch_policy(item_concurrency: usize) -> (usize, usize) {
    let transport_concurrency = item_concurrency.div_ceil(MAX_TRANSPORT_BATCH_ITEMS);
    let transport_batch_size = item_concurrency / transport_concurrency;
    (transport_batch_size, transport_concurrency)
}

fn receipt_entry(
    item: &BatchItem,
    result: Result<v1::ExecuteCommandResponse, ClientError>,
    error_outcomes: &BTreeSet<String>,
) -> Option<ReceiptEntry> {
    match result {
        Ok(response) => {
            let rejected = error_outcomes.contains(&response.outcome_type);
            Some(ReceiptEntry {
                item_sha256: item.digest.clone(),
                disposition: if rejected {
                    ReceiptDisposition::Rejected
                } else {
                    ReceiptDisposition::Succeeded
                },
                outcome: (!response.outcome_type.is_empty()).then_some(response.outcome_type),
                commit_sequence: (response.commit_sequence != 0)
                    .then_some(response.commit_sequence),
                contract_version: Some(response.contract_version),
                plan_hash: Some(hex_bytes(&response.plan_hash)),
                outcome_uri: response.outcome_uri,
                provenance_uri: (!response.provenance_uri.is_empty())
                    .then_some(response.provenance_uri),
                error_code: rejected.then(|| "declared_error_outcome".to_owned()),
                error_message: rejected.then(|| {
                    "the command returned an outcome classified as an import error".to_owned()
                }),
            })
        }
        Err(error) if terminal_item_error(&error) => Some(ReceiptEntry {
            item_sha256: item.digest.clone(),
            disposition: ReceiptDisposition::Rejected,
            outcome: None,
            commit_sequence: None,
            contract_version: None,
            plan_hash: None,
            outcome_uri: None,
            provenance_uri: None,
            error_code: Some(client_error_code(&error).to_owned()),
            error_message: Some(bounded_message(&error.to_string())),
        }),
        Err(_) => None,
    }
}

fn terminal_item_error(error: &ClientError) -> bool {
    if let Some(error) = error.application_error() {
        return matches!(
            error.code(),
            ApplicationErrorCode::InvalidRequest
                | ApplicationErrorCode::InputInvalid
                | ApplicationErrorCode::AuthorizationDenied
                | ApplicationErrorCode::ContractMismatch
                | ApplicationErrorCode::QueryInvalid
                | ApplicationErrorCode::QueryUnavailable
                | ApplicationErrorCode::ModuleUnavailable
                | ApplicationErrorCode::CursorInvalid
                | ApplicationErrorCode::ResponseTooLarge
                | ApplicationErrorCode::IdempotencyKeyReuse
                | ApplicationErrorCode::CommandExecutionFailed
                | ApplicationErrorCode::CapabilityRevoked
                | ApplicationErrorCode::ProtocolInvalid
        );
    }
    error.public_error().is_some_and(|error| {
        matches!(
            error.kind(),
            PublicErrorKind::Validation
                | PublicErrorKind::IdempotencyKeyReuse
                | PublicErrorKind::AuthorizationDenied
                | PublicErrorKind::ContractMismatch
                | PublicErrorKind::CommandExecutionFailed
        )
    })
}

fn client_error_code(error: &ClientError) -> &'static str {
    if let Some(error) = error.application_error() {
        return error.code().as_str();
    }
    if let Some(error) = error.public_error() {
        return error.code();
    }
    "batch_item_pending"
}

fn bounded_message(message: &str) -> String {
    if message.len() <= MAX_ERROR_MESSAGE_BYTES {
        return message.to_owned();
    }
    let mut end = MAX_ERROR_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

fn load_or_create_checkpoint(
    source: &BatchSource,
    options: &BatchOptions,
) -> Result<(Checkpoint, bool), BatchError> {
    let Some(path) = options.checkpoint_path.as_deref() else {
        return new_checkpoint(source, options).map(|checkpoint| (checkpoint, false));
    };
    validate_path(path.as_os_str()).map_err(BatchError::Input)?;
    match fs::read(path) {
        Ok(bytes) => {
            if bytes.len() > MAX_BATCH_SOURCE_BYTES {
                return Err(BatchError::CheckpointInvalid);
            }
            let checkpoint: Checkpoint =
                serde_json::from_slice(&bytes).map_err(|_| BatchError::CheckpointInvalid)?;
            if checkpoint.expected_contract_version != options.expected_contract_version {
                return Err(BatchError::CheckpointContractVersionMismatch {
                    checkpoint_file: path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("checkpoint.json")
                        .to_owned(),
                    checkpoint_contract_version: checkpoint.expected_contract_version,
                    requested_contract_version: options.expected_contract_version,
                });
            }
            validate_checkpoint(&checkpoint, source, options)?;
            Ok((checkpoint, true))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            new_checkpoint(source, options).map(|checkpoint| (checkpoint, false))
        }
        Err(_) => Err(BatchError::CheckpointInvalid),
    }
}

fn new_checkpoint(source: &BatchSource, options: &BatchOptions) -> Result<Checkpoint, BatchError> {
    let session_id = generate_agent_session_id()
        .map_err(|_| BatchError::IdentifierUnavailable)?
        .to_string();
    Ok(Checkpoint {
        schema: CHECKPOINT_SCHEMA.to_owned(),
        session_id,
        command: options.command_name.clone(),
        expected_contract_version: options.expected_contract_version,
        idempotency_field: options.idempotency_field.clone(),
        source_sha256: source.source_digest.clone(),
        entries: BTreeMap::new(),
        checksum_sha256: String::new(),
    })
}

fn validate_checkpoint(
    checkpoint: &Checkpoint,
    source: &BatchSource,
    options: &BatchOptions,
) -> Result<(), BatchError> {
    if checkpoint.schema != CHECKPOINT_SCHEMA
        || checkpoint.session_id.len() != 36
        || checkpoint.command != options.command_name
        || checkpoint.expected_contract_version != options.expected_contract_version
        || checkpoint.idempotency_field != options.idempotency_field
        || checkpoint.source_sha256 != source.source_digest
        || checkpoint.entries.len() > source.items.len()
        || checkpoint.checksum_sha256 != checkpoint_checksum(checkpoint)?
    {
        return Err(BatchError::CheckpointInvalid);
    }
    for (ordinal, entry) in &checkpoint.entries {
        let index = usize::try_from(ordinal.saturating_sub(1))
            .map_err(|_| BatchError::CheckpointInvalid)?;
        let item = source
            .items
            .get(index)
            .ok_or(BatchError::CheckpointInvalid)?;
        if item.ordinal != *ordinal || item.digest != entry.item_sha256 {
            return Err(BatchError::CheckpointInvalid);
        }
    }
    Ok(())
}

fn persist_checkpoint(path: Option<&Path>, checkpoint: &mut Checkpoint) -> Result<(), BatchError> {
    let Some(path) = path else {
        return Ok(());
    };
    checkpoint.checksum_sha256 = checkpoint_checksum(checkpoint)?;
    let bytes = serde_json::to_vec(checkpoint).map_err(|_| BatchError::CheckpointWriteFailed)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent).map_err(|_| BatchError::CheckpointWriteFailed)?;
    }
    reject_symlink(path)?;
    let temporary = temporary_path(path, &checkpoint.checksum_sha256);
    reject_symlink(&temporary)?;
    let opened = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| error.kind());
    match opened {
        Ok(mut file) => {
            file.write_all(&bytes)
                .and_then(|()| file.sync_all())
                .map_err(|_| BatchError::CheckpointWriteFailed)?;
        }
        Err(std::io::ErrorKind::AlreadyExists) => {
            reject_symlink(&temporary)?;
            let existing = fs::read(&temporary).map_err(|_| BatchError::CheckpointWriteFailed)?;
            if existing != bytes {
                return Err(BatchError::CheckpointWriteFailed);
            }
        }
        Err(_) => return Err(BatchError::CheckpointWriteFailed),
    }
    fs::rename(&temporary, path).map_err(|_| BatchError::CheckpointWriteFailed)?;
    if let Some(parent) = parent {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| BatchError::CheckpointWriteFailed)?;
    }
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), BatchError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(BatchError::CheckpointWriteFailed),
        Ok(_) | Err(_) => Ok(()),
    }
}

fn temporary_path(path: &Path, checksum: &str) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".");
    temporary.push(checksum);
    temporary.push(".tmp");
    PathBuf::from(temporary)
}

fn checkpoint_checksum(checkpoint: &Checkpoint) -> Result<String, BatchError> {
    let mut unsigned = checkpoint.clone();
    unsigned.checksum_sha256.clear();
    serde_json::to_vec(&unsigned)
        .map(|bytes| digest_hex(&bytes))
        .map_err(|_| BatchError::CheckpointInvalid)
}

/// The deduplication key for one seed line's declared idempotency input.
///
/// A command may key idempotency on any input its contract declares, so a seed
/// line carries whatever that input's type encodes to: a bare string, or a
/// tagged scalar such as `{"$uuid": "..."}`. Both identify one command
/// uniquely, so both are accepted rather than forcing a contract to declare a
/// string idempotency input purely to satisfy the seeder.
fn batch_idempotency_key(value: &serde_json::Value) -> Option<&str> {
    if let Some(text) = value.as_str() {
        return Some(text);
    }
    let tagged = value.as_object()?;
    if tagged.len() != 1 {
        return None;
    }
    let (name, inner) = tagged.iter().next()?;
    if !name.starts_with('$') {
        return None;
    }
    inner.as_str()
}

fn digest_hex(bytes: &[u8]) -> String {
    let digest = hash_command_batch_document(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> BatchOptions {
        BatchOptions {
            command_name: "CreateTicket".to_owned(),
            expected_contract_version: Some(7),
            concurrency: 8,
            idempotency_field: "idempotency_key".to_owned(),
            error_outcomes: BTreeSet::new(),
            checkpoint_path: None,
            progress: false,
        }
    }

    #[test]
    fn source_is_fully_bounded_and_rejects_duplicate_keys_before_execution() {
        let source = br#"{"idempotency_key":"seed:1","title":"one"}
{"idempotency_key":"seed:2","title":"two"}
"#;
        let parsed = parse_source(source, "CreateTicket", Some(7), "idempotency_key")
            .expect("bounded source");
        assert_eq!(parsed.items.len(), 2);
        assert_eq!(parsed.items[0].ordinal, 1);

        let duplicate = br#"{"idempotency_key":"seed:1","title":"one"}
{"idempotency_key":"seed:1","title":"two"}"#;
        assert!(matches!(
            parse_source(duplicate, "CreateTicket", Some(7), "idempotency_key"),
            Err(BatchError::Input(InputError::Invalid))
        ));

        let duplicate_field = br#"{"idempotency_key":"seed:1","title":"one","title":"two"}"#;
        assert!(matches!(
            parse_source(duplicate_field, "CreateTicket", Some(7), "idempotency_key"),
            Err(BatchError::Input(InputError::Invalid))
        ));
    }

    #[test]
    fn compiled_collection_count_is_rejected_before_batch_execution() {
        let constraint = CollectionInputConstraint {
            field: "tuples".to_owned(),
            minimum: 1,
            maximum: 2,
        };
        let empty = br#"{"idempotency_key":"seed:1","tuples":[]}"#;
        assert!(matches!(
            parse_source_with_constraint(
                empty,
                "WriteTuples",
                Some(1),
                "idempotency_key",
                Some(&constraint),
            ),
            Err(BatchError::Input(InputError::Invalid))
        ));
        let bounded = br#"{"idempotency_key":"seed:1","tuples":["a","b"]}"#;
        parse_source_with_constraint(
            bounded,
            "WriteTuples",
            Some(1),
            "idempotency_key",
            Some(&constraint),
        )
        .expect("compiled collection count");
    }

    #[test]
    fn checkpoint_is_checksummed_and_binds_exact_source_and_command() {
        let source = parse_source(
            br#"{"idempotency_key":"seed:1","title":"one"}"#,
            "CreateTicket",
            Some(7),
            "idempotency_key",
        )
        .expect("source");
        let options = options();
        let mut checkpoint = new_checkpoint(&source, &options).expect("checkpoint");
        checkpoint.checksum_sha256 = checkpoint_checksum(&checkpoint).expect("checksum");
        validate_checkpoint(&checkpoint, &source, &options).expect("valid");

        checkpoint.command = "OtherCommand".to_owned();
        assert!(matches!(
            validate_checkpoint(&checkpoint, &source, &options),
            Err(BatchError::CheckpointInvalid)
        ));
    }

    #[test]
    fn checkpoint_waves_are_bounded_and_flush_at_the_terminal_edge() {
        for completed in 0..MAX_UNCHECKPOINTED_TERMINALS {
            assert!(!checkpoint_wave_is_full(completed));
        }
        assert!(checkpoint_wave_is_full(MAX_UNCHECKPOINTED_TERMINALS));
        assert!(checkpoint_wave_is_full(MAX_UNCHECKPOINTED_TERMINALS + 1));
    }

    #[test]
    fn transport_batches_respect_both_item_and_wire_concurrency_bounds() {
        for concurrency in 1..=MAX_BATCH_CONCURRENCY {
            let (batch_size, transport_concurrency) = transport_batch_policy(concurrency);
            assert!((1..=MAX_TRANSPORT_BATCH_ITEMS).contains(&batch_size));
            assert!(transport_concurrency > 0);
            assert!(batch_size * transport_concurrency <= concurrency);
        }
        assert_eq!(transport_batch_policy(8), (8, 1));
        assert_eq!(transport_batch_policy(17), (8, 2));
        assert_eq!(transport_batch_policy(32), (16, 2));
    }

    #[test]
    fn receipt_never_serializes_command_input_or_idempotency_key() {
        let source = parse_source(
            br#"{"idempotency_key":"secret-key","title":"secret-title"}"#,
            "CreateTicket",
            Some(7),
            "idempotency_key",
        )
        .expect("source");
        let options = options();
        let mut checkpoint = new_checkpoint(&source, &options).expect("checkpoint");
        checkpoint.checksum_sha256 = checkpoint_checksum(&checkpoint).expect("checksum");
        let serialized = serde_json::to_string(&checkpoint).expect("json");
        assert!(!serialized.contains("secret-key"));
        assert!(!serialized.contains("secret-title"));
    }

    #[test]
    fn batch_summary_names_bounded_rejected_items_without_input_values() {
        let source = parse_source(
            br#"{"idempotency_key":"secret-key","price":{"$decimal":"12.34"}}"#,
            "CreateTicket",
            Some(7),
            "idempotency_key",
        )
        .expect("source");
        let mut checkpoint = new_checkpoint(&source, &options()).expect("checkpoint");
        checkpoint.entries.insert(
            1,
            ReceiptEntry {
                item_sha256: source.items[0].digest.clone(),
                disposition: ReceiptDisposition::Rejected,
                outcome: Some("InvalidPrice".to_owned()),
                commit_sequence: Some(1),
                contract_version: Some(7),
                plan_hash: Some("00".repeat(32)),
                outcome_uri: None,
                provenance_uri: None,
                error_code: Some("declared_error_outcome".to_owned()),
                error_message: Some("the command returned an import error".to_owned()),
            },
        );

        let (items, truncated) = rejection_summary(&checkpoint);
        assert_eq!(truncated, 0);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].ordinal, 1);
        assert_eq!(items[0].outcome.as_deref(), Some("InvalidPrice"));
        let serialized = serde_json::to_string(&items).expect("summary JSON");
        assert!(serialized.contains("declared_error_outcome"));
        assert!(!serialized.contains("secret-key"));
        assert!(!serialized.contains("12.34"));
    }

    #[test]
    fn batch_summary_truncation_is_explicit_and_deterministic() {
        let source = parse_source(
            br#"{"idempotency_key":"seed:1","title":"one"}"#,
            "CreateTicket",
            Some(7),
            "idempotency_key",
        )
        .expect("source");
        let mut checkpoint = new_checkpoint(&source, &options()).expect("checkpoint");
        for ordinal in 1..=MAX_REPORTED_REJECTIONS as u32 + 3 {
            checkpoint.entries.insert(
                ordinal,
                ReceiptEntry {
                    item_sha256: "00".repeat(32),
                    disposition: ReceiptDisposition::Rejected,
                    outcome: None,
                    commit_sequence: None,
                    contract_version: None,
                    plan_hash: None,
                    outcome_uri: None,
                    provenance_uri: None,
                    error_code: Some("RDB-INPUT-0101".to_owned()),
                    error_message: Some("input does not match the command schema".to_owned()),
                },
            );
        }

        let (items, truncated) = rejection_summary(&checkpoint);
        assert_eq!(items.len(), MAX_REPORTED_REJECTIONS);
        assert_eq!(items.first().map(|item| item.ordinal), Some(1));
        assert_eq!(items.last().map(|item| item.ordinal), Some(16));
        assert_eq!(truncated, 3);
    }

    #[test]
    fn partial_receipt_round_trips_and_changed_or_corrupt_source_fails_closed() {
        let source = parse_source(
            br#"{"idempotency_key":"seed:1","title":"one"}
{"idempotency_key":"seed:2","title":"two"}"#,
            "CreateTicket",
            Some(7),
            "idempotency_key",
        )
        .expect("source");
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-cli-batch-").expect("scratch directory");
        let path = scratch.path().join("riffdb-batch.json");
        let mut options = options();
        options.checkpoint_path = Some(path.clone());
        let mut checkpoint = new_checkpoint(&source, &options).expect("checkpoint");
        checkpoint.entries.insert(
            1,
            ReceiptEntry {
                item_sha256: source.items[0].digest.clone(),
                disposition: ReceiptDisposition::Succeeded,
                outcome: Some("Created".to_owned()),
                commit_sequence: Some(1),
                contract_version: Some(7),
                plan_hash: Some("00".repeat(32)),
                outcome_uri: Some("riffdb://outcome/test".to_owned()),
                provenance_uri: Some("riffdb://provenance/test".to_owned()),
                error_code: None,
                error_message: None,
            },
        );
        persist_checkpoint(Some(&path), &mut checkpoint).expect("durable receipt");
        let (loaded, resumed) =
            load_or_create_checkpoint(&source, &options).expect("resume receipt");
        assert!(resumed);
        assert_eq!(loaded.entries.len(), 1);

        let mut successor_options = options.clone();
        successor_options.expected_contract_version = Some(8);
        assert!(matches!(
            load_or_create_checkpoint(&source, &successor_options),
            Err(BatchError::CheckpointContractVersionMismatch {
                checkpoint_contract_version: Some(7),
                requested_contract_version: Some(8),
                ..
            })
        ));

        let changed = parse_source(
            br#"{"idempotency_key":"seed:1","title":"changed"}
{"idempotency_key":"seed:2","title":"two"}"#,
            "CreateTicket",
            Some(7),
            "idempotency_key",
        )
        .expect("changed source");
        assert!(matches!(
            load_or_create_checkpoint(&changed, &options),
            Err(BatchError::CheckpointInvalid)
        ));

        let mut bytes = fs::read(&path).expect("receipt bytes");
        let last = bytes.last_mut().expect("nonempty receipt");
        *last ^= 1;
        fs::write(&path, bytes).expect("corrupt test receipt");
        assert!(matches!(
            load_or_create_checkpoint(&source, &options),
            Err(BatchError::CheckpointInvalid)
        ));
        // Explicit form of what the deleted cleanup used to observe by
        // accident (remove_file on an absent receipt errored): rejecting a
        // corrupt checkpoint must not delete the operator's receipt file.
        assert!(
            path.exists(),
            "the receipt must survive the CheckpointInvalid rejection"
        );
    }
}
