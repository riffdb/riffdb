//! Isolated offline bootstrap credential generation and validation.
//!
//! This module has no authentication, policy, storage, capability-record, or
//! server key-provider API. Its owned token and document buffers are non-Clone,
//! redacted when formatted, and zeroized on drop.

use std::{error::Error, fmt, io::Read, path::Path};

use riffdb_types::CapabilityId;
use zeroize::Zeroizing;

use crate::generate_raw_capability_token;
use crate::protected_file::read_protected_file;
pub use crate::{
    CAPABILITY_TOKEN_TEXT_BYTES, CapabilityTokenText, EntropySource, EntropyUnavailable,
    SystemEntropy,
};

/// Exact encoded byte length of the v1 bootstrap credential document.
pub const BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES: usize = 132;
const BOOTSTRAP_CREDENTIAL_READ_LIMIT: u64 = 133;

const UUID_RANDOM_BYTES: usize = 10;
const UUID_TEXT_BYTES: usize = 36;
const HEADER: &[u8] = b"riffdb-bootstrap-credential-v1\n";
const CAPABILITY_ID_PREFIX: &[u8] = b"capability-id:";
const TOKEN_PREFIX: &[u8] = b"token:";
const CAPABILITY_ID_OFFSET: usize = HEADER.len() + CAPABILITY_ID_PREFIX.len();
const CAPABILITY_ID_LINE_END: usize = CAPABILITY_ID_OFFSET + UUID_TEXT_BYTES;
const TOKEN_PREFIX_OFFSET: usize = CAPABILITY_ID_LINE_END + 1;
const TOKEN_OFFSET: usize = TOKEN_PREFIX_OFFSET + TOKEN_PREFIX.len();
const TOKEN_LINE_END: usize = TOKEN_OFFSET + CAPABILITY_TOKEN_TEXT_BYTES;

/// One retained bootstrap capability ID and its caller-owned bearer token.
///
/// Retain this value across an uncertain RPC response. Re-rendering it never
/// regenerates either identity component.
pub struct BootstrapCredential {
    capability_id: CapabilityId,
    token: CapabilityTokenText,
}

impl BootstrapCredential {
    /// Strictly parses the exact 132-byte v1 credential document.
    pub fn parse_document(document: &[u8]) -> Result<Self, BootstrapCredentialError> {
        if document.len() != BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES {
            return Err(BootstrapCredentialError::InvalidLength);
        }
        if !document.starts_with(HEADER)
            || &document[HEADER.len()..CAPABILITY_ID_OFFSET] != CAPABILITY_ID_PREFIX
            || document[CAPABILITY_ID_LINE_END] != b'\n'
            || &document[TOKEN_PREFIX_OFFSET..TOKEN_OFFSET] != TOKEN_PREFIX
            || document[TOKEN_LINE_END] != b'\n'
        {
            return Err(BootstrapCredentialError::InvalidStructure);
        }

        let capability_id =
            parse_capability_id(&document[CAPABILITY_ID_OFFSET..CAPABILITY_ID_LINE_END])?;
        let token = CapabilityTokenText::parse(&document[TOKEN_OFFSET..TOKEN_LINE_END])
            .map_err(|_| BootstrapCredentialError::InvalidToken)?;
        Ok(Self {
            capability_id,
            token,
        })
    }

    /// Generates an offline credential from caller-supplied Unix milliseconds.
    ///
    /// The source receives one exact 10-byte UUID fill followed by one distinct
    /// exact 32-byte token fill. No clock is sampled by this module.
    pub fn generate(
        unix_milliseconds: u64,
        entropy: &dyn EntropySource,
    ) -> Result<Self, BootstrapCredentialGenerationError> {
        let mut uuid_random = Zeroizing::new([0_u8; UUID_RANDOM_BYTES]);
        entropy
            .fill(uuid_random.as_mut())
            .map_err(|_| BootstrapCredentialGenerationError::IdentifierEntropyUnavailable)?;
        let capability_id =
            CapabilityId::from_unix_milliseconds_and_random(unix_milliseconds, *uuid_random)
                .map_err(|_| BootstrapCredentialGenerationError::TimestampOutOfRange)?;

        let raw_token = generate_raw_capability_token(entropy)
            .map_err(|_| BootstrapCredentialGenerationError::TokenEntropyUnavailable)?;
        let token = raw_token
            .encode_text()
            .map_err(|_| BootstrapCredentialGenerationError::TokenEncodingFailure)?;
        let credential = Self {
            capability_id,
            token,
        };

        let document = credential.render_document();
        Self::parse_document(document.expose_secret())
            .map_err(|_| BootstrapCredentialGenerationError::DocumentValidationFailure)
    }

    /// Returns the retained caller-selected bootstrap capability ID.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Borrows the retained zeroizing token text.
    #[must_use]
    pub const fn token(&self) -> &CapabilityTokenText {
        &self.token
    }

    /// Renders the exact zeroizing 132-byte v1 document.
    #[must_use]
    pub fn render_document(&self) -> BootstrapCredentialDocument {
        let mut document = Zeroizing::new([0_u8; BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES]);
        document[..HEADER.len()].copy_from_slice(HEADER);
        document[HEADER.len()..CAPABILITY_ID_OFFSET].copy_from_slice(CAPABILITY_ID_PREFIX);
        document[CAPABILITY_ID_OFFSET..CAPABILITY_ID_LINE_END]
            .copy_from_slice(&encode_capability_id(self.capability_id));
        document[CAPABILITY_ID_LINE_END] = b'\n';
        document[TOKEN_PREFIX_OFFSET..TOKEN_OFFSET].copy_from_slice(TOKEN_PREFIX);
        document[TOKEN_OFFSET..TOKEN_LINE_END].copy_from_slice(self.token.expose_secret());
        document[TOKEN_LINE_END] = b'\n';
        BootstrapCredentialDocument(document)
    }
}

impl fmt::Debug for BootstrapCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BootstrapCredential")
            .field("capability_id", &self.capability_id)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for BootstrapCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "bootstrap credential for {} [REDACTED]",
            self.capability_id
        )
    }
}

/// An owned exact bootstrap credential document that zeroizes on drop.
pub struct BootstrapCredentialDocument(Zeroizing<[u8; BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES]>);

impl BootstrapCredentialDocument {
    /// Explicitly borrows the complete secret document for a reviewed file or
    /// standard-input delivery path.
    #[must_use]
    pub fn expose_secret(&self) -> &[u8; BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES] {
        &self.0
    }
}

impl fmt::Debug for BootstrapCredentialDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BootstrapCredentialDocument([REDACTED])")
    }
}

impl fmt::Display for BootstrapCredentialDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bootstrap credential document [REDACTED]")
    }
}

/// A safe reason an exact bootstrap credential document is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapCredentialError {
    /// The document is not exactly 132 bytes.
    InvalidLength,
    /// A fixed header, field prefix, line ending, or trailing byte is invalid.
    InvalidStructure,
    /// The capability ID is not canonical lowercase UUIDv7 text.
    InvalidCapabilityId,
    /// The token is not exact canonical v1 token text.
    InvalidToken,
}

impl fmt::Display for BootstrapCredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLength => "bootstrap credential has an invalid length",
            Self::InvalidStructure => "bootstrap credential has an invalid structure",
            Self::InvalidCapabilityId => "bootstrap credential has an invalid capability ID",
            Self::InvalidToken => "bootstrap credential has an invalid token",
        })
    }
}

impl Error for BootstrapCredentialError {}

/// A safe failure while generating an offline bootstrap credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapCredentialGenerationError {
    /// The exact UUID-randomness fill failed.
    IdentifierEntropyUnavailable,
    /// The caller-supplied millisecond value exceeds UUIDv7's 48-bit field.
    TimestampOutOfRange,
    /// The distinct exact token-randomness fill failed.
    TokenEntropyUnavailable,
    /// Fixed-size canonical token encoding failed.
    TokenEncodingFailure,
    /// The generated exact document did not pass its own strict parser.
    DocumentValidationFailure,
}

impl fmt::Display for BootstrapCredentialGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::IdentifierEntropyUnavailable => "bootstrap identifier entropy is unavailable",
            Self::TimestampOutOfRange => "bootstrap UUIDv7 timestamp is out of range",
            Self::TokenEntropyUnavailable => "bootstrap token entropy is unavailable",
            Self::TokenEncodingFailure => "bootstrap token encoding failed",
            Self::DocumentValidationFailure => "bootstrap credential validation failed",
        })
    }
}

impl Error for BootstrapCredentialGenerationError {}

/// A safe failure while reading an exact bootstrap credential stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapCredentialReadError {
    /// The reader failed before proving end of input.
    ReadFailed,
    /// EOF did not occur after exactly 132 bytes.
    InvalidLength,
    /// The exact-length document is malformed.
    InvalidCredential(BootstrapCredentialError),
}

impl fmt::Display for BootstrapCredentialReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadFailed => formatter.write_str("bootstrap credential read failed"),
            Self::InvalidLength => {
                formatter.write_str("bootstrap credential stream has an invalid length")
            }
            Self::InvalidCredential(error) => error.fmt(formatter),
        }
    }
}

impl Error for BootstrapCredentialReadError {}

/// A safe failure while loading a bootstrap credential from a protected file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapCredentialLoadError {
    /// The path did not satisfy the exact protected-file rule.
    ProtectedFileRejected,
    /// The protected file contained a malformed credential.
    InvalidCredential(BootstrapCredentialError),
}

impl fmt::Display for BootstrapCredentialLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProtectedFileRejected => {
                formatter.write_str("protected bootstrap credential file was rejected")
            }
            Self::InvalidCredential(error) => error.fmt(formatter),
        }
    }
}

impl Error for BootstrapCredentialLoadError {}

/// Generates an offline credential without sampling a clock.
pub fn generate_bootstrap_credential(
    unix_milliseconds: u64,
    entropy: &dyn EntropySource,
) -> Result<BootstrapCredential, BootstrapCredentialGenerationError> {
    BootstrapCredential::generate(unix_milliseconds, entropy)
}

/// Reads at most 133 bytes and accepts only EOF after exactly 132 bytes.
pub fn read_bootstrap_credential(
    reader: &mut dyn Read,
) -> Result<BootstrapCredential, BootstrapCredentialReadError> {
    let mut document = Zeroizing::new(Vec::with_capacity(BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES + 1));
    reader
        .take(BOOTSTRAP_CREDENTIAL_READ_LIMIT)
        .read_to_end(&mut document)
        .map_err(|_| BootstrapCredentialReadError::ReadFailed)?;
    if document.len() != BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES {
        return Err(BootstrapCredentialReadError::InvalidLength);
    }
    BootstrapCredential::parse_document(&document)
        .map_err(BootstrapCredentialReadError::InvalidCredential)
}

/// Loads and parses one exact credential through the ADR-0009 Linux
/// protected-file rule.
pub fn load_bootstrap_credential_file(
    path: &Path,
) -> Result<BootstrapCredential, BootstrapCredentialLoadError> {
    let document = read_protected_file(path, BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES)
        .map_err(|_| BootstrapCredentialLoadError::ProtectedFileRejected)?;
    BootstrapCredential::parse_document(document.expose_secret())
        .map_err(BootstrapCredentialLoadError::InvalidCredential)
}

fn parse_capability_id(bytes: &[u8]) -> Result<CapabilityId, BootstrapCredentialError> {
    if bytes.len() != UUID_TEXT_BYTES
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
    {
        return Err(BootstrapCredentialError::InvalidCapabilityId);
    }

    let mut decoded = [0_u8; 16];
    let mut source = bytes
        .iter()
        .copied()
        .filter(|byte| *byte != b'-')
        .collect::<Vec<_>>()
        .into_iter();
    for destination in &mut decoded {
        let high = source
            .next()
            .and_then(lowercase_hex_value)
            .ok_or(BootstrapCredentialError::InvalidCapabilityId)?;
        let low = source
            .next()
            .and_then(lowercase_hex_value)
            .ok_or(BootstrapCredentialError::InvalidCapabilityId)?;
        *destination = (high << 4) | low;
    }
    if source.next().is_some() {
        return Err(BootstrapCredentialError::InvalidCapabilityId);
    }
    CapabilityId::from_bytes(decoded).map_err(|_| BootstrapCredentialError::InvalidCapabilityId)
}

fn encode_capability_id(capability_id: CapabilityId) -> [u8; UUID_TEXT_BYTES] {
    const HYPHENS: [usize; 4] = [8, 13, 18, 23];
    let mut encoded = [b'-'; UUID_TEXT_BYTES];
    let mut destination = 0;
    for byte in capability_id.as_bytes() {
        while HYPHENS.contains(&destination) {
            destination += 1;
        }
        encoded[destination] = lowercase_hex_digit(byte >> 4);
        encoded[destination + 1] = lowercase_hex_digit(byte & 0x0f);
        destination += 2;
    }
    encoded
}

const fn lowercase_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

const fn lowercase_hex_digit(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        _ => b'a' + value - 10,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor},
        sync::Mutex,
    };

    use super::*;

    const FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT: &[u8;
        BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES] = b"riffdb-bootstrap-credential-v1\ncapability-id:01234567-89ab-7001-8203-040506070809\ntoken:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8\n";

    struct SequencedEntropy {
        calls: Mutex<Vec<usize>>,
        fail_call: Option<usize>,
    }

    impl SequencedEntropy {
        fn successful() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail_call: None,
            }
        }

        fn failing(call: usize) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail_call: Some(call),
            }
        }
    }

    impl EntropySource for SequencedEntropy {
        fn fill(&self, destination: &mut [u8]) -> Result<(), EntropyUnavailable> {
            let mut calls = self.calls.lock().expect("test mutex");
            calls.push(destination.len());
            for (index, byte) in destination.iter_mut().enumerate() {
                *byte = u8::try_from(index).expect("bounded test fill");
            }
            if self.fail_call == Some(calls.len()) {
                return Err(EntropyUnavailable);
            }
            Ok(())
        }
    }

    #[test]
    fn generation_uses_caller_time_and_two_distinct_exact_fills() {
        let entropy = SequencedEntropy::successful();
        let credential = generate_bootstrap_credential(0x0123_4567_89ab, &entropy)
            .expect("deterministic credential");
        assert_eq!(
            credential.capability_id().to_string(),
            "01234567-89ab-7001-8203-040506070809"
        );
        assert_eq!(
            credential.render_document().expose_secret(),
            FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT
        );
        assert_eq!(*entropy.calls.lock().expect("test mutex"), vec![10, 32]);
    }

    #[test]
    fn retained_credential_renders_retries_without_regeneration() {
        let entropy = SequencedEntropy::successful();
        let credential = BootstrapCredential::generate(0x0123_4567_89ab, &entropy)
            .expect("deterministic credential");
        let first = credential.render_document();
        let retry = credential.render_document();
        assert_eq!(first.expose_secret(), retry.expose_secret());
        assert_eq!(*entropy.calls.lock().expect("test mutex"), vec![10, 32]);
        assert!(!format!("{credential:?}").contains("AAEC"));
        assert!(!format!("{first:?}").contains("AAEC"));
    }

    #[test]
    fn exact_document_round_trips_and_rejects_mutations() {
        let parsed = BootstrapCredential::parse_document(FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT)
            .expect("exact credential document");
        assert_eq!(
            parsed.render_document().expose_secret(),
            FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT
        );

        assert!(matches!(
            BootstrapCredential::parse_document(&FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT[..131]),
            Err(BootstrapCredentialError::InvalidLength)
        ));
        let mut carriage_return = *FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT;
        carriage_return[CAPABILITY_ID_LINE_END] = b'\r';
        assert!(matches!(
            BootstrapCredential::parse_document(&carriage_return),
            Err(BootstrapCredentialError::InvalidStructure)
        ));
        let mut uppercase_uuid = *FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT;
        uppercase_uuid[CAPABILITY_ID_OFFSET + 10] = b'A';
        assert!(matches!(
            BootstrapCredential::parse_document(&uppercase_uuid),
            Err(BootstrapCredentialError::InvalidCapabilityId)
        ));
        let mut wrong_version = *FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT;
        wrong_version[CAPABILITY_ID_OFFSET + 14] = b'6';
        assert!(matches!(
            BootstrapCredential::parse_document(&wrong_version),
            Err(BootstrapCredentialError::InvalidCapabilityId)
        ));
        let mut padded_token = *FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT;
        padded_token[TOKEN_OFFSET] = b'=';
        assert!(matches!(
            BootstrapCredential::parse_document(&padded_token),
            Err(BootstrapCredentialError::InvalidToken)
        ));
    }

    #[test]
    fn each_partial_entropy_failure_returns_no_credential() {
        let identifier_failure = SequencedEntropy::failing(1);
        assert_eq!(
            BootstrapCredential::generate(0x0123_4567_89ab, &identifier_failure).unwrap_err(),
            BootstrapCredentialGenerationError::IdentifierEntropyUnavailable
        );
        assert_eq!(
            *identifier_failure.calls.lock().expect("test mutex"),
            vec![10]
        );

        let token_failure = SequencedEntropy::failing(2);
        assert_eq!(
            BootstrapCredential::generate(0x0123_4567_89ab, &token_failure).unwrap_err(),
            BootstrapCredentialGenerationError::TokenEntropyUnavailable
        );
        assert_eq!(
            *token_failure.calls.lock().expect("test mutex"),
            vec![10, 32]
        );
    }

    #[test]
    fn out_of_range_time_never_requests_token_entropy() {
        let entropy = SequencedEntropy::successful();
        assert_eq!(
            BootstrapCredential::generate(0x1_0000_0000_0000, &entropy).unwrap_err(),
            BootstrapCredentialGenerationError::TimestampOutOfRange
        );
        assert_eq!(*entropy.calls.lock().expect("test mutex"), vec![10]);
    }

    #[test]
    fn bounded_reader_requires_exact_length_and_eof() {
        let mut exact = Cursor::new(FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT.as_slice());
        assert!(read_bootstrap_credential(&mut exact).is_ok());
        assert_eq!(exact.position(), 132);

        let mut short = Cursor::new(&FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT[..131]);
        assert_eq!(
            read_bootstrap_credential(&mut short).unwrap_err(),
            BootstrapCredentialReadError::InvalidLength
        );

        let mut extended = FIXED_NONCREDENTIAL_BOOTSTRAP_DOCUMENT.to_vec();
        extended.extend_from_slice(b"ignored-after-sentinel");
        let mut extended = Cursor::new(extended);
        assert_eq!(
            read_bootstrap_credential(&mut extended).unwrap_err(),
            BootstrapCredentialReadError::InvalidLength
        );
        assert_eq!(
            extended.position(),
            133,
            "reader consumes at most limit plus one"
        );
    }

    #[test]
    fn bounded_reader_maps_io_failure_without_payload_detail() {
        struct FailingReader;

        impl Read for FailingReader {
            fn read(&mut self, _destination: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("secret-canary"))
            }
        }

        assert_eq!(
            read_bootstrap_credential(&mut FailingReader).unwrap_err(),
            BootstrapCredentialReadError::ReadFailed
        );
        assert!(
            !BootstrapCredentialReadError::ReadFailed
                .to_string()
                .contains("secret-canary")
        );
    }
}
