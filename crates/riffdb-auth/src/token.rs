//! Canonical opaque capability-token syntax and secret ownership.

use std::{error::Error, fmt, path::Path};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use riffdb_types::CapabilityTokenDigest;
use zeroize::Zeroizing;

use crate::protected_file::read_protected_file;
use crate::{CapabilityDigestKeyProvider, EntropySource, EntropyUnavailable};

/// Exact decoded byte length of a v1 capability token.
pub const RAW_CAPABILITY_TOKEN_BYTES: usize = 32;
/// Exact canonical base64url text length of a v1 capability token.
pub const CAPABILITY_TOKEN_TEXT_BYTES: usize = 43;

/// A safe reason that capability-token text is not canonical v1 syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityTokenError {
    /// The text is not exactly 43 bytes.
    InvalidLength,
    /// The text contains a byte outside the unpadded URL-safe alphabet.
    InvalidAlphabet,
    /// The complete input does not decode to exactly 32 bytes.
    InvalidEncoding,
    /// Re-encoding the decoded bytes does not reproduce the input exactly.
    NonCanonical,
}

impl fmt::Display for CapabilityTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLength => "capability token has an invalid length",
            Self::InvalidAlphabet => "capability token has an invalid alphabet",
            Self::InvalidEncoding => "capability token has an invalid encoding",
            Self::NonCanonical => "capability token is not canonical",
        })
    }
}

impl Error for CapabilityTokenError {}

/// A safe failure while loading exact token text from a protected local file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityTokenFileError {
    /// The path did not satisfy the exact protected-file rule.
    ProtectedFileRejected,
    /// The protected file was not exactly one canonical 43-byte token.
    InvalidToken(CapabilityTokenError),
}

impl fmt::Display for CapabilityTokenFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProtectedFileRejected => {
                formatter.write_str("protected capability-token file was rejected")
            }
            Self::InvalidToken(error) => error.fmt(formatter),
        }
    }
}

impl Error for CapabilityTokenFileError {}

/// Decoded v1 bearer-token bytes.
///
/// The value is deliberately non-`Clone`, nonserializable, redacted when
/// formatted, and zeroized on drop.
pub struct RawCapabilityToken(Zeroizing<[u8; RAW_CAPABILITY_TOKEN_BYTES]>);

impl RawCapabilityToken {
    /// Strictly decodes complete canonical unpadded base64url token text.
    pub fn parse_canonical(text: &[u8]) -> Result<Self, CapabilityTokenError> {
        if text.len() != CAPABILITY_TOKEN_TEXT_BYTES {
            return Err(CapabilityTokenError::InvalidLength);
        }
        if !text.iter().copied().all(is_url_safe_base64_byte) {
            return Err(CapabilityTokenError::InvalidAlphabet);
        }

        let mut decoded = Zeroizing::new([0_u8; RAW_CAPABILITY_TOKEN_BYTES]);
        let decoded_length = URL_SAFE_NO_PAD
            .decode_slice(text, decoded.as_mut())
            .map_err(|_| CapabilityTokenError::InvalidEncoding)?;
        if decoded_length != RAW_CAPABILITY_TOKEN_BYTES {
            return Err(CapabilityTokenError::InvalidEncoding);
        }

        let encoded = encode_raw(&decoded)?;
        if encoded.expose_secret().as_slice() != text {
            return Err(CapabilityTokenError::NonCanonical);
        }
        Ok(Self(decoded))
    }

    /// Encodes this token into its canonical zeroizing text wrapper.
    pub fn encode_text(&self) -> Result<CapabilityTokenText, CapabilityTokenError> {
        encode_raw(&self.0)
    }

    pub(crate) fn expose_secret(&self) -> &[u8; RAW_CAPABILITY_TOKEN_BYTES] {
        &self.0
    }
}

impl fmt::Debug for RawCapabilityToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RawCapabilityToken([REDACTED])")
    }
}

impl fmt::Display for RawCapabilityToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("capability token [REDACTED]")
    }
}

/// Canonical 43-byte unpadded base64url bearer-token text.
///
/// The value is deliberately non-`Clone`, nonserializable, redacted when
/// formatted, and zeroized on drop.
pub struct CapabilityTokenText(Zeroizing<[u8; CAPABILITY_TOKEN_TEXT_BYTES]>);

impl CapabilityTokenText {
    /// Validates and retains exact canonical v1 token text.
    pub fn parse(text: &[u8]) -> Result<Self, CapabilityTokenError> {
        let raw = RawCapabilityToken::parse_canonical(text)?;
        raw.encode_text()
    }

    /// Explicitly borrows the credential text for its reviewed delivery path.
    #[must_use]
    pub fn expose_secret(&self) -> &[u8; CAPABILITY_TOKEN_TEXT_BYTES] {
        &self.0
    }
}

impl fmt::Debug for CapabilityTokenText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityTokenText([REDACTED])")
    }
}

impl fmt::Display for CapabilityTokenText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("capability token text [REDACTED]")
    }
}

/// A newly generated normal-create token paired with its current-key digest.
///
/// Only the canonical token text is secret; the digest remains redacted by its
/// foundational type. The value is deliberately non-`Clone`.
pub struct NewlyIssuedCapabilityToken {
    text: CapabilityTokenText,
    digest: CapabilityTokenDigest,
}

impl NewlyIssuedCapabilityToken {
    pub(crate) fn new(text: CapabilityTokenText, digest: CapabilityTokenDigest) -> Self {
        Self { text, digest }
    }

    /// Borrows the zeroizing canonical token text.
    #[must_use]
    pub const fn text(&self) -> &CapabilityTokenText {
        &self.text
    }

    /// Returns the current-key lookup digest paired with this issued token.
    #[must_use]
    pub const fn digest(&self) -> CapabilityTokenDigest {
        self.digest
    }

    /// Separates the token text from its nonsecret digest for checked consumers.
    #[must_use]
    pub fn into_parts(self) -> (CapabilityTokenText, CapabilityTokenDigest) {
        (self.text, self.digest)
    }
}

impl fmt::Debug for NewlyIssuedCapabilityToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NewlyIssuedCapabilityToken([REDACTED])")
    }
}

impl fmt::Display for NewlyIssuedCapabilityToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("newly issued capability token [REDACTED]")
    }
}

/// Generates one decoded capability token with one exact 32-byte source fill.
pub fn generate_raw_capability_token(
    entropy: &dyn EntropySource,
) -> Result<RawCapabilityToken, EntropyUnavailable> {
    let mut bytes = Zeroizing::new([0_u8; RAW_CAPABILITY_TOKEN_BYTES]);
    entropy.fill(bytes.as_mut())?;
    Ok(RawCapabilityToken(bytes))
}

/// Loads exactly 43 canonical token bytes through the ADR-0009 Linux
/// protected-file rule and returns only their decoded zeroizing wrapper.
pub fn load_capability_token_file(
    path: &Path,
) -> Result<RawCapabilityToken, CapabilityTokenFileError> {
    let document = read_protected_file(path, CAPABILITY_TOKEN_TEXT_BYTES)
        .map_err(|_| CapabilityTokenFileError::ProtectedFileRejected)?;
    RawCapabilityToken::parse_canonical(document.expose_secret())
        .map_err(CapabilityTokenFileError::InvalidToken)
}

/// Generates a normal-create token and hashes it under the provider's current
/// capability-token key without exposing key bytes.
pub fn issue_capability_token(
    entropy: &dyn EntropySource,
    keys: &CapabilityDigestKeyProvider,
) -> Result<NewlyIssuedCapabilityToken, IssueCapabilityTokenError> {
    let raw = generate_raw_capability_token(entropy)?;
    let digest = keys.current_digest(&raw);
    let text = raw.encode_text()?;
    Ok(NewlyIssuedCapabilityToken::new(text, digest))
}

/// A redaction-safe normal token-issuance failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IssueCapabilityTokenError {
    /// The entropy source could not fill the exact token buffer.
    EntropyUnavailable,
    /// The fixed-size token could not be encoded canonically.
    EncodingFailure,
}

impl fmt::Display for IssueCapabilityTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EntropyUnavailable => "capability-token entropy is unavailable",
            Self::EncodingFailure => "capability-token encoding failed",
        })
    }
}

impl Error for IssueCapabilityTokenError {}

impl From<EntropyUnavailable> for IssueCapabilityTokenError {
    fn from(_: EntropyUnavailable) -> Self {
        Self::EntropyUnavailable
    }
}

impl From<CapabilityTokenError> for IssueCapabilityTokenError {
    fn from(_: CapabilityTokenError) -> Self {
        Self::EncodingFailure
    }
}

fn encode_raw(
    raw: &[u8; RAW_CAPABILITY_TOKEN_BYTES],
) -> Result<CapabilityTokenText, CapabilityTokenError> {
    let mut encoded = Zeroizing::new([0_u8; CAPABILITY_TOKEN_TEXT_BYTES]);
    let encoded_length = URL_SAFE_NO_PAD
        .encode_slice(raw, encoded.as_mut())
        .map_err(|_| CapabilityTokenError::InvalidEncoding)?;
    if encoded_length != CAPABILITY_TOKEN_TEXT_BYTES {
        return Err(CapabilityTokenError::InvalidEncoding);
    }
    Ok(CapabilityTokenText(encoded))
}

const fn is_url_safe_base64_byte(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_')
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    const FIXED_NONCREDENTIAL_TOKEN_TEXT: &[u8; CAPABILITY_TOKEN_TEXT_BYTES] =
        b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    const FIXED_NONCREDENTIAL_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";

    #[derive(Default)]
    struct RecordingEntropy {
        calls: Mutex<Vec<usize>>,
    }

    impl EntropySource for RecordingEntropy {
        fn fill(&self, destination: &mut [u8]) -> Result<(), EntropyUnavailable> {
            self.calls
                .lock()
                .expect("test mutex")
                .push(destination.len());
            for (index, byte) in destination.iter_mut().enumerate() {
                *byte = u8::try_from(index).expect("test destination is short");
            }
            Ok(())
        }
    }

    #[test]
    fn canonical_vector_round_trips_without_secret_formatting() {
        let raw = RawCapabilityToken::parse_canonical(FIXED_NONCREDENTIAL_TOKEN_TEXT)
            .expect("canonical token");
        let text = raw.encode_text().expect("fixed-size encoding");
        assert_eq!(text.expose_secret(), FIXED_NONCREDENTIAL_TOKEN_TEXT);
        assert!(!format!("{raw:?}").contains("AAEC"));
        assert!(!format!("{text:?}").contains("AAEC"));
        assert!(!raw.to_string().contains("AAEC"));
        assert!(!text.to_string().contains("AAEC"));
    }

    #[test]
    fn parser_rejects_every_noncanonical_token_shape() {
        let mut padded = FIXED_NONCREDENTIAL_TOKEN_TEXT.to_vec();
        padded.push(b'=');
        assert!(matches!(
            RawCapabilityToken::parse_canonical(&padded),
            Err(CapabilityTokenError::InvalidLength)
        ));

        for replacement in [b'=', b'+', b'/', b' ', b'\n', 0x80] {
            let mut malformed = *FIXED_NONCREDENTIAL_TOKEN_TEXT;
            malformed[0] = replacement;
            assert!(matches!(
                RawCapabilityToken::parse_canonical(&malformed),
                Err(CapabilityTokenError::InvalidAlphabet)
            ));
        }
        assert!(matches!(
            RawCapabilityToken::parse_canonical(&FIXED_NONCREDENTIAL_TOKEN_TEXT[..42]),
            Err(CapabilityTokenError::InvalidLength)
        ));
    }

    #[test]
    fn generation_requests_one_exact_token_fill() {
        let entropy = RecordingEntropy::default();
        let token = generate_raw_capability_token(&entropy).expect("test entropy");
        assert_eq!(
            token.encode_text().expect("encoding").expose_secret(),
            FIXED_NONCREDENTIAL_TOKEN_TEXT
        );
        assert_eq!(*entropy.calls.lock().expect("test mutex"), vec![32]);
    }

    #[test]
    fn normal_issue_pairs_one_fill_with_the_current_frozen_digest() {
        let entropy = RecordingEntropy::default();
        let keys = CapabilityDigestKeyProvider::parse_document(FIXED_NONCREDENTIAL_KEY_DOCUMENT)
            .expect("fixed noncredential key document");
        let issued = issue_capability_token(&entropy, &keys).expect("issue fixed token");

        assert_eq!(
            issued.text().expose_secret(),
            FIXED_NONCREDENTIAL_TOKEN_TEXT
        );
        assert_eq!(
            issued.digest().as_bytes(),
            &[
                0x83, 0x6b, 0x10, 0x36, 0xb3, 0x5f, 0x59, 0xf0, 0x4e, 0xfa, 0xac, 0xe1, 0x26, 0x44,
                0x6c, 0xbf, 0x0f, 0x81, 0xff, 0x11, 0x3e, 0x6e, 0x76, 0x8a, 0xe7, 0xb3, 0xc8, 0x8e,
                0x72, 0xa8, 0xe0, 0x42,
            ]
        );
        assert_eq!(*entropy.calls.lock().expect("test mutex"), vec![32]);
        assert!(!format!("{issued:?}").contains("AAEC"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn normal_token_file_is_exact_and_protected() {
        use std::fs;
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let root = tempfile::TempDir::with_prefix("riffdb-token-file-")
            .expect("create isolated test directory");
        let path = root.path().join("credential");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .expect("create private token file");
        file.write_all(FIXED_NONCREDENTIAL_TOKEN_TEXT)
            .expect("write fixed token");
        drop(file);

        let token = load_capability_token_file(&path).expect("load protected token");
        assert_eq!(
            token.encode_text().expect("encode token").expose_secret(),
            FIXED_NONCREDENTIAL_TOKEN_TEXT
        );

        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("reopen private token file");
        file.write_all(FIXED_NONCREDENTIAL_TOKEN_TEXT)
            .and_then(|()| file.write_all(b"\n"))
            .expect("write newline-extended token");
        drop(file);
        assert_eq!(
            load_capability_token_file(&path).unwrap_err(),
            CapabilityTokenFileError::ProtectedFileRejected
        );
    }
}
