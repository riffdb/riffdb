//! Typed operational digest-key custody and exact configuration documents.

use std::{error::Error, fmt, path::Path};

use riffdb_types::{
    CapabilityTokenDigest, DigestKeyId, KeyedDigest, KeyedHashDomain, hash_capability_token_secret,
    keyed_hash_secret,
};
use zeroize::Zeroizing;

use crate::RawCapabilityToken;
use crate::protected_file::read_protected_file;

/// Maximum number of current-plus-readable keys in one typed provider.
pub const MAX_READABLE_DIGEST_KEYS: usize = 8;
/// Maximum encoded size of one exact digest-key configuration document.
pub const MAX_DIGEST_KEY_DOCUMENT_BYTES: usize = 1_024;

const CAPABILITY_HEADER: &[u8] = b"riffdb-capability-digest-keys-v1\n";
const IDEMPOTENCY_HEADER: &[u8] = b"riffdb-idempotency-digest-keys-v1\n";
const DIGEST_KEY_BYTES: usize = 32;
const DIGEST_KEY_HEX_BYTES: usize = DIGEST_KEY_BYTES * 2;

/// A safe failure while parsing one exact typed digest-key document.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestKeyDocumentError {
    /// The complete document exceeds 1,024 bytes.
    TooLong,
    /// The namespace-specific first line is not exact.
    InvalidHeader,
    /// No key entry follows the header.
    MissingKey,
    /// An entry line, final line feed, key ID, or key encoding is malformed.
    InvalidEntry,
    /// More than eight entries are present.
    TooManyKeys,
    /// A nonzero key ID occurs more than once.
    DuplicateKeyId,
    /// The same 32-byte material is assigned more than once.
    DuplicateKeyMaterial,
}

impl fmt::Display for DigestKeyDocumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TooLong => "digest-key document exceeds its size limit",
            Self::InvalidHeader => "digest-key document has an invalid header",
            Self::MissingKey => "digest-key document contains no keys",
            Self::InvalidEntry => "digest-key document contains an invalid entry",
            Self::TooManyKeys => "digest-key document contains too many keys",
            Self::DuplicateKeyId => "digest-key document contains a duplicate key ID",
            Self::DuplicateKeyMaterial => {
                "digest-key document assigns the same material more than once"
            }
        })
    }
}

impl Error for DigestKeyDocumentError {}

/// One moved environment-secret document held under zeroization.
///
/// The wrapper is deliberately non-`Clone`, nonserializable, and redacted when
/// formatted. Moving a `String` or `Vec<u8>` retains its allocation rather than
/// copying the secret into a second ordinary buffer.
pub struct DigestKeyEnvironmentDocument(Zeroizing<Vec<u8>>);

impl DigestKeyEnvironmentDocument {
    /// Moves one environment string into zeroizing byte custody.
    #[must_use]
    pub fn from_string(document: String) -> Self {
        Self(Zeroizing::new(document.into_bytes()))
    }

    /// Moves one exact environment byte document into zeroizing custody.
    #[must_use]
    pub fn from_bytes(document: Vec<u8>) -> Self {
        Self(Zeroizing::new(document))
    }

    fn expose_secret(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Debug for DigestKeyEnvironmentDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DigestKeyEnvironmentDocument([REDACTED])")
    }
}

impl fmt::Display for DigestKeyEnvironmentDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("digest-key environment document [REDACTED]")
    }
}

/// A safe failure while selecting and loading one typed key-document source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestKeyProviderLoadError {
    /// Neither an environment document nor a protected path was configured.
    MissingSource,
    /// Both an environment document and a protected path were configured.
    MultipleSources,
    /// The configured protected path failed the exact ADR-0009 checks.
    ProtectedFileRejected,
    /// The selected document failed its namespace-specific exact parser.
    InvalidDocument(DigestKeyDocumentError),
}

impl fmt::Display for DigestKeyProviderLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSource => formatter.write_str("digest-key source is missing"),
            Self::MultipleSources => formatter.write_str("multiple digest-key sources configured"),
            Self::ProtectedFileRejected => {
                formatter.write_str("protected digest-key file was rejected")
            }
            Self::InvalidDocument(error) => error.fmt(formatter),
        }
    }
}

impl Error for DigestKeyProviderLoadError {}

/// A safe failure while loading and cross-checking both typed providers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestKeyProvidersLoadError {
    /// The capability-token provider could not be loaded.
    Capability(DigestKeyProviderLoadError),
    /// The idempotency provider could not be loaded.
    Idempotency(DigestKeyProviderLoadError),
    /// Both providers loaded but reuse material across namespaces.
    Namespace(DigestKeyNamespaceError),
}

impl fmt::Display for DigestKeyProvidersLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capability(error) => write!(formatter, "invalid capability key source: {error}"),
            Self::Idempotency(error) => {
                write!(formatter, "invalid idempotency key source: {error}")
            }
            Self::Namespace(error) => error.fmt(formatter),
        }
    }
}

impl Error for DigestKeyProvidersLoadError {}

struct SecretDigestKey {
    id: DigestKeyId,
    material: Zeroizing<[u8; DIGEST_KEY_BYTES]>,
}

impl fmt::Debug for SecretDigestKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretDigestKey")
            .field("id", &self.id)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// Current and readable previous capability-token digest keys.
///
/// Configuration order is retained exactly. The first entry is the only write
/// key. Owned key material is non-`Clone`, never exposed, and zeroized on drop.
pub struct CapabilityDigestKeyProvider {
    keys: Vec<SecretDigestKey>,
}

impl CapabilityDigestKeyProvider {
    /// Parses the exact capability-key document defined by ADR-0009.
    pub fn parse_document(document: &[u8]) -> Result<Self, DigestKeyDocumentError> {
        parse_document(document, CAPABILITY_HEADER).map(|keys| Self { keys })
    }

    /// Returns the current write key's nonsecret identifier.
    #[must_use]
    pub fn current_key_id(&self) -> DigestKeyId {
        self.keys[0].id
    }

    /// Iterates current then readable previous nonsecret key identifiers.
    pub fn readable_key_ids(&self) -> impl ExactSizeIterator<Item = DigestKeyId> + '_ {
        self.keys.iter().map(|key| key.id)
    }

    /// Computes the current-key digest for one already decoded raw token.
    #[must_use]
    pub fn current_digest(&self, token: &RawCapabilityToken) -> CapabilityTokenDigest {
        capability_digest(&self.keys[0], token)
    }

    /// Computes the current-key contextual-causation MAC without exposing key material.
    #[must_use]
    pub fn current_contextual_causation_mac(&self, canonical_payload: &[u8]) -> KeyedDigest {
        keyed_hash_secret(
            KeyedHashDomain::ContextualCausation,
            self.keys[0].id,
            &self.keys[0].material,
            canonical_payload,
        )
    }

    /// Checks a contextual-causation MAC against every readable key in constant work.
    #[must_use]
    pub fn matches_contextual_causation_mac(
        &self,
        canonical_payload: &[u8],
        supplied: &[u8; 32],
    ) -> bool {
        self.keys.iter().fold(false, |matched, key| {
            let expected = keyed_hash_secret(
                KeyedHashDomain::ContextualCausation,
                key.id,
                &key.material,
                canonical_payload,
            );
            let difference = expected
                .as_bytes()
                .iter()
                .zip(supplied)
                .fold(0_u8, |difference, (left, right)| {
                    difference | (left ^ right)
                });
            matched | (difference == 0)
        })
    }

    /// Consumes one validated bootstrap token and returns only its checked,
    /// bounded digest candidates.
    ///
    /// The raw token is dropped before this method returns, so downstream
    /// bootstrap handling cannot retain or recover the bearer credential.
    #[must_use]
    pub fn prepare_bootstrap_token(&self, token: RawCapabilityToken) -> BootstrapDigestCandidates {
        BootstrapDigestCandidates(self.digest_candidates(&token))
    }

    /// Computes every readable-key candidate before any match is selected.
    #[must_use]
    pub(crate) fn digest_candidates(
        &self,
        token: &RawCapabilityToken,
    ) -> CapabilityDigestCandidates {
        let digests = self
            .keys
            .iter()
            .map(|key| capability_digest(key, token))
            .collect::<Vec<_>>();
        CapabilityDigestCandidates {
            current: digests[0],
            digests,
        }
    }
}

impl fmt::Debug for CapabilityDigestKeyProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityDigestKeyProvider")
            .field("current_key_id", &self.current_key_id())
            .field("readable_key_count", &self.keys.len())
            .field("key_material", &"[REDACTED]")
            .finish()
    }
}

/// Bounded capability-token digests for every configured readable key.
pub(crate) struct CapabilityDigestCandidates {
    digests: Vec<CapabilityTokenDigest>,
    current: CapabilityTokenDigest,
}

impl CapabilityDigestCandidates {
    /// Returns candidates in exact newest-to-oldest configuration order.
    #[must_use]
    pub(crate) fn as_slice(&self) -> &[CapabilityTokenDigest] {
        &self.digests
    }

    /// Returns the candidate produced by the current write key.
    #[must_use]
    pub(crate) const fn current(&self) -> CapabilityTokenDigest {
        self.current
    }
}

impl fmt::Debug for CapabilityDigestCandidates {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityDigestCandidates")
            .field("count", &self.digests.len())
            .field("digests", &"[REDACTED]")
            .finish()
    }
}

/// Checked bootstrap-token digests for every configured readable key.
///
/// Values are privately constructed by
/// [`CapabilityDigestKeyProvider::prepare_bootstrap_token`], deliberately
/// non-`Clone`, and contain no raw bearer credential.
///
/// ```compile_fail
/// use riffdb_auth::BootstrapDigestCandidates;
///
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<BootstrapDigestCandidates>();
/// ```
pub struct BootstrapDigestCandidates(CapabilityDigestCandidates);

impl BootstrapDigestCandidates {
    /// Returns candidates in exact newest-to-oldest configuration order.
    #[must_use]
    pub fn as_slice(&self) -> &[CapabilityTokenDigest] {
        self.0.as_slice()
    }

    /// Returns the candidate produced by the current write key.
    #[must_use]
    pub const fn current(&self) -> CapabilityTokenDigest {
        self.0.current()
    }
}

impl fmt::Debug for BootstrapDigestCandidates {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BootstrapDigestCandidates")
            .field("count", &self.0.digests.len())
            .field("digests", &"[REDACTED]")
            .finish()
    }
}

/// Current and readable previous idempotency digest keys.
///
/// The provider accepts only an already canonical idempotency payload and
/// returns domain-fixed digests; operational key bytes never cross this API.
pub struct IdempotencyDigestKeyProvider {
    keys: Vec<SecretDigestKey>,
}

impl IdempotencyDigestKeyProvider {
    /// Parses the exact idempotency-key document defined by ADR-0009.
    pub fn parse_document(document: &[u8]) -> Result<Self, DigestKeyDocumentError> {
        parse_document(document, IDEMPOTENCY_HEADER).map(|keys| Self { keys })
    }

    /// Returns the current write key's nonsecret identifier.
    #[must_use]
    pub fn current_key_id(&self) -> DigestKeyId {
        self.keys[0].id
    }

    /// Iterates current then readable previous nonsecret key identifiers.
    pub fn readable_key_ids(&self) -> impl ExactSizeIterator<Item = DigestKeyId> + '_ {
        self.keys.iter().map(|key| key.id)
    }

    /// Computes the current-key digest for a canonical idempotency payload.
    #[must_use]
    pub fn current_digest(&self, canonical_payload: &[u8]) -> KeyedDigest {
        idempotency_digest(&self.keys[0], canonical_payload)
    }

    /// Computes every readable-key candidate before any match is selected.
    #[must_use]
    pub fn digest_candidates(&self, canonical_payload: &[u8]) -> IdempotencyDigestCandidates {
        let digests = self
            .keys
            .iter()
            .map(|key| idempotency_digest(key, canonical_payload))
            .collect::<Vec<_>>();
        IdempotencyDigestCandidates {
            current: digests[0],
            digests,
        }
    }
}

impl fmt::Debug for IdempotencyDigestKeyProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyDigestKeyProvider")
            .field("current_key_id", &self.current_key_id())
            .field("readable_key_count", &self.keys.len())
            .field("key_material", &"[REDACTED]")
            .finish()
    }
}

/// Bounded idempotency digests for every configured readable key.
pub struct IdempotencyDigestCandidates {
    digests: Vec<KeyedDigest>,
    current: KeyedDigest,
}

impl IdempotencyDigestCandidates {
    /// Returns candidates in exact newest-to-oldest configuration order.
    #[must_use]
    pub fn as_slice(&self) -> &[KeyedDigest] {
        &self.digests
    }

    /// Returns the candidate produced by the current write key.
    #[must_use]
    pub const fn current(&self) -> KeyedDigest {
        self.current
    }
}

impl fmt::Debug for IdempotencyDigestCandidates {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyDigestCandidates")
            .field("count", &self.digests.len())
            .field("digests", &"[REDACTED]")
            .finish()
    }
}

/// The two disjoint typed digest-key namespaces required for readiness.
pub struct DigestKeyProviders {
    capability: CapabilityDigestKeyProvider,
    idempotency: IdempotencyDigestKeyProvider,
}

impl DigestKeyProviders {
    /// Cross-checks and combines already parsed typed providers.
    pub fn new(
        capability: CapabilityDigestKeyProvider,
        idempotency: IdempotencyDigestKeyProvider,
    ) -> Result<Self, DigestKeyNamespaceError> {
        if capability.keys.iter().any(|capability_key| {
            idempotency.keys.iter().any(|idempotency_key| {
                capability_key.material.as_ref() == idempotency_key.material.as_ref()
            })
        }) {
            return Err(DigestKeyNamespaceError::ReusedKeyMaterial);
        }
        Ok(Self {
            capability,
            idempotency,
        })
    }

    /// Parses, cross-checks, and combines the two exact typed documents.
    pub fn parse_documents(
        capability_document: &[u8],
        idempotency_document: &[u8],
    ) -> Result<Self, DigestKeyProvidersError> {
        let capability = CapabilityDigestKeyProvider::parse_document(capability_document)
            .map_err(DigestKeyProvidersError::CapabilityDocument)?;
        let idempotency = IdempotencyDigestKeyProvider::parse_document(idempotency_document)
            .map_err(DigestKeyProvidersError::IdempotencyDocument)?;
        Self::new(capability, idempotency).map_err(DigestKeyProvidersError::Namespace)
    }

    /// Borrows the capability-token provider.
    #[must_use]
    pub const fn capability(&self) -> &CapabilityDigestKeyProvider {
        &self.capability
    }

    /// Borrows the idempotency provider.
    #[must_use]
    pub const fn idempotency(&self) -> &IdempotencyDigestKeyProvider {
        &self.idempotency
    }

    /// Separates the checked providers for independently typed consumers.
    #[must_use]
    pub fn into_parts(self) -> (CapabilityDigestKeyProvider, IdempotencyDigestKeyProvider) {
        (self.capability, self.idempotency)
    }
}

impl fmt::Debug for DigestKeyProviders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DigestKeyProviders")
            .field("capability", &self.capability)
            .field("idempotency", &self.idempotency)
            .finish()
    }
}

/// A safe cross-namespace digest-key configuration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestKeyNamespaceError {
    /// At least one secret key is reused across the two typed namespaces.
    ReusedKeyMaterial,
}

impl fmt::Display for DigestKeyNamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("digest-key material is reused across typed namespaces")
    }
}

impl Error for DigestKeyNamespaceError {}

/// A safe failure while parsing and combining both typed key documents.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestKeyProvidersError {
    /// The capability-token document is invalid.
    CapabilityDocument(DigestKeyDocumentError),
    /// The idempotency document is invalid.
    IdempotencyDocument(DigestKeyDocumentError),
    /// The independently valid namespaces cannot be combined.
    Namespace(DigestKeyNamespaceError),
}

impl fmt::Display for DigestKeyProvidersError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapabilityDocument(error) => {
                write!(formatter, "invalid capability keys: {error}")
            }
            Self::IdempotencyDocument(error) => {
                write!(formatter, "invalid idempotency keys: {error}")
            }
            Self::Namespace(error) => error.fmt(formatter),
        }
    }
}

impl Error for DigestKeyProvidersError {}

/// Loads capability-token keys from exactly one moved environment document or
/// protected local path. Environment bytes are placed under zeroization before
/// source selection or parsing.
pub fn load_capability_digest_key_provider(
    environment_document: Option<DigestKeyEnvironmentDocument>,
    protected_file_path: Option<&Path>,
) -> Result<CapabilityDigestKeyProvider, DigestKeyProviderLoadError> {
    load_provider(
        environment_document,
        protected_file_path,
        CapabilityDigestKeyProvider::parse_document,
    )
}

/// Loads idempotency keys from exactly one moved environment document or
/// protected local path. Environment bytes are placed under zeroization before
/// source selection or parsing.
pub fn load_idempotency_digest_key_provider(
    environment_document: Option<DigestKeyEnvironmentDocument>,
    protected_file_path: Option<&Path>,
) -> Result<IdempotencyDigestKeyProvider, DigestKeyProviderLoadError> {
    load_provider(
        environment_document,
        protected_file_path,
        IdempotencyDigestKeyProvider::parse_document,
    )
}

/// Loads both typed namespaces from their independently selected sources and
/// always cross-checks all readable material before returning either provider.
pub fn load_digest_key_providers(
    capability_environment_document: Option<DigestKeyEnvironmentDocument>,
    capability_protected_file_path: Option<&Path>,
    idempotency_environment_document: Option<DigestKeyEnvironmentDocument>,
    idempotency_protected_file_path: Option<&Path>,
) -> Result<DigestKeyProviders, DigestKeyProvidersLoadError> {
    let capability = load_capability_digest_key_provider(
        capability_environment_document,
        capability_protected_file_path,
    )
    .map_err(DigestKeyProvidersLoadError::Capability)?;
    let idempotency = load_idempotency_digest_key_provider(
        idempotency_environment_document,
        idempotency_protected_file_path,
    )
    .map_err(DigestKeyProvidersLoadError::Idempotency)?;
    DigestKeyProviders::new(capability, idempotency).map_err(DigestKeyProvidersLoadError::Namespace)
}

fn load_provider<T>(
    environment_document: Option<DigestKeyEnvironmentDocument>,
    protected_file_path: Option<&Path>,
    parse: impl FnOnce(&[u8]) -> Result<T, DigestKeyDocumentError>,
) -> Result<T, DigestKeyProviderLoadError> {
    match (environment_document, protected_file_path) {
        (None, None) => Err(DigestKeyProviderLoadError::MissingSource),
        (Some(_), Some(_)) => Err(DigestKeyProviderLoadError::MultipleSources),
        (Some(document), None) => {
            parse(document.expose_secret()).map_err(DigestKeyProviderLoadError::InvalidDocument)
        }
        (None, Some(path)) => read_protected_file(path, MAX_DIGEST_KEY_DOCUMENT_BYTES)
            .map_err(|_| DigestKeyProviderLoadError::ProtectedFileRejected)
            .and_then(|document| {
                parse(document.expose_secret()).map_err(DigestKeyProviderLoadError::InvalidDocument)
            }),
    }
}

fn capability_digest(key: &SecretDigestKey, token: &RawCapabilityToken) -> CapabilityTokenDigest {
    hash_capability_token_secret(key.id, &key.material, token.expose_secret())
}

fn idempotency_digest(key: &SecretDigestKey, payload: &[u8]) -> KeyedDigest {
    keyed_hash_secret(
        KeyedHashDomain::IdempotencyKey,
        key.id,
        &key.material,
        payload,
    )
}

fn parse_document(
    document: &[u8],
    expected_header: &[u8],
) -> Result<Vec<SecretDigestKey>, DigestKeyDocumentError> {
    if document.len() > MAX_DIGEST_KEY_DOCUMENT_BYTES {
        return Err(DigestKeyDocumentError::TooLong);
    }
    let Some(body) = document.strip_prefix(expected_header) else {
        return Err(DigestKeyDocumentError::InvalidHeader);
    };
    if body.is_empty() {
        return Err(DigestKeyDocumentError::MissingKey);
    }
    let Some(entries) = body.strip_suffix(b"\n") else {
        return Err(DigestKeyDocumentError::InvalidEntry);
    };
    if entries.is_empty() {
        return Err(DigestKeyDocumentError::MissingKey);
    }

    let mut keys = Vec::new();
    for line in entries.split(|byte| *byte == b'\n') {
        if keys.len() == MAX_READABLE_DIGEST_KEYS {
            return Err(DigestKeyDocumentError::TooManyKeys);
        }
        let key = parse_entry(line)?;
        if keys
            .iter()
            .any(|existing: &SecretDigestKey| existing.id == key.id)
        {
            return Err(DigestKeyDocumentError::DuplicateKeyId);
        }
        if keys
            .iter()
            .any(|existing| existing.material.as_ref() == key.material.as_ref())
        {
            return Err(DigestKeyDocumentError::DuplicateKeyMaterial);
        }
        keys.push(key);
    }
    Ok(keys)
}

fn parse_entry(line: &[u8]) -> Result<SecretDigestKey, DigestKeyDocumentError> {
    let Some(separator) = line.iter().position(|byte| *byte == b':') else {
        return Err(DigestKeyDocumentError::InvalidEntry);
    };
    let id = parse_key_id(&line[..separator])?;
    let encoded_key = &line[separator + 1..];
    if encoded_key.len() != DIGEST_KEY_HEX_BYTES {
        return Err(DigestKeyDocumentError::InvalidEntry);
    }

    let mut material = Zeroizing::new([0_u8; DIGEST_KEY_BYTES]);
    for (destination, pair) in material.iter_mut().zip(encoded_key.chunks_exact(2)) {
        let high = lowercase_hex_value(pair[0]).ok_or(DigestKeyDocumentError::InvalidEntry)?;
        let low = lowercase_hex_value(pair[1]).ok_or(DigestKeyDocumentError::InvalidEntry)?;
        *destination = (high << 4) | low;
    }
    Ok(SecretDigestKey { id, material })
}

fn parse_key_id(bytes: &[u8]) -> Result<DigestKeyId, DigestKeyDocumentError> {
    if bytes.is_empty() || bytes[0] == b'0' {
        return Err(DigestKeyDocumentError::InvalidEntry);
    }
    let value = bytes.iter().try_fold(0_u32, |value, byte| {
        if !byte.is_ascii_digit() {
            return None;
        }
        value.checked_mul(10)?.checked_add(u32::from(*byte - b'0'))
    });
    value
        .and_then(DigestKeyId::new)
        .ok_or(DigestKeyDocumentError::InvalidEntry)
}

const fn lowercase_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use riffdb_types::DIGEST_SCHEME_V1;

    use super::*;

    const KEY_ZERO_TO_31: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const KEY_32_TO_63: &str = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";
    const TOKEN_TEXT: &[u8] = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    fn capability_document(entries: &str) -> Vec<u8> {
        format!("riffdb-capability-digest-keys-v1\n{entries}").into_bytes()
    }

    fn idempotency_document(entries: &str) -> Vec<u8> {
        format!("riffdb-idempotency-digest-keys-v1\n{entries}").into_bytes()
    }

    #[test]
    fn typed_documents_preserve_current_and_readable_order() {
        let capability = CapabilityDigestKeyProvider::parse_document(&capability_document(
            &format!("7:{KEY_ZERO_TO_31}\n3:{KEY_32_TO_63}\n"),
        ))
        .expect("valid capability document");
        let idempotency = IdempotencyDigestKeyProvider::parse_document(&idempotency_document(
            &format!("9:{KEY_32_TO_63}\n8:{KEY_ZERO_TO_31}\n"),
        ))
        .expect("valid idempotency document");

        assert_eq!(
            capability.current_key_id(),
            DigestKeyId::new(7).expect("nonzero")
        );
        assert_eq!(
            capability.readable_key_ids().collect::<Vec<_>>(),
            vec![DigestKeyId::new(7).unwrap(), DigestKeyId::new(3).unwrap()]
        );
        assert_eq!(
            idempotency.current_key_id(),
            DigestKeyId::new(9).expect("nonzero")
        );
        assert_eq!(
            idempotency.readable_key_ids().collect::<Vec<_>>(),
            vec![DigestKeyId::new(9).unwrap(), DigestKeyId::new(8).unwrap()]
        );
        assert!(!format!("{capability:?}").contains(KEY_ZERO_TO_31));
        assert!(!format!("{idempotency:?}").contains(KEY_32_TO_63));
    }

    #[test]
    fn borrowed_secret_candidates_preserve_frozen_hmac_bytes_and_domains() {
        let capability = CapabilityDigestKeyProvider::parse_document(&capability_document(
            &format!("7:{KEY_ZERO_TO_31}\n"),
        ))
        .expect("valid capability document");
        let idempotency = IdempotencyDigestKeyProvider::parse_document(&idempotency_document(
            &format!("7:{KEY_ZERO_TO_31}\n"),
        ))
        .expect("valid idempotency document");
        let token = RawCapabilityToken::parse_canonical(TOKEN_TEXT).expect("canonical token");

        let capability_candidates = capability.digest_candidates(&token);
        let idempotency_candidates = idempotency.digest_candidates(token.expose_secret());
        assert_eq!(capability_candidates.as_slice().len(), 1);
        assert_eq!(capability_candidates.current().scheme(), DIGEST_SCHEME_V1);
        assert_eq!(
            encode_hex(capability_candidates.current().as_bytes()),
            "836b1036b35f59f04efaace126446cbf0f81ff113e6e768ae7b3c88e72a8e042"
        );
        assert_eq!(idempotency_candidates.as_slice().len(), 1);
        assert_eq!(idempotency_candidates.current().scheme(), DIGEST_SCHEME_V1);
        assert_ne!(
            capability_candidates.current().as_bytes(),
            idempotency_candidates.current().as_bytes()
        );
    }

    #[test]
    fn bootstrap_preparation_consumes_token_and_preserves_checked_key_order() {
        let keys = CapabilityDigestKeyProvider::parse_document(&capability_document(&format!(
            "7:{KEY_ZERO_TO_31}\n3:{KEY_32_TO_63}\n"
        )))
        .expect("valid capability document");
        let token = RawCapabilityToken::parse_canonical(TOKEN_TEXT).expect("canonical token");
        let expected = keys.digest_candidates(&token);
        let expected_current = keys.current_digest(&token);

        let prepared = keys.prepare_bootstrap_token(token);

        assert_eq!(prepared.as_slice(), expected.as_slice());
        assert_eq!(prepared.current(), expected_current);
        assert_eq!(prepared.current(), prepared.as_slice()[0]);
        assert_eq!(prepared.as_slice()[0].key_id().get(), 7);
        assert_eq!(prepared.as_slice()[1].key_id().get(), 3);
    }

    #[test]
    fn bootstrap_preparation_debug_is_redacted_and_value_is_move_only() {
        fn consume(candidates: BootstrapDigestCandidates) -> (usize, CapabilityTokenDigest) {
            (candidates.as_slice().len(), candidates.current())
        }

        let keys = CapabilityDigestKeyProvider::parse_document(&capability_document(&format!(
            "7:{KEY_ZERO_TO_31}\n3:{KEY_32_TO_63}\n"
        )))
        .expect("valid capability document");
        let token = RawCapabilityToken::parse_canonical(TOKEN_TEXT).expect("canonical token");
        let prepared = keys.prepare_bootstrap_token(token);
        let debug = format!("{prepared:?}");

        assert_eq!(
            debug,
            "BootstrapDigestCandidates { count: 2, digests: \"[REDACTED]\" }"
        );
        assert!(!debug.contains(std::str::from_utf8(TOKEN_TEXT).expect("ASCII token")));
        assert_eq!(consume(prepared).0, 2);
    }

    #[test]
    fn parser_rejects_nonexact_documents_and_namespace_substitution() {
        let valid_entry = format!("1:{KEY_ZERO_TO_31}\n");
        let malformed = [
            Vec::new(),
            b"riffdb-capability-digest-keys-v1\n".to_vec(),
            capability_document(&format!("01:{KEY_ZERO_TO_31}\n")),
            capability_document(&format!("0:{KEY_ZERO_TO_31}\n")),
            capability_document(&format!("4294967296:{KEY_ZERO_TO_31}\n")),
            capability_document(&format!("1:{}\n", KEY_ZERO_TO_31.to_uppercase())),
            capability_document(&format!("1:{KEY_ZERO_TO_31}\r\n")),
            capability_document(&format!("1:{KEY_ZERO_TO_31}")),
            capability_document(&format!("1:{KEY_ZERO_TO_31}\n\n")),
            idempotency_document(&valid_entry),
        ];
        for document in malformed {
            assert!(CapabilityDigestKeyProvider::parse_document(&document).is_err());
        }
    }

    #[test]
    fn parser_rejects_duplicate_ids_material_and_ninth_key() {
        let duplicate_id = capability_document(&format!("1:{KEY_ZERO_TO_31}\n1:{KEY_32_TO_63}\n"));
        assert_eq!(
            CapabilityDigestKeyProvider::parse_document(&duplicate_id).unwrap_err(),
            DigestKeyDocumentError::DuplicateKeyId
        );

        let duplicate_material =
            capability_document(&format!("1:{KEY_ZERO_TO_31}\n2:{KEY_ZERO_TO_31}\n"));
        assert_eq!(
            CapabilityDigestKeyProvider::parse_document(&duplicate_material).unwrap_err(),
            DigestKeyDocumentError::DuplicateKeyMaterial
        );

        let entries = (1_u8..=9)
            .map(|id| format!("{id}:{id:02x}{}\n", "00".repeat(31)))
            .collect::<String>();
        let exactly_eight = (1_u8..=8)
            .map(|id| format!("{id}:{id:02x}{}\n", "00".repeat(31)))
            .collect::<String>();
        assert_eq!(
            CapabilityDigestKeyProvider::parse_document(&capability_document(&exactly_eight))
                .expect("exactly eight keys")
                .readable_key_ids()
                .len(),
            MAX_READABLE_DIGEST_KEYS
        );
        assert_eq!(
            CapabilityDigestKeyProvider::parse_document(&capability_document(&entries))
                .unwrap_err(),
            DigestKeyDocumentError::TooManyKeys
        );
    }

    #[test]
    fn provider_pair_rejects_cross_namespace_material_reuse() {
        let capability = capability_document(&format!("1:{KEY_ZERO_TO_31}\n"));
        let idempotency = idempotency_document(&format!("9:{KEY_ZERO_TO_31}\n"));
        assert_eq!(
            DigestKeyProviders::parse_documents(&capability, &idempotency).unwrap_err(),
            DigestKeyProvidersError::Namespace(DigestKeyNamespaceError::ReusedKeyMaterial)
        );
    }

    #[test]
    fn exact_document_size_limit_is_enforced_before_parsing() {
        assert_eq!(
            CapabilityDigestKeyProvider::parse_document(&vec![
                b'x';
                MAX_DIGEST_KEY_DOCUMENT_BYTES + 1
            ])
            .unwrap_err(),
            DigestKeyDocumentError::TooLong
        );
    }

    #[test]
    fn typed_source_selection_requires_exactly_one_source() {
        assert_eq!(
            load_capability_digest_key_provider(
                Some(DigestKeyEnvironmentDocument::from_bytes(
                    capability_document(&format!("7:{KEY_ZERO_TO_31}\n"))
                )),
                None
            )
            .expect("environment source")
            .current_key_id(),
            DigestKeyId::new(7).expect("nonzero")
        );
        assert_eq!(
            load_capability_digest_key_provider(None, None).unwrap_err(),
            DigestKeyProviderLoadError::MissingSource
        );
        assert_eq!(
            load_capability_digest_key_provider(
                None,
                Some(Path::new("definitely-missing-digest-key-file"))
            )
            .unwrap_err(),
            DigestKeyProviderLoadError::ProtectedFileRejected
        );
        assert_eq!(
            load_capability_digest_key_provider(
                Some(DigestKeyEnvironmentDocument::from_bytes(
                    capability_document(&format!("7:{KEY_ZERO_TO_31}\n"))
                )),
                Some(Path::new("not-opened-when-both-are-configured"))
            )
            .unwrap_err(),
            DigestKeyProviderLoadError::MultipleSources
        );

        assert_eq!(
            load_capability_digest_key_provider(
                Some(DigestKeyEnvironmentDocument::from_bytes(
                    idempotency_document(&format!("7:{KEY_ZERO_TO_31}\n"))
                )),
                None
            )
            .unwrap_err(),
            DigestKeyProviderLoadError::InvalidDocument(DigestKeyDocumentError::InvalidHeader)
        );

        let canary = DigestKeyEnvironmentDocument::from_string(format!(
            "riffdb-capability-digest-keys-v1\n7:{KEY_ZERO_TO_31}\n"
        ));
        assert!(!format!("{canary:?}").contains(KEY_ZERO_TO_31));
        assert!(!canary.to_string().contains(KEY_ZERO_TO_31));
    }

    #[test]
    fn combined_loader_always_cross_checks_typed_namespaces() {
        let providers = load_digest_key_providers(
            Some(DigestKeyEnvironmentDocument::from_bytes(
                capability_document(&format!("7:{KEY_ZERO_TO_31}\n")),
            )),
            None,
            Some(DigestKeyEnvironmentDocument::from_bytes(
                idempotency_document(&format!("9:{KEY_32_TO_63}\n")),
            )),
            None,
        )
        .expect("disjoint typed providers");
        assert_eq!(
            providers.capability().current_key_id(),
            DigestKeyId::new(7).expect("nonzero")
        );
        assert_eq!(
            providers.idempotency().current_key_id(),
            DigestKeyId::new(9).expect("nonzero")
        );

        assert_eq!(
            load_digest_key_providers(
                Some(DigestKeyEnvironmentDocument::from_bytes(
                    capability_document(&format!("7:{KEY_ZERO_TO_31}\n"))
                )),
                None,
                Some(DigestKeyEnvironmentDocument::from_bytes(
                    idempotency_document(&format!("9:{KEY_ZERO_TO_31}\n"))
                )),
                None,
            )
            .unwrap_err(),
            DigestKeyProvidersLoadError::Namespace(DigestKeyNamespaceError::ReusedKeyMaterial)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn typed_provider_loads_through_the_protected_path() {
        use std::fs;
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let root = tempfile::TempDir::with_prefix("riffdb-digest-key-file-")
            .expect("create isolated test directory");
        let path = root.path().join("keys");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .expect("create private key file");
        file.write_all(&capability_document(&format!("7:{KEY_ZERO_TO_31}\n")))
            .expect("write fixed test keys");
        drop(file);

        let provider = load_capability_digest_key_provider(None, Some(&path))
            .expect("load protected typed keys");
        assert_eq!(
            provider.current_key_id(),
            DigestKeyId::new(7).expect("nonzero")
        );
    }

    fn encode_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
