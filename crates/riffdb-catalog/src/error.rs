//! Closed, redaction-safe catalog failures.

use std::error::Error;
use std::fmt;

use riffdb_contract_ir::IrValidationError;
use riffdb_storage_api::{StorageError, StorageErrorKind};

/// Stable catalog failure classification.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogErrorKind {
    /// Canonical bytes do not form a complete checked bundle.
    InvalidBundle,
    /// A bundle uses an unsupported format, grammar, or executable IR version.
    UnsupportedBundleVersion,
    /// The compiler-owned command-name registry is incomplete or inconsistent.
    InvalidCommandRegistry,
    /// A successor is not activatable under the POC additive policy.
    IncompatibleContract,
    /// One immutable lineage/version identity was supplied with different bytes.
    BundleIdentityConflict,
    /// The candidate does not name the exact required active predecessor.
    ActiveCatalogMismatch,
    /// A complete historical command plan could not be resolved.
    UnknownExecutablePlan,
    /// Startup evidence was incomplete, reordered, repeated, or misbound.
    InvalidHistoricalEvidence,
    /// A persisted key does not match its exact retained key schema.
    InvalidHistoricalKey,
    /// A storage read failed before a catalog decision could be made.
    Storage,
}

impl CatalogErrorKind {
    /// Returns fixed safe text with no source, bytes, key, or contract name.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::InvalidBundle => "contract bundle is invalid",
            Self::UnsupportedBundleVersion => "contract bundle version is unsupported",
            Self::InvalidCommandRegistry => "contract command registry is invalid",
            Self::IncompatibleContract => "contract change is not activatable",
            Self::BundleIdentityConflict => "contract bundle identity conflicts",
            Self::ActiveCatalogMismatch => "active contract does not match deployment",
            Self::UnknownExecutablePlan => "historical executable plan is unavailable",
            Self::InvalidHistoricalEvidence => "catalog history evidence is invalid",
            Self::InvalidHistoricalKey => "historical persisted key is invalid",
            Self::Storage => "catalog storage read failed",
        }
    }
}

/// A redaction-safe catalog error.
#[derive(Clone, Eq, PartialEq)]
pub struct CatalogError {
    kind: CatalogErrorKind,
    storage_kind: Option<StorageErrorKind>,
}

impl CatalogError {
    /// Constructs a semantic catalog failure without retaining an internal source.
    #[must_use]
    pub const fn new(kind: CatalogErrorKind) -> Self {
        Self {
            kind,
            storage_kind: None,
        }
    }

    /// Returns the stable catalog classification.
    #[must_use]
    pub const fn kind(&self) -> CatalogErrorKind {
        self.kind
    }

    /// Returns the safe lower storage classification when applicable.
    #[must_use]
    pub const fn storage_kind(&self) -> Option<StorageErrorKind> {
        self.storage_kind
    }

    pub(crate) const fn from_ir(error: &IrValidationError) -> Self {
        let kind = match error {
            IrValidationError::UnsupportedVersion { .. } => {
                CatalogErrorKind::UnsupportedBundleVersion
            }
            _ => CatalogErrorKind::InvalidBundle,
        };
        Self::new(kind)
    }
}

impl From<StorageError> for CatalogError {
    fn from(error: StorageError) -> Self {
        Self {
            kind: CatalogErrorKind::Storage,
            storage_kind: Some(error.kind()),
        }
    }
}

impl fmt::Debug for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogError")
            .field("kind", &self.kind)
            .field("storage_kind", &self.storage_kind)
            .finish()
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for CatalogError {}
