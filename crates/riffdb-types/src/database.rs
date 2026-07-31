//! Operator-visible database routing identities.

use std::error::Error;
use std::fmt;

/// Maximum number of databases one RiffDB process may host.
pub const MAX_DATABASES_PER_PROCESS: usize = 32;
/// Maximum bytes in one canonical database alias.
pub const MAX_DATABASE_ALIAS_BYTES: usize = 64;
/// Compatibility alias assigned to the legacy single-database configuration.
pub const DEFAULT_DATABASE_ALIAS: &str = "default";

/// A canonical, non-secret database selector.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatabaseAlias(String);

impl DatabaseAlias {
    /// Validates an exact lowercase ASCII database alias.
    pub fn new(value: impl Into<String>) -> Result<Self, DatabaseAliasError> {
        let value = value.into();
        let bytes = value.as_bytes();
        if bytes.is_empty() {
            return Err(DatabaseAliasError::Empty);
        }
        if bytes.len() > MAX_DATABASE_ALIAS_BYTES {
            return Err(DatabaseAliasError::TooLong {
                actual: bytes.len(),
                maximum: MAX_DATABASE_ALIAS_BYTES,
            });
        }
        if !bytes[0].is_ascii_lowercase() {
            return Err(DatabaseAliasError::InvalidCharacter { index: 0 });
        }
        if let Some(index) = bytes.iter().position(|byte| {
            !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && !matches!(byte, b'_' | b'-')
        }) {
            return Err(DatabaseAliasError::InvalidCharacter { index });
        }
        Ok(Self(value))
    }

    /// Returns the legacy single-database alias.
    #[must_use]
    pub fn default_alias() -> Self {
        Self(DEFAULT_DATABASE_ALIAS.to_owned())
    }

    /// Borrows the exact canonical selector.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrows the exact canonical ASCII bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for DatabaseAlias {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DatabaseAlias")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for DatabaseAlias {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A database alias did not match the public canonical grammar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseAliasError {
    /// The alias was empty.
    Empty,
    /// The alias exceeded its byte bound.
    TooLong {
        /// Submitted byte length.
        actual: usize,
        /// Accepted byte length.
        maximum: usize,
    },
    /// One byte violated `[a-z][a-z0-9_-]{0,63}`.
    InvalidCharacter {
        /// Zero-based byte position.
        index: usize,
    },
}

impl fmt::Display for DatabaseAliasError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid database alias")
    }
}

impl Error for DatabaseAliasError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_aliases_preserve_exact_text_and_sort_by_bytes() {
        let mut aliases = [
            DatabaseAlias::new("zeta-2").expect("valid alias"),
            DatabaseAlias::new("ea_local").expect("valid alias"),
            DatabaseAlias::default_alias(),
        ];
        aliases.sort();
        assert_eq!(
            aliases.map(|alias| alias.as_str().to_owned()),
            ["default", "ea_local", "zeta-2"]
        );
    }

    #[test]
    fn grammar_and_bounds_fail_closed() {
        assert_eq!(DatabaseAlias::new(""), Err(DatabaseAliasError::Empty));
        assert_eq!(
            DatabaseAlias::new("A"),
            Err(DatabaseAliasError::InvalidCharacter { index: 0 })
        );
        assert_eq!(
            DatabaseAlias::new("1database"),
            Err(DatabaseAliasError::InvalidCharacter { index: 0 })
        );
        assert_eq!(
            DatabaseAlias::new("data.base"),
            Err(DatabaseAliasError::InvalidCharacter { index: 4 })
        );
        assert_eq!(
            DatabaseAlias::new("a".repeat(MAX_DATABASE_ALIAS_BYTES + 1)),
            Err(DatabaseAliasError::TooLong {
                actual: MAX_DATABASE_ALIAS_BYTES + 1,
                maximum: MAX_DATABASE_ALIAS_BYTES,
            })
        );
        assert!(DatabaseAlias::new("a".repeat(MAX_DATABASE_ALIAS_BYTES)).is_ok());
    }
}
