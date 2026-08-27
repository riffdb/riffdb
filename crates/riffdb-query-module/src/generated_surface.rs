//! Closed compiler-owned generated application surface registry.

use crate::GeneratedApplicationArtifactKind;

/// One safe first-party generated application package surface.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GeneratedApplicationSurface {
    /// Rust facade.
    Rust,
    /// Go facade.
    Go,
    /// TypeScript facade.
    TypeScript,
    /// Python facade.
    Python,
    /// MCP tool catalog.
    Mcp,
}

impl GeneratedApplicationSurface {
    /// Every accepted V7 surface in canonical key order.
    pub const ALL: [Self; 5] = [
        Self::Go,
        Self::Mcp,
        Self::Python,
        Self::Rust,
        Self::TypeScript,
    ];

    /// Parses one exact application-source generation key.
    #[must_use]
    pub const fn parse(key: &str) -> Option<Self> {
        match key.as_bytes() {
            b"go" => Some(Self::Go),
            b"mcp" => Some(Self::Mcp),
            b"python" => Some(Self::Python),
            b"rust" => Some(Self::Rust),
            b"typescript" => Some(Self::TypeScript),
            _ => None,
        }
    }

    /// Canonical application-source generation key.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Go => "go",
            Self::TypeScript => "typescript",
            Self::Python => "python",
            Self::Mcp => "mcp",
        }
    }

    /// Default project scaffold path.
    #[must_use]
    pub const fn default_path(self) -> &'static str {
        match self {
            Self::Rust => "generated/rust/client.rs",
            Self::Go => "generated/go/client.go",
            Self::TypeScript => "generated/typescript/client.ts",
            Self::Python => "generated/python/client.py",
            Self::Mcp => "generated/mcp/tools.json",
        }
    }

    /// Exact application-lock artifact kind.
    #[must_use]
    pub const fn artifact_kind(self) -> GeneratedApplicationArtifactKind {
        match self {
            Self::Rust => GeneratedApplicationArtifactKind::Rust,
            Self::Go => GeneratedApplicationArtifactKind::Go,
            Self::TypeScript => GeneratedApplicationArtifactKind::TypeScript,
            Self::Python => GeneratedApplicationArtifactKind::Python,
            Self::Mcp => GeneratedApplicationArtifactKind::Mcp,
        }
    }
}
