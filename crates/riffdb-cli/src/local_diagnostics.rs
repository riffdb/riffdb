//! Closed local-refusal causes for `json-v2`. Paths, submitted values and file
//! contents cannot enter this DTO; human output carries any path detail.
use serde::Serialize;
use serde::ser::SerializeMap as _;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalDiagnostic {
    CredentialOutputUnavailable,
    BootstrapOutcomeUnknown,
    DeploymentDirectory(DeploymentDirectoryCause),
    DeploymentState(DeploymentStateCause),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentDirectoryCause {
    PathInvalid,
    ParentMissing,
    Symlink,
    NotDirectory,
    Uninspectable,
    CreateFailed,
    PermissionsFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentStateCause {
    Uninspectable,
    NotRegular,
    Unreadable,
    Malformed,
    DatabaseMismatch,
    InvariantsInvalid,
}

const DEPLOYMENT_DIRECTORY_EXPECTED: &str = "operator-owned regular directories, not symlinks, that the CLI can create and set to mode 0700";
const DEPLOYMENT_STATE_EXPECTED: &str = "absent state, or one regular deployment-state.json written by riffdb for the selected database";

impl LocalDiagnostic {
    pub(crate) const fn component(self) -> &'static str {
        match self {
            Self::CredentialOutputUnavailable => "credential_output",
            Self::BootstrapOutcomeUnknown => "bootstrap_recovery",
            Self::DeploymentDirectory(_) => "deployment_directory",
            Self::DeploymentState(_) => "deployment_state",
        }
    }

    pub(crate) const fn cause(self) -> &'static str {
        match self {
            Self::CredentialOutputUnavailable => "output_unavailable",
            Self::BootstrapOutcomeUnknown => "outcome_unknown",
            Self::DeploymentDirectory(cause) => match cause {
                DeploymentDirectoryCause::PathInvalid => "path_invalid",
                DeploymentDirectoryCause::ParentMissing => "parent_missing",
                DeploymentDirectoryCause::Symlink => "symlink",
                DeploymentDirectoryCause::NotDirectory => "not_directory",
                DeploymentDirectoryCause::Uninspectable => "uninspectable",
                DeploymentDirectoryCause::CreateFailed => "create_failed",
                DeploymentDirectoryCause::PermissionsFailed => "permissions_failed",
            },
            Self::DeploymentState(cause) => match cause {
                DeploymentStateCause::Uninspectable => "uninspectable",
                DeploymentStateCause::NotRegular => "not_regular",
                DeploymentStateCause::Unreadable => "unreadable",
                DeploymentStateCause::Malformed => "malformed",
                DeploymentStateCause::DatabaseMismatch => "database_mismatch",
                DeploymentStateCause::InvariantsInvalid => "invariants_invalid",
            },
        }
    }

    pub(crate) const fn expected(self) -> &'static str {
        match self {
            Self::CredentialOutputUnavailable => {
                "an absent output path, or with --force an existing regular non-symlink file, in an existing writable directory"
            }
            Self::BootstrapOutcomeUnknown => {
                "one replay of the same bootstrap with the retained bootstrap document before any new attempt"
            }
            Self::DeploymentDirectory(_) => DEPLOYMENT_DIRECTORY_EXPECTED,
            Self::DeploymentState(_) => DEPLOYMENT_STATE_EXPECTED,
        }
    }

    pub(crate) const fn fix(self) -> &'static str {
        match self {
            Self::CredentialOutputUnavailable => {
                "choose an unused path, or pass --force to overwrite an unused bootstrap or bearer file from an incomplete attempt; confirm the parent directory exists and is writable"
            }
            Self::BootstrapOutcomeUnknown => {
                "rerun capability bootstrap with --bootstrap-file naming the retained document: the --generate path, or .riffdb-bootstrap-<capability_id>.credential beside --bearer-output or in the temporary directory; pass --force to replace a retained --bearer-output; keep the document until the outcome is known"
            }
            Self::DeploymentDirectory(cause) => match cause {
                DeploymentDirectoryCause::PathInvalid => {
                    "pass a nonempty --deployment-dir or RIFFDB_DEPLOYMENT_DIR path other than -"
                }
                DeploymentDirectoryCause::ParentMissing => {
                    "create the parent directory of the deployment directory first"
                }
                DeploymentDirectoryCause::Symlink => {
                    "replace the symlink with a regular directory, or pass the canonical non-symlink path"
                }
                DeploymentDirectoryCause::NotDirectory => {
                    "move the non-directory entry aside or choose another deployment directory"
                }
                DeploymentDirectoryCause::Uninspectable
                | DeploymentDirectoryCause::CreateFailed
                | DeploymentDirectoryCause::PermissionsFailed => {
                    "grant the operator ownership and write permission on the deployment directory and its parent"
                }
            },
            Self::DeploymentState(cause) => match cause {
                DeploymentStateCause::Uninspectable | DeploymentStateCause::Unreadable => {
                    "restore read permission on deployment-state.json for the operator"
                }
                DeploymentStateCause::NotRegular => {
                    "replace deployment-state.json with the regular file riffdb wrote; never a symlink"
                }
                DeploymentStateCause::Malformed | DeploymentStateCause::InvariantsInvalid => {
                    "restore the deployment-state.json riffdb wrote, or move it aside only after confirming nothing was deployed from it"
                }
                DeploymentStateCause::DatabaseMismatch => {
                    "select the database recorded in deployment-state.json, or use a separate --deployment-dir for this database"
                }
            },
        }
    }

    pub(crate) fn human(self) -> String {
        format!(
            "component={} cause={}; expected={}; fix={}",
            self.component(),
            self.cause(),
            self.expected(),
            self.fix()
        )
    }
}

impl Serialize for LocalDiagnostic {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("component", self.component())?;
        map.serialize_entry("cause", self.cause())?;
        map.serialize_entry("expected", self.expected())?;
        map.serialize_entry("fix", self.fix())?;
        map.end()
    }
}

/// A deployment-path refusal: a closed cause for machines and the existing
/// detailed reason for human output.
#[derive(Debug)]
pub(crate) struct LocalRefusal {
    pub(crate) diagnostic: LocalDiagnostic,
    pub(crate) reason: String,
}

impl LocalRefusal {
    pub(crate) fn directory(cause: DeploymentDirectoryCause, reason: String) -> Self {
        Self {
            diagnostic: LocalDiagnostic::DeploymentDirectory(cause),
            reason,
        }
    }

    pub(crate) fn state(cause: DeploymentStateCause, reason: String) -> Self {
        Self {
            diagnostic: LocalDiagnostic::DeploymentState(cause),
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_local_diagnostic_is_closed_bounded_and_path_free() {
        use DeploymentDirectoryCause as D;
        use DeploymentStateCause as S;
        let all = [
            LocalDiagnostic::CredentialOutputUnavailable,
            LocalDiagnostic::BootstrapOutcomeUnknown,
            LocalDiagnostic::DeploymentDirectory(D::PathInvalid),
            LocalDiagnostic::DeploymentDirectory(D::ParentMissing),
            LocalDiagnostic::DeploymentDirectory(D::Symlink),
            LocalDiagnostic::DeploymentDirectory(D::NotDirectory),
            LocalDiagnostic::DeploymentDirectory(D::Uninspectable),
            LocalDiagnostic::DeploymentDirectory(D::CreateFailed),
            LocalDiagnostic::DeploymentDirectory(D::PermissionsFailed),
            LocalDiagnostic::DeploymentState(S::Uninspectable),
            LocalDiagnostic::DeploymentState(S::NotRegular),
            LocalDiagnostic::DeploymentState(S::Unreadable),
            LocalDiagnostic::DeploymentState(S::Malformed),
            LocalDiagnostic::DeploymentState(S::DatabaseMismatch),
            LocalDiagnostic::DeploymentState(S::InvariantsInvalid),
        ];
        let interface: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/diagnostics/cli-output-v2/interface.json"
        ))
        .expect("frozen v2 interface");
        let local = &interface["local"];
        for diagnostic in all {
            let rendered = serde_json::to_value(diagnostic).expect("serialize");
            let keys: Vec<_> = rendered
                .as_object()
                .expect("object")
                .keys()
                .cloned()
                .collect();
            assert_eq!(keys.len(), 4);
            for field in ["component", "cause", "expected", "fix"] {
                assert!(rendered[field].is_string(), "{field}");
            }
            let component = diagnostic.component();
            assert!(
                local["causes"][component]
                    .as_array()
                    .is_some_and(|causes| causes.iter().any(|cause| cause == diagnostic.cause())),
                "{component}/{} absent from the frozen interface",
                diagnostic.cause()
            );
            let text = diagnostic.human();
            assert!(text.len() < 1024);
        }
        let interface_causes: usize = local["causes"]
            .as_object()
            .expect("cause table")
            .values()
            .map(|causes| causes.as_array().map_or(0, Vec::len))
            .sum();
        assert_eq!(
            interface_causes,
            all.len(),
            "interface lists an unimplemented cause"
        );
    }
}
