//! Closed test-fixture controller for process-level maintenance recovery.

/// Internal maintenance boundaries that can terminate a dedicated recovery
/// child when the `test-fixtures` feature is enabled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceRecoveryBoundary {
    DrainComplete,
    DatabaseClosed,
    StagedAuthorizationComplete,
    FreshValidationComplete,
}

/// Explicitly injected controller for one daemon process.
///
/// Production construction is always disabled. The armed representation and
/// process termination path do not exist unless `test-fixtures` is selected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintenanceRecoveryController {
    #[cfg(feature = "test-fixtures")]
    armed: Option<MaintenanceRecoveryBoundary>,
}

impl MaintenanceRecoveryController {
    #[must_use]
    pub(crate) const fn disabled() -> Self {
        Self {
            #[cfg(feature = "test-fixtures")]
            armed: None,
        }
    }

    #[cfg(feature = "test-fixtures")]
    #[must_use]
    pub(crate) const fn armed(boundary: MaintenanceRecoveryBoundary) -> Self {
        Self {
            armed: Some(boundary),
        }
    }

    pub(crate) fn reached(self, boundary: MaintenanceRecoveryBoundary) {
        #[cfg(not(feature = "test-fixtures"))]
        let _ = boundary;

        #[cfg(feature = "test-fixtures")]
        if self.armed == Some(boundary) {
            std::process::abort();
        }
    }
}

#[cfg(feature = "test-fixtures")]
/// Closed daemon boundary for one dedicated process-abort fixture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceRecoveryTestPoint {
    /// Accepted work and background workers drained before storage ports close.
    DrainComplete,
    /// The owning production graph and its database handles were destroyed.
    DatabaseClosed,
    /// Staged authentication and authorization passed and credentials were dropped.
    StagedAuthorizationComplete,
    /// Fresh post-publication startup validation passed before terminal receipt.
    FreshValidationComplete,
}

#[cfg(feature = "test-fixtures")]
impl From<MaintenanceRecoveryTestPoint> for MaintenanceRecoveryBoundary {
    fn from(value: MaintenanceRecoveryTestPoint) -> Self {
        match value {
            MaintenanceRecoveryTestPoint::DrainComplete => Self::DrainComplete,
            MaintenanceRecoveryTestPoint::DatabaseClosed => Self::DatabaseClosed,
            MaintenanceRecoveryTestPoint::StagedAuthorizationComplete => {
                Self::StagedAuthorizationComplete
            }
            MaintenanceRecoveryTestPoint::FreshValidationComplete => Self::FreshValidationComplete,
        }
    }
}
