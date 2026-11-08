//! Refusal diagnostics derived from the already-authenticated deployment health response.
use crate::output::{CommandIdentity, Terminal, local_error};
use riffdb_client_rust::v1;
use riffdb_types::DatabaseAlias;
use serde::ser::{Error as _, SerializeMap};
use serde::{Serialize, Serializer};

#[derive(Clone, Copy, Debug)]
enum Cause {
    AliasMismatch,
    AudienceMissing,
    PreBootstrap,
    MissingEvidence,
    ComponentNotHealthy,
}
#[derive(Clone, Debug)]
pub(crate) struct ReadinessDiagnostic {
    cause: Cause,
    component: Option<v1::HealthComponentKind>,
    status: Option<v1::HealthComponentStatus>,
    bootstrap: Option<v1::PreBootstrapLifecycle>,
}
impl ReadinessDiagnostic {
    fn simple(cause: Cause) -> Self {
        Self {
            cause,
            component: None,
            status: None,
            bootstrap: None,
        }
    }
    fn cause(&self) -> &'static str {
        match self.cause {
            Cause::AliasMismatch => "alias_mismatch",
            Cause::AudienceMissing => "audience_missing",
            Cause::PreBootstrap => "pre_bootstrap",
            Cause::MissingEvidence => "missing_evidence",
            Cause::ComponentNotHealthy => "component_not_healthy",
        }
    }
    fn component(&self) -> Option<&'static str> {
        Some(match self.cause {
            Cause::AliasMismatch => "database_selection",
            Cause::AudienceMissing => "authentication_audience",
            Cause::PreBootstrap => "bootstrap",
            Cause::MissingEvidence => "readiness",
            Cause::ComponentNotHealthy => match self.component? {
                v1::HealthComponentKind::AuthoritativeStorage => "authoritative_storage",
                v1::HealthComponentKind::Catalog => "catalog",
                v1::HealthComponentKind::CommitCoordinator => "commit_coordinator",
                v1::HealthComponentKind::Projection => "projection",
                v1::HealthComponentKind::Outbox => "outbox",
                v1::HealthComponentKind::VectorStaleness => "vector_staleness",
                v1::HealthComponentKind::Replication => "replication",
                v1::HealthComponentKind::Unspecified => return None,
            },
        })
    }
    fn status(&self) -> Option<&'static str> {
        if let Some(status) = self.status {
            return match status {
                v1::HealthComponentStatus::Healthy => Some("healthy"),
                v1::HealthComponentStatus::Degraded => Some("degraded"),
                v1::HealthComponentStatus::Unavailable => Some("unavailable"),
                v1::HealthComponentStatus::Unspecified => None,
            };
        }
        self.bootstrap.and_then(|lifecycle| match lifecycle {
            v1::PreBootstrapLifecycle::InitializingValidation => Some("initializing_validation"),
            v1::PreBootstrapLifecycle::InitializingBootstrap => Some("initializing_bootstrap"),
            v1::PreBootstrapLifecycle::Unspecified => None,
        })
    }
    fn expected(&self) -> &'static str {
        match self.cause {
            Cause::AliasMismatch => "authenticated health for the selected database alias",
            Cause::AudienceMissing => "nonempty authenticated server audience",
            Cause::PreBootstrap => "authenticated ready health after startup and bootstrap",
            Cause::MissingEvidence => {
                "bounded authenticated readiness evidence for the selected database"
            }
            Cause::ComponentNotHealthy => "healthy component before database readiness is proven",
        }
    }
    fn fix(&self) -> &'static str {
        match self.cause {
            Cause::AliasMismatch => {
                "check the selected database alias and operator inventory; retry against the intended database"
            }
            Cause::AudienceMissing => {
                "verify server authentication-audience configuration before retrying deployment"
            }
            Cause::PreBootstrap => {
                "finish startup validation and the documented operator bootstrap, then retry authenticated deployment"
            }
            Cause::MissingEvidence => {
                "inspect authenticated server health and incident logs; retry when complete readiness evidence is available"
            }
            Cause::ComponentNotHealthy => match self.component {
                Some(v1::HealthComponentKind::Projection) => {
                    "inspect projection build/recovery health and retry after the component becomes healthy"
                }
                Some(v1::HealthComponentKind::Outbox) => {
                    "inspect outbox destination configuration and worker health; retry after the component becomes healthy"
                }
                Some(v1::HealthComponentKind::Replication) => {
                    "inspect replication frontier and peer health; retry after the component becomes healthy"
                }
                Some(v1::HealthComponentKind::VectorStaleness) => {
                    "inspect vector projection freshness; retry after the component becomes healthy"
                }
                Some(v1::HealthComponentKind::Catalog) => {
                    "inspect catalog startup/reload diagnostics; retry after the component becomes healthy"
                }
                Some(v1::HealthComponentKind::AuthoritativeStorage) => {
                    "inspect authoritative-storage health and incident logs; retry after the component becomes healthy"
                }
                Some(v1::HealthComponentKind::CommitCoordinator) => {
                    "inspect commit-coordinator health and incident logs; retry after the component becomes healthy"
                }
                _ => "inspect authenticated health and server incident logs before retrying",
            },
        }
    }
    pub(crate) fn human(&self) -> String {
        format!(
            "component={} cause={}{}; expected={}; fix={}",
            self.component().unwrap_or("readiness"),
            self.cause(),
            self.status()
                .map_or(String::new(), |status| format!(" status={status}")),
            self.expected(),
            self.fix()
        )
    }
}
impl Serialize for ReadinessDiagnostic {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let component = self
            .component()
            .ok_or_else(|| S::Error::custom("readiness diagnostic component"))?;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("component", component)?;
        map.serialize_entry("cause", self.cause())?;
        if let Some(status) = self.status() {
            map.serialize_entry("status", status)?;
        }
        map.serialize_entry("expected", self.expected())?;
        map.serialize_entry("fix", self.fix())?;
        map.end()
    }
}

fn observations(selected: &DatabaseAlias, health: &v1::HealthResponse) -> Vec<ReadinessDiagnostic> {
    let authenticated = match health.result.as_ref() {
        Some(v1::health_response::Result::Authenticated(value)) => value,
        Some(v1::health_response::Result::PreBootstrap(value)) => {
            let mut diagnostic = ReadinessDiagnostic::simple(Cause::PreBootstrap);
            diagnostic.bootstrap = v1::PreBootstrapLifecycle::try_from(value.lifecycle).ok();
            return vec![diagnostic];
        }
        None => return vec![ReadinessDiagnostic::simple(Cause::MissingEvidence)],
    };
    // No database observations escape a mismatched or absent authenticated identity.
    if health.database_alias != selected.as_str() {
        return vec![ReadinessDiagnostic::simple(Cause::AliasMismatch)];
    }
    if health.authentication_audience.is_empty() {
        return vec![ReadinessDiagnostic::simple(Cause::AudienceMissing)];
    }
    if authenticated.components.len() > 7 {
        return vec![ReadinessDiagnostic::simple(Cause::MissingEvidence)];
    }
    let mut components = Vec::with_capacity(7);
    for observation in &authenticated.components {
        let (Ok(component), Ok(status)) = (
            v1::HealthComponentKind::try_from(observation.component),
            v1::HealthComponentStatus::try_from(observation.status),
        ) else {
            return vec![ReadinessDiagnostic::simple(Cause::MissingEvidence)];
        };
        if component == v1::HealthComponentKind::Unspecified
            || status == v1::HealthComponentStatus::Unspecified
            || components.iter().any(|(kind, _)| *kind == component)
        {
            return vec![ReadinessDiagnostic::simple(Cause::MissingEvidence)];
        }
        components.push((component, status));
    }
    components.sort_by_key(|(kind, _)| *kind as i32);
    let mut diagnostics = components
        .into_iter()
        .filter(|(_, status)| *status != v1::HealthComponentStatus::Healthy)
        .map(|(component, status)| ReadinessDiagnostic {
            cause: Cause::ComponentNotHealthy,
            component: Some(component),
            status: Some(status),
            bootstrap: None,
        })
        .collect::<Vec<_>>();
    if diagnostics.is_empty() {
        diagnostics.push(ReadinessDiagnostic::simple(Cause::MissingEvidence));
    }
    diagnostics
}

pub(crate) fn refusal(
    identity: CommandIdentity,
    selected: &DatabaseAlias,
    health: &v1::HealthResponse,
) -> Terminal {
    local_error(
        identity,
        "database_readiness_unavailable",
        "authenticated health did not prove the selected database and audience ready",
    )
    .with_readiness_diagnostics(observations(selected, health))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::OutputMode;
    fn sample_health() -> v1::HealthResponse {
        v1::HealthResponse {
            database_alias: "selected".to_owned(),
            authentication_audience: "synthetic-audience".to_owned(),
            result: Some(v1::health_response::Result::Authenticated(
                v1::AuthenticatedHealth {
                    status: v1::HealthStatus::NotReady as i32,
                    components: vec![
                        v1::HealthComponent {
                            component: v1::HealthComponentKind::Outbox as i32,
                            status: v1::HealthComponentStatus::Unavailable as i32,
                            replication: None,
                        },
                        v1::HealthComponent {
                            component: v1::HealthComponentKind::Projection as i32,
                            status: v1::HealthComponentStatus::Degraded as i32,
                            replication: None,
                        },
                    ],
                    ..Default::default()
                },
            )),
        }
    }
    fn render(health: &v1::HealthResponse, mode: OutputMode) -> (Vec<u8>, Vec<u8>) {
        let terminal = refusal(
            CommandIdentity::ApplicationDeploy,
            &DatabaseAlias::new("selected").unwrap(),
            health,
        );
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        assert_eq!(
            terminal.emit(mode, &mut stdout, &mut stderr),
            std::process::ExitCode::from(2)
        );
        (stdout, stderr)
    }
    // req: DX-006, DX-007, DX-009, ID-005
    #[test]
    fn deploy_readiness_reports_all_blockers_from_one_health_response() {
        let health = sample_health();
        let (stdout, stderr) = render(&health, OutputMode::JsonV2);
        assert!(stderr.is_empty());
        assert_eq!(
            stdout,
            include_bytes!(
                "../../../fixtures/diagnostics/cli-output-v2/application.deploy.both_blocked.jsonl"
            )
        );
        let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
        let entries = value["diagnostics"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["component"], "projection");
        assert_eq!(entries[0]["status"], "degraded");
        assert_eq!(entries[1]["component"], "outbox");
        assert_eq!(entries[1]["status"], "unavailable");
        assert_eq!(value["error"]["code"], "database_readiness_unavailable");
        let (stdout, stderr) = render(&health, OutputMode::Human);
        assert!(stdout.is_empty());
        let text = std::str::from_utf8(&stderr).unwrap();
        assert!(text.contains("component=outbox"));
        assert!(text.contains("component=projection"));
        let (stdout, stderr) = render(&health, OutputMode::Json);
        assert!(stderr.is_empty());
        assert_eq!(stdout,b"{\"schema\":\"riffdb.cli.output/v1\",\"command\":\"application.deploy\",\"ok\":false,\"error\":{\"type\":\"local\",\"code\":\"database_readiness_unavailable\",\"message\":\"authenticated health did not prove the selected database and audience ready\"}}\n");
    }
    // req: DX-006, DX-007, DX-009, ID-005
    #[test]
    fn deploy_readiness_identity_mismatch_remains_fail_closed() {
        let mut health = sample_health();
        health.database_alias = "other-alias-canary".to_owned();
        health.authentication_audience = "other-audience-canary".to_owned();
        let (stdout, stderr) = render(&health, OutputMode::JsonV2);
        assert!(stderr.is_empty());
        let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(value["diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(value["diagnostics"][0]["cause"], "alias_mismatch");
        let text = std::str::from_utf8(&stdout).unwrap();
        for canary in [
            "other-alias-canary",
            "other-audience-canary",
            "outbox",
            "projection",
        ] {
            assert!(!text.contains(canary));
        }
    }
    // req: DX-006, DX-007, DX-009, ID-005
    #[test]
    fn deploy_readiness_handles_absent_audience_bootstrap_and_missing_evidence_without_inference() {
        let mut health = sample_health();
        health.authentication_audience.clear();
        let entries = observations(&DatabaseAlias::new("selected").unwrap(), &health);
        assert_eq!(entries[0].cause(), "audience_missing");
        assert_eq!(entries.len(), 1);
        health.result = Some(v1::health_response::Result::PreBootstrap(
            v1::PreBootstrapHealth {
                lifecycle: v1::PreBootstrapLifecycle::InitializingValidation as i32,
                liveness: true,
                readiness: false,
            },
        ));
        let entries = observations(&DatabaseAlias::new("selected").unwrap(), &health);
        assert_eq!(entries[0].cause(), "pre_bootstrap");
        assert_eq!(entries[0].status(), Some("initializing_validation"));
        health.result = None;
        assert_eq!(
            observations(&DatabaseAlias::new("selected").unwrap(), &health)[0].cause(),
            "missing_evidence"
        );
        health = sample_health();
        if let Some(v1::health_response::Result::Authenticated(value)) = &mut health.result {
            value.components.push(value.components[0]);
        }
        assert_eq!(
            observations(&DatabaseAlias::new("selected").unwrap(), &health)[0].cause(),
            "missing_evidence"
        );
    }
    // req: DX-006, DX-007, DX-009, ID-005
    #[test]
    fn individual_blockers_and_unavailable_evidence_remain_bounded() {
        for (healthy_kind, expected) in [
            (v1::HealthComponentKind::Projection, "outbox"),
            (v1::HealthComponentKind::Outbox, "projection"),
        ] {
            let mut health = sample_health();
            if let Some(v1::health_response::Result::Authenticated(value)) = &mut health.result {
                for component in &mut value.components {
                    if component.component == healthy_kind as i32 {
                        component.status = v1::HealthComponentStatus::Healthy as i32;
                    }
                }
            }
            let (stdout, stderr) = render(&health, OutputMode::JsonV2);
            assert!(stderr.is_empty());
            let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
            let entries = value["diagnostics"].as_array().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0]["component"], expected);
        }
        for mode in 0..3 {
            let mut health = sample_health();
            if let Some(v1::health_response::Result::Authenticated(value)) = &mut health.result {
                match mode {
                    0 => value.components.clear(),
                    1 => value.components[0].component = 999,
                    _ => value.components.resize(8, value.components[0]),
                }
            }
            let (stdout, _) = render(&health, OutputMode::JsonV2);
            let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
            assert_eq!(value["diagnostics"].as_array().unwrap().len(), 1);
            assert_eq!(value["diagnostics"][0]["cause"], "missing_evidence");
            assert!(stdout.len() < 4096);
        }
    }
}
