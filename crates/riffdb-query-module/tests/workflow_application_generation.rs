#![forbid(unsafe_code)]

//! Compiles the checked-in workflow adapter bindings as ordinary application clients.

use riffdb_client_rust::generated::GeneratedCommand;
use riffdb_client_rust::v1;

#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    clippy::deref_addrof,
    clippy::needless_question_mark
)]
mod mlflow {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/adapters/mlflow/generated/rust/client.rs"
    ));
}

#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    clippy::deref_addrof,
    clippy::needless_question_mark
)]
mod woodpecker {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/adapters/woodpecker/generated/rust/client.rs"
    ));
}

fn response(outcome_type: &str) -> v1::ExecuteCommandResponse {
    v1::ExecuteCommandResponse {
        outcome_type: outcome_type.to_owned(),
        ..v1::ExecuteCommandResponse::default()
    }
}

#[test]
fn mlflow_generated_claim_observes_the_exact_successor_revision() {
    let input = mlflow::ClaimRunInput {
        run_id: "00000000-0000-0000-0000-000000000011".to_owned(),
        duration: 60,
        owner_id: "00000000-0000-0000-0000-000000000012".to_owned(),
        request_key: "schedule/mlflow/17/claim/1".to_owned(),
        organization_id: "00000000-0000-0000-0000-000000000013".to_owned(),
        expected_revision: 41,
    };
    let revisions = input
        .workflow_successor_revisions(&response("RunClaimed"))
        .expect("generated revision");
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].binding(), "run");
    assert_eq!(revisions[0].revision(), 42);
    assert!(
        input
            .workflow_successor_revisions(&response("ClaimUnavailable"))
            .expect("declared failure")
            .is_empty()
    );
}

#[test]
fn woodpecker_generated_transition_observes_the_exact_successor_revision() {
    let input = woodpecker::StartPipelineInput {
        owner_id: "00000000-0000-0000-0000-000000000021".to_owned(),
        request_key: "schedule/woodpecker/29/start".to_owned(),
        fencing_token: 9,
        organization_id: "00000000-0000-0000-0000-000000000022".to_owned(),
        pipeline_id: "00000000-0000-0000-0000-000000000023".to_owned(),
        expected_revision: 72,
    };
    let revisions = input
        .workflow_successor_revisions(&response("PipelineStarted"))
        .expect("generated revision");
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].binding(), "pipeline");
    assert_eq!(revisions[0].revision(), 73);
}
