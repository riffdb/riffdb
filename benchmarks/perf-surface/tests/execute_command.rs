#![forbid(unsafe_code)]

//! The measurement is only meaningful if the command it times actually
//! commits. This drives one workspace and one document through the real
//! daemon, on the variant carrying every mechanism, so the path the harness
//! will measure is proven before any timing is reported.

use riffdb_client_rust::ApplicationValue;
use riffdb_perf_surface::daemon::{Daemon, riffdbd_binary};
use riffdb_perf_surface::session::{
    application_client, attempts, bearer, bootstrap_and_deploy, command, issue_command_capability,
    publish_document_input,
};
use riffdb_perf_surface::{Mechanism, contract_source};
use std::collections::BTreeMap;

#[test]
fn a_document_publishes_through_the_real_daemon() {
    let binary = riffdbd_binary().expect(
        "no riffdbd binary: build it with `cargo build --release --bin riffdbd` \
         or set RIFFDB_PERF_SURFACE_RIFFDBD_BIN",
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let run_dir = std::env::temp_dir().join(format!("perf-surface-execute-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&run_dir);
    let daemon = Daemon::start(&binary, &run_dir).expect("daemon starts");
    let endpoint = daemon.endpoint();
    let source = contract_source(&Mechanism::ALL);

    let outcome = runtime.block_on(async {
        let bootstrap_token =
            bootstrap_and_deploy(&endpoint, &run_dir.join("bootstrap.credential"), &source).await?;
        let runner = issue_command_capability(&endpoint, &bootstrap_token, &source, &[]).await?;
        let metadata = bearer(&runner)?;
        let mut client = application_client(&endpoint).await?;

        let workspace = [0x11_u8; 16];
        let mut create = BTreeMap::new();
        create.insert(
            "idempotency_key".to_owned(),
            ApplicationValue::String("workspace-1".to_owned()),
        );
        create.insert(
            "workspace_id".to_owned(),
            ApplicationValue::Uuid(riffdb_client_rust::ApplicationUuid::from_bytes(workspace)),
        );
        create.insert(
            "name".to_owned(),
            ApplicationValue::String("perf surface".to_owned()),
        );
        client
            .execute_command(command("CreateWorkspace", create)?, attempts(), &metadata)
            .await
            .map_err(|error| {
                riffdb_perf_surface::session::SessionError::Rpc(format!(
                    "CreateWorkspace: {error:?}"
                ))
            })?;

        let publish = publish_document_input(
            workspace,
            [0x22_u8; 16],
            "document-1".to_owned(),
            "a title".to_owned(),
            "a body".to_owned(),
            4_096,
            // This test deploys Mechanism::ALL, which declares the vector field,
            // so the command requires the embed inputs.
            Some(vec![0.5_f32, 0.25, 0.125, 0.0625]),
        );
        client
            .execute_command(command("PublishDocument", publish)?, attempts(), &metadata)
            .await
            .map_err(|error| {
                riffdb_perf_surface::session::SessionError::Rpc(format!(
                    "PublishDocument: {error:?}"
                ))
            })
    });

    if let Err(error) = outcome {
        let tail = daemon.stderr_tail(15).join(" | ");
        panic!("command execution failed: {error}; server tail: {tail}");
    }

    let stdout = daemon.shutdown().expect("clean shutdown");
    // One document published means the writer did real work; an empty census
    // would mean the harness timed a path that never committed.
    let census = stdout
        .iter()
        .find(|line| line.starts_with("riffdb-writer-frame-census-v1"))
        .expect("writer frame census");
    let counts: Vec<u64> = census
        .split('\t')
        .nth(1)
        .unwrap_or_default()
        .split(',')
        .filter_map(|value| value.trim().parse().ok())
        .collect();
    assert!(
        counts.first().is_some_and(|commands| *commands >= 2),
        "expected at least the two committed commands in the census, got {counts:?}"
    );

    let _ = std::fs::remove_dir_all(&run_dir);
}
