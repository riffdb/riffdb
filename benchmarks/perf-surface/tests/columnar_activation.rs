#![forbid(unsafe_code)]
//! Drives one projected vector query all the way to the columnar engine, which
//! is the step this repository had never taken from a benchmark.
//!
//! This is also OBL-0251-1: the daemon is never restarted. Until ADR-0251 the
//! source set was resolved once at startup, so a vector contract deployed into
//! a running daemon registered no source at all and this query failed as an
//! internal defect with an opaque incident.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use riffdb_client_rust::{ApplicationContract, ApplicationUuid, ApplicationValue, NamedQuery};
use riffdb_perf_surface::daemon::{Daemon, riffdbd_binary};
use riffdb_perf_surface::session::{
    application_client, attempts, bearer, bootstrap_and_deploy, command, deploy_query_module,
    publish_document_input,
};
use riffdb_perf_surface::{Mechanism, contract_source};

const NEAREST_QUERY: &str = r#"query SimilarDocuments(
    $workspace_id: Document.workspace_id,
    $query_vec: Document.embedding,
    $k: Limit<64>,
) {
    source projected Document.embedding
    freshness available

    many results from Document
        where workspace_id == $workspace_id
        nearest(embedding, $query_vec, $k)
    return Found { results: results { title } }
    outcomes Found
}"#;

/// What one poll loop observed: how the first demand was refused, and how many
/// rows the servable answer carried.
struct Demanded {
    first_refusal: Option<String>,
    rows: usize,
}

#[test]
#[ignore = "needs a built riffdbd; OBL-0251-1, end to end with no restart"]
fn a_vector_field_deployed_into_a_running_daemon_becomes_queryable() {
    let binary = riffdbd_binary().expect("riffdbd");
    let dir = std::env::temp_dir().join(format!("ps-activation-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");
    let daemon = Daemon::start(&binary, &dir).expect("daemon");
    let endpoint = daemon.endpoint();
    let source = contract_source(&[Mechanism::Vector]);
    let outcome = runtime.block_on(async {
        let token = bootstrap_and_deploy(&endpoint, &dir.join("bootstrap.cred"), &source).await?;
        let module_hash = deploy_query_module(
            &endpoint,
            &token,
            "PerfSurface",
            "perf_surface_vector",
            vec![("SimilarDocuments".to_owned(), NEAREST_QUERY.to_owned())],
        )
        .await?;
        // The runner capability must carry the named query, scoped to the hash
        // the deployment just returned.
        let runner = riffdb_perf_surface::session::issue_command_capability(
            &endpoint,
            &token,
            &source,
            &[("SimilarDocuments".to_owned(), module_hash)],
        )
        .await?;
        Ok::<_, riffdb_perf_surface::session::SessionError>(runner)
    });
    let token = match outcome {
        Ok(token) => token,
        Err(error) => {
            let _ = daemon.shutdown();
            let _ = std::fs::remove_dir_all(&dir);
            panic!("the nearest query module must deploy: {error}");
        }
    };

    // Write rows the projection can carry. An empty source activates too, but
    // it proves nothing about the population pass.
    let written = runtime.block_on(async {
        let mut client = application_client(&endpoint).await?;
        let metadata = bearer(&token)?;
        let mut create = BTreeMap::new();
        create.insert(
            "idempotency_key".to_owned(),
            ApplicationValue::String("activation-workspace".to_owned()),
        );
        create.insert(
            "workspace_id".to_owned(),
            ApplicationValue::Uuid(ApplicationUuid::from_bytes([0x11_u8; 16])),
        );
        create.insert(
            "name".to_owned(),
            ApplicationValue::String("activation".to_owned()),
        );
        client
            .execute_command(command("CreateWorkspace", create)?, attempts(), &metadata)
            .await
            .map_err(|error| {
                riffdb_perf_surface::session::SessionError::Input(format!("{error:?}"))
            })?;
        for ordinal in 0_u8..16 {
            let mut document = [0x22_u8; 16];
            document[15] = ordinal;
            let input = publish_document_input(
                [0x11_u8; 16],
                document,
                format!("activation-{ordinal}"),
                format!("document {ordinal}"),
                "body".repeat(8),
                64,
                Some(vec![f32::from(ordinal) / 16.0, 0.25, 0.125, 0.0625]),
            );
            client
                .execute_command(command("PublishDocument", input)?, attempts(), &metadata)
                .await
                .map_err(|error| {
                    riffdb_perf_surface::session::SessionError::Input(format!("{error:?}"))
                })?;
        }
        Ok::<_, riffdb_perf_surface::session::SessionError>(())
    });
    if let Err(error) = written {
        let _ = daemon.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
        panic!("the vector writes must be accepted: {error}");
    }

    // No restart. ADR-0251 admits a contract-derived source when its contract
    // is deployed, so the source this query demands was registered by the
    // deploy above, in this same process.

    // Demand the source. WP-777 keeps an admitted source cold until a
    // projected query asks for it: the first demand is refused while the
    // population pass runs, so this polls rather than sleeping — a fixed sleep
    // would read a cold source and report it as a fast one.
    let demanded = runtime.block_on(async {
        let mut client = application_client(&endpoint).await?;
        let metadata = bearer(&token)?;
        let mut parameters = BTreeMap::new();
        parameters.insert(
            "workspace_id".to_owned(),
            ApplicationValue::Uuid(ApplicationUuid::from_bytes([0x11_u8; 16])),
        );
        parameters.insert(
            "query_vec".to_owned(),
            ApplicationValue::Vector(
                riffdb_types::CanonicalVector::new(vec![0.5_f32, 0.25, 0.125, 0.0625])
                    .expect("query vector"),
            ),
        );
        parameters.insert("k".to_owned(), ApplicationValue::U64(8));

        let deadline = Instant::now() + Duration::from_secs(30);
        let mut first_refusal = None;
        let mut demands = 0_u32;
        loop {
            demands += 1;
            let query = NamedQuery::new(
                ApplicationContract::Active,
                "SimilarDocuments",
                None,
                parameters.clone(),
                None,
            )
            .map_err(|error| {
                riffdb_perf_surface::session::SessionError::Input(format!("{error:?}"))
            })?;
            match client.execute_named_query(query, &metadata).await {
                Ok(result) => {
                    return Ok::<_, riffdb_perf_surface::session::SessionError>(Demanded {
                        first_refusal,
                        rows: result
                            .fields
                            .get("results")
                            .map(|field| field.records.len())
                            .unwrap_or_default(),
                    });
                }
                Err(error) => {
                    if first_refusal.is_none() {
                        first_refusal = Some(format!("{error:?}"));
                    }
                    if Instant::now() >= deadline {
                        return Err(riffdb_perf_surface::session::SessionError::Rpc(format!(
                            "the source never became servable after {demands} demands: {error:?}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    });

    // The startup census reports the cold source count as of readiness, before
    // any demand. Activation and the population passes only settle as the
    // process closes, so the shutdown census is the line that carries them.
    let stderr = daemon.stderr_collector();
    let _ = daemon.shutdown();
    let census = stderr
        .lock()
        .map(|lines| lines.clone())
        .unwrap_or_default()
        .into_iter()
        .rfind(|line| line.contains("columnar_activations="))
        .unwrap_or_default();
    let _ = std::fs::remove_dir_all(&dir);

    let demanded = demanded.expect("the projected vector query must become servable");
    // The first demand is refused because the source is cold, not because the
    // query is wrong: a run that answers immediately would mean the population
    // pass never ran and the assertion below would be measuring nothing.
    let first_refusal = demanded
        .first_refusal
        .expect("the first demand on a cold source is refused");
    assert!(
        first_refusal.contains("QueryUnavailable"),
        "a cold source refuses with QueryUnavailable, found {first_refusal}"
    );
    assert_eq!(demanded.rows, 8, "the query asks for the nearest 8 of 16");
    assert!(
        census.contains("columnar_activations=1"),
        "one demand activates one source, census: {census}"
    );
    assert!(
        !census.contains("columnar_population_passes=0"),
        "activation runs at least one population pass, census: {census}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
