#![forbid(unsafe_code)]
//! Deploys a `nearest` query module against the vector variant. This is the
//! step that turns a cold columnar source into one a projected query can
//! demand, and it is the first time this repository has attempted it from a
//! benchmark.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use riffdb_client_rust::{ApplicationContract, ApplicationUuid, ApplicationValue, NamedQuery};
use riffdb_perf_surface::daemon::{Daemon, riffdbd_binary};
use riffdb_perf_surface::session::{
    application_client, bearer, bootstrap_and_deploy, deploy_query_module,
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

#[test]
#[ignore = "diagnostic: deploys a nearest query module against the vector variant"]
fn a_nearest_query_module_deploys_against_the_vector_contract() {
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
        Ok(token) => {
            println!("ACTIVATION nearest query module deployed");
            token
        }
        Err(error) => {
            println!("ACTIVATION deploy refused: {error}");
            let _ = daemon.shutdown();
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
    };

    // Demand the source. WP-777 returns the typed Building result with no rows
    // on first demand and activates in the background, so this polls until the
    // result stops being Building. A fixed sleep would measure a cold source
    // and report it as a fast one.
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
        let mut attempts = 0_u32;
        loop {
            attempts += 1;
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
            let outcome = client.execute_named_query(query, &metadata).await;
            match outcome {
                Ok(result) => {
                    return Ok::<_, riffdb_perf_surface::session::SessionError>(format!(
                        "ready after {attempts} attempt(s): {result:?}"
                    ));
                }
                Err(error) => {
                    let rendered = format!("{error:?}");
                    if Instant::now() >= deadline {
                        return Ok(format!("still not ready after {attempts}: {rendered}"));
                    }
                    if attempts == 1 {
                        println!("ACTIVATION first demand: {rendered}");
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });
    match demanded {
        Ok(text) => println!("ACTIVATION demand outcome: {text}"),
        Err(error) => println!("ACTIVATION demand failed: {error}"),
    }
    for line in daemon.stderr_tail(400) {
        if line.contains("columnar_") {
            println!("ACTIVATION census {line}");
        }
    }
    let _ = daemon.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
