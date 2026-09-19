#![forbid(unsafe_code)]

//! Every variant must survive contact with the real server, not just the
//! compiler. A clause the compiler accepts but the daemon refuses would make a
//! whole arm of the measurement silently unavailable.

use riffdb_perf_surface::daemon::{Daemon, riffdbd_binary};
use riffdb_perf_surface::session::bootstrap_and_deploy;
use riffdb_perf_surface::{contract_source, variants};

#[test]
fn every_variant_deploys_to_a_real_daemon() {
    let binary = riffdbd_binary().expect(
        "no riffdbd binary: build it with `cargo build --release --bin riffdbd` \
         or set RIFFDB_PERF_SURFACE_RIFFDBD_BIN",
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    for (name, mechanisms) in variants() {
        let run_dir = std::env::temp_dir().join(format!(
            "perf-surface-deploy-{}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&run_dir);

        let daemon = Daemon::start(&binary, &run_dir)
            .unwrap_or_else(|error| panic!("variant {name}: daemon start: {error}"));
        let endpoint = daemon.endpoint();
        let source = contract_source(&mechanisms);

        let result = runtime.block_on(bootstrap_and_deploy(
            &endpoint,
            &run_dir.join("bootstrap.credential"),
            &source,
        ));

        match result {
            Ok(token) => assert!(!token.is_empty(), "variant {name} must yield a token"),
            Err(error) => {
                let tail = daemon.stderr_tail(12).join(" | ");
                panic!("variant {name}: deploy failed: {error}; server tail: {tail}");
            }
        }

        daemon
            .shutdown()
            .unwrap_or_else(|error| panic!("variant {name}: shutdown: {error}"));
        let _ = std::fs::remove_dir_all(&run_dir);
    }
}
