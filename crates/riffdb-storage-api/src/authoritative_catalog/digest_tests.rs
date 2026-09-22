use std::sync::{Barrier, atomic::Ordering};

use sha2::{Digest, Sha256};

use crate::{AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2};

// req: REP-003, REP-005, STO-012
#[test]
fn competing_first_catalog_digests_preserve_fixture_hashes_and_compute_once() {
    const CHILD: &str = "RIFFDB_CATALOG_DIGEST_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // Other library tests may warm the process cache. Run only this test
        // in a fresh process so the barrier exercises first initialization.
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "authoritative_catalog::digest_tests::competing_first_catalog_digests_preserve_fixture_hashes_and_compute_once",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success(), "isolated cold-cache test failed");
        return;
    }

    let computations_v1 = &super::DIGEST_COMPUTATIONS;
    let computations_v2 = &crate::authoritative_catalog_v2::DIGEST_COMPUTATIONS;
    assert_eq!(computations_v1.load(Ordering::Relaxed), 0);
    assert_eq!(computations_v2.load(Ordering::Relaxed), 0);
    // Independently hash frozen fixture bytes, not the implementation's result.
    let expected_v1: [u8; 32] = Sha256::digest(include_bytes!(
        "../../../../fixtures/replication/authoritative-state-catalog-v1.txt"
    ))
    .into();
    let expected_v2: [u8; 32] = Sha256::digest(include_bytes!(
        "../../../../fixtures/replication/authoritative-state-catalog-v2.txt"
    ))
    .into();
    assert_ne!(expected_v1, expected_v2);
    let start = Barrier::new(9);
    std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|worker| {
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..64 {
                        let (v1, v2) = if worker % 2 == 0 {
                            (
                                AuthoritativeStateCatalogV1.digest(),
                                AuthoritativeStateCatalogV2.digest(),
                            )
                        } else {
                            let v2 = AuthoritativeStateCatalogV2.digest();
                            (AuthoritativeStateCatalogV1.digest(), v2)
                        };
                        assert_eq!(v1, expected_v1);
                        assert_eq!(v2, expected_v2);
                    }
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    assert_eq!(computations_v1.load(Ordering::Relaxed), 1);
    assert_eq!(computations_v2.load(Ordering::Relaxed), 1);
}
