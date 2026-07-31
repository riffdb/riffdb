//! Migration digest-domain compatibility tests.

use riffdb_types::{
    HashDomain, hash, hash_contract_bundle, hash_migration_bundle, hash_migration_source,
};

#[test]
fn migration_hashes_have_distinct_registered_domains() {
    let payload = b"same canonical bytes";

    assert_eq!(
        hash_migration_source(payload).as_bytes(),
        hash(HashDomain::MigrationSource, payload).as_bytes()
    );
    assert_eq!(
        hash_migration_bundle(payload).as_bytes(),
        hash(HashDomain::MigrationBundle, payload).as_bytes()
    );
    assert_ne!(
        hash_migration_bundle(payload).as_bytes(),
        hash_contract_bundle(payload).as_bytes()
    );
    assert_ne!(
        hash_migration_source(payload).as_bytes(),
        hash_migration_bundle(payload).as_bytes()
    );
}
