//! Closed namespace, byte-bound and redaction contract for authoritative state rows.
// req: REP-003, STO-012
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateRowV3, AuthoritativeStateStepV3,
    ChangelogV3Error, MAX_CHANGELOG_FRAME_BYTES, ReplicationAuthorityClassV1 as Class,
};

#[test]
fn authoritative_state_rows_share_the_closed_mutation_namespace_and_bounds() {
    for namespace in N::ALL {
        let key = namespace
            .metadata_key()
            .map_or(b"key".as_slice(), str::as_bytes);
        let result = AuthoritativeStateRowV3::new(namespace, key, b"private-payload");
        if namespace.class() == Class::ReplicatedAuthoritative {
            let row = result.unwrap();
            assert_eq!(row.namespace(), namespace);
            assert_eq!(row.key(), key);
            assert_eq!(row.value(), b"private-payload");
            assert!(
                !format!("{:?}", AuthoritativeStateStepV3::Row(row.clone()))
                    .contains("private-payload")
            );
            let (observed, stored_key, stored_value) = row.into_parts();
            assert_eq!(
                (observed, stored_key.as_ref(), stored_value.as_ref()),
                (namespace, key, b"private-payload".as_slice())
            );
        } else {
            assert_eq!(result.unwrap_err(), ChangelogV3Error::InvalidNamespace);
        }
    }
    assert_eq!(
        AuthoritativeStateRowV3::new(N::DatabaseIdentity, b"foreign-key", b"value").unwrap_err(),
        ChangelogV3Error::InvalidNamespace
    );
    assert_eq!(
        AuthoritativeStateRowV3::new(N::Entities, b"", b"value").unwrap_err(),
        ChangelogV3Error::InvalidEncoding
    );
    let too_large = vec![0; MAX_CHANGELOG_FRAME_BYTES];
    assert_eq!(
        AuthoritativeStateRowV3::new(N::Entities, b"key", &too_large).unwrap_err(),
        ChangelogV3Error::LimitExceeded
    );
}
