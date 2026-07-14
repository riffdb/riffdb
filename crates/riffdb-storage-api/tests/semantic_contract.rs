//! Frozen foundational storage semantics and exact identity fixtures.

use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, DatabaseIdentityProbe,
    IdempotencyIdentity, IdempotencyIdentityKey, IdempotencyKeyDigest, IndexEpochPosition,
    OpenSessionId, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StorageError, StorageErrorKind,
};
use riffdb_types::{
    ActorId, CommandId, CommitSequence, ContractLineage, DatabaseId, DigestKeyId, Environment,
    IndexEpoch, TenantId, TenantScope,
};

fn database_id() -> DatabaseId {
    let mut bytes = [0x11; 16];
    bytes[6] = 0x71;
    bytes[8] = 0x91;
    DatabaseId::from_bytes(bytes).expect("valid UUIDv7 fixture")
}

#[test]
fn application_allocator_starts_at_one_and_exhausts_without_wrapping() {
    let initial = ApplicationSequenceAllocator::initial();
    let one = initial.allocate_one().expect("first allocation succeeds");
    assert_eq!(one.assigned(), CommitSequence::first());
    assert_eq!(
        one.next(),
        ApplicationSequenceAllocator::next(CommitSequence::new(2).expect("nonzero"))
    );

    let maximum = CommitSequence::new(u64::MAX).expect("nonzero");
    let last = ApplicationSequenceAllocator::next(maximum)
        .allocate_one()
        .expect("last value can be assigned");
    assert_eq!(last.assigned(), maximum);
    assert_eq!(last.next(), ApplicationSequenceAllocator::Exhausted);
    assert!(last.next().allocate_one().is_err());
}

#[test]
fn administration_multi_slot_preflight_is_atomic() {
    let maximum = riffdb_types::AdministrationSequence::new(u64::MAX).expect("nonzero");
    let allocator = AdministrationSequenceAllocator::next(maximum);
    assert!(allocator.allocate_consecutive(2).is_err());
    assert_eq!(allocator, AdministrationSequenceAllocator::next(maximum));
}

#[test]
fn idempotency_key_uses_the_exact_v1_envelope_and_round_trips() {
    let digest =
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(7).expect("nonzero"), [0xa5; 32]);
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        ActorId::new("principal-a").expect("actor"),
        ContractLineage::new("budget").expect("lineage"),
        CommandId::new(9).expect("command"),
        digest,
    );
    let key = identity.storage_key().expect("bounded key");
    assert_eq!(&key.as_bytes()[..2], &[0x59, 0x01]);
    assert_eq!(key.as_bytes().len(), 105);
    assert_eq!(
        IdempotencyIdentityKey::decode(key.as_bytes()).unwrap(),
        identity
    );
    assert!(format!("{key:?}").contains("[REDACTED]"));
    assert!(!format!("{key:?}").contains("principal-a"));
}

#[test]
fn digest_inventories_are_distinct_sorted_and_duplicate_free() {
    let one = ReadableDigestKey::v1(DigestKeyId::new(1).expect("nonzero"));
    let two = ReadableDigestKey::v1(DigestKeyId::new(2).expect("nonzero"));
    let capability =
        ReadableCapabilityDigestInventory::new(vec![two, one]).expect("inventory canonicalizes");
    let idempotency =
        ReadableIdempotencyDigestInventory::new(vec![one, two]).expect("inventory canonicalizes");
    assert_eq!(capability.as_slice(), &[one, two]);
    assert_eq!(idempotency.as_slice(), &[one, two]);
    assert!(ReadableCapabilityDigestInventory::new(vec![one, one]).is_err());
}

#[test]
fn session_and_epoch_empty_states_are_explicit() {
    assert!(OpenSessionId::new(0).is_none());
    assert_eq!(OpenSessionId::new(1).unwrap().get(), 1);
    assert_eq!(
        IndexEpochPosition::BeforeFirst,
        IndexEpochPosition::BeforeFirst
    );
    assert_eq!(
        IndexEpochPosition::Value(IndexEpoch::first()),
        IndexEpochPosition::Value(IndexEpoch::first())
    );
}

#[test]
fn identity_probe_and_storage_errors_keep_normal_state_separate() {
    assert_eq!(
        DatabaseIdentityProbe::Existing(database_id()),
        DatabaseIdentityProbe::Existing(database_id())
    );
    assert_eq!(
        DatabaseIdentityProbe::NeedsInitialization,
        DatabaseIdentityProbe::NeedsInitialization
    );
    let error = StorageError::new(StorageErrorKind::CorruptData, None);
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    assert!(!error.to_string().contains("path"));
}
