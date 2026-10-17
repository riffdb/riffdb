// req: STO-032, REC-001, TXN-042
use super::*;
#[test]
fn refused_private_or_fenced_close_preserves_registered_work() {
    for private in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("close-role-refusal");
        let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
        if private {
            Arc::get_mut(&mut store.shared).unwrap().open_mode = OpenMode::OfflineSource;
        } else {
            store.shared.write_fenced.store(true, Ordering::Release);
        }
        let domain = Arc::clone(store.shared.database.locator_build_domain());
        let work = domain.register().unwrap();
        let before = store.shared.durable_commit_epoch();
        let receipt = store.complete_graceful_close();
        assert_eq!(
            receipt.disposition(),
            crate::GracefulCheckpointDispositionV1::BarrierFailed
        );
        assert_eq!(
            receipt.lifecycle(),
            crate::GracefulLifecycleOutcomeV1::CleanNotAttempted
        );
        assert_eq!(store.shared.durable_commit_epoch(), before);
        assert!(!domain.is_cancelled());
        assert_eq!(domain.registrations_for_test(), 1);
        drop(work);
        assert_eq!(domain.registrations_for_test(), 0);
    }
}
