//! Bounded publication notification custody. No writer or storage engine handle
//! is retained; consumers see only pinned, already-published durable snapshots.

use std::sync::Arc;

use riffdb_storage_api::{
    ChangelogPublicationPort, PublishedDurableSnapshot, PublishedFrontierAdvancement,
    ReplicationStreamErrorV3,
};
use tokio::sync::watch;

type Publication = Option<Result<Arc<dyn PublishedDurableSnapshot>, ReplicationStreamErrorV3>>;

/// The production publication observer. The watch slot retains only the newest
/// notification. Slow consumers resume through durable receipt history, so a
/// coalesced notification cannot discard a transaction.
pub struct ReplicationPublication {
    sender: watch::Sender<Publication>,
}

/// Cloneable snapshot notification reader with no publication or write authority.
#[derive(Clone)]
pub struct ReplicationPublishedSnapshots {
    receiver: watch::Receiver<Publication>,
}

impl ReplicationPublication {
    /// Constructs one bounded notification slot and its least-authority reader.
    #[must_use]
    pub fn channel() -> (Arc<Self>, ReplicationPublishedSnapshots) {
        let (sender, receiver) = watch::channel(None);
        (
            Arc::new(Self { sender }),
            ReplicationPublishedSnapshots { receiver },
        )
    }
}

impl ChangelogPublicationPort for ReplicationPublication {
    fn observe_published_advancement(&self, advancement: PublishedFrontierAdvancement) {
        self.observe_published_snapshot_v3(Arc::clone(advancement.snapshot()));
    }

    fn observe_published_snapshot_v3(&self, snapshot: Arc<dyn PublishedDurableSnapshot>) {
        // No cursor is opened here. The displaced pin is dropped after watch's
        // internal write guard is released; no consumer can retain that guard.
        drop(self.sender.send_replace(Some(Ok(snapshot))));
    }

    fn observe_source_unavailable_v3(&self) {
        drop(
            self.sender
                .send_replace(Some(Err(ReplicationStreamErrorV3::Unavailable))),
        );
    }
}

impl ReplicationPublishedSnapshots {
    pub(crate) fn take_newer(
        &mut self,
    ) -> Result<Option<Arc<dyn PublishedDurableSnapshot>>, ReplicationStreamErrorV3> {
        if self
            .receiver
            .has_changed()
            .map_err(|_| ReplicationStreamErrorV3::Unavailable)?
        {
            self.latest()
        } else {
            Ok(None)
        }
    }

    /// Clones the newest pin, releasing the watch guard before returning it.
    /// Absence means startup has not published a seed snapshot yet.
    pub fn latest(
        &mut self,
    ) -> Result<Option<Arc<dyn PublishedDurableSnapshot>>, ReplicationStreamErrorV3> {
        self.receiver.borrow_and_update().clone().transpose()
    }

    /// Waits for a notification after the last observed pin. The owner supplies
    /// its bounded stream deadline/cancellation; dropping this future leaves no
    /// mutex guard, write lease, or non-durable capability behind.
    pub async fn changed(
        &mut self,
    ) -> Result<Arc<dyn PublishedDurableSnapshot>, ReplicationStreamErrorV3> {
        self.receiver
            .changed()
            .await
            .map_err(|_| ReplicationStreamErrorV3::Unavailable)?;
        self.latest()?.ok_or(ReplicationStreamErrorV3::Unavailable)
    }
}

impl std::fmt::Debug for ReplicationPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationPublication([redacted])")
    }
}

impl std::fmt::Debug for ReplicationPublishedSnapshots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationPublishedSnapshots([redacted])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::{CompositeRow, CompositeTableV1, StorageError};
    use riffdb_types::{AdministrationSequence, CommitSequence};

    struct Pin;
    impl PublishedDurableSnapshot for Pin {
        fn read_value(
            &self,
            _: CompositeTableV1,
            _: &[u8],
        ) -> Result<Option<Vec<u8>>, StorageError> {
            panic!("publication must not read storage")
        }
        fn read_range(
            &self,
            _: CompositeTableV1,
            _: &[u8],
            _: &[u8],
            _: usize,
        ) -> Result<Vec<CompositeRow>, StorageError> {
            panic!("publication must not scan storage")
        }
        fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
            panic!("publication must not read storage")
        }
        fn administration_frontier(&self) -> Result<Option<AdministrationSequence>, StorageError> {
            panic!("publication must not read storage")
        }
    }

    #[tokio::test]
    // req: REP-003, PERF-007
    async fn coalesced_publication_retains_only_the_newest_pin_and_wakes_pending_readers() {
        let (publisher, mut reader) = ReplicationPublication::channel();
        assert!(reader.latest().unwrap().is_none());
        let first: Arc<dyn PublishedDurableSnapshot> = Arc::new(Pin);
        let dropped = Arc::downgrade(&first);
        publisher.observe_published_snapshot_v3(first);
        let latest: Arc<dyn PublishedDurableSnapshot> = Arc::new(Pin);
        for _ in 0..1_000 {
            publisher.observe_published_snapshot_v3(Arc::clone(&latest));
        }
        assert!(dropped.upgrade().is_none());
        assert!(Arc::ptr_eq(&reader.latest().unwrap().unwrap(), &latest));
        let next: Arc<dyn PublishedDurableSnapshot> = Arc::new(Pin);
        let mut waiting = Box::pin(reader.changed());
        assert!(futures_util::poll!(&mut waiting).is_pending());
        publisher.observe_published_snapshot_v3(Arc::clone(&next));
        assert!(Arc::ptr_eq(&waiting.await.unwrap(), &next));
    }

    #[tokio::test]
    // req: REP-003, PERF-007
    async fn publication_cancellation_and_source_close_release_waiters() {
        let (publisher, mut reader) = ReplicationPublication::channel();
        let mut waiting = Box::pin(reader.changed());
        assert!(futures_util::poll!(&mut waiting).is_pending());
        drop(waiting);
        publisher.observe_source_unavailable_v3();
        assert_eq!(
            reader.changed().await.err(),
            Some(ReplicationStreamErrorV3::Unavailable)
        );
        drop(publisher);
        assert_eq!(
            reader.changed().await.err(),
            Some(ReplicationStreamErrorV3::Unavailable)
        );
    }
}
