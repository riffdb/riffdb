//! Cloneable checkpoint completion: waiting never requires the writer's state lock.
use super::*;

#[derive(Default)]
pub(super) struct CheckpointCompletion {
    result: Mutex<Option<Result<(), StorageError>>>,
    changed: Condvar,
}

impl CheckpointCompletion {
    pub(super) fn poll(&self, wait: bool) -> Option<Result<(), StorageError>> {
        let Ok(mut result) = self.result.lock() else {
            return Some(Err(storage_error(StorageErrorKind::CommitStatusUnknown)));
        };
        while wait && result.is_none() {
            result = match self.changed.wait(result) {
                Ok(result) => result,
                Err(_) => {
                    return Some(Err(storage_error(StorageErrorKind::CommitStatusUnknown)));
                }
            };
        }
        result.clone()
    }

    fn publish(&self, outcome: Result<(), StorageError>) {
        let mut result = match self.result.lock() {
            Ok(result) => result,
            Err(poisoned) => poisoned.into_inner(),
        };
        if result.is_none() {
            *result = Some(outcome);
        }
        self.changed.notify_all();
    }
}

pub(super) struct CheckpointPublisher(pub(super) Arc<CheckpointCompletion>);

impl CheckpointPublisher {
    pub(super) fn publish(self, result: Result<(), StorageError>) {
        self.0.publish(result);
    }
}

impl Drop for CheckpointPublisher {
    fn drop(&mut self) {
        // Thread spawn failure and unwinding must wake every admission waiter.
        // A successful published result cannot be overwritten by this fallback.
        self.0
            .publish(Err(storage_error(StorageErrorKind::Unavailable)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: REC-001, END-008
    #[test]
    fn checkpoint_completion_wakes_all_waiters_without_consuming_the_result() {
        let completion = Arc::new(CheckpointCompletion::default());
        assert!(completion.poll(false).is_none());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let readers = (0..2)
            .map(|_| {
                let completion = Arc::clone(&completion);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    assert!(completion.poll(true).unwrap().is_ok());
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        CheckpointPublisher(Arc::clone(&completion)).publish(Ok(()));
        for reader in readers {
            reader.join().unwrap();
        }
        assert!(completion.poll(false).unwrap().is_ok());
    }

    // req: REC-001, END-008
    #[test]
    fn abandoned_checkpoint_publisher_reports_failure_and_cannot_replace_success() {
        let completion = Arc::new(CheckpointCompletion::default());
        drop(CheckpointPublisher(Arc::clone(&completion)));
        assert!(completion.poll(true).unwrap().is_err());
        let completed = Arc::new(CheckpointCompletion::default());
        CheckpointPublisher(Arc::clone(&completed)).publish(Ok(()));
        drop(CheckpointPublisher(Arc::clone(&completed)));
        assert!(completed.poll(true).unwrap().is_ok());
    }
}
