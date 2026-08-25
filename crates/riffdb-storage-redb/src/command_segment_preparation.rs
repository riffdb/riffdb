//! Authority-free canonical command-member preparation.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use riffdb_storage_api::{
    CommandCapsuleWireVersionV1, PreparedCommandSegmentCapsuleV1, StoredCommandCapsuleV2,
    prepare_command_segment_capsule_v1,
};

const MAX_COMMAND_SEGMENT_PREPARATION_WORKERS: usize = 8;

type PreparedChunk = (
    usize,
    Vec<StoredCommandCapsuleV2>,
    Vec<Result<PreparedCommandSegmentCapsuleV1, ()>>,
);

enum PreparationTask {
    Prepare {
        ordinal: usize,
        capsules: Vec<StoredCommandCapsuleV2>,
        wire_version: CommandCapsuleWireVersionV1,
        completion: mpsc::Sender<PreparedChunk>,
    },
}

/// Fixed bounded workers that own no storage, sequence, or durability port.
pub(crate) struct CommandSegmentPreparationPool {
    worker_count: usize,
    sender: Option<mpsc::SyncSender<PreparationTask>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl CommandSegmentPreparationPool {
    pub(crate) fn production_worker_count() -> usize {
        let available = thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        if available <= 8 {
            0
        } else {
            available
                .saturating_sub(1)
                .min(MAX_COMMAND_SEGMENT_PREPARATION_WORKERS)
        }
    }

    pub(crate) fn new(worker_count: usize) -> Result<Self, ()> {
        let worker_count = worker_count.min(MAX_COMMAND_SEGMENT_PREPARATION_WORKERS);
        if worker_count == 0 {
            return Ok(Self {
                worker_count,
                sender: None,
                workers: Vec::new(),
            });
        }
        let (sender, receiver) = mpsc::sync_channel(worker_count);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let worker = thread::Builder::new()
                .name(format!("riffdb-segment-prepare-{index}"))
                .spawn(move || {
                    loop {
                        let task = {
                            let Ok(receiver) = receiver.lock() else {
                                return;
                            };
                            let Ok(task) = receiver.recv() else {
                                return;
                            };
                            task
                        };
                        match task {
                            PreparationTask::Prepare {
                                ordinal,
                                capsules,
                                wire_version,
                                completion,
                            } => {
                                let prepared = capsules
                                    .iter()
                                    .map(|capsule| {
                                        prepare_command_segment_capsule_v1(capsule, wire_version)
                                            .map_err(|_| ())
                                    })
                                    .collect();
                                let _coordinator_may_have_stopped =
                                    completion.send((ordinal, capsules, prepared));
                            }
                        }
                    }
                })
                .map_err(|_| ());
            match worker {
                Ok(worker) => workers.push(worker),
                Err(()) => {
                    drop(sender);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(());
                }
            }
        }
        Ok(Self {
            worker_count,
            sender: Some(sender),
            workers,
        })
    }

    pub(crate) fn prepare(
        &self,
        capsules: Vec<StoredCommandCapsuleV2>,
        wire_version: CommandCapsuleWireVersionV1,
    ) -> Result<
        (
            Vec<StoredCommandCapsuleV2>,
            Option<Vec<PreparedCommandSegmentCapsuleV1>>,
        ),
        (),
    > {
        if capsules.len() <= 1 || self.worker_count == 0 {
            return Ok((capsules, None));
        }
        let task_count = capsules.len().min(self.worker_count);
        let chunk_size = capsules.len().div_ceil(task_count);
        let (completion, completed) = mpsc::channel();
        let mut chunks = capsules.into_iter();
        let mut submitted = 0_usize;
        for ordinal in 0..task_count {
            let chunk = chunks.by_ref().take(chunk_size).collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            self.sender
                .as_ref()
                .ok_or(())?
                .send(PreparationTask::Prepare {
                    ordinal,
                    capsules: chunk,
                    wire_version,
                    completion: completion.clone(),
                })
                .map_err(|_| ())?;
            submitted = submitted.saturating_add(1);
        }
        drop(completion);
        let mut ordered = BTreeMap::new();
        for _ in 0..submitted {
            let (ordinal, capsules, prepared) = completed.recv().map_err(|_| ())?;
            if ordered.insert(ordinal, (capsules, prepared)).is_some() {
                return Err(());
            }
        }
        let mut capsules = Vec::new();
        let mut prepared = Vec::new();
        for ordinal in 0..submitted {
            let (mut chunk_capsules, chunk_prepared) = ordered.remove(&ordinal).ok_or(())?;
            capsules.append(&mut chunk_capsules);
            prepared.extend(chunk_prepared.into_iter().collect::<Result<Vec<_>, _>>()?);
        }
        if !ordered.is_empty() || capsules.len() != prepared.len() {
            return Err(());
        }
        Ok((capsules, Some(prepared)))
    }
}

impl Drop for CommandSegmentPreparationPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_worker_count_is_bounded() {
        assert!(
            CommandSegmentPreparationPool::production_worker_count()
                <= MAX_COMMAND_SEGMENT_PREPARATION_WORKERS
        );
    }

    #[test]
    fn zero_worker_pool_selects_the_inline_path() {
        let pool = CommandSegmentPreparationPool::new(0).expect("zero-worker inline pool");
        assert_eq!(pool.worker_count, 0);
        assert!(pool.workers.is_empty());
    }
}
