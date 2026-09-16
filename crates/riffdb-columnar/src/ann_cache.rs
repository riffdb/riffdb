//! Disposable post-admission graph reuse (ADR-0229).
use crate::{PrimaryKeyBytes, hnsw::HnswIndex};
use riffdb_types::{CanonicalVector, DistanceMetric, EntityVersion, FieldId};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};

const MAX_ROWS: usize = 500;
const MAX_CONTEXT_BYTES: usize = 64 * 1024;
const MEMORY_LIMIT: usize = 64 * 1024 * 1024;
static MEMORY_USED: AtomicUsize = AtomicUsize::new(0);

struct Reservation(usize);
impl Reservation {
    fn acquire(bytes: usize) -> Option<Self> {
        MEMORY_USED
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= MEMORY_LIMIT)
            })
            .ok()?;
        Some(Self(bytes))
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        MEMORY_USED.fetch_sub(self.0, Ordering::Relaxed);
    }
}

pub(crate) struct Population {
    context: Vec<u8>,
    org: Vec<u8>,
    field: FieldId,
    metric: DistanceMetric,
    rows: Vec<(PrimaryKeyBytes, EntityVersion)>,
    vectors: Vec<CanonicalVector>,
    // The graph and all clones share this custody through in-flight searches.
    _reservation: Reservation,
}
pub(crate) struct PopulationView<'a> {
    pub context: &'a [u8],
    pub org: &'a [u8],
    pub field: FieldId,
    pub metric: DistanceMetric,
    pub rows: &'a [(PrimaryKeyBytes, EntityVersion)],
    pub vectors: &'a [CanonicalVector],
}
impl Population {
    fn matches(&self, view: &PopulationView<'_>) -> bool {
        self.context == view.context
            && self.org == view.org
            && self.field == view.field
            && self.metric == view.metric
            && self.rows == view.rows
            && self.vectors == view.vectors
    }
    fn retain(view: &PopulationView<'_>) -> Option<Arc<Self>> {
        if view.vectors.is_empty()
            || view.vectors.len() > MAX_ROWS
            || view.rows.len() != view.vectors.len()
            || view.context.len() > MAX_CONTEXT_BYTES
            || view.org.len() > MAX_CONTEXT_BYTES
        {
            return None;
        }
        // HNSW has at most nine levels and 12 neighbors per level. Reserve
        // 8 KiB/node for capacity rounding and build/search temporaries, plus
        // a 256 KiB builder stack and fixed bookkeeping. Canonical data and
        // identity buffers are charged separately before cloning any of them.
        let mut bytes = 320_usize.checked_mul(1024)?;
        bytes = bytes
            .checked_add(view.context.len())?
            .checked_add(view.org.len())?;
        for ((key, _), vector) in view.rows.iter().zip(view.vectors) {
            bytes = bytes
                .checked_add(8192)?
                .checked_add(key.as_bytes().len())?
                .checked_add(vector.components().len().checked_mul(4)?)?;
        }
        let reservation = Reservation::acquire(bytes)?;
        Some(Arc::new(Self {
            context: view.context.to_vec(),
            org: view.org.to_vec(),
            field: view.field,
            metric: view.metric,
            rows: view.rows.to_vec(),
            vectors: view.vectors.to_vec(),
            _reservation: reservation,
        }))
    }
}

pub(crate) struct CachedGraph {
    pub index: HnswIndex,
    population: Arc<Population>,
    #[cfg(test)]
    pub(crate) build_distances: usize,
}
#[derive(Default)]
struct State {
    active: Option<Arc<Population>>,
    pending: Option<Arc<Population>>,
    ready: Option<Arc<CachedGraph>>,
    selected: Option<Weak<Population>>,
}
struct Shared {
    state: Mutex<State>,
    #[cfg(test)]
    idle: std::sync::Condvar,
    #[cfg(test)]
    hooks: Mutex<Option<Arc<TestHooks>>>,
    #[cfg(test)]
    builds: AtomicUsize,
    #[cfg(test)]
    refuse: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
struct TestHooks {
    started: std::sync::mpsc::Sender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
    finished: std::sync::mpsc::Sender<()>,
}

/// Bounded memory-only graph owner for one immutable provider generation.
///
/// Callers still perform admission on every query. A provider must drop this
/// owner when its generation is invalidated; builders hold only a weak owner.
/// No application can supply topology or turn a cache hit into admission proof.
pub struct NearestGraphCache {
    shared: Arc<Shared>,
}
impl Default for NearestGraphCache {
    fn default() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                #[cfg(test)]
                idle: std::sync::Condvar::new(),
                #[cfg(test)]
                hooks: Mutex::new(None),
                #[cfg(test)]
                builds: AtomicUsize::new(0),
                #[cfg(test)]
                refuse: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }
}
impl NearestGraphCache {
    /// Constructs an empty generation-local cache. Reopening always starts cold.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn lookup_or_offer(&self, view: PopulationView<'_>) -> Option<Arc<CachedGraph>> {
        // Busy or poisoned optimization state never delays the exact path.
        let Ok(mut state) = self.shared.state.try_lock() else {
            return None;
        };
        if let Some(graph) = &state.ready
            && graph.population.matches(&view)
        {
            return Some(Arc::clone(graph));
        }
        if let Some(population) = state
            .active
            .as_ref()
            .filter(|p| p.matches(&view))
            .or_else(|| state.pending.as_ref().filter(|p| p.matches(&view)))
        {
            state.selected = Some(Arc::downgrade(population));
            return None;
        }
        // Superseded queued and ready states relinquish budget immediately.
        state.pending = None;
        state.ready = None;
        state.selected = None;
        #[cfg(test)]
        if self.shared.refuse.load(Ordering::Relaxed) {
            return None;
        }
        let population = Population::retain(&view)?;
        state.selected = Some(Arc::downgrade(&population));
        if state.active.is_some() {
            state.pending = Some(population);
            return None;
        }
        state.active = Some(Arc::clone(&population));
        let owner = Arc::downgrade(&self.shared);
        let spawned = std::thread::Builder::new()
            .name("riffdb-ann-build".into())
            .stack_size(256 * 1024)
            .spawn(move || build_loop(owner, population));
        if spawned.is_err() {
            state.active = None;
            state.selected = None;
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn refuse_memory(&self) {
        self.shared.refuse.store(true, Ordering::Relaxed);
    }
    #[cfg(test)]
    pub(crate) fn retained_allocation_ledger(&self) -> usize {
        let state = self.shared.state.lock().unwrap();
        state.ready.as_ref().map_or(0, |graph| {
            // Graph/population Arcs, context/org/row/vector buffers, and each
            // owned key/vector buffer. This excludes allocator metadata and
            // transient query/build allocations; it is not an allocator hook.
            6 + graph.population.rows.len() * 2 + graph.index.retained_buffer_allocations()
        })
    }

    #[cfg(test)]
    pub(crate) fn test_stats(&self) -> (usize, usize, usize) {
        let state = self.shared.state.lock().unwrap();
        (
            self.shared.builds.load(Ordering::Relaxed),
            state.ready.as_ref().map_or(0, |g| g.build_distances),
            state
                .ready
                .as_ref()
                .map_or(0, |g| g.population._reservation.0),
        )
    }

    #[cfg(test)]
    pub(crate) fn wait_for_idle(&self) {
        let state = self.shared.state.lock().unwrap();
        let (state, timeout) = self
            .shared
            .idle
            .wait_timeout_while(state, std::time::Duration::from_secs(30), |state| {
                state.active.is_some()
            })
            .unwrap();
        assert!(!timeout.timed_out() && state.active.is_none());
    }
}
fn build_loop(owner: Weak<Shared>, population: Arc<Population>) {
    #[cfg(test)]
    let hooks = owner
        .upgrade()
        .and_then(|shared| shared.hooks.lock().ok()?.clone());
    build_loop_inner(owner, population);
    #[cfg(test)]
    if let Some(hooks) = hooks {
        let _ = hooks.finished.send(());
    }
}
fn build_loop_inner(owner: Weak<Shared>, mut population: Arc<Population>) {
    loop {
        #[cfg(test)]
        {
            let hooks = owner.upgrade().and_then(|shared| {
                shared.builds.fetch_add(1, Ordering::Relaxed);
                shared.hooks.lock().ok()?.clone()
            });
            if let Some(hooks) = hooks {
                let _ = hooks.started.send(());
                let _ = hooks.resume.lock().unwrap().recv();
            }
            crate::nearest::DISTANCE_COUNT.set(0);
        }
        let refs = population.vectors.iter().collect::<Vec<_>>();
        let graph = HnswIndex::build_while(&refs, population.metric, || {
            owner.upgrade().is_some_and(|shared| {
                shared.state.lock().is_ok_and(|state| {
                    state
                        .selected
                        .as_ref()
                        .is_some_and(|selected| selected.ptr_eq(&Arc::downgrade(&population)))
                })
            })
        })
        .ok()
        .flatten();
        let Some(shared) = owner.upgrade() else {
            return;
        };
        let Ok(mut state) = shared.state.lock() else {
            return;
        };
        if state
            .selected
            .as_ref()
            .is_some_and(|selected| selected.ptr_eq(&Arc::downgrade(&population)))
        {
            state.ready = graph.map(|index| {
                Arc::new(CachedGraph {
                    index,
                    population: Arc::clone(&population),
                    #[cfg(test)]
                    build_distances: crate::nearest::DISTANCE_COUNT.get(),
                })
            });
        }
        state.active = None;
        let Some(next) = state.pending.take() else {
            #[cfg(test)]
            shared.idle.notify_all();
            return;
        };
        state.active = Some(Arc::clone(&next));
        drop(state);
        drop(shared);
        population = next;
    }
}

#[cfg(test)]
mod tests {
    // req: VEC-007, VEC-008, VEC-009, VEC-011
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn paused_build_supersession_drop_and_search_keep_bounded_custody() {
        let cache = NearestGraphCache::new();
        let (started, start_rx) = mpsc::channel();
        let (resume_tx, resume) = mpsc::channel();
        let (finished, finish_rx) = mpsc::channel();
        *cache.shared.hooks.lock().unwrap() = Some(Arc::new(TestHooks {
            started,
            resume: Mutex::new(resume),
            finished,
        }));
        let rows = vec![(
            PrimaryKeyBytes::from_entity_key_bytes(vec![1]),
            EntityVersion::first(),
        )];
        let vectors = vec![CanonicalVector::new(vec![1.0, 2.0]).unwrap()];
        let view = |context| PopulationView {
            context,
            org: b"org",
            field: FieldId::new(1).unwrap(),
            metric: DistanceMetric::Cosine,
            rows: &rows,
            vectors: &vectors,
        };
        assert!(cache.lookup_or_offer(view(b"first")).is_none());
        start_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let first = Arc::downgrade(cache.shared.state.lock().unwrap().active.as_ref().unwrap());
        // A hundred offers of the same population must not create queued jobs.
        for _ in 0..100 {
            assert!(cache.lookup_or_offer(view(b"first")).is_none());
        }
        assert!(cache.shared.state.lock().unwrap().pending.is_none());
        assert!(cache.lookup_or_offer(view(b"second")).is_none());
        let second = Arc::downgrade(cache.shared.state.lock().unwrap().pending.as_ref().unwrap());
        assert!(cache.lookup_or_offer(view(b"third")).is_none());
        assert_eq!(
            second.strong_count(),
            0,
            "superseded queued data releases its reservation"
        );
        resume_tx.send(()).unwrap();
        start_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(
            first.strong_count(),
            0,
            "cancelled first build cannot install itself"
        );
        assert!(cache.shared.state.lock().unwrap().ready.is_none());
        resume_tx.send(()).unwrap();
        finish_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let graph = cache.lookup_or_offer(view(b"third")).unwrap();
        let retained = Arc::downgrade(&graph.population);
        // An outstanding search retains budget after eviction/provider drop.
        drop(cache);
        assert_eq!(retained.strong_count(), 1);
        drop(graph);
        assert_eq!(retained.strong_count(), 0);
        assert!(Reservation::acquire(MEMORY_LIMIT + 1).is_none());
    }

    #[test]
    fn invalidated_provider_cancels_paused_builder_without_installation() {
        let cache = NearestGraphCache::new();
        let (started, start_rx) = mpsc::channel();
        let (resume_tx, resume) = mpsc::channel();
        let (finished, finish_rx) = mpsc::channel();
        *cache.shared.hooks.lock().unwrap() = Some(Arc::new(TestHooks {
            started,
            resume: Mutex::new(resume),
            finished,
        }));
        let rows = vec![(
            PrimaryKeyBytes::from_entity_key_bytes(vec![1]),
            EntityVersion::first(),
        )];
        let vectors = vec![CanonicalVector::new(vec![1.0]).unwrap()];
        cache.lookup_or_offer(PopulationView {
            context: b"old-generation",
            org: b"org",
            field: FieldId::new(1).unwrap(),
            metric: DistanceMetric::Cosine,
            rows: &rows,
            vectors: &vectors,
        });
        start_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let population =
            Arc::downgrade(cache.shared.state.lock().unwrap().active.as_ref().unwrap());
        let owner = Arc::downgrade(&cache.shared);
        drop(cache);
        assert!(owner.upgrade().is_none());
        assert_eq!(
            population.strong_count(),
            1,
            "builder still owns its reservation"
        );
        resume_tx.send(()).unwrap();
        finish_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(
            population.strong_count(),
            0,
            "cancelled builder releases all state"
        );
    }
}
