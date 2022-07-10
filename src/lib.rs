#![deny(rust_2018_idioms, unused, unused_crate_dependencies, unused_import_braces, unused_qualifications, warnings)]
#![forbid(unsafe_code)]

use {
    std::{
        collections::{
            HashMap,
            hash_map,
        },
        fmt,
        hash::Hash,
        marker::PhantomData,
        ops::Range,
        pin::Pin,
        sync::Arc,
    },
    derivative::Derivative,
    futures::{
        future::Future,
        stream::{
            self,
            FusedStream,
            Stream,
            StreamExt as _,
        },
    },
    log::debug,
    petgraph::{
        algo::astar,
        graphmap::DiGraphMap,
    },
    tokio::sync::{
        Mutex,
        RwLockReadGuard,
        broadcast,
        oneshot,
    },
    typemap_rev::{
        TypeMap,
        TypeMapKey,
    },
    crate::rw_future::RwFuture,
};

#[cfg(feature = "fs")] pub mod fs;
mod rw_future;
pub mod util;

pub trait Delta<S>: Clone + Send + Sync + 'static {
    fn apply(&self, state: &mut S);
}

impl<T: Clone + Send + Sync + 'static> Delta<T> for T {
    fn apply(&self, state: &mut T) {
        *state = self.clone();
    }
}

pub trait Key: Clone + Eq + Hash + Send + Sync + Sized + 'static {
    type State: Send + Sync;
    type Delta: Delta<Self::State>;

    /// Returns an initial state of this node, as well as a stream with all changes from that point.
    fn maintain(self, runner: RunnerInternal<Self>) -> Pin<Box<dyn Future<Output = (Self::State, Pin<Box<dyn Stream<Item = Self::Delta> + Send>>)> + Send>>;
}

/// A handle to a node whose state is being maintained by a `Runner`.
#[derive(Derivative)]
#[derivative(Debug(bound = ""))]
pub struct Handle<K: Key> {
    state: RwFuture<(K::State, broadcast::Sender<K::Delta>)>,
}

impl<K: Key> Handle<K> {
    /// Returns the current state of this node.
    pub async fn state(&self) -> RwLockReadGuard<'_, K::State> {
        RwLockReadGuard::map(self.state.read().await, |(state, _)| state)
    }

    /// Returns the current state of this node, as well as a stream with all changes from that point.
    pub async fn stream(&self) -> (RwLockReadGuard<'_, K::State>, broadcast::Receiver<K::Delta>) {
        let read_guard = self.state.read().await;
        let receiver = read_guard.1.subscribe();
        (RwLockReadGuard::map(read_guard, |(state, _)| state), receiver)
    }

    /// Returns a stream that yields the current state of this node at the start of the stream, as well as the state of this node after each change.
    ///
    /// Note that some states may be skipped if the underlying deltas channel lags.
    pub fn states(&self) -> impl Stream<Item = K::State> + FusedStream + '_
    where K::State: Clone + Unpin {
        debug!("ctrlflow states called");
        stream::unfold(None::<(K::State, broadcast::Receiver<K::Delta>)>, |state| async {
            if let Some((mut state, mut deltas)) = state {
                debug!("ctrlflow states: waiting for delta");
                match deltas.recv().await {
                    Ok(delta) => {
                        debug!("ctrlflow states received delta");
                        delta.apply(&mut state);
                        Some((state.clone(), Some((state, deltas))))
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("ctrlflow states closed");
                        None
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        debug!("ctrlflow states lagged");
                        let (init, deltas) = self.stream().await;
                        Some((init.clone(), Some((init.clone(), deltas))))
                    }
                }
            } else {
                debug!("ctrlflow states stream started");
                let (init, deltas) = self.stream().await;
                debug!("ctrlflow states: got inner stream");
                Some((init.clone(), Some((init.clone(), deltas))))
            }
        })
    }
}

// manually implemented to not require K::State: Clone
impl<K: Key> Clone for Handle<K> {
    fn clone(&self) -> Handle<K> {
        Handle { state: self.state.clone() }
    }
}

#[derive(Debug)]
pub struct DependencyLoop;

impl fmt::Display for DependencyLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dependency loop")
    }
}

impl std::error::Error for DependencyLoop {}

struct DepsMapKey<K: Key>(PhantomData<K>);

impl<K: Key> TypeMapKey for DepsMapKey<K> {
    type Value = HashMap<K, usize>;
}

struct Deps {
    graph: DiGraphMap<usize, ()>,
    indices: TypeMap,
    next_idx: Range<usize>,
}

impl Default for Deps {
    fn default() -> Self {
        Self {
            graph: DiGraphMap::default(),
            indices: TypeMap::default(),
            next_idx: 0..usize::MAX,
        }
    }
}

/// An internal handle to a runner passed to the [`Key::maintain`] method.
///
/// Nodes subscribed to from here will be tracked as internal dependencies.
#[derive(Clone)]
pub struct RunnerInternal<KD: Key> {
    runner: Runner,
    key: KD,
}

impl<KD: Key> RunnerInternal<KD> {
    pub async fn subscribe<KU: Key>(&self, upstream_key: KU) -> Result<Handle<KU>, DependencyLoop> {
        {
            let mut deps = self.runner.deps.lock().await;
            let upstream_ix = if let Some(&upstream_ix) = deps.indices.get::<DepsMapKey<KU>>().and_then(|upstream_indices| upstream_indices.get(&upstream_key)) {
                if let Some(&downstream_ix) = deps.indices.get::<DepsMapKey<KD>>().and_then(|downstream_indices| downstream_indices.get(&self.key)) {
                    if astar(&deps.graph, upstream_ix, |ix| ix == downstream_ix, |_| 1, |_| 0).is_some() {
                        return Err(DependencyLoop) //TODO include loop path in error?
                    }
                }
                upstream_ix
            } else {
                let upstream_ix = deps.next_idx.next().expect("too many nodes");
                deps.indices.entry::<DepsMapKey<KU>>().or_default().insert(upstream_key.clone(), upstream_ix);
                deps.graph.add_node(upstream_ix);
                upstream_ix
            };
            let downstream_ix = if let Some(&downstream_ix) = deps.indices.get::<DepsMapKey<KD>>().and_then(|downstream_indices| downstream_indices.get(&self.key)) {
                downstream_ix
            } else {
                let downstream_ix = deps.next_idx.next().expect("too many nodes");
                deps.indices.entry::<DepsMapKey<KD>>().or_default().insert(self.key.clone(), downstream_ix);
                deps.graph.add_node(downstream_ix);
                downstream_ix
            };
            // graph edges point in the direction the data flows
            deps.graph.add_edge(upstream_ix, downstream_ix, ());
        }
        Ok(self.runner.subscribe(upstream_key).await)
    }
}

struct HandleMapKey<K: Key>(PhantomData<K>);

impl<K: Key> TypeMapKey for HandleMapKey<K> {
    type Value = HashMap<K, Handle<K>>;
}

#[derive(Default, Clone)]
pub struct Runner {
    deps: Arc<Mutex<Deps>>,
    map: Arc<Mutex<TypeMap>>,
}

impl Runner {
    pub async fn subscribe<K: Key>(&self, key: K) -> Handle<K> {
        match self.map.lock().await.entry::<HandleMapKey<K>>().or_default().entry(key.clone()) {
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
            hash_map::Entry::Vacant(entry) => {
                let runner = RunnerInternal { runner: self.clone(), key: key.clone() };
                let (state_tx, state_rx) = oneshot::channel();
                let (stream_tx, stream_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let (state, stream) = key.maintain(runner).await;
                    let _ = state_tx.send(state);
                    let _ = stream_tx.send(stream);
                });
                let (sender, _) = broadcast::channel(1_024); // usize::MAX causes a “assertion failed: permits <= MAX_PERMITS” panic
                let state = RwFuture::new(async move {
                    (state_rx.await.expect("no state was sent"), sender)
                });
                let state_clone = state.clone();
                tokio::spawn(async move {
                    let mut stream = stream_rx.await.expect("no stream was sent");
                    while let Some(delta) = stream.next().await {
                        let mut write_guard = state_clone.write().await;
                        delta.apply(&mut write_guard.0);
                        if write_guard.1.send(delta).is_err() { break }
                    }
                });
                let handle = Handle { state };
                entry.insert(handle.clone());
                handle
            }
        }
    }
}

/// Starts keeping the state for this node up to date in the background.
///
/// If you need the state of multiple nodes, you should instead create a [`Runner`] and use its [`subscribe`](Runner::subscribe) method for each node you need.
/// This avoids redundant copies of shared dependencies of the nodes.
pub async fn run<K: Key>(key: K) -> Handle<K> {
    Runner::default().subscribe(key).await
}

/// If the expression evaluates to `Err`, returns the error as the initial state and a stream with no elements.
#[macro_export] macro_rules! maintain_try {
    ($res:expr) => {
        match $res {
            Ok(x) => x,
            Err(e) => return (Err(e.into()), ::futures::stream::StreamExt::boxed(::futures::stream::empty()))
        }
    };
}
