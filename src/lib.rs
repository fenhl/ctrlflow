#![deny(rust_2018_idioms, unused, unused_crate_dependencies, unused_import_braces, unused_qualifications, warnings)]
#![forbid(unsafe_code)]

use {
    std::{
        collections::{
            HashMap,
            hash_map,
        },
        hash::Hash,
        marker::PhantomData,
        pin::Pin,
        sync::Arc,
    },
    async_stream::stream,
    derivative::Derivative,
    futures::{
        future::Future,
        stream::{
            Stream,
            StreamExt as _,
        },
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
    fn maintain(self, runner: RunnerInternal) -> Pin<Box<dyn Future<Output = (Self::State, Pin<Box<dyn Stream<Item = Self::Delta> + Send>>)> + Send>>;
}

struct HandleMapKey<K: Key>(PhantomData<K>);

impl<K: Key> TypeMapKey for HandleMapKey<K> {
    type Value = HashMap<K, Handle<K>>;
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
    pub fn states(&self) -> impl Stream<Item = K::State> + '_
    where K::State: Clone + Unpin {
        stream! {
            let (init, mut deltas) = self.stream().await;
            let mut state = init.clone();
            yield state.clone();
            loop {
                match deltas.recv().await {
                    Ok(delta) => {
                        delta.apply(&mut state);
                        yield state.clone();
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        }
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

/// An internal handle to a runner passed to the [`Key::maintain`] method.
///
/// Nodes subscribed to from here will be tracked as internal dependencies.
pub struct RunnerInternal {
    runner: Runner,
}

impl RunnerInternal {
    pub async fn subscribe<K: Key>(&self, key: K) -> Result<Handle<K>, DependencyLoop> {
        //TODO check for dependency loops
        Ok(self.runner.subscribe(key).await)
    }
}

#[derive(Clone)]
pub struct Runner {
    map: Arc<Mutex<TypeMap>>,
}

impl Runner {
    pub async fn subscribe<K: Key>(&self, key: K) -> Handle<K> {
        match self.map.lock().await.entry::<HandleMapKey<K>>().or_default().entry(key.clone()) {
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
            hash_map::Entry::Vacant(entry) => {
                let runner = RunnerInternal { runner: self.clone() };
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

impl Default for Runner {
    fn default() -> Runner {
        Runner {
            map: Arc::new(Mutex::new(TypeMap::new())),
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
