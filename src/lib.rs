#![deny(rust_2018_idioms, unused, unused_crate_dependencies, unused_import_braces, unused_lifetimes, unused_qualifications, warnings)] //TODO missing_docs
#![forbid(unsafe_code)]

use {
    std::{
        any::Any,
        collections::{
            HashSet,
            VecDeque,
            hash_map::{
                self,
                HashMap,
            },
        },
        hash::Hash,
        iter,
        pin::Pin,
        sync::Arc,
    },
    futures::{
        future::Future,
        stream::{
            self,
            Stream,
            StreamExt as _,
        },
    },
    parking_lot::Mutex,
    tokio::sync::broadcast,
    tokio_stream::wrappers::BroadcastStream,
    crate::dynamic::AnyKey,
};

mod dynamic;

const CHANNEL_CAPACITY: usize = 256;

pub trait Key: Clone + Eq + Hash + Send + 'static {
    type State: Clone + Send + Sync;

    /// This function must consistently return the same variant of [`Maintenance`] for the same key, i.e. a source must never become derived or vice versa.
    fn maintain(&self) -> Maintenance<Self>;
}

pub enum Maintenance<K: Key> {
    Source(Pin<Box<dyn Stream<Item = K::State> + Unpin + Send>>),
    Derived(Box<dyn FnOnce(&mut Dependencies<K>, Option<K::State>) -> Pin<Box<dyn Future<Output = Option<K::State>> + Send>> + Send>),
}

/*
pub fn filter_eq<K: Key>(f: impl FnOnce(&mut Dependencies<K>, Option<&K::State>) -> Pin<Box<dyn Future<Output = K::State> + Send + 'a>> + Send + 'static) -> Maintenance<K>
where K::State: PartialEq {
    Maintenance::Derived(Box::new(|dependencies, previous| Box::pin(async move {
        let next = f(dependencies, previous.as_ref()).await;
        previous.map_or(false, |previous| next != previous).then_some(next)
    })))
}
*/

#[derive(Debug, thiserror::Error)]
#[error("gap in dependency states")]
pub struct Lagged;

/// Calling one of the methods on this type registers the key passed as a parameter as a dependency of the key from which it is being called, so if the dependency changes, the dependent will also be recomputed.
pub struct Dependencies<KD: Key> {
    runner: Runner,
    key: KD,
    new: HashSet<AnyKey>,
}

impl<KD: Key> Dependencies<KD> {
    /// Returns the current state of the given key.
    ///
    /// If there is not already a known state for the key, this waits until it is computed.
    pub async fn get_latest<KU: Key>(&mut self, key: KU) -> KU::State {
        self.new.insert(AnyKey::new(key.clone()));
        let mut rx = {
            let mut map = self.runner.map.lock();
            if let Some(handle) = map.get_mut(&AnyKey::new(self.key.clone())) {
                let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
                if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                    let state = queue.pop_back();
                    queue.clear();
                    if let Some(state) = state {
                        return *state.downcast().expect("queued dependency type mismatch")
                    }
                }
            }
            match map.entry(AnyKey::new(key.clone())) {
                hash_map::Entry::Occupied(mut entry) => {
                    let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                    handle.dependents.insert(AnyKey::new(self.key.clone()));
                    if let Some(ref state) = handle.state {
                        return state.clone()
                    } else {
                        handle.tx.subscribe()
                    }
                }
                hash_map::Entry::Vacant(entry) => {
                    let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
                    entry.insert(Box::new(Handle::<KU> {
                        updating: false,
                        state: None,
                        dependents: iter::once(AnyKey::new(self.key.clone())).collect(),
                        dependencies: HashMap::default(),
                        tx,
                    }));
                    self.runner.clone().start_maintaining(key);
                    rx
                }
            }
        };
        loop {
            match rx.recv().await {
                Ok(state) => break state,
                Err(broadcast::error::RecvError::Closed) => panic!("channel closed with active dependency"),
                Err(broadcast::error::RecvError::Lagged(_)) => {}
            }
        }
    }

    /// Returns the current state of the given key.
    ///
    /// If there is not already a known state for the key, this returns `None`.
    pub fn try_get_latest<KU: Key>(&mut self, key: KU) -> Option<KU::State> {
        self.new.insert(AnyKey::new(key.clone()));
        let mut map = self.runner.map.lock();
        if let Some(handle) = map.get_mut(&AnyKey::new(self.key.clone())) {
            let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
            if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                let state = queue.pop_back();
                queue.clear();
                if let Some(state) = state {
                    return *state.downcast().expect("queued dependency type mismatch")
                }
            }
        }
        match map.entry(AnyKey::new(key.clone())) {
            hash_map::Entry::Occupied(mut entry) => {
                let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                handle.dependents.insert(AnyKey::new(self.key.clone()));
                if let Some(ref state) = handle.state {
                    return Some(state.clone())
                }
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(Handle::<KU> {
                    updating: false,
                    state: None,
                    tx: broadcast::channel(CHANNEL_CAPACITY).0,
                    dependents: iter::once(AnyKey::new(self.key.clone())).collect(),
                    dependencies: HashMap::default(),
                }));
                self.runner.clone().start_maintaining(key);
            }
        }
        None
    }

    /// Returns the next state of the given key.
    ///
    /// To ensure that no intermediate states are skipped, this must be called each time, and must not be mixed with `get_latest` or `try_get_latest` calls on the same key.
    ///
    /// If there is not already a known state for the key, this waits until it is computed.
    pub async fn get_next<KU: Key>(&mut self, key: KU) -> Result<KU::State, Lagged> {
        self.new.insert(AnyKey::new(key.clone()));
        let mut rx = {
            let mut map = self.runner.map.lock();
            if let Some(handle) = map.get_mut(&AnyKey::new(self.key.clone())) {
                let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
                if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                    if let Some(state) = queue.pop_front() {
                        return *state.downcast().expect("queued dependency type mismatch")
                    }
                }
            }
            match map.entry(AnyKey::new(key.clone())) {
                hash_map::Entry::Occupied(mut entry) => {
                    let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                    handle.dependents.insert(AnyKey::new(self.key.clone()));
                    if let Some(ref state) = handle.state {
                        return Ok(state.clone())
                    } else {
                        handle.tx.subscribe()
                    }
                }
                hash_map::Entry::Vacant(entry) => {
                    let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
                    entry.insert(Box::new(Handle::<KU> {
                        updating: false,
                        state: None,
                        dependents: iter::once(AnyKey::new(self.key.clone())).collect(),
                        dependencies: HashMap::default(),
                        tx,
                    }));
                    self.runner.clone().start_maintaining(key);
                    rx
                }
            }
        };
        match rx.recv().await {
            Ok(state) => Ok(state),
            Err(broadcast::error::RecvError::Closed) => panic!("channel closed with active dependency"),
            Err(broadcast::error::RecvError::Lagged(_)) => Err(Lagged),
        }
    }

    /// Returns the next state of the given key.
    ///
    /// To ensure that no intermediate states are skipped, this must be called each time, and must not be mixed with `get_latest` or `try_get_latest` calls on the same key.
    ///
    /// If there is not already a known state for the key, this returns `Ok(None)`.
    pub fn try_get_next<KU: Key>(&mut self, key: KU) -> Result<Option<KU::State>, Lagged> {
        self.new.insert(AnyKey::new(key.clone()));
        let mut map = self.runner.map.lock();
        if let Some(handle) = map.get_mut(&AnyKey::new(self.key.clone())) {
            let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
            if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                if let Some(state) = queue.pop_front() {
                    return *state.downcast().expect("queued dependency type mismatch")
                }
            }
        }
        match map.entry(AnyKey::new(key.clone())) {
            hash_map::Entry::Occupied(mut entry) => {
                let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                handle.dependents.insert(AnyKey::new(self.key.clone()));
                if let Some(ref state) = handle.state {
                    return Ok(Some(state.clone()))
                }
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(Handle::<KU> {
                    updating: false,
                    state: None,
                    tx: broadcast::channel(CHANNEL_CAPACITY).0,
                    dependents: iter::once(AnyKey::new(self.key.clone())).collect(),
                    dependencies: HashMap::default(),
                }));
                self.runner.clone().start_maintaining(key);
            }
        }
        Ok(None)
    }
}

struct Handle<K: Key> {
    updating: bool,
    state: Option<K::State>,
    tx: broadcast::Sender<K::State>,
    dependents: HashSet<AnyKey>,
    dependencies: HashMap<AnyKey, VecDeque<Box<dyn Any + Send>>>,
}

#[derive(Default, Clone)]
pub struct Runner {
    map: Arc<Mutex<HashMap<AnyKey, Box<dyn Any + Send>>>>,
}

impl Runner {
    fn update_derived_state<K: Key>(&self, key: K) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let runner = self.clone();
        Box::pin(async move {
            let mut deps = Dependencies {
                runner: runner.clone(),
                key: key.clone(),
                new: HashSet::default(),
            };
            let previous = {
                let mut map = runner.map.lock();
                let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else { return };
                let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                if handle.updating { return }
                handle.updating = true;
                handle.state.clone()
            };
            let Maintenance::Derived(get_state) = key.maintain() else { panic!("derived key turned into source") };
            if let Some(new_state) = get_state(&mut deps, previous).await {
                let mut map = runner.map.lock();
                let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else {
                    // no subscribers and no dependents
                    for dep in deps.new {
                        (dep.delete_dependent)(&runner, AnyKey::new(key.clone()));
                    }
                    return
                };
                let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                handle.state = Some(new_state.clone());
                let mut any_notified = false;
                if handle.tx.send(new_state.clone()).is_ok() {
                    any_notified = true;
                }
                for dependent in &handle.dependents {
                    tokio::spawn((dependent.update)(&runner, AnyKey::new(key.clone()), Box::new(new_state.clone())));
                    any_notified = true;
                }
                if any_notified {
                    handle.dependencies.retain(|dep, _| if deps.new.contains(dep) {
                        true
                    } else {
                        (dep.delete_dependent)(&runner, AnyKey::new(key.clone()));
                        false
                    });
                    for dep in deps.new {
                        if let hash_map::Entry::Vacant(entry) = handle.dependencies.entry(dep) {
                            entry.insert(VecDeque::default());
                        }
                    }
                    handle.updating = false;
                    if handle.dependencies.values().any(|queue| !queue.is_empty()) {
                        tokio::spawn(runner.update_derived_state(key));
                    }
                } else {
                    for dep in handle.dependencies.keys().chain(&deps.new) {
                        (dep.delete_dependent)(&runner, AnyKey::new(key.clone()));
                    }
                    map.remove(&AnyKey::new(key));
                }
            }
        })
    }

    fn start_maintaining<K: Key>(self, key: K) -> tokio::task::JoinHandle<()> {
        match key.maintain() {
            Maintenance::Source(mut stream) => tokio::spawn(async move {
                while let Some(new_state) = stream.next().await {
                    let mut map = self.map.lock();
                    let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else { break };
                    let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                    handle.state = Some(new_state.clone());
                    let mut any_notified = false;
                    if handle.tx.send(new_state.clone()).is_ok() {
                        any_notified = true;
                    }
                    for dependent in &handle.dependents {
                        tokio::spawn((dependent.update)(&self, AnyKey::new(key.clone()), Box::new(new_state.clone())));
                        any_notified = true;
                    }
                    if !any_notified {
                        map.remove(&AnyKey::new(key));
                        break
                    }
                }
            }),
            Maintenance::Derived(_) => tokio::spawn(self.update_derived_state(key)),
        }
    }

    pub fn subscribe<K: Key>(&self, key: K) -> impl Stream<Item = K::State> {
        match self.map.lock().entry(AnyKey::new(key.clone())) {
            hash_map::Entry::Occupied(entry) => {
                let handle = entry.get().downcast_ref::<Handle<K>>().expect("handle type mismatch");
                stream::iter(handle.state.clone())
                    .chain(BroadcastStream::new(handle.tx.subscribe()).filter_map(|res| async move { res.ok() }))
                    .left_stream()
            }
            hash_map::Entry::Vacant(entry) => {
                let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
                entry.insert(Box::new(Handle::<K> {
                    updating: false,
                    state: None,
                    dependents: HashSet::default(),
                    dependencies: HashMap::default(),
                    tx,
                }));
                self.clone().start_maintaining(key);
                BroadcastStream::new(rx).filter_map(|res| async move { res.ok() }).right_stream()
            },
        }
    }
}
