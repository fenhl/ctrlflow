#![deny(rust_2018_idioms, unused, unused_crate_dependencies, unused_import_braces, unused_lifetimes, unused_qualifications, warnings)] //TODO missing_docs
#![forbid(unsafe_code)]

use {
    std::{
        any::Any,
        collections::{
            hash_map::{
                self,
                HashMap,
            },
            HashSet,
        },
        hash::Hash,
        iter,
        mem,
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

pub struct Dependencies<KD: Key> {
    runner: Runner,
    key: KD,
    new: HashSet<AnyKey>,
}

impl<KD: Key> Dependencies<KD> {
    pub async fn get<KU: Key>(&mut self, key: KU) -> KU::State {
        self.new.insert(AnyKey::new(key.clone()));
        let mut rx = match self.runner.map.lock().entry(AnyKey::new(key.clone())) {
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
                    updating: Updating::No,
                    state: None,
                    dependents: iter::once(AnyKey::new(self.key.clone())).collect(),
                    dependencies: HashSet::default(),
                    tx,
                }));
                self.runner.clone().start_maintaining(key);
                rx
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

    pub fn try_get<KU: Key>(&mut self, key: KU) -> Option<KU::State> {
        self.new.insert(AnyKey::new(key.clone()));
        match self.runner.map.lock().entry(AnyKey::new(key.clone())) {
            hash_map::Entry::Occupied(mut entry) => {
                let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                handle.dependents.insert(AnyKey::new(self.key.clone()));
                if let Some(ref state) = handle.state {
                    return Some(state.clone())
                }
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(Handle::<KU> {
                    updating: Updating::No,
                    state: None,
                    tx: broadcast::channel(CHANNEL_CAPACITY).0,
                    dependents: iter::once(AnyKey::new(self.key.clone())).collect(),
                    dependencies: HashSet::default(),
                }));
                self.runner.clone().start_maintaining(key);
            }
        }
        None
    }
}

enum Updating {
    No,
    Yes,
    /// A dependency updated while this was updating, so another update should be run after the current one finishes.
    Queued,
}

struct Handle<K: Key> {
    updating: Updating,
    state: Option<K::State>,
    tx: broadcast::Sender<K::State>,
    dependents: HashSet<AnyKey>,
    dependencies: HashSet<AnyKey>,
}

#[derive(Default, Clone)]
pub struct Runner {
    map: Arc<Mutex<HashMap<AnyKey, Box<dyn Any + Send>>>>,
}

impl Runner {
    fn update_derived_state<K: Key>(&self, key: K) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        enum UpdateMode<K: Key> {
            Active {
                previous: Option<K::State>,
            },
            Passive {
                rx: broadcast::Receiver<K::State>,
            },
        }

        let runner = self.clone();
        Box::pin(async move {
            let mut deps = Dependencies {
                runner: runner.clone(),
                key: key.clone(),
                new: HashSet::default(),
            };
            let mode = {
                let mut map = runner.map.lock();
                let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else { return };
                let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                match handle.updating {
                    Updating::No => {
                        handle.updating = Updating::Yes;
                        UpdateMode::Active::<K> { previous: handle.state.clone() }
                    }
                    Updating::Yes => {
                        handle.updating = Updating::Queued;
                        UpdateMode::Passive { rx: handle.tx.subscribe() }
                    }
                    Updating::Queued => UpdateMode::Passive { rx: handle.tx.subscribe() },
                }
            };
            match mode {
                UpdateMode::Active { previous } => {
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
                        if handle.tx.send(new_state).is_ok() {
                            any_notified = true;
                        }
                        for dependent in &handle.dependents {
                            tokio::spawn((dependent.update)(&runner));
                            any_notified = true;
                        }
                        if any_notified {
                            for dep in handle.dependencies.difference(&deps.new) {
                                (dep.delete_dependent)(&runner, AnyKey::new(key.clone()));
                            }
                            handle.dependencies = deps.new;
                            if let Updating::Queued = mem::replace(&mut handle.updating, Updating::No) {
                                drop(map);
                                tokio::spawn(async move { runner.update_derived_state(key).await });
                            }
                        } else {
                            for dep in handle.dependencies.union(&deps.new) {
                                (dep.delete_dependent)(&runner, AnyKey::new(key.clone()));
                            }
                            map.remove(&AnyKey::new(key));
                        }
                    }
                }
                UpdateMode::Passive { mut rx } => while rx.recv().await.is_err() {},
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
                    if handle.tx.send(new_state).is_ok() {
                        any_notified = true;
                    }
                    for dependent in &handle.dependents {
                        tokio::spawn((dependent.update)(&self));
                        any_notified = true;
                    }
                    if !any_notified {
                        map.remove(&AnyKey::new(key));
                        break
                    }
                }
            }),
            Maintenance::Derived(_) => tokio::spawn(async move { self.update_derived_state(key).await }),
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
                    updating: Updating::No,
                    state: None,
                    dependents: HashSet::default(),
                    dependencies: HashSet::default(),
                    tx,
                }));
                self.clone().start_maintaining(key);
                BroadcastStream::new(rx).filter_map(|res| async move { res.ok() }).right_stream()
            },
        }
    }
}
