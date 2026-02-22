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
        fmt,
        hash::Hash,
        iter,
        pin::Pin,
        sync::Arc,
    },
    futures::{
        future::{
            self,
            Future,
        },
        stream::{
            self,
            StreamExt as _,
        },
    },
    infinite_stream::{
        InfiniteStream,
        InfiniteStreamExt as _,
        StreamExt as _,
    },
    log_lock::*,
    tokio::sync::broadcast,
    tokio_stream::wrappers::BroadcastStream,
    crate::dynamic::AnyKey,
};

mod dynamic;

const CHANNEL_CAPACITY: usize = 256;

pub trait Key: fmt::Debug + Clone + Eq + Hash + Send + Sync + 'static {
    type State: fmt::Debug + Clone + Send + Sync;

    /// This function must consistently return the same variant of [`Maintenance`] for the same key, i.e. a source must never become derived or vice versa.
    fn maintain(&self) -> Maintenance<Self>;
}

pub enum Maintenance<K: Key> {
    Stream(Box<dyn FnOnce(Runner) -> Pin<Box<dyn InfiniteStream<Item = K::State> + Send>> + Send>),
    Derived(Box<dyn for<'d> FnOnce(&'d mut Dependencies<K>, Option<K::State>) -> Pin<Box<dyn Future<Output = Option<K::State>> + Send + 'd>> + Send>),
}

pub fn filter_eq<K: Key>(f: impl for<'d> FnOnce(&'d mut Dependencies<K>, Option<&K::State>) -> Pin<Box<dyn Future<Output = K::State> + Send + 'd>> + Send + 'static) -> Maintenance<K>
where K::State: PartialEq {
    Maintenance::Derived(Box::new(|dependencies, previous| Box::pin(async move {
        let next = f(dependencies, previous.as_ref()).await;
        previous.map_or(true, |previous| next != previous).then_some(next)
    })))
}

/// Return value of [`Dependencies::get_next`] and [`Dependencies::try_get_next`].
///
/// See those functions' docs for details.
pub enum Next<K: Key> {
    /// The state has not changed since the previous `get_next`/`try_get_next` call. Contains a copy of that state.
    Old(K::State),
    /// The state has changed since the previous `get_next`/`try_get_next` call. Contains the new state.
    New(K::State),
}

/// Calling one of the methods on this type registers the key passed as a parameter as a dependency of the key from which it is being called, so if the dependency changes, the dependent will also be recomputed.
#[derive(Debug)]
pub struct Dependencies<KD: Key> {
    runner: Runner,
    key: KD,
    new: HashSet<AnyKey>,
}

impl<KD: Key> Dependencies<KD> {
    /// Returns the current state of the given key.
    ///
    /// If there is not already a known state for the key, this waits until it is computed.
    pub fn get_latest<KU: Key>(&mut self, key: KU) -> impl Future<Output = KU::State> + use<KU, KD> {
        self.new.insert(AnyKey::new(key.clone()));
        let runner = self.runner.clone();
        let dependency_key = self.key.clone();
        async move {
            let mut rx = 'lock: {
                lock!(@sync map = runner.map; format!("ctrlflow::Dependencies {{ key: {dependency_key:?}, .. }}.get_latest({key:?})"); {
                    if let Some(handle) = map.get_mut(&AnyKey::new(dependency_key.clone())) {
                        let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
                        if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                            let state = queue.pop_back();
                            queue.clear();
                            if let Some(state) = state {
                                unlock!();
                                return *state.downcast::<KU::State>().expect("queued dependency type mismatch")
                            }
                        }
                    }
                    match map.entry(AnyKey::new(key.clone())) {
                        hash_map::Entry::Occupied(mut entry) => {
                            let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                            handle.dependents.insert(AnyKey::new(dependency_key));
                            if let Some(ref state) = handle.state {
                                let state = state.clone();
                                unlock!();
                                return state
                            } else {
                                handle.tx.subscribe()
                            }
                        }
                        hash_map::Entry::Vacant(entry) => {
                            let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
                            entry.insert(Box::new(Handle::<KU> {
                                updating: false,
                                state: None,
                                dependents: iter::once(AnyKey::new(dependency_key)).collect(),
                                dependencies: HashMap::default(),
                                tx,
                            }));
                            unlock!();
                            runner.clone().start_maintaining(key);
                            break 'lock rx
                        }
                    }
                })
            };
            loop {
                match rx.recv().await {
                    Ok(state) => break state,
                    Err(broadcast::error::RecvError::Closed) => panic!("channel closed with active dependency"),
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        }
    }

    /// Returns the current state of the given key.
    ///
    /// If there is not already a known state for the key, this returns `None`.
    pub fn try_get_latest<KU: Key>(&mut self, key: KU) -> Option<KU::State> {
        self.new.insert(AnyKey::new(key.clone()));
        lock!(@sync map = self.runner.map; format!("ctrlflow::Dependencies {{ key: {:?}, .. }}.try_get_latest({key:?})", self.key); {
            if let Some(handle) = map.get_mut(&AnyKey::new(self.key.clone())) {
                let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
                if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                    let state = queue.pop_back();
                    queue.clear();
                    if let Some(state) = state {
                        unlock!();
                        return Some(*state.downcast::<KU::State>().expect("queued dependency type mismatch"))
                    }
                }
            }
            match map.entry(AnyKey::new(key.clone())) {
                hash_map::Entry::Occupied(mut entry) => {
                    let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                    handle.dependents.insert(AnyKey::new(self.key.clone()));
                    if let Some(ref state) = handle.state {
                        let state = state.clone();
                        unlock!();
                        return Some(state)
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
                    unlock!();
                    self.runner.clone().start_maintaining(key);
                    return None
                }
            }
            None
        })
    }

    /// Returns the next state of the given key.
    ///
    /// To ensure that no intermediate states are skipped, this must be called each time, and must not be mixed with `get_latest` or `try_get_latest` calls on the same key.
    ///
    /// If there is not already a known state for the key, this waits until it is computed.
    pub fn get_next<KU: Key>(&mut self, key: KU) -> impl Future<Output = Next<KU>> + use<KU, KD> {
        self.new.insert(AnyKey::new(key.clone()));
        let runner = self.runner.clone();
        let dependency_key = self.key.clone();
        async move {
            let mut rx = 'lock: {
                lock!(@sync map = runner.map; format!("ctrlflow::Dependencies {{ key: {dependency_key:?}, .. }}.get_next({key:?})"); {
                    if let Some(handle) = map.get_mut(&AnyKey::new(dependency_key.clone())) {
                        let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
                        if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                            if let Some(state) = queue.pop_front() {
                                unlock!();
                                return Next::New(*state.downcast::<KU::State>().expect("queued dependency type mismatch"))
                            }
                        }
                    }
                    match map.entry(AnyKey::new(key.clone())) {
                        hash_map::Entry::Occupied(mut entry) => {
                            let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                            handle.dependents.insert(AnyKey::new(dependency_key.clone()));
                            if let Some(ref state) = handle.state {
                                let state = state.clone();
                                unlock!();
                                return Next::Old(state)
                            } else {
                                handle.tx.subscribe()
                            }
                        }
                        hash_map::Entry::Vacant(entry) => {
                            let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
                            entry.insert(Box::new(Handle::<KU> {
                                updating: false,
                                state: None,
                                dependents: iter::once(AnyKey::new(dependency_key.clone())).collect(),
                                dependencies: HashMap::default(),
                                tx,
                            }));
                            unlock!();
                            runner.clone().start_maintaining(key.clone());
                            break 'lock rx
                        }
                    }
                })
            };
            let state = rx.recv().await.unwrap();
            lock!(@sync map = runner.map; format!("ctrlflow::Dependencies {{ key: {dependency_key:?}, .. }}.get_next({key:?})"); {
                let handle = map.get_mut(&AnyKey::new(dependency_key)).unwrap();
                let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
                let queue = handle.dependencies.get_mut(&AnyKey::new(key)).unwrap();
                queue.pop_front().unwrap(); // don't report this state twice; calling `dependent.update` before `handle.tx.send` ensures this state is already present
            });
            Next::New(state)
        }
    }

    /// Returns the next state of the given key.
    ///
    /// To ensure that no intermediate states are skipped, this must be called each time, and must not be mixed with `get_latest` or `try_get_latest` calls on the same key.
    ///
    /// If there is not already a known state for the key, this returns `None`.
    pub fn try_get_next<KU: Key>(&mut self, key: KU) -> Option<Next<KU>> {
        self.new.insert(AnyKey::new(key.clone()));
        lock!(@sync map = self.runner.map; format!("ctrlflow::Dependencies {{ key: {:?}, .. }}.try_get_next({key:?})", self.key); {
            if let Some(handle) = map.get_mut(&AnyKey::new(self.key.clone())) {
                let handle = handle.downcast_mut::<Handle<KD>>().expect("handle type mismatch");
                if let Some(queue) = handle.dependencies.get_mut(&AnyKey::new(key.clone())) {
                    if let Some(state) = queue.pop_front() {
                        unlock!();
                        return Some(Next::New(*state.downcast::<KU::State>().expect("queued dependency type mismatch")))
                    }
                }
            }
            match map.entry(AnyKey::new(key.clone())) {
                hash_map::Entry::Occupied(mut entry) => {
                    let handle = entry.get_mut().downcast_mut::<Handle<KU>>().expect("handle type mismatch");
                    handle.dependents.insert(AnyKey::new(self.key.clone()));
                    if let Some(ref state) = handle.state {
                        let state = state.clone();
                        unlock!();
                        return Some(Next::Old(state))
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
                    unlock!();
                    self.runner.clone().start_maintaining(key);
                    return None
                }
            }
            None
        })
    }
}

struct Handle<K: Key> {
    updating: bool,
    state: Option<K::State>,
    tx: broadcast::Sender<K::State>,
    dependents: HashSet<AnyKey>,
    dependencies: HashMap<AnyKey, VecDeque<Box<dyn Any + Send>>>,
}

#[derive(Debug, Default, Clone)]
pub struct Runner {
    map: Arc<ParkingLotMutex<HashMap<AnyKey, Box<dyn Any + Send>>>>,
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
            let previous = lock!(@sync map = runner.map; format!("ctrlflow::Runner {{ .. }}.update_derived_state({key:?})"); {
                let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else { return };
                let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                if handle.updating {
                    unlock!();
                    return
                }
                handle.updating = true;
                handle.state.clone()
            });
            let Maintenance::Derived(get_state) = key.maintain() else { panic!("derived key turned into source") };
            if let Some(new_state) = get_state(&mut deps, previous).await {
                lock!(@sync map = runner.map; format!("ctrlflow::Runner {{ .. }}.update_derived_state({key:?})"); {
                    let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else {
                        // no subscribers and no dependents
                        for dep in deps.new {
                            (dep.delete_dependent)(&mut map, AnyKey::new(key.clone()));
                        }
                        unlock!();
                        return
                    };
                    let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                    handle.state = Some(new_state.clone());
                    let mut any_notified = false;
                    for update in handle.dependents.iter().map(|dependent| dependent.update.clone()).collect::<Vec<_>>() {
                        update(&runner, &mut map, AnyKey::new(key.clone()), Box::new(new_state.clone()));
                        any_notified = true;
                    }
                    let handle = map.get_mut(&AnyKey::new(key.clone())).expect("checked above").downcast_mut::<Handle<K>>().expect("handle type mismatch");
                    if handle.tx.send(new_state.clone()).is_ok() {
                        any_notified = true;
                    }
                    if any_notified {
                        let dependencies_to_delete;
                        (handle.dependencies, dependencies_to_delete) = handle.dependencies.drain().partition(|(dep, _)| deps.new.contains(dep));
                        handle.updating = false;
                        let re_run = handle.dependencies.values().any(|queue| !queue.is_empty());
                        for (dep, _) in dependencies_to_delete {
                            (dep.delete_dependent)(&mut map, AnyKey::new(key.clone()));
                        }
                        if re_run {
                            tokio::spawn(runner.update_derived_state(key));
                        }
                    } else {
                        for dep in handle.dependencies.drain().map(|(key, _)| key).chain(deps.new).collect::<Vec<_>>() {
                            (dep.delete_dependent)(&mut map, AnyKey::new(key.clone()));
                        }
                        map.remove(&AnyKey::new(key));
                    }
                });
            } else {
                lock!(@sync map = runner.map; format!("ctrlflow::Runner {{ .. }}.update_derived_state({key:?})"); {
                    let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else {
                        // no subscribers and no dependents
                        for dep in deps.new {
                            (dep.delete_dependent)(&mut map, AnyKey::new(key.clone()));
                        }
                        unlock!();
                        return
                    };
                    let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                    let has_dependents = !handle.dependents.is_empty() || handle.tx.receiver_count() > 0;
                    if has_dependents {
                        let dependencies_to_delete;
                        (handle.dependencies, dependencies_to_delete) = handle.dependencies.drain().partition(|(dep, _)| deps.new.contains(dep));
                        handle.updating = false;
                        let re_run = handle.dependencies.values().any(|queue| !queue.is_empty());
                        for (dep, _) in dependencies_to_delete {
                            (dep.delete_dependent)(&mut map, AnyKey::new(key.clone()));
                        }
                        if re_run {
                            tokio::spawn(runner.update_derived_state(key));
                        }
                    } else {
                        for dep in handle.dependencies.drain().map(|(key, _)| key).chain(deps.new).collect::<Vec<_>>() {
                            (dep.delete_dependent)(&mut map, AnyKey::new(key.clone()));
                        }
                        map.remove(&AnyKey::new(key));
                    }
                });
            }
        })
    }

    fn start_maintaining<K: Key>(self, key: K) -> tokio::task::JoinHandle<()> {
        match key.maintain() {
            Maintenance::Stream(stream_fn) => {
                let mut stream = stream_fn(self.clone());
                tokio::spawn(async move {
                    loop {
                        let new_state = stream.next().await;
                        lock!(@sync map = self.map; format!("ctrlflow::Runner {{ .. }}.start_maintaining({key:?})"); {
                            let Some(handle) = map.get_mut(&AnyKey::new(key.clone())) else { break };
                            let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                            handle.state = Some(new_state.clone());
                            let mut any_notified = false;
                            for update in handle.dependents.iter().map(|dependent| dependent.update.clone()).collect::<Vec<_>>() {
                                update(&self, &mut map, AnyKey::new(key.clone()), Box::new(new_state.clone()));
                                any_notified = true;
                            }
                            let handle = map.get_mut(&AnyKey::new(key.clone())).expect("checked above").downcast_mut::<Handle<K>>().expect("handle type mismatch");
                            if handle.tx.send(new_state.clone()).is_ok() {
                                any_notified = true;
                            }
                            if !any_notified {
                                map.remove(&AnyKey::new(key));
                                unlock!();
                                break
                            }
                        });
                    }
                })
            }
            Maintenance::Derived(_) => tokio::spawn(self.update_derived_state(key)),
        }
    }

    pub fn subscribe<K: Key>(&self, key: K) -> impl InfiniteStream<Item = K::State> + Unpin + use<K> {
        lock!(@sync map = self.map; format!("ctrlflow::Runner {{ .. }}.subscribe({key:?})"); match map.entry(AnyKey::new(key.clone())) {
            hash_map::Entry::Occupied(entry) => {
                let handle = entry.get().downcast_ref::<Handle<K>>().expect("handle type mismatch");
                stream::iter(handle.state.clone())
                    .chain(BroadcastStream::new(handle.tx.subscribe()).filter_map(|res| future::ready(res.ok())))
                    .expect("broadcast ended")
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
                unlock!();
                self.clone().start_maintaining(key);
                return BroadcastStream::new(rx).filter_map(|res| future::ready(res.ok())).expect("broadcast ended").right_stream()
            },
        })
    }
}
