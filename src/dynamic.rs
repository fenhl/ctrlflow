use {
    std::{
        any::Any,
        collections::HashMap,
        fmt,
        hash::{
            BuildHasher as _,
            BuildHasherDefault,
            DefaultHasher,
            Hash,
            Hasher,
        },
        pin::Pin,
    },
    futures::future::Future,
    log_lock::*,
    crate::{
        Handle,
        Key,
        Runner,
    },
};

pub(crate) struct AnyKey {
    data: Box<dyn Any + Send + Sync>,
    eq: Box<dyn Fn(&dyn Any) -> bool + Send + Sync>,
    hash: u64,
    pub(crate) update: Box<dyn Fn(&Runner, AnyKey, Box<dyn Any + Send>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    pub(crate) delete_dependent: Box<dyn Fn(&mut HashMap<AnyKey, Box<dyn Any + Send>>, AnyKey) + Send + Sync>,
}

impl AnyKey {
    pub(crate) fn new<K: Key>(key: K) -> Self {
        let self_eq = key.clone();
        let self_hash = key.clone();
        let self_update = key.clone();
        let self_delete_dependent = key.clone();
        Self {
            data: Box::new(key),
            eq: Box::new(move |other| other.downcast_ref::<K>().map_or(false, |other| self_eq == *other)),
            hash: BuildHasherDefault::<DefaultHasher>::default().hash_one(self_hash),
            update: Box::new(move |runner, dependency_key, dependency_value| {
                let runner = runner.clone();
                let self_update = self_update.clone();
                Box::pin(async move {
                    let should_update = lock!(@sync map = runner.map; format!("ctrlflow::AnyKey::new({self_update:?})"); {
                        if let Some(handle) = map.get_mut(&AnyKey::new(self_update.clone())) {
                            let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                            handle.dependencies.entry(dependency_key).or_default().push_back(dependency_value);
                            true
                        } else {
                            false
                        }
                    });
                    if should_update {
                        runner.update_derived_state(self_update.clone()).await;
                    }
                })
            }),
            delete_dependent: Box::new(move |map, dependent_key| if let Some(handle) = map.get_mut(&AnyKey::new(self_delete_dependent.clone())) {
                let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                handle.dependents.remove(&dependent_key);
            }),
        }
    }
}

impl fmt::Debug for AnyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { data, eq: _, hash, update: _, delete_dependent: _ } = self;
        f.debug_struct("AnyKey")
            .field("data", data)
            .field("eq", &format_args!("_"))
            .field("hash", hash)
            .field("update", &format_args!("_"))
            .field("delete_dependent", &format_args!("_"))
            .finish()
    }
}

impl PartialEq for AnyKey {
    fn eq(&self, other: &Self) -> bool {
        (self.eq)(&*other.data)
    }
}

impl Eq for AnyKey {}

impl Hash for AnyKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self.data).type_id().hash(state);
        state.write_u64(self.hash);
    }
}
