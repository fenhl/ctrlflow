use {
    std::{
        any::Any,
        collections::hash_map::DefaultHasher,
        hash::{
            BuildHasher as _,
            BuildHasherDefault,
            Hash,
            Hasher,
        },
        pin::Pin,
    },
    futures::future::Future,
    crate::{
        Handle,
        Key,
        Runner,
    },
};

pub(crate) struct AnyKey {
    data: Box<dyn Any + Send>,
    eq: Box<dyn Fn(&dyn Any) -> bool + Send>,
    hash: u64,
    pub(crate) update: Box<dyn Fn(&Runner) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
    pub(crate) delete_dependent: Box<dyn Fn(&Runner, AnyKey) + Send>,
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
            update: Box::new(move |runner| runner.update_derived_state(self_update.clone())),
            delete_dependent: Box::new(move |runner, dependent_key| if let Some(handle) = runner.map.lock().get_mut(&AnyKey::new(self_delete_dependent.clone())) {
                let handle = handle.downcast_mut::<Handle<K>>().expect("handle type mismatch");
                handle.dependents.remove(&dependent_key);
            }),
        }
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
