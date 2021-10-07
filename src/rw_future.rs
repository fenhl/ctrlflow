use {
    std::{fmt,
        future::Future,
        sync::Arc,
    },
    tokio::sync::{
        RwLock,
        RwLockMappedWriteGuard,
        RwLockReadGuard,
        RwLockWriteGuard,
    },
};

#[derive(Debug)]
enum RwFutureData<T: Send + Sync> {
    Pending(tokio::sync::broadcast::Sender<()>),
    Ready(T),
}

impl<T: Send + Sync> RwFutureData<T> {
    fn unwrap(&self) -> &T {
        match self {
            RwFutureData::Pending(_) => panic!("not ready"),
            RwFutureData::Ready(value) => value,
        }
    }

    fn unwrap_mut(&mut self) -> &mut T {
        match self {
            RwFutureData::Pending(_) => panic!("not ready"),
            RwFutureData::Ready(value) => value,
        }
    }
}

/// A type that eventually resolves to its inner type, like a future, but can be accessed like a `RwLock`.
pub struct RwFuture<T: Send + Sync>(Arc<RwLock<RwFutureData<T>>>);

impl<T: Send + Sync + 'static> RwFuture<T> {
    /// Creates a new `RwFuture` which will hold the output of the given future.
    pub fn new<F: Future<Output = T> + Send + 'static>(fut: F) -> RwFuture<T> {
        let (tx, _) = tokio::sync::broadcast::channel(1);
        let data = Arc::new(RwLock::new(RwFutureData::Pending(tx.clone())));
        let data_clone = data.clone();
        tokio::spawn(async move {
            let value = fut.await;
            *data_clone.write().await = RwFutureData::Ready(value);
            let _ = tx.send(()); // an error just means no one's listening, which is fine
        });
        RwFuture(data)
    }

    /// Waits until the value is available, then locks this `RwFuture` for read access.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        let mut rx = {
            let data = self.0.read().await;
            match *data {
                RwFutureData::Pending(ref tx) => tx.subscribe(),
                RwFutureData::Ready(_) => return RwLockReadGuard::map(data, RwFutureData::unwrap),
            }
        };
        let () = rx.recv().await.expect("RwFuture notifier dropped");
        let data = self.0.read().await;
        match *data {
            RwFutureData::Pending(_) => unreachable!("RwFuture should be ready after notification"),
            RwFutureData::Ready(_) => RwLockReadGuard::map(data, RwFutureData::unwrap),
        }
    }

    /// Waits until the value is available, then locks this `RwFuture` for write access.
    pub async fn write(&self) -> RwLockMappedWriteGuard<'_, T> {
        let mut rx = {
            let data = self.0.write().await;
            match *data {
                RwFutureData::Pending(ref tx) => tx.subscribe(),
                RwFutureData::Ready(_) => return RwLockWriteGuard::map(data, RwFutureData::unwrap_mut),
            }
        };
        let () = rx.recv().await.expect("RwFuture notifier dropped");
        let data = self.0.write().await;
        match *data {
            RwFutureData::Pending(_) => unreachable!("RwFuture should be ready after notification"),
            RwFutureData::Ready(_) => RwLockWriteGuard::map(data, RwFutureData::unwrap_mut),
        }
    }
}

impl<T: Send + Sync> fmt::Debug for RwFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("RwFuture")
    }
}

impl<T: Send + Sync + Default> Default for RwFuture<T> {
    fn default() -> RwFuture<T> {
        RwFuture(Arc::new(RwLock::new(RwFutureData::Ready(T::default()))))
    }
}

// manually implemented to not require T: Clone
impl<T: Send + Sync> Clone for RwFuture<T> {
    fn clone(&self) -> RwFuture<T> {
        RwFuture(Arc::clone(&self.0))
    }
}
