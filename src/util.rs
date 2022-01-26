use {
    std::{
        collections::HashMap,
        hash::Hash,
        //iter::FromIterator,
        pin::Pin,
    },
    //async_stream::try_stream,
    futures::{
        future::Future,
        stream::{
            self,
            //FuturesUnordered,
            Stream,
            StreamExt as _,
            //TryStreamExt as _,
        },
    },
    /*
    tokio::{
        select,
        sync::broadcast,
    },
    */
    crate::{
        Delta,
        Key,
        //RunnerInternal,
    },
};

/// A state that can be compared with another to reconstruct a [`Delta`].
pub trait Diff: Sized {
    type Delta: Delta<Self>;

    /// Compare `self` with `prev`, returning a `delta` such that `delta.apply(prev)` would result in `self`.
    fn diff(&self, prev: &Self) -> Self::Delta;
}

/// Turns a stream of states into the expected return type of the [`Key::maintain`] method by using the [`Diff`] trait to generate deltas between the states.
///
/// # Panics
///
/// The returned future panics if `states` is an empty stream.
pub fn maintain_from_states<'a, S: Diff + Clone + Send + Sync + 'a>(mut states: impl Stream<Item = S> + Unpin + Send + 'a) -> Pin<Box<dyn Future<Output = (S, Pin<Box<dyn Stream<Item = S::Delta> + Send + 'a>>)> + Send + 'a>>
where S: Diff {
    Box::pin(async move {
        let init = states.next().await.expect("empty states stream");
        (init.clone(), stream::unfold((init, states), |(prev_state, mut states)| async move {
            states.next().await.map(|next_state| (next_state.diff(&prev_state), (next_state, states)))
        }).boxed())
    })
}

pub enum SubscribeAllDelta<S: Hash, K: Key> {
    Init(HashMap<S, K::State>),
    Content(S, K::Delta),
    Lagged(u64),
}

/*
/// # Panics
///
/// The returned stream panics if `states` is an empty stream, or if subscribing to a key returned by `get_key` results in a dependency loop.
pub fn try_subscribe_all<'a, S: Clone + Eq + Hash + Unpin + 'a, E: Unpin + 'a, K: Key, Fut: Future<Output = K>>(runner: &'a RunnerInternal, mut states: impl Stream<Item = Result<impl IntoIterator<Item = S>, E>> + Unpin + 'a, get_key: impl Fn(&S) -> Fut + Copy + 'a) -> impl Stream<Item = Result<SubscribeAllDelta<S, K>, E>> + 'a
where K::State: Clone + Unpin, K::Delta: Unpin {
    try_stream! {
        let mut inits = HashMap::default();
        let mut subscriptions = FuturesUnordered::from_iter(states.try_next().await?.expect("empty stream").into_iter().map(|key| async move {
            let subscription = runner.subscribe(get_key(&key).await).await.expect("dependency loop");
            let (init, stream) = subscription.stream().await;
            (key, init.clone(), stream)
        }));
        let mut streams = Vec::with_capacity(subscriptions.len());
        while let Some((key, init, stream)) = subscriptions.next().await {
            inits.insert(key.clone(), init);
            streams.push((key, stream));
        }
        yield SubscribeAllDelta::Init(inits);
        let mut nexts = FuturesUnordered::from_iter(streams.into_iter().map(|(key, mut stream)| Box::pin(async move { (key, stream.recv().await, stream) }) as Pin<Box<dyn Future<Output = (S, Result<K::Delta, broadcast::error::RecvError>, broadcast::Receiver<K::Delta>)>>>));
        loop {
            let delta = select! {
                Some((key, delta, mut stream)) = nexts.next() => match delta {
                    Ok(delta) => {
                        let key_clone = key.clone();
                        //let next = streams.iter_mut().find(|&&mut (k, _)| k == key).expect("stream for this key not found").1.recv();
                        nexts.push(Box::pin(async move { (key_clone, stream.recv().await, stream) }));
                        SubscribeAllDelta::Content(key, delta)
                    }
                    Err(broadcast::error::RecvError::Closed) => continue,
                    Err(broadcast::error::RecvError::Lagged(lag)) => SubscribeAllDelta::Lagged(lag), //TODO more info?
                },
                //TODO check for next state
            };
            yield delta;
        }
    }
    /*
    try_stream! {
        let mut inits = HashMap::default();
        let mut subscriptions = FuturesUnordered::from_iter(states.try_next().await?.expect("empty stream").into_iter().map(|key| async move {
            let subscription = runner.subscribe(get_key(&key).await).await.expect("dependency loop");
            let (init, stream) = subscription.stream().await;
            (key, init.clone(), stream)
        }));
        let mut streams = Vec::with_capacity(subscriptions.len());
        while let Some((key, init, stream)) = subscriptions.next().await {
            inits.insert(key.clone(), init);
            streams.push((key, stream));
        }
        yield SubscribeAllDelta::Init(inits);
        //TODO rest of the fucking owl
        let mut nexts = FuturesUnordered::from_iter(streams.into_iter().map(|(key, mut stream)| Box::pin(async move { (key, stream.recv().await, stream) }) as Pin<Box<dyn Future<Output = (S, Result<K::Delta, broadcast::error::RecvError>, broadcast::Receiver<K::Delta>)>>>));
        loop {
            if let Some((key, delta, mut stream)) = nexts.next().await {
                let delta = match delta {
                    Ok(delta) => {
                        let key_clone = key.clone();
                        //let next = streams.iter_mut().find(|&&mut (k, _)| k == key).expect("stream for this key not found").1.recv();
                        nexts.push(Box::pin(async move { (key_clone, stream.recv().await, stream) }));
                        SubscribeAllDelta::Content(key, delta)
                    }
                    Err(broadcast::error::RecvError::Closed) => continue,
                    Err(broadcast::error::RecvError::Lagged(lag)) => SubscribeAllDelta::Lagged(lag), //TODO more info?
                };
                yield delta;
            }
        }
    }
    */ //DEBUG
}
*/ //TODO?
