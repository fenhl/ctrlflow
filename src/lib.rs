#![deny(rust_2018_idioms, unused, unused_import_braces, unused_qualifications, warnings)]

#![recursion_limit = "256"] // for stream!

#[cfg(all(feature = "async-std", feature = "tokio"))]
compile_error!("Features async-std and tokio are mutually exclusive.");
#[cfg(not(any(feature = "async-std", feature = "tokio")))]
compile_error!("Either the async-std feature or the tokio feature must be enabled.");

use {
    std::{
        collections::HashMap,
        convert::TryFrom,
        fmt,
        hash::Hash,
        sync::Arc
    },
    async_stream::stream,
    futures::prelude::*,
    itertools::Itertools as _,
    petgraph::{
        algo::astar,
        graph::{
            DiGraph,
            NodeIndex
        }
    }
};
#[cfg(feature = "async-std")]
use async_std::{
    sync::{
        Mutex,
        Sender,
        channel
    },
    task
};
#[cfg(feature = "tokio")]
use tokio::{
    sync::{
        Mutex,
        mpsc::{
            Sender,
            channel
        }
    },
    task
};

#[derive(Debug, Clone, Copy)]
pub struct MissingInitialState(&'static str);

pub trait Delta: fmt::Debug + Clone + Send + Sync + Unpin + 'static {
    type State: fmt::Debug + Clone + Send;

    fn from_initial_state(init: Self::State) -> Self;

    /// Returns the initial state if this is an “initial state” event.
    ///
    /// # Correctness
    ///
    /// The first `Delta` in a stream should always return `Some` here.
    fn initial_state(self) -> Option<Self::State>;

    fn kind(&self) -> &'static str;
    fn update_inner(self, state: &mut Self::State);

    fn update(self, state: &mut Option<Self::State>) -> Result<(), MissingInitialState> {
        let kind = self.kind();
        if let Some(state) = state {
            self.update_inner(state);
        } else if let Some(init) = self.initial_state() {
            *state = Some(init);
        } else {
            return Err(MissingInitialState(kind));
        }
        Ok(())
    }
}

pub trait NodeId: fmt::Debug + Clone + Eq + Hash {
    /// The type yielded by streams of nodes of this type.
    type Delta: Delta;

    /// Contains the logic for this node.
    fn stream(&self) -> Box<dyn Stream<Item = Self::Delta> + Send + Unpin + 'static>;
}

/// The main entry point for the API. An instance of this type manages the control-flow graph, ensures that there are no cycles, and handles the type magic.
#[derive(Debug, Default)]
pub struct CtrlFlow<I: NodeId> {
    /// A graph of the “depends on” relation between control flow nodes.
    graph: DiGraph<I, ()>,
    indices: HashMap<I, NodeIndex>,
    state_deltas: HashMap<I, StateDelta<I::Delta>>
}

impl<I: NodeId> CtrlFlow<I> {
    /// Requests a copy of the data stream for the given node.
    ///
    /// # Correctness
    ///
    /// This method must only be called from outside the graph represented by this `CtrlFlow`. Stream implementations for `DepId` should use `register_internal` instead.
    ///
    /// # Panics
    ///
    /// The returned stream panics if `nid` doesn't produce values of type `T`.
    pub fn get_ext<T: TryFrom<I::Delta>>(&mut self, nid: I) -> impl Stream<Item = T>
    where T::Error: fmt::Debug {
        let sd = if let Some(sd) = self.state_deltas.get(&nid) {
            sd.clone()
        } else {
            let sd = StateDelta::new(nid.stream());
            self.state_deltas.insert(nid, sd.clone());
            sd
        };
        sd.stream().map(|x| T::try_from(x).expect("cannot convert delta"))
    }

    /// Requests a copy of the data stream for the given node.
    ///
    /// # Correctness
    ///
    /// This method must only be called from inside the graph represented by this `CtrlFlow`. The `from` parameter must be the ID of the calling node.
    ///
    /// # Panics
    ///
    /// The returned stream panics if `nid` doesn't produce values of type `T`.
    pub fn get_int<T: TryFrom<I::Delta>>(&mut self, from: I, nid: I) -> Result<impl Stream<Item = T>, Vec<I>>
    where T::Error: fmt::Debug {
        let nid_ix = if let Some(&nid_ix) = self.indices.get(&nid) {
            if let Some(&from_ix) = self.indices.get(&from) {
                if let Some((_, path)) = astar(&self.graph, nid_ix, |ix| ix == from_ix, |_| 1, |_| 0) {
                    // A directed path from `nid` to `from` already exists, so adding an edge from `from` to `nid` would create a cycle.
                    return Err(path
                        .into_iter()
                        .map(|ix1| self.indices
                            .iter()
                            .filter_map(|(nid, &ix2)| if ix1 == ix2 { Some(nid.clone()) } else { None })
                            .exactly_one()
                            .expect("cycle path contains unknown nodes")
                        )
                        .collect()
                    );
                }
            }
            nid_ix
        } else {
            let nid_ix = self.graph.add_node(nid.clone());
            self.indices.insert(nid.clone(), nid_ix);
            nid_ix
        };
        let from_ix = if let Some(&from_ix) = self.indices.get(&from) {
            from_ix
        } else {
            let from_ix = self.graph.add_node(from.clone());
            self.indices.insert(from, from_ix);
            from_ix
        };
        self.graph.add_edge(nid_ix, from_ix, ());
        Ok(self.get_ext::<T>(nid))
    }
}

#[derive(Debug, Clone)]
pub struct StateDelta<D: Delta>(Arc<Mutex<(Option<D::State>, Vec<Sender<D>>)>>);

impl<D: Delta> StateDelta<D> {
    fn new(mut stream: impl Stream<Item = D> + Send + Unpin + 'static) -> StateDelta<D> {
        let arc = Arc::<Mutex<(Option<_>, Vec<Sender<_>>)>>::default();
        let arc_clone = Arc::clone(&arc);
        task::spawn(async move {
            while let Some(delta) = stream.next().await {
                let (ref mut state, ref mut txs) = *arc_clone.lock().await;
                for tx in txs {
                    #[cfg(feature = "async-std")] tx.send(delta.clone()).await;
                    #[cfg(feature = "tokio")] if tx.send(delta.clone()).await.is_err() {
                        // no longer listening
                        //TODO remove `tx` from `txs`
                    }
                }
                delta.update(state).expect("delta is non-init but state is None"); //TODO
            }
        });
        StateDelta(arc)
    }

    pub fn stream(&self) -> impl Stream<Item = D> {
        let arc = self.0.clone();
        stream! {
            let mut rx = {
                let (ref state, ref mut txs) = *arc.lock().await;
                let (mut tx, mut rx) = channel(usize::MAX);
                if let Some(state) = state {
                    #[cfg(feature = "async-std")] tx.send(D::from_initial_state(state.clone())).await;
                    #[cfg(feature = "tokio")] if tx.send(D::from_initial_state(state.clone())).await.is_err() {
                        // no longer listening
                        return;
                    }
                }
                txs.push(tx);
                rx
            };
            #[cfg(feature = "async-std")] while let Some(item) = rx.next().await {
                yield item;
            }
            #[cfg(feature = "tokio")] while let Some(item) = rx.recv().await {
                yield item;
            }
        }
    }
}
