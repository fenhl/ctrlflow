//! This crate provides a handler for dependency-based asynchronous control flow.
//!
//! It assumes you have several interdependent units of changing state, which are modeled as nodes in a control flow graph. Nodes could be the contents of a file, the current time, etc.
//!
//! A control flow graph is represented by the type `CtrlFlow`, whose `get_ext` method is used to query the state of a node (starting its computation if necessary).

#![deny(missing_docs, rust_2018_idioms, unused, unused_import_braces, unused_qualifications, warnings)]

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
        pin::Pin,
        sync::Arc
    },
    futures::prelude::*,
    itertools::Itertools as _,
    petgraph::{
        algo::astar,
        graph::{
            DiGraph,
            NodeIndex
        }
    },
    smart_default::SmartDefault
};
#[cfg(feature = "async-std")]
use async_std::{
    sync::{
        Mutex,
        Receiver,
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
            Receiver,
            Sender,
            channel
        }
    },
    task
};

/// Generates a `NodeId` enum that can yield different types of deltas depending on the variant.
#[macro_export] macro_rules! ctrlflow {
    ($($NodeKind:ident($InnerId:ty, $Delta:ty) => $stream:expr),*) => {
        #[derive(Debug, Clone)]
        enum StateWrap {
            $(
                $NodeKind(<$Delta as $crate::Delta>::State)
            ),*
        }

        #[derive(Debug, Clone)]
        enum DeltaWrap {
            $(
                $NodeKind($Delta)
            ),*
        }

        $(
            impl From<$Delta> for DeltaWrap {
                fn from(inner: $Delta) -> DeltaWrap {
                    DeltaWrap::$NodeKind(inner)
                }
            }

            impl TryFrom<DeltaWrap> for $Delta {
                type Error = ();

                fn try_from(wrap: DeltaWrap) -> Result<$Delta, ()> {
                    match wrap {
                        DeltaWrap::$NodeKind(inner) => Ok(inner),
                        #[allow(unreachable_patterns)] _ => Err(())
                    }
                }
            }
        )*

        impl $crate::Delta for DeltaWrap {
            type State = StateWrap;

            fn from_initial_state(state: StateWrap) -> DeltaWrap {
                match state {
                    $(
                        StateWrap::$NodeKind(inner) => DeltaWrap::$NodeKind(<$Delta as $crate::Delta>::from_initial_state(inner))
                    ),*
                }
            }

            fn initial_state(self) -> Option<StateWrap> {
                match self {
                    $(
                        DeltaWrap::$NodeKind(inner) => inner.initial_state().map(StateWrap::$NodeKind)
                    ),*
                }
            }

            fn kind(&self) -> &'static str {
                match self {
                    $(
                        DeltaWrap::$NodeKind(_) => stringify!($NodeKind)
                    )*
                }
            }

            fn update_inner(self, state: &mut StateWrap) {
                let kind = self.kind();
                #[allow(irrefutable_let_patterns)] match self {
                    $(
                        DeltaWrap::$NodeKind(delta) => if let StateWrap::$NodeKind(state) = state {
                            delta.update_inner(state);
                        } else {
                            panic!("expected delta of kind {}", kind);
                        }
                    )*
                }
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum NodeId {
            $(
                $NodeKind($InnerId)
            ),*
        }

        impl $crate::NodeId for NodeId {
            type Delta = DeltaWrap;

            fn stream(&self) -> Pin<Box<dyn Stream<Item = DeltaWrap> + Send + 'static>> {
                match self {
                    $(
                        NodeId::$NodeKind(inner) => {
                            Box::pin($stream(inner.clone()).map(|d| <DeltaWrap as From<$Delta>>::from(d)))
                        }
                    )*
                }
            }
        }
    };
}

/// An error returned by `Delta::update`.
#[derive(Debug, Clone, Copy)]
pub struct MissingInitialState(&'static str);

/// A type implementing this trait represents a change to a node's state.
pub trait Delta: fmt::Debug + Clone + Send + Sync + 'static {
    /// The state which this delta modifies.
    type State: fmt::Debug + Clone + Send;

    /// Returns the “initial state” event corresponding to the given state.
    ///
    /// # Correctness
    ///
    /// ```rust
    /// Delta::from_initial_state(init).initial_state() == Some(init)
    /// ```
    fn from_initial_state(init: Self::State) -> Self;

    /// Returns the initial state if this is an “initial state” event.
    ///
    /// # Correctness
    ///
    /// The first `Delta` in a stream should always return `Some` here.
    fn initial_state(self) -> Option<Self::State>;

    /// A brief representation of the kind of event this delta represents, used for `Debug` in `MissingInitialState`. Usually corresponds to the enum discriminant if `Self` is an enum.
    fn kind(&self) -> &'static str;

    /// Called by `update` if the `state` is already initialized.
    fn update_inner(self, state: &mut Self::State);

    /// Apply this `Delta` to the given `state`.
    ///
    /// # Errors
    ///
    /// If `state` is `None` and `self.initial_state()` returns `None`, the stream is malformed and an error is returned.
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

/// A type implementing this trait describes the nodes of a control flow graph.
pub trait NodeId: fmt::Debug + Clone + Eq + Hash {
    /// The type yielded by streams of nodes of this type.
    type Delta: Delta;

    /// Contains the logic for this node.
    fn stream(&self) -> Pin<Box<dyn Stream<Item = Self::Delta> + Send + 'static>>;
}

/// The main entry point for the API. An instance of this type manages the control-flow graph and ensures that there are no cycles.
#[derive(Debug, SmartDefault)]
pub struct CtrlFlow<I: NodeId> {
    /// A graph of the “depends on” relation between control flow nodes.
    #[default(DiGraph::new())]
    graph: DiGraph<I, ()>,
    indices: HashMap<I, NodeIndex>,
    state_deltas: HashMap<I, StateDelta<I::Delta>>
}

impl<I: NodeId> CtrlFlow<I> {
    /// Requests a copy of the data stream for the given node.
    ///
    /// # Correctness
    ///
    /// This method must only be called from outside the graph represented by this `CtrlFlow`. Stream implementations for `DepId` should use `get_int` instead.
    ///
    /// # Panics
    ///
    /// The returned stream panics if `nid` doesn't produce values of type `T`.
    pub async fn get_ext<T: Delta + TryFrom<I::Delta>>(&mut self, nid: I) -> StateDelta<T>
    where <T as TryFrom<I::Delta>>::Error: fmt::Debug {
        let sd = if let Some(sd) = self.state_deltas.get(&nid) {
            sd.clone()
        } else {
            let sd = StateDelta::new(nid.stream());
            self.state_deltas.insert(nid, sd.clone());
            sd
        };
        sd.try_into().await
    }

    /// Requests a copy of the data stream for the given node.
    ///
    /// # Correctness
    ///
    /// This method must only be called from inside the graph represented by this `CtrlFlow`. The `from` parameter must be the ID of the calling node.
    ///
    /// # Errors
    ///
    /// If adding this dependency would result in a dependency loop, an error is returned containing the dependency path from `nid` to `from`.
    ///
    /// # Panics
    ///
    /// The returned stream panics if `nid` doesn't produce values of type `T`.
    pub async fn get_int<T: Delta + TryFrom<I::Delta>>(&mut self, from: I, nid: I) -> Result<StateDelta<T>, Vec<I>>
    where <T as TryFrom<I::Delta>>::Error: fmt::Debug {
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
        Ok(self.get_ext::<T>(nid).await)
    }
}

/// This type facilitates subscribing to a node's state.
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
                delta.update(state).expect("delta is non-init but state is None");
            }
        });
        StateDelta(arc)
    }

    /// Returns the current state, potentially waiting for initialization.
    pub async fn state(&self) -> D::State {
        let opt_state = &self.0.lock().await.0;
        if let Some(state) = opt_state { return state.clone(); }
        let _ = self.stream().await.next().await;
        self.0.lock().await.0.clone().expect("state empty after initial state event")
    }

    /// The first item of this stream is guaranteed to be an “initial state” delta.
    pub async fn stream(&self) -> Receiver<D> {
        let (ref state, ref mut txs) = *self.0.lock().await;
        #[allow(unused_mut)] let (mut tx, rx) = channel(usize::MAX);
        if let Some(state) = state {
            #[cfg(feature = "async-std")] tx.send(D::from_initial_state(state.clone())).await;
            #[cfg(feature = "tokio")] tx.send(D::from_initial_state(state.clone())).await.expect("rx is still in scope");
        }
        txs.push(tx);
        rx
    }

    async fn try_into<T: Delta + TryFrom<D>>(self) -> StateDelta<T>
    where <T as TryFrom<D>>::Error: fmt::Debug {
        let stream = self.stream().await.map(|x|
            T::try_from(x).expect("cannot convert delta")
        );
        StateDelta::new(stream)
    }
}
