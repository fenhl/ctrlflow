//! Helper ctrlflow nodes that track the contents of a file or directory.

use {
    std::{
        collections::HashSet,
        ffi::OsString,
        fmt,
        io,
        path::PathBuf,
        pin::Pin,
        sync::Arc,
    },
    derive_more::From,
    futures::{
        prelude::*,
        stream,
    },
    inotify::{
        EventMask,
        Inotify,
        WatchMask,
    },
    tokio::fs,
    tokio_stream::wrappers::{
        BroadcastStream,
        ReadDirStream,
        errors::BroadcastStreamRecvError,
    },
    crate::{
        Delta,
        Key,
        RunnerInternal,
    },
};

/// A `Key` for watching the contents of a directory.
#[derive(Debug, From, Clone, PartialEq, Eq, Hash)]
pub struct Dir(pub PathBuf);

impl Key for Dir {
    type State = Result<HashSet<OsString>, Arc<io::Error>>;
    type Delta = Result<inotify::EventOwned, Arc<io::Error>>;

    fn maintain(self, _: RunnerInternal) -> Pin<Box<dyn Future<Output = (Result<HashSet<OsString>, Arc<io::Error>>, Pin<Box<dyn Stream<Item = Result<inotify::EventOwned, Arc<io::Error>>> + Send>>)> + Send>> {
        Box::pin(async move {
            let mut watcher = match Inotify::init() {
                Ok(watcher) => watcher,
                Err(e) => return (Err(Arc::new(e)), stream::empty().boxed()),
            };
            if let Err(e) = watcher.add_watch(&self.0, WatchMask::ATTRIB | WatchMask::CLOSE_WRITE | WatchMask::CREATE | WatchMask::DELETE | WatchMask::DELETE_SELF | WatchMask::MOVE | WatchMask::MOVE_SELF) {
                return (Err(Arc::new(e)), stream::empty().boxed())
            }
            let read_dir = match fs::read_dir(self.0).await {
                Ok(read_dir) => ReadDirStream::new(read_dir),
                Err(e) => return (Err(Arc::new(e)), stream::empty().boxed()),
            };
            (
                read_dir.map_ok(|entry| entry.file_name()).try_collect().await.map_err(Arc::new),
                Box::pin(stream::once(async move { watcher.event_stream(Vec::default()) }).try_flatten().map_err(Arc::new)),
            )
        })
    }
}

/// A `Key` for watching the contents of a regular file.
#[derive(Debug, From, Clone, PartialEq, Eq, Hash)]
pub struct File(pub PathBuf);

impl Key for File {
    type State = Result<Option<Vec<u8>>, Arc<io::Error>>;
    type Delta = Result<Option<Vec<u8>>, Arc<io::Error>>;

    fn maintain(self, runner: RunnerInternal) -> Pin<Box<dyn Future<Output = (Result<Option<Vec<u8>>, Arc<io::Error>>, Pin<Box<dyn Stream<Item = Result<Option<Vec<u8>>, Arc<io::Error>>> + Send>>)> + Send>> {
        let path = self.0.clone();
        let name = self.0.file_name().expect("watched file has no name").to_owned();
        Box::pin(async move {
            let parent = path.parent().expect("file has no parent").to_owned();
            let parent_handle = runner.subscribe(Dir(parent)).await.expect("dependency loop");
            let (parent_state, parent_stream) = parent_handle.stream().await;
            (
                match *parent_state {
                    Ok(ref names) => if names.contains(&name) {
                        match fs::read(&path).await {
                            Ok(buf) => Ok(Some(buf)),
                            Err(e) => Err(Arc::new(e)),
                        }
                    } else {
                        Ok(None)
                    },
                    Err(ref e) => Err(Arc::clone(e)),
                },
                BroadcastStream::new(parent_stream)
                    .map_err(|e| match e {
                        BroadcastStreamRecvError::Lagged(skipped_msgs) => Arc::new(io::Error::new(io::ErrorKind::Other, BroadcastLaggedError(skipped_msgs))),
                    })
                    .try_filter_map(move |evt| {
                        let name = name.clone();
                        async move {
                            let evt = evt.as_ref().map_err(Arc::clone)?;
                            Ok(match evt.mask {
                                EventMask::ATTRIB | EventMask::CLOSE_WRITE | EventMask::CREATE | EventMask::MOVED_TO => if evt.name.as_ref().map_or(false, |evt_name| name == *evt_name) { Some(true) } else { None },
                                EventMask::DELETE_SELF | EventMask::MOVE_SELF => if evt.name.as_ref().map_or(false, |evt_name| name == *evt_name) { Some(false) } else { None },
                                _ => None,
                            })
                        }
                    })
                    .and_then(move |exists| {
                        let path = path.clone();
                        async move {
                            Ok(if exists {
                                Some(fs::read(&path).await?)
                            } else {
                                None
                            })
                        }
                    })
                    .boxed(),
            )
        })
    }
}

#[derive(Debug)]
struct BroadcastLaggedError(u64);

impl fmt::Display for BroadcastLaggedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "broadcast receiver lagged behind and skipped {} messages", self.0)
    }
}

impl std::error::Error for BroadcastLaggedError {}

#[derive(Debug)]
struct InotifyDeleteSelfError;

impl fmt::Display for InotifyDeleteSelfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "watched directory was deleted")
    }
}

impl std::error::Error for InotifyDeleteSelfError {}

#[derive(Debug)]
struct InotifyMaskError(EventMask);

impl fmt::Display for InotifyMaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected inotify event mask: {:?}", self.0)
    }
}

impl std::error::Error for InotifyMaskError {}

#[derive(Debug)]
struct InotifyNameError(EventMask);

impl fmt::Display for InotifyNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "missing filename for inotify event of mask {:?}", self.0)
    }
}

impl std::error::Error for InotifyNameError {}

impl Delta<Result<HashSet<OsString>, Arc<io::Error>>> for Result<inotify::EventOwned, Arc<io::Error>> {
    fn apply(&self, state_res: &mut Result<HashSet<OsString>, Arc<io::Error>>) {
        if let Ok(state) = state_res {
            match self {
                Ok(evt) => match evt.mask {
                    EventMask::ATTRIB | EventMask::CLOSE_WRITE => {} // no changes to the file list (these events are only watched to be picked up by File nodes)
                    EventMask::CREATE | EventMask::MOVED_TO => if let Some(ref name) = evt.name {
                        state.insert(name.clone());
                    } else {
                        *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::Other, Box::new(InotifyNameError(evt.mask)))));
                    },
                    EventMask::DELETE | EventMask::MOVED_FROM => if let Some(ref name) = evt.name {
                        state.remove(name);
                    } else {
                        *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::Other, Box::new(InotifyNameError(evt.mask)))));
                    },
                    EventMask::DELETE_SELF | EventMask::MOVE_SELF => { *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::NotFound, InotifyDeleteSelfError))); }
                    _ => *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::Other, Box::new(InotifyMaskError(evt.mask))))),
                },
                Err(e) => *state_res = Err(Arc::clone(e)),
            }
        }
    }
}
