//! Helper ctrlflow nodes that track the contents of a file or directory.

use {
    std::{
        collections::HashSet,
        ffi::OsString,
        fmt,
        io,
        path::PathBuf,
        sync::Arc,
    },
    futures::{
        prelude::*,
        stream,
    },
    inotify::{
        EventMask,
        Inotify,
        WatchMask,
    },
    tokio::{
        fs::{
            self,
            File,
        },
        prelude::*,
    },
    crate::{
        CtrlFlow,
        Delta,
        NodeId,
    },
};

/// Helper trait for getting the node ID for a given file or directory.
pub trait NodeIdExt {
    /// Returns the node ID for this directory.
    fn dir(path: PathBuf) -> Self;

    /// Returns the node ID for this file.
    fn file(path: PathBuf) -> Self;
}

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

/// A change to the contents of a directory.
#[derive(Debug, Clone)]
pub enum DirDelta {
    /// Generated when the ctrlflow starts watching the directory or when an error occurs.
    ///
    /// The success case contains the set of names inside the directory.
    Init(Result<HashSet<OsString>, Arc<io::Error>>),
    /// Any other non-error event that occurs after the initial contents of the directory are known.
    Other(inotify::EventOwned),
}

impl Delta for DirDelta {
    type State = Result<HashSet<OsString>, Arc<io::Error>>;

    fn from_initial_state(init: Result<HashSet<OsString>, Arc<io::Error>>) -> DirDelta {
        DirDelta::Init(init)
    }

    fn initial_state(self) -> Option<Result<HashSet<OsString>, Arc<io::Error>>> {
        if let DirDelta::Init(init) = self {
            Some(init)
        } else {
            None
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            DirDelta::Init(Ok(_)) => "Init",
            DirDelta::Init(Err(_)) => "Error",
            DirDelta::Other(_) => "Other",
        }
    }

    fn update_inner(self, state_res: &mut Result<HashSet<OsString>, Arc<io::Error>>) {
        match self {
            DirDelta::Init(init) => { *state_res = init; }
            DirDelta::Other(evt) => if let Ok(ref mut state) = state_res {
                match evt.mask {
                    EventMask::ATTRIB | EventMask::CLOSE_WRITE => {} // no changes to the file list (these events are only watched to be picked up by File nodes)
                    EventMask::CREATE | EventMask::MOVED_TO => if let Some(name) = evt.name {
                        state.insert(name);
                    } else {
                        *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::Other, Box::new(InotifyNameError(evt.mask)))));
                    },
                    EventMask::DELETE | EventMask::MOVED_FROM => if let Some(name) = evt.name {
                        state.remove(&name);
                    } else {
                        *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::Other, Box::new(InotifyNameError(evt.mask)))));
                    },
                    EventMask::DELETE_SELF | EventMask::MOVE_SELF => { *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::NotFound, InotifyDeleteSelfError))); }
                    _ => { *state_res = Err(Arc::new(io::Error::new(io::ErrorKind::Other, Box::new(InotifyMaskError(evt.mask))))); }
                }
            },
        }
    }
}

/// Subscribe to changes to the contents of a directory.
///
/// Usage: `Dir(std::path::PathBuf, ctrlflow::fs::DirDelta) => ctrlflow::fs::dir`
pub fn dir<I: NodeId>(_: CtrlFlow<I>, path: PathBuf) -> impl Stream<Item = DirDelta> {
    stream::once(async move {
        let mut watcher = Inotify::init()?;
        watcher.add_watch(&path, WatchMask::ATTRIB | WatchMask::CLOSE_WRITE | WatchMask::CREATE | WatchMask::DELETE | WatchMask::DELETE_SELF | WatchMask::MOVE | WatchMask::MOVE_SELF)?;
        let init = fs::read_dir(path).await?.map_ok(|entry| entry.file_name()).try_collect().await?;
        Ok::<_, io::Error>(
            stream::once(async move { Ok(DirDelta::Init(Ok(init))) })
                .chain(watcher.event_stream(Vec::default())?.map_ok(DirDelta::Other))
        )
    })
        .try_flatten()
        .map(|res| match res {
            Ok(event) => event,
            Err(e) => DirDelta::Init(Err(Arc::new(e))),
        })
}

impl Delta for Result<Option<Vec<u8>>, Arc<io::Error>> {
    type State = Result<Option<Vec<u8>>, Arc<io::Error>>;

    fn from_initial_state(init: Result<Option<Vec<u8>>, Arc<io::Error>>) -> Result<Option<Vec<u8>>, Arc<io::Error>> {
        init
    }

    fn initial_state(self) -> Option<Result<Option<Vec<u8>>, Arc<io::Error>>> {
        Some(self)
    }

    fn kind(&self) -> &'static str {
        match self {
            Ok(Some(_)) => "Exists",
            Ok(None) => "DoesNotExist",
            Err(_) => "Error",
        }
    }

    fn update_inner(self, state: &mut Result<Option<Vec<u8>>, Arc<io::Error>>) {
        *state = self;
    }
}

/// Subscribe to changes to the contents of a regular file.
///
/// Usage: `Dir(std::path::PathBuf, Result<Option<Vec<u8>>, std::sync::Arc<std::io::Error>>) => ctrlflow::fs::file`
pub fn file<I: NodeId + NodeIdExt>(flow: CtrlFlow<I>, path: PathBuf) -> impl Stream<Item = Result<Option<Vec<u8>>, Arc<io::Error>>>
where DirDelta: From<I::Delta> {
    let path_clone = path.clone();
    let name = path.file_name().expect("watched file has no name").to_owned();
    stream::once(async move {
        let parent = path_clone.parent().expect("file has no parent").to_owned();
        flow.get_int::<DirDelta>(I::file(path_clone), I::dir(parent)).await.expect("dependency loop").stream().await
    })
        .flatten()
        .filter_map(move |delta| {
            let name = name.clone();
            async move {
                match delta {
                    DirDelta::Init(Ok(names)) => Some(Ok(names.contains(&name))),
                    DirDelta::Init(Err(e)) => Some(Err(e)),
                    DirDelta::Other(evt) => match evt.mask {
                        EventMask::ATTRIB | EventMask::CLOSE_WRITE | EventMask::CREATE | EventMask::MOVED_TO => if evt.name.map_or(false, |evt_name| name == evt_name) { Some(Ok(true)) } else { None },
                        EventMask::DELETE_SELF | EventMask::MOVE_SELF => if evt.name.map_or(false, |evt_name| name == evt_name) { Some(Ok(false)) } else { None },
                        _ => None,
                    }
                }
            }
        })
        .and_then(move |exists| {
            let path = path.clone();
            async move { Ok(if exists {
                let mut f = File::open(&path).await?;
                let mut buf = Vec::default();
                f.read_to_end(&mut buf).await?;
                Some(buf)
            } else {
                None
            }) }
        })
}
