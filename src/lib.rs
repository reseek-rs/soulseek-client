//! A Soulseek client: the server session (connect, log in, stay connected, disconnect), the
//! operations that ask the server for something, connections to other users (directly or
//! through the server's relay, cached one per user for messages), downloads and uploads,
//! searches, browsing users and fetching their info, the distributed search tree, answering
//! other users' searches, the listener that accepts peers, and the keyed waits that match
//! replies to requests.
//!
//! [`Client`] is the entry point. It reports what happens as [`Event`]s on a broadcast channel
//! and answers peers through the application's [`Resolvers`].

mod browse;
mod bucket;
mod cache;
mod client;
mod conn;
mod distributed;
mod error;
mod events;
mod inbound;
mod limit;
mod listener;
mod log;
mod options;
mod patch;
mod peers;
mod resolvers;
mod responder;
mod search;
mod state;
mod token;
mod transfers;
mod user_info;
mod waiter;

#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing;

use std::sync::{Mutex, MutexGuard, PoisonError};

pub use soulseek_proto;
pub use tokio_util::sync::CancellationToken;

pub use browse::{BrowseOptions, BrowseProgress, BrowseProgressUpdated, PeerRequestOptions};
pub use bucket::TokenBucket;
pub use client::{Client, DEFAULT_SERVER_ADDRESS, DEFAULT_SERVER_PORT};
pub use conn::EncodedFrame;
pub use error::{ClientError, ConnectionErrorKind, ErrorSource, MessageErrorKind, RoomErrorKind};
pub use events::{DistributedParent, Event, PrivateMessage, RoomMembers, SearchDelivery};
pub use log::LogLimiter;
pub use options::{
  ClientLimits, ClientOptions, ConnectionOptions, KeepAlive, ProxyCredentials, ProxyOptions,
};
pub use patch::ClientOptionsPatch;
pub use resolvers::{
  BrowseReply, BrowseResponseResolver, FolderContentsResolver, PlaceInQueueResolver,
  ResolverFuture, Resolvers, SearchReply, SearchResponseResolver, UnaskedOfferResolver,
  UploadRequestResolver, UserInfoResolver,
};
pub use search::{
  FileFilter, ResponseFilter, Search, SearchOptions, SearchOutcome, SearchRequest,
  SearchResponseReceived, SearchScope, SearchState, SearchStateChanged, SearchToken,
};
pub use state::{ClientState, DistributedNetworkInfo, ServerInfo, SessionEnd, SessionStatus};
pub use transfers::{
  DownloadOutput, DownloadRequest, DownloadSink, Governor, InputFactory, OutputFactory,
  ProgressUpdated, Reporter, RequestStyle, SizeMismatchResolver, SlotAwaiter, SlotReleased,
  StateChanged, Transfer, TransferFuture, TransferOptions, TransferOutcome, TransferState,
  TransferTask, UploadInput, UploadRequest, UploadSource,
};

/// A count a peer reports of its own upload queue, its length or a file's place in it, or `None`
/// when the peer does not know it: some clients send -1, which reads unsigned as past `i32::MAX`.
pub fn peer_count(value: u32) -> Option<u32> {
  i32::try_from(value).is_ok().then_some(value)
}

/// locks a mutex whose critical sections cannot leave its data half-updated
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
  mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
