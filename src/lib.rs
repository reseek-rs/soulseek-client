//! A Soulseek client.
//!
//! The crate covers the server session (connect, log in, stay connected, disconnect), the
//! operations that ask the server for something, connections to other users (directly or
//! through the server's relay, cached one per user for messages), downloads and uploads,
//! searches, browsing users and fetching their info, the distributed search tree, answering
//! other users' searches, the listener that accepts peers, and the keyed waits that match
//! replies to requests.
//!
//! [`Client`] is the entry point. It reports what happens as [`Event`]s on a broadcast channel
//! and answers peers through the application's [`Resolvers`].
//!
//! # Examples
//!
//! ```no_run
//! use soulseek_client::{CancellationToken, Client, ClientError, ClientOptions, Event};
//!
//! # async fn run() -> Result<(), ClientError> {
//! let client = Client::new(ClientOptions::default())?;
//! let mut events = client.subscribe();
//! let cancel = CancellationToken::new();
//! client
//!   .connect("server.example.org", 2271, "username", "password", &cancel)
//!   .await?;
//! while let Ok(event) = events.recv().await {
//!   if let Event::PrivateMessageReceived(message) = event {
//!     println!("{}: {}", message.username, message.message);
//!   }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Policy
//!
//! The client decides no policy. When to reconnect, what to share, how many uploads run at once
//! and what to answer a peer are the application's, given as [`ClientOptions`] and
//! [`Resolvers`]. The limits that bound what remote input can make the client hold are the
//! client's own ([`ClientLimits`]).
//!
//! # Protocol types
//!
//! The protocol's types (messages, filenames, codes) are those of [`soulseek_proto`], re-exported
//! whole at the version the client is built on. An application takes them from there: the same
//! types from another version of `soulseek-proto` are different types to the compiler.

// checked without `testing`, which makes the engine public for the crate's own tests only
#![cfg_attr(not(feature = "testing"), warn(missing_docs))]

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctest;

mod browse;
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

/// The wire protocol crate, whole, at the version the client is built on.
///
/// The protocol types in the client's API are these. A `soulseek-proto` dependency of another
/// version has types of the same names that do not match them.
pub use soulseek_proto;
/// Cancellation token the client's operations take.
///
/// It comes from the `tokio-util` the client is built on.
pub use tokio_util::sync::CancellationToken;

pub use browse::{BrowseOptions, BrowseProgress, BrowseProgressUpdated, PeerRequestOptions};
pub use client::{Client, DEFAULT_SERVER_ADDRESS, DEFAULT_SERVER_PORT};
pub use conn::EncodedFrame;
pub use error::{ClientError, ConnectionErrorKind, ErrorSource, MessageErrorKind, RoomErrorKind};
pub use events::{DistributedParent, Event, PrivateMessage, RoomMembers, SearchDelivery};
pub use options::{
  ClientLimits, ClientOptions, ConnectionOptions, KeepAlive, ProxyCredentials, ProxyOptions,
};
pub use patch::ClientOptionsPatch;
pub use resolvers::{
  BrowseReply, BrowseResponseResolver, ConnectionRequestResolver, DistributedChildResolver,
  DistributedParentResolver, FolderContentsResolver, Peer, PlaceInQueueResolver, ResolverFuture,
  Resolvers, SearchReply, SearchResponseResolver, UnaskedOfferResolver, UploadAsk, UploadDecision,
  UploadRequestResolver, UserInfoResolver,
};
pub use search::{
  FileFilter, ResponseFilter, Search, SearchOptions, SearchOutcome, SearchRequest,
  SearchResponseReceived, SearchScope, SearchState, SearchStateChanged, SearchToken,
};
pub use state::{ClientState, DistributedNetworkInfo, ServerInfo, SessionEnd, SessionStatus};
pub use transfers::{
  DownloadOutput, DownloadRequest, DownloadSink, InputFactory, OutputFactory, RequestStyle,
  SizeMismatchResolver, SlotAwaiter, SlotReleased, Transfer, TransferDirection, TransferFuture,
  TransferGovernor, TransferOptions, TransferOutcome, TransferProgressUpdated, TransferReporter,
  TransferState, TransferStateChanged, TransferTask, UploadEnded, UploadInput, UploadNotice,
  UploadRequest, UploadSource,
};

/// Reads a count a peer reports of its own upload queue.
///
/// The count is the queue's length or a file's place in it. Returns `None` when the peer does
/// not know it: some clients send -1, which reads unsigned as past `i32::MAX`.
pub fn peer_count(value: u32) -> Option<u32> {
  i32::try_from(value).is_ok().then_some(value)
}

/// `from` plus `duration`. An instant too far off to name is 30 years on, which nothing the
/// client waits for reaches, so no duration an option holds overflows the clock.
pub(crate) fn later(
  from: tokio::time::Instant,
  duration: std::time::Duration,
) -> tokio::time::Instant {
  const FAR: std::time::Duration = std::time::Duration::from_secs(30 * 365 * 24 * 3600);
  from.checked_add(duration).unwrap_or_else(|| from + FAR)
}

/// locks a mutex whose critical sections cannot leave its data half-updated
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
  mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
