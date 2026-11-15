//! A Soulseek client.
//!
//! The crate covers the server session (connect, log in, stay connected, disconnect), the
//! operations that ask the server for something, connections to other users (directly or
//! through the server's relay, cached one per user for messages), downloads and uploads,
//! searches, browsing users and fetching their info, the distributed search tree, answering
//! other users' searches, the listener that accepts peers, and the keyed waits that match
//! replies to requests.
//!
//! [`Client`] is the entry point. It reports what happens as a stream of [`Event`]s
//! and answers peers through the application's [`Handler`].
//!
//! # Examples
//!
//! ```no_run
//! use soulseek_client::{CancellationToken, Client, Error, Config, Event};
//!
//! # async fn run() -> Result<(), Error> {
//! let client = Client::new(Config::default())?;
//! let mut events = client.events();
//! let cancel = CancellationToken::new();
//! client
//!   .connect("username", "password", &cancel)
//!   .await?;
//! while let Some(event) = events.next().await {
//!   if let Event::PrivateMessage(message) = event {
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
//! and what to answer a peer are the application's, given as [`Config`] and
//! the [`Handler`]. The limits that bound what remote input can make the client hold are the
//! client's own ([`Limits`](config::Limits)).
//!
//! # Peer identity
//!
//! A peer that connects to the client names itself, and the name is not proof. A connection is
//! known to be the user's when the client opened it, when it answers a connection request the
//! client had the server relay, or when it comes from the address the server gives for the user.
//! [`Peer::confirmed`] tells the handler whether the peer it answers is such a one.
//!
//! A connection that is none of these is still served under the name it gave. Its requests are
//! answered through the handler, and what it sends in answer to the client's requests to that
//! user is taken as that user's: a share listing, folder contents, user info, a file's place in
//! the queue, the answer to a transfer request, and the refusal or failure of a download. An
//! application that needs more than the name for its own answers weighs [`Peer::confirmed`] in
//! its handler; the client applies no such check to the answers it takes.
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

pub mod account;
mod browse;
mod cache;
mod client;
pub mod config;
mod conn;
pub mod distributed;
mod error;
mod events;
pub mod handler;
mod inbound;
pub mod interests;
mod limit;
mod listener;
mod log;
mod peers;
mod responder;
pub mod room;
pub mod search;
pub mod session;
mod token;
pub mod transfer;
pub mod user;
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
pub use client::Client;
pub use config::Config;
pub use conn::EncodedFrame;
pub use error::{ConnectionErrorKind, Error, ErrorSource, MessageErrorKind, RoomErrorKind};
pub use events::{Event, Events, PrivateMessage};
pub use handler::{Handler, Peer};
pub use search::{
  FileFilter, ResponseFilter, Search, SearchOptions, SearchOutcome, SearchRequest,
  SearchResponseReceived, SearchScope, SearchState, SearchStateChanged, SearchToken,
};
pub use session::{ClientState, DistributedNetworkInfo, ServerInfo, SessionEnd, SessionStatus};
pub use transfer::{
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
