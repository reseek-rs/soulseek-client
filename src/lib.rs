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
//! use soulseek_client::{Client, Config, Error, Event};
//!
//! # async fn run() -> Result<(), Error> {
//! let client = Client::new(Config::default());
//! let mut events = client.events();
//! client.connect("username", "password").await?;
//! while let Some(event) = events.next().await {
//!   match event {
//!     Ok(Event::PrivateMessage(message)) => {
//!       println!("{}: {}", message.username, message.message);
//!     }
//!     Ok(_) => {}
//!     // the stream fell behind and missed some events
//!     Err(lagged) => eprintln!("{lagged}"),
//!   }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Layout
//!
//! The client itself holds the session and what starts something of its own: [`Client::connect`],
//! [`Client::search`], [`Client::download`] and [`Client::upload`]. Everything else is on a
//! handle for its topic: [`Client::users`], [`Client::rooms`], [`Client::interests`],
//! [`Client::account`], [`Client::transfers`] and [`Client::distributed`]. Each topic has a
//! module with its types and, where it has any, its events: [`user`], [`room`], [`interest`],
//! [`account`], [`transfer`] and [`distributed`].
//!
//! [`Client::events`] is a stream of everything that happens, as an [`Event`] that names its
//! topic. A handle whose topic has events gives a stream of those alone, as
//! [`Rooms::events`](room::Rooms::events) does.
//!
//! An operation with settings of its own returns a value to set up and await, as
//! `client.search("text").idle_timeout(duration).await`. One that runs for a while gives something
//! to follow it with: a search its [`search::Responses`], a transfer started with `start` its
//! [`transfer::Handle`].
//!
//! # Stopping an operation
//!
//! Dropping the future of a call stops waiting for it, and dropping a search's responses cancels
//! the search. Dropping the future of an awaited transfer cancels the transfer. One started with
//! `start` runs on a task of its own and is canceled through its handle.
//!
//! # Policy
//!
//! The client decides no policy. When to reconnect, what to share, how many uploads run at once
//! and what to answer a peer are the application's, given as [`Config`], the [`Handler`] and the
//! settings of an operation, as [`transfer::Upload::slot`] for the uploads. The limits that
//! bound what remote input can make the client hold are the client's own
//! ([`Limits`](config::Limits)).
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
//!
//! # Features
//!
//! `serde`, off by default, implements `Serialize` and `Deserialize` for the plain data the client
//! hands out and takes, and turns on the same feature of `soulseek-proto`. Fields and variants keep
//! their Rust names, and enums are externally tagged. Covered both ways are [`Config`], whose
//! [`Connection`](config::Connection)s and [`Limits`](config::Limits) are read only as part of it,
//! with its [`KeepAlive`](config::KeepAlive) and [`Proxy`](config::Proxy), [`ErrorKind`] and its
//! sub-kinds, [`PrivateMessage`], [`Peer`], [`Lagged`], the events and data types of the [`user`],
//! [`room`] and [`distributed`] modules, [`session::State`] and [`session::ServerInfo`], a
//! [`search::Search`] with its scope, state and outcome, [`search::ReplyEvent`], the requests and
//! replies of the [`handler`] module but a [`BrowseReply`](handler::BrowseReply), which may stream
//! from a reader, and of a transfer its [`State`](transfer::State), [`Outcome`](transfer::Outcome),
//! [`Direction`](transfer::Direction), [`Chunk`](transfer::Chunk),
//! [`RequestStyle`](transfer::RequestStyle) and [`UploadNotice`](transfer::UploadNotice).
//!
//! An [`Error`] serializes as a map of `kind`, its [`ErrorKind`], and `message`, its `Display`
//! text. It cannot be deserialized, so what holds one implements `Serialize` only: [`Event`],
//! [`session::Event`], [`session::Status`], [`session::End`], [`transfer::Event`],
//! [`transfer::Transfer`] and [`config::Applied`].
//!
//! A serialized [`Config`] holds every setting, the password of a proxy included. The
//! [`Handler`] and the transfer governor and reporter set on the configuration are left out. A
//! deserialized [`Config`] is checked as [`ConfigBuilder::build`](config::ConfigBuilder::build)
//! checks it. A setting left out keeps its default, also inside a kind of connection or the
//! limits, where the default is that of the kind: a `server_connection` that names only its
//! connect timeout keeps the server connection's write timeout and keep-alive. Only the decode
//! limits, which are `soulseek-proto`'s, are read whole. An unknown key is refused. An optional
//! setting is turned off with a null where the format has one. TOML has none, so a setting read
//! from TOML is turned off on `ConfigBuilder::from(config)`, which is also where the application
//! sets the handler, governor and reporter a deserialized [`Config`] lacks.
//!
//! The client, the topic handles, the builders and requests, the transfer handles and the
//! streams have no implementations.

// checked without `__testing`, which makes the engine public for the crate's own tests only
#![cfg_attr(not(feature = "__testing"), warn(missing_docs))]

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
pub mod error;
mod events;
pub mod handler;
mod inbound;
pub mod interest;
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

#[cfg(feature = "__testing")]
#[doc(hidden)]
pub mod testing;

use std::sync::{Mutex, MutexGuard, PoisonError};

/// The wire protocol crate, whole, at the version the client is built on.
///
/// The protocol types in the client's API are these. A `soulseek-proto` dependency of another
/// version has types of the same names that do not match them.
pub use soulseek_proto;

pub use client::Client;
pub use config::Config;
pub use error::{Error, ErrorKind};
pub use events::{Event, Events, Lagged, PrivateMessage};
pub use handler::{Handler, Peer};

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
