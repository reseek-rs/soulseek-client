//! Other users.
//!
//! The module covers what the server tells the client about them, and what the client asks of
//! them over a peer connection.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use soulseek_proto::peer::BrowseResponse;
use soulseek_proto::server::{UserStatistics, UserStatus};
use tokio_util::sync::CancellationToken;

pub use crate::browse::BrowseProgress;
use crate::browse::{BrowseOptions, BrowseProgressUpdated};
use crate::client::Client;
use crate::error::Error;

/// Something the server told the client about a user.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// A user's presence, asked for or pushed.
  ///
  /// It is pushed for a watched user or a room member. `asked` is set when a request of the
  /// client's for it was still waiting and took it as its answer.
  StatusChanged {
    /// User and their presence.
    status: UserStatus,
    /// Whether a request of the client's for it was waiting and took it as its answer.
    asked: bool,
  },
  /// A user's statistics, asked for or pushed.
  ///
  /// They are pushed for a member of a room the client is in. `asked` is set when a request of
  /// the client's for them was still waiting and took them as its answer.
  StatisticsChanged {
    /// User and their figures.
    statistics: UserStatistics,
    /// Whether a request of the client's for them was waiting and took them as its answer.
    asked: bool,
  },
  /// The server reports that a user could not connect to the client.
  ///
  /// The report is for a connection request the client made. The server usually sends only the
  /// token, and then this event is not raised.
  CannotConnect {
    /// Token of the connection request.
    token: u32,
    /// User who could not connect.
    username: String,
  },
}

/// Established message connection to a user.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Connection {
  /// Username of the user.
  pub username: String,
  /// Address of the connection's other end.
  pub address: std::net::SocketAddr,
}

/// What a client knows of and asks about other users.
///
/// Made by [`Client::users`](crate::Client::users).
#[derive(Clone, Copy, Debug)]
pub struct Users<'a> {
  pub(crate) client: &'a crate::Client,
}

type Sending<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// Request to another user that has not been sent yet.
///
/// Made by the methods that ask a user something over a peer connection, such as
/// [`Users::info`]. Awaiting it sends the request and returns the answer.
#[must_use = "a request is sent when it is awaited"]
pub struct Request<'a, T> {
  timeout: Option<Duration>,
  send: Box<dyn FnOnce(Option<Duration>) -> Sending<'a, T> + Send + 'a>,
}

impl<'a, T> Request<'a, T> {
  pub(crate) fn new<F, Fut>(send: F) -> Self
  where
    F: FnOnce(Option<Duration>) -> Fut + Send + 'a,
    Fut: Future<Output = Result<T, Error>> + Send + 'a,
  {
    Self {
      timeout: None,
      send: Box::new(move |timeout| Box::pin(send(timeout))),
    }
  }

  /// Sets the time to wait for the answer.
  ///
  /// Defaults to the client's [`request_timeout`](crate::Config::request_timeout).
  pub fn timeout(mut self, timeout: Duration) -> Self {
    self.timeout = Some(timeout);
    self
  }
}

impl<'a, T> IntoFuture for Request<'a, T> {
  type Output = Result<T, Error>;
  type IntoFuture = Sending<'a, T>;

  fn into_future(self) -> Self::IntoFuture {
    (self.send)(self.timeout)
  }
}

impl<T> std::fmt::Debug for Request<'_, T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Request")
      .field("timeout", &self.timeout)
      .finish_non_exhaustive()
  }
}

/// Browse of another user's shares that has not been sent yet.
///
/// Made by [`Users::browse`]. Awaiting it asks the user for its share listing and returns it.
#[must_use = "a browse is sent when it is awaited"]
pub struct Browse<'a> {
  client: &'a Client,
  username: String,
  options: BrowseOptions,
}

impl<'a> Browse<'a> {
  pub(crate) fn new(client: &'a Client, username: String) -> Self {
    Self {
      client,
      username,
      options: BrowseOptions::default(),
    }
  }

  /// Sets the time to wait for the first bytes of the response.
  ///
  /// A peer with a large share takes a while to put it together. Defaults to the client's
  /// [`request_timeout`](crate::Config::request_timeout).
  pub fn timeout(mut self, timeout: Duration) -> Self {
    self.options.timeout = Some(timeout);
    self
  }

  /// Sets what is told as the response arrives.
  ///
  /// `progress` is called with how much of the response has arrived, first with none of it.
  pub fn progress(mut self, progress: impl Fn(&BrowseProgress) + Send + Sync + 'static) -> Self {
    self.options.progress_updated = Some(Arc::new(progress) as BrowseProgressUpdated);
    self
  }
}

impl<'a> IntoFuture for Browse<'a> {
  type Output = Result<BrowseResponse, Error>;
  type IntoFuture = Sending<'a, BrowseResponse>;

  fn into_future(self) -> Self::IntoFuture {
    Box::pin(async move {
      self
        .client
        .browse_user(&self.username, self.options, &CancellationToken::new())
        .await
    })
  }
}

impl std::fmt::Debug for Browse<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Browse")
      .field("username", &self.username)
      .field("options", &self.options)
      .finish()
  }
}
