//! Other users as the server sees them: status, statistics, privileges, and watching them so
//! their status changes are pushed to the client.

use std::net::SocketAddr;

use crate::events::{Event, Events, PrivateMessage};
use crate::user::{MessageConnection, Users};
use soulseek_proto::server::{
  GetStatusRequest, GetUserStatsRequest, UnwatchUser, UserPrivilegesRequest, UserStatistics,
  UserStatus, WatchUserRequest, WatchUserResponse, WatchedUser,
};
use soulseek_proto::{ServerCode, ServerRequest};

use super::request::{non_blank, user_key};
use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;

impl Users<'_> {
  /// Returns the presence of `username`.
  ///
  /// The server answers for any name.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  ///
  /// # Cancel safety
  ///
  /// A call dropped stops waiting at once, so a status that comes after it is raised as not asked
  /// for.
  pub async fn status(self, username: &str) -> Result<UserStatus, Error> {
    non_blank("username", username)?;
    self
      .client
      .ask(
        &format!("get status for user {username}"),
        user_key(ServerCode::GetStatus, username),
        ServerRequest::GetStatus(GetStatusRequest {
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Returns the upload speed and share counts of `username`.
  ///
  /// They are zeros for a name nobody has.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn statistics(self, username: &str) -> Result<UserStatistics, Error> {
    non_blank("username", username)?;
    self
      .client
      .ask(
        &format!("get statistics for user {username}"),
        user_key(ServerCode::GetUserStats, username),
        ServerRequest::GetUserStats(GetUserStatsRequest {
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Returns whether `username` is privileged.
  ///
  /// The status of a watched user says the same.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn is_privileged(self, username: &str) -> Result<bool, Error> {
    non_blank("username", username)?;
    self
      .client
      .ask(
        &format!("get privileges for user {username}"),
        user_key(ServerCode::UserPrivileges, username),
        ServerRequest::UserPrivileges(UserPrivilegesRequest {
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Watches `username` and returns their status and statistics.
  ///
  /// The server pushes their status from now on, until the session ends.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns [`ErrorKind::UserNotFound`] for a name nobody
  /// has. Returns an [`ErrorKind::Connection`] if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn watch(self, username: &str) -> Result<WatchedUser, Error> {
    non_blank("username", username)?;
    let watched: WatchUserResponse = self
      .client
      .ask(
        &format!("watch user {username}"),
        user_key(ServerCode::WatchUser, username),
        ServerRequest::WatchUser(WatchUserRequest {
          username: username.to_owned(),
        }),
      )
      .await?;
    watched
      .user
      .ok_or_else(|| Error::user_not_found(username.to_owned()))
  }

  /// Stops the status pushes a watch started.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn unwatch(self, username: &str) -> Result<(), Error> {
    non_blank("username", username)?;
    self
      .client
      .tell(
        &format!("unwatch user {username}"),
        ServerRequest::UnwatchUser(UnwatchUser {
          username: username.to_owned(),
        }),
      )
      .await
  }
}

impl Users<'_> {
  /// Returns a stream of what the server tells the client about users.
  pub fn events(self) -> Events<crate::user::Event> {
    self.client.events_of(|event| match event {
      Event::User(event) => Some(event),
      _ => None,
    })
  }

  /// Returns a stream of the private messages to the client.
  pub fn messages(self) -> Events<PrivateMessage> {
    self.client.events_of(|event| match event {
      Event::PrivateMessage(message) => Some(message),
      _ => None,
    })
  }

  /// Returns every user with an established message connection.
  ///
  /// A user has at most one.
  pub fn connections(self) -> Vec<MessageConnection> {
    self.client.inner.peers.message_connections()
  }

  /// Returns the address of the message connection to `username`.
  ///
  /// The connection is the established one the client's requests to `username` go on, if there
  /// is one.
  pub fn connection_address(self, username: &str) -> Option<SocketAddr> {
    self
      .client
      .inner
      .peers
      .cached(username)
      .map(|conn| conn.handle().remote())
  }
}
