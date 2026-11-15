//! Other users as the server sees them: status, statistics, privileges, and watching them so
//! their status changes are pushed to the client.

use soulseek_proto::server::{
  GetStatusRequest, GetUserStatsRequest, UnwatchUser, UserPrivilegesRequest, UserStatistics,
  UserStatus, WatchUserRequest, WatchUserResponse, WatchedUser,
};
use soulseek_proto::{ServerCode, ServerRequest};

use super::Client;
use super::request::{non_blank, user_key};
use crate::error::ClientError;

impl Client {
  /// Returns the presence of `username`.
  ///
  /// The server answers for any name.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  ///
  /// # Cancel safety
  ///
  /// A call dropped stops waiting at once, so a status that comes after it is raised as not asked
  /// for.
  pub async fn get_user_status(&self, username: &str) -> Result<UserStatus, ClientError> {
    non_blank("username", username)?;
    self
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
  /// Returns [`ClientError::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn get_user_statistics(&self, username: &str) -> Result<UserStatistics, ClientError> {
    non_blank("username", username)?;
    self
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
  /// Returns [`ClientError::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn get_user_privileged(&self, username: &str) -> Result<bool, ClientError> {
    non_blank("username", username)?;
    self
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
  /// Returns [`ClientError::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns
  /// [`ClientError::UserNotFound`] for a name nobody has. Returns a [`ClientError::Client`]
  /// wrapping the cause if the request could not be written or the server connection closed before
  /// the answer.
  pub async fn watch_user(&self, username: &str) -> Result<WatchedUser, ClientError> {
    non_blank("username", username)?;
    let watched: WatchUserResponse = self
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
      .ok_or_else(|| ClientError::UserNotFound(format!("User {username} does not exist")))
  }

  /// Stops the status pushes a watch started.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written.
  pub async fn unwatch_user(&self, username: &str) -> Result<(), ClientError> {
    non_blank("username", username)?;
    self
      .tell(
        &format!("unwatch user {username}"),
        ServerRequest::UnwatchUser(UnwatchUser {
          username: username.to_owned(),
        }),
      )
      .await
  }
}
