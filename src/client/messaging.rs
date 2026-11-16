//! Private messages: sending one, and acknowledging one the client received so the server
//! stops replaying it at every login.

use crate::user::Users;
use soulseek_proto::ServerRequest;
use soulseek_proto::server::{AcknowledgePrivateMessage, PrivateMessageRequest};

use super::Client;
use super::request::{failed, non_blank, non_empty};
use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;

impl Client {
  /// fails unless connected and logging in or logged in, naming what was attempted
  pub(super) fn require_logging_in_or_logged_in(&self, verb: &str) -> Result<(), Error> {
    let state = self.state();
    if matches!(
      state,
      crate::session::State::LoggingIn | crate::session::State::LoggedIn
    ) {
      return Ok(());
    }
    Err(Error::invalid_operation(format!(
      "the server connection must be connected and logging in or logged in to {verb} (currently: {state})"
    )))
  }
}

impl Users<'_> {
  /// Sends `message` to `username`.
  ///
  /// The server does not echo it back to the client.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace, or if
  /// `message` is empty. Returns [`ErrorKind::Shutdown`] if the client has been shut down.
  /// Returns [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Operation`] wrapping the cause if the request could not be written.
  pub async fn message(self, username: &str, message: &str) -> Result<(), Error> {
    non_blank("username", username)?;
    non_empty("message", message)?;
    self
      .client
      .tell(
        &format!("send private message to user {username}"),
        ServerRequest::PrivateMessage(PrivateMessageRequest {
          username: username.to_owned(),
          message: message.to_owned(),
        }),
      )
      .await
  }

  /// Acknowledges the private message `id`.
  ///
  /// Allowed while logging in too, because the server replays stored messages before the login
  /// completes.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidOperation`] if the client is neither logging in nor logged in.
  /// Returns an [`ErrorKind::Operation`] wrapping the cause if the request could not be written.
  pub async fn acknowledge_message(self, id: u32) -> Result<(), Error> {
    let verb = format!("acknowledge private message with ID {id}");
    self.client.require_logging_in_or_logged_in(&verb)?;
    self
      .client
      .write_server_message(&ServerRequest::AcknowledgePrivateMessage(
        AcknowledgePrivateMessage { id },
      ))
      .await
      .map_err(|error| failed(&verb, error))
  }
}
