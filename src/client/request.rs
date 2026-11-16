//! The shape every server operation shares: check the arguments, require a logged-in session,
//! register the wait for the reply before writing the request, and report a failure as it is,
//! led by what was asked unless it is a timeout, a cancel or one of the operation's own
//! outcomes, which say so themselves.

use soulseek_proto::{ServerCode, ServerRequest};
use tokio_util::sync::CancellationToken;

use super::Client;
use crate::error::{Error, Repr};
use crate::waiter::WaitKey;

impl Client {
  /// fails unless logged in, naming what was attempted
  pub(crate) fn require_logged_in(&self, verb: &str) -> Result<(), Error> {
    self.check_not_shut_down()?;
    let state = self.state();
    if state.is_logged_in() {
      return Ok(());
    }
    Err(not_logged_in(verb, state))
  }

  /// Writes `request` and returns the reply that completes the wait under `key`, which is in
  /// place before the request is written so a fast reply cannot be missed
  pub(super) async fn ask<T: Send + 'static>(
    &self,
    verb: &str,
    key: WaitKey,
    request: ServerRequest,
  ) -> Result<T, Error> {
    self.require_logged_in(verb)?;
    let reply = self.waiter().wait::<T>(key, None, CancellationToken::new());
    self
      .write_server_message(&request)
      .await
      .map_err(|error| failed(verb, error))?;
    reply.await.map_err(|error| failed(verb, error))
  }

  /// [`Client::ask`] for a request whose answer is matched by its code alone and depends on what
  /// was asked: one at a time, so a caller never takes the answer to another's request
  pub(super) async fn ask_in_order<T: Send + 'static>(
    &self,
    verb: &str,
    key: WaitKey,
    request: ServerRequest,
  ) -> Result<T, Error> {
    let _turn = self.inner.ordered_asks.lock().await;
    self.ask(verb, key, request).await
  }

  /// writes `request`, which the server does not answer, and waits until it is on the socket
  pub(super) async fn tell(&self, verb: &str, request: ServerRequest) -> Result<(), Error> {
    self.require_logged_in(verb)?;
    self
      .write_server_message(&request)
      .await
      .map_err(|error| failed(verb, error))
  }
}

/// what a failed operation reports: its own outcomes as they are, anything else led by the
/// failure to `verb`
pub(crate) fn failed(verb: &str, error: Error) -> Error {
  match error.repr() {
    Repr::Timeout(_)
    | Repr::Canceled
    | Repr::UserOffline(_)
    | Repr::UserNotFound(_)
    | Repr::Room { .. }
    | Repr::NoResponse(_) => error,
    _ => {
      tracing::debug!(verb, %error, "server operation failed");
      error.context(format!("failed to {verb}"))
    }
  }
}

pub(super) fn user_key(code: ServerCode, username: &str) -> WaitKey {
  WaitKey::ServerUser {
    code,
    username: username.to_owned(),
  }
}

pub(super) fn interest_key(code: ServerCode, interest: &str) -> WaitKey {
  WaitKey::ServerInterest {
    code,
    interest: interest.to_owned(),
  }
}

pub(super) fn room_key(code: ServerCode, room: &str) -> WaitKey {
  WaitKey::ServerRoom {
    code,
    room: room.to_owned(),
  }
}

pub(super) fn room_user_key(code: ServerCode, room: &str, username: &str) -> WaitKey {
  WaitKey::ServerRoomUser {
    code,
    room: room.to_owned(),
    username: username.to_owned(),
  }
}

/// fails when `value` is empty or only whitespace, naming it as `name`
pub(crate) fn non_blank(name: &str, value: &str) -> Result<(), Error> {
  if value.trim().is_empty() {
    return Err(Error::invalid_argument(format!(
      "the {name} must not be empty or only whitespace"
    )));
  }
  Ok(())
}

/// fails when `value` is empty, naming it as `name`; whitespace is allowed
pub(super) fn non_empty(name: &str, value: &str) -> Result<(), Error> {
  if value.is_empty() {
    return Err(Error::invalid_argument(format!(
      "the {name} must not be empty"
    )));
  }
  Ok(())
}

/// The refusal of the operation `verb` while the session is in `state`, short of logged in.
pub(super) fn not_logged_in(verb: &str, state: crate::session::State) -> Error {
  Error::invalid_operation(format!(
    "the server connection must be connected and logged in to {verb} (currently: {state})"
  ))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn an_operation_keeps_its_own_outcomes_and_names_itself_in_the_rest() {
    assert!(matches!(
      failed("join chat room x", Error::timeout("late".into())).repr(),
      Repr::Timeout(_)
    ));
    assert!(matches!(
      failed(
        "get the status of user x",
        Error::user_offline("gone".into())
      )
      .repr(),
      Repr::UserOffline(_)
    ));
    let named = failed(
      "leave chat room x",
      Error::invalid_operation("closed".into()),
    );
    assert!(matches!(named.repr(), Repr::InvalidOperation(_)));
    assert_eq!(named.to_string(), "failed to leave chat room x: closed");
  }
}
