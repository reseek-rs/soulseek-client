//! The client's engine, public only for the crate's own integration tests, which enable the
//! `__testing` feature: connections, the waits replies complete and the tokens transfers take.

use std::future::Future;
use std::net::SocketAddr;

use soulseek_proto::{RemoteFilename, ServerRequest};
use tokio_util::sync::CancellationToken;

pub use crate::conn::{Conn, ConnKind, MessageConn};
pub use crate::peers::IncomingBrowse;
pub use crate::waiter::{WaitKey, Waiter};

use crate::search::Scope;
use crate::{Client, Error};

/// Work the client does inside its operations.
///
/// The methods are called the way the operations call them.
pub trait Engine {
  /// Returns the table that matches replies to waiting requests.
  fn waiter(&self) -> &Waiter;

  /// Returns the next token of the client's count.
  fn next_token(&self) -> u32;

  /// Writes several messages to the server as one write.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidOperation`](crate::ErrorKind::InvalidOperation) if the client
  /// has no connection to the server, or if that connection is closing. Returns
  /// [`ErrorKind::Connection`](crate::ErrorKind::Connection) if the connection's write queue is
  /// full or the write fails.
  fn write_server_messages(
    &self,
    messages: &[ServerRequest],
  ) -> impl Future<Output = Result<(), Error>> + Send;

  /// Returns the [`Client`]'s message connection to `username`.
  ///
  /// The connection is made if there is none. A given address is taken as the server's word
  /// for the user.
  fn get_or_add_message_connection(
    &self,
    username: &str,
    address: Option<SocketAddr>,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<MessageConn, Error>> + Send;

  /// Returns a transfer connection to `username` for the transfer `token`.
  fn get_transfer_connection(
    &self,
    username: &str,
    address: SocketAddr,
    token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, Error>> + Send;

  /// Returns the transfer connection `username` opens for `filename`.
  ///
  /// The connection is the one opened under `remote_token`.
  fn await_transfer_connection(
    &self,
    username: &str,
    filename: RemoteFilename,
    remote_token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, Error>> + Send + 'static;

  /// Sends a search for `text` with `token` to `scope`.
  ///
  /// Returns without waiting for responses.
  fn send_search_request(
    &self,
    scope: &Scope,
    token: u32,
    text: &str,
  ) -> impl Future<Output = Result<(), Error>> + Send;
}

impl Engine for Client {
  fn waiter(&self) -> &Waiter {
    Client::waiter(self)
  }

  fn next_token(&self) -> u32 {
    Client::next_token(self)
  }

  fn write_server_messages(
    &self,
    messages: &[ServerRequest],
  ) -> impl Future<Output = Result<(), Error>> + Send {
    Client::write_server_messages(self, messages)
  }

  fn get_or_add_message_connection(
    &self,
    username: &str,
    address: Option<SocketAddr>,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<MessageConn, Error>> + Send {
    self.message_connection(username, address, cancel)
  }

  fn get_transfer_connection(
    &self,
    username: &str,
    address: SocketAddr,
    token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, Error>> + Send {
    Client::get_transfer_connection(self, username, address, token, cancel)
  }

  fn await_transfer_connection(
    &self,
    username: &str,
    filename: RemoteFilename,
    remote_token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, Error>> + Send + 'static {
    Client::await_transfer_connection(self, username, filename, remote_token, cancel)
  }

  fn send_search_request(
    &self,
    scope: &Scope,
    token: u32,
    text: &str,
  ) -> impl Future<Output = Result<(), Error>> + Send {
    Client::send_search_request(self, scope, token, text)
  }
}
