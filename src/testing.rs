//! The client's engine, public only for the crate's own integration tests, which enable the
//! `testing` feature: connections, the waits replies complete and the tokens transfers take.

use std::future::Future;
use std::net::SocketAddr;

use soulseek_proto::{RemoteFilename, ServerRequest};
use tokio_util::sync::CancellationToken;

pub use crate::conn::{Conn, ConnKind, MessageConn};
pub use crate::peers::IncomingBrowse;
pub use crate::waiter::{WaitKey, Waiter};

use crate::{Client, ClientError, SearchScope};

/// What the client does inside its operations, called the way the operations call it
pub trait Engine {
  /// the table that matches replies to the requests waiting for them
  fn waiter(&self) -> &Waiter;

  /// the next token of the client's count
  fn next_token(&self) -> u32;

  /// writes several messages to the server as one write
  fn write_server_messages(
    &self,
    messages: &[ServerRequest],
  ) -> impl Future<Output = Result<(), ClientError>> + Send;

  /// [`Client`]'s message connection to `username`, made if there is none; a given address is
  /// taken as the server's word for the user
  fn get_or_add_message_connection(
    &self,
    username: &str,
    endpoint: Option<SocketAddr>,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<MessageConn, ClientError>> + Send;

  /// a transfer connection to `username` for the transfer `token`
  fn get_transfer_connection(
    &self,
    username: &str,
    endpoint: SocketAddr,
    token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, ClientError>> + Send;

  /// the transfer connection `username` opens for `filename` under `remote_token`
  fn await_transfer_connection(
    &self,
    username: &str,
    filename: RemoteFilename,
    remote_token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, ClientError>> + Send + 'static;

  /// sends a search for `text` with `token` to `scope` without waiting for responses
  fn send_search_request(
    &self,
    scope: &SearchScope,
    token: u32,
    text: &str,
  ) -> impl Future<Output = Result<(), ClientError>> + Send;
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
  ) -> impl Future<Output = Result<(), ClientError>> + Send {
    Client::write_server_messages(self, messages)
  }

  fn get_or_add_message_connection(
    &self,
    username: &str,
    endpoint: Option<SocketAddr>,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<MessageConn, ClientError>> + Send {
    self.message_connection(username, endpoint, cancel)
  }

  fn get_transfer_connection(
    &self,
    username: &str,
    endpoint: SocketAddr,
    token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, ClientError>> + Send {
    Client::get_transfer_connection(self, username, endpoint, token, cancel)
  }

  fn await_transfer_connection(
    &self,
    username: &str,
    filename: RemoteFilename,
    remote_token: u32,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Conn, ClientError>> + Send + 'static {
    Client::await_transfer_connection(self, username, filename, remote_token, cancel)
  }

  fn send_search_request(
    &self,
    scope: &SearchScope,
    token: u32,
    text: &str,
  ) -> impl Future<Output = Result<(), ClientError>> + Send {
    Client::send_search_request(self, scope, token, text)
  }
}
