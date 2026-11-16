//! The one error type every client operation returns.
//!
//! Callers ask it for its kind and for what the kind carries. Its message says what failed and
//! where, and its source is the underlying error when there is one.

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use soulseek_proto::DecodeError;

use crate::transfer::Transfer;

/// Underlying error of an [`Error`].
///
/// Shared so errors can be cloned into events and handed to several waiters.
pub(crate) type Source = Arc<dyn StdError + Send + Sync>;

/// Error of a client operation.
///
/// [`Error::kind`] says what went wrong, the `Display` text says what failed and where, and
/// [`std::error::Error::source`] is the error underneath when there is one.
#[derive(Clone)]
pub struct Error(Box<Inner>);

#[derive(Clone, Debug)]
struct Inner {
  /// what was being done, outermost first, written ahead of the failure
  context: Option<String>,
  repr: Repr,
  /// the transfer this error ended, as it ended
  transfer: Option<Box<Transfer>>,
  /// the source and the reason the application gave, which stand in for the error's own
  source: Option<Source>,
  reason: Option<String>,
}

impl fmt::Debug for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Debug::fmt(&self.0, f)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if let Some(context) = &self.0.context {
      write!(f, "{context}: ")?;
    }
    fmt::Display::fmt(&self.0.repr, f)
  }
}

impl StdError for Error {
  fn source(&self) -> Option<&(dyn StdError + 'static)> {
    // the error inside the shared pointer, so a caller can downcast it
    let source = self
      .0
      .source
      .as_ref()
      .or_else(|| self.0.repr.underneath())?;
    let cause: &(dyn StdError + Send + Sync + 'static) = &**source;
    Some(cause)
  }
}

/// Serializes as a map of `kind`, the [`ErrorKind`], and `message`, the `Display` text.
#[cfg(feature = "serde")]
impl serde::Serialize for Error {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;

    let mut error = serializer.serialize_struct("Error", 2)?;
    error.serialize_field("kind", &self.kind())?;
    error.serialize_field("message", &self.to_string())?;
    error.end()
  }
}

impl From<Repr> for Error {
  fn from(repr: Repr) -> Self {
    Self(Box::new(Inner {
      context: None,
      repr,
      transfer: None,
      source: None,
      reason: None,
    }))
  }
}

/// What an [`Error`] holds: its kind with what the kind carries
// thiserror writes the `Display` text; `Error` gives the cause itself
#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum Repr {
  #[error("{0}")]
  InvalidArgument(String),
  #[error("{0}")]
  InvalidOperation(String),
  #[error("{0}")]
  Timeout(String),
  #[error("the operation was canceled")]
  Canceled,
  #[error("{0}")]
  Address(String),
  #[error("{message}")]
  Listen {
    message: String,
    source: Option<Source>,
  },
  #[error(
    "the server rejected login attempt: {reason}{}",
    .detail.as_ref().map(|detail| format!(": {detail}")).unwrap_or_default()
  )]
  LoginRejected {
    reason: String,
    detail: Option<String>,
  },
  #[error("{0}")]
  LoginNoResponse(String),
  #[error("kicked from server")]
  Kicked,
  #[error("{message}")]
  Connection {
    kind: ConnectionKind,
    message: String,
    source: Option<Source>,
  },
  #[error("{message}")]
  Message {
    kind: MessageKind,
    message: String,
    source: Option<Source>,
  },
  #[error("user {0} appears to be offline")]
  UserOffline(String),
  #[error("user {0} does not exist")]
  UserNotFound(String),
  #[error("{message}")]
  Room { kind: RoomKind, message: String },
  #[error("{0}")]
  NoResponse(String),
  #[error("token {0} is already in use")]
  DuplicateToken(u32),
  #[error("a transfer of {filename} with {username} is already active")]
  DuplicateTransfer { username: String, filename: String },
  #[error("local file not found: {0}")]
  FileNotFound(String),
  #[error("transfer rejected: {0}")]
  TransferRejected(String),
  #[error("{0}")]
  TransferFailed(String),
  #[error("transfer aborted: size mismatch (local {local}, remote {remote})")]
  SizeMismatch { local: u64, remote: u64 },
  #[error("transfer error: {message}")]
  Transfer {
    message: String,
    source: Option<Source>,
  },
  #[error("I/O error: {message}")]
  Io {
    message: String,
    source: Option<Source>,
  },
  #[error("{0}")]
  Operation(String),
  #[error("the client has been shut down")]
  Shutdown,
  /// made by the application, with `Error::new`
  #[error("{message}")]
  Made { kind: ErrorKind, message: String },
}

/// What kind of failure an [`Error`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ErrorKind {
  /// The caller passed a value the operation does not accept.
  ///
  /// Nothing was sent.
  InvalidArgument,
  /// The client or the connection is in a state that does not allow the operation.
  InvalidOperation,
  /// A wait, a connect or an inactivity deadline expired.
  Timeout,
  /// The caller, a disconnect or a shutdown canceled the operation.
  Canceled,
  /// The address of the server, or of its proxy, could not be resolved.
  Address,
  /// The listener could not bind its address.
  Listen,
  /// The server refused the login.
  ///
  /// [`Error::reason`] is the server's reason verbatim.
  LoginRejected,
  /// The server did not answer the login in time.
  ///
  /// The login was written and the connection stayed up, which is how the server treats a banned
  /// account.
  LoginNoResponse,
  /// Another session logged in with the client's credentials.
  Kicked,
  /// A socket-level failure on one connection.
  Connection(ConnectionKind),
  /// A frame could not be read, decoded or encoded.
  Message(MessageKind),
  /// The server reports the user as offline.
  UserOffline,
  /// The server reports that the user does not exist.
  UserNotFound,
  /// A chat room request failed.
  Room(RoomKind),
  /// A request was written and never answered.
  ///
  /// The request is a room join or leave, which the server leaves unanswered for a room the
  /// client is already in or not in, or a question of a file's place in a peer's queue.
  NoResponse,
  /// The token is already in use.
  ///
  /// Another transfer uses it.
  DuplicateToken,
  /// A transfer of the same file is already active.
  ///
  /// The active transfer is with the same user in the same direction.
  DuplicateTransfer,
  /// The local file to upload does not exist.
  FileNotFound,
  /// The peer refused or canceled a transfer.
  ///
  /// [`Error::reason`] is the peer's reason, kept as it was sent.
  TransferRejected,
  /// The uploader reported that the upload of a file to the client failed.
  TransferFailed,
  /// The size the peer reports for a file differs from the size the client expects.
  ///
  /// Nothing settled which one is right.
  SizeMismatch,
  /// A transfer could not go on.
  ///
  /// Covers no upload slot, a bad start offset, an uploader that names no size, a source that
  /// ended early or changed size, and a stream that cannot seek.
  Transfer,
  /// Reading or writing a local file or stream failed.
  Io,
  /// An operation failed for a reason no other kind names.
  ///
  /// The message says what failed.
  Operation,
  /// The client has been shut down and accepts no more work.
  Shutdown,
}

/// Which socket-level failure an [`ErrorKind::Connection`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ConnectionKind {
  /// Connecting failed, or the connection closed or was refused for another reason.
  Other,
  /// A read failed or the remote closed the connection.
  Read,
  /// A write failed.
  Write,
  /// The connection's write queue was full.
  ///
  /// The write was dropped and the connection closed.
  WriteDropped,
  /// The SOCKS5 proxy refused or failed the connection.
  Proxy,
  /// The remote ended or reset a transfer connection before the transfer was done.
  RemoteClosed,
}

/// Which framing failure an [`ErrorKind::Message`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum MessageKind {
  /// A message could not be encoded or handled.
  Other,
  /// A frame could not be read or decoded.
  Read,
  /// A compressed payload could not be inflated.
  Compression,
}

/// Which room failure an [`ErrorKind::Room`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum RoomKind {
  /// Any other room failure.
  Other,
  /// Joining would create a room whose name a private room already holds.
  ///
  /// The private room is one the client is not in.
  JoinForbidden,
}

impl Repr {
  fn underneath(&self) -> Option<&Source> {
    match self {
      Self::Listen { source, .. }
      | Self::Connection { source, .. }
      | Self::Message { source, .. }
      | Self::Transfer { source, .. }
      | Self::Io { source, .. } => source.as_ref(),
      _ => None,
    }
  }
}

impl Error {
  /// Creates an error of `kind` whose message is `message`.
  ///
  /// The client returns its own errors; this is for an application that hands one to code
  /// written for the client's, as its tests do. [`Error::with_source`] and
  /// [`Error::with_reason`] give it a source and a reason; it has no sizes and no transfer.
  pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
    Repr::Made {
      kind,
      message: message.into(),
    }
    .into()
  }

  /// Returns the same error with `source` as its [`std::error::Error::source`].
  ///
  /// For an error made with [`Error::new`], as an application's tests make one that a cause can
  /// be read from. The message is left as it is.
  #[must_use]
  pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
    self.0.source = Some(Arc::new(source));
    self
  }

  /// Returns the same error with `reason` as its [`Error::reason`].
  ///
  /// For an error made with [`Error::new`], as an application's tests make a refusal. The
  /// message is left as it is.
  #[must_use]
  pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
    self.0.reason = Some(reason.into());
    self
  }

  /// Returns what kind of failure the error is.
  pub fn kind(&self) -> ErrorKind {
    match &self.0.repr {
      Repr::InvalidArgument(_) => ErrorKind::InvalidArgument,
      Repr::InvalidOperation(_) => ErrorKind::InvalidOperation,
      Repr::Timeout(_) => ErrorKind::Timeout,
      Repr::Canceled => ErrorKind::Canceled,
      Repr::Address(_) => ErrorKind::Address,
      Repr::Listen { .. } => ErrorKind::Listen,
      Repr::LoginRejected { .. } => ErrorKind::LoginRejected,
      Repr::LoginNoResponse(_) => ErrorKind::LoginNoResponse,
      Repr::Kicked => ErrorKind::Kicked,
      Repr::Connection { kind, .. } => ErrorKind::Connection(*kind),
      Repr::Message { kind, .. } => ErrorKind::Message(*kind),
      Repr::UserOffline(_) => ErrorKind::UserOffline,
      Repr::UserNotFound(_) => ErrorKind::UserNotFound,
      Repr::Room { kind, .. } => ErrorKind::Room(*kind),
      Repr::NoResponse(_) => ErrorKind::NoResponse,
      Repr::DuplicateToken(_) => ErrorKind::DuplicateToken,
      Repr::DuplicateTransfer { .. } => ErrorKind::DuplicateTransfer,
      Repr::FileNotFound(_) => ErrorKind::FileNotFound,
      Repr::TransferRejected(_) => ErrorKind::TransferRejected,
      Repr::TransferFailed(_) => ErrorKind::TransferFailed,
      Repr::SizeMismatch { .. } => ErrorKind::SizeMismatch,
      Repr::Transfer { .. } => ErrorKind::Transfer,
      Repr::Io { .. } => ErrorKind::Io,
      Repr::Operation(_) => ErrorKind::Operation,
      Repr::Shutdown => ErrorKind::Shutdown,
      Repr::Made { kind, .. } => *kind,
    }
  }

  /// Returns `true` if a wait, a connect or an inactivity deadline expired.
  pub fn is_timeout(&self) -> bool {
    self.kind() == ErrorKind::Timeout
  }

  /// Returns `true` if the operation was canceled.
  pub fn is_canceled(&self) -> bool {
    self.kind() == ErrorKind::Canceled
  }

  /// Returns `true` if the client has been shut down.
  pub fn is_shutdown(&self) -> bool {
    self.kind() == ErrorKind::Shutdown
  }

  /// Returns the reason the server or the peer gave, as it was sent.
  ///
  /// Present for [`ErrorKind::LoginRejected`] and [`ErrorKind::TransferRejected`], and for an
  /// error given one with [`Error::with_reason`].
  pub fn reason(&self) -> Option<&str> {
    if let Some(reason) = &self.0.reason {
      return Some(reason);
    }
    match &self.0.repr {
      Repr::LoginRejected { reason, .. } | Repr::TransferRejected(reason) => Some(reason),
      _ => None,
    }
  }

  /// Returns the sizes of an [`ErrorKind::SizeMismatch`], in bytes.
  ///
  /// The first is the size the client expects, the second the size the peer reports.
  pub fn size_mismatch(&self) -> Option<(u64, u64)> {
    match &self.0.repr {
      Repr::SizeMismatch { local, remote } => Some((*local, *remote)),
      _ => None,
    }
  }

  /// Returns the transfer this error ended, as it was when it ended.
  ///
  /// Present on the error a started transfer ends with, so the bytes it moved are known, as a
  /// resume needs for its start offset.
  pub fn transfer(&self) -> Option<&Transfer> {
    self.0.transfer.as_deref()
  }

  /// what the error holds, for matching inside the crate
  pub(crate) fn repr(&self) -> &Repr {
    &self.0.repr
  }

  /// the same error, carrying the transfer it ended
  pub(crate) fn ended(mut self, transfer: Transfer) -> Self {
    self.0.transfer = Some(Box::new(transfer));
    self
  }

  /// The same error, its message led by `context`, which says what was being done.
  pub(crate) fn context(mut self, context: impl Into<String>) -> Self {
    let context = context.into();
    self.0.context = Some(match self.0.context.take() {
      Some(inner) => format!("{context}: {inner}"),
      None => context,
    });
    self
  }

  pub(crate) fn invalid_argument(text: String) -> Self {
    Repr::InvalidArgument(text).into()
  }

  pub(crate) fn invalid_operation(text: String) -> Self {
    Repr::InvalidOperation(text).into()
  }

  pub(crate) fn timeout(text: String) -> Self {
    Repr::Timeout(text).into()
  }

  pub(crate) fn address(text: String) -> Self {
    Repr::Address(text).into()
  }

  pub(crate) fn login_rejected(reason: String, detail: Option<String>) -> Self {
    Repr::LoginRejected { reason, detail }.into()
  }

  pub(crate) fn login_no_response(text: String) -> Self {
    Repr::LoginNoResponse(text).into()
  }

  pub(crate) fn user_offline(text: String) -> Self {
    Repr::UserOffline(text).into()
  }

  pub(crate) fn user_not_found(text: String) -> Self {
    Repr::UserNotFound(text).into()
  }

  pub(crate) fn no_response(text: String) -> Self {
    Repr::NoResponse(text).into()
  }

  pub(crate) fn file_not_found(text: String) -> Self {
    Repr::FileNotFound(text).into()
  }

  pub(crate) fn transfer_rejected(text: String) -> Self {
    Repr::TransferRejected(text).into()
  }

  pub(crate) fn transfer_failed(text: String) -> Self {
    Repr::TransferFailed(text).into()
  }

  pub(crate) fn duplicate_token(token: u32) -> Self {
    Repr::DuplicateToken(token).into()
  }

  pub(crate) fn canceled() -> Self {
    Repr::Canceled.into()
  }

  pub(crate) fn kicked() -> Self {
    Repr::Kicked.into()
  }

  pub(crate) fn shutdown() -> Self {
    Repr::Shutdown.into()
  }

  /// Creates a socket-level error without an underlying cause.
  pub(crate) fn connection(kind: ConnectionKind, message: impl Into<String>) -> Self {
    Repr::Connection {
      kind,
      message: message.into(),
      source: None,
    }
    .into()
  }

  /// Creates a socket-level error caused by `source`.
  pub(crate) fn connection_from(
    kind: ConnectionKind,
    message: impl Into<String>,
    source: impl StdError + Send + Sync + 'static,
  ) -> Self {
    Repr::Connection {
      kind,
      message: message.into(),
      source: Some(Arc::new(source)),
    }
    .into()
  }

  /// a frame that failed to decode
  pub(crate) fn message_read(message: impl Into<String>, source: DecodeError) -> Self {
    Repr::Message {
      kind: MessageKind::Read,
      message: message.into(),
      source: Some(Arc::new(source)),
    }
    .into()
  }

  /// Creates a failure no other kind names, described by `message`.
  pub(crate) fn operation(message: impl Into<String>) -> Self {
    Repr::Operation(message.into()).into()
  }

  /// Creates the error of a transfer that could not go on, without an underlying cause.
  pub(crate) fn transfer_stopped(message: impl Into<String>) -> Self {
    Repr::Transfer {
      message: message.into(),
      source: None,
    }
    .into()
  }

  /// Creates a local I/O failure, described by `message`.
  pub(crate) fn io(message: impl Into<String>, source: std::io::Error) -> Self {
    Repr::Io {
      message: message.into(),
      source: Some(Arc::new(source)),
    }
    .into()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_made_error_answers_to_its_kind() {
    let error = Error::new(ErrorKind::Timeout, "bob did not answer");
    assert!(error.is_timeout() && !error.is_canceled());
    assert_eq!(error.to_string(), "bob did not answer");
    assert!(error.reason().is_none() && error.transfer().is_none());

    let full = std::io::Error::from(std::io::ErrorKind::StorageFull);
    let error = Error::new(ErrorKind::Io, "writing failed")
      .with_source(full)
      .with_reason("disk full");
    let source =
      StdError::source(&error).and_then(|source| source.downcast_ref::<std::io::Error>());
    assert_eq!(
      source.map(std::io::Error::kind),
      Some(std::io::ErrorKind::StorageFull)
    );
    assert_eq!(error.reason(), Some("disk full"));
    assert_eq!(error.to_string(), "writing failed");
  }

  #[cfg(feature = "serde")]
  #[test]
  fn an_error_serializes_as_its_kind_and_its_message() {
    use serde_json::{json, to_value};

    let error = Error::connection(ConnectionKind::Read, "the peer closed the connection")
      .context("reading the reply of bob");
    assert_eq!(
      to_value(&error).unwrap(),
      json!({
        "kind": { "Connection": "Read" },
        "message": "reading the reply of bob: the peer closed the connection",
      })
    );
    assert_eq!(
      to_value(Error::canceled()).unwrap(),
      json!({ "kind": "Canceled", "message": "the operation was canceled" })
    );
  }
}
