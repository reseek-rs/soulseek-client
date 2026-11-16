//! The one error type every client operation returns. Callers ask it for its kind and for what
//! the kind carries; its message says what failed and where, and its source is the underlying
//! error when there is one.

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use soulseek_proto::DecodeError;

/// Underlying error of an [`Error`].
///
/// Shared so errors can be cloned into events and handed to several waiters.
pub(crate) type Source = Arc<dyn StdError + Send + Sync>;

/// Error of a client operation.
///
/// [`Error::kind`] says what went wrong, the `Display` text says what failed and where, and
/// [`std::error::Error::source`] is the error underneath when there is one.
#[derive(Clone)]
pub struct Error(Box<Repr>);

impl fmt::Debug for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Debug::fmt(&self.0, f)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Display::fmt(&self.0, f)
  }
}

impl StdError for Error {
  fn source(&self) -> Option<&(dyn StdError + 'static)> {
    // the error inside the shared pointer, so a caller can downcast it
    let cause: &(dyn StdError + Send + Sync + 'static) = &**self.0.underneath()?;
    Some(cause)
  }
}

impl From<Repr> for Error {
  fn from(repr: Repr) -> Self {
    Self(Box::new(repr))
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
  #[error("{message}")]
  Connect {
    message: String,
    source: Option<Source>,
  },
  #[error("{0}")]
  LoginRejected(String),
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
  #[error("failed to retrieve address for user {username}: {source}")]
  UserAddress { username: String, source: Source },
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
  #[error("{message}")]
  Operation {
    message: String,
    source: Option<Source>,
  },
  #[error("the client has been shut down")]
  Shutdown,
}

/// What kind of failure an [`Error`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
  /// The caller canceled, or the client was disconnected while the operation ran.
  Canceled,
  /// The server address could not be resolved.
  Address,
  /// The listener could not bind its address.
  Listen,
  /// Connecting to the server failed, other than by a rejected or unanswered login.
  Connect,
  /// The server refused the login.
  ///
  /// [`Error::reason`] is the server's reason verbatim.
  LoginRejected,
  /// The server never answered the login.
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
  ///
  /// [`Error::username`] names the user.
  UserOffline,
  /// The server reports that the user does not exist.
  ///
  /// [`Error::username`] names the user.
  UserNotFound,
  /// Asking the server for a user's address failed.
  UserAddress,
  /// A chat room request failed.
  Room(RoomKind),
  /// A request was written and never answered.
  ///
  /// The request is a room join or leave, which the server leaves unanswered for a room the
  /// client is already in or not in, or a question of a file's place in a peer's queue.
  NoResponse,
  /// The token is already in use.
  ///
  /// Another search or transfer uses it, or it is a connection request's ticket.
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
  /// Covers no upload slot, a bad start offset, a source that ended early or changed size, and
  /// a stream that cannot seek.
  Transfer,
  /// Reading or writing a local file or stream failed.
  Io,
  /// An operation failed for another reason.
  ///
  /// The message names the operation.
  Operation,
  /// The client has been shut down and accepts no more work.
  Shutdown,
}

/// Which socket-level failure an [`ErrorKind::Connection`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
      | Self::Connect { source, .. }
      | Self::Connection { source, .. }
      | Self::Message { source, .. }
      | Self::Transfer { source, .. }
      | Self::Io { source, .. }
      | Self::Operation { source, .. } => source.as_ref(),
      Self::UserAddress { source, .. } => Some(source),
      _ => None,
    }
  }
}

impl Error {
  /// Returns what kind of failure the error is.
  pub fn kind(&self) -> ErrorKind {
    match &*self.0 {
      Repr::InvalidArgument(_) => ErrorKind::InvalidArgument,
      Repr::InvalidOperation(_) => ErrorKind::InvalidOperation,
      Repr::Timeout(_) => ErrorKind::Timeout,
      Repr::Canceled => ErrorKind::Canceled,
      Repr::Address(_) => ErrorKind::Address,
      Repr::Listen { .. } => ErrorKind::Listen,
      Repr::Connect { .. } => ErrorKind::Connect,
      Repr::LoginRejected(_) => ErrorKind::LoginRejected,
      Repr::LoginNoResponse(_) => ErrorKind::LoginNoResponse,
      Repr::Kicked => ErrorKind::Kicked,
      Repr::Connection { kind, .. } => ErrorKind::Connection(*kind),
      Repr::Message { kind, .. } => ErrorKind::Message(*kind),
      Repr::UserOffline(_) => ErrorKind::UserOffline,
      Repr::UserNotFound(_) => ErrorKind::UserNotFound,
      Repr::UserAddress { .. } => ErrorKind::UserAddress,
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
      Repr::Operation { .. } => ErrorKind::Operation,
      Repr::Shutdown => ErrorKind::Shutdown,
    }
  }

  /// Returns `true` if a wait, a connect or an inactivity deadline expired.
  pub fn is_timeout(&self) -> bool {
    matches!(&*self.0, Repr::Timeout(_))
  }

  /// Returns `true` if the operation was canceled.
  pub fn is_canceled(&self) -> bool {
    matches!(&*self.0, Repr::Canceled)
  }

  /// Returns `true` if the client has been shut down.
  pub fn is_shutdown(&self) -> bool {
    matches!(&*self.0, Repr::Shutdown)
  }

  /// Returns the user the error is about.
  ///
  /// Present for [`ErrorKind::UserOffline`], [`ErrorKind::UserNotFound`],
  /// [`ErrorKind::UserAddress`] and [`ErrorKind::DuplicateTransfer`].
  pub fn username(&self) -> Option<&str> {
    match &*self.0 {
      Repr::UserOffline(username)
      | Repr::UserNotFound(username)
      | Repr::UserAddress { username, .. }
      | Repr::DuplicateTransfer { username, .. } => Some(username),
      _ => None,
    }
  }

  /// Returns the file the error is about.
  ///
  /// Present for [`ErrorKind::DuplicateTransfer`], as the remote filename, and for
  /// [`ErrorKind::FileNotFound`], as the local path.
  pub fn filename(&self) -> Option<&str> {
    match &*self.0 {
      Repr::DuplicateTransfer { filename, .. } | Repr::FileNotFound(filename) => Some(filename),
      _ => None,
    }
  }

  /// Returns the token of an [`ErrorKind::DuplicateToken`].
  pub fn token(&self) -> Option<u32> {
    match &*self.0 {
      Repr::DuplicateToken(token) => Some(*token),
      _ => None,
    }
  }

  /// Returns the reason the server or the peer gave, as it was sent.
  ///
  /// Present for [`ErrorKind::LoginRejected`] and [`ErrorKind::TransferRejected`].
  pub fn reason(&self) -> Option<&str> {
    match &*self.0 {
      Repr::LoginRejected(reason) | Repr::TransferRejected(reason) => Some(reason),
      _ => None,
    }
  }

  /// Returns the sizes of an [`ErrorKind::SizeMismatch`], in bytes.
  ///
  /// The first is the size the client expects, the second the size the peer reports.
  pub fn size_mismatch(&self) -> Option<(u64, u64)> {
    match &*self.0 {
      Repr::SizeMismatch { local, remote } => Some((*local, *remote)),
      _ => None,
    }
  }

  /// what the error holds, for matching inside the crate
  pub(crate) fn repr(&self) -> &Repr {
    &self.0
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

  pub(crate) fn login_rejected(text: String) -> Self {
    Repr::LoginRejected(text).into()
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

  /// Creates the generic wrapper an operation returns.
  ///
  /// `message` names the operation.
  pub(crate) fn operation(message: impl Into<String>, source: Option<Error>) -> Self {
    Repr::Operation {
      message: message.into(),
      source: source.map(|source| Arc::new(source) as Source),
    }
    .into()
  }

  /// Creates the error of a transfer that could not go on, without an underlying cause.
  pub(crate) fn transfer(message: impl Into<String>) -> Self {
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
