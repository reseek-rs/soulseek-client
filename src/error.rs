//! The one error type every client operation returns. Callers match on the variant (and on the
//! kind of the grouped variants); the message says what failed and where, and the source carries
//! the underlying error when there is one.

use std::error::Error as StdError;
use std::sync::Arc;

use soulseek_proto::DecodeError;

/// Underlying error of a wrapping variant.
///
/// Shared so errors can be cloned into events and handed to several waiters.
pub type Source = Arc<dyn StdError + Send + Sync>;

/// Everything that can go wrong in the client.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
  /// The caller passed a value the operation does not accept.
  ///
  /// Nothing was sent.
  #[error("{0}")]
  InvalidArgument(String),
  /// The client or the connection is in a state that does not allow the operation.
  #[error("{0}")]
  InvalidOperation(String),
  /// A wait, a connect or an inactivity deadline expired.
  #[error("{0}")]
  Timeout(String),
  /// The caller canceled, or the client was disconnected while the operation ran.
  #[error("the operation was canceled")]
  Canceled,
  /// The server address could not be resolved.
  #[error("{0}")]
  Address(String),
  /// The listener could not bind its address.
  #[error("{message}")]
  Listen {
    /// What failed.
    message: String,
    #[source]
    /// Error underneath.
    source: Option<Source>,
  },
  /// Connecting to the server failed, other than by a rejected or unanswered login.
  #[error("{message}")]
  Connect {
    /// What failed.
    message: String,
    #[source]
    /// Error underneath.
    source: Option<Source>,
  },
  /// The server refused the login.
  ///
  /// The message carries the server's reason verbatim.
  #[error("{0}")]
  LoginRejected(String),
  /// The server never answered the login.
  ///
  /// The login was written and the connection stayed up, which is how the server treats a banned
  /// account.
  #[error("{0}")]
  LoginNoResponse(String),
  /// Another session logged in with the client's credentials.
  #[error("kicked from server")]
  Kicked,
  /// A socket-level failure on one connection.
  #[error("{message}")]
  Connection {
    /// What went wrong with the connection.
    kind: ConnectionKind,
    /// What failed.
    message: String,
    #[source]
    /// Error underneath.
    source: Option<Source>,
  },
  /// A frame could not be read, decoded or encoded.
  #[error("{message}")]
  Message {
    /// What went wrong with the message.
    kind: MessageKind,
    /// What failed.
    message: String,
    #[source]
    /// Error underneath.
    source: Option<Source>,
  },
  /// The server reports the user as offline.
  ///
  /// Holds the username.
  #[error("user {0} appears to be offline")]
  UserOffline(String),
  /// The server reports that the user does not exist.
  ///
  /// Holds the username.
  #[error("user {0} does not exist")]
  UserNotFound(String),
  /// Asking the server for a user's address failed.
  #[error("failed to retrieve address for user {username}: {source}")]
  UserAddress {
    /// User whose address was asked for.
    username: String,
    #[source]
    /// Error underneath.
    source: Source,
  },
  /// A chat room request failed.
  #[error("{message}")]
  Room {
    /// What went wrong with the room.
    kind: RoomKind,
    /// What failed.
    message: String,
  },
  /// A request was written and never answered.
  ///
  /// The request is a room join or leave, which the server leaves unanswered for a room the
  /// client is already in or not in, or a question of a file's place in a peer's queue.
  #[error("{0}")]
  NoResponse(String),
  /// The token is already in use.
  ///
  /// Another search or transfer uses it, or it is a connection request's ticket.
  #[error("token {0} is already in use")]
  DuplicateToken(u32),
  /// A transfer of the same file is already active.
  ///
  /// The active transfer is with the same user in the same direction.
  #[error("a transfer of {filename} with {username} is already active")]
  DuplicateTransfer {
    /// Other user.
    username: String,
    /// File.
    filename: String,
  },
  /// The local file to upload does not exist.
  #[error("local file not found: {0}")]
  FileNotFound(String),
  /// The peer refused or canceled a transfer.
  ///
  /// Carries the peer's reason, which is kept as it was sent.
  #[error("transfer rejected: {0}")]
  TransferRejected(String),
  /// The uploader reported that the upload of a file to the client failed.
  #[error("{0}")]
  TransferFailed(String),
  /// The size the peer reports for a file differs from the size the client expects.
  ///
  /// Nothing settled which one is right.
  #[error("transfer aborted: size mismatch (local {local}, remote {remote})")]
  SizeMismatch {
    /// Size the client expects.
    local: u64,
    /// Size the peer reports.
    remote: u64,
  },
  /// A transfer could not go on.
  ///
  /// Covers no upload slot, a bad start offset, a source that ended early or changed size, and
  /// a stream that cannot seek.
  #[error("transfer error: {message}")]
  Transfer {
    /// What failed.
    message: String,
    #[source]
    /// Error underneath.
    source: Option<Source>,
  },
  /// Reading or writing a local file or stream failed.
  #[error("I/O error: {message}")]
  Io {
    /// What failed.
    message: String,
    #[source]
    /// Error underneath.
    source: Option<Source>,
  },
  /// An operation failed for another reason.
  ///
  /// The message names the operation.
  #[error("{message}")]
  Operation {
    /// What failed.
    message: String,
    #[source]
    /// Error underneath.
    source: Option<Source>,
  },
  /// The client has been shut down and accepts no more work.
  #[error("the client has been shut down")]
  Shutdown,
}

/// Which socket-level failure a [`Error::Connection`] is.
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

/// Which framing failure a [`Error::Message`] is.
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

/// Which room failure a [`Error::Room`] is.
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

impl Error {
  /// Creates a socket-level error without an underlying cause.
  pub(crate) fn connection(kind: ConnectionKind, message: impl Into<String>) -> Self {
    Self::Connection {
      kind,
      message: message.into(),
      source: None,
    }
  }

  /// Creates a socket-level error caused by `source`.
  pub(crate) fn connection_from(
    kind: ConnectionKind,
    message: impl Into<String>,
    source: impl StdError + Send + Sync + 'static,
  ) -> Self {
    Self::Connection {
      kind,
      message: message.into(),
      source: Some(Arc::new(source)),
    }
  }

  /// a frame that failed to decode
  pub(crate) fn message_read(message: impl Into<String>, source: DecodeError) -> Self {
    Self::Message {
      kind: MessageKind::Read,
      message: message.into(),
      source: Some(Arc::new(source)),
    }
  }

  /// Creates the generic wrapper an operation returns.
  ///
  /// `message` names the operation.
  pub(crate) fn operation(message: impl Into<String>, source: Option<Error>) -> Self {
    Self::Operation {
      message: message.into(),
      source: source.map(|source| Arc::new(source) as Source),
    }
  }

  /// Creates the error of a transfer that could not go on, without an underlying cause.
  pub(crate) fn transfer(message: impl Into<String>) -> Self {
    Self::Transfer {
      message: message.into(),
      source: None,
    }
  }

  /// Creates a local I/O failure, described by `message`.
  pub(crate) fn io(message: impl Into<String>, source: std::io::Error) -> Self {
    Self::Io {
      message: message.into(),
      source: Some(Arc::new(source)),
    }
  }

  /// Returns the kind of a [`Error::Connection`], or `None` for any other variant.
  pub fn connection_kind(&self) -> Option<ConnectionKind> {
    match self {
      Self::Connection { kind, .. } => Some(*kind),
      _ => None,
    }
  }
}
