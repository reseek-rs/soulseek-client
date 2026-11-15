//! What the client exposes about its server session: the connection state, the tuning values
//! the server sends, and the shape of the distributed network snapshot.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};

/// Stage of the client in its server session.
///
/// A session goes [`Connecting`](Self::Connecting), [`Connected`](Self::Connected),
/// [`LoggingIn`](Self::LoggingIn), [`LoggedIn`](Self::LoggedIn), and from any of those to
/// [`Disconnecting`](Self::Disconnecting) and [`Disconnected`](Self::Disconnected).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClientState {
  /// No server connection.
  Disconnected,
  /// The TCP connect to the server is in progress.
  Connecting,
  /// The TCP connection to the server is up and no login was sent yet.
  Connected,
  /// The login was sent and its reply has not arrived.
  LoggingIn,
  /// The server accepted the client's login.
  ///
  /// Server requests can be made in this state.
  LoggedIn,
  /// The session is being torn down.
  Disconnecting,
}

impl ClientState {
  /// Returns the state's name, such as `"logged_in"`.
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Disconnected => "disconnected",
      Self::Connecting => "connecting",
      Self::Connected => "connected",
      Self::LoggingIn => "logging_in",
      Self::LoggedIn => "logged_in",
      Self::Disconnecting => "disconnecting",
    }
  }

  /// Returns `true` if the server accepted the client's login.
  pub fn is_logged_in(self) -> bool {
    self == Self::LoggedIn
  }

  /// Returns `true` if the TCP connection to the server is up.
  ///
  /// That is the case when connected, logging in or logged in.
  pub fn is_connected(self) -> bool {
    matches!(self, Self::Connected | Self::LoggingIn | Self::LoggedIn)
  }

  /// Returns `true` on the way from one settled state to another.
  ///
  /// That is the case when connecting, logging in or disconnecting.
  pub fn is_transitioning(self) -> bool {
    matches!(
      self,
      Self::Connecting | Self::LoggingIn | Self::Disconnecting
    )
  }
}

impl fmt::Display for ClientState {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// Server session read at one moment.
///
/// Holds which session it is, where it stands, and how the latest session to end ended. A
/// subscriber that missed events compares it with the last session it handled to tell which
/// edges it missed.
#[derive(Clone, Debug)]
pub struct SessionStatus {
  /// Number of the session.
  ///
  /// Sessions are counted from 1 as each connect begins, and 0 means none has begun.
  pub number: u64,
  /// Where the session stands.
  pub state: ClientState,
  /// Whether the session's TCP connection to the server came up.
  pub reached: bool,
  /// The client's username, while logged in.
  pub username: Option<String>,
  /// End of the latest session that ended.
  ///
  /// This is the end of this session once it has ended, else of one before it.
  pub last_end: Option<SessionEnd>,
}

/// How a server session ended.
///
/// Holds what the session's [`session::Event::Disconnected`](crate::session::Event::Disconnected) said.
#[derive(Clone, Debug)]
pub struct SessionEnd {
  /// Number of the session.
  pub session: u64,
  /// What ended the session.
  pub message: String,
  /// Error that ended the session, when one did.
  pub error: Option<crate::error::Error>,
  /// When the session ended.
  pub at: std::time::SystemTime,
}

/// Tuning values the server sends after login.
///
/// Each is `None` until its message arrives. The last four come only from older servers; they
/// are kept for diagnostics and never acted on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServerInfo {
  /// Upload speed a client needs to be a distributed parent, in bytes per second.
  pub parent_min_speed: Option<u32>,
  /// Divisor of upload speed for a parent's number of children.
  ///
  /// Upload speed divided by this value is the number of children a parent may take.
  pub parent_speed_ratio: Option<u32>,
  /// Time between wishlist searches, in seconds.
  pub wishlist_interval: Option<u32>,
  /// Whether the account has ever bought privileges.
  pub is_supporter: Option<bool>,
  /// The client's address as the server sees it, from the login reply.
  pub own_public_ip: Option<Ipv4Addr>,
  /// Parent inactivity timeout, in seconds.
  pub parent_inactivity_timeout: Option<u32>,
  /// Search inactivity timeout, in seconds.
  pub search_inactivity_timeout: Option<u32>,
  /// Number of parent candidates the server wanted a client to keep.
  pub minimum_parents_in_cache: Option<u32>,
  /// Distributed alive interval, in seconds.
  pub distributed_alive_interval: Option<u32>,
}

/// The client's place in the distributed search tree.
///
/// This is the place as the distributed network reports it.
#[derive(Clone, Debug, PartialEq)]
pub struct DistributedNetworkInfo {
  /// Moving average of how long passing a search to every child takes, in milliseconds.
  pub average_broadcast_latency: Option<f64>,
  /// The client's depth in the tree.
  ///
  /// This is the parent's level plus one, or 0 without a parent.
  pub branch_level: u32,
  /// Username at the top of the client's branch.
  ///
  /// This is the parent's root, or the client's own name without a parent.
  pub branch_root: String,
  /// Whether the client is the top of a branch.
  ///
  /// It is when the server sends the client searches directly.
  pub is_branch_root: bool,
  /// Most children the client takes.
  ///
  /// This is the configured limit capped by the client's upload speed.
  pub child_limit: u32,
  /// Whether the client tells the server it takes children.
  pub can_accept_children: bool,
  /// Username and address of every established child.
  pub children: Vec<(String, SocketAddr)>,
  /// The parent's username and address.
  ///
  /// The name is empty and there is no address when the client has no parent.
  pub parent: (String, Option<SocketAddr>),
  /// Whether a parent is connected.
  pub has_parent: bool,
}

/// Something that happened to the server session.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// The client moved from one state to another.
  StateChanged {
    /// State left.
    previous_state: ClientState,
    /// State entered.
    state: ClientState,
    /// Why the state changed.
    message: String,
    /// Error that moved the client, if one did.
    error: Option<crate::error::Error>,
  },
  /// The TCP connection of server session `session` is up.
  Connected {
    /// Session, counted from 1 for the client's first.
    session: u64,
  },
  /// The server accepted the login of server session `session`.
  LoggedIn {
    /// Session whose login was accepted.
    session: u64,
    /// Name the client logged in as.
    username: String,
  },
  /// Server session `session` ended at `at`.
  ///
  /// Raised once per session, after its last [`session::Event::StateChanged`](crate::session::Event::StateChanged).
  Disconnected {
    /// Session that ended.
    session: u64,
    /// Why the session ended.
    message: String,
    /// Error that ended the session, if one did.
    error: Option<crate::error::Error>,
    /// When the session ended.
    at: std::time::SystemTime,
  },
  /// Another session logged in with the client's credentials.
  ///
  /// The server dropped the client.
  Kicked,
  /// The merged server tuning values.
  ///
  /// Raised after the login reply or any message that changes one.
  ServerInfo(ServerInfo),
  /// A message from the server's administrators to every user.
  GlobalMessage(String),
  /// A server message the client has no use for.
  ///
  /// It is one soulseek-proto does not know, or one that answers nothing the client sends
  /// itself, such as the reply to a request the application wrote with
  /// [`Client::write_server_message`](crate::Client::write_server_message).
  Unhandled {
    /// Code of the message.
    code: u32,
    /// Whole frame, length prefix included.
    ///
    /// It is in the form [`soulseek_proto::ServerResponse::decode`] takes.
    frame: std::sync::Arc<[u8]>,
  },
  /// Phrases the client's search responses must not contain.
  ///
  /// The client sends each answer as the handler gives it, so the application leaves
  /// the phrases out.
  ExcludedSearchPhrases(Vec<String>),
  /// A user was granted privileges.
  ///
  /// `id` is set when the server wants the notification acknowledged, with
  /// [`Account::acknowledge_privilege_notification`](crate::account::Account::acknowledge_privilege_notification).
  PrivilegeNotification {
    /// User who was granted privileges.
    username: String,
    /// Id of the notification, when the server wants it acknowledged.
    id: Option<u32>,
  },
  /// Every privileged user, pushed after login.
  PrivilegedUsers(Vec<String>),
}
