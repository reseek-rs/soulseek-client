//! Other users: what the server tells the client about them.

use soulseek_proto::server::{UserStatistics, UserStatus};

/// Something the server told the client about a user.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// A user's presence, asked for or pushed.
  ///
  /// It is pushed for a watched user or a room member. `asked` is set when a request of the
  /// client's for it was still waiting and took it as its answer.
  StatusChanged {
    /// User and their presence.
    status: UserStatus,
    /// Whether a request of the client's for it was waiting and took it as its answer.
    asked: bool,
  },
  /// A user's statistics, asked for or pushed.
  ///
  /// They are pushed for a member of a room the client is in. `asked` is set when a request of
  /// the client's for them was still waiting and took them as its answer.
  StatisticsChanged {
    /// User and their figures.
    statistics: UserStatistics,
    /// Whether a request of the client's for them was waiting and took them as its answer.
    asked: bool,
  },
  /// The server reports that a user could not connect to the client.
  ///
  /// The report is for a connection request the client made. The server usually sends only the
  /// token, and then this event is not raised.
  CannotConnect {
    /// Token of the connection request.
    token: u32,
    /// User who could not connect.
    username: String,
  },
}

/// What a client knows of and asks about other users.
///
/// Made by [`Client::users`](crate::Client::users).
#[derive(Clone, Copy, Debug)]
pub struct Users<'a> {
  pub(crate) client: &'a crate::Client,
}
