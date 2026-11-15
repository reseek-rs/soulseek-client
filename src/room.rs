//! Chat rooms: what happens in the rooms the client is in.

use std::sync::Arc;

use soulseek_proto::server::{RoomList, RoomTicker, UserData};

/// Something that happened in a chat room, or to the client's place in one.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// The rooms on the server, pushed after login or asked for.
  List(Arc<RoomList>),
  /// A user, the client's own included, joined a room the client is in.
  Joined {
    /// Name of the room.
    room_name: String,
    /// User who joined.
    username: String,
    /// What the server says of the user.
    user_data: UserData,
  },
  /// A user left a room the client is in.
  ///
  /// Raised for the client's own user when the client leaves, which the server does not
  /// announce.
  Left {
    /// Name of the room.
    room_name: String,
    /// User who left.
    username: String,
  },
  /// A chat line in a room the client is in, the client's own included.
  Message {
    /// Name of the room.
    room_name: String,
    /// User who said the line.
    username: String,
    /// What the user said.
    message: String,
  },
  /// A line from any public room, while the public chat feed is on.
  PublicChat {
    /// Name of the room.
    room_name: String,
    /// User who said the line.
    username: String,
    /// What the user said.
    message: String,
  },
  /// Every ticker of a room, pushed when the client joins it.
  Tickers {
    /// Name of the room.
    room_name: String,
    /// Every ticker set in the room.
    tickers: Vec<RoomTicker>,
  },
  /// A user set their ticker in a room the client is in.
  TickerAdded {
    /// Name of the room.
    room_name: String,
    /// Ticker and who set it.
    ticker: RoomTicker,
  },
  /// A user cleared their ticker in a room the client is in.
  TickerRemoved {
    /// Name of the room.
    room_name: String,
    /// User whose ticker it was.
    username: String,
  },
  /// The client was added to a private room.
  MembershipAdded(String),
  /// The client was removed from a private room.
  MembershipRemoved(String),
  /// The client was made an operator of a private room.
  OperatorshipAdded(String),
  /// The client stopped being an operator of a private room.
  OperatorshipRemoved(String),
  /// A user was added to a private room the client is in.
  MemberAdded {
    /// Name of the room.
    room_name: String,
    /// Member added.
    username: String,
  },
  /// A user was removed from a private room the client is in.
  MemberRemoved {
    /// Name of the room.
    room_name: String,
    /// Member removed.
    username: String,
  },
  /// A user was made an operator of a private room the client is in.
  OperatorAdded {
    /// Name of the room.
    room_name: String,
    /// Operator added.
    username: String,
  },
  /// A user stopped being an operator of a private room the client is in.
  OperatorRemoved {
    /// Name of the room.
    room_name: String,
    /// Operator removed.
    username: String,
  },
  /// The members of a private room the client is in, without its owner.
  Members(Members),
  /// The operators of a private room the client is in.
  Operators(Members),
}

/// Users of a private room, as the room membership messages list them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Members {
  /// Name of the room.
  pub room_name: String,
  /// Members of the room, or its operators.
  pub users: Vec<String>,
}
