//! Chat rooms: what happens in the rooms the client is in.

use std::sync::Arc;

use soulseek_proto::server::{RoomList, RoomTicker, UserData};

/// Something that happened in a chat room, or to the client's place in one.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Event {
  /// The rooms on the server, pushed after login or asked for.
  List(Arc<RoomList>),
  /// A user, the client's own included, joined a room the client is in.
  Joined {
    /// Name of the room.
    room: String,
    /// User who joined.
    username: String,
    /// What the server says of the user.
    user_data: UserData,
  },
  /// The server put the client in a room, with the room as it was then.
  ///
  /// `asked` is whether a [`Rooms::join`] call was still waiting for it; one that came after
  /// the call gave up leaves the client in the room all the same.
  Entered {
    /// Room and its members, as [`Rooms::join`] returns them.
    room: Arc<soulseek_proto::server::RoomData>,
    /// Whether a join was still waiting for it.
    asked: bool,
  },
  /// A user left a room the client is in.
  ///
  /// Raised for the client's own user when the client leaves, which the server does not
  /// announce.
  Left {
    /// Name of the room.
    room: String,
    /// User who left.
    username: String,
  },
  /// A chat line in a room the client is in, the client's own included.
  Message {
    /// Name of the room.
    room: String,
    /// User who said the line.
    username: String,
    /// What the user said.
    message: String,
  },
  /// A line from any public room, while the public chat feed is on.
  PublicChat {
    /// Name of the room.
    room: String,
    /// User who said the line.
    username: String,
    /// What the user said.
    message: String,
  },
  /// Every ticker of a room, pushed when the client joins it.
  Tickers {
    /// Name of the room.
    room: String,
    /// Every ticker set in the room.
    tickers: Vec<RoomTicker>,
  },
  /// A user set their ticker in a room the client is in.
  TickerAdded {
    /// Name of the room.
    room: String,
    /// Ticker and who set it.
    ticker: RoomTicker,
  },
  /// A user cleared their ticker in a room the client is in.
  TickerRemoved {
    /// Name of the room.
    room: String,
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
    room: String,
    /// Member added.
    username: String,
  },
  /// A user was removed from a private room the client is in.
  MemberRemoved {
    /// Name of the room.
    room: String,
    /// Member removed.
    username: String,
  },
  /// A user was made an operator of a private room the client is in.
  OperatorAdded {
    /// Name of the room.
    room: String,
    /// Operator added.
    username: String,
  },
  /// A user stopped being an operator of a private room the client is in.
  OperatorRemoved {
    /// Name of the room.
    room: String,
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Members {
  /// Name of the room.
  pub room: String,
  /// Members of the room, or its operators.
  pub users: Vec<String>,
}

/// The chat rooms of a client.
///
/// Made by [`Client::rooms`](crate::Client::rooms).
#[derive(Clone, Copy, Debug)]
pub struct Rooms<'a> {
  pub(crate) client: &'a crate::Client,
}

#[cfg(all(test, feature = "serde"))]
mod tests {
  use serde_json::{from_value, json, to_value};

  use super::*;

  #[test]
  fn a_room_event_round_trips_under_its_variant_name() {
    let event = Event::Message {
      room: "lobby".into(),
      username: "bob".into(),
      message: "hello".into(),
    };
    let json = to_value(&event).unwrap();
    assert_eq!(
      json,
      json!({ "Message": { "room": "lobby", "username": "bob", "message": "hello" } })
    );
    assert!(matches!(
      from_value(json).unwrap(),
      Event::Message { room, username, message }
        if room == "lobby" && username == "bob" && message == "hello"
    ));

    let members = Event::Members(Members {
      room: "lobby".into(),
      users: vec!["alice".into()],
    });
    let json = to_value(&members).unwrap();
    assert_eq!(
      json,
      json!({ "Members": { "room": "lobby", "users": ["alice"] } })
    );
    assert!(matches!(
      from_value(json).unwrap(),
      Event::Members(members) if members.users == ["alice"]
    ));
  }
}
