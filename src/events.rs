//! What the client tells the application: every state change, every pushed server message the
//! application acts on, and how its transfers, searches and browses go. Events go out on one
//! broadcast channel; a subscriber that falls more than the `event_capacity` option behind gets
//! `RecvError::Lagged(n)` from its receiver and goes on from the oldest event still held.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use soulseek_proto::RemoteFilename;
use soulseek_proto::peer::SearchResponse;
use soulseek_proto::server::{RoomList, RoomTicker, UserData, UserStatistics, UserStatus};

use crate::browse::BrowseProgress;
use crate::error::ClientError;
use crate::resolvers::SearchReply;
use crate::search::{Search, SearchState};
use crate::state::{ClientState, DistributedNetworkInfo, ServerInfo};
use crate::transfers::{Transfer, TransferState};

/// One thing that happened in the client.
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
    error: Option<ClientError>,
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
  /// Raised once per session, after its last [`Event::StateChanged`].
  Disconnected {
    /// Session that ended.
    session: u64,
    /// Why the session ended.
    message: String,
    /// Error that ended the session, if one did.
    error: Option<ClientError>,
    /// When the session ended.
    at: SystemTime,
  },
  /// Another session logged in with the client's credentials.
  ///
  /// The server dropped the client.
  KickedFromServer,
  /// The merged server tuning values.
  ///
  /// Raised after the login reply or any message that changes one.
  ServerInfoReceived(ServerInfo),
  /// A message from the server's administrators to every user.
  GlobalMessageReceived(String),
  /// A server message the client has no use for.
  ///
  /// It is one soulseek-proto does not know, or one that answers nothing the client sends
  /// itself, such as the reply to a request the application wrote with
  /// [`Client::write_server_message`](crate::Client::write_server_message).
  ServerMessageUnhandled {
    /// Code of the message.
    code: u32,
    /// Whole frame, length prefix included.
    ///
    /// It is in the form [`soulseek_proto::ServerResponse::decode`] takes.
    frame: Arc<[u8]>,
  },
  /// Phrases the client's search responses must not contain.
  ///
  /// The client sends each answer as the search resolver gives it, so the application leaves
  /// the phrases out.
  ExcludedSearchPhrasesReceived(Vec<String>),
  /// A private message to the client, live or replayed at login.
  PrivateMessageReceived(PrivateMessage),
  /// A user was granted privileges.
  ///
  /// `id` is set when the server wants the notification acknowledged, with
  /// [`crate::Client::acknowledge_privilege_notification`].
  PrivilegeNotificationReceived {
    /// User who was granted privileges.
    username: String,
    /// Id of the notification, when the server wants it acknowledged.
    id: Option<u32>,
  },
  /// Every privileged user, pushed after login.
  PrivilegedUserListReceived(Vec<String>),
  /// A user's presence, asked for or pushed.
  ///
  /// It is pushed for a watched user or a room member. `asked` is set when a request of the
  /// client's for it was still waiting and took it as its answer.
  UserStatusChanged {
    /// User and their presence.
    status: UserStatus,
    /// Whether a request of the client's for it was waiting and took it as its answer.
    asked: bool,
  },
  /// A user's statistics, asked for or pushed.
  ///
  /// They are pushed for a member of a room the client is in. `asked` is set when a request of
  /// the client's for them was still waiting and took them as its answer.
  UserStatisticsChanged {
    /// User and their figures.
    statistics: UserStatistics,
    /// Whether a request of the client's for them was waiting and took them as its answer.
    asked: bool,
  },
  /// The server reports that a user could not connect to the client.
  ///
  /// The report is for a connection request the client made. The server usually sends only the
  /// token, and then this event is not raised.
  UserCannotConnect {
    /// Token of the connection request.
    token: u32,
    /// User who could not connect.
    username: String,
  },
  /// The rooms on the server, pushed after login or asked for.
  RoomListReceived(Arc<RoomList>),
  /// A user, the client's own included, joined a room the client is in.
  RoomJoined {
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
  RoomLeft {
    /// Name of the room.
    room_name: String,
    /// User who left.
    username: String,
  },
  /// A chat line in a room the client is in, the client's own included.
  RoomMessageReceived {
    /// Name of the room.
    room_name: String,
    /// User who said the line.
    username: String,
    /// What the user said.
    message: String,
  },
  /// A line from any public room, while the public chat feed is on.
  PublicChatMessageReceived {
    /// Name of the room.
    room_name: String,
    /// User who said the line.
    username: String,
    /// What the user said.
    message: String,
  },
  /// Every ticker of a room, pushed when the client joins it.
  RoomTickerListReceived {
    /// Name of the room.
    room_name: String,
    /// Every ticker set in the room.
    tickers: Vec<RoomTicker>,
  },
  /// A user set their ticker in a room the client is in.
  RoomTickerAdded {
    /// Name of the room.
    room_name: String,
    /// Ticker and who set it.
    ticker: RoomTicker,
  },
  /// A user cleared their ticker in a room the client is in.
  RoomTickerRemoved {
    /// Name of the room.
    room_name: String,
    /// User whose ticker it was.
    username: String,
  },
  /// The client was added to a private room.
  PrivateRoomMembershipAdded(String),
  /// The client was removed from a private room.
  PrivateRoomMembershipRemoved(String),
  /// The client was made an operator of a private room.
  PrivateRoomOperatorshipAdded(String),
  /// The client stopped being an operator of a private room.
  PrivateRoomOperatorshipRemoved(String),
  /// A user was added to a private room the client is in.
  PrivateRoomUserAdded {
    /// Name of the room.
    room_name: String,
    /// Member added.
    username: String,
  },
  /// A user was removed from a private room the client is in.
  PrivateRoomUserRemoved {
    /// Name of the room.
    room_name: String,
    /// Member removed.
    username: String,
  },
  /// A user was made an operator of a private room the client is in.
  PrivateRoomOperatorAdded {
    /// Name of the room.
    room_name: String,
    /// Operator added.
    username: String,
  },
  /// A user stopped being an operator of a private room the client is in.
  PrivateRoomOperatorRemoved {
    /// Name of the room.
    room_name: String,
    /// Operator removed.
    username: String,
  },
  /// The members of a private room the client is in, without its owner.
  PrivateRoomUserListReceived(RoomMembers),
  /// The operators of a private room the client is in.
  PrivateRoomOperatorListReceived(RoomMembers),
  /// The client's place in the distributed search tree changed.
  DistributedNetworkStateChanged(DistributedNetworkInfo),
  /// The server told every client to rebuild the distributed search tree.
  DistributedNetworkReset,
  /// The client took a parent in the distributed search tree.
  DistributedParentAdopted(DistributedParent),
  /// The connection of the client's distributed parent closed.
  DistributedParentDisconnected(DistributedParent),
  /// A user joined the distributed search tree below the client.
  DistributedChildAdded {
    /// Username of the child.
    username: String,
    /// Address of the child.
    endpoint: SocketAddr,
  },
  /// A distributed child's connection closed.
  DistributedChildDisconnected {
    /// Username of the child.
    username: String,
    /// Address of the child.
    endpoint: SocketAddr,
  },
  /// The server made the client the root of a branch of the distributed search tree.
  PromotedToDistributedBranchRoot,
  /// The client stopped being a branch root.
  DemotedFromDistributedBranchRoot,
  /// The client sent its results for another user's search.
  SearchResponseDelivered(SearchDelivery),
  /// Results the client kept for an unreachable searcher were given up.
  SearchResponseDeliveryFailed(SearchDelivery),
  /// A peer refused to queue a file the client asked for.
  DownloadDenied {
    /// Username of the uploader.
    username: String,
    /// File the peer refused.
    filename: RemoteFilename,
    /// Why, as the uploader sent it.
    message: String,
  },
  /// A peer reported that the upload of a file to the client failed.
  DownloadFailed {
    /// Username of the uploader.
    username: String,
    /// File whose upload failed.
    filename: RemoteFilename,
  },
  /// A peer told the client its place in the peer's queue for a file, unasked.
  ///
  /// A peer does so once it queued the file.
  PlaceInQueueReceived {
    /// Username of the uploader.
    username: String,
    /// File the place is for.
    filename: RemoteFilename,
    /// Place of the file, counted from 1.
    ///
    /// It is read with [`peer_count`](crate::peer_count).
    place: u32,
  },
  /// A download or upload moved from one state to another.
  ///
  /// A new transfer also reports the state it starts in, with no previous state.
  TransferStateChanged {
    /// State left, or `None` for a new transfer.
    previous_state: Option<TransferState>,
    /// Transfer as it is now.
    transfer: Transfer,
  },
  /// Bytes of a download or upload moved.
  ///
  /// Raised for a transfer started without a progress callback. It is raised for every chunk,
  /// so a consumer that shows it should pace itself.
  TransferProgressUpdated {
    /// Position before the bytes moved.
    previous_bytes_transferred: u64,
    /// Transfer as it is now.
    transfer: Transfer,
  },
  /// One of the client's searches moved from one state to another.
  SearchStateChanged {
    /// State left.
    previous_state: SearchState,
    /// Search as it is now.
    search: Search,
  },
  /// One of the client's searches took a response.
  ///
  /// The event may come just after the search completed.
  SearchResponseReceived {
    /// Search as it is now.
    search: Search,
    /// Response as it was taken.
    response: Arc<SearchResponse>,
  },
  /// More of a browse response arrived.
  ///
  /// Raised for a browse started without a progress callback.
  BrowseProgressUpdated(BrowseProgress),
}

/// Private message to the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivateMessage {
  /// Id to acknowledge the message with.
  ///
  /// Acknowledging the message stops the server replaying it at login.
  pub id: u32,
  /// When the server says the message was sent.
  pub timestamp: SystemTime,
  /// User who sent the message.
  pub username: String,
  /// What the user wrote.
  pub message: String,
  /// Whether the server is replaying it from its store of unacknowledged messages.
  pub replayed: bool,
}

/// Users of a private room, as the room membership messages list them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomMembers {
  /// Name of the room.
  pub room_name: String,
  /// Members of the room, or its operators.
  pub users: Vec<String>,
}

/// Distributed parent the client adopted or lost, or a candidate for one.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DistributedParent {
  /// Username of the parent.
  pub username: String,
  /// Address of the parent.
  pub endpoint: SocketAddr,
  /// Depth of the parent in the tree, 0 for a branch root.
  pub branch_level: u32,
  /// Branch root above the parent.
  pub branch_root: String,
  /// Whether the parent is a branch root itself.
  pub is_branch_root: bool,
}

/// The client's answer to another user's search, and whom it was for.
#[derive(Clone, Debug)]
pub struct SearchDelivery {
  /// User who searched.
  pub username: String,
  /// Token of the searcher.
  pub token: u32,
  /// Query as the searcher sent it.
  pub query: String,
  /// What the client sent.
  pub search_response: Arc<SearchReply>,
}
