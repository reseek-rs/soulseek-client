//! What the client tells the application: every state change, every pushed server message the
//! application acts on, and how its transfers, searches and browses go. Events go out on one
//! broadcast channel; a subscriber that falls more than the `event_capacity` option behind gets
//! `RecvError::Lagged(n)` from its receiver and continues with the newest events.

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

/// One thing that happened in the client
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// the client moved from one state to another
  StateChanged {
    /// the state left
    previous_state: ClientState,
    /// the state entered
    state: ClientState,
    /// why it changed
    message: String,
    /// the error that moved it, if one did
    error: Option<ClientError>,
  },
  /// the TCP connection of server session `session` is up
  Connected {
    /// the session, counted from 1 for the client's first
    session: u64,
  },
  /// the server accepted the login of server session `session` as `username`
  LoggedIn {
    /// the session
    session: u64,
    /// the name we logged in as
    username: String,
  },
  /// server session `session` ended at `at`; raised once per session, after its last
  /// `StateChanged`
  Disconnected {
    /// the session that ended
    session: u64,
    /// why it ended
    message: String,
    /// the error that ended it, if one did
    error: Option<ClientError>,
    /// when it ended
    at: SystemTime,
  },
  /// another session logged in with our credentials and the server dropped us
  KickedFromServer,
  /// the merged server tuning values, after the login reply or any message that changes one
  ServerInfoReceived(ServerInfo),
  /// a message from the server's administrators to every user
  GlobalMessageReceived(String),
  /// phrases our search responses must not contain; the client sends each answer as the search
  /// resolver gives it, so the application leaves them out
  ExcludedSearchPhrasesReceived(Vec<String>),
  /// a private message to us, live or replayed at login
  PrivateMessageReceived(PrivateMessage),
  /// a user was granted privileges; `id` is set when the server wants it acknowledged, with
  /// [`crate::Client::acknowledge_privilege_notification`]
  PrivilegeNotificationReceived {
    /// who was granted them
    username: String,
    /// the notification's id, when the server wants it acknowledged
    id: Option<u32>,
  },
  /// every privileged user, pushed after login
  PrivilegedUserListReceived(Vec<String>),
  /// a user's presence, asked for or pushed for a watched user or a room member; `asked` when
  /// a request of ours for it was still waiting and took it as its answer
  UserStatusChanged {
    /// the user and their presence
    status: UserStatus,
    /// a request of ours for it was waiting and took it as its answer
    asked: bool,
  },
  /// a user's statistics, asked for or pushed for a member of a room we are in; `asked` when a
  /// request of ours for them was still waiting and took them as its answer
  UserStatisticsChanged {
    /// the user and their figures
    statistics: UserStatistics,
    /// a request of ours for them was waiting and took them as its answer
    asked: bool,
  },
  /// the server reports that a user could not connect to us for a connection request we made;
  /// the server usually sends only the token, and then this is not raised
  UserCannotConnect {
    /// the connection request's token
    token: u32,
    /// the user who could not connect
    username: String,
  },
  /// the rooms on the server, pushed after login or asked for
  RoomListReceived(Arc<RoomList>),
  /// a user, ourselves included, joined a room we are in
  RoomJoined {
    /// the room
    room_name: String,
    /// who joined
    username: String,
    /// what the server says of them
    user_data: UserData,
  },
  /// a user left a room we are in; raised for ourselves when we leave, which the server does
  /// not announce
  RoomLeft {
    /// the room
    room_name: String,
    /// who left
    username: String,
  },
  /// a chat line in a room we are in, our own included
  RoomMessageReceived {
    /// the room
    room_name: String,
    /// who said it
    username: String,
    /// what they said
    message: String,
  },
  /// a line from any public room, while the public chat feed is on
  PublicChatMessageReceived {
    /// the room
    room_name: String,
    /// who said it
    username: String,
    /// what they said
    message: String,
  },
  /// every ticker of a room, pushed when we join it
  RoomTickerListReceived {
    /// the room
    room_name: String,
    /// every ticker set in it
    tickers: Vec<RoomTicker>,
  },
  /// a user set their ticker in a room we are in
  RoomTickerAdded {
    /// the room
    room_name: String,
    /// the ticker and who set it
    ticker: RoomTicker,
  },
  /// a user cleared their ticker in a room we are in
  RoomTickerRemoved {
    /// the room
    room_name: String,
    /// whose ticker it was
    username: String,
  },
  /// we were added to a private room
  PrivateRoomMembershipAdded(String),
  /// we were removed from a private room
  PrivateRoomMembershipRemoved(String),
  /// we were made an operator of a private room
  PrivateRoomModerationAdded(String),
  /// we stopped being an operator of a private room
  PrivateRoomModerationRemoved(String),
  /// someone was added to a private room we are in
  PrivateRoomUserAdded {
    /// the room
    room_name: String,
    /// the member added
    username: String,
  },
  /// someone was removed from a private room we are in
  PrivateRoomUserRemoved {
    /// the room
    room_name: String,
    /// the member removed
    username: String,
  },
  /// someone was made an operator of a private room we are in
  PrivateRoomOperatorAdded {
    /// the room
    room_name: String,
    /// the operator added
    username: String,
  },
  /// someone stopped being an operator of a private room we are in
  PrivateRoomOperatorRemoved {
    /// the room
    room_name: String,
    /// the operator removed
    username: String,
  },
  /// the members of a private room we are in, without its owner
  PrivateRoomUserListReceived(RoomMembers),
  /// the operators of a private room we are in
  PrivateRoomModeratedUserListReceived(RoomMembers),
  /// our place in the distributed search tree changed
  DistributedNetworkStateChanged(DistributedNetworkInfo),
  /// the server told every client to rebuild the distributed search tree
  DistributedNetworkReset,
  /// we took a parent in the distributed search tree
  DistributedParentAdopted(DistributedParent),
  /// our distributed parent's connection closed
  DistributedParentDisconnected(DistributedParent),
  /// a user joined the distributed search tree below us
  DistributedChildAdded {
    /// the child
    username: String,
    /// its address
    ip_endpoint: SocketAddr,
  },
  /// a distributed child's connection closed
  DistributedChildDisconnected {
    /// the child
    username: String,
    /// its address
    ip_endpoint: SocketAddr,
  },
  /// the server made us the root of a branch of the distributed search tree
  PromotedToDistributedBranchRoot,
  /// we stopped being a branch root
  DemotedFromDistributedBranchRoot,
  /// we sent our results for another user's search
  SearchResponseDelivered(SearchDelivery),
  /// results we kept for an unreachable searcher were given up
  SearchResponseDeliveryFailed(SearchDelivery),
  /// a peer refused to queue a file we asked for
  DownloadDenied {
    /// the uploader
    username: String,
    /// the file
    filename: RemoteFilename,
    /// why, as the uploader sent it
    message: String,
  },
  /// a peer reported that the upload of a file to us failed
  DownloadFailed {
    /// the uploader
    username: String,
    /// the file
    filename: RemoteFilename,
  },
  /// a peer told us our place in its queue for a file without our asking, as it does once it
  /// queued the file
  PlaceInQueueReceived {
    /// the uploader
    username: String,
    /// the file
    filename: RemoteFilename,
    /// its place, counted from 1; read it with [`peer_count`](crate::peer_count)
    place: u32,
  },
  /// a download or upload moved from one state to another, or a new one reports the state it
  /// starts in, with no previous state
  TransferStateChanged {
    /// the state left; none for a new transfer
    previous_state: Option<TransferState>,
    /// the transfer as it is now
    transfer: Transfer,
  },
  /// bytes of a download or upload started without a progress callback moved; raised for every
  /// chunk, so a consumer that shows it should pace itself
  TransferProgressUpdated {
    /// the position before
    previous_bytes_transferred: u64,
    /// the transfer as it is now
    transfer: Transfer,
  },
  /// one of our searches moved from one state to another
  SearchStateChanged {
    /// the state left
    previous_state: SearchState,
    /// the search as it is now
    search: Search,
  },
  /// one of our searches took a response; it may come just after the search completed
  SearchResponseReceived {
    /// the search as it is now
    search: Search,
    /// the response as it was taken
    response: Arc<SearchResponse>,
  },
  /// more of a browse response arrived, for a browse started without a progress callback
  BrowseProgressUpdated(BrowseProgress),
}

/// A private message to us
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivateMessage {
  /// acknowledge it with this so the server stops replaying it at login
  pub id: u32,
  /// when the server says it was sent
  pub timestamp: SystemTime,
  /// who sent it
  pub username: String,
  /// what they wrote
  pub message: String,
  /// the server is replaying it from its store of unacknowledged messages
  pub replayed: bool,
}

/// The users of a private room, as the room membership messages list them
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomMembers {
  /// the room
  pub room_name: String,
  /// its members, or its operators
  pub users: Vec<String>,
}

/// A distributed parent we adopted or lost
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DistributedParent {
  /// the user
  pub username: String,
  /// its address
  pub ip_endpoint: SocketAddr,
  /// its depth in the tree, 0 for a branch root
  pub branch_level: u32,
  /// the branch root above it
  pub branch_root: String,
  /// it is a branch root itself
  pub is_branch_root: bool,
}

/// Our answer to another user's search and whom it was for
#[derive(Clone, Debug)]
pub struct SearchDelivery {
  /// who searched
  pub username: String,
  /// the searcher's token
  pub token: u32,
  /// the query as the searcher sent it
  pub query: String,
  /// what we sent
  pub search_response: Arc<SearchReply>,
}
