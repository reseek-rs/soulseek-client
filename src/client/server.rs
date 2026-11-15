//! What the client does with each message the server sends: complete the wait of the request it
//! answers, raise the event it carries, or pass it to the part of the client that owns it (peer
//! connections, the distributed tree, the search responder). Frames are handled in order on the
//! server connection's read loop and nothing here blocks; whatever needs a connection or a
//! lookup runs on a task of its own or goes to the tree task.
//!
//! The waits each reply completes, and the value it completes them with:
//!
//! | reply | key | value |
//! |---|---|---|
//! | `Login` | `Server(Login)` | `LoginResponse` |
//! | `GetPeerAddress` | `ServerUser` | `GetPeerAddressResponse` |
//! | `WatchUser` | `ServerUser` | `WatchUserResponse` |
//! | `GetStatus` | `ServerUser` | `UserStatus` |
//! | `JoinRoom` | `ServerRoom` | `RoomData` (failed by `CannotJoinRoom`) |
//! | `LeaveRoom` | `ServerRoom` | `()` |
//! | `Ping` | `Server(Ping)` | `()` |
//! | `GetUserStats` | `ServerUser` | `UserStatistics` |
//! | `GetPersonalRecommendations`, `GetGlobalRecommendations` | `Server` | `Recommendations` |
//! | `GetUserInterests` | `ServerUser` | `UserInterests` |
//! | `RoomList` | `Server(RoomList)` | `Arc<RoomList>` |
//! | `CheckPrivileges` | `Server(CheckPrivileges)` | `u32`, seconds |
//! | `GetPersonalSimilarUsers` | `Server` | `GetPersonalSimilarUsersResponse` |
//! | `GetInterestRecommendations` | `ServerInterest` | `GetInterestRecommendationsResponse` |
//! | `GetInterestSimilarUsers` | `ServerInterest` | `GetInterestSimilarUsersResponse` |
//! | `UserPrivileges` | `ServerUser` | `bool` |
//! | `PrivateRoomAddUser`, `PrivateRoomRemoveUser` | `ServerRoomUser` | `()` |
//! | `PrivateRoomAddOperator`, `PrivateRoomRemoveOperator` | `ServerRoomUser` | `()` |
//! | `PrivateRoomRemoved`, `PrivateRoomOperatorRemoved` | `ServerRoom` | `()` |
//! | `PrivateRoomToggle` | `Server(PrivateRoomToggle)` | `bool` |
//! | `NewPassword` | `Server(NewPassword)` | `Secret`, the password echoed back |

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use soulseek_proto::server::{CannotConnectResponse, ConnectToPeerResponse, LoginResponse};
use soulseek_proto::{DecodeError, PeerConnectionType, ServerCode, ServerResponse};

use super::Client;
use super::disconnect::Cause;
use super::request::{interest_key, room_key, room_user_key, user_key};
use super::session::{Edge, Stage};
use crate::distributed::Command;
use crate::error::Error;
use crate::events::{Event, PrivateMessage};
use crate::handler::Peer;
use crate::log::LogLimiter;
use crate::peers::request_endpoint;
use crate::waiter::WaitKey;

/// a server message that does not decode, which can come with every message of its kind
static UNREADABLE: LogLimiter = LogLimiter::new();

/// codes the server no longer sends; seeing one is worth telling apart from a new code
const OBSOLETE: [ServerCode; 13] = [
  ServerCode::IgnoreUser,
  ServerCode::UnignoreUser,
  ServerCode::FileSearchRoom,
  ServerCode::SendConnectToken,
  ServerCode::SimilarRecommendations,
  ServerCode::MyRecommendations,
  ServerCode::AdminCommand,
  ServerCode::PlaceInLineRequest,
  ServerCode::PlaceInLineResponse,
  ServerCode::RoomAdded,
  ServerCode::RoomRemoved,
  ServerCode::GlobalUserList,
  ServerCode::TunneledMessage,
];

impl Client {
  /// Handles one complete frame from the server. A frame that does not decode is dropped and
  /// the connection stays up.
  pub(super) fn dispatch_server_frame(&self, frame: &[u8]) {
    let message = match ServerResponse::decode(frame) {
      Ok(message) => message,
      Err(DecodeError::UnknownCode { code }) => return self.unhandled(code, frame),
      // the server is the only sender here, so a message the client cannot read is the client's
      // misreading of it, and what it said is lost
      Err(error) => {
        if let Some(suppressed) = UNREADABLE.allow() {
          tracing::warn!(%error, suppressed, "server message not read, dropped");
        }
        return;
      }
    };
    // the message is encoded again to see what was left unread, only when someone listens
    if tracing::enabled!(tracing::Level::TRACE) {
      let read = message.encode().len();
      if read < frame.len() {
        tracing::trace!(
          code = message.code().code(),
          name = message.code().name(),
          unread = frame.len() - read,
          "server message has bytes past its fields"
        );
      }
    }
    self.handle_server_message(message, frame);
  }

  fn handle_server_message(&self, message: ServerResponse, frame: &[u8]) {
    use ServerResponse as R;
    let inner = &self.inner;
    match message {
      R::Login(response) => self.login_replied(response),
      R::GetPeerAddress(response) => {
        self.complete(
          user_key(ServerCode::GetPeerAddress, &response.username),
          response,
        );
      }
      R::WatchUser(response) => {
        self.complete(
          user_key(ServerCode::WatchUser, &response.username),
          response,
        );
      }
      R::GetStatus(status) => {
        let asked = self.inner.waiter.complete(
          &user_key(ServerCode::GetStatus, &status.username),
          status.clone(),
        );
        self.emit(Event::User(crate::user::Event::StatusChanged {
          status,
          asked,
        }));
      }
      R::SayInChatRoom(said) => self.emit(Event::Room(crate::room::Event::Message {
        room_name: said.room,
        username: said.username,
        message: said.message,
      })),
      R::JoinRoom(room) => self.complete(room_key(ServerCode::JoinRoom, &room.name), room),
      R::LeaveRoom(left) => {
        self.complete(room_key(ServerCode::LeaveRoom, &left.room), ());
        // the server does not announce the client's own departure, so it is raised here
        match self.username() {
          Some(username) => self.emit(Event::Room(crate::room::Event::Left {
            room_name: left.room,
            username,
          })),
          None => tracing::debug!(room = left.room, "room left after the session ended"),
        }
      }
      R::UserJoinedRoom(joined) => self.emit(Event::Room(crate::room::Event::Joined {
        room_name: joined.room,
        username: joined.user.username.clone(),
        user_data: joined.user,
      })),
      R::UserLeftRoom(left) => self.emit(Event::Room(crate::room::Event::Left {
        room_name: left.room,
        username: left.username,
      })),
      R::ConnectToPeer(request) => self.connect_to_peer(request),
      R::PrivateMessage(message) => {
        self.emit(Event::PrivateMessage(PrivateMessage {
          id: message.id,
          timestamp: UNIX_EPOCH + Duration::from_secs(message.timestamp.into()),
          username: message.username,
          message: message.message,
          replayed: !message.is_new,
        }));
      }
      R::FileSearch(search) => {
        self.respond_to_search(search.username, search.token, search.query);
      }
      R::Ping => self.complete(WaitKey::Server(ServerCode::Ping), ()),
      R::GetUserStats(statistics) => {
        if self.username().as_deref() == Some(statistics.username.as_str()) {
          // the client's own speed decides how many distributed children it may take
          self.distributed_command(Command::OwnSpeed(statistics.stats.average_speed));
        }
        let asked = self.inner.waiter.complete(
          &user_key(ServerCode::GetUserStats, &statistics.username),
          statistics.clone(),
        );
        self.emit(Event::User(crate::user::Event::StatisticsChanged {
          statistics,
          asked,
        }));
      }
      R::KickedFromServer => {
        tracing::debug!("kicked from the server");
        self.emit(Event::Session(crate::session::Event::Kicked));
        if let Some(teardown) = inner.begin_disconnect(
          Some("kicked from server"),
          Some(Error::Kicked),
          Cause::Kicked,
        ) {
          drop(teardown.spawn(self));
        }
      }
      R::GetPersonalRecommendations(recommendations) => self.complete(
        WaitKey::Server(ServerCode::GetPersonalRecommendations),
        recommendations,
      ),
      R::GetGlobalRecommendations(recommendations) => self.complete(
        WaitKey::Server(ServerCode::GetGlobalRecommendations),
        recommendations,
      ),
      R::GetUserInterests(interests) => {
        self.complete(
          user_key(ServerCode::GetUserInterests, &interests.username),
          interests,
        );
      }
      R::RoomList(list) => {
        let list = Arc::new(list);
        self.complete(WaitKey::Server(ServerCode::RoomList), list.clone());
        self.emit(Event::Room(crate::room::Event::List(list)));
      }
      R::GlobalAdminMessage(message) => self.emit(Event::Session(
        crate::session::Event::GlobalMessage(message.message),
      )),
      R::PrivilegedUsers(users) => self.emit(Event::Session(
        crate::session::Event::PrivilegedUsers(users.usernames),
      )),
      R::ParentMinSpeed(speed) => {
        inner.merge_server_info(|info| info.parent_min_speed = Some(speed.speed));
        self.distributed_command(Command::Changed);
      }
      R::ParentSpeedRatio(ratio) => {
        inner.merge_server_info(|info| info.parent_speed_ratio = Some(ratio.ratio));
        self.distributed_command(Command::Changed);
      }
      R::WishlistInterval(interval) => {
        inner.merge_server_info(|info| {
          info.wishlist_interval = Some(Duration::from_secs(interval.seconds.into()))
        });
      }
      R::ParentInactivityTimeout(value) => {
        self.store_obsolete(ServerCode::ParentInactivityTimeout, |info| {
          info.parent_inactivity_timeout = Some(Duration::from_secs(value.seconds.into()));
        });
      }
      R::SearchInactivityTimeout(value) => {
        self.store_obsolete(ServerCode::SearchInactivityTimeout, |info| {
          info.search_inactivity_timeout = Some(Duration::from_secs(value.seconds.into()));
        });
      }
      R::MinimumParentsInCache(value) => {
        self.store_obsolete(ServerCode::MinimumParentsInCache, |info| {
          info.minimum_parents_in_cache = Some(value.count);
        });
      }
      R::DistributedAliveInterval(value) => {
        self.store_obsolete(ServerCode::DistributedAliveInterval, |info| {
          info.distributed_alive_interval = Some(Duration::from_secs(value.seconds.into()));
        });
      }
      R::AddPrivilegedUser(user) => self.emit(Event::Session(
        crate::session::Event::PrivilegeNotification {
          username: user.username,
          id: None,
        },
      )),
      R::CheckPrivileges(privileges) => self.complete(
        WaitKey::Server(ServerCode::CheckPrivileges),
        privileges.seconds_remaining,
      ),
      R::EmbeddedMessage(embedded) => self.handle_embedded(&embedded),
      R::NetInfo(candidates) => {
        self.distributed_command(Command::NetInfo(candidates.parents));
      }
      R::GetPersonalSimilarUsers(users) => {
        self.complete(WaitKey::Server(ServerCode::GetPersonalSimilarUsers), users);
      }
      R::GetInterestRecommendations(recommendations) => self.complete(
        interest_key(
          ServerCode::GetInterestRecommendations,
          &recommendations.interest,
        ),
        recommendations,
      ),
      R::GetInterestSimilarUsers(users) => self.complete(
        interest_key(ServerCode::GetInterestSimilarUsers, &users.interest),
        users,
      ),
      R::RoomTickers(tickers) => self.emit(Event::Room(crate::room::Event::Tickers {
        room_name: tickers.room,
        tickers: tickers.tickers,
      })),
      R::RoomTickerAdd(added) => self.emit(Event::Room(crate::room::Event::TickerAdded {
        room_name: added.room,
        ticker: added.ticker,
      })),
      R::RoomTickerRemove(removed) => self.emit(Event::Room(crate::room::Event::TickerRemoved {
        room_name: removed.room,
        username: removed.username,
      })),
      R::UserPrivileges(privileges) => self.complete(
        user_key(ServerCode::UserPrivileges, &privileges.username),
        privileges.privileged,
      ),
      R::NotifyPrivileges(notification) => self.emit(Event::Session(
        crate::session::Event::PrivilegeNotification {
          username: notification.username,
          id: Some(notification.id),
        },
      )),
      R::DistributedReset => {
        tracing::info!("the server reset the distributed network");
        self.emit(Event::Distributed(crate::distributed::Event::Reset));
        self.distributed_command(Command::Reset);
      }
      R::PrivateRoomUsers(members) => {
        self.emit(Event::Room(crate::room::Event::Members(
          crate::room::Members {
            room_name: members.room,
            users: members.usernames,
          },
        )));
      }
      R::PrivateRoomAddUser(added) => {
        self.complete(
          room_user_key(ServerCode::PrivateRoomAddUser, &added.room, &added.username),
          (),
        );
        self.emit(Event::Room(crate::room::Event::MemberAdded {
          room_name: added.room,
          username: added.username,
        }));
      }
      R::PrivateRoomRemoveUser(removed) => {
        self.complete(
          room_user_key(
            ServerCode::PrivateRoomRemoveUser,
            &removed.room,
            &removed.username,
          ),
          (),
        );
        self.emit(Event::Room(crate::room::Event::MemberRemoved {
          room_name: removed.room,
          username: removed.username,
        }));
      }
      R::PrivateRoomAdded(added) => {
        self.emit(Event::Room(crate::room::Event::MembershipAdded(added.room)))
      }
      R::PrivateRoomRemoved(removed) => {
        self.complete(room_key(ServerCode::PrivateRoomRemoved, &removed.room), ());
        self.emit(Event::Room(crate::room::Event::MembershipRemoved(
          removed.room,
        )));
      }
      R::PrivateRoomToggle(toggle) => self.complete(
        WaitKey::Server(ServerCode::PrivateRoomToggle),
        toggle.accept_invitations,
      ),
      R::NewPassword(echo) => {
        self.complete(WaitKey::Server(ServerCode::NewPassword), echo.new_password);
      }
      R::PrivateRoomAddOperator(added) => {
        self.complete(
          room_user_key(
            ServerCode::PrivateRoomAddOperator,
            &added.room,
            &added.username,
          ),
          (),
        );
        self.emit(Event::Room(crate::room::Event::OperatorAdded {
          room_name: added.room,
          username: added.username,
        }));
      }
      R::PrivateRoomRemoveOperator(removed) => {
        self.complete(
          room_user_key(
            ServerCode::PrivateRoomRemoveOperator,
            &removed.room,
            &removed.username,
          ),
          (),
        );
        self.emit(Event::Room(crate::room::Event::OperatorRemoved {
          room_name: removed.room,
          username: removed.username,
        }));
      }
      R::PrivateRoomOperatorAdded(added) => {
        self.emit(Event::Room(crate::room::Event::OperatorshipAdded(
          added.room,
        )));
      }
      R::PrivateRoomOperatorRemoved(removed) => {
        self.complete(
          room_key(ServerCode::PrivateRoomOperatorRemoved, &removed.room),
          (),
        );
        self.emit(Event::Room(crate::room::Event::OperatorshipRemoved(
          removed.room,
        )));
      }
      R::PrivateRoomOwned(operators) => {
        self.emit(Event::Room(crate::room::Event::Operators(
          crate::room::Members {
            room_name: operators.room,
            users: operators.operators,
          },
        )));
      }
      R::PublicChat(said) => self.emit(Event::Room(crate::room::Event::PublicChat {
        room_name: said.room,
        username: said.username,
        message: said.message,
      })),
      R::ExcludedSearchPhrases(phrases) => {
        self.emit(Event::Session(
          crate::session::Event::ExcludedSearchPhrases(phrases.phrases),
        ));
      }
      R::CannotConnect(failure) => self.cannot_connect(&failure),
      R::CannotJoinRoom(refused) => {
        let faulted = inner.waiter.fault(
          &room_key(ServerCode::JoinRoom, &refused.room),
          Error::Room {
            kind: crate::error::RoomKind::JoinForbidden,
            message: "a private room with this name already exists".into(),
          },
        );
        if !faulted {
          tracing::debug!(room = refused.room, "refused join nobody waits for");
        }
      }
      R::IgnoreUser(_)
      | R::UnignoreUser(_)
      | R::SendConnectToken(_)
      | R::QueuedDownloads(_)
      | R::SimilarRecommendations(_)
      | R::MyRecommendations(_)
      | R::PlaceInLineRequest(_)
      | R::PlaceInLineResponse(_)
      | R::RoomAdded(_)
      | R::RoomRemoved(_)
      | R::ExactFileSearch(_)
      | R::GlobalUserList(_)
      | R::TunneledMessage(_)
      | R::PrivateRoomUnknown(_)
      | R::RelatedSearch(_)
      | R::AcknowledgeNotifyPrivileges(_) => self.unhandled(message.code().code(), frame),
      // a message a later soulseek-proto decodes
      _ => self.unhandled(message.code().code(), frame),
    }
  }

  /// A successful login takes effect as its reply is read, before the next frame is handled, so
  /// whatever the server sends right after it is raised after `LoggedIn`. The login in progress
  /// then takes the reply from its wait.
  fn login_replied(&self, response: LoginResponse) {
    if let LoginResponse::Success {
      ip, is_supporter, ..
    } = &response
    {
      let mut session = self.inner.session();
      if matches!(session.stage, Stage::LoggingIn { .. }) {
        self.inner.merge_server_info(|info| {
          info.is_supporter = Some(*is_supporter);
          info.own_public_ip = Some(*ip);
        });
      }
      match self
        .inner
        .transition(&mut session, Edge::Accept, "logged in".into(), None)
      {
        Ok(_) => {
          if let Some(Some(listener)) = session.stage.listener() {
            listener.logged_in();
          }
        }
        Err(refused) => {
          tracing::debug!(state = %refused, "a login reply outside a login is not taken");
        }
      }
    }
    self.complete(WaitKey::Server(ServerCode::Login), response);
  }

  /// A peer asks, through the server, that the client connect to it: for messages, for a file
  /// it is about to upload to the client, or to become the client's distributed child. Each is
  /// handled away from the read loop, and a request the client cannot honor is reported back so
  /// the peer stops waiting.
  fn connect_to_peer(&self, request: ConnectToPeerResponse) {
    tracing::debug!(
      username = request.username,
      connection_type = ?request.connection_type,
      token = request.token,
      "connection request relayed by the server"
    );
    // an address that cannot be dialed is refused where the request is handled
    if let Some(endpoint) = request_endpoint(request.ip, request.port)
      && !self.options().handler.accept_connection(
        &Peer {
          username: request.username.clone(),
          endpoint,
          confirmed: true,
        },
        request.connection_type,
      )
    {
      tracing::debug!(
        username = request.username,
        token = request.token,
        "connection request refused by the application"
      );
      return self.send_cannot_connect(request.token, &request.username);
    }
    match request.connection_type {
      PeerConnectionType::Peer => self.connect_to_peer_for_messages(request),
      PeerConnectionType::Transfer => self.connect_to_peer_for_transfer(request),
      PeerConnectionType::Distributed => {
        self.distributed_command(Command::ChildRequested(request));
      }
    }
  }

  /// A peer could not act on a connection request the client had the server relay. A search
  /// answer kept under that token is given up, and a solicitation still waiting fails at once
  /// rather than at its timeout; a connection answering it afterwards is unknown.
  fn cannot_connect(&self, failure: &CannotConnectResponse) {
    let token = failure.token;
    if let Some(username) = &failure.username {
      self.emit(Event::User(crate::user::Event::CannotConnect {
        token,
        username: username.clone(),
      }));
    }
    let discarded = self.try_discard(token);
    let refused = self.peers().solicitations.refused(token);
    // a search answer riding on this request is not worth keeping for a late connection; the
    // request is refused before its attempt is woken, which keeps such an answer otherwise
    if let Some(solicitation) = &refused
      && solicitation.connection_type == PeerConnectionType::Peer
      && self.responder().answers_waiting_for(&solicitation.username)
    {
      self.responder().refuse(token);
    }
    let solicited = refused.as_ref().is_some_and(|solicitation| {
      self.waiter().fault(
        &solicitation.key(token),
        Error::connection(
          crate::error::ConnectionKind::Other,
          format!("CannotConnect received for token {token}"),
        ),
      )
    });
    tracing::debug!(token, discarded, solicited, "peer cannot connect");
  }

  /// completes the oldest wait under `key`; a reply nobody waits for is normal for pushed
  /// messages and is dropped
  fn complete<T: Send + 'static>(&self, key: WaitKey, value: T) {
    if !self.inner.waiter.complete(&key, value) {
      tracing::trace!(?key, "reply nobody waits for");
    }
  }

  fn store_obsolete(&self, code: ServerCode, store: impl FnOnce(&mut crate::session::ServerInfo)) {
    tracing::debug!(
      code = code.code(),
      name = code.name(),
      "server sent an obsolete tuning value; stored, not used"
    );
    self.inner.merge_server_info(store);
  }
}

impl Client {
  /// A server message the client has no use for goes to the application as it came, with a
  /// debug line naming the code when it is one the server no longer sends
  fn unhandled(&self, code: u32, frame: &[u8]) {
    let len = frame.len();
    match ServerCode::from_code(code).filter(|code| OBSOLETE.contains(code)) {
      Some(obsolete) => {
        tracing::debug!(code, name = obsolete.name(), len, "obsolete server message");
      }
      None => tracing::debug!(code, len, "unhandled server message"),
    }
    self.emit(Event::Session(crate::session::Event::Unhandled {
      code,
      frame: frame.into(),
    }));
  }
}
