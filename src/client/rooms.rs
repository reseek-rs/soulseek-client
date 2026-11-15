//! Chat rooms: joining and leaving, speaking, tickers, the room list and the public chat feed.

use crate::room::Rooms;
use std::sync::Arc;

use soulseek_proto::server::{
  JoinRoomRequest, LeaveRoom, RoomData, RoomList, SayInChatRoomRequest, SetRoomTicker,
};
use soulseek_proto::{RoomType, ServerCode, ServerRequest, validate_room_name};

use super::request::{non_blank, non_empty, room_key};
use crate::error::Error;
use crate::waiter::WaitKey;

impl Rooms<'_> {
  /// Joins `room` and returns its members.
  ///
  /// The members come with the room's owner and operators when it is private. A room that does not
  /// exist is created as `room_type`. Unless [`check_names`](crate::Config::check_names) is
  /// off, the name is checked first, since the server answers a bad name only with a private
  /// message from `server`.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `room` is empty or only whitespace, or if the name
  /// is checked and is a bad one. Returns [`Error::Shutdown`] if the client has been shut
  /// down. Returns [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::Room`] of kind [`JoinForbidden`](crate::error::RoomKind::JoinForbidden) if a
  /// private room the client is not in holds the name. Returns [`Error::NoResponse`] if the
  /// server gives no answer at all, which means the client is in the room already. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn join(self, room: &str, room_type: RoomType) -> Result<RoomData, Error> {
    non_blank("room name", room)?;
    if self.client.options().check_names {
      validate_room_name(room)
        .map_err(|error| Error::InvalidArgument(format!("invalid room name: {error}")))?;
    }
    let joined = self
      .client
      .ask(
        &format!("join chat room {room}"),
        room_key(ServerCode::JoinRoom, room),
        ServerRequest::JoinRoom(JoinRoomRequest {
          room: room.to_owned(),
          room_type,
        }),
      )
      .await;
    match joined {
      Err(Error::Timeout(_)) => Err(Error::NoResponse(format!(
        "the server didn't respond to the request to join chat room {room}"
      ))),
      joined => joined,
    }
  }

  /// Leaves `room`.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::NoResponse`] if the server gives no answer, which means the client was not in
  /// the room. Returns a [`Error::Operation`] wrapping the cause if the request could not be
  /// written or the server connection closed before the answer.
  pub async fn leave(self, room: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    let left = self
      .client
      .ask(
        &format!("leave chat room {room}"),
        room_key(ServerCode::LeaveRoom, room),
        ServerRequest::LeaveRoom(LeaveRoom {
          room: room.to_owned(),
        }),
      )
      .await;
    match left {
      Err(Error::Timeout(_)) => Err(Error::NoResponse(format!(
        "the server didn't respond to the request to leave chat room {room}"
      ))),
      left => left,
    }
  }

  /// Says `message` in `room`.
  ///
  /// The server echoes it back to the client like any other line.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `room` is empty or only whitespace, or if
  /// `message` is empty. Returns [`Error::Shutdown`] if the client has been shut down.
  /// Returns [`Error::InvalidOperation`] if the client is not logged in. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn say(self, room: &str, message: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    non_empty("message", message)?;
    self
      .client
      .tell(
        &format!("send message to room {room}"),
        ServerRequest::SayInChatRoom(SayInChatRoomRequest {
          room: room.to_owned(),
          message: message.to_owned(),
        }),
      )
      .await
  }

  /// Sets the client's ticker in `room`.
  ///
  /// An empty message clears it.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn set_ticker(self, room: &str, message: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    self
      .client
      .tell(
        &format!("set ticker for room {room}"),
        ServerRequest::SetRoomTicker(SetRoomTicker {
          room: room.to_owned(),
          ticker: message.to_owned(),
        }),
      )
      .await
  }

  /// Returns every room on the server with its member count.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::Timeout`] if the server does not answer in time. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn list(self) -> Result<Arc<RoomList>, Error> {
    self
      .client
      .ask(
        "get the room list",
        WaitKey::Server(ServerCode::RoomList),
        ServerRequest::RoomList,
      )
      .await
  }

  /// Starts the feed of every line said in every public room.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn start_public_chat(self) -> Result<(), Error> {
    self
      .client
      .tell("start public chat", ServerRequest::AskPublicChat)
      .await
  }

  /// Stops the public chat feed.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn stop_public_chat(self) -> Result<(), Error> {
    self
      .client
      .tell("stop public chat", ServerRequest::StopPublicChat)
      .await
  }
}
