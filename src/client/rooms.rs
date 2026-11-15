//! Chat rooms: joining and leaving, speaking, tickers, the room list and the public chat feed.

use std::sync::Arc;

use soulseek_proto::server::{
  JoinRoomRequest, LeaveRoom, RoomData, RoomList, SayInChatRoomRequest, SetRoomTicker,
};
use soulseek_proto::{RoomType, ServerCode, ServerRequest, validate_room_name};

use super::Client;
use super::request::{non_blank, non_empty, room_key};
use crate::error::ClientError;
use crate::waiter::WaitKey;

impl Client {
  /// Joins `room` and returns its members.
  ///
  /// The members come with the room's owner and operators when it is private. A room that does not
  /// exist is created as `room_type`. Unless [`check_names`](crate::ClientOptions::check_names) is
  /// off, the name is checked first, since the server answers a bad name only with a private
  /// message from `server`.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` is empty or only whitespace, or if the name
  /// is checked and is a bad one. Returns [`ClientError::Shutdown`] if the client has been shut
  /// down. Returns [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Room`] of kind [`JoinForbidden`](crate::RoomErrorKind::JoinForbidden) if a
  /// private room the client is not in holds the name. Returns [`ClientError::NoResponse`] if the
  /// server gives no answer at all, which means the client is in the room already. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn join_room(&self, room: &str, room_type: RoomType) -> Result<RoomData, ClientError> {
    non_blank("room name", room)?;
    if self.options().check_names {
      validate_room_name(room)
        .map_err(|error| ClientError::InvalidArgument(format!("Invalid room name: {error}")))?;
    }
    let joined = self
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
      Err(ClientError::Timeout(_)) => Err(ClientError::NoResponse(format!(
        "The server didn't respond to the request to join chat room {room}."
      ))),
      joined => joined,
    }
  }

  /// Leaves `room`.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::NoResponse`] if the server gives no answer, which means the client was not in
  /// the room. Returns a [`ClientError::Client`] wrapping the cause if the request could not be
  /// written or the server connection closed before the answer.
  pub async fn leave_room(&self, room: &str) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    let left = self
      .ask(
        &format!("leave chat room {room}"),
        room_key(ServerCode::LeaveRoom, room),
        ServerRequest::LeaveRoom(LeaveRoom {
          room: room.to_owned(),
        }),
      )
      .await;
    match left {
      Err(ClientError::Timeout(_)) => Err(ClientError::NoResponse(format!(
        "The server didn't respond to the request to leave chat room {room}."
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
  /// Returns [`ClientError::InvalidArgument`] if `room` is empty or only whitespace, or if
  /// `message` is empty. Returns [`ClientError::Shutdown`] if the client has been shut down.
  /// Returns [`ClientError::InvalidOperation`] if the client is not logged in. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written.
  pub async fn send_room_message(&self, room: &str, message: &str) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    non_empty("message", message)?;
    self
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
  /// Returns [`ClientError::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written.
  pub async fn set_room_ticker(&self, room: &str, message: &str) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    self
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
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn get_room_list(&self) -> Result<Arc<RoomList>, ClientError> {
    self
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
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written.
  pub async fn start_public_chat(&self) -> Result<(), ClientError> {
    self
      .tell("start public chat", ServerRequest::AskPublicChat)
      .await
  }

  /// Stops the public chat feed.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written.
  pub async fn stop_public_chat(&self) -> Result<(), ClientError> {
    self
      .tell("stop public chat", ServerRequest::StopPublicChat)
      .await
  }
}
