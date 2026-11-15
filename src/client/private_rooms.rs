//! Private rooms: whether other users may add the client to theirs, adding and removing members
//! and operators, and giving up membership or ownership. Each completes when the server
//! announces the change, except giving up ownership, which the server does not answer.

use soulseek_proto::server::{
  PrivateRoomAddOperator, PrivateRoomAddUser, PrivateRoomDropMembership, PrivateRoomDropOwnership,
  PrivateRoomRemoveOperator, PrivateRoomRemoveUser, PrivateRoomToggle,
};
use soulseek_proto::{ServerCode, ServerRequest};

use super::Client;
use super::request::{non_blank, room_key, room_user_key};
use crate::error::ClientError;
use crate::waiter::WaitKey;

impl Client {
  /// Sets whether other users may add the client to their rooms.
  ///
  /// Tells the server whether other users may add the client to their private rooms, and returns
  /// the setting it confirms.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn set_private_room_invitations(&self, accept: bool) -> Result<bool, ClientError> {
    self
      .ask(
        &format!("set private room invitations to {accept}"),
        WaitKey::Server(ServerCode::PrivateRoomToggle),
        ServerRequest::PrivateRoomToggle(PrivateRoomToggle {
          accept_invitations: accept,
        }),
      )
      .await
  }

  /// Adds `username` to the client's private room `room`.
  ///
  /// The user must accept invitations.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn add_private_room_member(
    &self,
    room: &str,
    username: &str,
  ) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .ask(
        &format!("add user {username} to private room {room}"),
        room_user_key(ServerCode::PrivateRoomAddUser, room, username),
        ServerRequest::PrivateRoomAddUser(PrivateRoomAddUser {
          room: room.to_owned(),
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Makes `username` an operator of the client's private room `room`.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn add_private_room_operator(
    &self,
    room: &str,
    username: &str,
  ) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .ask(
        &format!("add operator {username} to private room {room}"),
        room_user_key(ServerCode::PrivateRoomAddOperator, room, username),
        ServerRequest::PrivateRoomAddOperator(PrivateRoomAddOperator {
          room: room.to_owned(),
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Removes `username` from the private room `room`.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn remove_private_room_member(
    &self,
    room: &str,
    username: &str,
  ) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .ask(
        &format!("remove user {username} from private room {room}"),
        room_user_key(ServerCode::PrivateRoomRemoveUser, room, username),
        ServerRequest::PrivateRoomRemoveUser(PrivateRoomRemoveUser {
          room: room.to_owned(),
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Takes operator rights in the private room `room` from `username`.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn remove_private_room_operator(
    &self,
    room: &str,
    username: &str,
  ) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .ask(
        &format!("remove operator {username} from private room {room}"),
        room_user_key(ServerCode::PrivateRoomRemoveOperator, room, username),
        ServerRequest::PrivateRoomRemoveOperator(PrivateRoomRemoveOperator {
          room: room.to_owned(),
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Leaves the private room `room` as a member.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::Timeout`] if the server does not answer in time. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn drop_private_room_membership(&self, room: &str) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    self
      .ask(
        &format!("drop membership of private room {room}"),
        room_key(ServerCode::PrivateRoomRemoved, room),
        ServerRequest::PrivateRoomDropMembership(PrivateRoomDropMembership {
          room: room.to_owned(),
        }),
      )
      .await
  }

  /// Gives up the private room `room` the client owns.
  ///
  /// The server turns it into a public room without an owner, keeps the client in it and cuts its
  /// members off, telling no one, so this completes once the request is on the socket.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request could not be written.
  pub async fn drop_private_room_ownership(&self, room: &str) -> Result<(), ClientError> {
    non_blank("room name", room)?;
    self
      .tell(
        &format!("drop ownership of private room {room}"),
        ServerRequest::PrivateRoomDropOwnership(PrivateRoomDropOwnership {
          room: room.to_owned(),
        }),
      )
      .await
  }
}
