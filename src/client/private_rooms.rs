//! Private rooms: whether other users may add the client to theirs, adding and removing members
//! and operators, and giving up membership or ownership. Each completes when the server
//! announces the change, except giving up ownership, which the server does not answer.

use crate::room::Rooms;
use soulseek_proto::server::{
  PrivateRoomAddOperator, PrivateRoomAddUser, PrivateRoomDropMembership, PrivateRoomDropOwnership,
  PrivateRoomRemoveOperator, PrivateRoomRemoveUser, PrivateRoomToggle,
};
use soulseek_proto::{ServerCode, ServerRequest};

use super::request::{non_blank, room_key, room_user_key};
use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;
use crate::waiter::WaitKey;

impl Rooms<'_> {
  /// Sets whether other users may add the client to their rooms.
  ///
  /// Tells the server whether other users may add the client to their private rooms, and returns
  /// the setting it confirms.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn accept_invitations(self, accept: bool) -> Result<bool, Error> {
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn add_member(self, room: &str, username: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn add_operator(self, room: &str, username: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn remove_member(self, room: &str, username: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `room` or `username` is empty or only whitespace.
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn remove_operator(self, room: &str, username: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    non_blank("username", username)?;
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn give_up_membership(self, room: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `room` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn give_up_ownership(self, room: &str) -> Result<(), Error> {
    non_blank("room name", room)?;
    self
      .client
      .tell(
        &format!("drop ownership of private room {room}"),
        ServerRequest::PrivateRoomDropOwnership(PrivateRoomDropOwnership {
          room: room.to_owned(),
        }),
      )
      .await
  }
}
