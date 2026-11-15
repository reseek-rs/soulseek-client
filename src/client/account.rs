//! The client's own account: presence, share counts, upload speed, password and privileges.

use std::time::Duration;

use soulseek_proto::server::{
  AcknowledgeNotifyPrivileges, GivePrivileges, NewPassword, SendUploadSpeed, SetOnlineStatus,
  SharedFoldersAndFiles,
};
use soulseek_proto::{Secret, ServerCode, ServerRequest, UserPresence};

use super::Client;
use super::request::{failed, non_blank};
use crate::error::Error;
use crate::waiter::WaitKey;

impl Client {
  /// Tells the server whether the client is away or online.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn set_status(&self, presence: UserPresence) -> Result<(), Error> {
    self
      .tell(
        "set status",
        ServerRequest::SetOnlineStatus(SetOnlineStatus { status: presence }),
      )
      .await
  }

  /// Tells the server how many folders and files the client shares.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn set_shared_counts(&self, directories: u32, files: u32) -> Result<(), Error> {
    self
      .tell(
        "set shared counts",
        ServerRequest::SharedFoldersAndFiles(SharedFoldersAndFiles {
          directory_count: directories,
          file_count: files,
        }),
      )
      .await
  }

  /// Reports the average speed of an upload the client just finished.
  ///
  /// `speed` is in bytes per second.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `speed` is zero. Returns [`Error::Shutdown`]
  /// if the client has been shut down. Returns [`Error::InvalidOperation`] if the client is
  /// not logged in. Returns a [`Error::Operation`] wrapping the cause if the request could not
  /// be written.
  pub async fn send_upload_speed(&self, speed: u32) -> Result<(), Error> {
    if speed == 0 {
      return Err(Error::InvalidArgument(
        "the upload speed must be greater than zero".into(),
      ));
    }
    self
      .tell(
        "send upload speed",
        ServerRequest::SendUploadSpeed(SendUploadSpeed { speed }),
      )
      .await
  }

  /// Changes the client's password.
  ///
  /// The server echoes the new password once it took it. An echo that differs means the change
  /// probably failed.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `password` is empty or only whitespace. Returns
  /// [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::Timeout`] if the server does not answer in time. Returns a
  /// [`Error::Operation`] if the server echoes a different password. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn change_password(&self, password: &str) -> Result<(), Error> {
    non_blank("password", password)?;
    let echo: Secret = self
      .ask(
        "change password",
        WaitKey::Server(ServerCode::NewPassword),
        ServerRequest::NewPassword(NewPassword {
          new_password: Secret::new(password),
        }),
      )
      .await?;
    if echo.expose() != password {
      return Err(Error::operation(
        "the server answered the password change with a different password",
        None,
      ));
    }
    Ok(())
  }

  /// Returns how much privilege time the client's account has left.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::Timeout`] if the server does not answer in time. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written or the server
  /// connection closed before the answer.
  pub async fn get_privileges(&self) -> Result<Duration, Error> {
    let seconds: u32 = self
      .ask(
        "get privileges",
        WaitKey::Server(ServerCode::CheckPrivileges),
        ServerRequest::CheckPrivileges,
      )
      .await?;
    Ok(Duration::from_secs(seconds.into()))
  }

  /// Gives `days` of the client's privileges to `username`.
  ///
  /// The server does not answer. A failure comes later as a private message from `server`.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `username` is empty or only whitespace, or if
  /// `days` is zero. Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns a
  /// [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn grant_user_privileges(&self, username: &str, days: u32) -> Result<(), Error> {
    non_blank("username", username)?;
    if days == 0 {
      return Err(Error::InvalidArgument(
        "the number of days granted must be greater than zero".into(),
      ));
    }
    self
      .tell(
        &format!("grant privileges to user {username}"),
        ServerRequest::GivePrivileges(GivePrivileges {
          username: username.to_owned(),
          days,
        }),
      )
      .await
  }

  /// Acknowledges the privilege notification `id`.
  ///
  /// Allowed while logging in too, like private message acknowledgments.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidOperation`] if the client is neither logging in nor logged in.
  /// Returns a [`Error::Operation`] wrapping the cause if the request could not be written.
  pub async fn acknowledge_privilege_notification(&self, id: u32) -> Result<(), Error> {
    let verb = format!("acknowledge privilege notification with ID {id}");
    self.require_logging_in_or_logged_in(&verb)?;
    self
      .write_server_message(&ServerRequest::AcknowledgeNotifyPrivileges(
        AcknowledgeNotifyPrivileges { id },
      ))
      .await
      .map_err(|error| failed(&verb, error))
  }
}
