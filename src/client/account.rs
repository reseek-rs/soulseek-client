//! The client's own account: presence, share counts, upload speed, password and privileges.

use crate::account::Account;
use crate::user::Users;
use std::time::Duration;

use soulseek_proto::server::{
  AcknowledgeNotifyPrivileges, GivePrivileges, NewPassword, SendUploadSpeed, SetOnlineStatus,
  SharedFoldersAndFiles,
};
use soulseek_proto::{Secret, ServerCode, ServerRequest, UserPresence};

use super::request::{failed, non_blank};
use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;
use crate::waiter::WaitKey;

impl Account<'_> {
  /// Tells the server whether the client is away or online.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn set_status(self, presence: UserPresence) -> Result<(), Error> {
    self
      .client
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
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn set_shared_counts(self, directories: u32, files: u32) -> Result<(), Error> {
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `speed` is zero. Returns [`ErrorKind::Shutdown`]
  /// if the client has been shut down. Returns [`ErrorKind::InvalidOperation`] if the client is
  /// not logged in. Returns an [`ErrorKind::Connection`] if the request could not be written.
  pub async fn report_upload_speed(self, speed: u32) -> Result<(), Error> {
    if speed == 0 {
      return Err(Error::invalid_argument(
        "the upload speed must be greater than zero".into(),
      ));
    }
    self
      .client
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
  /// Returns [`ErrorKind::InvalidArgument`] if `password` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Operation`] if the server
  /// echoes a different password. Returns an [`ErrorKind::Connection`] if the request could not be
  /// written or the server connection closed before the answer.
  pub async fn change_password(self, password: &str) -> Result<(), Error> {
    non_blank("password", password)?;
    let echo: Secret = self
      .client
      .ask_in_order(
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
      ));
    }
    Ok(())
  }

  /// Returns how much privilege time the client's account has left.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn privileges(self) -> Result<Duration, Error> {
    let seconds: u32 = self
      .client
      .ask(
        "get privileges",
        WaitKey::Server(ServerCode::CheckPrivileges),
        ServerRequest::CheckPrivileges,
      )
      .await?;
    Ok(Duration::from_secs(seconds.into()))
  }

  /// Acknowledges the privilege notification `id`.
  ///
  /// Allowed while logging in too, like private message acknowledgments.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is neither logging in nor logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn acknowledge_privilege_notification(self, id: u32) -> Result<(), Error> {
    let verb = format!("acknowledge privilege notification with ID {id}");
    self.client.require_logging_in_or_logged_in(&verb)?;
    self
      .client
      .write_server_message(&ServerRequest::AcknowledgeNotifyPrivileges(
        AcknowledgeNotifyPrivileges { id },
      ))
      .await
      .map_err(|error| failed(&verb, error))
  }
}

impl Users<'_> {
  /// Gives `days` of the client's privileges to `username`.
  ///
  /// The server does not answer. A failure comes later as a private message from `server`.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace, or if
  /// `days` is zero. Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn grant_privileges(self, username: &str, days: u32) -> Result<(), Error> {
    non_blank("username", username)?;
    if days == 0 {
      return Err(Error::invalid_argument(
        "the number of days granted must be greater than zero".into(),
      ));
    }
    self
      .client
      .tell(
        &format!("grant privileges to user {username}"),
        ServerRequest::GivePrivileges(GivePrivileges {
          username: username.to_owned(),
          days,
        }),
      )
      .await
  }
}
