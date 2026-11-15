//! Fetching a peer's user info: its description, picture, upload slots and queue. An answer
//! too large or malformed to read fails the request.

use soulseek_proto::peer::UserInfo;
use soulseek_proto::{PeerCode, PeerMessage};
use tokio_util::sync::CancellationToken;

use crate::browse::PeerRequestOptions;
use crate::client::{Client, failed, non_blank};
use crate::error::ClientError;
use crate::peers::write_peer;
use crate::waiter::WaitKey;

impl Client {
  /// Fetches `username`'s user info.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in.
  ///
  /// Returns [`ClientError::UserOffline`] if the server reports the user as offline. Returns
  /// [`ClientError::Timeout`] if the server does not say in time where the user listens, or if
  /// the peer does not answer in the time [`PeerRequestOptions::timeout`] allows. Returns
  /// [`ClientError::Canceled`] if `cancel` fires, or if a shutdown of the client cancels a wait
  /// of the call.
  ///
  /// Returns [`ClientError::Timeout`] if the call waits on a dial the client makes because the
  /// user asked the server for a connection and the dial is not answered in time, and
  /// [`ClientError::Canceled`] if that dial is given up because the client is shut down.
  ///
  /// Returns a [`ClientError::Client`] wrapping the cause in the remaining cases. The cause is a
  /// [`ClientError::UserEndpoint`] if the address lookup fails in any other way or the server
  /// gives a port that is out of range, a [`ClientError::Connection`] if no message connection
  /// to the user can be made, if the request cannot be written on it, or if it closes before
  /// the answer arrives, a [`ClientError::Message`] if the answer is longer than
  /// [`ClientLimits::decode`](crate::ClientLimits::decode) allows or cannot be decoded, a
  /// [`ClientError::Shutdown`] if the client is found shut down after the call's first check,
  /// and a [`ClientError::InvalidOperation`] if the client is logged out before it starts
  /// connecting to the user, or if a connection to the user closes after it was made and before
  /// the client wrote on it.
  pub async fn get_user_info(
    &self,
    username: &str,
    options: PeerRequestOptions,
    cancel: &CancellationToken,
  ) -> Result<UserInfo, ClientError> {
    non_blank("username", username)?;
    let verb = format!("get the user info of {username}");
    self.require_logged_in(&verb)?;
    let answer = self.waiter().wait_from_first_poll::<UserInfo>(
      info_key(username),
      Some(self.answer_timeout(options.timeout)),
      cancel.clone(),
    );
    let ask = async {
      let endpoint = self.get_user_endpoint(username).await?;
      let conn = self
        .message_connection(username, Some(endpoint), cancel)
        .await?;
      write_peer(&conn, PeerMessage::InfoRequest).await?;
      answer.await
    };
    ask.await.map_err(|error| failed(&verb, error))
  }

  /// a peer's user info arrived: it answers the request waiting for it
  pub(crate) fn user_info_received(&self, username: &str, info: UserInfo) {
    if !self.waiter().complete(&info_key(username), info) {
      tracing::debug!(username, "user info nobody waits for, dropped");
    }
  }

  /// whether a request of the client's waits for `username`'s user info
  pub(crate) fn user_info_awaited(&self, username: &str) -> bool {
    self.waiter().has_wait(&info_key(username))
  }

  /// A peer's user info could not be read, being too long or malformed: the request waiting for
  /// it fails with `error` now rather than timing out
  pub(crate) fn user_info_unreadable(&self, username: &str, error: ClientError) {
    if self.waiter().fault(&info_key(username), error) {
      tracing::debug!(username, "user info request failed by an unreadable answer");
    }
  }
}

fn info_key(username: &str) -> WaitKey {
  WaitKey::Peer {
    code: PeerCode::InfoResponse,
    username: username.to_owned(),
  }
}
