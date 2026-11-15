//! Browsing a peer's shares, and fetching one of its folders. A share listing can be large and
//! slow to come: the browse waits a long while for its first bytes, then follows the connection
//! that brings it until the whole response is in, reporting progress as it arrives. A folder comes
//! back as the folder and every folder below it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use soulseek_proto::peer::{
  BrowseResponse, Directory, FolderContentsRequest, FolderContentsResponse,
};
use soulseek_proto::{PeerCode, PeerMessage, RemoteFilename};
use tokio_util::sync::CancellationToken;

use crate::client::{Client, failed, non_blank};
use crate::error::{ClientError, ConnectionErrorKind};
use crate::events::Event;
use crate::lock;
use crate::peers::{IncomingBrowse, write_peer};
use crate::waiter::WaitKey;

/// How many of the client's folder requests wait on each user. A folder's token is inside its
/// compressed payload, so this is what lets a folder nobody asked that user for be skipped from
/// its header.
#[derive(Default)]
pub(crate) struct FolderRequests(Mutex<HashMap<String, usize>>);

impl FolderRequests {
  /// counts a request to `username` until the returned guard is dropped
  fn hold(&self, username: &str) -> FolderRequest<'_> {
    *lock(&self.0).entry(username.to_owned()).or_default() += 1;
    FolderRequest {
      requests: self,
      username: username.to_owned(),
    }
  }

  /// whether a folder request of the client's to `username` waits
  pub(crate) fn any(&self, username: &str) -> bool {
    lock(&self.0).contains_key(username)
  }
}

/// one folder request counted in [`FolderRequests`] while it waits
struct FolderRequest<'a> {
  requests: &'a FolderRequests,
  username: String,
}

impl Drop for FolderRequest<'_> {
  fn drop(&mut self) {
    let mut counts = lock(&self.requests.0);
    let count = counts
      .get_mut(&self.username)
      .expect("a held request is counted");
    *count -= 1;
    if *count == 0 {
      counts.remove(&self.username);
    }
  }
}

/// Progress of a browse response.
#[derive(Clone, Debug, PartialEq)]
pub struct BrowseProgress {
  /// User browsed.
  pub username: String,
  /// Number of compressed bytes received.
  pub bytes_transferred: u64,
  /// Number of compressed bytes still to come.
  pub bytes_remaining: u64,
  /// How much of the response has arrived, from 0 to 100.
  pub percent_complete: f64,
  /// Compressed size of the whole response.
  pub size: u64,
}

impl BrowseProgress {
  fn new(username: &str, received: u64, size: u64) -> Self {
    Self {
      username: username.to_owned(),
      bytes_transferred: received,
      bytes_remaining: size.saturating_sub(received),
      // an empty response is complete as soon as it starts
      percent_complete: if size == 0 {
        100.0
      } else {
        received as f64 / size as f64 * 100.0
      },
      size,
    }
  }
}

/// Callback for the progress of a browse response.
///
/// Called with `(progress)` as the response arrives. When it is given, no progress event is raised
/// for the browse.
pub type BrowseProgressUpdated = Arc<dyn Fn(&BrowseProgress) + Send + Sync>;

/// Options of one browse.
#[derive(Clone, Default)]
pub struct BrowseOptions {
  /// Time to wait for the first bytes of the response.
  ///
  /// If `None`, the client's [`message_timeout`](crate::ClientOptions::message_timeout) is used. A
  /// peer with a large share takes a while to put it together. Unset by default.
  pub timeout: Option<Duration>,
  /// Callback called as the response arrives.
  ///
  /// It is called in place of the progress event. Unset by default.
  pub progress_updated: Option<BrowseProgressUpdated>,
}

/// shows whether the callback is set; the callback itself has nothing to print
impl std::fmt::Debug for BrowseOptions {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BrowseOptions")
      .field("timeout", &self.timeout)
      .field("progress_updated", &self.progress_updated.is_some())
      .finish()
  }
}

/// Options of one request to a peer.
///
/// The request is for the peer's folder contents, its user info or a file's place in its queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerRequestOptions {
  /// Time to wait for the answer.
  ///
  /// If `None`, the client's [`message_timeout`](crate::ClientOptions::message_timeout) is used.
  /// Unset by default.
  pub timeout: Option<Duration>,
}

impl Client {
  /// how long a request with `timeout` waits for its answer
  pub(crate) fn answer_timeout(&self, timeout: Option<Duration>) -> Duration {
    timeout.unwrap_or_else(|| self.waiter().default_timeout())
  }
}

impl Client {
  /// Fetches `username`'s whole share listing.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::UserOffline`] if the server reports the user as offline. Returns
  /// [`ClientError::Timeout`] if a wait expired, the wait for the first bytes of the response among
  /// them. Returns [`ClientError::Canceled`] if `cancel` fires. Returns [`ClientError::Connection`]
  /// for a socket-level failure, such as the peer connection closing before the whole response
  /// arrived. Returns [`ClientError::Message`] for a frame that could not be read, decoded or
  /// encoded. Returns a [`ClientError::Client`] wrapping the cause for any other failure.
  pub async fn browse(
    &self,
    username: &str,
    options: BrowseOptions,
    cancel: &CancellationToken,
  ) -> Result<BrowseResponse, ClientError> {
    non_blank("username", username)?;
    let verb = format!("browse user {username}");
    self.require_logged_in(&verb)?;
    let browsed = self
      .browse_from(username, &options, cancel)
      .await
      .map_err(|error| match error {
        error @ (ClientError::Timeout(_)
        | ClientError::Canceled
        | ClientError::UserOffline(_)
        | ClientError::Connection { .. }
        | ClientError::Message { .. }) => error,
        error => failed(&verb, error),
      });
    match &browsed {
      Ok(response) => tracing::debug!(
        username,
        directories = response.directories.len(),
        "browse completed"
      ),
      Err(error) => tracing::debug!(username, %error, "browse failed"),
    }
    browsed
  }

  async fn browse_from(
    &self,
    username: &str,
    options: &BrowseOptions,
    cancel: &CancellationToken,
  ) -> Result<BrowseResponse, ClientError> {
    // both in place before the request, so a fast answer is not missed; the wait for the first
    // bytes counts from the request, not from the connection made for it
    let mut result = Box::pin(self.waiter().wait_indefinitely::<Arc<BrowseResponse>>(
      WaitKey::Peer {
        code: PeerCode::BrowseResponse,
        username: username.to_owned(),
      },
      cancel.clone(),
    ));
    let first_bytes = self.waiter().wait_from_first_poll::<IncomingBrowse>(
      WaitKey::BrowseResponseConnection {
        username: username.to_owned(),
      },
      Some(self.answer_timeout(options.timeout)),
      cancel.clone(),
    );
    let endpoint = self.get_user_endpoint(username).await?;
    let conn = self
      .message_connection(username, Some(endpoint), cancel)
      .await?;
    write_peer(&conn, PeerMessage::BrowseRequest).await?;
    let IncomingBrowse {
      length,
      connection,
      mut received,
    } = first_bytes.await?;
    // the payload after the code; a response too large to follow failed the wait above
    let size = u64::from(length).saturating_sub(4);
    tracing::debug!(username, size, "browse response arriving");
    let progress = |received: u64| {
      let progress = BrowseProgress::new(username, received, size);
      match &options.progress_updated {
        Some(callback) => callback(&progress),
        None => self.emit(Event::BrowseProgressUpdated(progress)),
      }
    };
    // the first chunk was read with the header, so its own progress is not seen
    progress(0);
    let mut reported = 0;
    let handle = connection.handle().clone();
    let response = loop {
      tokio::select! {
        biased;
        response = &mut result => break response?,
        reason = handle.disconnected() => {
          // a response that arrived whole is decoded and delivered all the same
          if (*received.borrow() as u64) < size {
            return Err(ClientError::connection(
              ConnectionErrorKind::Other,
              format!("Peer connection disconnected unexpectedly: {}", reason.message),
            ));
          }
          break result.as_mut().await?;
        }
        changed = received.changed() => {
          if changed.is_err() {
            // the read loop moved on to the next frame; the response is complete or failed
            break result.as_mut().await?;
          }
          reported = *received.borrow_and_update() as u64;
          progress(reported);
        }
      }
    };
    if reported < size {
      progress(size);
    }
    Ok(Arc::unwrap_or_clone(response))
  }

  /// Fetches `directory` from `username`'s shares.
  ///
  /// Every directory below it comes back as an entry of its own.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if `username` or `directory` is empty or only
  /// whitespace. Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::UserOffline`] if the server reports the user as offline. Returns
  /// [`ClientError::Timeout`] if a wait expired, the wait for the answer among them. Returns
  /// [`ClientError::Canceled`] if `cancel` fires. Returns a [`ClientError::Client`] wrapping the
  /// cause for any other failure.
  pub async fn get_folder_contents(
    &self,
    username: &str,
    directory: impl Into<RemoteFilename>,
    options: PeerRequestOptions,
    cancel: &CancellationToken,
  ) -> Result<Vec<Directory>, ClientError> {
    let directory = directory.into();
    non_blank("username", username)?;
    non_blank("directory", directory.as_str())?;
    let verb = format!("get the contents of {directory} from {username}");
    self.require_logged_in(&verb)?;
    let token = self.next_token();
    let _asking = self.folder_requests().hold(username);
    let answer = self
      .waiter()
      .wait_from_first_poll::<FolderContentsResponse>(
        WaitKey::PeerToken {
          code: PeerCode::FolderContentsResponse,
          username: username.to_owned(),
          token,
        },
        Some(self.answer_timeout(options.timeout)),
        cancel.clone(),
      );
    let ask = async {
      let endpoint = self.get_user_endpoint(username).await?;
      let conn = self
        .message_connection(username, Some(endpoint), cancel)
        .await?;
      let request = PeerMessage::FolderContentsRequest(FolderContentsRequest {
        token,
        directory_name: directory,
      });
      write_peer(&conn, request).await?;
      Ok(answer.await?.directories)
    };
    ask.await.map_err(|error| failed(&verb, error))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn browse_progress_counts_an_empty_response_as_complete() {
    assert_eq!(BrowseProgress::new("bob", 0, 0).percent_complete, 100.0);
    let half = BrowseProgress::new("bob", 50, 100);
    assert_eq!(half.percent_complete, 50.0);
    assert_eq!(half.bytes_remaining, 50);
  }
}
