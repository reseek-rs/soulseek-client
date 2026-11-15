//! What peers say about the files the client downloads from them: the offer to start sending
//! one, a refusal or a failure, the client's place in their queue. A refusal or failure ends the
//! download wherever it is. The offer of a file a user sends unasked is held while the
//! application decides whether to take it. Also asking a peer for the client's place in its
//! queue, and failing the waits of a download whose peer sent one of these messages in a form
//! the client cannot read.

use soulseek_proto::peer::{
  PlaceInQueueRequest, PlaceInQueueResponse, TransferRequest, TransferResponse, UploadDenied,
  UploadFailed,
};
use soulseek_proto::{DecodeError, PeerCode, PeerMessage, RemoteFilename};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::RemoteFailure;
use crate::browse::PeerRequestOptions;
use crate::client::{Client, failed, non_blank};
use crate::conn::MessageConn;
use crate::error::{ClientError, MessageErrorKind};
use crate::events::Event;
use crate::peers::{Answering, write_peer};
use crate::resolvers::Peer;
use crate::waiter::WaitKey;

impl Client {
  /// An uploader is about to send the client a file. A download of that file from that user
  /// takes the offer when it waits for one. Any other file is one the user sends unasked: the
  /// application decides whether to take it, and a file it does not take is refused, which is
  /// also how a canceled download tells the uploader to drop it.
  pub(crate) async fn upload_offered(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    peer: &Peer,
    request: TransferRequest,
  ) -> Result<(), ClientError> {
    let username = peer.username.as_str();
    let token = request.token();
    if self.transfers().has_download(username, request.filename()) {
      let key = offer_key(username, request.filename());
      return match self.waiter().offer(&key, request) {
        Ok(()) => Ok(()),
        // a download already under way or past asking has no use for it, and the uploader waits
        // for an answer
        Err(request) => {
          tracing::debug!(filename = %request.filename(), token, "upload offer for a download not waiting for one, refused");
          refuse_offer(conn, answering, token, NOT_TAKEN.into()).await
        }
      };
    }
    let filename = request.filename().clone();
    match self.receive_offer(peer, request).await {
      Ok(()) => {
        self.expire_offer(conn.clone(), username, filename, token);
        Ok(())
      }
      Err(reason) => refuse_offer(conn, answering, token, reason).await,
    }
  }

  /// Asks the application whether to take a file `username` offers unasked, holding the offer
  /// meanwhile. It succeeds when the application started a download of the file, which takes the
  /// offer, and fails with the reason the sender is told: the application's, or [`NOT_TAKEN`]
  async fn receive_offer(&self, peer: &Peer, offer: TransferRequest) -> Result<(), String> {
    let username = peer.username.as_str();
    let filename = offer.filename().clone();
    let token = offer.token();
    let Some(receive) = self.options().resolvers.unasked_offer.clone() else {
      tracing::debug!(username, %filename, token, "file offered unasked refused; nothing receives such files");
      return Err(NOT_TAKEN.into());
    };
    let Some(size) = offered_size(&offer) else {
      tracing::debug!(username, %filename, token, "file offered unasked without a size refused");
      return Err(NOT_TAKEN.into());
    };
    if !self.transfers().hold_offer(username, offer) {
      tracing::debug!(username, %filename, token, "file offered unasked again while its offer is held, refused");
      return Err(NOT_TAKEN.into());
    }
    let reason = match receive(peer.clone(), filename.clone(), size).await {
      Ok(()) => {
        tracing::debug!(username, %filename, token, size, "file offered unasked accepted");
        return Ok(());
      }
      Err(ClientError::TransferRejected(reason)) => {
        tracing::debug!(username, %filename, token, reason, "file offered unasked refused");
        reason
      }
      Err(error) => {
        tracing::warn!(username, %filename, token, %error, "file offered unasked not received");
        NOT_TAKEN.into()
      }
    };
    self.transfers().release_offer(username, &filename, token);
    Err(reason)
  }

  /// Refuses the offer of `filename` with `token` from `username` once the unasked offer timeout
  /// has passed, unless a download took it by then
  fn expire_offer(&self, conn: MessageConn, username: &str, filename: RemoteFilename, token: u32) {
    let held = self.options().unasked_offer_timeout;
    let client = self.downgrade();
    let lifetime = self.cancellation().clone();
    let username = username.to_owned();
    tokio::spawn(
      async move {
        tokio::select! {
          () = lifetime.cancelled() => return,
          () = tokio::time::sleep(held) => {}
        }
        let Some(client) = client.upgrade() else {
          return;
        };
        if !client
          .transfers()
          .release_offer(&username, &filename, token)
        {
          return;
        }
        tracing::debug!(username, %filename, token, "file offered unasked not taken in time, refused");
        if let Err(error) = refuse_offer(&conn, &mut Answering::default(), token, NOT_TAKEN.into()).await {
          tracing::debug!(%error, "refusal of an offer not taken in time not sent");
        }
      }
      .in_current_span(),
    );
  }

  /// The uploader refused a file the client asked for: the download ends rejected with its reason
  pub(crate) fn download_denied(&self, username: &str, denied: UploadDenied) {
    tracing::debug!(filename = %denied.filename, reason = denied.reason, "download denied by the uploader");
    let error = ClientError::TransferRejected(denied.reason.clone());
    if self.end_download(username, &denied.filename, error) {
      self.emit(Event::DownloadDenied {
        username: username.to_owned(),
        filename: denied.filename,
        message: denied.reason,
      });
    }
  }

  /// The uploader reports that sending a file to the client failed: the download ends with that
  pub(crate) fn download_failed(&self, username: &str, failed: UploadFailed) {
    tracing::debug!(filename = %failed.filename, "download reported as failed by the uploader");
    let error =
      ClientError::TransferReportedFailed("Download reported as failed by remote client".into());
    if self.end_download(username, &failed.filename, error) {
      self.emit(Event::DownloadFailed {
        username: username.to_owned(),
        filename: failed.filename,
      });
    }
  }

  /// Ends the download of `filename` from `username` with `error`, at whatever stage it is,
  /// unless it has not asked for the file yet; false when the report was set aside for that
  fn end_download(&self, username: &str, filename: &RemoteFilename, error: ClientError) -> bool {
    match self
      .transfers()
      .fail_download(username, filename, error.clone())
    {
      RemoteFailure::Ended => {
        self.waiter().fault(&offer_key(username, filename), error);
      }
      RemoteFailure::NotAsked => {
        tracing::debug!(%filename, %error, "refusal or failure for a download that has not asked yet, ignored");
        return false;
      }
      RemoteFailure::Untracked => tracing::debug!(%filename, "no such download"),
    }
    true
  }

  /// The client's place in the uploader's queue: the answer to the client's question, or news
  /// the uploader sent on its own, which is raised as an event
  pub(crate) fn place_in_queue_received(&self, username: &str, response: PlaceInQueueResponse) {
    let key = WaitKey::PeerFile {
      code: PeerCode::PlaceInQueueResponse,
      username: username.to_owned(),
      filename: response.filename.clone(),
    };
    if let Err(response) = self.waiter().offer(&key, response) {
      tracing::debug!(filename = %response.filename, place = response.place, "place in queue received");
      self.emit(Event::PlaceInQueueReceived {
        username: username.to_owned(),
        filename: response.filename,
        place: response.place,
      });
    }
  }

  /// Asks a user for the client's place in its queue for a file.
  ///
  /// `username` is asked for the place of `filename`. Peers count from 1; some say 0 while the
  /// file is being sent.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if the username or the filename is empty or only
  /// whitespace. Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidOperation`] if the client is not logged in. Returns
  /// [`ClientError::NoResponse`] if the question was written and never answered. Returns
  /// [`ClientError::Timeout`] if a lookup or connect timed out before the question could be
  /// written. Returns [`ClientError::Canceled`] if the request was canceled. Returns
  /// [`ClientError::UserOffline`] if the server reports the user as offline. Returns a
  /// [`ClientError::Client`] wrapping the cause if the request fails in any other way.
  pub async fn get_download_place_in_queue(
    &self,
    username: &str,
    filename: impl Into<RemoteFilename>,
    options: PeerRequestOptions,
    cancel: &CancellationToken,
  ) -> Result<u32, ClientError> {
    let filename = filename.into();
    non_blank("username", username)?;
    non_blank("filename", filename.as_str())?;
    let verb = format!("get the place in queue of {filename} from {username}");
    self.require_logged_in(&verb)?;
    let answer = self.waiter().wait_from_first_poll::<PlaceInQueueResponse>(
      WaitKey::PeerFile {
        code: PeerCode::PlaceInQueueResponse,
        username: username.to_owned(),
        filename: filename.clone(),
      },
      Some(self.answer_timeout(options.timeout)),
      cancel.clone(),
    );
    let ask = async {
      let endpoint = self.get_user_endpoint(username).await?;
      let conn = self
        .message_connection(username, Some(endpoint), cancel)
        .await?;
      write_peer(
        &conn,
        PeerMessage::PlaceInQueueRequest(PlaceInQueueRequest {
          filename: filename.clone(),
        }),
      )
      .await?;
      match answer.await {
        Ok(answer) => Ok(answer.place),
        Err(ClientError::Timeout(_)) => Err(ClientError::NoResponse(format!(
          "{username} didn't answer the question of the place of {filename} in its queue"
        ))),
        Err(error) => Err(error),
      }
    };
    ask.await.map_err(|error| failed(&verb, error))
  }

  /// A transfer message from `username` that does not decode fails the wait its fields name, as
  /// far as they can be read, so the download waiting for it ends at once
  pub(crate) fn fault_unparseable(&self, username: &str, frame: &[u8], error: &DecodeError) {
    let Some((key, name)) = unparseable_key(username, frame) else {
      return;
    };
    // like a refusal or failure that decodes, one for a download that has not asked yet is
    // about an earlier download of the file
    if let ("UploadDenied" | "UploadFailed", WaitKey::PeerFile { filename, .. }) = (name, &key)
      && !self.transfers().has_asked(username, filename)
    {
      return tracing::debug!(name, %filename, "unparseable refusal or failure for a download that has not asked yet, ignored");
    }
    let error = ClientError::Message {
      kind: MessageErrorKind::Read,
      message: format!("peer sent an unparseable {name}"),
      source: Some(std::sync::Arc::new(error.clone())),
    };
    if self.waiter().fault(&key, error) {
      tracing::debug!(?key, "wait failed by an unparseable message");
    }
  }
}

/// the reason the sender of a file is told when the application gave none
const NOT_TAKEN: &str = "Cancelled";

/// tells the sender of a file the client does not take to drop it
async fn refuse_offer(
  conn: &MessageConn,
  answering: &mut Answering,
  token: u32,
  message: String,
) -> Result<(), ClientError> {
  let refused = PeerMessage::TransferResponse(TransferResponse::Denied { token, message });
  answering.send(conn, &[refused]).await
}

/// the wait a download holds for its uploader's offer
pub(super) fn offer_key(username: &str, filename: &RemoteFilename) -> WaitKey {
  WaitKey::PeerFile {
    code: PeerCode::TransferRequest,
    username: username.to_owned(),
    filename: filename.clone(),
  }
}

/// the direction word of a transfer request in which the sender asks to download
pub(crate) const DIRECTION_DOWNLOAD: u32 = 0;
/// the direction word of a transfer request in which the sender offers to upload
pub(crate) const DIRECTION_UPLOAD: u32 = 1;

/// the size an uploader's offer names; a request to download carries none
pub(super) fn offered_size(offer: &TransferRequest) -> Option<u64> {
  match offer {
    TransferRequest::Upload { file_size, .. } => *file_size,
    TransferRequest::Download { .. } => None,
  }
}

/// The wait a transfer message that did not decode was for, read from the fields at its start,
/// and the message's name; `None` when not even those can be read or no download waits for it
fn unparseable_key(username: &str, frame: &[u8]) -> Option<(WaitKey, &'static str)> {
  let code = u32_at(frame, 4)?;
  let payload = frame.get(8..)?;
  match PeerCode::from_code(code)? {
    PeerCode::TransferRequest => {
      // only an uploader's offer is waited for
      let direction = u32_at(payload, 0)?;
      let filename = string_at(payload, 8)?;
      (direction == DIRECTION_UPLOAD).then(|| (offer_key(username, &filename), "TransferRequest"))
    }
    PeerCode::TransferResponse => Some((
      WaitKey::PeerToken {
        code: PeerCode::TransferResponse,
        username: username.to_owned(),
        token: u32_at(payload, 0)?,
      },
      "TransferResponse",
    )),
    PeerCode::UploadDenied => Some((offer_key(username, &string_at(payload, 0)?), "UploadDenied")),
    PeerCode::UploadFailed => Some((offer_key(username, &string_at(payload, 0)?), "UploadFailed")),
    _ => None,
  }
}

fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
  let bytes = bytes.get(at..at + 4)?;
  Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn string_at(bytes: &[u8], at: usize) -> Option<RemoteFilename> {
  let len = u32_at(bytes, at)? as usize;
  let text = bytes.get(at + 4..(at + 4).checked_add(len)?)?;
  Some(RemoteFilename::from_bytes(text.to_vec()))
}

#[cfg(test)]
mod tests {
  use super::*;

  /// a frame of `code` whose payload is `payload`
  fn frame(code: PeerCode, payload: &[u8]) -> Vec<u8> {
    let mut frame = ((payload.len() + 4) as u32).to_le_bytes().to_vec();
    frame.extend(code.code().to_le_bytes());
    frame.extend(payload);
    frame
  }

  fn string(text: &str) -> Vec<u8> {
    let mut bytes = (text.len() as u32).to_le_bytes().to_vec();
    bytes.extend(text.as_bytes());
    bytes
  }

  #[test]
  fn an_unparseable_offer_names_the_downloads_wait() {
    // an upload offer whose size is cut short
    let mut payload = 1u32.to_le_bytes().to_vec();
    payload.extend(9u32.to_le_bytes());
    payload.extend(string("a.mp3"));
    payload.extend([1, 2, 3]);
    let (key, name) = unparseable_key("bob", &frame(PeerCode::TransferRequest, &payload)).unwrap();
    assert_eq!(key, offer_key("bob", &"a.mp3".into()));
    assert_eq!(name, "TransferRequest");
    // a download request is never waited for
    payload[0] = 0;
    assert!(unparseable_key("bob", &frame(PeerCode::TransferRequest, &payload)).is_none());
  }

  #[test]
  fn an_unparseable_answer_or_refusal_names_its_wait() {
    let (key, _) =
      unparseable_key("bob", &frame(PeerCode::TransferResponse, &[7, 0, 0, 0, 9])).unwrap();
    assert_eq!(
      key,
      WaitKey::PeerToken {
        code: PeerCode::TransferResponse,
        username: "bob".into(),
        token: 7,
      }
    );
    let denied = frame(PeerCode::UploadDenied, &string("a.mp3"));
    assert_eq!(
      unparseable_key("bob", &denied).unwrap().0,
      offer_key("bob", &"a.mp3".into())
    );
    // a filename cut short names nothing
    assert!(unparseable_key("bob", &frame(PeerCode::UploadFailed, &[9, 0, 0, 0, b'a'])).is_none());
  }
}
