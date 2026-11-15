//! How the client answers what a peer asks of it on a message connection: shares, user info
//! and folder contents come from the application's resolvers, and a download the peer wants is
//! queued through the application, refused, or failed so the peer asks again later. A resolver
//! that fails yields no answer, or for a download the word that it failed; its error never
//! reaches the peer. An answer is worked out in full, then queued in one write.

use soulseek_proto::peer::{
  FolderContentsRequest, FolderContentsResponse, PlaceInQueueResponse, TransferRequest,
  TransferResponse, UploadDenied, UploadFailed,
};
use soulseek_proto::{PeerCode, PeerMessage, RemoteFilename};

use super::dispatch::Turn;
use crate::client::Client;
use crate::conn::{EncodedFrame, HandlerPermit, MessageConn, written};
use crate::error::ClientError;
use crate::resolvers::{BrowseReply, Peer, UploadAsk, UploadDecision};

/// What the handler of a peer frame holds while it works out its answer: its places against the
/// bounds on frames handled at once, across connections and on its own, and for a frame handled
/// in order, its turn. Once the answer is queued, before it is written, the shared place and the
/// turn are let go; the connection's own place is held until the write ends. An answer sent
/// from outside a handler holds nothing.
#[derive(Default)]
pub(crate) struct Answering {
  permit: Option<HandlerPermit>,
  turn: Option<Turn>,
}

impl Answering {
  pub(super) fn new(permit: Option<HandlerPermit>, turn: Option<Turn>) -> Self {
    Self { permit, turn }
  }

  /// Queues `messages` on `conn` in one write, lets go of the shared place and the turn, and
  /// waits until they are on the socket
  pub(crate) async fn send(
    &mut self,
    conn: &MessageConn,
    messages: &[PeerMessage],
  ) -> Result<(), ClientError> {
    let bytes = messages.iter().flat_map(PeerMessage::encode).collect();
    let receipt = conn.send_with_receipt(bytes);
    self.let_go();
    written(receipt?).await?;
    for message in messages {
      let code = message.code();
      tracing::debug!(code = code.code(), name = code.name(), "peer message sent");
    }
    Ok(())
  }

  /// [`Answering::send`] for a message already encoded into `frame`
  pub(crate) async fn send_frame(
    &mut self,
    conn: &MessageConn,
    frame: Vec<u8>,
  ) -> Result<(), ClientError> {
    let receipt = conn.send_with_receipt(frame);
    self.let_go();
    written(receipt?).await
  }

  /// [`Answering::send`] for a frame read from a stream as it is written
  pub(crate) async fn send_encoded(
    &mut self,
    conn: &MessageConn,
    frame: EncodedFrame,
  ) -> Result<(), ClientError> {
    let receipt = conn.send_encoded_with_receipt(frame);
    self.let_go();
    written(receipt?).await
  }

  /// lets go of the shared permit and the turn; the connection's own permit is kept until the
  /// handler ends, so a peer that does not read has no more answers pending than that bound
  fn let_go(&mut self) {
    if let Some(permit) = &mut self.permit {
      permit.release_shared();
    }
    self.turn = None;
  }
}

/// Why the application did not queue an upload a peer asked for.
enum Unqueued {
  /// refused, with the reason the peer is told
  Denied(String),
  /// not now; the peer is told the upload failed, so it asks again
  Later,
}

impl Client {
  /// A peer asks to queue a download of `filename`; it is told its place in the queue, why the
  /// application refused, or that the upload failed when the application cannot queue it now
  pub(super) async fn queue_requested(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    peer: &Peer,
    filename: RemoteFilename,
  ) -> Result<(), ClientError> {
    match self.try_queue_upload(peer, &filename, None).await {
      Ok(()) => {
        if self.options().send_place_when_queued {
          self
            .try_send_place_in_queue(conn, answering, peer, &filename)
            .await;
        }
        Ok(())
      }
      Err(Unqueued::Denied(reason)) => {
        let denied = PeerMessage::UploadDenied(UploadDenied { filename, reason });
        answering.send(conn, &[denied]).await
      }
      Err(Unqueued::Later) => {
        let failed = PeerMessage::UploadFailed(UploadFailed { filename });
        answering.send(conn, &[failed]).await
      }
    }
  }

  /// A peer asks for a file with the legacy transfer request. It is queued, never started at
  /// once: the answer is "Queued" and the place in the queue, the refusal, or "Queued" and the
  /// failure when the application cannot queue it now.
  pub(super) async fn download_requested(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    peer: &Peer,
    request: TransferRequest,
  ) -> Result<(), ClientError> {
    let (token, filename) = match request {
      TransferRequest::Download { token, filename }
      | TransferRequest::Upload {
        token, filename, ..
      } => (token, filename),
    };
    let answer = match self.try_queue_upload(peer, &filename, Some(token)).await {
      Ok(()) => {
        let queued = PeerMessage::TransferResponse(TransferResponse::Denied {
          token,
          message: "Queued".into(),
        });
        let place = if self.options().send_place_when_queued {
          self.resolve_place_in_queue(peer, &filename).await
        } else {
          None
        };
        std::iter::once(queued).chain(place).collect::<Vec<_>>()
      }
      Err(Unqueued::Denied(reason)) => {
        vec![PeerMessage::TransferResponse(TransferResponse::Denied {
          token,
          message: reason,
        })]
      }
      // queued, then failed: the peer waits for the upload and is told it failed, so it asks
      // again, where a denial would end its download
      Err(Unqueued::Later) => vec![
        PeerMessage::TransferResponse(TransferResponse::Denied {
          token,
          message: "Queued".into(),
        }),
        PeerMessage::UploadFailed(UploadFailed { filename }),
      ],
    };
    answering.send(conn, &answer).await
  }

  /// Asks the application to queue an upload; why it did not, as the peer is told, which never
  /// carries anything internal
  async fn try_queue_upload(
    &self,
    peer: &Peer,
    filename: &RemoteFilename,
    legacy_token: Option<u32>,
  ) -> Result<(), Unqueued> {
    let Some(enqueue) = self.options().resolvers.upload_request.clone() else {
      // with nothing to queue uploads through, nothing is shared
      tracing::debug!(%filename, "upload refused; no upload queue");
      return Err(Unqueued::Denied("File not shared.".into()));
    };
    let ask = UploadAsk {
      filename: filename.clone(),
      legacy_token,
    };
    match enqueue(peer.clone(), ask).await {
      Ok(UploadDecision::Queue) => Ok(()),
      Ok(UploadDecision::Deny(reason)) => {
        tracing::debug!(%filename, reason, "upload refused");
        Err(Unqueued::Denied(reason))
      }
      Ok(UploadDecision::Later) => {
        tracing::debug!(%filename, "upload not queued for now; the peer is told it failed");
        Err(Unqueued::Later)
      }
      // the application decided nothing, so the peer is not told it was refused
      Err(error) => {
        tracing::warn!(%filename, %error, "upload request resolver failed; the peer is told the upload failed");
        Err(Unqueued::Later)
      }
    }
  }

  /// tells the peer its place in the client's queue for `filename`, when the application knows it
  pub(super) async fn try_send_place_in_queue(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    peer: &Peer,
    filename: &RemoteFilename,
  ) {
    let Some(response) = self.resolve_place_in_queue(peer, filename).await else {
      return;
    };
    if let Err(error) = answering.send(conn, &[response]).await {
      tracing::debug!(%filename, %error, "place in queue not sent");
    }
  }

  /// the message telling the peer its place in the client's queue for `filename`, when the
  /// application knows it
  async fn resolve_place_in_queue(
    &self,
    peer: &Peer,
    filename: &RemoteFilename,
  ) -> Option<PeerMessage> {
    let resolve = self.options().resolvers.place_in_queue.clone()?;
    let place = match resolve(peer.clone(), filename.clone()).await {
      Ok(place) => place?,
      Err(error) => {
        tracing::warn!(%filename, %error, "Failed to resolve place in queue");
        return None;
      }
    };
    Some(PeerMessage::PlaceInQueueResponse(PlaceInQueueResponse {
      filename: filename.clone(),
      place,
    }))
  }

  pub(super) async fn send_browse_response(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    peer: &Peer,
  ) -> Result<(), ClientError> {
    let Some(resolve) = self.options().resolvers.browse_response.clone() else {
      return Ok(());
    };
    let reply = match resolve(peer.clone()).await {
      Ok(Some(reply)) => reply,
      Ok(None) => return Ok(()),
      Err(error) => {
        tracing::warn!(%error, "Failed to resolve browse response");
        return Ok(());
      }
    };
    match reply {
      BrowseReply::Response(response) => {
        answering
          .send(conn, &[PeerMessage::BrowseResponse(response)])
          .await?;
      }
      BrowseReply::Encoded(frame) => {
        answering.send_encoded(conn, frame).await?;
        tracing::debug!(code = PeerCode::BrowseResponse.code(), "peer message sent");
      }
    }
    tracing::debug!("Share contents sent");
    Ok(())
  }

  pub(super) async fn send_user_info(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    peer: &Peer,
  ) -> Result<(), ClientError> {
    let Some(resolve) = self.options().resolvers.user_info.clone() else {
      return Ok(());
    };
    let info = match resolve(peer.clone()).await {
      Ok(Some(info)) => info,
      Ok(None) => return Ok(()),
      Err(error) => {
        tracing::warn!(%error, "Failed to resolve user info");
        return Ok(());
      }
    };
    answering
      .send(conn, &[PeerMessage::InfoResponse(info)])
      .await?;
    tracing::debug!("User info sent");
    Ok(())
  }

  pub(super) async fn send_folder_contents(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    peer: &Peer,
    request: FolderContentsRequest,
  ) -> Result<(), ClientError> {
    let Some(resolve) = self.options().resolvers.folder_contents.clone() else {
      return Ok(());
    };
    let directories = match resolve(peer.clone(), request.token, request.directory_name.clone())
      .await
    {
      Ok(Some(directories)) => directories,
      Ok(None) => return Ok(()),
      Err(error) => {
        tracing::warn!(directory = %request.directory_name, %error, "Failed to resolve folder contents");
        return Ok(());
      }
    };
    let folder = PeerMessage::FolderContentsResponse(FolderContentsResponse {
      token: request.token,
      directory_name: request.directory_name,
      directories,
    });
    answering.send(conn, &[folder]).await?;
    tracing::debug!(token = request.token, "Folder contents sent");
    Ok(())
  }
}
