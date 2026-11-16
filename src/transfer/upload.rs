//! Uploading a file to a peer that asked for it. An upload first waits for the application's slot
//! policy, then the client offers the file to the peer; once it accepts the client opens the
//! transfer connection, the peer says where to start, and the client writes the rest of the file.
//! The peer closes the connection when it has every byte, and the client closes it itself after a
//! grace period; once every byte is written the upload stands however the connection ends. An
//! upload that does not succeed tells the peer, which queues the file again unless the client
//! canceled it or it let the file go; a peer that refused it is told nothing.

use std::io::SeekFrom;
use std::sync::Arc;

use soulseek_proto::peer::{TransferRequest, TransferResponse, UploadDenied, UploadFailed};
use soulseek_proto::{PeerCode, PeerMessage, RemoteFilename};
use tokio::io::AsyncSeekExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::io::{self, Linger, Pace};
use super::options::{
  Options, Slot, SlotGuard, TransferStateChanged, UploadInput, UploadNotice, UploadRequest,
  UploadSource,
};
use super::record::{Direction, Tracker, Transfer};
use super::request::{Handle, Upload};
use super::state::State;
use super::task::{self, Stop};
use super::{NewTransfer, Registration};
use crate::client::{Client, non_blank};
use crate::conn::{Conn, ConnHandle};
use crate::error::Error;
use crate::peers::write_peer;
use crate::waiter::WaitKey;

/// the peer of an upload, as a failure names it
const DOWNLOADER: &str = "the downloader";

impl Client {
  /// Uploads a file to a user.
  ///
  /// Returns the upload to set up and await, which gives its [`Handle`] once the upload is
  /// queued. The upload runs on a task of its own: it stops when the handle is canceled, not
  /// when the handle is dropped. Awaiting the handle returns the upload as it ended.
  ///
  /// # Errors
  ///
  /// The upload is not started in these cases, which awaiting the [`Upload`] returns.
  /// Returns [`Error::InvalidArgument`] if the
  /// username or the filename is empty or only whitespace, or if the path of a file input is
  /// empty. Returns [`Error::FileNotFound`] if a file input does not exist. Returns
  /// [`Error::Io`] if a file input cannot be looked for, opened for reading or measured.
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::DuplicateToken`] if another transfer uses the request's token. Returns
  /// [`Error::DuplicateTransfer`] if an upload of the file to the user is tracked
  /// already.
  ///
  /// An upload that starts and does not succeed returns the error that ended it, from its
  /// [`Handle`]. Returns [`Error::Canceled`] if the handle is canceled, if the client is shut
  /// down, which cancels every transfer, or if the slot wait answered
  /// [`Slot::Cancel`](crate::transfer::Slot::Cancel). Returns [`Error::Transfer`] with the
  /// reason if the slot wait answered [`Slot::Fail`](crate::transfer::Slot::Fail).
  ///
  /// Returns [`Error::TransferRejected`] if the peer refused the upload. Returns
  /// [`Error::Transfer`] if the downloader asked to start past the end of the file.
  /// Returns [`Error::Message`] if the peer's answer to the offer cannot be decoded, or
  /// if the start offset cannot be read from the transfer connection for a reason other than
  /// its inactivity or the downloader closing it.
  ///
  /// Returns [`Error::UserOffline`] if the server reports the user as offline,
  /// [`Error::Timeout`] if the server does not say in time where the user listens, and
  /// [`Error::UserAddress`] if the address lookup fails in any other way or the server
  /// gives a port that is out of range. Returns [`Error::Shutdown`] or
  /// [`Error::InvalidOperation`] if the client is found shut down or not logged in at the
  /// lookup or when it starts connecting to the user.
  ///
  /// Returns [`Error::Timeout`] if the peer does not answer the offer within
  /// [`Config::transfer_response_timeout`](crate::Config::transfer_response_timeout),
  /// or if the transfer connection goes its inactivity timeout without a byte. Returns
  /// [`Error::Timeout`] if neither a direct connection to the user nor one through the
  /// server is made in time, for messages or for the transfer, or if the upload waits on a dial
  /// the client makes because the user asked the server for a connection and the dial is not
  /// answered in time.
  ///
  /// Returns [`Error::Connection`] if no message connection to the user can be made for
  /// another reason, if the offer cannot be written on it, if it closes before the peer answers
  /// the offer, if no transfer connection can be made for another reason, or if the transfer
  /// connection fails before every byte is written, with the kind
  /// [`crate::error::ConnectionKind::RemoteClosed`] when the downloader closed or reset it, before or
  /// after it sent the start offset. Returns
  /// [`Error::InvalidOperation`] if a connection closes after it was made and before the
  /// upload used it.
  ///
  /// Returns [`Error::Io`] if a file input cannot be opened, measured or positioned, if a
  /// stream input cannot be opened, or if reading the input fails. Returns
  /// [`Error::Transfer`] if a file input changed size since the upload was started, if a
  /// stream input cannot seek to the start offset, or if the input ends before every byte was
  /// read. Returns [`Error::Operation`] if the task the upload runs on panics.
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use soulseek_client::{Client, Error};
  ///
  /// # async fn run(client: Client) -> Result<(), Error> {
  /// let upload = client
  ///   .upload("username", "@@music\\album\\song.flac")
  ///   .from_file("/srv/music/album/song.flac")
  ///   .await?;
  /// let transfer = upload.await?;
  /// println!("uploaded {} bytes", transfer.bytes_transferred);
  /// # Ok(())
  /// # }
  /// ```
  pub fn upload(
    &self,
    username: impl Into<String>,
    filename: impl Into<RemoteFilename>,
  ) -> Upload<'_> {
    Upload::new(self, username.into(), filename.into())
  }

  /// starts an upload and returns its handle once it is queued, or it is past that
  pub(crate) async fn enqueue_upload(&self, request: UploadRequest) -> Result<Handle, Error> {
    // a caller that stops waiting before it has the handle has no other way to stop the
    // transfer, so it is canceled then
    let cancel = CancellationToken::new();
    let unhandled = cancel.clone().drop_guard();
    let (observer, reached) = task::observer(State::QueuedLocally);
    let (task, latest) = self.start_upload(request, &cancel, observer).await?;
    let task = task::enqueued(task, reached).await?;
    unhandled.disarm();
    Ok(Handle::new(task, cancel, latest))
  }

  async fn start_upload(
    &self,
    request: UploadRequest,
    cancel: &CancellationToken,
    observer: TransferStateChanged,
  ) -> Result<
    (
      JoinHandle<Result<Transfer, Error>>,
      watch::Receiver<Transfer>,
    ),
    Error,
  > {
    let UploadRequest {
      username,
      filename,
      input,
      token,
      options,
    } = request;
    non_blank("username", &username)?;
    non_blank("filename", filename.as_str())?;
    if let UploadInput::File(path) = &input {
      if path.as_os_str().is_empty() {
        return Err(Error::InvalidArgument(
          "the local filename must not be empty".into(),
        ));
      }
      let exists = tokio::fs::try_exists(path).await.map_err(|error| {
        Error::io(
          format!("failed to look for {}: {error}", path.display()),
          error,
        )
      })?;
      if !exists {
        return Err(Error::FileNotFound(path.display().to_string()));
      }
    }
    self.require_logged_in(&format!("upload {filename} to {username}"))?;
    let size = match &input {
      UploadInput::File(path) => file_size(path).await?,
      UploadInput::Stream { size, .. } => *size,
    };
    let (registration, _) = self.transfer_registry().register(
      self.tokens(),
      token,
      NewTransfer {
        direction: Direction::Upload,
        username: &username,
        filename: &filename,
        size,
        start_offset: 0,
        cancel,
      },
    )?;
    let (tracker, latest) = Tracker::followed(self.clone(), registration.record.clone(), observer);
    let token = tracker.snapshot().token;
    let span = tracing::info_span!("transfer", direction = "upload", username, token, %filename);
    let upload = Uploading {
      client: self.clone(),
      tracker,
      stop: Stop::new(registration.cancel.clone(), None),
      username,
      filename,
      token,
      size,
      input: Some(input),
      options,
      registration: Some(registration),
    };
    Ok((tokio::spawn(upload.run().instrument(span)), latest))
  }

  /// Sends `username` the `message` that says an upload to them ended. Best effort and bounded;
  /// skipped while the client disconnects or once it is shut down, but not because the server
  /// session was lost.
  pub(crate) async fn tell_upload_ended(&self, username: &str, message: PeerMessage) {
    if self.state() == crate::session::State::Disconnecting || self.cancellation().is_cancelled() {
      tracing::debug!(
        "the client is disconnecting or shut down; the peer is not told the upload ended"
      );
      return;
    }
    let tell = async {
      // the address is looked up only when there is no connection to the peer to reuse
      let conn = self
        .message_connection(username, None, &CancellationToken::new())
        .await?;
      write_peer(&conn, message).await
    };
    match tokio::time::timeout(self.options().upload_notice_timeout, tell).await {
      Ok(Ok(())) => tracing::debug!("the peer was told the upload ended"),
      Ok(Err(error)) => tracing::debug!(%error, "the peer could not be told the upload ended"),
      Err(_) => tracing::debug!("telling the peer the upload ended took too long; given up"),
    }
  }
}

/// Measures a file to upload, opening it the way the upload will, so a file that cannot be read
/// fails now
async fn file_size(path: &std::path::Path) -> Result<u64, Error> {
  let unreadable = |error: std::io::Error| {
    Error::io(
      format!(
        "the local file {} could not be opened for reading: {error}",
        path.display()
      ),
      error,
    )
  };
  let file = tokio::fs::File::open(path).await.map_err(unreadable)?;
  Ok(file.metadata().await.map_err(unreadable)?.len())
}

/// One upload's task
struct Uploading {
  client: Client,
  tracker: Tracker,
  stop: Stop,
  username: String,
  filename: RemoteFilename,
  token: u32,
  size: u64,
  input: Option<UploadInput>,
  options: Options,
  /// keeps the upload tracked until its cleanup lets go of it
  registration: Option<Registration>,
}

/// What an upload waited for and holds until its cleanup gives it back, in order
#[derive(Default)]
struct Held {
  /// what the application's slot policy gave when it admitted the upload
  slot: Option<SlotGuard>,
}

impl Uploading {
  async fn run(mut self) -> Result<Transfer, Error> {
    let mut held = Held::default();
    let result = self.transfer(&mut held).await;
    let outcome = task::finish(&self.tracker, result);
    match &outcome {
      Ok(transfer) => tracing::info!(
        bytes = transfer.bytes_transferred,
        average_speed = transfer.average_speed,
        "upload completed"
      ),
      Err(error) => tracing::info!(state = %self.tracker.state(), %error, "upload ended"),
    }
    self.clean_up(held, &outcome);
    outcome
  }

  async fn transfer(&mut self, held: &mut Held) -> Result<(), Error> {
    self.tracker.emit_first_state();
    held.slot = self.await_slot().await?;
    let cancel = self.stop.cancel().clone();
    let address = self
      .stop
      .run(self.client.users().address(&self.username))
      .await?;
    let conn = self
      .stop
      .run(
        self
          .client
          .message_connection(&self.username, Some(address), &cancel),
      )
      .await?;
    let answer = self.client.waiter().wait::<TransferResponse>(
      WaitKey::PeerToken {
        code: PeerCode::TransferResponse,
        username: self.username.clone(),
        token: self.token,
      },
      Some(self.client.options().transfer_response_timeout),
      cancel.clone(),
    );
    let offer = PeerMessage::TransferRequest(TransferRequest::Upload {
      token: self.token,
      filename: self.filename.clone(),
      file_size: Some(self.size),
    });
    self.stop.run(write_peer(&conn, offer)).await?;
    self.tracker.emit_state(State::Requested, None)?;
    match self.stop.run(answer).await? {
      // a downloader may not queue the uploader, so its "Queued" is a refusal too
      TransferResponse::Denied { message, .. } => {
        return Err(Error::TransferRejected(message));
      }
      TransferResponse::Allowed { .. } => {}
    }
    self.tracker.emit_state(State::Initializing, None)?;
    let conn = self
      .stop
      .run(
        self
          .client
          .get_transfer_connection(&self.username, address, self.token, &cancel),
      )
      .await?;
    self.send(conn).await
  }

  /// waits for the application's slot policy to admit the upload
  async fn await_slot(&mut self) -> Result<Option<SlotGuard>, Error> {
    let Some(awaiter) = self.options.slot_awaiter.clone() else {
      return Ok(None);
    };
    let transfer = self.tracker.snapshot();
    let slot = awaiter(&transfer);
    match self.stop.run(async { Ok(slot.await) }).await? {
      Slot::Granted(guard) => Ok(Some(guard)),
      Slot::Cancel => Err(Error::Canceled),
      Slot::Fail(reason) => Err(Error::Transfer {
        message: format!(
          "failed to acquire an upload slot for {} to {}: {reason}",
          self.filename, self.username
        ),
        source: None,
      }),
    }
  }

  /// Reads where the downloader wants to start, writes the rest of the file and waits for the
  /// downloader to close the connection
  async fn send(&mut self, conn: Conn) -> Result<(), Error> {
    let handle = conn.handle().clone();
    self.tracker.set_address(handle.remote());
    let result = self.send_on(conn, &handle).await;
    // the downloader or the linger may have closed it already; the first reason given stands
    match &result {
      Ok(()) => handle.disconnect("transfer complete", None),
      Err(Error::Canceled) => handle.disconnect("transfer canceled", None),
      Err(error) => handle.disconnect(error.to_string(), Some(error.clone())),
    };
    result
  }

  async fn send_on(&mut self, conn: Conn, handle: &ConnHandle) -> Result<(), Error> {
    let (mut read, mut write) = conn.into_split()?;
    let mut offset = [0; 8];
    self.stop.run(read.read_exact(&mut offset)).await.map_err(
      |error| match io::connection_failure(handle, error, DOWNLOADER) {
        // a downloader that closes before sending its offset left, as one that closes mid-file did
        error @ (Error::Timeout(_)
        | Error::Canceled
        | Error::Connection {
          kind: crate::error::ConnectionKind::RemoteClosed,
          ..
        }) => error,
        error => Error::Message {
          kind: crate::error::MessageKind::Read,
          message: "failed to read transfer start offset".into(),
          source: Some(Arc::new(error)),
        },
      },
    )?;
    let start = self.check_offset(u64::from_le_bytes(offset))?;
    self.tracker.set_start_offset(start);
    let mut source = self.open_input(start).await?;
    self.tracker.emit_state(State::InProgress, None)?;
    self.tracker.emit_progress(start);
    tracing::info!(size = self.size, start, "upload started");
    let cancel = self.stop.cancel().clone();
    let pace = Pace {
      tracker: &self.tracker,
      governor: self.options.governor.clone(),
      reporter: self.options.reporter.clone(),
      cancel: &cancel,
    };
    // a peer that already has the whole file asks from its end, gets nothing more and closes
    let len = self.size - start;
    let sent = async {
      if len == 0 {
        return Ok(());
      }
      tokio::select! {
        // the last chunk written counts even when the peer closes right after reading it
        biased;
        sent = io::send(&mut write, source.as_mut(), len, start, &pace) => sent,
        closed = io::closed(&mut read) => Err(closed),
      }
    };
    self
      .stop
      .run(sent)
      .await
      .map_err(|error| io::connection_failure(handle, error, DOWNLOADER))?;
    drop(source);
    // every byte was written, so the upload counts as delivered
    let ended = io::linger(&mut read, self.options.maximum_linger_time, &cancel).await;
    if ended == Linger::Canceled {
      tracing::debug!("canceled after the last byte; the upload stands");
    }
    self.tracker.set_closed_by_peer(ended == Linger::Closed);
    Ok(())
  }

  /// The start offset the downloader asked for, unless it is past the end of the file. Soulseek
  /// NS sends the largest offset there is when it resumes a file over 2 GiB; that is refused the
  /// same way, logged once per peer, and the peer queues the file again.
  fn check_offset(&self, start: u64) -> Result<u64, Error> {
    if start == u64::MAX
      && self
        .client
        .transfer_registry()
        .first_bad_offset_from(&self.username)
    {
      tracing::info!(
        username = self.username,
        "peer sent an invalid resume offset (NS > 2 GiB bug); rejecting, the peer will re-queue"
      );
    }
    if start > self.size {
      return Err(Error::transfer(format!(
        "requested start offset of {start} bytes exceeds file length of {} bytes",
        self.size
      )));
    }
    Ok(start)
  }

  /// Opens the source at `start`. A file is measured again: one whose size changed since the
  /// upload was queued fails it, and the peer queues the file again with the new size.
  async fn open_input(&mut self, start: u64) -> Result<Box<dyn UploadSource>, Error> {
    let input = self
      .input
      .take()
      .expect("the input is opened once per upload");
    match input {
      UploadInput::File(path) => {
        let failed = |error: std::io::Error| {
          Error::io(format!("failed to open {}: {error}", path.display()), error)
        };
        let mut file = tokio::fs::File::open(&path).await.map_err(failed)?;
        let len = file.metadata().await.map_err(failed)?.len();
        if len != self.size {
          return Err(Error::transfer(format!(
            "file changed size since it was enqueued ({} bytes then, {len} bytes now)",
            self.size
          )));
        }
        file.seek(SeekFrom::Start(start)).await.map_err(failed)?;
        Ok(Box::new(file))
      }
      UploadInput::Stream { open, .. } => {
        let mut source = open(start)
          .await
          .map_err(|error| Error::io(format!("failed to open the input: {error}"), error))?;
        if start > 0 && self.options.seek_input_stream_automatically {
          source
            .seek(SeekFrom::Start(start))
            .await
            .map_err(|error| Error::Transfer {
              message: format!(
                "requested non-zero start offset but input stream does not support seeking: {error}"
              ),
              source: Some(Arc::new(error)),
            })?;
        }
        Ok(source)
      }
    }
  }

  /// Lets go of everything the upload held, in order. The record goes first, so a peer that
  /// queues the file again as soon as it hears of the failure is not refused as a duplicate.
  /// The slot is handed back before the peer is told, which happens on a task of its own, so
  /// a peer that is slow to reach holds up neither the slot nor the caller.
  fn clean_up(&mut self, held: Held, outcome: &Result<Transfer, Error>) {
    drop(self.registration.take());
    drop(held.slot);
    if let Err(error) = outcome
      && let Some(message) = self.ended_notice(error)
    {
      let client = self.client.clone();
      let username = self.username.clone();
      tokio::spawn(
        async move { client.tell_upload_ended(&username, message).await }.in_current_span(),
      );
    }
  }

  /// What the peer is told about an upload that did not succeed. The upload's options decide;
  /// without them, that the client canceled it, so it drops the file, or that it failed, so it
  /// queues the file again. A peer that refused the offer is told nothing. A peer whose side
  /// closed or reset the transfer connection is told it failed too: the socket does not tell the
  /// network's reset from the peer's, and a peer that let the file go ignores it.
  fn ended_notice(&self, error: &Error) -> Option<PeerMessage> {
    // an offline peer cannot hear it, and one that refused the file has said it does not want it
    if matches!(error, Error::UserOffline(_) | Error::TransferRejected(_)) {
      return None;
    }
    let notice = match (&self.options.upload_ended, error) {
      (Some(decide), error) => decide(&self.tracker.snapshot(), error),
      (None, Error::Canceled) => UploadNotice::Denied("Cancelled".into()),
      (None, _) => UploadNotice::Failed,
    };
    let filename = self.filename.clone();
    match notice {
      UploadNotice::Denied(reason) => {
        Some(PeerMessage::UploadDenied(UploadDenied { filename, reason }))
      }
      UploadNotice::Failed => Some(PeerMessage::UploadFailed(UploadFailed { filename })),
      UploadNotice::Nothing => None,
    }
  }
}
