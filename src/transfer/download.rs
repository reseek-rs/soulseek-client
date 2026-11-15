//! Downloading a file from a peer. The client asks for it, by default with `QueueDownload`, and
//! the peer queues it; when the file reaches the front of the peer's queue the peer offers to
//! send it, the client accepts, and the peer connects to the client (or the client to it, when
//! it cannot) with a transfer connection. The client says where to start, reads the rest of the
//! file and closes the connection, which tells the peer the client has it all. The peer can
//! refuse or fail the download at any point before that. A file the peer offered unasked starts
//! at its offer, which the client holds.

use std::future::Future;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::pin::Pin;

use soulseek_proto::peer::{QueueDownload, TransferRequest, TransferResponse};
use soulseek_proto::{PeerCode, PeerMessage, RemoteFilename};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::io::{self, Pace};
use super::options::{
  DownloadOutput, DownloadRequest, DownloadSink, Options, RequestStyle, TransferStateChanged,
};
use super::record::{Direction, Tracker, Transfer};
use super::remote::{offer_key, offered_size};
use super::request::{Download, Handle};
use super::size::resolve_size;
use super::state::State;
use super::task::{self, Stop};
use super::{NewTransfer, Registration, RequestTurn};
use crate::client::{Client, non_blank};
use crate::conn::{Conn, ConnHandle, MessageConn};
use crate::error::Error;
use crate::peers::write_peer;
use crate::waiter::WaitKey;

/// the peer of a download, as a failure names it
const UPLOADER: &str = "the uploader";

/// the wait for the uploader's offer to send the file
type OfferWait = Pin<Box<dyn Future<Output = Result<TransferRequest, Error>> + Send>>;

impl Client {
  /// Downloads a file from a user.
  ///
  /// Returns the download to set up and await, which gives its [`Handle`] once the uploader
  /// has the request. The download runs on a task of its own: it stops when the handle is
  /// canceled, not when the handle is dropped. Awaiting the handle returns the download as it
  /// ended.
  ///
  /// # Errors
  ///
  /// The download is not started in these cases, which awaiting the [`Download`] returns.
  /// Returns [`Error::InvalidArgument`] if
  /// the username or the filename is empty or only whitespace, if the path of a file output is
  /// empty, if the start offset is beyond the size, or if a file output holds fewer bytes than
  /// the start offset. Returns [`Error::Shutdown`] if the client has been shut down.
  /// Returns [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::Io`] if the length of a file output cannot be read. Returns
  /// [`Error::DuplicateToken`] if another transfer uses the request's token. Returns
  /// [`Error::DuplicateTransfer`] if a download of the file from the user is tracked
  /// already.
  ///
  /// A download that starts and does not succeed returns the error that ended it, from the
  /// [`Download`] if the uploader did not have the request yet and from the [`Handle`] after
  /// that. Returns [`Error::Canceled`] if the handle is canceled, or if the client is shut
  /// down, which cancels
  /// every transfer. Returns [`Error::InvalidOperation`] if the request style is
  /// [`RequestStyle::Offered`] and no offer of the file from the user is held.
  ///
  /// Returns [`Error::TransferRejected`] if the peer refused the download. Returns
  /// [`Error::TransferFailed`] if the uploader reported that the upload failed.
  /// Returns [`Error::SizeMismatch`] if the peer reported a different size and nothing
  /// settled which is right. Returns [`Error::Transfer`] if the request named no size and
  /// the uploader names none or one below the start offset, or if a stream output cannot seek
  /// to the start offset. Returns [`Error::Message`] if the uploader's offer, refusal or
  /// failure report of the file, or its answer to a transfer request, cannot be decoded.
  ///
  /// The user's address is looked up when the download asks for the file, and again when it
  /// takes the uploader's offer if no message connection to the user is open then. Returns [`Error::UserOffline`] if the server reports the
  /// user as offline, [`Error::Timeout`] if the server does not answer in time, and
  /// [`Error::UserEndpoint`] if the lookup fails in any other way or the server gives a
  /// port that is out of range. Returns [`Error::Shutdown`] or
  /// [`Error::InvalidOperation`] if the client is found shut down or not logged in at a
  /// lookup or when it starts connecting to the user.
  ///
  /// Returns [`Error::Timeout`] if the uploader does not offer the file within
  /// [`Download::queued_timeout`] of being asked for it, if the peer does not answer a
  /// transfer request within
  /// [`Config::transfer_response_timeout`](crate::Config::transfer_response_timeout),
  /// if the uploader opens no transfer connection within
  /// [`Config::transfer_accept_timeout`](crate::Config::transfer_accept_timeout),
  /// or if the transfer connection goes its inactivity timeout without a byte. Returns
  /// [`Error::Timeout`] if neither a direct connection to the user nor one through the
  /// server is made in time, for messages or for the transfer, or if the download waits on a
  /// dial the client makes because the user asked the server for a connection and the dial is
  /// not answered in time.
  ///
  /// Returns [`Error::Connection`] if no message connection to the user can be made for
  /// another reason, if the request or the acceptance of the offer cannot be written on it, if
  /// it closes before the peer answers a transfer request, if no transfer connection can be
  /// made for another reason, or if the transfer connection fails before every byte arrived,
  /// with the kind
  /// [`ConnectionErrorKind::RemoteClosed`](crate::ConnectionErrorKind::RemoteClosed) when the
  /// uploader closed or reset it. Returns [`Error::InvalidOperation`] if a connection
  /// closes after it was made and before the download wrote on it.
  ///
  /// Returns [`Error::Io`] if a file output cannot be opened, measured, truncated or
  /// positioned, if a stream output cannot be opened, or if writing or flushing the output
  /// fails. Returns [`Error::InvalidArgument`] if a file output holds fewer bytes than
  /// the start offset when it is opened. Returns [`Error::Operation`] if the task the
  /// download runs on panics.
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use soulseek_client::{Client, Error};
  ///
  /// # async fn run(client: Client) -> Result<(), Error> {
  /// // the filename and size as a search response gave them
  /// let download = client
  ///   .download("username", "@@music\\album\\song.flac")
  ///   .size(31_457_280)
  ///   .to_file("song.flac")
  ///   .await?;
  /// let transfer = download.await?;
  /// println!("downloaded {} bytes", transfer.bytes_transferred);
  /// # Ok(())
  /// # }
  /// ```
  pub fn download(
    &self,
    username: impl Into<String>,
    filename: impl Into<RemoteFilename>,
  ) -> Download<'_> {
    Download::new(self, username.into(), filename.into())
  }

  /// starts a download and returns its handle once the peer has queued it, or it is past that
  pub(crate) async fn enqueue_download(&self, request: DownloadRequest) -> Result<Handle, Error> {
    // a caller that stops waiting before it has the handle has no other way to stop the
    // transfer, so it is canceled then
    let cancel = CancellationToken::new();
    let unhandled = cancel.clone().drop_guard();
    let (observer, reached) = task::observer(State::QueuedRemotely);
    let (task, latest) = self.start_download(request, &cancel, observer).await?;
    let task = task::enqueued(task, reached).await?;
    unhandled.disarm();
    Ok(Handle::new(task, cancel, latest))
  }

  async fn start_download(
    &self,
    request: DownloadRequest,
    cancel: &CancellationToken,
    observer: TransferStateChanged,
  ) -> Result<
    (
      JoinHandle<Result<Transfer, Error>>,
      watch::Receiver<Transfer>,
    ),
    Error,
  > {
    let DownloadRequest {
      username,
      filename,
      size,
      start_offset,
      token,
      output,
      mut options,
    } = request;
    non_blank("username", &username)?;
    non_blank("filename", filename.as_str())?;
    if let DownloadOutput::File(path) = &output {
      if path.as_os_str().is_empty() {
        return Err(Error::InvalidArgument(
          "the local filename must not be empty".into(),
        ));
      }
      // the file is closed whatever way the download ends
      options.close_output_stream_on_completion = true;
    }
    if let Some(size) = size
      && start_offset > size
    {
      return Err(Error::InvalidArgument(format!(
        "the start offset {start_offset} is beyond the size {size}"
      )));
    }
    self.require_logged_in(&format!("download {filename} from {username}"))?;
    if let DownloadOutput::File(path) = &output {
      check_resumable(path, start_offset).await?;
    }
    // A download that asks for the file waits for its offer from before it is tracked: an
    // offer that finds the download tracked finds the wait too, even one the uploader sends
    // while the client still connects, the file being in its queue from an earlier request. Its
    // timeout counts from the request.
    let offered = (options.request_style != RequestStyle::Offered).then(|| {
      Box::pin(self.waiter().wait_from_first_poll::<TransferRequest>(
        offer_key(&username, &filename),
        options.queued_timeout,
        cancel.clone(),
      )) as OfferWait
    });
    let (registration, remote_failure) = self.transfer_registry().register(
      self.tokens(),
      token,
      NewTransfer {
        direction: Direction::Download,
        username: &username,
        filename: &filename,
        // 0 until the uploader names the size of a download that expects none
        size: size.unwrap_or(0),
        start_offset,
        cancel,
      },
    )?;
    let request_turn = offered
      .is_some()
      .then(|| self.transfer_registry().request_turn(&username));
    let (tracker, latest) = Tracker::followed(self.clone(), registration.record.clone(), observer);
    let token = tracker.snapshot().token;
    let span = tracing::info_span!("transfer", direction = "download", username, token, %filename);
    let download = Downloading {
      client: self.clone(),
      tracker,
      stop: Stop::new(registration.cancel.clone(), remote_failure),
      username,
      filename,
      token,
      start_offset,
      expects_size: size.is_some(),
      output: Some(output),
      options,
      offered,
      request_turn,
      _registration: registration,
    };
    Ok((tokio::spawn(download.run().instrument(span)), latest))
  }
}

/// Whether `error` is `conn` closing under a write or a wait on it: a write's only refusals of
/// this kind are the connection's not being connected or its closing, and a connection error is
/// a write or a wait the close cut
fn closed_under(error: &Error, conn: &MessageConn) -> bool {
  matches!(error, Error::InvalidOperation(_) | Error::Connection { .. }) && !conn.handle().is_open()
}

/// A resumed download needs the file to hold at least the bytes before the start offset; one
/// that holds fewer was changed by something else since the offset was taken from it
async fn check_resumable(path: &std::path::Path, start_offset: u64) -> Result<(), Error> {
  if start_offset == 0 {
    return Ok(());
  }
  let len = match tokio::fs::metadata(path).await {
    Ok(metadata) => metadata.len(),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
    Err(error) => {
      return Err(Error::io(
        format!("failed to read the length of {}: {error}", path.display()),
        error,
      ));
    }
  };
  if len < start_offset {
    return Err(Error::InvalidArgument(
      "local file is shorter than the start offset".into(),
    ));
  }
  Ok(())
}

/// One download's task
struct Downloading {
  client: Client,
  tracker: Tracker,
  stop: Stop,
  username: String,
  filename: RemoteFilename,
  token: u32,
  start_offset: u64,
  /// whether the request named a size to check the uploader's against
  expects_size: bool,
  output: Option<DownloadOutput>,
  options: Options,
  /// the wait for the uploader's offer, for a download that asks for the file
  offered: Option<OfferWait>,
  /// the download's place among those from the same user that have yet to ask, until it asks
  request_turn: Option<RequestTurn>,
  /// keeps the download tracked until the task ends
  _registration: Registration,
}

impl Downloading {
  async fn run(mut self) -> Result<Transfer, Error> {
    let result = self.transfer().await;
    let outcome = task::finish(&self.tracker, result);
    match &outcome {
      Ok(transfer) => tracing::info!(
        bytes = transfer.bytes_transferred,
        average_speed = transfer.average_speed,
        "download completed"
      ),
      Err(error) => tracing::info!(state = %self.tracker.state(), %error, "download ended"),
    }
    outcome
  }

  async fn transfer(&mut self) -> Result<(), Error> {
    self.tracker.emit_first_state();
    if self.options.request_style == RequestStyle::Offered {
      let offer = self
        .client
        .transfer_registry()
        .take_offer(&self.username, &self.filename)
        .ok_or_else(|| {
          Error::InvalidOperation(format!(
            "no offer of {} from {} is held",
            self.filename, self.username
          ))
        })?;
      return self.take(offer).await;
    }
    // The user's downloads connect side by side and ask in the order they started.
    let turn = self
      .request_turn
      .take()
      .expect("a download that asks for the file holds its turn until it does");
    let cancel = self.stop.cancel().clone();
    let endpoint = self
      .stop
      .run(self.client.users().address(&self.username))
      .await?;
    let conn = self.message_connection(Some(endpoint)).await?;
    self
      .stop
      .run(async {
        turn.reached().await;
        Ok(())
      })
      .await?;
    let mut offered = self
      .offered
      .take()
      .expect("a download that asks for the file waits for its offer");
    // an offer that came while the download waited or connected is taken without asking for the
    // file
    let early = tokio::select! {
      biased;
      offer = &mut offered => Some(offer),
      () = std::future::ready(()) => None,
    };
    if let Some(offer) = early {
      drop(turn);
      tracing::debug!("offered by the peer before it was asked");
      return self.take(offer?).await;
    }
    self.client.transfer_registry().asked(self.token);
    match self.options.request_style {
      RequestStyle::Queue => {
        let request = PeerMessage::QueueDownload(QueueDownload {
          filename: self.filename.clone(),
        });
        self.request(&conn, Some(endpoint), request, || {}).await?;
        drop(turn);
        self.tracker.emit_state(State::Requested, None)?;
        // the peer does not answer this request; refusing it later is all it can do
        self.queued()?;
      }
      RequestStyle::TransferRequest => {
        if let Some(size) = self.ask(&conn, endpoint, turn).await? {
          // the peer sends at once, and expects the client to connect with the client's token
          self.tracker.emit_state(State::Initializing, None)?;
          let conn = self
            .stop
            .run(
              self
                .client
                .get_transfer_connection(&self.username, endpoint, self.token, &cancel),
            )
            .await?;
          return self.receive(conn, size).await;
        }
        self.queued()?;
      }
      RequestStyle::Offered => unreachable!("an offered download takes its offer above"),
    }
    let offer = self.stop.run(offered).await?;
    self.take(offer).await
  }

  /// Takes the peer's offer to send the file: accepts it and reads the file
  async fn take(&mut self, offer: TransferRequest) -> Result<(), Error> {
    self.client.transfer_registry().asked(self.token);
    let size = self.resolve_size(offered_size(&offer))?;
    self.tracker.set_remote_token(offer.token());
    self.tracker.emit_state(State::Initializing, None)?;
    tracing::info!(remote_token = offer.token(), "download offered by the peer");
    let conn = self.accept(offer.token()).await?;
    self.receive(conn, size).await
  }

  /// the peer has the request in its queue
  fn queued(&self) -> Result<(), Error> {
    self.tracker.emit_state(State::QueuedRemotely, None)?;
    tracing::info!("download queued by the peer");
    Ok(())
  }

  /// Writes `request`, a request for the file or the answer to the peer's offer, to the peer on
  /// `conn`, and returns the connection it went on. A cached connection that
  /// closed, or began closing after its queued writes, between being taken and the write is no
  /// fault of the peer's: the message goes once more, on the current or a new connection, after
  /// `renew` has made again what the close undid, and a failure there stands.
  async fn request(
    &mut self,
    conn: &MessageConn,
    endpoint: Option<SocketAddr>,
    request: PeerMessage,
    renew: impl FnOnce(),
  ) -> Result<MessageConn, Error> {
    let error = match self.stop.run(write_peer(conn, request.clone())).await {
      Ok(()) => return Ok(conn.clone()),
      Err(error) => error,
    };
    if !closed_under(&error, conn) {
      return Err(error);
    }
    tracing::debug!(%error, "the message connection closed under the message; sending it on a new one");
    // a closed connection's entry left the cache in the same step as its user's waits failed, and
    // a closing one was replaced, whose close fails none; what renew makes is not failed by it
    let fresh = self.message_connection(endpoint).await?;
    renew();
    self.stop.run(write_peer(&fresh, request)).await?;
    Ok(fresh)
  }

  /// the cached message connection to the peer, or a new one to `endpoint`, which the server is
  /// asked for when none is given
  async fn message_connection(
    &mut self,
    endpoint: Option<SocketAddr>,
  ) -> Result<MessageConn, Error> {
    let cancel = self.stop.cancel().clone();
    self
      .stop
      .run(
        self
          .client
          .message_connection(&self.username, endpoint, &cancel),
      )
      .await
  }

  /// Asks with a download `TransferRequest`, leaving `turn` once it is written, and waits for the
  /// answer: `Some(size)` when the peer sends at once, `None` when it queued the request
  async fn ask(
    &mut self,
    conn: &MessageConn,
    endpoint: SocketAddr,
    turn: RequestTurn,
  ) -> Result<Option<u64>, Error> {
    // registered before the request is written, and again before it is written anew, since a
    // closed connection fails the waits on its user's replies
    let (client, username, token) = (self.client.clone(), self.username.clone(), self.token);
    let cancel = self.stop.cancel().clone();
    let answer_wait = || {
      client.waiter().wait::<TransferResponse>(
        WaitKey::PeerToken {
          code: PeerCode::TransferResponse,
          username: username.clone(),
          token,
        },
        Some(client.options().transfer_response_timeout),
        cancel.clone(),
      )
    };
    let mut answer = answer_wait();
    let request = PeerMessage::TransferRequest(TransferRequest::Download {
      token: self.token,
      filename: self.filename.clone(),
    });
    let written = self
      .request(conn, Some(endpoint), request.clone(), || {
        answer = answer_wait()
      })
      .await?;
    drop(turn);
    self.tracker.emit_state(State::Requested, None)?;
    let answered = match self.stop.run(answer).await {
      // the connection closed after the request was written and before the answer, so the
      // peer may never have read it: it goes once more on a new connection, and a failure
      // there stands
      Err(error) if closed_under(&error, &written) => {
        tracing::debug!(%error, "the message connection closed before the answer; asking on a new one");
        let fresh = self.message_connection(Some(endpoint)).await?;
        let answer = answer_wait();
        self.stop.run(write_peer(&fresh, request)).await?;
        self.stop.run(answer).await?
      }
      answered => answered?,
    };
    match answered {
      TransferResponse::Denied { message, .. } if is_queued(&message) => Ok(None),
      TransferResponse::Denied { message, .. } => Err(Error::TransferRejected(message)),
      TransferResponse::Allowed { file_size, .. } => {
        let size = self.resolve_size(file_size)?;
        // passed through so every download that starts was queued at the peer first
        self.tracker.emit_state(State::QueuedRemotely, None)?;
        Ok(Some(size))
      }
    }
  }

  /// Accepts the peer's offer and waits for its transfer connection, which it opens to the client
  /// or has the server ask the client to dial
  async fn accept(&mut self, remote_token: u32) -> Result<Conn, Error> {
    // the message connection still open carries the answer, with or without a server session.
    // One that closed during a long wait in the queue is made again where the server now says
    // the user is
    let conn = self.message_connection(None).await?;
    let cancel = self.stop.cancel().clone();
    let incoming = self.client.await_transfer_connection(
      &self.username,
      self.filename.clone(),
      remote_token,
      &cancel,
    );
    let accepted = PeerMessage::TransferResponse(TransferResponse::Allowed {
      token: remote_token,
      file_size: None,
    });
    // the wait for its transfer connection is not one a closed message connection fails
    self.request(&conn, None, accepted, || {}).await?;
    self.stop.run(incoming).await
  }

  /// the size to download once the peer named one
  fn resolve_size(&self, remote: Option<u64>) -> Result<u64, Error> {
    let snapshot = self.tracker.snapshot();
    let size = if self.expects_size {
      resolve_size(
        &snapshot,
        remote,
        self.options.size_mismatch_resolver.as_ref(),
      )?
    } else {
      let size = remote.ok_or_else(|| {
        Error::transfer("the uploader named no size and the download expects none")
      })?;
      if size < self.start_offset {
        return Err(Error::transfer(format!(
          "the start offset {} is beyond the uploader's size {size}",
          self.start_offset
        )));
      }
      size
    };
    if size != snapshot.size {
      tracing::debug!(
        local = snapshot.size,
        ?remote,
        size,
        "download size settled"
      );
      self.tracker.set_size(size);
    }
    Ok(size)
  }

  /// Reads the file from the transfer connection, from the start offset on, and closes the
  /// connection once every byte is in
  async fn receive(&mut self, conn: Conn, size: u64) -> Result<(), Error> {
    let handle = conn.handle().clone();
    self.tracker.set_endpoint(handle.remote());
    let result = self.receive_on(conn, size, &handle).await;
    match &result {
      Ok(()) => handle.disconnect("transfer complete", None),
      Err(Error::Canceled) => handle.disconnect("transfer canceled", None),
      Err(error) => handle.disconnect(error.to_string(), Some(error.clone())),
    };
    result
  }

  async fn receive_on(&mut self, conn: Conn, size: u64, handle: &ConnHandle) -> Result<(), Error> {
    let (mut read, mut write) = conn.into_split()?;
    let mut sink = self.open_output().await?;
    let start = self.start_offset;
    let result = async {
      self
        .stop
        .run(write.write_all(&start.to_le_bytes()))
        .await
        .map_err(|error| io::connection_failure(handle, error, UPLOADER))?;
      self.tracker.emit_state(State::InProgress, None)?;
      self.tracker.emit_progress(start);
      tracing::info!(size, start, "download started");
      let cancel = self.stop.cancel().clone();
      let pace = Pace {
        tracker: &self.tracker,
        governor: self.options.governor.clone(),
        reporter: self.options.reporter.clone(),
        cancel: &cancel,
      };
      let received = io::receive(&mut read, sink.as_mut(), size - start, start, &pace);
      self
        .stop
        .run(received)
        .await
        .map_err(|error| io::connection_failure(handle, error, UPLOADER))
    }
    .await;
    if self.options.close_output_stream_on_completion
      && let Err(error) = sink.shutdown().await
    {
      tracing::warn!(%error, "the download's output did not close cleanly");
    }
    result
  }

  /// Opens where the bytes go, positioned at the start offset. A file keeps what is before the
  /// offset and loses what is after it.
  async fn open_output(&mut self) -> Result<Box<dyn DownloadSink>, Error> {
    let start = self.start_offset;
    let output = self
      .output
      .take()
      .expect("the output is opened once per download");
    let sink: Box<dyn DownloadSink> = match output {
      DownloadOutput::File(path) => {
        let file = tokio::fs::OpenOptions::new()
          .read(true)
          .write(true)
          .create(true)
          .truncate(false)
          .open(&path)
          .await
          .map_err(|error| {
            Error::io(format!("failed to open {}: {error}", path.display()), error)
          })?;
        let len = file
          .metadata()
          .await
          .map_err(|error| {
            Error::io(
              format!("failed to read the length of {}: {error}", path.display()),
              error,
            )
          })?
          .len();
        if len < start {
          return Err(Error::InvalidArgument(
            "local file is shorter than the start offset".into(),
          ));
        }
        file.set_len(start).await.map_err(|error| {
          Error::io(
            format!("failed to truncate {}: {error}", path.display()),
            error,
          )
        })?;
        let mut file = file;
        file.seek(SeekFrom::Start(start)).await.map_err(|error| {
          Error::io(
            format!("failed to seek in {}: {error}", path.display()),
            error,
          )
        })?;
        return Ok(Box::new(file));
      }
      DownloadOutput::Stream(open) => open()
        .await
        .map_err(|error| Error::io(format!("failed to open the output: {error}"), error))?,
    };
    let mut sink = sink;
    if start > 0 && self.options.seek_output_stream_automatically {
      sink
        .seek(SeekFrom::Start(start))
        .await
        .map_err(|error| Error::Transfer {
          message: format!(
            "requested non-zero start offset but output stream does not support seeking: {error}"
          ),
          source: Some(std::sync::Arc::new(error)),
        })?;
    }
    Ok(sink)
  }
}

/// whether a refusal is the peer saying it queued the request
fn is_queued(reason: &str) -> bool {
  reason.trim_end_matches('.').eq_ignore_ascii_case("Queued")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn queued_is_matched_without_case_or_trailing_dots() {
    assert!(is_queued("Queued"));
    assert!(is_queued("queued."));
    assert!(is_queued("QUEUED.."));
    assert!(!is_queued("Queued later"));
    assert!(!is_queued("File not shared."));
  }
}
