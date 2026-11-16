//! How a caller starts a transfer and follows it: the download and upload it sets up and
//! awaits, the handle it gets back, and the transfers of a client as a whole.

use std::future::{Future, IntoFuture};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use soulseek_proto::peer::{UploadDenied, UploadFailed};
use soulseek_proto::{PeerMessage, RemoteFilename};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, ReusableBoxFuture};

use super::options::{
  DownloadOutput, DownloadRequest, DownloadSink, Options, RequestStyle, Slot, UploadInput,
  UploadNotice, UploadRequest, UploadSource,
};
use super::task::joined;
use super::{Direction, Transfer};
use crate::client::Client;
use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;

/// Download that has not started yet.
///
/// Made by [`Client::download`]. Its setters say where the bytes go and what shapes the
/// transfer. Awaiting it runs the download to its end and returns it as it ended, and dropping
/// that future cancels the download. [`Download::start`] returns a [`Handle`] instead.
///
/// # Errors
///
/// Awaiting it returns the errors [`Client::download`] lists: those of the checks made before
/// the download is tracked, and the error that ended the download if it started and did not
/// succeed.
#[must_use = "a download starts when it is awaited"]
pub struct Download {
  client: Client,
  username: String,
  filename: RemoteFilename,
  size: Option<u64>,
  start_offset: u64,
  token: Option<u32>,
  output: Option<DownloadOutput>,
  options: Options,
}

impl Download {
  pub(crate) fn new(client: &Client, username: String, filename: RemoteFilename) -> Self {
    Self {
      client: client.clone(),
      username,
      filename,
      size: None,
      start_offset: 0,
      token: None,
      output: None,
      options: Options::default(),
    }
  }

  /// Sets the size the file is expected to have, in bytes.
  ///
  /// It is the size a search response or a share listing gave. Without one the download takes
  /// the size the uploader names. Unset by default.
  pub fn size(mut self, size: impl Into<Option<u64>>) -> Self {
    self.size = size.into();
    self
  }

  /// Sets the offset the download starts at, in bytes.
  ///
  /// The bytes before it are the ones a file destination already holds. Defaults to 0.
  pub fn start_offset(mut self, offset: u64) -> Self {
    self.start_offset = offset;
    self
  }

  /// Sets the token the download is known by.
  ///
  /// By default the client picks one.
  pub fn token(mut self, token: u32) -> Self {
    self.token = Some(token);
    self
  }

  /// Writes the download to the file at `path`.
  ///
  /// The file is created when the uploader connects, and written from the start offset.
  pub fn to_file(mut self, path: impl Into<PathBuf>) -> Self {
    self.output = Some(DownloadOutput::File(path.into()));
    self
  }

  /// Writes the download to the sink `open` returns.
  ///
  /// `open` is called once the uploader has connected, so a download that never starts creates
  /// nothing.
  pub fn to_sink<F, Fut, S>(mut self, open: F) -> Self
  where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = std::io::Result<S>> + Send + 'static,
    S: DownloadSink + 'static,
  {
    self.output = Some(DownloadOutput::Stream(Box::new(move || {
      Box::pin(async move { Ok(Box::new(open().await?) as Box<dyn DownloadSink>) })
    })));
    self
  }

  /// Sets the message that asks the uploader for the file.
  ///
  /// Defaults to [`RequestStyle::Queue`].
  pub fn request_style(mut self, style: RequestStyle) -> Self {
    self.options.request_style = style;
    self
  }

  /// Sets the time the download may wait in the uploader's queue.
  ///
  /// `None` waits as long as it takes. Unset by default.
  pub fn queued_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
    self.options.queued_timeout = timeout.into();
    self
  }

  /// Sets what settles a size the uploader reports differently.
  ///
  /// `settle` is called with the transfer and the uploader's size, and returns the size the
  /// download should use. `None`, or a size below the start offset, aborts the download.
  /// Without it the download aborts, except for the size 0 that old clients report for files
  /// of 2 GiB and more.
  pub fn size_mismatch(
    mut self,
    settle: impl Fn(&Transfer, u64) -> Option<u64> + Send + Sync + 'static,
  ) -> Self {
    self.options.size_mismatch_resolver = Some(Arc::new(settle));
    self
  }

  /// Sets whether to position the sink at the start offset.
  ///
  /// If unset, that is left to the sink. Defaults to `true`.
  pub fn seek_sink(mut self, seek: bool) -> Self {
    self.options.seek_output_stream_automatically = seek;
    self
  }

  /// Sets whether to flush and close the sink when the download ends.
  ///
  /// This applies however the download ends. A file destination is always closed. Defaults to
  /// `true`.
  pub fn close_sink(mut self, close: bool) -> Self {
    self.options.close_output_stream_on_completion = close;
    self
  }

  /// Sets what decides how many bytes each chunk may move.
  ///
  /// `govern` is called before every chunk with the transfer and the bytes wanted, and returns
  /// how many may move now, at most the bytes wanted. It may wait until bytes are allowed,
  /// which is how a per-user limit is kept. One that returns 0 is asked again after a short
  /// pause. Without it the client's
  /// [`transfer_governor`](crate::config::ConfigBuilder::transfer_governor) is asked, and
  /// without that everything is allowed.
  pub fn governor<F, Fut>(mut self, govern: F) -> Self
  where
    F: Fn(&Transfer, u32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = u32> + Send + 'static,
  {
    self.options.governor = Some(Arc::new(move |transfer, wanted| {
      Box::pin(govern(transfer, wanted))
    }));
    self
  }

  /// Sets what is told after every chunk what moved.
  ///
  /// `report` is called with the transfer, the bytes the chunk wanted, the bytes the governor
  /// allowed, and the bytes that moved. Without it the client's
  /// [`transfer_reporter`](crate::config::ConfigBuilder::transfer_reporter) is told.
  pub fn reporter(
    mut self,
    report: impl Fn(&Transfer, u32, u32, u32) + Send + Sync + 'static,
  ) -> Self {
    self.options.reporter = Some(Arc::new(report));
    self
  }
}

impl Download {
  /// Starts the download and returns its [`Handle`].
  ///
  /// The call does not wait for the download to end. The download then runs on a task of its
  /// own, whether or not the handle is kept.
  ///
  /// # Errors
  ///
  /// Returns the errors of the checks made before the download is tracked, which
  /// [`Client::download`] lists first. Every later error comes from the handle.
  pub async fn start(self) -> Result<Handle, Error> {
    let Some(output) = self.output else {
      return Err(Error::invalid_argument(
        "the download has no destination".into(),
      ));
    };
    let request = DownloadRequest {
      username: self.username,
      filename: self.filename,
      size: self.size,
      start_offset: self.start_offset,
      token: self.token,
      output,
      options: self.options,
    };
    self.client.start_download(request).await
  }
}

impl IntoFuture for Download {
  type Output = Result<Transfer, Error>;
  type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

  fn into_future(self) -> Self::IntoFuture {
    Box::pin(async move { self.start().await?.until_ended().await })
  }
}

impl std::fmt::Debug for Download {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Download")
      .field("username", &self.username)
      .field("filename", &self.filename)
      .field("size", &self.size)
      .field("start_offset", &self.start_offset)
      .field("token", &self.token)
      .field("output", &self.output)
      .field("options", &self.options)
      .finish()
  }
}

/// Upload that has not started yet.
///
/// Made by [`Client::upload`]. Its setters say where the bytes come from and what shapes the
/// transfer. Awaiting it runs the upload to its end and returns it as it ended, and dropping
/// that future cancels the upload. [`Upload::start`] returns a [`Handle`] instead.
///
/// # Errors
///
/// Awaiting it returns the errors [`Client::upload`] lists: those of the checks made before the
/// upload is tracked, and the error that ended the upload if it started and did not succeed.
#[must_use = "an upload starts when it is awaited"]
pub struct Upload {
  client: Client,
  username: String,
  filename: RemoteFilename,
  token: Option<u32>,
  input: Option<UploadInput>,
  options: Options,
}

impl Upload {
  pub(crate) fn new(client: &Client, username: String, filename: RemoteFilename) -> Self {
    Self {
      client: client.clone(),
      username,
      filename,
      token: None,
      input: None,
      options: Options::default(),
    }
  }

  /// Sets the token the upload is known by.
  ///
  /// By default the client picks one.
  pub fn token(mut self, token: u32) -> Self {
    self.token = Some(token);
    self
  }

  /// Reads the upload from the file at `path`.
  pub fn from_file(mut self, path: impl Into<PathBuf>) -> Self {
    self.input = Some(UploadInput::File(path.into()));
    self
  }

  /// Reads the upload, `size` bytes long, from the source `open` returns.
  ///
  /// `open` is called with the start offset once the downloader has said where to start, so
  /// nothing is held open while the upload waits in a queue. The upload owns the source and
  /// drops it when it ends.
  pub fn from_source<F, Fut, S>(mut self, size: u64, open: F) -> Self
  where
    F: FnOnce(u64) -> Fut + Send + 'static,
    Fut: Future<Output = std::io::Result<S>> + Send + 'static,
    S: UploadSource + 'static,
  {
    self.input = Some(UploadInput::Stream {
      size,
      open: Box::new(move |offset| {
        Box::pin(async move { Ok(Box::new(open(offset).await?) as Box<dyn UploadSource>) })
      }),
    });
    self
  }

  /// Sets whether to position the source at the start offset.
  ///
  /// If unset, that is left to the source. Defaults to `true`.
  pub fn seek_source(mut self, seek: bool) -> Self {
    self.options.seek_input_stream_automatically = seek;
    self
  }

  /// Sets how long to wait for the downloader to close the connection.
  ///
  /// The wait starts after the last byte. The client closes the connection itself after that
  /// time. Defaults to 3 seconds.
  pub fn linger(mut self, time: Duration) -> Self {
    self.options.maximum_linger_time = time;
    self
  }

  /// Sets what waits for the application's upload slot.
  ///
  /// `wait` is called with the transfer and resolves once the application's slot policy has
  /// decided: [`Slot::granted`] starts the upload, which holds the guard given and drops it as
  /// soon as it ends, [`Slot::Cancel`] cancels the upload and [`Slot::Fail`] fails it. Without
  /// it the upload starts at once.
  pub fn slot<F, Fut>(mut self, wait: F) -> Self
  where
    F: Fn(&Transfer) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Slot> + Send + 'static,
  {
    self.options.slot_awaiter = Some(Arc::new(move |transfer| Box::pin(wait(transfer))));
    self
  }

  /// Sets what decides what the peer is told when the upload does not succeed.
  ///
  /// `notice` is called with the transfer and the error it ended with. Without it a canceled
  /// upload is denied with "Cancelled" and any other is said to have failed. A peer that is
  /// offline or refused the file is told nothing either way.
  pub fn ended(
    mut self,
    notice: impl Fn(&Transfer, &Error) -> UploadNotice + Send + Sync + 'static,
  ) -> Self {
    self.options.upload_ended = Some(Arc::new(notice));
    self
  }

  /// Sets what decides how many bytes each chunk may move.
  ///
  /// `govern` is called before every chunk with the transfer and the bytes wanted, and returns
  /// how many may move now, at most the bytes wanted. It may wait until bytes are allowed,
  /// which is how a per-user limit is kept. One that returns 0 is asked again after a short
  /// pause. Without it the client's
  /// [`transfer_governor`](crate::config::ConfigBuilder::transfer_governor) is asked, and
  /// without that everything is allowed.
  pub fn governor<F, Fut>(mut self, govern: F) -> Self
  where
    F: Fn(&Transfer, u32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = u32> + Send + 'static,
  {
    self.options.governor = Some(Arc::new(move |transfer, wanted| {
      Box::pin(govern(transfer, wanted))
    }));
    self
  }

  /// Sets what is told after every chunk what moved.
  ///
  /// `report` is called with the transfer, the bytes the chunk wanted, the bytes the governor
  /// allowed, and the bytes that moved. Without it the client's
  /// [`transfer_reporter`](crate::config::ConfigBuilder::transfer_reporter) is told.
  pub fn reporter(
    mut self,
    report: impl Fn(&Transfer, u32, u32, u32) + Send + Sync + 'static,
  ) -> Self {
    self.options.reporter = Some(Arc::new(report));
    self
  }
}

impl Upload {
  /// Starts the upload and returns its [`Handle`].
  ///
  /// The call does not wait for the upload to end. The upload then runs on a task of its own,
  /// whether or not the handle is kept.
  ///
  /// # Errors
  ///
  /// Returns the errors of the checks made before the upload is tracked, which
  /// [`Client::upload`] lists first. Every later error comes from the handle.
  pub async fn start(self) -> Result<Handle, Error> {
    let Some(input) = self.input else {
      return Err(Error::invalid_argument("the upload has no source".into()));
    };
    let request = UploadRequest {
      username: self.username,
      filename: self.filename,
      input,
      token: self.token,
      options: self.options,
    };
    self.client.start_upload(request).await
  }
}

impl IntoFuture for Upload {
  type Output = Result<Transfer, Error>;
  type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

  fn into_future(self) -> Self::IntoFuture {
    Box::pin(async move { self.start().await?.until_ended().await })
  }
}

impl std::fmt::Debug for Upload {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Upload")
      .field("username", &self.username)
      .field("filename", &self.filename)
      .field("token", &self.token)
      .field("input", &self.input)
      .field("options", &self.options)
      .finish()
  }
}

/// Running transfer.
///
/// Made by [`Download::start`] and [`Upload::start`]. Awaiting the handle waits for the transfer
/// to end and returns it as it ended, or the error that ended it. Dropping the handle leaves
/// the transfer running.
pub struct Handle {
  task: JoinHandle<Result<Transfer, Error>>,
  cancel: CancellationToken,
  latest: watch::Receiver<Transfer>,
}

impl Handle {
  pub(crate) fn new(
    task: JoinHandle<Result<Transfer, Error>>,
    cancel: CancellationToken,
    latest: watch::Receiver<Transfer>,
  ) -> Self {
    Self {
      task,
      cancel,
      latest,
    }
  }

  /// waits for the transfer to end, canceling it if the wait is dropped first
  async fn until_ended(self) -> Result<Transfer, Error> {
    let dropped = self.cancel.clone().drop_guard();
    let result = self.await;
    dropped.disarm();
    result
  }

  /// Cancels the transfer.
  ///
  /// The transfer then ends with [`ErrorKind::Canceled`], which awaiting the handle returns.
  /// An upload that has written every byte stands.
  pub fn cancel(&self) {
    self.cancel.cancel();
  }

  /// Returns the transfer as it is now.
  pub fn transfer(&self) -> Transfer {
    self.latest.borrow().clone()
  }

  /// Returns a stream of the transfer as it changes.
  ///
  /// The stream starts with the transfer as it is now. One read slower than the transfer
  /// changes skips to the latest. It ends once the transfer has.
  pub fn changes(&self) -> Changes {
    let mut latest = self.latest.clone();
    latest.mark_changed();
    Changes {
      next: ReusableBoxFuture::new(change(latest)),
    }
  }
}

impl Future for Handle {
  type Output = Result<Transfer, Error>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    Pin::new(&mut self.task).poll(cx).map(joined)
  }
}

impl std::fmt::Debug for Handle {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Handle")
      .field("transfer", &self.transfer())
      .finish_non_exhaustive()
  }
}

type Changed = (Option<Transfer>, watch::Receiver<Transfer>);

/// A transfer as it changes.
///
/// Made by [`Handle::changes`].
pub struct Changes {
  next: ReusableBoxFuture<'static, Changed>,
}

impl Changes {
  /// Returns the transfer once it has changed, or `None` once it has ended.
  ///
  /// # Cancel safety
  ///
  /// This method is cancel safe. A change not returned stays for the next call.
  pub async fn next(&mut self) -> Option<Transfer> {
    std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
  }
}

async fn change(mut latest: watch::Receiver<Transfer>) -> Changed {
  let changed = latest
    .changed()
    .await
    .is_ok()
    .then(|| latest.borrow_and_update().clone());
  (changed, latest)
}

impl Stream for Changes {
  type Item = Transfer;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Transfer>> {
    let (changed, latest) = std::task::ready!(self.next.poll(cx));
    self.next.set(change(latest));
    Poll::Ready(changed)
  }
}

impl std::fmt::Debug for Changes {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Changes").finish_non_exhaustive()
  }
}

/// The transfers of a client.
///
/// Made by [`Client::transfers`].
#[derive(Clone, Copy, Debug)]
pub struct Transfers<'a> {
  pub(crate) client: &'a Client,
}

impl Transfers<'_> {
  /// Returns a stream of what happens to the client's transfers.
  pub fn events(self) -> crate::Events<super::Event> {
    self.client.events_of(|event| match event {
      crate::Event::Transfer(event) => Some(event),
      _ => None,
    })
  }

  /// Returns every download the client tracks, as it is now.
  pub fn downloads(self) -> Vec<Transfer> {
    self
      .client
      .transfer_registry()
      .snapshots(Direction::Download)
  }

  /// Returns every upload the client tracks, as it is now.
  pub fn uploads(self) -> Vec<Transfer> {
    self.client.transfer_registry().snapshots(Direction::Upload)
  }

  /// Tells a user that an upload they asked for will not come.
  ///
  /// `username` is told that the upload of `filename` will not come, with `reason`, as a
  /// refusal of their request would have told them: their client drops the file. The message
  /// is sent best effort and bounded, as when an upload ends.
  pub async fn deny(self, username: &str, filename: RemoteFilename, reason: String) {
    let denied = PeerMessage::UploadDenied(UploadDenied { filename, reason });
    self.client.tell_upload_ended(username, denied).await;
  }

  /// Tells a user that an upload they asked for failed.
  ///
  /// `username` is told that the upload of `filename` failed, as an upload that broke off
  /// would have told them: their client queues the file again. The message is sent best effort
  /// and bounded, as when an upload ends.
  pub async fn fail(self, username: &str, filename: RemoteFilename) {
    let failed = PeerMessage::UploadFailed(UploadFailed { filename });
    self.client.tell_upload_ended(username, failed).await;
  }
}
