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
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, ReusableBoxFuture};

use super::options::{
  Chunk, DownloadOutput, DownloadRequest, DownloadSink, Options, RequestStyle, Slot, UploadInput,
  UploadNotice, UploadRequest, UploadSource,
};
use super::record::Following;
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
/// It can be awaited or started once [`Download::to_file`] or [`Download::to_sink`] has set
/// where the bytes go; `To` is [`NoDestination`] until then, and awaiting or starting it does
/// not compile.
///
/// # Errors
///
/// Awaiting it returns the errors [`Client::download`] lists: those of the checks made before
/// the download is tracked, and the error that ended the download if it started and did not
/// succeed.
#[must_use = "a download starts when it is awaited"]
pub struct Download<To = NoDestination> {
  client: Client,
  username: String,
  filename: RemoteFilename,
  size: Option<u64>,
  start_offset: u64,
  token: Option<u32>,
  output: To,
  options: Options,
}

/// a download or an upload awaited without saying where the bytes go or come from does not
/// compile
///
/// ```compile_fail
/// # async fn run(client: soulseek_client::Client) {
/// let _ = client.download("user", "@@share\\a.mp3").await;
/// # }
/// ```
///
/// ```compile_fail
/// # async fn run(client: soulseek_client::Client) {
/// let _ = client.upload("user", "@@share\\a.mp3").start().await;
/// # }
/// ```
#[cfg(doctest)]
struct EndsRequired;

/// Marks a [`Download`] whose destination is not set yet.
#[derive(Debug)]
pub struct NoDestination;

/// Where a [`Download`] writes its bytes.
///
/// Set with [`Download::to_file`] or [`Download::to_sink`].
#[derive(Debug)]
pub struct Destination(DownloadOutput);

impl Download {
  pub(crate) fn new(client: &Client, username: String, filename: RemoteFilename) -> Self {
    Self {
      client: client.clone(),
      username,
      filename,
      size: None,
      start_offset: 0,
      token: None,
      output: NoDestination,
      options: Options::default(),
    }
  }
}

impl<To> Download<To> {
  /// the same download, writing to `output`
  fn writing(self, output: DownloadOutput) -> Download<Destination> {
    Download {
      client: self.client,
      username: self.username,
      filename: self.filename,
      size: self.size,
      start_offset: self.start_offset,
      token: self.token,
      output: Destination(output),
      options: self.options,
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
  /// The file is created when the uploader connects, and written from the start offset. What it
  /// holds past the start offset is dropped.
  pub fn to_file(self, path: impl Into<PathBuf>) -> Download<Destination> {
    self.writing(DownloadOutput::File(path.into()))
  }

  /// Writes the download to the sink `open` returns.
  ///
  /// `open` is called once the uploader has connected, so a download that never starts creates
  /// nothing. A cancel ends the download even while `open` has not returned.
  pub fn to_sink<F, Fut, S>(self, open: F) -> Download<Destination>
  where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = std::io::Result<S>> + Send + 'static,
    S: DownloadSink + 'static,
  {
    self.writing(DownloadOutput::Stream(Box::new(move || {
      Box::pin(async move { Ok(Box::new(open().await?) as Box<dyn DownloadSink>) })
    })))
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
  /// When `false`, the sink is written as `open` returned it. A file destination is always
  /// positioned. Defaults to `true`.
  pub fn seek_sink(mut self, seek: bool) -> Self {
    self.options.seek_output_stream_automatically = seek;
    self
  }

  /// Sets whether to flush and close the sink when the download ends.
  ///
  /// This applies however the download ends. A close that fails or takes longer than 10 seconds
  /// is given up and does not change how the download ends. A file destination is always
  /// closed. Defaults to `true`.
  pub fn close_sink(mut self, close: bool) -> Self {
    self.options.close_output_stream_on_completion = close;
    self
  }

  /// Sets what decides how many bytes each chunk may move.
  ///
  /// `govern` is called before every chunk with the transfer and the bytes wanted, and returns
  /// how many may move now, at most the bytes wanted. It may wait until bytes are allowed,
  /// which is how a per-user limit is kept; the connection's inactivity timeout does not run
  /// while it waits. One that returns 0 is asked again after a short pause. Without it the
  /// client's [`transfer_governor`](crate::config::ConfigBuilder::transfer_governor) is asked,
  /// and without that everything is allowed.
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
  /// `report` is called with the transfer and the [`Chunk`]: the bytes it wanted, the bytes the
  /// governor allowed, and the bytes that moved. Without it the client's
  /// [`transfer_reporter`](crate::config::ConfigBuilder::transfer_reporter) is told.
  pub fn reporter(mut self, report: impl Fn(&Transfer, Chunk) + Send + Sync + 'static) -> Self {
    self.options.reporter = Some(Arc::new(report));
    self
  }
}

impl Download<Destination> {
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
    let request = DownloadRequest {
      username: self.username,
      filename: self.filename,
      size: self.size,
      start_offset: self.start_offset,
      token: self.token,
      output: self.output.0,
      options: self.options,
    };
    self.client.start_download(request).await
  }
}

impl IntoFuture for Download<Destination> {
  type Output = Result<Transfer, Error>;
  type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

  fn into_future(self) -> Self::IntoFuture {
    Box::pin(async move { self.start().await?.until_ended().await })
  }
}

impl<To: std::fmt::Debug> std::fmt::Debug for Download<To> {
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
/// It can be awaited or started once [`Upload::from_file`] or [`Upload::from_source`] has set
/// where the bytes come from; `From` is [`NoSource`] until then, and awaiting or starting it
/// does not compile.
///
/// # Errors
///
/// Awaiting it returns the errors [`Client::upload`] lists: those of the checks made before the
/// upload is tracked, and the error that ended the upload if it started and did not succeed.
#[must_use = "an upload starts when it is awaited"]
pub struct Upload<From = NoSource> {
  client: Client,
  username: String,
  filename: RemoteFilename,
  token: Option<u32>,
  input: From,
  options: Options,
}

/// Marks an [`Upload`] whose source is not set yet.
#[derive(Debug)]
pub struct NoSource;

/// Where an [`Upload`] reads its bytes from.
///
/// Set with [`Upload::from_file`] or [`Upload::from_source`].
#[derive(Debug)]
pub struct Source(UploadInput);

impl Upload {
  pub(crate) fn new(client: &Client, username: String, filename: RemoteFilename) -> Self {
    Self {
      client: client.clone(),
      username,
      filename,
      token: None,
      input: NoSource,
      options: Options::default(),
    }
  }
}

impl<From> Upload<From> {
  /// the same upload, reading from `input`
  fn reading(self, input: UploadInput) -> Upload<Source> {
    Upload {
      client: self.client,
      username: self.username,
      filename: self.filename,
      token: self.token,
      input: Source(input),
      options: self.options,
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
  pub fn from_file(self, path: impl Into<PathBuf>) -> Upload<Source> {
    self.reading(UploadInput::File(path.into()))
  }

  /// Reads the upload, `size` bytes long, from the source `open` returns.
  ///
  /// `open` is called with the start offset once the downloader has said where to start, so
  /// nothing is held open while the upload waits in a queue, and returns the source positioned
  /// at that offset: the upload reads from it as it is. The upload owns the source and drops it
  /// when it ends. A cancel ends the upload even while `open` has not returned.
  pub fn from_source<F, Fut, S>(self, size: u64, open: F) -> Upload<Source>
  where
    F: FnOnce(u64) -> Fut + Send + 'static,
    Fut: Future<Output = std::io::Result<S>> + Send + 'static,
    S: UploadSource + 'static,
  {
    self.reading(UploadInput::Stream {
      size,
      open: Box::new(move |offset| {
        Box::pin(async move { Ok(Box::new(open(offset).await?) as Box<dyn UploadSource>) })
      }),
    })
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
  /// `notice` is called with the transfer and the error it ended with, and returns what the
  /// peer is told, or `None` to leave it to the client. The client denies a canceled upload with
  /// "Cancelled" and says any other failed. A peer that is offline or refused the file is told
  /// nothing either way, and `notice` is not called. Nothing is sent while the client
  /// disconnects or once it is shut down.
  pub fn ended(
    mut self,
    notice: impl Fn(&Transfer, &Error) -> Option<UploadNotice> + Send + Sync + 'static,
  ) -> Self {
    self.options.upload_ended = Some(Arc::new(notice));
    self
  }

  /// Sets what decides how many bytes each chunk may move.
  ///
  /// `govern` is called before every chunk with the transfer and the bytes wanted, and returns
  /// how many may move now, at most the bytes wanted. It may wait until bytes are allowed,
  /// which is how a per-user limit is kept; the connection's inactivity timeout does not run
  /// while it waits. One that returns 0 is asked again after a short pause. Without it the
  /// client's [`transfer_governor`](crate::config::ConfigBuilder::transfer_governor) is asked,
  /// and without that everything is allowed.
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
  /// `report` is called with the transfer and the [`Chunk`]: the bytes it wanted, the bytes the
  /// governor allowed, and the bytes that moved. Without it the client's
  /// [`transfer_reporter`](crate::config::ConfigBuilder::transfer_reporter) is told.
  pub fn reporter(mut self, report: impl Fn(&Transfer, Chunk) + Send + Sync + 'static) -> Self {
    self.options.reporter = Some(Arc::new(report));
    self
  }
}

impl Upload<Source> {
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
    let request = UploadRequest {
      username: self.username,
      filename: self.filename,
      input: self.input.0,
      token: self.token,
      options: self.options,
    };
    self.client.start_upload(request).await
  }
}

impl IntoFuture for Upload<Source> {
  type Output = Result<Transfer, Error>;
  type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

  fn into_future(self) -> Self::IntoFuture {
    Box::pin(async move { self.start().await?.until_ended().await })
  }
}

impl<From: std::fmt::Debug> std::fmt::Debug for Upload<From> {
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
/// the transfer running. [`Handle::control`] gives what controls the transfer without the wait,
/// to share.
pub struct Handle {
  task: JoinHandle<Result<Transfer, Error>>,
  control: Control,
}

impl Handle {
  pub(crate) fn new(
    task: JoinHandle<Result<Transfer, Error>>,
    cancel: CancellationToken,
    latest: Following,
  ) -> Self {
    Self {
      task,
      control: Control::new(cancel, latest),
    }
  }

  /// waits for the transfer to end, canceling it if the wait is dropped first
  async fn until_ended(self) -> Result<Transfer, Error> {
    let dropped = self.control.cancel.clone().drop_guard();
    let result = self.await;
    dropped.disarm();
    result
  }

  /// Cancels the transfer.
  ///
  /// The transfer then ends with [`ErrorKind::Canceled`], which awaiting the handle returns.
  /// An upload that has written every byte stands.
  pub fn cancel(&self) {
    self.control.cancel();
  }

  /// Returns the transfer as it is now.
  pub fn transfer(&self) -> Transfer {
    self.control.transfer()
  }

  /// Returns a stream of the transfer as it changes.
  ///
  /// The stream gives the transfer as it entered each state, every state from the first, in
  /// order. Between two states it gives the latest progress: one read slower than the progress
  /// skips to the latest, never past a state. It ends once the transfer has.
  pub fn changes(&self) -> Changes {
    self.control.changes()
  }

  /// Returns what controls the transfer, without the wait for its end.
  pub fn control(&self) -> Control {
    self.control.clone()
  }
}

/// Control of a running transfer.
///
/// Made by [`Handle::control`] and [`Transfers::get`]. A clone controls the same transfer, so
/// any number of tasks can follow or cancel it. Only the [`Handle`] waits for its end.
#[derive(Clone)]
pub struct Control {
  cancel: CancellationToken,
  latest: Following,
}

impl Control {
  pub(crate) fn new(cancel: CancellationToken, latest: Following) -> Self {
    Self { cancel, latest }
  }

  /// Cancels the transfer.
  ///
  /// The transfer then ends with [`ErrorKind::Canceled`]. An upload that has written every byte
  /// stands.
  pub fn cancel(&self) {
    self.cancel.cancel();
  }

  /// Returns the transfer as it is now.
  pub fn transfer(&self) -> Transfer {
    self.latest.borrow().latest().clone()
  }

  /// Returns a stream of the transfer as it changes.
  ///
  /// The stream gives the transfer as it entered each state, every state from the first, in
  /// order. Between two states it gives the latest progress: one read slower than the progress
  /// skips to the latest, never past a state. It ends once the transfer has.
  pub fn changes(&self) -> Changes {
    let follow = Follow {
      following: self.latest.clone(),
      states_seen: 0,
      seen: None,
    };
    Changes {
      next: ReusableBoxFuture::new(change(follow)),
    }
  }
}

impl std::fmt::Debug for Control {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Control")
      .field("transfer", &self.transfer())
      .finish_non_exhaustive()
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

/// Where one stream of a transfer's changes is
struct Follow {
  following: Following,
  /// how many of the states the stream has given
  states_seen: usize,
  /// the number of the last change the stream gave
  seen: Option<u64>,
}

type Changed = (Option<Transfer>, Follow);

/// A transfer as it changes.
///
/// Made by [`Handle::changes`] and [`Control::changes`].
pub struct Changes {
  next: ReusableBoxFuture<'static, Changed>,
}

impl Changes {
  /// Returns the next transfer the stream gives, or `None` once it has ended.
  ///
  /// # Cancel safety
  ///
  /// This method is cancel safe. A change not returned stays for the next call.
  pub async fn next(&mut self) -> Option<Transfer> {
    std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
  }
}

async fn change(mut follow: Follow) -> Changed {
  let mut closed = false;
  loop {
    let next = {
      let followed = follow.following.borrow_and_update();
      followed
        .after(follow.states_seen, follow.seen)
        .map(|(version, transfer, state)| (version, transfer.clone(), state))
    };
    if let Some((version, transfer, state)) = next {
      follow.states_seen += usize::from(state);
      follow.seen = Some(version);
      return (Some(transfer), follow);
    }
    // the sender's last change is read before its close, so a closed channel has nothing left
    if closed {
      return (None, follow);
    }
    closed = follow.following.changed().await.is_err();
  }
}

impl Stream for Changes {
  type Item = Transfer;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Transfer>> {
    let (changed, follow) = std::task::ready!(self.next.poll(cx));
    self.next.set(change(follow));
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

  /// Returns the control of the transfer the client tracks under `token`.
  ///
  /// Returns `None` if no transfer has the token, or it has ended.
  pub fn get(self, token: u32) -> Option<Control> {
    self.client.transfer_registry().control(token)
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
  pub async fn deny(
    self,
    username: &str,
    filename: impl Into<RemoteFilename>,
    reason: impl Into<String>,
  ) {
    let denied = PeerMessage::UploadDenied(UploadDenied {
      filename: filename.into(),
      reason: reason.into(),
    });
    self.client.tell_upload_ended(username, denied).await;
  }

  /// Tells a user that an upload they asked for failed.
  ///
  /// `username` is told that the upload of `filename` failed, as an upload that broke off
  /// would have told them: their client queues the file again. The message is sent best effort
  /// and bounded, as when an upload ends.
  pub async fn fail(self, username: &str, filename: impl Into<RemoteFilename>) {
    let failed = PeerMessage::UploadFailed(UploadFailed {
      filename: filename.into(),
    });
    self.client.tell_upload_ended(username, failed).await;
  }
}
