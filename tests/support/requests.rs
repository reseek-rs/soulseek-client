//! Transfers as the tests start them: a download or an upload as a value with its settings, run
//! through the client's builders, and a token that cancels it.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use soulseek_client::transfer::{Handle, RequestStyle, Transfer, UploadNotice, UploadSource};
use soulseek_client::{Client, Error};
use soulseek_proto::RemoteFilename;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

pub type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;
/// says how many of the bytes wanted may move; the token is never canceled
pub type TransferGovernor =
  Arc<dyn Fn(&Transfer, u32, CancellationToken) -> Boxed<u32> + Send + Sync>;
pub type TransferReporter = Arc<dyn Fn(&Transfer, u32, u32, u32) + Send + Sync>;
pub type ProgressUpdated = Arc<dyn Fn(u64, &Transfer) + Send + Sync>;
/// waits for an upload slot; the token is never canceled
pub type SlotAwaiter =
  Arc<dyn Fn(&Transfer, CancellationToken) -> Boxed<Result<(), Error>> + Send + Sync>;
pub type SlotReleased = Arc<dyn Fn(&Transfer) + Send + Sync>;
pub type UploadEnded = Arc<dyn Fn(&Transfer, &Error) -> UploadNotice + Send + Sync>;
pub type SizeMismatchResolver = Arc<dyn Fn(&Transfer, u64) -> Result<u64, Error> + Send + Sync>;
/// opens the source of an upload at a start offset
pub type InputFactory =
  Box<dyn FnOnce(u64) -> Boxed<std::io::Result<Box<dyn UploadSource>>> + Send>;

/// The settings of a transfer, each as its builder names it. The defaults are the client's.
#[derive(Clone)]
pub struct TransferOptions {
  pub governor: Option<TransferGovernor>,
  pub reporter: Option<TransferReporter>,
  pub progress_updated: Option<ProgressUpdated>,
  pub slot_awaiter: Option<SlotAwaiter>,
  pub slot_released: Option<SlotReleased>,
  pub upload_ended: Option<UploadEnded>,
  pub size_mismatch_resolver: Option<SizeMismatchResolver>,
  pub maximum_linger_time: Duration,
  pub request_style: RequestStyle,
  pub queued_timeout: Option<Duration>,
}

impl Default for TransferOptions {
  fn default() -> Self {
    Self {
      governor: None,
      reporter: None,
      progress_updated: None,
      slot_awaiter: None,
      slot_released: None,
      upload_ended: None,
      size_mismatch_resolver: None,
      maximum_linger_time: Duration::from_secs(3),
      request_style: RequestStyle::Queue,
      queued_timeout: None,
    }
  }
}

pub enum DownloadOutput {
  File(PathBuf),
}

pub struct DownloadRequest {
  pub username: String,
  pub filename: RemoteFilename,
  pub size: Option<u64>,
  pub start_offset: u64,
  pub token: Option<u32>,
  pub output: DownloadOutput,
  pub options: TransferOptions,
}

impl DownloadRequest {
  pub fn new(
    username: impl Into<String>,
    filename: impl Into<RemoteFilename>,
    size: impl Into<Option<u64>>,
    output: DownloadOutput,
  ) -> Self {
    Self {
      username: username.into(),
      filename: filename.into(),
      size: size.into(),
      start_offset: 0,
      token: None,
      output,
      options: TransferOptions::default(),
    }
  }
}

pub enum UploadInput {
  File(PathBuf),
  Stream { size: u64, open: InputFactory },
}

pub struct UploadRequest {
  pub username: String,
  pub filename: RemoteFilename,
  pub input: UploadInput,
  pub token: Option<u32>,
  pub options: TransferOptions,
}

impl UploadRequest {
  pub fn new(
    username: impl Into<String>,
    filename: impl Into<RemoteFilename>,
    input: UploadInput,
  ) -> Self {
    Self {
      username: username.into(),
      filename: filename.into(),
      input,
      token: None,
      options: TransferOptions::default(),
    }
  }
}

/// A running transfer that `cancel` cancels
pub struct Task {
  handle: Handle,
  canceled: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl Future for Task {
  type Output = Result<Transfer, Error>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.canceled.as_mut().poll(cx).is_ready() {
      self.handle.cancel();
    }
    Pin::new(&mut self.handle).poll(cx)
  }
}

/// follows `handle` for the test's progress callback, and ties it to `cancel`
fn task(handle: Handle, progress: Option<ProgressUpdated>, cancel: &CancellationToken) -> Task {
  if let Some(progress) = progress {
    let mut changes = handle.changes();
    tokio::spawn(async move {
      let mut previous = 0;
      while let Some(transfer) = changes.next().await {
        progress(previous, &transfer);
        previous = transfer.bytes_transferred;
      }
    });
  }
  Task {
    handle,
    canceled: Box::pin(cancel.clone().cancelled_owned()),
  }
}

/// waits for `starting`, a download being queued at its uploader, unless `cancel` fires first,
/// which drops it and so cancels the download
async fn queued(
  starting: impl Future<Output = Result<Handle, Error>>,
  cancel: &CancellationToken,
) -> Result<Handle, Error> {
  tokio::select! {
    biased;
    handle = starting => handle,
    () = cancel.cancelled() => Err(Error::Canceled),
  }
}

/// Transfers started from a request value and canceled with a token
pub trait Requests: Sync {
  fn enqueue_download_request(
    &self,
    request: DownloadRequest,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Task, Error>> + Send;

  fn enqueue_upload_request(
    &self,
    request: UploadRequest,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Task, Error>> + Send;

  fn download_request(
    &self,
    request: DownloadRequest,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Transfer, Error>> + Send {
    async move { self.enqueue_download_request(request, cancel).await?.await }
  }

  fn upload_request(
    &self,
    request: UploadRequest,
    cancel: &CancellationToken,
  ) -> impl Future<Output = Result<Transfer, Error>> + Send {
    async move { self.enqueue_upload_request(request, cancel).await?.await }
  }
}

impl Requests for Client {
  async fn enqueue_download_request(
    &self,
    request: DownloadRequest,
    cancel: &CancellationToken,
  ) -> Result<Task, Error> {
    let options = request.options;
    let DownloadOutput::File(path) = request.output;
    let mut download = self
      .download(request.username, request.filename)
      .start_offset(request.start_offset)
      .to_file(path)
      .request_style(options.request_style)
      .queued_timeout(options.queued_timeout);
    if let Some(size) = request.size {
      download = download.size(size);
    }
    if let Some(token) = request.token {
      download = download.token(token);
    }
    if let Some(govern) = options.governor {
      download = download
        .governor(move |transfer, wanted| govern(transfer, wanted, CancellationToken::new()));
    }
    if let Some(report) = options.reporter {
      download = download.reporter(move |transfer, wanted, granted, moved| {
        report(transfer, wanted, granted, moved);
      });
    }
    if let Some(settle) = options.size_mismatch_resolver {
      download = download.size_mismatch(move |transfer, size| settle(transfer, size));
    }
    let handle = queued(download.into_future(), cancel).await?;
    Ok(task(handle, options.progress_updated, cancel))
  }

  async fn enqueue_upload_request(
    &self,
    request: UploadRequest,
    cancel: &CancellationToken,
  ) -> Result<Task, Error> {
    let options = request.options;
    let mut upload = self
      .upload(request.username, request.filename)
      .linger(options.maximum_linger_time);
    upload = match request.input {
      UploadInput::File(path) => upload.from_file(path),
      UploadInput::Stream { size, open } => upload.from_source(size, open),
    };
    if let Some(token) = request.token {
      upload = upload.token(token);
    }
    if let Some(govern) = options.governor {
      upload =
        upload.governor(move |transfer, wanted| govern(transfer, wanted, CancellationToken::new()));
    }
    if let Some(report) = options.reporter {
      upload = upload.reporter(move |transfer, wanted, granted, moved| {
        report(transfer, wanted, granted, moved);
      });
    }
    if let Some(wait) = options.slot_awaiter {
      upload = upload.slot(move |transfer| wait(transfer, CancellationToken::new()));
    }
    if let Some(released) = options.slot_released {
      upload = upload.slot_released(move |transfer| released(transfer));
    }
    if let Some(notice) = options.upload_ended {
      upload = upload.ended(move |transfer, error| notice(transfer, error));
    }
    // an upload is queued at once, and one canceled by then goes on to tell its peer
    let handle = upload.await?;
    Ok(task(handle, options.progress_updated, cancel))
  }
}
