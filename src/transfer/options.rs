//! What a caller passes with one download or upload: where the bytes go or come from, and the
//! options and callbacks that shape the transfer (speed governing, slot policy, size disputes,
//! how long an upload waits for the downloader to close, which message asks for a file).

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use soulseek_proto::RemoteFilename;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

use super::Transfer;
use super::state::State;
use crate::error::Error;

/// Boxed future the transfer callbacks return.
pub(crate) type TransferFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// decides how many bytes the next chunk may move, given the transfer and the bytes wanted
pub(crate) type TransferGovernor = Arc<dyn Fn(&Transfer, u32) -> TransferFuture<u32> + Send + Sync>;

/// told what moved after every chunk: the bytes wanted, allowed and moved
pub(crate) type TransferReporter = Arc<dyn Fn(&Transfer, Chunk) + Send + Sync>;

/// What one chunk of a transfer moved, as its reporter is told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct Chunk {
  /// Bytes the chunk wanted to move.
  pub wanted: u32,
  /// Bytes the governor allowed.
  pub allowed: u32,
  /// Bytes that moved.
  pub moved: u32,
}

/// The governor and reporter of every transfer that sets none of its own
#[derive(Clone, Default)]
pub(crate) struct Defaults {
  pub(crate) governor: Option<TransferGovernor>,
  pub(crate) reporter: Option<TransferReporter>,
}

/// the callbacks have nothing to print
impl fmt::Debug for Defaults {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Defaults")
      .field("governor", &self.governor.is_some())
      .field("reporter", &self.reporter.is_some())
      .finish()
  }
}

/// told every state change of a transfer, with the state before it; the first report of a new
/// transfer has no previous state
pub(crate) type TransferStateChanged = Arc<dyn Fn(Option<State>, &Transfer) + Send + Sync>;

/// told every progress update of a transfer, with the bytes transferred before it
pub(crate) type TransferProgressUpdated = Arc<dyn Fn(u64, &Transfer) + Send + Sync>;

/// waits for the application's upload slot policy to let the upload start
pub(crate) type SlotAwaiter = Arc<dyn Fn(&Transfer) -> TransferFuture<Slot> + Send + Sync>;

/// Answer to an upload that waits for a slot.
#[derive(Debug)]
#[non_exhaustive]
pub enum Slot {
  /// The upload starts, holding the guard until it ends.
  Granted(SlotGuard),
  /// The upload is canceled.
  Cancel,
  /// The upload fails, with the reason.
  Fail(String),
}

impl Slot {
  /// Grants the slot, with a value the upload holds and drops when it ends.
  ///
  /// Dropping the value is how the application learns the slot is free again, so a semaphore
  /// permit or a guard of the application's own fits. `()` holds nothing.
  pub fn granted(guard: impl Send + 'static) -> Self {
    Self::Granted(SlotGuard(Box::new(guard)))
  }
}

/// Value an upload holds for its slot and drops when it ends.
pub struct SlotGuard(#[allow(dead_code, reason = "held only to be dropped")] Box<dyn Send>);

impl std::fmt::Debug for SlotGuard {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SlotGuard").finish_non_exhaustive()
  }
}

/// What a peer is told about an upload to them that did not succeed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum UploadNotice {
  /// The upload will not come, with the reason as written.
  ///
  /// The peer drops the file.
  Denied(String),
  /// The upload failed.
  ///
  /// The peer queues the file again.
  Failed,
  /// Nothing.
  Nothing,
}

/// decides what the peer is told about an upload that ended with an error
pub(crate) type UploadEnded = Arc<dyn Fn(&Transfer, &Error) -> Option<UploadNotice> + Send + Sync>;

/// settles the size of a download whose peer reports another than the expected one; `None`
/// aborts
pub(crate) type SizeMismatchResolver = Arc<dyn Fn(&Transfer, u64) -> Option<u64> + Send + Sync>;

/// Something a download writes into.
pub trait DownloadSink: AsyncWrite + AsyncSeek + Unpin + Send {}

impl<T: AsyncWrite + AsyncSeek + Unpin + Send> DownloadSink for T {}

/// Something an upload reads from.
pub trait UploadSource: AsyncRead + AsyncSeek + Unpin + Send {}

impl<T: AsyncRead + AsyncSeek + Unpin + Send> UploadSource for T {}

/// opens the sink of a download once the uploader has connected
pub(crate) type OutputFactory =
  Box<dyn FnOnce() -> TransferFuture<std::io::Result<Box<dyn DownloadSink>>> + Send>;

/// opens the source of an upload at the start offset the downloader named
pub(crate) type InputFactory =
  Box<dyn FnOnce(u64) -> TransferFuture<std::io::Result<Box<dyn UploadSource>>> + Send>;

/// Message that asks a peer for a file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum RequestStyle {
  /// [`soulseek_proto::peer::QueueDownload`], as every current client sends it.
  ///
  /// The peer does not answer it, so the download counts as queued at the peer once the request
  /// is written.
  #[default]
  Queue,
  /// A download [`soulseek_proto::peer::TransferRequest`].
  ///
  /// The peer answers it with "queued", "send now" or a refusal.
  TransferRequest,
  /// No message: the peer offered the file without being asked.
  ///
  /// The client holds the offer, which the download takes at once, since the sender waits for
  /// the client's answer.
  Offered,
}

/// the settings and hooks of one transfer, as its builder collected them
#[derive(Clone)]
pub(crate) struct Options {
  pub(crate) governor: Option<TransferGovernor>,
  pub(crate) reporter: Option<TransferReporter>,
  pub(crate) slot_awaiter: Option<SlotAwaiter>,
  pub(crate) upload_ended: Option<UploadEnded>,
  pub(crate) size_mismatch_resolver: Option<SizeMismatchResolver>,
  pub(crate) maximum_linger_time: Duration,
  pub(crate) request_style: RequestStyle,
  pub(crate) seek_input_stream_automatically: bool,
  pub(crate) seek_output_stream_automatically: bool,
  pub(crate) close_output_stream_on_completion: bool,
  pub(crate) queued_timeout: Option<Duration>,
}

impl Default for Options {
  fn default() -> Self {
    Self {
      governor: None,
      reporter: None,
      slot_awaiter: None,
      upload_ended: None,
      size_mismatch_resolver: None,
      maximum_linger_time: Duration::from_secs(3),
      request_style: RequestStyle::Queue,
      seek_input_stream_automatically: true,
      seek_output_stream_automatically: true,
      close_output_stream_on_completion: true,
      queued_timeout: None,
    }
  }
}

/// shows which callbacks are set; the callbacks themselves have nothing to print
impl fmt::Debug for Options {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Options")
      .field("governor", &self.governor.is_some())
      .field("reporter", &self.reporter.is_some())
      .field("slot_awaiter", &self.slot_awaiter.is_some())
      .field("upload_ended", &self.upload_ended.is_some())
      .field(
        "size_mismatch_resolver",
        &self.size_mismatch_resolver.is_some(),
      )
      .field("maximum_linger_time", &self.maximum_linger_time)
      .field("request_style", &self.request_style)
      .field(
        "seek_input_stream_automatically",
        &self.seek_input_stream_automatically,
      )
      .field(
        "seek_output_stream_automatically",
        &self.seek_output_stream_automatically,
      )
      .field(
        "close_output_stream_on_completion",
        &self.close_output_stream_on_completion,
      )
      .field("queued_timeout", &self.queued_timeout)
      .finish()
  }
}

/// where a download's bytes go
pub(crate) enum DownloadOutput {
  /// A local file, opened read-write once the uploader connects.
  ///
  /// A start offset of 0 truncates it. A larger one keeps what is before the offset and drops
  /// what is after.
  File(PathBuf),
  /// Whatever the factory opens once the uploader connects.
  Stream(OutputFactory),
}

/// shows the path of a file; a factory has nothing to print
impl fmt::Debug for DownloadOutput {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::File(path) => f.debug_tuple("File").field(path).finish(),
      Self::Stream(_) => f.write_str("Stream"),
    }
  }
}

/// where an upload's bytes come from
pub(crate) enum UploadInput {
  /// A local file.
  ///
  /// Its size is taken when the upload is requested, and the upload fails if the size differs
  /// once the file is opened to start.
  File(PathBuf),
  /// Whatever the factory opens once the downloader says where to start.
  ///
  /// `size` is what the downloader is told.
  Stream {
    /// Number of bytes the downloader is told to expect.
    size: u64,
    /// Factory that opens the bytes from the offset the downloader asks for.
    open: InputFactory,
  },
}

/// shows the path of a file or the size of a stream; a factory has nothing to print
impl fmt::Debug for UploadInput {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::File(path) => f.debug_tuple("File").field(path).finish(),
      Self::Stream { size, .. } => f
        .debug_struct("Stream")
        .field("size", size)
        .finish_non_exhaustive(),
    }
  }
}

/// a download as its builder hands it over
#[derive(Debug)]
pub(crate) struct DownloadRequest {
  pub(crate) username: String,
  pub(crate) filename: RemoteFilename,
  pub(crate) size: Option<u64>,
  pub(crate) start_offset: u64,
  pub(crate) token: Option<u32>,
  pub(crate) output: DownloadOutput,
  pub(crate) options: Options,
}

/// an upload as its builder hands it over
#[derive(Debug)]
pub(crate) struct UploadRequest {
  pub(crate) username: String,
  pub(crate) filename: RemoteFilename,
  pub(crate) input: UploadInput,
  pub(crate) token: Option<u32>,
  pub(crate) options: Options,
}
