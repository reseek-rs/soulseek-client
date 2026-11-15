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
use tokio_util::sync::CancellationToken;

use super::Transfer;
use super::state::TransferState;
use crate::error::Error;
use crate::resolvers::ResolverFuture;

/// Boxed future the transfer callbacks return.
pub type TransferFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Callback that decides how many bytes the next chunk may move.
///
/// Called with `(transfer, wanted, cancel)`. Returns how many bytes the next chunk may move now,
/// at most `wanted`. It may wait until bytes are allowed, which is how a per-user limit is kept.
/// A governor that returns 0 is asked again after a short pause.
pub type TransferGovernor =
  Arc<dyn Fn(&Transfer, u32, CancellationToken) -> TransferFuture<u32> + Send + Sync>;

/// Callback told what moved after every chunk.
///
/// Called with `(transfer, wanted, granted, moved)`: the bytes the chunk wanted, the bytes the
/// governor allowed, and the bytes that actually moved.
pub type TransferReporter = Arc<dyn Fn(&Transfer, u32, u32, u32) + Send + Sync>;

/// Callback for every state change of a transfer.
///
/// Called with `(previous_state, transfer)`, before the client raises the event. The first
/// report of a new transfer has no previous state.
pub type TransferStateChanged = Arc<dyn Fn(Option<TransferState>, &Transfer) + Send + Sync>;

/// Callback for every progress update of a transfer.
///
/// Called with `(previous_bytes_transferred, transfer)`. When it is given, no progress event is
/// raised for the transfer.
pub type TransferProgressUpdated = Arc<dyn Fn(u64, &Transfer) + Send + Sync>;

/// Callback that waits for the application's upload slot policy.
///
/// Called with `(transfer, cancel)`. Resolves once the application's upload slot policy lets the
/// upload start. It must hold nothing when it fails. Failing with [`Error::Canceled`]
/// cancels the upload, and any other error fails it.
pub type SlotAwaiter =
  Arc<dyn Fn(&Transfer, CancellationToken) -> ResolverFuture<()> + Send + Sync>;

/// What a peer is told about an upload to them that did not succeed.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// Callback that decides what a peer is told about an ended upload.
///
/// Called with `(transfer, error)`. Returns what the peer is told about an upload that ended
/// with `error`.
pub type UploadEnded = Arc<dyn Fn(&Transfer, &Error) -> UploadNotice + Send + Sync>;

/// Callback told that an upload slot is free again.
///
/// Called with `(transfer)` when the slot the awaiter granted is free again. It is called once
/// for every upload whose awaiter succeeded, as soon as the upload ends.
pub type SlotReleased = Arc<dyn Fn(&Transfer) + Send + Sync>;

/// Resolver that settles the size of a download.
///
/// Called with `(transfer, remote_size)` when the peer reports a size other than the expected
/// one. Returns the size the download should use. An error, or a size below the start offset,
/// aborts the download.
pub type SizeMismatchResolver = Arc<dyn Fn(&Transfer, u64) -> Result<u64, Error> + Send + Sync>;

/// Something a download writes into.
pub trait DownloadSink: AsyncWrite + AsyncSeek + Unpin + Send {}

impl<T: AsyncWrite + AsyncSeek + Unpin + Send> DownloadSink for T {}

/// Something an upload reads from.
pub trait UploadSource: AsyncRead + AsyncSeek + Unpin + Send {}

impl<T: AsyncRead + AsyncSeek + Unpin + Send> UploadSource for T {}

/// Factory that opens the sink of a download.
///
/// It is called once the uploader has connected, so a download that never starts creates
/// nothing.
pub type OutputFactory =
  Box<dyn FnOnce() -> TransferFuture<std::io::Result<Box<dyn DownloadSink>>> + Send>;

/// Factory that opens the source of an upload at a start offset.
///
/// It is called with the start offset once the downloader has said where to start, so nothing
/// is held open while the upload waits in a queue. The upload owns the source and drops it when
/// it ends.
pub type InputFactory =
  Box<dyn FnOnce(u64) -> TransferFuture<std::io::Result<Box<dyn UploadSource>>> + Send>;

/// Message that asks a peer for a file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

/// Options and callbacks of one transfer.
///
/// Every callback is optional.
#[derive(Clone)]
pub struct TransferOptions {
  /// Callback asked before every chunk how many bytes may move.
  ///
  /// If unset, everything is allowed. Unset by default.
  pub governor: Option<TransferGovernor>,
  /// Callback told after every chunk what moved.
  ///
  /// Unset by default.
  pub reporter: Option<TransferReporter>,
  /// Callback called on every state change.
  ///
  /// Unset by default.
  pub state_changed: Option<TransferStateChanged>,
  /// Callback called on every progress update in place of the event.
  ///
  /// Every chunk of every transfer would otherwise put the event on the one event channel all
  /// subscribers share. Unset by default.
  pub progress_updated: Option<TransferProgressUpdated>,
  /// Callback that waits for the application's upload slot.
  ///
  /// Applies to uploads. If unset, the upload starts at once. Unset by default.
  pub slot_awaiter: Option<SlotAwaiter>,
  /// Callback that hands the slot back.
  ///
  /// Applies to uploads. Unset by default.
  pub slot_released: Option<SlotReleased>,
  /// Callback that decides what the peer is told when the upload does not succeed.
  ///
  /// Applies to uploads. If unset, a canceled upload is denied with "Cancelled" and any other is
  /// said to have failed. A peer that is offline or refused the file is told nothing either
  /// way. Unset by default.
  pub upload_ended: Option<UploadEnded>,
  /// Resolver that settles a size the peer reports differently.
  ///
  /// Applies to downloads. If unset, the download aborts, except for the size 0 that old clients
  /// report for files of 2 GiB and more. Unset by default.
  pub size_mismatch_resolver: Option<SizeMismatchResolver>,
  /// Time to wait after the last byte for the downloader to close the connection.
  ///
  /// Applies to uploads. The client closes the connection itself after that time. Defaults to
  /// 3 seconds.
  pub maximum_linger_time: Duration,
  /// Message that asks the peer for the file.
  ///
  /// Applies to downloads. Defaults to [`RequestStyle::Queue`].
  pub request_style: RequestStyle,
  /// Whether to position the source at the start offset.
  ///
  /// Applies to uploads. If unset, that is left to the input factory. Defaults to `true`.
  pub seek_input_stream_automatically: bool,
  /// Whether to position the sink at the start offset.
  ///
  /// Applies to downloads. If unset, that is left to the output factory. Defaults to `true`.
  pub seek_output_stream_automatically: bool,
  /// Whether to flush and close the sink when the download ends, however it ends.
  ///
  /// Applies to downloads. Always set for a file. Defaults to `true`.
  pub close_output_stream_on_completion: bool,
  /// Time to wait in the peer's queue for it to start sending.
  ///
  /// Applies to downloads. If unset, the download waits as long as it takes. Unset by default.
  pub queued_timeout: Option<Duration>,
}

impl Default for TransferOptions {
  fn default() -> Self {
    Self {
      governor: None,
      reporter: None,
      state_changed: None,
      progress_updated: None,
      slot_awaiter: None,
      slot_released: None,
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
impl fmt::Debug for TransferOptions {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("TransferOptions")
      .field("governor", &self.governor.is_some())
      .field("reporter", &self.reporter.is_some())
      .field("state_changed", &self.state_changed.is_some())
      .field("progress_updated", &self.progress_updated.is_some())
      .field("slot_awaiter", &self.slot_awaiter.is_some())
      .field("slot_released", &self.slot_released.is_some())
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

/// Destination of a download's bytes.
#[non_exhaustive]
pub enum DownloadOutput {
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

/// Source of an upload's bytes.
#[non_exhaustive]
pub enum UploadInput {
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

/// One file to download from a peer.
#[derive(Debug)]
pub struct DownloadRequest {
  /// Username of the uploader.
  pub username: String,
  /// File as the peer names it.
  pub filename: RemoteFilename,
  /// Size the download expects.
  ///
  /// The uploader's size is checked against it. If unset, the download takes the size the
  /// uploader names.
  pub size: Option<u64>,
  /// Position at which to resume.
  ///
  /// Bytes before it are not asked for.
  pub start_offset: u64,
  /// Token of the transfer, or a fresh one when unset.
  pub token: Option<u32>,
  /// Destination of the bytes.
  pub output: DownloadOutput,
  /// How the download behaves.
  pub options: TransferOptions,
}

impl DownloadRequest {
  /// Creates a download of `filename` from `username` into `output`.
  ///
  /// The download runs from the start, with default options. `size` is the size expected, or
  /// `None` to take the uploader's.
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

/// One file to upload to a peer.
#[derive(Debug)]
pub struct UploadRequest {
  /// Username of the downloader.
  pub username: String,
  /// File as the peer asked for it.
  pub filename: RemoteFilename,
  /// Source of the bytes.
  pub input: UploadInput,
  /// Token of the transfer, or a fresh one when unset.
  pub token: Option<u32>,
  /// How the upload behaves.
  pub options: TransferOptions,
}

impl UploadRequest {
  /// Creates an upload of `input` to `username` as `filename`.
  ///
  /// The upload has default options.
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
