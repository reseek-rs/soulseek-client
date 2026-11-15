//! Moving a file's bytes over its transfer connection, in chunks of at most the connection's
//! buffer size. Before each chunk the caller's governor, when it set one, says how much may move,
//! and after it the caller's reporter hears what moved. Progress is reported after every chunk.
//! Also the uploader's wait for the downloader to close, and what an I/O failure on a closed
//! connection comes to.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, sleep, sleep_until};
use tokio_util::sync::CancellationToken;

use super::options::{DownloadSink, TransferGovernor, TransferReporter, UploadSource};
use super::record::Tracker;
use crate::conn::{ConnHandle, ConnReadHalf, ConnState, ConnWriteHalf, closed_by_remote};
use crate::error::{ClientError, ConnectionErrorKind};

/// how long a governor that permitted nothing is left alone before it is asked again
const GOVERNOR_PAUSE: Duration = Duration::from_millis(100);

/// How fast one transfer may go: as its governor allows, or as fast as it can without one
pub(super) struct Pace<'a> {
  pub(super) tracker: &'a Tracker,
  pub(super) governor: Option<TransferGovernor>,
  pub(super) reporter: Option<TransferReporter>,
  pub(super) cancel: &'a CancellationToken,
}

impl Pace<'_> {
  /// How many of `want` bytes the next chunk may move: at least one, waiting as long as the
  /// governor needs
  async fn grant(&self, want: u32) -> Result<u32, ClientError> {
    let Some(governor) = &self.governor else {
      return Ok(want);
    };
    loop {
      let transfer = self.tracker.snapshot();
      let permitted = tokio::select! {
        biased;
        () = self.cancel.cancelled() => return Err(ClientError::Canceled),
        permitted = governor(&transfer, want, self.cancel.clone()) => permitted,
      };
      let granted = want.min(permitted);
      if granted > 0 {
        return Ok(granted);
      }
      // a governor that permits nothing yet is asked again after a pause, so a misbehaving one
      // cannot spin the loop, and no zero-length read or write is made
      tracing::trace!("governor permitted nothing, asking again");
      tokio::select! {
        biased;
        () = self.cancel.cancelled() => return Err(ClientError::Canceled),
        () = sleep(GOVERNOR_PAUSE) => {}
      }
    }
  }

  /// after a chunk: reports it
  fn moved(&self, want: u32, granted: u32, moved: u32) {
    if let Some(reporter) = &self.reporter {
      reporter(&self.tracker.snapshot(), want, granted, moved);
    }
  }
}

/// the next chunk's size: the buffer, or what is left
fn chunk(buffer: usize, left: u64) -> u32 {
  u32::try_from(left.min(buffer as u64)).unwrap_or(u32::MAX)
}

/// Reads `len` bytes from the connection into `sink`, reporting the position from `start` on
/// after every chunk, then flushes the sink
pub(super) async fn receive(
  read: &mut ConnReadHalf,
  sink: &mut dyn DownloadSink,
  len: u64,
  start: u64,
  pace: &Pace<'_>,
) -> Result<(), ClientError> {
  let mut buffer = vec![0; read.handle().options().read_buffer_size.max(1)];
  let mut moved = 0;
  while moved < len {
    let want = chunk(buffer.len(), len - moved);
    let granted = pace.grant(want).await?;
    let received = read.read(&mut buffer[..granted as usize]).await?;
    sink
      .write_all(&buffer[..received])
      .await
      .map_err(|error| ClientError::io(format!("failed to write the download: {error}"), error))?;
    moved += received as u64;
    pace.moved(want, granted, received as u32);
    pace.tracker.emit_progress(start + moved);
  }
  sink
    .flush()
    .await
    .map_err(|error| ClientError::io(format!("failed to flush the download: {error}"), error))
}

/// Writes `len` bytes from `source` to the connection, reporting the position from `start` on
/// after every chunk. A source that ends before `len` bytes fails the upload.
pub(super) async fn send(
  write: &mut ConnWriteHalf,
  source: &mut dyn UploadSource,
  len: u64,
  start: u64,
  pace: &Pace<'_>,
) -> Result<(), ClientError> {
  let mut buffer = vec![0; write.handle().options().write_buffer_size.max(1)];
  let mut moved = 0;
  while moved < len {
    let want = chunk(buffer.len(), len - moved);
    let granted = pace.grant(want).await?;
    let read = source
      .read(&mut buffer[..granted as usize])
      .await
      .map_err(|error| ClientError::io(format!("failed to read the upload: {error}"), error))?;
    if read == 0 {
      return Err(ClientError::transfer(format!(
        "input ended prematurely after {moved} of {len} bytes"
      )));
    }
    write.write_all(&buffer[..read]).await?;
    moved += read as u64;
    pace.moved(want, granted, read as u32);
    pace.tracker.emit_progress(start + moved);
  }
  Ok(())
}

/// Reads and ignores what the downloader sends while an upload is written, until the connection
/// fails or closes, which is what this resolves with
pub(super) async fn closed(read: &mut ConnReadHalf) -> ClientError {
  let mut stray = [0; 64];
  loop {
    if let Err(error) = read.read(&mut stray).await {
      return error;
    }
  }
}

/// How an upload's wait for the downloader to close ended
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Linger {
  /// the downloader closed or reset the connection, which one that read every byte and closes
  /// abortively does as well as one that left bytes unread
  Closed,
  /// the downloader kept it open past the deadline, so the client closed it
  Forced,
  /// the caller canceled while waiting
  Canceled,
}

/// Waits up to `maximum` for the downloader to close the connection after the last byte, which
/// is how a downloader says it has everything; past that the connection is closed here
pub(super) async fn linger(
  read: &mut ConnReadHalf,
  maximum: Duration,
  cancel: &CancellationToken,
) -> Linger {
  let deadline = crate::later(Instant::now(), maximum);
  let mut stray = [0; 1];
  loop {
    tokio::select! {
      biased;
      () = cancel.cancelled() => return Linger::Canceled,
      read = read.read(&mut stray) => match read {
        // a byte the downloader had no reason to send; keep waiting for the close
        Ok(_) => {}
        Err(_) => return Linger::Closed,
      },
      () = sleep_until(deadline) => {
        read
          .handle()
          .disconnect("transfer complete, maximum linger time exceeded", None);
        tracing::warn!(
          maximum = ?maximum,
          "transfer connection forcibly closed after exceeding the maximum linger time"
        );
        return Linger::Forced;
      }
    }
  }
}

/// What an I/O failure on a transfer connection comes to: the reason the connection closed,
/// when it closed under the operation (its inactivity timeout, a cancel), else the failure. A
/// remote that ended or reset the connection is named, `remote` being who it is ("the
/// downloader"); whether it canceled, quit or crashed, the socket does not tell.
pub(super) fn connection_failure(
  handle: &ConnHandle,
  error: ClientError,
  remote: &str,
) -> ClientError {
  if !matches!(error, ClientError::Connection { .. }) {
    return error;
  }
  let ConnState::Disconnected(reason) = handle.state() else {
    return error;
  };
  match reason.error {
    Some(ClientError::Timeout(message)) => ClientError::Timeout(message),
    Some(ClientError::Canceled) => ClientError::Canceled,
    Some(error) if closed_by_remote(&error) => ClientError::connection(
      ConnectionErrorKind::RemoteClosed,
      format!("transfer failed: {remote} closed the connection"),
    ),
    _ => ClientError::connection(
      ConnectionErrorKind::Other,
      format!("transfer failed: {}", reason.message),
    ),
  }
}
