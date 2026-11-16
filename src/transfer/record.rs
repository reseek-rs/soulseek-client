//! What the client keeps about one transfer while it runs, the snapshot it hands out, and how
//! progress and speed are worked out. Every state change and progress update goes through the
//! transfer's tracker, which updates the record, calls the caller's callback and raises the event,
//! in that order, on the transfer's task; a progress update with a callback raises no event.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use soulseek_proto::RemoteFilename;
use tokio::sync::watch;
use tokio::time::Instant;

use super::options::{TransferProgressUpdated, TransferStateChanged};
use super::state::{Refused, State};
use crate::client::Client;
use crate::error::Error;
use crate::events::Event;
use crate::lock;

/// how long the current speed is measured over, and how long a transfer runs before it has a
/// speed
const SPEED_SAMPLE: Duration = Duration::from_secs(1);

/// Direction a transfer moves its file in, seen from the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Direction {
  /// The client receives the file.
  Download,
  /// The client sends the file.
  Upload,
}

/// Transfer as it was at one moment.
///
/// Handed to callbacks and events, and returned by the operations.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Transfer {
  /// Direction the file moves in, seen from the client.
  pub direction: Direction,
  /// Username of the other user.
  pub username: String,
  /// File as the peer names it, never a local path.
  pub filename: RemoteFilename,
  /// The client's token for the transfer.
  pub token: u32,
  /// Token the uploader gave the transfer when it offered to start it.
  ///
  /// Applies to downloads.
  pub remote_token: Option<u32>,
  /// Stage of the transfer in its life.
  pub state: State,
  /// Size expected, in bytes.
  ///
  /// For a download that expects none, 0 until the uploader names it.
  pub size: u64,
  /// Position in the file at which the transfer starts.
  pub start_offset: u64,
  /// Position reached: the start offset plus the bytes moved.
  pub bytes_transferred: u64,
  /// Number of bytes still to move.
  pub bytes_remaining: u64,
  /// How much has moved, from 0 to 100.
  pub percent_complete: f64,
  /// Speed over the whole transfer so far, in bytes per second.
  ///
  /// This is the bytes moved over the time since they started moving. It is 0 for the first
  /// second.
  pub average_speed: f64,
  /// Speed over the last second measured, in bytes per second.
  ///
  /// It is 0 for the first second and once the transfer is over.
  pub current_speed: f64,
  /// When bytes started moving, or when the transfer ended if they never did.
  pub start_time: Option<SystemTime>,
  /// When the transfer ended.
  pub end_time: Option<SystemTime>,
  /// Time from the start to the end, or to now while the transfer runs.
  pub elapsed_time: Option<Duration>,
  /// Time the bytes remaining take at the average speed.
  ///
  /// Unknown while the speed is 0.
  pub remaining_time: Option<Duration>,
  /// Far end of the transfer connection, once there is one.
  pub address: Option<SocketAddr>,
  /// Whether the downloader closed the connection after an upload sent every byte.
  ///
  /// Closing the connection is how the downloader says it has the file. The value is `false`
  /// when the downloader kept the connection open past the linger time and the client closed
  /// it, or when the upload was canceled while waiting. It is `None` for any transfer other
  /// than an upload that sent every byte.
  pub closed_by_peer: Option<bool>,
  /// Error that ended the transfer, unless it succeeded.
  pub error: Option<Error>,
}

/// The mutable record behind a transfer's snapshots
pub(crate) struct Record {
  direction: Direction,
  username: String,
  filename: RemoteFilename,
  token: u32,
  remote_token: Option<u32>,
  state: State,
  size: u64,
  start_offset: u64,
  bytes_transferred: u64,
  average_speed: f64,
  current_speed: f64,
  started: Option<(Instant, SystemTime)>,
  ended: Option<(Instant, SystemTime)>,
  /// when the current speed was last measured, and the position then
  sample: (Instant, u64),
  address: Option<SocketAddr>,
  closed_by_peer: Option<bool>,
  error: Option<Error>,
}

impl Record {
  pub(crate) fn new(
    direction: Direction,
    username: &str,
    filename: &RemoteFilename,
    token: u32,
    size: u64,
    start_offset: u64,
  ) -> Self {
    Self {
      direction,
      username: username.to_owned(),
      filename: filename.clone(),
      token,
      remote_token: None,
      state: State::QueuedLocally,
      size,
      start_offset,
      bytes_transferred: start_offset,
      average_speed: 0.0,
      started: None,
      ended: None,
      sample: (Instant::now(), start_offset),
      current_speed: 0.0,
      address: None,
      closed_by_peer: None,
      error: None,
    }
  }

  pub(crate) fn snapshot(&self) -> Transfer {
    let now = Instant::now();
    let bytes_remaining = self.size.saturating_sub(self.bytes_transferred);
    Transfer {
      direction: self.direction,
      username: self.username.clone(),
      filename: self.filename.clone(),
      token: self.token,
      remote_token: self.remote_token,
      state: self.state,
      size: self.size,
      start_offset: self.start_offset,
      bytes_transferred: self.bytes_transferred,
      bytes_remaining,
      percent_complete: if self.size > 0 {
        self.bytes_transferred as f64 / self.size as f64 * 100.0
      } else {
        0.0
      },
      average_speed: self.average_speed,
      current_speed: self.current_speed,
      start_time: self.started.map(|(_, at)| at),
      end_time: self.ended.map(|(_, at)| at),
      elapsed_time: self
        .started
        .map(|(started, _)| self.ended.map_or(now, |(ended, _)| ended) - started),
      // a size the peer named and a crawling speed can make this too long for a duration
      remaining_time: (self.average_speed > 0.0)
        .then(|| Duration::try_from_secs_f64(bytes_remaining as f64 / self.average_speed).ok())
        .flatten(),
      address: self.address,
      closed_by_peer: self.closed_by_peer,
      error: self.error.clone(),
    }
  }

  /// Moves to `state` when the state machine allows it, stamping the start on the state with
  /// bytes moving and the end on the completed one; a transfer that ends without bytes moving
  /// starts when it ends
  fn set_state(&mut self, state: State, now: Instant) -> Result<(), Refused> {
    self.state = self.state.to(state)?;
    if state == State::InProgress && self.started.is_none() {
      self.started = Some((now, SystemTime::now()));
      self.sample = (now, self.start_offset);
    }
    if state.is_terminal() && self.ended.is_none() {
      let ended = (now, SystemTime::now());
      self.ended = Some(ended);
      self.started.get_or_insert(ended);
      // the speed becomes the overall average
      self.set_progress(self.bytes_transferred, now);
    }
    Ok(())
  }

  /// Records the position `bytes` and the speeds. A transfer has none for its first second,
  /// unless it is over or every byte moved before that.
  fn set_progress(&mut self, bytes: u64, now: Instant) {
    self.bytes_transferred = bytes;
    let Some((started, _)) = self.started else {
      return;
    };
    let ended = self.ended.map(|(ended, _)| ended);
    let elapsed = ended.unwrap_or(now) - started;
    if ended.is_some() || bytes >= self.size || elapsed >= SPEED_SAMPLE {
      let moved = bytes.saturating_sub(self.start_offset) as f64;
      self.average_speed = moved / elapsed.max(Duration::from_millis(1)).as_secs_f64();
    }
    if ended.is_some() {
      self.current_speed = 0.0;
      return;
    }
    let (at, at_bytes) = self.sample;
    let since = now - at;
    if since >= SPEED_SAMPLE {
      self.current_speed = bytes.saturating_sub(at_bytes) as f64 / since.as_secs_f64();
      self.sample = (now, bytes);
    }
  }
}

/// Reports one transfer's changes: updates its record, then calls the caller's callback, then
/// raises the event; a progress update with a callback raises none
#[derive(Clone)]
pub(crate) struct Tracker {
  client: Client,
  record: Arc<Mutex<Record>>,
  state_changed: TransferStateChanged,
  progress_updated: TransferProgressUpdated,
}

impl Tracker {
  /// A tracker of `record`, with a receiver of the transfer as each state change and progress
  /// update leaves it
  pub(crate) fn followed(
    client: Client,
    record: Arc<Mutex<Record>>,
  ) -> (Self, watch::Receiver<Transfer>) {
    let (latest, following) = watch::channel(lock(&record).snapshot());
    let latest = Arc::new(latest);
    let state: TransferStateChanged = Arc::new({
      let latest = latest.clone();
      move |previous, transfer| {
        // the channel began with the first state, so its report changes nothing a reader saw
        latest.send_if_modified(|latest| {
          *latest = transfer.clone();
          previous.is_some()
        });
      }
    });
    let progress: TransferProgressUpdated = Arc::new(move |_, transfer| {
      latest.send_replace(transfer.clone());
    });
    let tracker = Self {
      client,
      record,
      state_changed: state,
      progress_updated: progress,
    };
    (tracker, following)
  }

  pub(crate) fn snapshot(&self) -> Transfer {
    lock(&self.record).snapshot()
  }

  pub(crate) fn state(&self) -> State {
    lock(&self.record).state
  }

  /// the position reached
  pub(crate) fn bytes_transferred(&self) -> u64 {
    lock(&self.record).bytes_transferred
  }

  pub(crate) fn update(&self, change: impl FnOnce(&mut Record)) {
    change(&mut lock(&self.record));
  }

  pub(crate) fn set_size(&self, size: u64) {
    self.update(|record| record.size = size);
  }

  pub(crate) fn set_remote_token(&self, token: u32) {
    self.update(|record| record.remote_token = Some(token));
  }

  pub(crate) fn set_address(&self, address: SocketAddr) {
    self.update(|record| record.address = Some(address));
  }

  /// an upload sent every byte, and `closed` says whether the downloader then closed
  pub(crate) fn set_closed_by_peer(&self, closed: bool) {
    self.update(|record| record.closed_by_peer = Some(closed));
  }

  /// the transfer starts at `offset`, as the downloader told the uploader
  pub(crate) fn set_start_offset(&self, offset: u64) {
    self.update(|record| {
      record.start_offset = offset;
      record.bytes_transferred = offset;
    });
  }

  /// Reports the state a new transfer starts in, with no state before it
  pub(crate) fn emit_first_state(&self) {
    self.report(None, self.snapshot());
  }

  /// Moves the transfer to `state` and reports it; a terminal state carries `error`. A move the
  /// state machine does not allow leaves the transfer as it is and fails as an invalid
  /// operation.
  pub(crate) fn emit_state(&self, state: State, error: Option<Error>) -> Result<(), Error> {
    let (previous, transfer) = {
      let mut record = lock(&self.record);
      let previous = record.state;
      record
        .set_state(state, Instant::now())
        .map_err(|refused| Error::invalid_operation(refused.to_string()))?;
      record.error = error;
      (previous, record.snapshot())
    };
    tracing::debug!(%previous, %state, "transfer state changed");
    self.report(Some(previous), transfer);
    Ok(())
  }

  /// calls the callback with the state change, then raises its event
  fn report(&self, previous: Option<State>, transfer: Transfer) {
    (self.state_changed)(previous, &transfer);
    self
      .client
      .emit(Event::Transfer(crate::transfer::Event::StateChanged {
        previous,
        transfer: Box::new(transfer),
      }));
  }

  /// records the position `bytes` and reports it to the transfer's handle
  pub(crate) fn emit_progress(&self, bytes: u64) {
    let (previous, transfer) = {
      let mut record = lock(&self.record);
      let previous = record.bytes_transferred;
      record.set_progress(bytes, Instant::now());
      (previous, record.snapshot())
    };
    tracing::trace!(bytes, "transfer progress");
    (self.progress_updated)(previous, &transfer);
  }
}

#[cfg(test)]
mod tests {
  use super::super::state::Outcome;
  use super::*;

  fn record(size: u64, start_offset: u64) -> Record {
    Record::new(
      Direction::Download,
      "bob",
      &"a.flac".into(),
      1,
      size,
      start_offset,
    )
  }

  /// moves `record` from Initializing to InProgress
  fn start(record: &mut Record) {
    record.state = State::Initializing;
    record.set_state(State::InProgress, Instant::now()).unwrap();
  }

  #[tokio::test(start_paused = true)]
  async fn speeds_are_zero_for_the_first_second_then_overall_and_of_the_last_second() {
    let mut record = record(10_000, 0);
    start(&mut record);
    tokio::time::advance(Duration::from_millis(500)).await;
    record.set_progress(500, Instant::now());
    assert_eq!((record.average_speed, record.current_speed), (0.0, 0.0));
    assert!(record.snapshot().remaining_time.is_none());
    tokio::time::advance(Duration::from_millis(500)).await;
    record.set_progress(1000, Instant::now());
    assert_eq!(
      (record.average_speed, record.current_speed),
      (1000.0, 1000.0)
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    record.set_progress(3000, Instant::now());
    // 3000 bytes in two seconds, 2000 of them in the last one
    assert_eq!(
      (record.average_speed, record.current_speed),
      (1500.0, 2000.0)
    );
    assert_eq!(
      record.snapshot().remaining_time,
      Some(Duration::from_secs_f64(7000.0 / 1500.0))
    );
  }

  #[tokio::test(start_paused = true)]
  async fn completion_makes_the_speed_the_overall_average_of_the_bytes_moved() {
    let mut record = record(4000, 1000);
    start(&mut record);
    tokio::time::advance(Duration::from_millis(1500)).await;
    record.set_progress(4000, Instant::now());
    record
      .set_state(State::Completed(Outcome::Succeeded), Instant::now())
      .unwrap();
    let transfer = record.snapshot();
    assert_eq!(
      (transfer.average_speed, transfer.current_speed),
      (2000.0, 0.0)
    );
    assert_eq!(transfer.bytes_transferred, 4000);
    assert_eq!(transfer.bytes_remaining, 0);
    assert_eq!(transfer.percent_complete, 100.0);
    assert_eq!(transfer.elapsed_time, Some(Duration::from_millis(1500)));
  }

  #[tokio::test(start_paused = true)]
  async fn a_transfer_that_ends_before_moving_bytes_starts_when_it_ends() {
    let mut record = record(4000, 0);
    tokio::time::advance(Duration::from_secs(3)).await;
    record
      .set_state(State::Completed(Outcome::Canceled), Instant::now())
      .unwrap();
    let transfer = record.snapshot();
    assert_eq!(transfer.start_time, transfer.end_time);
    assert_eq!(transfer.elapsed_time, Some(Duration::ZERO));
    assert_eq!(transfer.average_speed, 0.0);
  }

  #[tokio::test(start_paused = true)]
  async fn a_remaining_time_too_long_to_hold_is_unknown() {
    let mut record = record(u64::MAX, 0);
    start(&mut record);
    tokio::time::advance(Duration::from_secs(1000)).await;
    record.set_progress(1, Instant::now());
    assert!(record.average_speed > 0.0);
    assert_eq!(record.snapshot().remaining_time, None);
  }

  #[test]
  fn an_empty_file_is_zero_percent_complete() {
    assert_eq!(record(0, 0).snapshot().percent_complete, 0.0);
  }

  #[cfg(feature = "serde")]
  #[test]
  fn a_transfer_that_ended_with_an_error_serializes_it_and_its_filename_bytes() {
    use serde_json::{json, to_value};

    let filename = RemoteFilename::from_bytes(b"caf\xe9.mp3".to_vec());
    let mut record = Record::new(Direction::Download, "bob", &filename, 1, 4000, 0);
    record.state = State::Completed(Outcome::Errored);
    record.bytes_transferred = 1000;
    record.error = Some(Error::connection(
      crate::error::ConnectionKind::RemoteClosed,
      "the peer closed the transfer connection",
    ));
    assert_eq!(
      to_value(record.snapshot()).unwrap(),
      json!({
        "direction": "Download",
        "username": "bob",
        "filename": { "bytes": [99, 97, 102, 233, 46, 109, 112, 51] },
        "token": 1,
        "remote_token": null,
        "state": { "Completed": "Errored" },
        "size": 4000,
        "start_offset": 0,
        "bytes_transferred": 1000,
        "bytes_remaining": 3000,
        "percent_complete": 25.0,
        "average_speed": 0.0,
        "current_speed": 0.0,
        "start_time": null,
        "end_time": null,
        "elapsed_time": null,
        "remaining_time": null,
        "address": null,
        "closed_by_peer": null,
        "error": {
          "kind": { "Connection": "RemoteClosed" },
          "message": "the peer closed the transfer connection",
        },
      })
    );
  }
}
