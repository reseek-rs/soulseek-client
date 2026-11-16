//! What the transfer suites share: files to send, a client that queues every upload a peer asks
//! for and hands the request to the test, and the states a transfer goes through.

use soulseek_client::transfer::{Handle, Transfer};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use soulseek_client::handler::{Handler, Upload, UploadRequest};
use soulseek_client::{Error, Event, Peer};
use soulseek_proto::RemoteFilename;
use tokio::sync::mpsc;

use super::PATIENCE;

static DIRECTORIES: AtomicU32 = AtomicU32::new(0);

/// an empty directory of its own under the target directory
pub fn scratch() -> PathBuf {
  let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
    "transfers-{}-{}",
    std::process::id(),
    DIRECTORIES.fetch_add(1, Ordering::Relaxed)
  ));
  // a directory left by an earlier run of the same process id goes first
  let _ = std::fs::remove_dir_all(&dir);
  std::fs::create_dir_all(&dir).unwrap();
  dir
}

/// `len` bytes that differ from one offset to the next, so a byte out of place shows
pub fn content(len: usize) -> Vec<u8> {
  (0..len).map(|n| (n * 7 % 251) as u8).collect()
}

/// a file of [`content`] in `dir`
pub fn file(dir: &std::path::Path, name: &str, len: usize) -> PathBuf {
  let path = dir.join(name);
  std::fs::write(&path, content(len)).unwrap();
  path
}

/// A peer's request to queue an upload, as the client's handler saw it
pub type Asked = mpsc::UnboundedReceiver<(String, RemoteFilename)>;

/// Queues every upload a peer asks for and hands the request to a receiver, which is where a
/// test starts the upload
pub struct Queueing {
  asked: mpsc::UnboundedSender<(String, RemoteFilename)>,
  place: Option<u32>,
}

/// a [`Queueing`] handler and the receiver of the requests it queues
pub fn queueing() -> (Queueing, Asked) {
  let (asked, requests) = mpsc::unbounded_channel();
  (Queueing { asked, place: None }, requests)
}

impl Queueing {
  /// answers every place in queue request with `place`
  pub fn at_place(mut self, place: u32) -> Self {
    self.place = Some(place);
    self
  }
}

impl Handler for Queueing {
  async fn upload(
    &self,
    _client: &soulseek_client::Client,
    peer: &Peer,
    request: &UploadRequest,
  ) -> Upload {
    // the test may have stopped listening; the request is queued either way
    let _ = self
      .asked
      .send((peer.username.clone(), request.filename.clone()));
    Upload::Queue
  }

  async fn place_in_queue(
    &self,
    _client: &soulseek_client::Client,
    _peer: &Peer,
    _filename: &RemoteFilename,
  ) -> Option<u32> {
    self.place
  }
}

/// the next upload a peer asked to queue
pub async fn asked(requests: &mut Asked) -> (String, RemoteFilename) {
  tokio::time::timeout(PATIENCE, requests.recv())
    .await
    .expect("the peer asks for the file")
    .unwrap()
}

/// The handle of a transfer once its peer has it queued or it is past that: a download not yet
/// asked for, or asked for and still unanswered, is waited on. One that ended without succeeding
/// gives its error.
pub async fn once_queued(started: Result<Handle, Error>) -> Result<Handle, Error> {
  use soulseek_client::transfer::{Direction, State};
  let mut handle = started?;
  let mut changes = handle.changes();
  while let Some(transfer) = changes.next().await {
    match transfer.state {
      State::Completed(_) if !transfer.state.succeeded() => {
        return Err((&mut handle).await.unwrap_err());
      }
      State::QueuedLocally | State::Requested if transfer.direction == Direction::Download => {}
      _ => break,
    }
  }
  Ok(handle)
}

/// Every state the next transfer going `direction` reported, until it ended, and its last
/// snapshot
pub async fn states(
  events: &mut soulseek_client::Events,
  direction: soulseek_client::transfer::Direction,
) -> (Vec<soulseek_client::transfer::State>, Transfer) {
  let mut states = Vec::new();
  let mut token = None;
  tokio::time::timeout(PATIENCE * 3, async {
    loop {
      let Event::Transfer(soulseek_client::transfer::Event::StateChanged { transfer, .. }) =
        events.next().await.unwrap().expect("no event was dropped")
      else {
        continue;
      };
      if transfer.direction != direction || token.is_some_and(|token| token != transfer.token) {
        continue;
      }
      token = Some(transfer.token);
      states.push(transfer.state);
      if transfer.state.is_terminal() {
        return (states, *transfer);
      }
    }
  })
  .await
  .expect("the transfer ends")
}

/// waits until one of `client`'s transfers has moved `bytes`
pub async fn reached(client: &soulseek_client::Client, bytes: u64) {
  let moved = || {
    let transfers = client.transfers();
    let all = transfers.downloads().into_iter().chain(transfers.uploads());
    all.map(|transfer| transfer.bytes_transferred).max()
  };
  tokio::time::timeout(PATIENCE, async {
    while moved().is_none_or(|moved| moved < bytes) {
      tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
  })
  .await
  .expect("the transfer moves that far");
}

/// the states a download goes through when it succeeds
pub fn download() -> Vec<soulseek_client::transfer::State> {
  vec![
    soulseek_client::transfer::State::QueuedLocally,
    soulseek_client::transfer::State::Requested,
    soulseek_client::transfer::State::QueuedRemotely,
    soulseek_client::transfer::State::Initializing,
    soulseek_client::transfer::State::InProgress,
    soulseek_client::transfer::State::Completed(soulseek_client::transfer::Outcome::Succeeded),
  ]
}

/// the states an upload goes through when it succeeds
pub fn upload() -> Vec<soulseek_client::transfer::State> {
  vec![
    soulseek_client::transfer::State::QueuedLocally,
    soulseek_client::transfer::State::Requested,
    soulseek_client::transfer::State::Initializing,
    soulseek_client::transfer::State::InProgress,
    soulseek_client::transfer::State::Completed(soulseek_client::transfer::Outcome::Succeeded),
  ]
}
