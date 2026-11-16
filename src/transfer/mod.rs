//! Downloads and uploads.
//!
//! A transfer is set up with [`Client::download`] or [`Client::upload`] and runs on a task of its
//! own, through the [`State`]s from [`State::QueuedLocally`] to a completed one. The client tracks
//! every transfer from the moment it is accepted until its task has cleaned up, one transfer per
//! file, user and direction, with tokens unique across both directions. The downloads from one
//! user ask for their files in the order they started. The offer of a file a user sends unasked
//! is held until a download takes it, at most
//! [`unasked_offer_timeout`](crate::Config::unasked_offer_timeout) after the application accepted
//! it.

mod download;
mod io;
mod options;
mod record;
mod remote;
mod request;
mod size;
mod state;
mod task;
mod upload;

pub(crate) use options::Defaults;
pub use options::{Chunk, DownloadSink, RequestStyle, Slot, SlotGuard, UploadNotice, UploadSource};
pub use record::{Direction, Transfer};
pub(crate) use remote::DIRECTION_DOWNLOAD;
pub use request::{
  Changes, Control, Destination, Download, Handle, NoDestination, NoSource, Source, Transfers,
  Upload,
};
pub use state::{Outcome, ParseStateError, State};

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use soulseek_proto::RemoteFilename;
use soulseek_proto::peer::TransferRequest;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::cache::BoundedCache;
use crate::client::Client;
use crate::error::{Error, Repr};
use crate::lock;
use crate::token::TokenGenerator;
use record::Record;

/// peers whose broken resume offset was logged, so it is logged once per peer
const OFFSET_LOGGED_PEERS: usize = 1000;
/// how long a peer stays among those
const OFFSET_LOGGED_FOR: Duration = Duration::from_secs(24 * 3600);

/// what makes a transfer unique: the file, the user and the direction
type FileKey = (Direction, String, RemoteFilename);

fn file_key(direction: Direction, username: &str, filename: &RemoteFilename) -> FileKey {
  (direction, username.to_owned(), filename.clone())
}

/// where a download hears that its uploader refused or failed it
pub(crate) type RemoteFailures = watch::Receiver<Option<Error>>;

/// A tracked transfer
struct Tracked {
  direction: Direction,
  record: Arc<Mutex<Record>>,
  /// stops the transfer: the caller's cancellation, or the client's shutdown
  cancel: CancellationToken,
  /// downloads: fails the download wherever it is, when the uploader refuses or fails it
  remote_failure: Option<watch::Sender<Option<Error>>>,
  /// downloads: the uploader has been asked for the file, or its offer taken, so a refusal or
  /// failure it reports from now on can be about this download
  asked: bool,
  /// the transfer as it changes, once its task follows it
  latest: Option<record::Following>,
}

#[derive(Default)]
struct Table {
  by_token: HashMap<u32, Tracked>,
  by_file: HashMap<FileKey, u32>,
}

/// What the client keeps for its transfers
pub(crate) struct Registry {
  table: Mutex<Table>,
  /// peers that sent the broken resume offset, logged once each
  offset_logged: Mutex<BoundedCache<String, ()>>,
  /// the offers of files users send the client unasked, by user and file, until a download takes
  /// them
  offered: Mutex<HashMap<(String, RemoteFilename), (u64, TransferRequest)>>,
  /// numbers the offers held, so an offer's expiry never lets go of a later one with its token
  next_offer: AtomicU64,
  /// by user, the downloads that have yet to ask for their file, in the order they started
  request_turns: Mutex<HashMap<String, watch::Sender<VecDeque<u64>>>>,
  next_turn: AtomicU64,
}

/// Who and what a new transfer is
pub(crate) struct NewTransfer<'a> {
  pub(crate) direction: Direction,
  pub(crate) username: &'a str,
  pub(crate) filename: &'a RemoteFilename,
  pub(crate) size: u64,
  pub(crate) start_offset: u64,
  /// the caller's cancellation
  pub(crate) cancel: &'a CancellationToken,
}

/// A transfer's entry in the table, removed when dropped
pub(crate) struct Registration {
  transfers: Arc<Registry>,
  token: u32,
  key: FileKey,
  pub(crate) record: Arc<Mutex<Record>>,
  /// stops the transfer: canceled with the caller's cancellation or when the client is shut down
  pub(crate) cancel: CancellationToken,
}

impl Registration {
  /// lets the transfer be found by its token and followed through `latest`
  pub(crate) fn follow(&self, latest: &record::Following) {
    let mut table = self.transfers.table();
    let tracked = table
      .by_token
      .get_mut(&self.token)
      .expect("a registration's entry lives as long as it");
    tracked.latest = Some(latest.clone());
  }
}

impl Drop for Registration {
  fn drop(&mut self) {
    let mut table = self.transfers.table();
    table.by_token.remove(&self.token);
    if table.by_file.get(&self.key) == Some(&self.token) {
      table.by_file.remove(&self.key);
    }
  }
}

impl Registry {
  pub(crate) fn new() -> Self {
    Self {
      table: Mutex::new(Table::default()),
      offset_logged: Mutex::new(BoundedCache::new(OFFSET_LOGGED_PEERS, OFFSET_LOGGED_FOR)),
      offered: Mutex::new(HashMap::new()),
      next_offer: AtomicU64::new(0),
      request_turns: Mutex::new(HashMap::new()),
      next_turn: AtomicU64::new(0),
    }
  }

  fn table(&self) -> MutexGuard<'_, Table> {
    lock(&self.table)
  }

  /// the control of the transfer under `token`, once its task follows it
  pub(crate) fn control(&self, token: u32) -> Option<Control> {
    let table = self.table();
    let tracked = table.by_token.get(&token)?;
    Some(Control::new(
      tracked.cancel.clone(),
      tracked.latest.clone()?,
    ))
  }

  /// Takes `token`, or a fresh one, and the file for one transfer, in one step so two callers
  /// cannot both pass the check. A download also gets the channel its remote failures arrive on.
  pub(crate) fn register(
    self: &Arc<Self>,
    tokens: &TokenGenerator,
    token: Option<u32>,
    new: NewTransfer<'_>,
  ) -> Result<(Registration, Option<RemoteFailures>), Error> {
    let NewTransfer {
      direction,
      username,
      filename,
      size,
      start_offset,
      cancel,
    } = new;
    let mut table = self.table();
    let token = match token {
      Some(token) if table.by_token.contains_key(&token) => {
        return Err(Error::duplicate_token(token));
      }
      Some(token) => token,
      None => tokens.next_free(|token| table.by_token.contains_key(&token)),
    };
    let key = file_key(direction, username, filename);
    if table.by_file.contains_key(&key) {
      return Err(Error::from(Repr::DuplicateTransfer {
        username: username.to_owned(),
        filename: filename.to_string(),
      }));
    }
    let record = Arc::new(Mutex::new(Record::new(
      direction,
      username,
      filename,
      token,
      size,
      start_offset,
    )));
    let (remote_failure, failures) = match direction {
      Direction::Download => {
        let (sender, receiver) = watch::channel(None);
        (Some(sender), Some(receiver))
      }
      Direction::Upload => (None, None),
    };
    let cancel = cancel.child_token();
    table.by_token.insert(
      token,
      Tracked {
        direction,
        record: record.clone(),
        cancel: cancel.clone(),
        remote_failure,
        asked: false,
        latest: None,
      },
    );
    table.by_file.insert(key.clone(), token);
    Ok((
      Registration {
        transfers: self.clone(),
        token,
        key,
        record,
        cancel,
      },
      failures,
    ))
  }

  /// whether a download of `filename` from `username` is tracked
  pub(crate) fn has_download(&self, username: &str, filename: &RemoteFilename) -> bool {
    let key = file_key(Direction::Download, username, filename);
    self.table().by_file.contains_key(&key)
  }

  /// Holds the offer `username` made to send the client a file unasked, and returns its number;
  /// `None`, holding nothing, when an offer of the same file is held already. That one is being
  /// decided or was accepted, and the download that takes it must get the offer whose size was
  /// checked
  pub(crate) fn hold_offer(&self, username: &str, offer: TransferRequest) -> Option<u64> {
    let key = (username.to_owned(), offer.filename().clone());
    match lock(&self.offered).entry(key) {
      Entry::Occupied(_) => None,
      Entry::Vacant(vacant) => {
        let id = self.next_offer.fetch_add(1, Ordering::Relaxed);
        vacant.insert((id, offer));
        Some(id)
      }
    }
  }

  /// takes the offer held from `username` for `filename`, for the download that accepts it
  pub(crate) fn take_offer(
    &self,
    username: &str,
    filename: &RemoteFilename,
  ) -> Option<TransferRequest> {
    lock(&self.offered)
      .remove(&(username.to_owned(), filename.clone()))
      .map(|(_, offer)| offer)
  }

  /// Lets go of the offer `id` of `filename` from `username`, unless a download took it or
  /// another offer is held in its place; returns whether it was still held
  pub(crate) fn release_offer(&self, username: &str, filename: &RemoteFilename, id: u64) -> bool {
    let mut offered = lock(&self.offered);
    let key = (username.to_owned(), filename.clone());
    if offered.get(&key).is_some_and(|(held, _)| *held == id) {
      offered.remove(&key);
      return true;
    }
    false
  }

  /// The download `token` is about to ask its uploader for the file, or to take its offer: a
  /// refusal or failure the uploader reports from now on can be about it
  pub(crate) fn asked(&self, token: u32) {
    self
      .table()
      .by_token
      .get_mut(&token)
      .expect("a download is tracked while its task runs")
      .asked = true;
  }

  /// whether a download of `filename` from `username` is tracked and has asked for the file
  pub(crate) fn has_asked(&self, username: &str, filename: &RemoteFilename) -> bool {
    let table = self.table();
    let key = file_key(Direction::Download, username, filename);
    table
      .by_file
      .get(&key)
      .and_then(|token| table.by_token.get(token))
      .is_some_and(|tracked| tracked.asked)
  }

  /// Fails the download of `filename` from `username`, at whatever stage it is, with `error`.
  /// A download that has not asked for the file yet is left alone: the uploader cannot have
  /// refused or failed what it was not asked for, so the report is about an earlier download
  /// of the same file.
  pub(crate) fn fail_download(
    &self,
    username: &str,
    filename: &RemoteFilename,
    error: Error,
  ) -> RemoteFailure {
    let table = self.table();
    let key = file_key(Direction::Download, username, filename);
    let Some(tracked) = table
      .by_file
      .get(&key)
      .and_then(|token| table.by_token.get(token))
    else {
      return RemoteFailure::Untracked;
    };
    if !tracked.asked {
      return RemoteFailure::NotAsked;
    }
    if let Some(remote_failure) = &tracked.remote_failure {
      remote_failure.send_replace(Some(error));
    }
    RemoteFailure::Ended
  }

  /// Takes the last place among the downloads from `username` that have yet to ask for their
  /// file
  pub(crate) fn request_turn(self: &Arc<Self>, username: &str) -> RequestTurn {
    let id = self.next_turn.fetch_add(1, Ordering::Relaxed);
    lock(&self.request_turns)
      .entry(username.to_owned())
      .or_insert_with(|| watch::Sender::new(VecDeque::new()))
      .send_modify(|waiting| waiting.push_back(id));
    RequestTurn {
      transfers: self.clone(),
      username: username.to_owned(),
      id,
    }
  }

  /// stops every transfer, for when the client is shut down
  pub(crate) fn cancel_all(&self) {
    let table = self.table();
    if !table.by_token.is_empty() {
      tracing::debug!(count = table.by_token.len(), "transfers canceled");
    }
    for tracked in table.by_token.values() {
      tracked.cancel.cancel();
    }
  }

  /// snapshots of the tracked transfers going `direction`
  fn snapshots(&self, direction: Direction) -> Vec<Transfer> {
    self
      .table()
      .by_token
      .values()
      .filter(|tracked| tracked.direction == direction)
      .map(|tracked| lock(&tracked.record).snapshot())
      .collect()
  }

  /// Whether to log a broken resume offset from `username`: true the first time only
  pub(crate) fn first_bad_offset_from(&self, username: &str) -> bool {
    let mut logged = lock(&self.offset_logged);
    if logged.get(&username.to_owned()).is_some() {
      return false;
    }
    logged.insert(username.to_owned(), ());
    true
  }
}

/// What a refusal or failure the uploader reported did to the download it names
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RemoteFailure {
  /// the download ends with it
  Ended,
  /// the download has not asked for the file yet, so the report is about an earlier one
  NotAsked,
  /// no download of that file is tracked
  Untracked,
}

/// A download's place among the downloads from one user that have yet to ask for their file.
/// Requests to a user go out in the order the downloads started, so an album is queued at the
/// peer track by track. The downloads connect side by side, sharing the attempt, and each waits
/// only for the ones before it to write. Leaving the
/// place, by asking or by ending, lets the next one ask.
pub(crate) struct RequestTurn {
  transfers: Arc<Registry>,
  username: String,
  id: u64,
}

impl RequestTurn {
  /// resolves once every download from the user that started before this one has asked or ended
  pub(crate) async fn reached(&self) {
    self
      .waiting()
      .wait_for(|waiting| waiting.front() == Some(&self.id))
      .await
      .expect("a user's turns are kept while one of them is held");
  }

  /// follows the user's queue of downloads that have yet to ask
  fn waiting(&self) -> watch::Receiver<VecDeque<u64>> {
    lock(&self.transfers.request_turns)
      .get(&self.username)
      .expect("a user's turns are kept while one of them is held")
      .subscribe()
  }
}

impl Drop for RequestTurn {
  fn drop(&mut self) {
    let mut turns = lock(&self.transfers.request_turns);
    let Some(waiting) = turns.get(&self.username) else {
      return;
    };
    waiting.send_modify(|waiting| waiting.retain(|turn| *turn != self.id));
    if waiting.borrow().is_empty() {
      turns.remove(&self.username);
    }
  }
}

impl Client {
  /// Returns the client's transfers.
  pub fn transfers(&self) -> Transfers<'_> {
    Transfers { client: self }
  }
}

/// Something that happened to a transfer.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub enum Event {
  /// A peer refused a file the client asked for.
  DownloadDenied {
    /// Username of the uploader.
    username: String,
    /// File the peer refused.
    filename: soulseek_proto::RemoteFilename,
    /// Why, as the uploader sent it.
    message: String,
  },
  /// A peer reported that the upload of a file to the client failed.
  DownloadFailed {
    /// Username of the uploader.
    username: String,
    /// File whose upload failed.
    filename: soulseek_proto::RemoteFilename,
  },
  /// A peer told the client its place in the peer's queue for a file, unasked.
  ///
  /// A peer does so once it queued the file.
  PlaceInQueue {
    /// Username of the uploader.
    username: String,
    /// File the place is for.
    filename: soulseek_proto::RemoteFilename,
    /// Place of the file, counted from 1.
    ///
    /// It is read with [`queue_count`](soulseek_proto::queue_count).
    place: u32,
  },
  /// A download or upload moved from one state to another.
  ///
  /// A new transfer also reports the state it starts in, with no previous state.
  StateChanged {
    /// State left, or `None` for a new transfer.
    previous: Option<State>,
    /// Transfer as it is now.
    transfer: Box<Transfer>,
  },
}

#[cfg(test)]
mod tests {
  use super::*;

  fn offer(token: u32) -> TransferRequest {
    TransferRequest::Upload {
      token,
      filename: "@@share\\a.mp3".into(),
      file_size: Some(1),
    }
  }

  #[test]
  fn an_offers_expiry_never_lets_go_of_a_later_one_with_its_token() {
    let registry = Registry::new();
    let filename: RemoteFilename = "@@share\\a.mp3".into();
    let first = registry.hold_offer("bob", offer(5)).unwrap();
    assert!(registry.take_offer("bob", &filename).is_some());
    // bob offers the file again under the same token
    let second = registry.hold_offer("bob", offer(5)).unwrap();
    assert!(!registry.release_offer("bob", &filename, first));
    assert!(registry.release_offer("bob", &filename, second));
  }
}
