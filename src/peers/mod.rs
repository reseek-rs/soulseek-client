//! Connections to other users. Message connections (`P`) are kept one per user: whoever asks
//! for one shares the attempt already under way, a peer that connects to us replaces the one we
//! had unless that one is open and the server gives the user another address, and an entry is
//! only ever removed by the attempt or connection it belongs to, so an old connection closing
//! never takes a newer one with it. Transfer connections (`F`) are made per file and never kept.

mod answer;
mod dispatch;
mod message;
mod solicit;
mod transfer;

pub(crate) use answer::Answering;
pub use dispatch::IncomingBrowse;
pub(crate) use message::{Unreached, request_endpoint};
pub(crate) use solicit::{Answer, Solicitation, Solicitations, Via, both_failed, race};

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use soulseek_proto::{PeerMessage, RemoteFilename};
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::conn::{ConnKind, MessageConn};
use crate::error::{ClientError, ConnectionErrorKind};
use crate::limit::Limiter;
use crate::lock;
use crate::waiter::{Scope, Waiter};

/// peer messages handled at once across every connection, of which one connection holds only a
/// few; at the bound a connection's reader waits for one of them to finish before reading its
/// next frame
const PEER_HANDLERS: usize = 256;

/// message connections kept open per user beside the current one for each reason they are kept;
/// a peer reconnecting over and over cannot hold more than this
const KEPT: usize = 2;

/// message connections one address may have opened to us and keep open, current, replaced or
/// still arriving; a few users behind one NAT fit, a host posing as many users cannot fill the
/// cache
pub(crate) const MESSAGE_CONNECTIONS_PER_ADDRESS: usize = 8;

type Outcome = Option<Result<MessageConn, ClientError>>;

/// One user's message connection, or the attempt that will produce it
#[derive(Clone)]
pub(crate) struct Entry {
  /// tells this entry apart from whatever replaces it under the same user
  id: u64,
  /// the token of the connection request we had the server relay for this attempt, for an
  /// attempt of ours
  request: Option<u32>,
  outcome: watch::Receiver<Outcome>,
}

impl Entry {
  /// the connection, once the attempt has produced one
  fn connection(&self) -> Option<MessageConn> {
    match &*self.outcome.borrow() {
      Some(Ok(conn)) => Some(conn.clone()),
      _ => None,
    }
  }

  /// waits for the attempt to finish
  async fn settled(mut self) -> Result<MessageConn, ClientError> {
    let outcome = self
      .outcome
      .wait_for(Option::is_some)
      .await
      .map_err(|_| ClientError::Canceled)?;
    outcome
      .clone()
      .expect("wait_for returned once the outcome is set")
  }
}

/// What the client keeps about connections to other users
pub(crate) struct PeerConnections {
  /// one entry per user
  cache: Mutex<HashMap<String, Entry>>,
  /// connections that are not their user's current one but stay open until they go idle,
  /// oldest first, by user and why they are kept
  kept: Mutex<HashMap<(String, Keep), VecDeque<MessageConn>>>,
  /// message connections from each address that introduced themselves and are not kept or
  /// refused yet, such as ones waiting for the server's word on that address
  arriving: Mutex<HashMap<IpAddr, usize>>,
  next_entry: AtomicU64,
  /// cancels an inbound indirect attempt still connecting, so a direct connection from the same
  /// user can replace it; keyed by user, with the id of the entry the attempt belongs to
  pending_inbound_indirect: Mutex<HashMap<String, (u64, CancellationToken)>>,
  /// the connection requests we had the server relay and still wait to be answered
  pub(crate) solicitations: Solicitations,
  /// transfer connections downloads wait for an uploader to open, by user and the token the
  /// download gave the uploader
  awaited_transfers: Mutex<HashMap<(String, u32), RemoteFilename>>,
  /// TCP connects to peers in flight at once
  connects: Limiter,
  /// peer messages being handled at once
  handlers: Arc<Semaphore>,
  /// fails the waits on a user's replies once their connection is gone
  waiter: Waiter,
}

impl PeerConnections {
  pub(crate) fn new(max_concurrent_connects: u32, waiter: Waiter) -> Self {
    Self {
      cache: Mutex::new(HashMap::new()),
      kept: Mutex::new(HashMap::new()),
      arriving: Mutex::new(HashMap::new()),
      next_entry: AtomicU64::new(0),
      pending_inbound_indirect: Mutex::new(HashMap::new()),
      solicitations: Solicitations::default(),
      awaited_transfers: Mutex::new(HashMap::new()),
      connects: Limiter::new(max_concurrent_connects),
      handlers: Arc::new(Semaphore::new(PEER_HANDLERS)),
      waiter,
    }
  }

  fn cache(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
    lock(&self.cache)
  }

  /// a fresh entry for an attempt, made with the connection request `request` when it is one of
  /// ours, with the sender that settles it
  fn new_entry(&self, request: Option<u32>) -> (Entry, watch::Sender<Outcome>) {
    let (settle, outcome) = watch::channel(None);
    (
      Entry {
        id: self.next_id(),
        request,
        outcome,
      },
      settle,
    )
  }

  /// an id no entry has had
  fn next_id(&self) -> u64 {
    self.next_entry.fetch_add(1, Ordering::Relaxed)
  }

  /// an entry already settled with `conn`
  fn settled_entry(&self, conn: MessageConn) -> Entry {
    let (entry, settle) = self.new_entry(None);
    settle.send_replace(Some(Ok(conn)));
    entry
  }

  /// whether `id` is still the entry the cache holds for `username`
  fn is_current(cache: &HashMap<String, Entry>, username: &str, id: u64) -> bool {
    cache.get(username).is_some_and(|entry| entry.id == id)
  }

  /// Removes the entry for `username` if it is still `id`, and then fails the waits on the
  /// user's replies, whose connection closed for `reason`; returns whether it was. Both happen
  /// under the cache's lock, so a caller that finds the entry gone, and connects and waits
  /// anew, does so after the waits failed and keeps its own.
  fn remove_closed(&self, username: &str, id: u64, reason: &str) -> bool {
    let mut cache = self.cache();
    let current = Self::is_current(&cache, username, id);
    if current {
      cache.remove(username);
      self.fail_waits(username, reason);
    }
    current
  }

  /// the cached connection to `username`, once its attempt has produced one
  pub(crate) fn cached(&self, username: &str) -> Option<MessageConn> {
    self.cache().get(username).and_then(Entry::connection)
  }

  /// `username`'s current connection, while it is open
  pub(crate) fn open(&self, username: &str) -> Option<MessageConn> {
    Self::open_in(&self.cache(), username)
  }

  /// [`PeerConnections::open`] in the locked `cache`
  fn open_in(cache: &HashMap<String, Entry>, username: &str) -> Option<MessageConn> {
    cache
      .get(username)
      .and_then(Entry::connection)
      .filter(|open| open.handle().is_open())
  }

  /// every user with an established message connection, and its address
  pub(crate) fn message_connections(&self) -> Vec<(String, SocketAddr)> {
    self
      .cache()
      .iter()
      .filter_map(|(username, entry)| {
        let conn = entry.connection()?;
        Some((username.clone(), conn.handle().remote()))
      })
      .collect()
  }

  /// Closes the connection to `username` after its entry left the cache, and fails the waits
  /// on that user's replies, which it can no longer bring. Its own watcher finds the entry gone
  /// and leaves the waits alone, so ones registered for a replacement are kept.
  fn close(&self, username: &str, conn: &MessageConn, reason: &str) {
    conn.handle().disconnect(reason, None);
    self.fail_waits(username, reason);
  }

  /// fails every wait on a reply from `username`, whose connection closed for `reason`
  fn fail_waits(&self, username: &str, reason: &str) {
    let faulted = self.waiter.fault_scope(
      &Scope::Peer(username.to_owned()),
      &ClientError::connection(
        ConnectionErrorKind::Other,
        format!("The message connection to {username} closed: {reason}"),
      ),
    );
    if faulted > 0 {
      tracing::debug!(
        username,
        faulted,
        "failed the waits on the closed message connection"
      );
    }
  }

  /// Makes room for one more entry when the cache is at `capacity`, by closing the established
  /// connection that has been idle longest. Attempts still connecting are never evicted.
  fn make_room(&self, cache: &mut HashMap<String, Entry>, capacity: usize) {
    if cache.len() < capacity {
      return;
    }
    let idlest = cache
      .iter()
      .filter_map(|(username, entry)| Some((username, entry.connection()?)))
      .max_by_key(|(_, conn)| conn.handle().inactive_time())
      .map(|(username, conn)| (username.clone(), conn));
    if let Some((username, conn)) = idlest {
      cache.remove(&username);
      tracing::debug!(username, "message connection evicted from the full cache");
      self.close(&username, &conn, "Evicted");
    }
  }

  /// the open message connections the remote at `ip` opened to us, current, replaced or still
  /// arriving
  fn opened_from(&self, cache: &HashMap<String, Entry>, ip: IpAddr) -> usize {
    let opened = |conn: &MessageConn| {
      let handle = conn.handle();
      handle.kind().contains(ConnKind::INBOUND) && handle.remote().ip() == ip && handle.is_open()
    };
    let current = cache
      .values()
      .filter_map(Entry::connection)
      .filter(opened)
      .count();
    let kept = lock(&self.kept)
      .values()
      .flatten()
      .filter(|conn| opened(conn))
      .count();
    let arriving = lock(&self.arriving).get(&ip).copied().unwrap_or(0);
    current + kept + arriving
  }

  /// Counts a message connection the remote at `ip` opened to us against the address's bound
  /// until the returned guard drops, once the connection is kept or refused; `None` when the
  /// address has as many open already
  pub(crate) fn arrive(&self, ip: IpAddr) -> Option<Arrival<'_>> {
    let cache = self.cache();
    if self.opened_from(&cache, ip) >= MESSAGE_CONNECTIONS_PER_ADDRESS {
      return None;
    }
    *lock(&self.arriving).entry(ip).or_default() += 1;
    Some(Arrival { peers: self, ip })
  }

  /// Keeps `conn`, a message connection naming `username` that is not the user's current one,
  /// open beside the current one for `why`. Past the last few kept for the same reason, the
  /// oldest is closed once its queued writes are out.
  fn keep(&self, username: &str, conn: MessageConn, why: Keep) {
    let mut kept = lock(&self.kept);
    // checked under the lock its watcher takes to forget it, so a closed one is never kept
    if !conn.handle().is_open() {
      return;
    }
    let list = kept.entry((username.to_owned(), why)).or_default();
    list.push_back(conn);
    if list.len() > KEPT
      && let Some(oldest) = list.pop_front()
    {
      tracing::debug!(
        username,
        remote = %oldest.handle().remote(),
        ?why,
        "oldest message connection kept beside the current one closed"
      );
      oldest.close_when_flushed("Superseded");
    }
  }

  /// forgets the kept connection `id` to `username` once it closed
  fn forget_kept(&self, username: &str, id: Uuid) {
    let mut kept = lock(&self.kept);
    for why in [Keep::Replaced, Keep::Beside] {
      let key = (username.to_owned(), why);
      if let Some(list) = kept.get_mut(&key) {
        list.retain(|conn| conn.handle().id() != id);
        if list.is_empty() {
          kept.remove(&key);
        }
      }
    }
  }

  /// cancels the inbound indirect attempt still connecting to `username`, if there is one
  fn cancel_inbound_indirect(&self, username: &str) {
    if let Some((_, cancel)) = lock(&self.pending_inbound_indirect).remove(username) {
      cancel.cancel();
    }
  }

  /// Forgets every pending connection request and closes every message connection; for when
  /// the client is disposed
  pub(crate) fn remove_and_dispose_all(&self) {
    self.solicitations.clear();
    for (_, (_, cancel)) in lock(&self.pending_inbound_indirect).drain() {
      cancel.cancel();
    }
    let kept = std::mem::take(&mut *lock(&self.kept));
    let current = self
      .cache()
      .drain()
      .filter_map(|(_, entry)| entry.connection())
      .collect::<Vec<_>>();
    for conn in current.iter().chain(kept.values().flatten()) {
      conn
        .handle()
        .disconnect("Client is being disposed", Some(ClientError::Disposed));
    }
  }
}

/// Why a message connection that is not its user's current one is kept open. Each reason keeps
/// its own few, so connections from an address the server does not give for the user never push
/// out the user's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Keep {
  /// the user's own, replaced by a newer one
  Replaced,
  /// one naming the user from an address the server does not give for it
  Beside,
}

/// A message connection from `ip` counted against the address's bound before it is kept
pub(crate) struct Arrival<'a> {
  peers: &'a PeerConnections,
  ip: IpAddr,
}

impl Drop for Arrival<'_> {
  fn drop(&mut self) {
    let mut arriving = lock(&self.peers.arriving);
    let count = arriving
      .get_mut(&self.ip)
      .expect("an arrival is counted until it drops");
    *count -= 1;
    if *count == 0 {
      arriving.remove(&self.ip);
    }
  }
}

/// writes `message` on a peer connection and waits until it is on the socket
pub(crate) async fn write_peer(
  conn: &MessageConn,
  message: PeerMessage,
) -> Result<(), ClientError> {
  let code = message.code();
  conn.write(message.encode()).await?;
  tracing::debug!(code = code.code(), name = code.name(), "peer message sent");
  Ok(())
}
