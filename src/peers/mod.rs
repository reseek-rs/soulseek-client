//! Connections to other users. Message connections (`P`) are kept one per user, the one the
//! client's requests to the user go on: whoever asks for one shares the attempt already under
//! way, and an entry is only ever removed by the attempt or connection it belongs to, so an old
//! connection closing never takes a newer one with it. A peer can give any name when it connects,
//! so the entry is only ever a connection that is the user's own: one the client made, one that
//! answered a ticket of the client's, or one from the address the server gives for the user. Any
//! other is served beside it. Transfer connections (`F`) are made per file and never kept.

mod answer;
mod dispatch;
mod message;
mod solicit;
mod transfer;

pub(crate) use answer::Answering;
pub use dispatch::IncomingBrowse;
pub(crate) use message::{Unreached, request_address};
pub(crate) use solicit::{Answer, Solicitation, Solicitations, Via, both_failed, race};

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use soulseek_proto::{PeerMessage, RemoteFilename};
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::conn::{ConnKind, MessageConn};
use crate::error::Error;
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

type Outcome = Option<Result<MessageConn, Error>>;

/// One user's message connection, or the attempt that will produce it
#[derive(Clone)]
pub(crate) struct Entry {
  /// tells this entry apart from whatever replaces it under the same user
  id: u64,
  /// the token of the connection request the client had the server relay for this attempt, for
  /// an attempt of the client's
  request: Option<u32>,
  /// canceled once a connection of the user's takes this entry's place in the cache
  superseded: CancellationToken,
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
  async fn settled(mut self) -> Result<MessageConn, Error> {
    let outcome = self
      .outcome
      .wait_for(Option::is_some)
      .await
      .map_err(|_| Error::canceled())?;
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
  /// the inbound indirect attempts still connecting; keyed by user, with the id of the entry
  /// the attempt belongs to, the address the server relayed the user's request with, and what
  /// stops the attempt
  pending_inbound_indirect: Mutex<HashMap<String, (u64, IpAddr, CancellationToken)>>,
  /// the connection requests the client had the server relay and still waits to be answered
  pub(crate) solicitations: Solicitations,
  /// transfer connections downloads wait for an uploader to open, by user and the token the
  /// download gave the uploader
  awaited_transfers: Mutex<HashMap<(String, u32), RemoteFilename>>,
  /// TCP connects in flight at once for connections the client wants
  connects: Limiter,
  /// TCP connects in flight at once to users that asked the server to have the client connect
  requested_connects: Limiter,
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
      requested_connects: Limiter::new(max_concurrent_connects),
      handlers: Arc::new(Semaphore::new(PEER_HANDLERS)),
      waiter,
    }
  }

  fn cache(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
    lock(&self.cache)
  }

  /// a fresh entry for an attempt, made with the connection request `request` when it is one of
  /// the client's, with the sender that settles it
  fn new_entry(&self, request: Option<u32>) -> (Entry, watch::Sender<Outcome>) {
    let (settle, outcome) = watch::channel(None);
    (
      Entry {
        id: self.next_id(),
        request,
        superseded: CancellationToken::new(),
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

  /// whether the cache holds an entry for `username` other than `id`
  fn replaced(cache: &HashMap<String, Entry>, username: &str, id: u64) -> bool {
    cache.get(username).is_some_and(|entry| entry.id != id)
  }

  /// Removes the entry for `username` if it is still `id`, its connection having closed for
  /// `reason`. A connection of the user's that it replaced and that is still open takes its
  /// place, and the waits on the user's replies stand: a peer that keeps the older of two
  /// connections and closes the newer answers on the older. Without one the waits fail. All of
  /// it happens under the cache's lock, so a caller that finds the entry gone, and connects and
  /// waits anew, does so after the waits failed and keeps its own.
  fn remove_closed(&self, username: &str, id: u64, reason: &str) -> Closed {
    let mut cache = self.cache();
    if !Self::is_current(&cache, username, id) {
      return Closed::NotCurrent;
    }
    cache.remove(username);
    let Some(older) = self.take_replaced(username) else {
      self.fail_waits(username, reason);
      return Closed::Gone;
    };
    let entry = self.settled_entry(older.clone());
    let id = entry.id;
    cache.insert(username.to_owned(), entry);
    Closed::Succeeded { id, by: older }
  }

  /// takes the newest connection of `username`'s own that a newer one replaced, if it is open
  fn take_replaced(&self, username: &str) -> Option<MessageConn> {
    let mut kept = lock(&self.kept);
    let key = (username.to_owned(), Keep::Replaced);
    let list = kept.get_mut(&key)?;
    let at = list.iter().rposition(|conn| conn.handle().is_open())?;
    let conn = list.remove(at);
    if list.is_empty() {
      kept.remove(&key);
    }
    conn
  }

  /// Takes out of the connections kept beside `username`'s an open one from `ip`, the address
  /// the server gives for the user
  fn take_beside(&self, username: &str, ip: IpAddr) -> Option<MessageConn> {
    let mut kept = lock(&self.kept);
    let key = (username.to_owned(), Keep::Beside);
    let list = kept.get_mut(&key)?;
    let at = list.iter().rposition(|conn| {
      let handle = conn.handle();
      handle.is_open() && same_ip(handle.remote().ip(), ip)
    })?;
    let conn = list.remove(at);
    if list.is_empty() {
      kept.remove(&key);
    }
    conn
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
  pub(crate) fn message_connections(&self) -> Vec<crate::user::MessageConnection> {
    self
      .cache()
      .iter()
      .filter_map(|(username, entry)| {
        let conn = entry.connection()?;
        Some(crate::user::MessageConnection {
          username: username.clone(),
          address: conn.handle().remote(),
        })
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
      &Error::connection(
        crate::error::ConnectionKind::Other,
        format!("the message connection to {username} closed: {reason}"),
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
  /// connection that has been idle longest. Attempts still connecting are never evicted, so a
  /// cache of attempts stays full. Whether there is room.
  fn make_room(&self, cache: &mut HashMap<String, Entry>, capacity: usize) -> bool {
    if cache.len() < capacity {
      return true;
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
    cache.len() < capacity
  }

  /// the open message connections the remote at `ip` opened to the client, current, replaced or
  /// still arriving
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

  /// Counts a message connection the remote at `ip` opened to the client against the address's
  /// bound until the returned guard drops, once the connection is kept or refused; `None` when
  /// the address has as many open already
  pub(crate) fn arrive(&self, ip: IpAddr, bound: u32) -> Option<Arrival<'_>> {
    let cache = self.cache();
    if self.opened_from(&cache, ip) >= bound as usize {
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

  /// [`PeerConnections::keep`] for a connection naming `username` that the user's address has
  /// not confirmed. Past `capacity` of those over every name, the idlest is closed once its
  /// queued writes are out, so names nobody confirms hold no more connections than the cache.
  fn keep_beside(&self, username: &str, conn: MessageConn, capacity: usize) {
    self.keep(username, conn, Keep::Beside);
    let mut kept = lock(&self.kept);
    let beside = || kept.iter().filter(|((_, why), _)| *why == Keep::Beside);
    if beside().map(|(_, list)| list.len()).sum::<usize>() <= capacity {
      return;
    }
    let idlest = beside()
      .flat_map(|(key, list)| {
        let idle = list.iter().map(|conn| conn.handle().inactive_time());
        idle.enumerate().map(move |(at, idle)| (key, at, idle))
      })
      .max_by_key(|(_, _, idle)| *idle)
      .map(|(key, at, _)| (key.clone(), at));
    let Some((key, at)) = idlest else {
      return;
    };
    let list = kept.get_mut(&key).expect("the idlest is in its list");
    let idlest = list.remove(at).expect("the idlest is in its list");
    if list.is_empty() {
      kept.remove(&key);
    }
    tracing::debug!(
      username = key.0,
      remote = %idlest.handle().remote(),
      capacity,
      "idlest message connection nobody confirmed closed"
    );
    idlest.close_when_flushed("Evicted");
  }

  /// forgets the kept connection `id` to `username` once it closed
  fn forget_kept(&self, username: &str, id: Uuid) {
    lock(&self.kept).retain(|(user, _), list| {
      if user == username {
        list.retain(|conn| conn.handle().id() != id);
      }
      !list.is_empty()
    });
  }

  /// Puts `entry` in `username`'s place in the locked `cache` and stops the attempt behind the
  /// entry it replaces, so whoever waits on that attempt looks again and finds this one
  fn replace(
    &self,
    cache: &mut HashMap<String, Entry>,
    username: &str,
    entry: Entry,
  ) -> Option<Entry> {
    let replaced = cache.insert(username.to_owned(), entry);
    if let Some(replaced) = &replaced {
      replaced.superseded.cancel();
    }
    lock(&self.pending_inbound_indirect).remove(username);
    replaced
  }

  /// whether an inbound indirect attempt to `username` is still connecting to `ip`, the address
  /// the server relayed the user's request with
  pub(crate) fn relayed_from(&self, username: &str, ip: IpAddr) -> bool {
    lock(&self.pending_inbound_indirect)
      .get(username)
      .is_some_and(|(_, relayed, _)| same_ip(*relayed, ip))
  }

  /// Forgets every pending connection request and closes every message connection; for when
  /// the client is shut down
  pub(crate) fn remove_and_close_all(&self) {
    self.solicitations.clear();
    for (_, (_, _, cancel)) in lock(&self.pending_inbound_indirect).drain() {
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
        .disconnect("client is shutting down", Some(Error::shutdown()));
    }
  }
}

/// What became of a user's entry when the connection behind it closed
pub(crate) enum Closed {
  /// the entry was another's already: the connection had been replaced
  NotCurrent,
  /// the entry is gone and the waits on the user's replies failed
  Gone,
  /// an older connection of the user's took the entry, as `id`
  Succeeded { id: u64, by: MessageConn },
}

/// Why a message connection that is not its user's current one is kept open. Each reason keeps
/// its own few, so connections nobody confirmed as the user's never push out the user's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Keep {
  /// the user's own, replaced by a newer one
  Replaced,
  /// one introducing itself under the user's name from an address the server has not given for
  /// the user
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

/// whether `a` and `b` are one address; an IPv4 address and the same address mapped into IPv6, as
/// a dual-stack listener sees it, are one
pub(crate) fn same_ip(a: IpAddr, b: IpAddr) -> bool {
  a.to_canonical() == b.to_canonical()
}

/// writes `message` on a peer connection and waits until it is on the socket
pub(crate) async fn write_peer(conn: &MessageConn, message: PeerMessage) -> Result<(), Error> {
  let code = message.code();
  conn.write(message.encode()).await?;
  tracing::debug!(code = code.code(), name = code.name(), "peer message sent");
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn an_address_mapped_into_ipv6_is_the_same_address() {
    let ip: IpAddr = "203.0.113.5".parse().unwrap();
    let mapped: IpAddr = "::ffff:203.0.113.5".parse().unwrap();
    assert!(same_ip(ip, mapped));
    assert!(same_ip(mapped, ip));
    assert!(!same_ip(ip, "203.0.113.6".parse().unwrap()));
  }
}
