//! Message connections to other users: the one the client opens, which dials the user and asks the
//! server to have the user dial the client at the same time; one a user opens to the client
//! answering the client's request late, which replaces whatever the client had; one a peer opens to
//! the client introducing itself; and one the client opens because a user asked the server to have
//! the client connect. A connection introducing itself replaces the client's only from the address
//! the server gives for the user, which is asked when one of the client's is open. Any other is
//! served beside it, and becomes the user's once the server gives its address for the user: the one
//! the client looks up when it needs a connection to the user, or the one the server relays a
//! request of the user's to connect with. A replaced connection is not closed: what is queued on it
//! is still written and what the user sends on it is still read and answered, until it goes idle or
//! the user closes it. Per user, only the last two replaced ones and the last two nobody confirmed
//! stay, so those never push out the user's own; an older one closes once its queued writes are
//! out. One address opens at most a few at once, counted from when they introduce themselves, and
//! more are refused.

use std::collections::HashMap;
use std::net::SocketAddr;

use soulseek_proto::frame::FrameKind;
use soulseek_proto::server::{CannotConnectRequest, ConnectToPeerResponse};
use soulseek_proto::{PeerConnectionType, ServerRequest};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use super::solicit::{Ticket, Via, both_failed, race};
use super::{Arrival, Closed, Entry, Keep, Outcome, PeerConnections, lock};
use crate::client::Client;
use crate::conn::{Conn, ConnKind, Disconnect, MessageConn};
use crate::error::{ClientError, ConnectionErrorKind};
use crate::options::ConnectionOptions;

/// how many times a caller follows a connection that was replaced while it waited for it
const FOLLOW_REPLACEMENTS: usize = 3;

/// Why no message connection to a user could be had, and the token of the connection request
/// the client had the server relay for the attempt that failed, when it was an attempt of the
/// client's
pub(crate) struct Unreached {
  pub(crate) error: ClientError,
  pub(crate) request: Option<u32>,
}

impl Client {
  /// The message connection to `username`: the cached one, the attempt already under way, one
  /// the user opened to the client from that address's IP, or a new attempt that dials the user
  /// directly and asks the server to have the user dial the client, and keeps whichever connects
  /// first.
  /// The address is looked up when not given; a given one is taken as the server's word for the
  /// user. `cancel` stops this caller waiting; an attempt others share runs on.
  pub(crate) async fn message_connection(
    &self,
    username: &str,
    endpoint: Option<SocketAddr>,
    cancel: &CancellationToken,
  ) -> Result<MessageConn, ClientError> {
    self
      .reach(username, endpoint, cancel, |_| {})
      .await
      .map_err(|unreached| unreached.error)
  }

  /// [`Client::message_connection`] that, when it fails, also names the connection request the
  /// server relayed for the attempt it waited on, which the user may still act on late. Each
  /// attempt of the client's it waits on is told to `waits_on` by its request as the wait
  /// starts.
  pub(crate) async fn reach(
    &self,
    username: &str,
    endpoint: Option<SocketAddr>,
    cancel: &CancellationToken,
    mut waits_on: impl FnMut(u32),
  ) -> Result<MessageConn, Unreached> {
    let unreached = |error| Unreached {
      error,
      request: None,
    };
    self.check_not_shut_down().map_err(unreached)?;
    for _ in 0..FOLLOW_REPLACEMENTS {
      let cached = self.peers().cache().get(username).cloned();
      let entry = match cached {
        Some(entry) => {
          tracing::debug!(username, "retrieved cached message connection");
          entry
        }
        None => {
          let endpoint = match endpoint {
            Some(endpoint) => endpoint,
            None => tokio::select! {
              biased;
              () = cancel.cancelled() => return Err(unreached(ClientError::Canceled)),
              endpoint = self.get_user_endpoint(username) => endpoint.map_err(unreached)?,
            },
          };
          self.start_outbound(username, endpoint)
        }
      };
      let (id, request) = (entry.id, entry.request);
      // a settled attempt's request was answered or given up, and nothing rides on it now
      if let Some(request) = request
        && entry.connection().is_none()
      {
        waits_on(request);
      }
      let conn = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(unreached(ClientError::Canceled)),
        settled = entry.settled() => settled,
      };
      let conn = match conn {
        Ok(conn) => conn,
        // the attempt failed after another connection or attempt took its entry, as a dial
        // canceled for the connection the user opened does: the next look finds that one
        Err(_) if PeerConnections::replaced(&self.peers().cache(), username, id) => continue,
        Err(error) => return Err(Unreached { error, request }),
      };
      let current = PeerConnections::is_current(&self.peers().cache(), username, id);
      if current && conn.handle().is_open() {
        return Ok(conn);
      }
      // closed before its own watcher got to it: this caller does what the watcher would, so
      // the waits on the user still fail with the connection's own reason
      if current {
        let reason = conn.handle().disconnected().await;
        self.message_connection_closed(username, id, conn.handle().id(), &reason);
      }
      // the next look finds what replaced it
    }
    Err(unreached(ClientError::connection(
      ConnectionErrorKind::Other,
      format!("the message connection to {username} kept being replaced while it was established"),
    )))
  }

  /// The entry for a new outbound attempt to `username` at `endpoint`, the one another caller
  /// put there first, or one for a connection the user opened to the client from `endpoint`'s
  /// address, which is the user's now that the address is known
  fn start_outbound(&self, username: &str, endpoint: SocketAddr) -> Entry {
    let peers = self.peers();
    let mut cache = peers.cache();
    if let Some(entry) = cache.get(username) {
      return entry.clone();
    }
    peers.make_room(
      &mut cache,
      self.options().limits.max_cached_peer_connections as usize,
    );
    if let Some(entry) = self.take_confirmed(&mut cache, username, endpoint) {
      return entry;
    }
    let options = self.options().peer_connection_options.clone();
    let ticket = self.issue_ticket(username, PeerConnectionType::Peer, options.connect_timeout);
    let (entry, settle) = peers.new_entry(Some(ticket.token()));
    cache.insert(username.to_owned(), entry.clone());
    drop(cache);
    let client = self.clone();
    let username = username.to_owned();
    let id = entry.id;
    let span = tracing::debug_span!("message_connection", username, %endpoint);
    tokio::spawn(
      async move {
        let lifetime = client.cancellation().clone();
        let result = tokio::select! {
          () = lifetime.cancelled() => Err(ClientError::Shutdown),
          result = client.establish_message_connection(&username, endpoint, ticket, options) => result,
        };
        if let Err(error) = &result {
          tracing::debug!(%error, "outbound message connection failed");
        }
        client.settle(&username, id, settle, result);
      }
      .instrument(span),
    );
    entry
  }

  /// dials `username` and asks the server, with `ticket`, to have the user dial the client,
  /// keeping whichever connects first, then introduces the client on a connection it dialed
  async fn establish_message_connection(
    &self,
    username: &str,
    endpoint: SocketAddr,
    ticket: Ticket,
    options: ConnectionOptions,
  ) -> Result<MessageConn, ClientError> {
    let own = self.own_username("establish a peer connection")?;
    let stop = self.cancellation().child_token();
    let direct = self.connect_to_peer_socket(
      endpoint,
      username,
      Via::Direct.kind(),
      options.clone(),
      &stop,
    );
    let indirect = self.solicit(ticket, options.clone(), &stop);
    let (conn, via) = race(direct, indirect, &stop).await.map_err(|errors| {
      both_failed(
        format!(
          "failed to establish a direct or indirect message connection to {username} ({endpoint})"
        ),
        errors,
      )
    })?;
    tracing::debug!(?via, "message connection established");
    let conn = self.peer_message_connection(conn)?;
    self.start_peer_reading(&conn, true);
    if let Some(introduction) = via.introduction(&own, PeerConnectionType::Peer)
      && let Err(error) = conn.write(introduction).await
    {
      conn
        .handle()
        .disconnect(format!("write error: {error}"), Some(error.clone()));
      return Err(ClientError::connection(
        ConnectionErrorKind::Other,
        format!("failed to negotiate message connection to {username} ({endpoint}): {error}"),
      ));
    }
    Ok(conn)
  }

  /// A peer connected to the client for messages, introducing itself or answering a connection
  /// request of the client's. One `vouched` for, by a ticket of the client's or by the server
  /// giving its address for the user, replaces whatever the client had for that user, since the
  /// peer considers this one current, and takes the client's writes from now on; the one it
  /// replaces stays open until it goes idle. One that is not is served beside it the same way
  /// and takes none of the client's writes. `kind` says how it was made. One the remote opened
  /// to the client comes with its `arrival`, which counts it against its address until it is
  /// counted as open.
  pub(crate) fn add_or_update_message_connection(
    &self,
    username: &str,
    provisional: Conn,
    kind: ConnKind,
    vouched: bool,
    arrival: Option<Arrival<'_>>,
  ) -> Result<(), ClientError> {
    self.check_not_shut_down()?;
    let remote = provisional.handle().remote();
    let conn = provisional
      .handoff()
      .and_then(|stream| {
        Conn::from_handoff(
          stream,
          Some(username.to_owned()),
          kind,
          self.options().peer_connection_options.clone(),
        )
      })
      .and_then(|conn| self.peer_message_connection(conn))
      .map_err(|error| {
        ClientError::connection(
          ConnectionErrorKind::Other,
          format!(
            "failed to establish an inbound message connection to {username} ({remote}): {error}"
          ),
        )
      })?;
    let peers = self.peers();
    let replaced = {
      let mut cache = peers.cache();
      if !vouched {
        self.watch_message_connection(username, peers.next_id(), &conn);
        peers.keep_beside(
          username,
          conn.clone(),
          self.options().limits.max_cached_peer_connections as usize,
        );
        // counted as kept before it stops counting as arriving, under the lock the count takes
        drop(arrival);
        let open = PeerConnections::open_in(&cache, username);
        drop(cache);
        match open {
          Some(open) => tracing::debug!(
            username,
            %remote,
            open = %open.handle().remote(),
            "message connection from an address the server does not give for the user, kept beside the open one"
          ),
          None => tracing::debug!(
            username,
            %remote,
            "message connection not confirmed as the user's, kept beside"
          ),
        }
        self.start_peer_reading(&conn, false);
        return Ok(());
      }
      if !cache.contains_key(username) {
        peers.make_room(
          &mut cache,
          self.options().limits.max_cached_peer_connections as usize,
        );
      }
      let entry = peers.settled_entry(conn.clone());
      let replaced = cache.insert(username.to_owned(), entry.clone());
      drop(arrival);
      self.watch_message_connection(username, entry.id, &conn);
      replaced
    };
    peers.cancel_inbound_indirect(username);
    self.start_peer_reading(&conn, true);
    // an attempt still connecting finds it was replaced when it settles
    match replaced.as_ref().and_then(Entry::connection) {
      Some(old) => {
        tracing::debug!(
          username,
          %remote,
          ?kind,
          replaced = %old.handle().remote(),
          "message connection replaced"
        );
        peers.keep(username, old, Keep::Replaced);
      }
      None => tracing::debug!(username, %remote, ?kind, "inbound message connection"),
    }
    Ok(())
  }

  /// A user asked the server to have the client connect to it for messages: the client dials it
  /// and answers with the token, unless it has or is making a connection to that user already.
  /// A failure is reported to the server so the user stops waiting.
  pub(crate) fn connect_to_peer_for_messages(&self, request: ConnectToPeerResponse) {
    let username = request.username;
    let Some(endpoint) = request_endpoint(request.ip, request.port) else {
      tracing::debug!(
        username,
        port = request.port,
        "connection request with an invalid port"
      );
      self.send_cannot_connect(request.token, &username);
      return;
    };
    let peers = self.peers();
    let mut cache = peers.cache();
    if cache.contains_key(&username) {
      tracing::debug!(username, "connection request ignored; a connection exists");
      return;
    }
    peers.make_room(
      &mut cache,
      self.options().limits.max_cached_peer_connections as usize,
    );
    // the server relays the user's address with the request
    if self
      .take_confirmed(&mut cache, &username, endpoint)
      .is_some()
    {
      tracing::debug!(
        username,
        "connection request ignored; the user's connection exists"
      );
      return;
    }
    let (entry, settle) = peers.new_entry(None);
    cache.insert(username.clone(), entry.clone());
    // under the cache lock, so a connection the user opens to the client once the entry is in
    // finds the attempt to cancel
    let cancel = self.cancellation().child_token();
    lock(&peers.pending_inbound_indirect)
      .insert(username.clone(), (entry.id, endpoint.ip(), cancel.clone()));
    drop(cache);
    let client = self.clone();
    let token = request.token;
    let span = tracing::debug_span!("message_connection", username, %endpoint, token);
    tokio::spawn(
      async move {
        let options = client.options().peer_connection_options.clone();
        let result = client
          .pierce(&username, endpoint, token, options, &cancel)
          .await
          .and_then(|conn| client.peer_message_connection(conn));
        {
          let mut pending = lock(&client.peers().pending_inbound_indirect);
          if pending
            .get(&username)
            .is_some_and(|(id, _, _)| *id == entry.id)
          {
            pending.remove(&username);
          }
        }
        match &result {
          Ok(conn) => client.start_peer_reading(conn, true),
          // a direct connection from the user replaced this one
          Err(ClientError::Canceled) => {
            tracing::debug!("inbound indirect message connection superseded")
          }
          Err(error) => {
            tracing::debug!(%error, "failed to establish an inbound indirect message connection");
            client.send_cannot_connect(token, &username);
          }
        }
        client.settle(&username, entry.id, settle, result);
      }
      .instrument(span),
    );
  }

  /// Makes an open connection kept beside `username`'s from the address of `endpoint`, which
  /// the server gives for the user, the user's current one in the locked `cache`, which has room
  /// for it; its entry, when there was one
  fn take_confirmed(
    &self,
    cache: &mut HashMap<String, Entry>,
    username: &str,
    endpoint: SocketAddr,
  ) -> Option<Entry> {
    let peers = self.peers();
    let conn = peers.take_beside(username, endpoint.ip())?;
    let entry = peers.settled_entry(conn.clone());
    cache.insert(username.to_owned(), entry.clone());
    // its watcher from when it was kept beside finds no entry of its own when it closes
    self.watch_message_connection(username, entry.id, &conn);
    tracing::debug!(
      username,
      remote = %conn.handle().remote(),
      "message connection from the user's address taken as theirs"
    );
    Some(entry)
  }

  /// Settles the attempt behind entry `id` with `result`. A connection whose entry was replaced
  /// meanwhile is left to go idle like any replaced one; a failed attempt leaves the cache unless
  /// something replaced it.
  fn settle(
    &self,
    username: &str,
    id: u64,
    settle: watch::Sender<Outcome>,
    result: Result<MessageConn, ClientError>,
  ) {
    let peers = self.peers();
    let mut cache = peers.cache();
    let current = PeerConnections::is_current(&cache, username, id);
    match &result {
      Ok(conn) if current => self.watch_message_connection(username, id, conn),
      Ok(conn) => {
        tracing::debug!(
          username,
          "message connection made after another replaced it"
        );
        self.watch_message_connection(username, id, conn);
        peers.keep(username, conn.clone(), Keep::Replaced);
      }
      Err(_) if current => {
        cache.remove(username);
      }
      Err(_) => {}
    }
    settle.send_replace(Some(result));
  }

  /// Follows the connection behind entry `id` until it closes
  fn watch_message_connection(&self, username: &str, id: u64, conn: &MessageConn) {
    let client = self.downgrade();
    let handle = conn.handle().clone();
    let username = username.to_owned();
    tokio::spawn(
      async move {
        let reason = handle.disconnected().await;
        if let Some(client) = client.upgrade() {
          client.message_connection_closed(&username, id, handle.id(), &reason);
        }
      }
      .instrument(conn.handle().span().clone()),
    );
  }

  /// The connection `conn` behind entry `id` closed for `reason`. If the entry is still
  /// current it leaves the cache: an older connection of the user's that is still open takes
  /// its place, and without one the waits for the user's replies fail. One kept beside the
  /// current one is forgotten. Its watcher and a caller that found it closed first both come
  /// here, and only the one that removes the entry acts on it.
  fn message_connection_closed(&self, username: &str, id: u64, conn: Uuid, reason: &Disconnect) {
    match self.peers().remove_closed(username, id, &reason.message) {
      // an entry that is gone was replaced, or closed with its waits failed already
      Closed::NotCurrent => self.peers().forget_kept(username, conn),
      Closed::Gone => tracing::debug!(
        username,
        reason = reason.message,
        "message connection closed"
      ),
      Closed::Succeeded { id, by } => {
        tracing::debug!(
          username,
          reason = reason.message,
          remote = %by.handle().remote(),
          "message connection closed; the one it replaced is current again"
        );
        // its watcher from when it was current finds no entry of its own when it closes
        self.watch_message_connection(username, id, &by);
      }
    }
  }

  /// a peer connection made into a message connection with the configured frame limits
  fn peer_message_connection(&self, conn: Conn) -> Result<MessageConn, ClientError> {
    conn.into_message(FrameKind::Peer, self.options().limits.decode)
  }

  /// Tells the server the client could not act on the connection request `token` from
  /// `username`, so the user stops waiting for it. Best effort.
  pub(crate) fn send_cannot_connect(&self, token: u32, username: &str) {
    self.send_server_message(&ServerRequest::CannotConnect(CannotConnectRequest {
      token,
      username: username.to_owned(),
    }));
  }
}

/// the address a `ConnectToPeer` names, unless its port does not fit
pub(crate) fn request_endpoint(ip: std::net::Ipv4Addr, port: u32) -> Option<SocketAddr> {
  let port = u16::try_from(port).ok()?;
  Some(SocketAddr::from((ip, port)))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::conn::tests::pair;
  use crate::options::ClientOptions;

  #[tokio::test]
  async fn a_connection_kept_beside_is_the_users_once_the_address_given_is_its_own() {
    let client = Client::new(ClientOptions {
      enable_listener: false,
      ..ClientOptions::default()
    })
    .unwrap();
    let inbound = ConnKind::INBOUND | ConnKind::DIRECT;
    let (first, _first) = pair(ConnectionOptions::default()).await;
    let (second, _second) = pair(ConnectionOptions::default()).await;
    let newcomer = second.handle().remote();
    client
      .add_or_update_message_connection("bob", first, inbound, true, None)
      .unwrap();
    // closed, and nothing that would see it has run since
    let cached = client.peers().cached("bob").unwrap();
    cached.handle().disconnect("remote connection closed", None);
    // one nothing vouches for is served beside, never in its place
    client
      .add_or_update_message_connection("bob", second, inbound, false, None)
      .unwrap();
    assert_ne!(client.message_connection_address("bob"), Some(newcomer));
    // the address given for bob is the newcomer's, so it is bob's, and nothing is dialed
    let given = SocketAddr::from((newcomer.ip(), 2234));
    let taken = client
      .message_connection("bob", Some(given), &CancellationToken::new())
      .await
      .unwrap();
    assert_eq!(taken.handle().remote(), newcomer);
    assert_eq!(client.message_connection_address("bob"), Some(newcomer));
  }
}
