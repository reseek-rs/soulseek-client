//! Finding where another user listens. The server knows; a bounded cache remembers each answer
//! for a minute by default, and lookups for one user take turns so concurrent callers share one
//! request. An address that refuses a connection or does not answer one in time is forgotten, so
//! the next lookup asks the server again. The per-user turn locks that nobody holds are swept
//! every five minutes. Whether a connection comes from where the server says a user is also asks
//! the server again when the cached address is another, since the user may have moved, once per
//! address the connections come from while the answer stands.

use crate::user::Users;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use soulseek_proto::server::{GetPeerAddressRequest, GetPeerAddressResponse};
use soulseek_proto::{ServerCode, ServerRequest};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::Client;
use super::request::{failed, non_blank, user_key};
use crate::cache::BoundedCache;
use crate::error::{Error, MessageErrorKind};
use crate::lock;
use crate::peers::same_ip;

/// how often idle per-user locks are dropped
const LOCK_CLEANUP: Duration = Duration::from_secs(300);

/// What the client keeps for address lookups
pub(super) struct Endpoints {
  cache: Mutex<BoundedCache<String, SocketAddr>>,
  locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
  /// by user and an address a connection naming the user came from, the address the server gave
  /// for the user when asked because of it, or `None` when the lookup failed or the server said
  /// the user is offline
  refuted: Mutex<BoundedCache<(String, IpAddr), Option<SocketAddr>>>,
}

impl Endpoints {
  pub(super) fn new(capacity: u32, lifetime: Duration) -> Self {
    Self {
      cache: Mutex::new(BoundedCache::new(capacity as usize, lifetime)),
      locks: Mutex::new(HashMap::new()),
      refuted: Mutex::new(BoundedCache::new(capacity as usize, lifetime)),
    }
  }

  fn cached(&self, username: &str) -> Option<SocketAddr> {
    lock(&self.cache).get(&username.to_owned()).copied()
  }

  fn peek(&self, username: &str) -> Option<SocketAddr> {
    lock(&self.cache).peek(&username.to_owned()).copied()
  }

  /// the turn lookups for `username` take
  fn turn(&self, username: &str) -> Arc<tokio::sync::Mutex<()>> {
    lock(&self.locks)
      .entry(username.to_owned())
      .or_default()
      .clone()
  }

  /// follows a change of the cache bound and of how long an address is remembered
  pub(super) fn resize(&self, capacity: u32, lifetime: Duration) {
    let mut cache = lock(&self.cache);
    cache.set_capacity(capacity as usize);
    cache.set_ttl(lifetime);
    let mut refuted = lock(&self.refuted);
    refuted.set_capacity(capacity as usize);
    refuted.set_ttl(lifetime);
  }

  /// forgets `username`'s address when it is still `endpoint`; a newer one stays
  fn forget(&self, username: &str, endpoint: SocketAddr) -> bool {
    let mut cache = lock(&self.cache);
    let key = username.to_owned();
    if cache.get(&key) != Some(&endpoint) {
      return false;
    }
    cache.remove(&key);
    true
  }
}

/// Whether a failed connect means nothing takes connections at that address now: it was
/// refused, or it was not answered in time.
fn unreachable(error: &Error) -> bool {
  match error {
    Error::Timeout(_) => true,
    Error::Connection {
      source: Some(source),
      ..
    } => {
      if let Some(io) = source.downcast_ref::<std::io::Error>() {
        return io.kind() == std::io::ErrorKind::ConnectionRefused;
      }
      source.downcast_ref::<Error>().is_some_and(unreachable)
    }
    _ => false,
  }
}

impl Client {
  /// Asks the server where `username` listens and caches the answer; for a caller holding the
  /// user's turn
  async fn ask_user_endpoint(&self, username: &str) -> Result<SocketAddr, Error> {
    let lookup_failed = |error: Error| match error {
      Error::Timeout(_) | Error::Canceled => error,
      error => Error::UserEndpoint {
        username: username.to_owned(),
        source: Arc::new(error),
      },
    };
    let reply = self.waiter().wait::<GetPeerAddressResponse>(
      user_key(ServerCode::GetPeerAddress, username),
      None,
      CancellationToken::new(),
    );
    self
      .write_server_message(&ServerRequest::GetPeerAddress(GetPeerAddressRequest {
        username: username.to_owned(),
      }))
      .await
      .map_err(lookup_failed)?;
    let address = reply.await.map_err(lookup_failed)?;
    if address.ip.is_unspecified() {
      return Err(Error::UserOffline(username.to_owned()));
    }
    let port = u16::try_from(address.port).map_err(|_| Error::UserEndpoint {
      username: username.to_owned(),
      source: Arc::new(Error::Message {
        kind: MessageErrorKind::Other,
        message: format!("the port {} is out of range", address.port),
        source: None,
      }),
    })?;
    let endpoint = SocketAddr::from((address.ip, port));
    lock(&self.inner.endpoints.cache).insert(username.to_owned(), endpoint);
    tracing::debug!(username, %endpoint, "address cache miss");
    Ok(endpoint)
  }

  /// Whether the server gives `ip` as `username`'s address. A cached address that matches
  /// answers; otherwise the server is asked again, unless it was asked about `ip` already and
  /// gave the address still cached, failed or said the user is offline, or a caller ahead in the
  /// user's turn had it answered meanwhile. A lookup that fails is a no.
  pub(crate) async fn server_gives(&self, username: &str, ip: IpAddr) -> bool {
    let ip = ip.to_canonical();
    let matches = |endpoint: SocketAddr| same_ip(endpoint.ip(), ip);
    let endpoints = &self.inner.endpoints;
    let cached = endpoints.cached(username);
    if cached.is_some_and(matches) {
      return true;
    }
    let refuted = lock(&endpoints.refuted)
      .get(&(username.to_owned(), ip))
      .copied();
    match refuted {
      // asked already, and the lookup failed or the user was offline
      Some(None) => return false,
      Some(given) if cached.is_some() && given == cached => return false,
      _ => {}
    }
    let asked = Instant::now();
    let turn = endpoints.turn(username);
    let _turn = turn.lock().await;
    let answered = lock(&endpoints.cache)
      .stored_since(&username.to_owned(), asked)
      .copied();
    if let Some(endpoint) = answered {
      return matches(endpoint);
    }
    match self.ask_user_endpoint(username).await {
      Ok(endpoint) if matches(endpoint) => true,
      Ok(endpoint) => {
        lock(&endpoints.refuted).insert((username.to_owned(), ip), Some(endpoint));
        false
      }
      Err(error) => {
        tracing::debug!(username, %ip, %error, "address lookup for a connection naming the user failed");
        lock(&endpoints.refuted).insert((username.to_owned(), ip), None);
        false
      }
    }
  }

  /// whether the address cached for `username` is `ip`, without asking the server
  pub(crate) fn server_gave(&self, username: &str, ip: IpAddr) -> bool {
    self
      .inner
      .endpoints
      .cached(username)
      .is_some_and(|endpoint| same_ip(endpoint.ip(), ip))
  }

  /// Forgets the address cached for `username` when a connect to it, `endpoint`, failed with
  /// `error` because it was refused or timed out.
  pub(crate) fn forget_unreachable(&self, username: &str, endpoint: SocketAddr, error: &Error) {
    if unreachable(error) && self.inner.endpoints.forget(username, endpoint) {
      tracing::debug!(username, %endpoint, %error, "cached endpoint dropped after a failed connect");
    }
  }

  /// Every five minutes, drops the per-user lookup locks nobody holds or waits for.
  pub(super) fn start_endpoint_lock_cleanup(&self) {
    let client = self.downgrade();
    let lifetime = self.cancellation().clone();
    tokio::spawn(
      async move {
        let mut ticks = interval_at(Instant::now() + LOCK_CLEANUP, LOCK_CLEANUP);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
          tokio::select! {
            () = lifetime.cancelled() => return,
            _ = ticks.tick() => {}
          }
          let Some(client) = client.upgrade() else {
            return;
          };
          let mut locks = lock(&client.inner.endpoints.locks);
          let before = locks.len();
          // a lock only the map refers to is neither held nor waited for
          locks.retain(|_, turn| Arc::strong_count(turn) > 1);
          tracing::trace!(
            dropped = before - locks.len(),
            "idle endpoint lookup locks dropped"
          );
        }
      }
      .in_current_span(),
    );
  }
}

impl Users<'_> {
  /// Returns the address `username` listens on.
  ///
  /// The address comes from the cache or the server. Port 0 means the user is online but takes
  /// no direct connections; it is returned as it is.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if `username` is empty or only whitespace.
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::UserOffline`] if the server reports the user as offline. Returns
  /// [`Error::Timeout`] if the server does not answer in time, and
  /// [`Error::Canceled`] if the wait for its answer is canceled. Returns
  /// [`Error::UserEndpoint`], wrapping the cause when there is one, if the lookup fails
  /// in any other way or the server gives a port that is out of range.
  pub async fn address(self, username: &str) -> Result<SocketAddr, Error> {
    non_blank("username", username)?;
    let verb = format!("get the endpoint for user {username}");
    self.client.require_logged_in(&verb)?;
    let endpoints = &self.client.inner.endpoints;
    if let Some(endpoint) = endpoints.cached(username) {
      tracing::debug!(username, %endpoint, "address cache hit");
      return Ok(endpoint);
    }
    let turn = endpoints.turn(username);
    let _turn = turn.lock().await;
    // a caller ahead of this one may have just looked it up
    if let Some(endpoint) = endpoints.cached(username) {
      tracing::debug!(username, %endpoint, "address cache hit");
      return Ok(endpoint);
    }
    self.client.ask_user_endpoint(username).await
  }

  /// Returns the address cached for `username`.
  ///
  /// The server is not asked, and the read does not count as a use of the cache. Returns `None`
  /// when no address is cached or the cached one expired.
  pub fn cached_address(self, username: &str) -> Option<SocketAddr> {
    self.client.inner.endpoints.peek(username)
  }

  /// Makes sure the client has a message connection to `username`.
  ///
  /// This is for warming a connection up ahead of use.
  ///
  /// # Errors
  ///
  /// Fails under the conditions [`Users::address`](crate::user::Users::address) gives.
  ///
  /// Returns [`Error::Timeout`] if neither a direct connection to the user nor one through
  /// the server is made in time. Returns [`Error::Timeout`] if the call waits on a dial the
  /// client makes because the user asked the server for a connection and the dial is not
  /// answered in time, and [`Error::Canceled`] if that dial is given up because the client
  /// is shut down.
  ///
  /// Returns a [`Error::Operation`] wrapping the cause if the message connection cannot be
  /// made for another reason. The cause is a [`Error::Connection`] if no connection to
  /// the user can be made or the client's first message cannot be written on it, a
  /// [`Error::Shutdown`] if the client is found shut down after the address lookup, and a
  /// [`Error::InvalidOperation`] if the client is logged out before it starts connecting
  /// to the user, or if the connection closes after it was made and before the client wrote on
  /// it.
  pub async fn connect(self, username: &str) -> Result<(), Error> {
    non_blank("username", username)?;
    let verb = format!("connect to user {username}");
    let endpoint = self.client.users().address(username).await?;
    self
      .client
      .message_connection(username, Some(endpoint), &CancellationToken::new())
      .await
      .map(drop)
      .map_err(|error| failed(&verb, error))
  }
}

#[cfg(test)]
mod tests {
  use std::net::TcpListener;

  use super::*;
  use crate::config::Connection;
  use crate::conn::{Conn, ConnKind};

  #[tokio::test]
  async fn a_refused_or_unanswered_connect_makes_an_address_unreachable() {
    // a port that was just let go takes no connections
    let closed = TcpListener::bind("127.0.0.1:0")
      .unwrap()
      .local_addr()
      .unwrap();
    let refused = Conn::connect(
      closed,
      Some("alice".into()),
      ConnKind::OUTBOUND | ConnKind::DIRECT,
      Connection::default(),
      &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(unreachable(&refused), "{refused:?}");
    assert!(unreachable(&Error::Timeout(
      "operation timed out after 10000 milliseconds".into()
    )));
    assert!(!unreachable(&Error::Canceled));
    assert!(!unreachable(&Error::connection(
      crate::error::ConnectionErrorKind::Read,
      "remote connection closed"
    )));
  }

  #[tokio::test]
  async fn a_proxy_whose_destination_refused_makes_the_address_unreachable() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = proxy.local_addr().unwrap().port();
    let refusing = tokio::spawn(async move {
      let (mut stream, _) = proxy.accept().await.unwrap();
      let mut methods = [0; 3];
      stream.read_exact(&mut methods).await.unwrap();
      stream.write_all(&[5, 0]).await.unwrap();
      let mut request = [0; 10];
      stream.read_exact(&mut request).await.unwrap();
      // reply 5: the destination refused the connection
      stream.write_all(&[5, 5, 0, 1]).await.unwrap();
    });
    let options = Connection {
      proxy: Some(crate::config::Proxy {
        address: "127.0.0.1".into(),
        port,
        credentials: None,
      }),
      ..Connection::default()
    };
    let refused = Conn::connect(
      "198.51.100.7:2234".parse().unwrap(),
      Some("alice".into()),
      ConnKind::OUTBOUND | ConnKind::DIRECT,
      options,
      &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    refusing.await.unwrap();
    assert!(unreachable(&refused), "{refused:?}");
  }

  #[tokio::test]
  async fn an_address_answered_while_waiting_for_the_turn_is_taken_without_asking_again() {
    use std::future::Future;
    use std::task::{Context, Waker};

    // not logged in, so asking the server fails, which is a no
    let client = Client::new(crate::config::Config {
      enable_listener: false,
      ..crate::config::Config::default()
    })
    .unwrap();
    let ip = IpAddr::from([198, 51, 100, 7]);
    let endpoints = &client.inner.endpoints;
    let turn = endpoints.turn("alice");
    let ahead = turn.lock().await;
    let mut gives = Box::pin(client.server_gives("alice", ip));
    let polled = gives.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(polled.is_pending(), "waits for the turn");
    // the caller ahead has the server's answer
    lock(&endpoints.cache).insert("alice".into(), SocketAddr::from((ip, 2234)));
    drop(ahead);
    assert!(gives.await);
  }

  #[test]
  fn only_the_address_that_failed_is_forgotten() {
    let endpoints = Endpoints::new(10, Duration::from_secs(60));
    let old: SocketAddr = "198.51.100.7:2234".parse().unwrap();
    let new: SocketAddr = "198.51.100.7:2235".parse().unwrap();
    lock(&endpoints.cache).insert("alice".into(), new);
    assert!(!endpoints.forget("alice", old), "a newer address stays");
    assert_eq!(endpoints.cached("alice"), Some(new));
    assert!(endpoints.forget("alice", new));
    assert_eq!(endpoints.cached("alice"), None);
  }
}
