//! The client's configuration: the server it logs in to, what it listens on, how each kind of
//! connection behaves, the protocol version it logs in with, the limits that bound remote input,
//! and the application's resolvers. It is validated when the client is built and whenever it is
//! changed while the client runs.

mod connection;

pub(crate) use connection::ProxyCredentials;
pub use connection::{Connection, KeepAlive, Proxy};

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use soulseek_proto::{PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION};

use crate::error::Error;
use crate::resolvers::Resolvers;

/// host name of the official Soulseek server
const DEFAULT_SERVER_ADDRESS: &str = "server.slsknet.org";
/// port of the official Soulseek server
const DEFAULT_SERVER_PORT: u16 = 2271;

/// Configuration of a [`Client`](crate::Client).
///
/// Each setting has a setter of its name, and one left alone keeps its default.
/// [`Client::new`](crate::Client::new) checks the whole configuration.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use soulseek_client::Config;
///
/// let config = Config::new()
///   .listen_port(50_300)
///   .enable_distributed_network(false)
///   .peer_connection(|connection| connection.connect_timeout(Duration::from_secs(5)));
/// # drop(config);
/// ```
#[derive(Clone, Debug)]
pub struct Config {
  pub(crate) enable_listener: bool,
  pub(crate) listen_while_disconnected: bool,
  pub(crate) listen_ip_address: IpAddr,
  pub(crate) listen_port: u16,
  pub(crate) advertised_port: Option<u16>,
  pub(crate) listen_backlog: u32,
  pub(crate) enable_distributed_network: bool,
  pub(crate) parent_must_relay_search: bool,
  pub(crate) accept_distributed_children: bool,
  pub(crate) distributed_child_limit: u32,
  pub(crate) follow_server_child_limits: bool,
  pub(crate) cancel_searches_on_disconnect: bool,
  pub(crate) check_names: bool,
  pub(crate) message_timeout: Duration,
  pub(crate) server_ping_interval: Option<Duration>,
  pub(crate) event_capacity: u32,
  pub(crate) transfer_response_timeout: Duration,
  pub(crate) transfer_accept_timeout: Duration,
  pub(crate) send_place_when_queued: bool,
  pub(crate) upload_notice_timeout: Duration,
  pub(crate) unasked_offer_timeout: Duration,
  pub(crate) server_connection: Connection,
  pub(crate) peer_connection: Connection,
  pub(crate) transfer_connection: Connection,
  pub(crate) incoming_connection: Connection,
  pub(crate) distributed_connection: Connection,
  pub(crate) protocol_major_version: u32,
  pub(crate) protocol_minor_version: u32,
  pub(crate) limits: Limits,
  #[doc(hidden)]
  pub resolvers: Resolvers,
  pub(crate) server_address: String,
  pub(crate) server_port: u16,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      server_address: DEFAULT_SERVER_ADDRESS.to_owned(),
      server_port: DEFAULT_SERVER_PORT,
      enable_listener: true,
      listen_while_disconnected: false,
      listen_ip_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
      listen_port: 50_000,
      advertised_port: None,
      listen_backlog: i32::MAX as u32,
      enable_distributed_network: true,
      parent_must_relay_search: true,
      accept_distributed_children: true,
      distributed_child_limit: 25,
      follow_server_child_limits: true,
      cancel_searches_on_disconnect: true,
      check_names: true,
      message_timeout: Duration::from_secs(5),
      server_ping_interval: Some(Duration::from_secs(60)),
      event_capacity: 4096,
      transfer_response_timeout: Duration::from_secs(15),
      transfer_accept_timeout: Duration::from_secs(45),
      send_place_when_queued: true,
      upload_notice_timeout: Duration::from_secs(5),
      unasked_offer_timeout: Duration::from_secs(30),
      server_connection: Connection {
        inactivity_timeout: None,
        write_timeout: Some(Duration::from_secs(30)),
        keepalive: Some(KeepAlive::new(
          Duration::from_secs(10),
          Duration::from_secs(2),
          3,
        )),
        ..Connection::default()
      },
      peer_connection: Connection::default(),
      transfer_connection: Connection::default(),
      incoming_connection: Connection::default(),
      // a parent on a quiet server sends nothing for longer than other connections stay idle
      distributed_connection: Connection {
        inactivity_timeout: Some(Duration::from_secs(60)),
        ..Connection::default()
      },
      protocol_major_version: PROTOCOL_MAJOR_VERSION,
      protocol_minor_version: PROTOCOL_MINOR_VERSION,
      limits: Limits::default(),
      resolvers: Resolvers::default(),
    }
  }
}

impl Config {
  /// Creates the default configuration.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Sets the server the client logs in to.
  ///
  /// Read at each connect. Defaults to the official server, `server.slsknet.org:2271`.
  #[must_use]
  pub fn server(mut self, address: impl Into<String>, port: u16) -> Self {
    self.server_address = address.into();
    self.server_port = port;
    self
  }

  /// Sets the version sent at login.
  ///
  /// Fixed for the client's lifetime. The minor version tells client programs apart, so an
  /// application sends a number of its own. Defaults to soulseek-proto's,
  /// [`PROTOCOL_MAJOR_VERSION`] and [`PROTOCOL_MINOR_VERSION`].
  #[must_use]
  pub fn protocol_version(mut self, major: u32, minor: u32) -> Self {
    self.protocol_major_version = major;
    self.protocol_minor_version = minor;
    self
  }

  /// Sets whether to accept connections from peers while connected.
  ///
  /// The listener accepts them on `listen_ip_address:listen_port`. Defaults to `true`.
  #[must_use]
  pub fn enable_listener(mut self, enable_listener: bool) -> Self {
    self.enable_listener = enable_listener;
    self
  }

  /// Sets whether to keep the listener up between server sessions.
  ///
  /// Transfers under way then still take the connections peers open. If unset, the listener
  /// stops when the session ends. A change of the listener options while disconnected stops it
  /// until the next connect. Defaults to `false`.
  #[must_use]
  pub fn listen_while_disconnected(mut self, listen_while_disconnected: bool) -> Self {
    self.listen_while_disconnected = listen_while_disconnected;
    self
  }

  /// Sets the address the listener binds.
  ///
  /// Defaults to `0.0.0.0`, every interface.
  #[must_use]
  pub fn listen_ip_address(mut self, listen_ip_address: IpAddr) -> Self {
    self.listen_ip_address = listen_ip_address;
    self
  }

  /// Sets the port the listener binds.
  ///
  /// Must be 1 or more while the listener is on. Defaults to 50000.
  #[must_use]
  pub fn listen_port(mut self, listen_port: u16) -> Self {
    self.listen_port = listen_port;
    self
  }

  /// Sets the port the server tells other users to connect to.
  ///
  /// If unset, it is `listen_port` while the listener is on and 0 while it is off. 0 says the
  /// client takes no direct connections. A port mapped from outside differs from the one bound.
  /// Unset by default.
  #[must_use]
  pub fn advertised_port(mut self, advertised_port: impl Into<Option<u16>>) -> Self {
    self.advertised_port = advertised_port.into();
    self
  }

  /// Sets the TCP accept backlog of the listener.
  ///
  /// Defaults to [`i32::MAX`].
  #[must_use]
  pub fn listen_backlog(mut self, listen_backlog: u32) -> Self {
    self.listen_backlog = listen_backlog;
    self
  }

  /// Sets whether to look for a parent in the distributed search tree.
  ///
  /// The client then also passes searches on to children. Defaults to `true`.
  #[must_use]
  pub fn enable_distributed_network(mut self, enable_distributed_network: bool) -> Self {
    self.enable_distributed_network = enable_distributed_network;
    self
  }

  /// Sets whether a parent candidate must pass on a search before it counts.
  ///
  /// A search passed on shows that searches reach the candidate. On a quiet server none may come
  /// within the negotiation timeout. Defaults to `true`.
  #[must_use]
  pub fn parent_must_relay_search(mut self, parent_must_relay_search: bool) -> Self {
    self.parent_must_relay_search = parent_must_relay_search;
    self
  }

  /// Sets whether other users may join the distributed search tree below the client.
  ///
  /// Defaults to `true`.
  #[must_use]
  pub fn accept_distributed_children(mut self, accept_distributed_children: bool) -> Self {
    self.accept_distributed_children = accept_distributed_children;
    self
  }

  /// Sets the most children the client keeps at once.
  ///
  /// `follow_server_child_limits` may lower it. Defaults to 25.
  #[must_use]
  pub fn distributed_child_limit(mut self, distributed_child_limit: u32) -> Self {
    self.distributed_child_limit = distributed_child_limit;
    self
  }

  /// Sets whether to follow the server's limits on children.
  ///
  /// The client then lowers the child limit to what the server's speed ratio allows the client's
  /// upload speed, which is the speed divided by the ratio and by 100, takes no children below
  /// the server's minimum speed, and asks for its own statistics after login to learn the speed.
  /// If unset, `distributed_child_limit` alone bounds the children. Defaults to `true`.
  #[must_use]
  pub fn follow_server_child_limits(mut self, follow_server_child_limits: bool) -> Self {
    self.follow_server_child_limits = follow_server_child_limits;
    self
  }

  /// Sets whether to cancel every running search when the server session ends.
  ///
  /// If unset, each search ends by its own timeout or limit, taking the responses peers still
  /// send. Defaults to `true`.
  #[must_use]
  pub fn cancel_searches_on_disconnect(mut self, cancel_searches_on_disconnect: bool) -> Self {
    self.cancel_searches_on_disconnect = cancel_searches_on_disconnect;
    self
  }

  /// Sets whether to check names against the official server's rules.
  ///
  /// The login username and the names of rooms to join are checked before anything is sent. A
  /// server with other rules needs this unset. Defaults to `true`.
  #[must_use]
  pub fn check_names(mut self, check_names: bool) -> Self {
    self.check_names = check_names;
    self
  }

  /// Sets the time a request waits for the reply that answers it.
  ///
  /// Fixed for the client's lifetime. Defaults to 5 seconds.
  #[must_use]
  pub fn message_timeout(mut self, message_timeout: Duration) -> Self {
    self.message_timeout = message_timeout;
    self
  }

  /// Sets the interval at which a ping is written to the server.
  ///
  /// The pings make a socket that stopped taking writes noticed. If unset, none is written,
  /// which leaves that to the connection's TCP keep-alive. The server needs no pings. Read at
  /// each login. Defaults to 60 seconds.
  #[must_use]
  pub fn server_ping_interval(mut self, server_ping_interval: impl Into<Option<Duration>>) -> Self {
    self.server_ping_interval = server_ping_interval.into();
    self
  }

  /// Sets the number of events the channel holds for its slowest subscriber.
  ///
  /// A subscriber gets [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged)
  /// once it is further behind. Fixed for the client's lifetime. Defaults to 4096.
  #[must_use]
  pub fn event_capacity(mut self, event_capacity: u32) -> Self {
    self.event_capacity = event_capacity;
    self
  }

  /// Sets the time a transfer waits for the peer to answer the transfer request.
  ///
  /// Applies to an upload, and to a download asked for with a transfer request. Defaults to 15
  /// seconds.
  #[must_use]
  pub fn transfer_response_timeout(mut self, transfer_response_timeout: Duration) -> Self {
    self.transfer_response_timeout = transfer_response_timeout;
    self
  }

  /// Sets the time a download waits for the uploader to open the transfer connection.
  ///
  /// The wait starts once the uploader's offer is accepted. Some clients take 20 to 30 seconds
  /// to fall back to a connection through the server. Defaults to 45 seconds.
  #[must_use]
  pub fn transfer_accept_timeout(mut self, transfer_accept_timeout: Duration) -> Self {
    self.transfer_accept_timeout = transfer_accept_timeout;
    self
  }

  /// Sets whether to tell a peer its place in the queue as soon as its download is queued.
  ///
  /// The place in the client's queue comes from [`Resolvers::place_in_queue`] and is sent as
  /// soon as a download the peer asked for is queued. If unset, the place is sent only when the
  /// peer asks. Defaults to `true`.
  #[must_use]
  pub fn send_place_when_queued(mut self, send_place_when_queued: bool) -> Self {
    self.send_place_when_queued = send_place_when_queued;
    self
  }

  /// Sets the time telling a peer that an upload to it ended may take.
  ///
  /// The notice is given up after that. Defaults to 5 seconds.
  #[must_use]
  pub fn upload_notice_timeout(mut self, upload_notice_timeout: Duration) -> Self {
    self.upload_notice_timeout = upload_notice_timeout;
    self
  }

  /// Sets the time the offer of a file a user sends unasked is held.
  ///
  /// The offer is held for the download the application starts for it. One not taken by then is
  /// refused, as its sender no longer waits for an answer. Defaults to 30 seconds.
  #[must_use]
  pub fn unasked_offer_timeout(mut self, unasked_offer_timeout: Duration) -> Self {
    self.unasked_offer_timeout = unasked_offer_timeout;
    self
  }

  /// Edits the options of the server connection.
  ///
  /// Its inactivity timeout is always off, since the server can be silent for a long time. The
  /// server never answers a ping either, so the connection's TCP keep-alive is how a dead server
  /// connection is noticed: after about `idle + probes * interval`. A server that stops reading
  /// is noticed by the write timeout. Defaults to the defaults of [`Connection`] with no
  /// inactivity timeout, a write timeout of 30 seconds and keep-alive on: 10 seconds of silence,
  /// 2 seconds between probes, 3 probes.
  #[must_use]
  pub fn server_connection(mut self, edit: impl FnOnce(Connection) -> Connection) -> Self {
    self.server_connection = edit(self.server_connection);
    self
  }

  /// Edits the options of peer message connections, outbound and inbound once identified.
  ///
  /// Defaults to the defaults of [`Connection`].
  #[must_use]
  pub fn peer_connection(mut self, edit: impl FnOnce(Connection) -> Connection) -> Self {
    self.peer_connection = edit(self.peer_connection);
    self
  }

  /// Edits the options of file transfer connections.
  ///
  /// Defaults to the defaults of [`Connection`].
  #[must_use]
  pub fn transfer_connection(mut self, edit: impl FnOnce(Connection) -> Connection) -> Self {
    self.transfer_connection = edit(self.transfer_connection);
    self
  }

  /// Edits the options of an accepted connection until its first message says what it is for.
  ///
  /// Defaults to the defaults of [`Connection`].
  #[must_use]
  pub fn incoming_connection(mut self, edit: impl FnOnce(Connection) -> Connection) -> Self {
    self.incoming_connection = edit(self.incoming_connection);
    self
  }

  /// Edits the options of parent and child connections in the distributed search tree.
  ///
  /// Defaults to the defaults of [`Connection`] with an inactivity timeout of 60 seconds.
  #[must_use]
  pub fn distributed_connection(mut self, edit: impl FnOnce(Connection) -> Connection) -> Self {
    self.distributed_connection = edit(self.distributed_connection);
    self
  }

  /// Edits the bounds on remote input and on the work it can start.
  ///
  /// Defaults to the defaults of [`Limits`].
  #[must_use]
  pub fn limits(mut self, edit: impl FnOnce(Limits) -> Limits) -> Self {
    self.limits = edit(self.limits);
    self
  }

  /// Sets the callbacks and caches of the application.
  ///
  /// Every resolver is unset by default.
  #[must_use]
  pub fn resolvers(mut self, resolvers: Resolvers) -> Self {
    self.resolvers = resolvers;
    self
  }

  /// the port the server is told other users reach the client on
  pub(crate) fn port_advertised(&self) -> u16 {
    let listening = if self.enable_listener {
      self.listen_port
    } else {
      0
    };
    self.advertised_port.unwrap_or(listening)
  }

  /// checks every setting, failing on the first one out of range
  pub(crate) fn validate(&self) -> Result<(), Error> {
    // other users are told to connect to it, so the system cannot pick it
    if self.enable_listener && self.listen_port == 0 {
      return Err(invalid("listen_port", "must be between 1 and 65535"));
    }
    // the system takes the backlog as a positive signed number
    if !(1..=i32::MAX as u32).contains(&self.listen_backlog) {
      return Err(invalid(
        "listen_backlog",
        format!(
          "must be greater than or equal to 1 and at most {} (specified: {})",
          i32::MAX,
          self.listen_backlog
        ),
      ));
    }
    if self.message_timeout.is_zero() {
      return Err(invalid("message_timeout", "must be greater than zero"));
    }
    if self
      .server_ping_interval
      .is_some_and(|every| every.is_zero())
    {
      return Err(invalid("server_ping_interval", "must be greater than zero"));
    }
    at_least_one("event_capacity", self.event_capacity)?;
    if self.transfer_response_timeout.is_zero() {
      return Err(invalid(
        "transfer_response_timeout",
        "must be greater than zero",
      ));
    }
    if self.transfer_accept_timeout.is_zero() {
      return Err(invalid(
        "transfer_accept_timeout",
        "must be greater than zero",
      ));
    }
    if self.upload_notice_timeout.is_zero() {
      return Err(invalid(
        "upload_notice_timeout",
        "must be greater than zero",
      ));
    }
    if self.unasked_offer_timeout.is_zero() {
      return Err(invalid(
        "unasked_offer_timeout",
        "must be greater than zero",
      ));
    }
    self.server_connection.validate("server_connection")?;
    self.peer_connection.validate("peer_connection")?;
    self.transfer_connection.validate("transfer_connection")?;
    self.incoming_connection.validate("incoming_connection")?;
    self
      .distributed_connection
      .validate("distributed_connection")?;
    if self.protocol_major_version == 0 {
      return Err(invalid(
        "protocol_major_version",
        "must be greater than zero",
      ));
    }
    if self.protocol_minor_version == 0 {
      return Err(invalid(
        "protocol_minor_version",
        "must be greater than zero",
      ));
    }
    self.limits.validate()
  }

  /// Checks that `self` may replace `current` on a running client: every setting in range, and
  /// the ones fixed for the client's lifetime as they were.
  pub(crate) fn validate_change(&self, current: &Config) -> Result<(), Error> {
    self.validate()?;
    let fixed = [
      (
        "message_timeout",
        self.message_timeout == current.message_timeout,
      ),
      (
        "event_capacity",
        self.event_capacity == current.event_capacity,
      ),
      (
        "protocol_version",
        self.protocol_major_version == current.protocol_major_version
          && self.protocol_minor_version == current.protocol_minor_version,
      ),
    ];
    match fixed.iter().find(|(_, unchanged)| !unchanged) {
      Some((field, _)) => Err(invalid(field, "is fixed for the client's lifetime")),
      None => Ok(()),
    }
  }

  /// whether the listener's socket has to be bound again to go from `old` to `self`
  pub(crate) fn listener_changed(&self, old: &Config) -> bool {
    old.enable_listener != self.enable_listener
      || old.listen_ip_address != self.listen_ip_address
      || old.listen_port != self.listen_port
      || old.listen_backlog != self.listen_backlog
  }

  /// Whether going from `old` to `self` only takes effect on a new server session: the
  /// distributed network or children being turned off (the server and the tree already count on
  /// the client), or the server or distributed connections' options or the server ping interval
  /// changing. Turning things on, and the peer, transfer and incoming options, apply to what
  /// happens next without reconnecting.
  pub(crate) fn reconnect_required(&self, old: &Config) -> bool {
    (old.enable_distributed_network && !self.enable_distributed_network)
      || (old.accept_distributed_children && !self.accept_distributed_children)
      || old.distributed_connection != self.distributed_connection
      || old.effective_server_connection_options() != self.effective_server_connection_options()
      || old.server_ping_interval != self.server_ping_interval
  }

  /// the options the server connection is actually created with
  pub(crate) fn effective_server_connection_options(&self) -> Connection {
    Connection {
      inactivity_timeout: None,
      ..self.server_connection.clone()
    }
  }
}

/// Bounds on remote input and on the work it can make the client start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
  pub(crate) decode: soulseek_proto::Limits,
  pub(crate) max_concurrent_peer_connects: u32,
  pub(crate) max_cached_peer_connections: u32,
  pub(crate) max_message_connections_per_address: u32,
  pub(crate) max_unidentified_connections: u32,
  pub(crate) search_response_cache_size: u32,
  pub(crate) search_response_cache_lifetime: Duration,
  pub(crate) user_endpoint_cache_size: u32,
  pub(crate) user_endpoint_cache_lifetime: Duration,
  pub(crate) distributed_parent_negotiation_timeout: Duration,
  pub(crate) distributed_search_memory: u32,
  pub(crate) distributed_search_memory_lifetime: Duration,
  pub(crate) distributed_status_debounce: Duration,
  pub(crate) distributed_status_max_age: Duration,
  pub(crate) distributed_parent_retry: Duration,
}

impl Default for Limits {
  fn default() -> Self {
    Self {
      decode: soulseek_proto::Limits::DEFAULT,
      max_concurrent_peer_connects: 50,
      max_cached_peer_connections: 1000,
      max_message_connections_per_address: 8,
      max_unidentified_connections: 128,
      search_response_cache_size: 1000,
      search_response_cache_lifetime: Duration::from_secs(180),
      user_endpoint_cache_size: 10_000,
      user_endpoint_cache_lifetime: Duration::from_secs(60),
      distributed_parent_negotiation_timeout: Duration::from_secs(5),
      distributed_search_memory: 1000,
      distributed_search_memory_lifetime: Duration::from_secs(60),
      distributed_status_debounce: Duration::from_secs(5),
      distributed_status_max_age: Duration::from_secs(300),
      distributed_parent_retry: Duration::from_secs(900),
    }
  }
}

impl Limits {
  /// Sets what the decoders accept from peers.
  ///
  /// These are soulseek-proto's own limits. A frame past its bound is skipped unread. Defaults
  /// to [`soulseek_proto::Limits::DEFAULT`], the limits as soulseek-proto sets them.
  #[must_use]
  pub fn decode(mut self, decode: soulseek_proto::Limits) -> Self {
    self.decode = decode;
    self
  }

  /// Sets the most outbound peer connection attempts in flight at once.
  ///
  /// The attempts the client makes for itself and the ones it makes because a user asked the
  /// server to have the client connect are counted apart, each up to this many. Defaults to 50.
  #[must_use]
  pub fn max_concurrent_peer_connects(mut self, max_concurrent_peer_connects: u32) -> Self {
    self.max_concurrent_peer_connects = max_concurrent_peer_connects;
    self
  }

  /// Sets the most peer message connections kept open at once.
  ///
  /// Defaults to 1000.
  #[must_use]
  pub fn max_cached_peer_connections(mut self, max_cached_peer_connections: u32) -> Self {
    self.max_cached_peer_connections = max_cached_peer_connections;
    self
  }

  /// Sets the most message connections one address may have open to the client at once.
  ///
  /// Counts current, replaced and still arriving connections. One more is closed. A few users
  /// behind one NAT fit, a host posing as many users cannot fill the cache. Defaults to 8.
  #[must_use]
  pub fn max_message_connections_per_address(
    mut self,
    max_message_connections_per_address: u32,
  ) -> Self {
    self.max_message_connections_per_address = max_message_connections_per_address;
    self
  }

  /// Sets the most accepted connections at once that have not yet said what they are for.
  ///
  /// One more is closed on accept, so peers that connect and stay silent cannot use up file
  /// descriptors. Read when the listener starts. Defaults to 128.
  #[must_use]
  pub fn max_unidentified_connections(mut self, max_unidentified_connections: u32) -> Self {
    self.max_unidentified_connections = max_unidentified_connections;
    self
  }

  /// Sets the most undelivered search responses kept for a late connection from the searcher.
  ///
  /// Defaults to 1000.
  #[must_use]
  pub fn search_response_cache_size(mut self, search_response_cache_size: u32) -> Self {
    self.search_response_cache_size = search_response_cache_size;
    self
  }

  /// Sets the time an undelivered search response is kept.
  ///
  /// Defaults to 180 seconds.
  #[must_use]
  pub fn search_response_cache_lifetime(
    mut self,
    search_response_cache_lifetime: Duration,
  ) -> Self {
    self.search_response_cache_lifetime = search_response_cache_lifetime;
    self
  }

  /// Sets the most user addresses remembered between connects.
  ///
  /// Defaults to 10000.
  #[must_use]
  pub fn user_endpoint_cache_size(mut self, user_endpoint_cache_size: u32) -> Self {
    self.user_endpoint_cache_size = user_endpoint_cache_size;
    self
  }

  /// Sets the time a user's address is remembered before the server is asked again.
  ///
  /// Defaults to 60 seconds.
  #[must_use]
  pub fn user_endpoint_cache_lifetime(mut self, user_endpoint_cache_lifetime: Duration) -> Self {
    self.user_endpoint_cache_lifetime = user_endpoint_cache_lifetime;
    self
  }

  /// Sets the time a would-be parent has to send its branch level, root and a search.
  ///
  /// Defaults to 5 seconds.
  #[must_use]
  pub fn distributed_parent_negotiation_timeout(
    mut self,
    distributed_parent_negotiation_timeout: Duration,
  ) -> Self {
    self.distributed_parent_negotiation_timeout = distributed_parent_negotiation_timeout;
    self
  }

  /// Sets the most searches from the distributed parent remembered by searcher and token.
  ///
  /// A search that comes again while remembered is neither passed on to the children nor
  /// answered. 0 remembers none. Defaults to 1000.
  #[must_use]
  pub fn distributed_search_memory(mut self, distributed_search_memory: u32) -> Self {
    self.distributed_search_memory = distributed_search_memory;
    self
  }

  /// Sets the time a search from the distributed parent is remembered.
  ///
  /// Defaults to 60 seconds.
  #[must_use]
  pub fn distributed_search_memory_lifetime(
    mut self,
    distributed_search_memory_lifetime: Duration,
  ) -> Self {
    self.distributed_search_memory_lifetime = distributed_search_memory_lifetime;
    self
  }

  /// Sets the time changes of the client's place in the distributed tree are coalesced.
  ///
  /// The server is told after that. Defaults to 5 seconds.
  #[must_use]
  pub fn distributed_status_debounce(mut self, distributed_status_debounce: Duration) -> Self {
    self.distributed_status_debounce = distributed_status_debounce;
    self
  }

  /// Sets the age at which a status the server has is sent again without the debounce.
  ///
  /// A status the server has had for this long is sent again when another change comes, without
  /// waiting out the debounce. Defaults to 300 seconds.
  #[must_use]
  pub fn distributed_status_max_age(mut self, distributed_status_max_age: Duration) -> Self {
    self.distributed_status_max_age = distributed_status_max_age;
    self
  }

  /// Sets the interval at which the server is asked for parent candidates again.
  ///
  /// Applies to a client with neither parent nor branch. Defaults to 900 seconds.
  #[must_use]
  pub fn distributed_parent_retry(mut self, distributed_parent_retry: Duration) -> Self {
    self.distributed_parent_retry = distributed_parent_retry;
    self
  }

  fn validate(&self) -> Result<(), Error> {
    // a frame must at least hold its 4-byte code
    if self.decode.browse_message_bytes < 4 {
      return Err(invalid(
        "limits.decode.browse_message_bytes",
        "must be at least 4 bytes",
      ));
    }
    at_least_one(
      "limits.max_concurrent_peer_connects",
      self.max_concurrent_peer_connects,
    )?;
    at_least_one(
      "limits.max_cached_peer_connections",
      self.max_cached_peer_connections,
    )?;
    at_least_one(
      "limits.max_message_connections_per_address",
      self.max_message_connections_per_address,
    )?;
    at_least_one(
      "limits.max_unidentified_connections",
      self.max_unidentified_connections,
    )?;
    at_least_one(
      "limits.search_response_cache_size",
      self.search_response_cache_size,
    )?;
    at_least_one(
      "limits.user_endpoint_cache_size",
      self.user_endpoint_cache_size,
    )?;
    if self.distributed_parent_negotiation_timeout.is_zero() {
      return Err(invalid(
        "limits.distributed_parent_negotiation_timeout",
        "must be greater than zero",
      ));
    }
    if self.distributed_parent_retry.is_zero() {
      return Err(invalid(
        "limits.distributed_parent_retry",
        "must be greater than zero",
      ));
    }
    Ok(())
  }
}

fn invalid(field: &str, message: impl AsRef<str>) -> Error {
  Error::InvalidArgument(format!("{field}: {}", message.as_ref()))
}

fn invalid_in(name: &str, field: &str, message: impl AsRef<str>) -> Error {
  invalid(&format!("{name}.{field}"), message)
}

fn at_least_one(field: &str, value: u32) -> Result<(), Error> {
  if value == 0 {
    return Err(invalid(field, "must be greater than or equal to 1"));
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn listen_port_and_backlog_ranges() {
    let with = |listen_port, listen_backlog| Config {
      listen_port,
      listen_backlog,
      ..Config::default()
    };
    with(1, 1).validate().unwrap();
    // a client that does not listen binds nothing
    let not_listening = Config {
      enable_listener: false,
      ..with(0, 1)
    };
    not_listening.validate().unwrap();
    assert!(matches!(
      with(0, 1).validate(),
      Err(Error::InvalidArgument(message)) if message.starts_with("listen_port: must be between 1 and 65535")
    ));
    assert!(matches!(
      with(1, 0).validate(),
      Err(Error::InvalidArgument(message)) if message.starts_with("listen_backlog: must be greater than or equal to 1")
    ));
  }

  #[test]
  fn any_protocol_version_but_zero() {
    let with = |protocol_major_version, protocol_minor_version| Config {
      protocol_major_version,
      protocol_minor_version,
      ..Config::default()
    };
    with(1, 1).validate().unwrap();
    assert!(with(0, 1).validate().is_err());
    assert!(with(1, 0).validate().is_err());
  }

  #[test]
  fn reconnect_required_only_for_turning_things_off_or_server_and_distributed_options() {
    let old = Config::default();

    let off = old.clone().enable_distributed_network(false);
    assert!(off.reconnect_required(&old));
    assert!(!old.reconnect_required(&off), "turning it back on");

    let no_ping = old.clone().server_ping_interval(None);
    assert!(no_ping.reconnect_required(&old));

    let no_children = old.clone().accept_distributed_children(false);
    assert!(no_children.reconnect_required(&old));

    let peer = old
      .clone()
      .peer_connection(|connection| connection.connect_timeout(Duration::from_secs(3)));
    assert!(!peer.reconnect_required(&old));

    let keepalive = old.clone().server_connection(|connection| {
      connection.keepalive(KeepAlive::new(
        Duration::from_secs(10),
        Duration::from_secs(2),
        5,
      ))
    });
    assert!(keepalive.reconnect_required(&old));
  }

  #[test]
  fn the_server_connection_never_times_out_for_inactivity() {
    let old = Config::default();
    let new = old
      .clone()
      .server_connection(|connection| connection.inactivity_timeout(Duration::from_secs(30)));
    assert_eq!(
      new.effective_server_connection_options().inactivity_timeout,
      None
    );
    assert!(
      !new.reconnect_required(&old),
      "identical once the timeout is forced off"
    );
  }

  #[test]
  fn a_change_out_of_range_or_to_a_fixed_setting_is_rejected() {
    let old = Config::default();
    assert!(matches!(
      old.clone().listen_port(0).validate_change(&old),
      Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
      old.clone().event_capacity(8).validate_change(&old),
      Err(Error::InvalidArgument(message)) if message.starts_with("event_capacity: is fixed")
    ));
    old
      .clone()
      .listen_port(50_001)
      .validate_change(&old)
      .unwrap();
  }
}
