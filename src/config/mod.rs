//! Configuration of the client.
//!
//! It covers the server the client logs in to, what it listens on, how each kind of connection
//! behaves, the protocol version it logs in with, the limits that bound remote input, and the
//! application's handler. It is validated when the client is built and whenever it is changed
//! while the client runs.

mod connection;

pub(crate) use connection::ProxyCredentials;
pub use connection::{Connection, ConnectionBuilder, KeepAlive, Proxy};

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use soulseek_proto::{PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION};

use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;
use crate::handler::{self, Handler};

/// host name of the official Soulseek server
const DEFAULT_SERVER_HOST: &str = "server.slsknet.org";
/// port of the official Soulseek server
const DEFAULT_SERVER_PORT: u16 = 2271;

/// Configuration of a [`Client`](crate::Client).
///
/// Made by a [`ConfigBuilder`], which checks the whole configuration when it builds one, so a
/// `Config` is always valid. Each setting is read through a method of its name. The default
/// configuration is [`Config::default`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use soulseek_client::Config;
///
/// let config = Config::builder()
///   .listen_port(50_300)
///   .distributed_network(false)
///   .peer_connection(|connection| connection.connect_timeout(Duration::from_secs(5)))
///   .build()?;
/// assert_eq!(config.listen_port(), 50_300);
/// # Ok::<(), soulseek_client::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct Config {
  pub(crate) listener: bool,
  pub(crate) listen_while_disconnected: bool,
  pub(crate) listen_address: IpAddr,
  pub(crate) listen_port: u16,
  pub(crate) advertised_port: Option<u16>,
  pub(crate) listen_backlog: u32,
  pub(crate) distributed_network: bool,
  pub(crate) parent_must_relay_search: bool,
  pub(crate) distributed_children: bool,
  pub(crate) max_distributed_children: u32,
  pub(crate) follow_server_child_limits: bool,
  pub(crate) cancel_searches_on_disconnect: bool,
  pub(crate) check_names: bool,
  pub(crate) request_timeout: Duration,
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
  pub(crate) handler: handler::Shared,
  pub(crate) server_host: String,
  pub(crate) server_port: u16,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      server_host: DEFAULT_SERVER_HOST.to_owned(),
      server_port: DEFAULT_SERVER_PORT,
      listener: true,
      listen_while_disconnected: false,
      listen_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
      listen_port: 50_000,
      advertised_port: None,
      listen_backlog: i32::MAX as u32,
      distributed_network: true,
      parent_must_relay_search: true,
      distributed_children: true,
      max_distributed_children: 25,
      follow_server_child_limits: true,
      cancel_searches_on_disconnect: true,
      check_names: true,
      request_timeout: Duration::from_secs(5),
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
      handler: handler::Shared::default(),
    }
  }
}

impl Config {
  /// Returns whether the client accepts connections from peers while connected.
  pub fn listener(&self) -> bool {
    self.listener
  }

  /// Returns whether the listener stays up between server sessions.
  pub fn listen_while_disconnected(&self) -> bool {
    self.listen_while_disconnected
  }

  /// Returns the address the listener binds.
  pub fn listen_address(&self) -> IpAddr {
    self.listen_address
  }

  /// Returns the port the listener binds.
  pub fn listen_port(&self) -> u16 {
    self.listen_port
  }

  /// Returns the port the server tells other users to connect to, if set.
  pub fn advertised_port(&self) -> Option<u16> {
    self.advertised_port
  }

  /// Returns the TCP accept backlog of the listener.
  pub fn listen_backlog(&self) -> u32 {
    self.listen_backlog
  }

  /// Returns whether the client looks for a parent in the distributed search tree.
  pub fn distributed_network(&self) -> bool {
    self.distributed_network
  }

  /// Returns whether a parent candidate must pass on a search before it counts.
  pub fn parent_must_relay_search(&self) -> bool {
    self.parent_must_relay_search
  }

  /// Returns whether other users may join the distributed search tree below the client.
  pub fn distributed_children(&self) -> bool {
    self.distributed_children
  }

  /// Returns the most children the client keeps at once.
  pub fn max_distributed_children(&self) -> u32 {
    self.max_distributed_children
  }

  /// Returns whether the client follows the server's limits on children.
  pub fn follow_server_child_limits(&self) -> bool {
    self.follow_server_child_limits
  }

  /// Returns whether every running search is canceled when the server session ends.
  pub fn cancel_searches_on_disconnect(&self) -> bool {
    self.cancel_searches_on_disconnect
  }

  /// Returns whether names are checked against the official server's rules.
  pub fn check_names(&self) -> bool {
    self.check_names
  }

  /// Returns the time a request waits for the reply that answers it.
  pub fn request_timeout(&self) -> Duration {
    self.request_timeout
  }

  /// Returns the interval at which a ping is written to the server.
  pub fn server_ping_interval(&self) -> Option<Duration> {
    self.server_ping_interval
  }

  /// Returns the number of events the channel holds for its slowest subscriber.
  pub fn event_capacity(&self) -> u32 {
    self.event_capacity
  }

  /// Returns the time a transfer waits for the peer to answer the transfer request.
  pub fn transfer_response_timeout(&self) -> Duration {
    self.transfer_response_timeout
  }

  /// Returns the time a download waits for the uploader to open the transfer connection.
  pub fn transfer_accept_timeout(&self) -> Duration {
    self.transfer_accept_timeout
  }

  /// Returns whether a peer is told its place in the queue as soon as its download is queued.
  pub fn send_place_when_queued(&self) -> bool {
    self.send_place_when_queued
  }

  /// Returns the time telling a peer that an upload to it ended may take.
  pub fn upload_notice_timeout(&self) -> Duration {
    self.upload_notice_timeout
  }

  /// Returns the time the offer of a file a user sends unasked is held.
  pub fn unasked_offer_timeout(&self) -> Duration {
    self.unasked_offer_timeout
  }

  /// Returns the options of the server connection.
  pub fn server_connection(&self) -> &Connection {
    &self.server_connection
  }

  /// Returns the options of peer message connections, outbound and inbound once identified.
  pub fn peer_connection(&self) -> &Connection {
    &self.peer_connection
  }

  /// Returns the options of file transfer connections.
  pub fn transfer_connection(&self) -> &Connection {
    &self.transfer_connection
  }

  /// Returns the options of an accepted connection until its first message says what it is for.
  pub fn incoming_connection(&self) -> &Connection {
    &self.incoming_connection
  }

  /// Returns the options of parent and child connections in the distributed search tree.
  pub fn distributed_connection(&self) -> &Connection {
    &self.distributed_connection
  }

  /// Returns the bounds on remote input and on the work it can start.
  pub fn limits(&self) -> &Limits {
    &self.limits
  }

  /// Returns the host of the server the client logs in to.
  pub fn server_host(&self) -> &str {
    &self.server_host
  }

  /// Returns the port of the server the client logs in to.
  pub fn server_port(&self) -> u16 {
    self.server_port
  }

  /// Returns the major version sent at login.
  pub fn protocol_major_version(&self) -> u32 {
    self.protocol_major_version
  }

  /// Returns the minor version sent at login.
  pub fn protocol_minor_version(&self) -> u32 {
    self.protocol_minor_version
  }

  /// Returns a builder that starts from the default configuration.
  #[must_use]
  pub fn builder() -> ConfigBuilder {
    ConfigBuilder {
      config: Self::default(),
    }
  }

  /// the port the server is told other users reach the client on
  pub(crate) fn port_advertised(&self) -> u16 {
    let listening = if self.listener { self.listen_port } else { 0 };
    self.advertised_port.unwrap_or(listening)
  }

  /// checks every setting, failing on the first one out of range
  pub(crate) fn validate(&self) -> Result<(), Error> {
    // other users are told to connect to it, so the system cannot pick it
    if self.listener && self.listen_port == 0 {
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
    if self.request_timeout.is_zero() {
      return Err(invalid("request_timeout", "must be greater than zero"));
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

  /// checks that `self` may replace `current` on a running client: the settings fixed for the
  /// client's lifetime are as they were
  pub(crate) fn validate_change(&self, current: &Config) -> Result<(), Error> {
    let fixed = [
      (
        "request_timeout",
        self.request_timeout == current.request_timeout,
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
    old.listener != self.listener
      || old.listen_address != self.listen_address
      || old.listen_port != self.listen_port
      || old.listen_backlog != self.listen_backlog
  }

  /// Whether going from `old` to `self` only takes effect on a new server session: the
  /// distributed network or children being turned off (the server and the tree already count on
  /// the client), or the server or distributed connections' options or the server ping interval
  /// changing. Turning things on, and the peer, transfer and incoming options, apply to what
  /// happens next without reconnecting.
  pub(crate) fn reconnect_required(&self, old: &Config) -> bool {
    (old.distributed_network && !self.distributed_network)
      || (old.distributed_children && !self.distributed_children)
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

/// Builder of a [`Config`].
///
/// Made by [`Config::builder`]. Each setting has a setter of its name, and one left alone keeps
/// its default. [`ConfigBuilder::build`] checks the whole configuration.
#[derive(Clone, Debug)]
pub struct ConfigBuilder {
  pub(crate) config: Config,
}

impl From<Config> for ConfigBuilder {
  /// Returns a builder that starts from `config`.
  fn from(config: Config) -> Self {
    Self { config }
  }
}

/// When a change made with [`Client::reconfigure`](crate::Client::reconfigure) takes effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Applied {
  /// The whole change is in effect.
  Now,
  /// Some of the change takes effect only at the next connect.
  AfterReconnect,
}

impl ConfigBuilder {
  /// Checks the configuration and returns it.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] naming the first setting that is out of range.
  pub fn build(self) -> Result<Config, Error> {
    self.config.validate()?;
    Ok(self.config)
  }

  /// Sets the server the client logs in to.
  ///
  /// Read at each connect. Defaults to the official server, `server.slsknet.org:2271`.
  #[must_use]
  pub fn server(mut self, host: impl Into<String>, port: u16) -> Self {
    self.config.server_host = host.into();
    self.config.server_port = port;
    self
  }

  /// Sets the version sent at login.
  ///
  /// Fixed for the client's lifetime. The minor version tells client programs apart, so an
  /// application sends a number of its own. Defaults to soulseek-proto's,
  /// [`PROTOCOL_MAJOR_VERSION`] and [`PROTOCOL_MINOR_VERSION`].
  #[must_use]
  pub fn protocol_version(mut self, major: u32, minor: u32) -> Self {
    self.config.protocol_major_version = major;
    self.config.protocol_minor_version = minor;
    self
  }

  /// Sets whether to accept connections from peers while connected.
  ///
  /// The listener accepts them on `listen_address:listen_port`. Defaults to `true`.
  #[must_use]
  pub fn listener(mut self, listener: bool) -> Self {
    self.config.listener = listener;
    self
  }

  /// Sets whether to keep the listener up between server sessions.
  ///
  /// Transfers under way then still take the connections peers open. If unset, the listener
  /// stops when the session ends. A change of the listener options while disconnected stops it
  /// until the next connect. Defaults to `false`.
  #[must_use]
  pub fn listen_while_disconnected(mut self, listen_while_disconnected: bool) -> Self {
    self.config.listen_while_disconnected = listen_while_disconnected;
    self
  }

  /// Sets the address the listener binds.
  ///
  /// Defaults to `0.0.0.0`, every interface.
  #[must_use]
  pub fn listen_address(mut self, listen_address: IpAddr) -> Self {
    self.config.listen_address = listen_address;
    self
  }

  /// Sets the port the listener binds.
  ///
  /// Must be 1 or more while the listener is on. Defaults to 50000.
  #[must_use]
  pub fn listen_port(mut self, listen_port: u16) -> Self {
    self.config.listen_port = listen_port;
    self
  }

  /// Sets the port the server tells other users to connect to.
  ///
  /// If unset, it is `listen_port` while the listener is on and 0 while it is off. 0 says the
  /// client takes no direct connections. A port mapped from outside differs from the one bound.
  /// Unset by default.
  #[must_use]
  pub fn advertised_port(mut self, advertised_port: impl Into<Option<u16>>) -> Self {
    self.config.advertised_port = advertised_port.into();
    self
  }

  /// Sets the TCP accept backlog of the listener.
  ///
  /// Defaults to [`i32::MAX`].
  #[must_use]
  pub fn listen_backlog(mut self, listen_backlog: u32) -> Self {
    self.config.listen_backlog = listen_backlog;
    self
  }

  /// Sets whether to look for a parent in the distributed search tree.
  ///
  /// The client then also passes searches on to children. Defaults to `true`.
  #[must_use]
  pub fn distributed_network(mut self, distributed_network: bool) -> Self {
    self.config.distributed_network = distributed_network;
    self
  }

  /// Sets whether a parent candidate must pass on a search before it counts.
  ///
  /// A search passed on shows that searches reach the candidate. On a quiet server none may come
  /// within the negotiation timeout. Defaults to `true`.
  #[must_use]
  pub fn parent_must_relay_search(mut self, parent_must_relay_search: bool) -> Self {
    self.config.parent_must_relay_search = parent_must_relay_search;
    self
  }

  /// Sets whether other users may join the distributed search tree below the client.
  ///
  /// Defaults to `true`.
  #[must_use]
  pub fn distributed_children(mut self, distributed_children: bool) -> Self {
    self.config.distributed_children = distributed_children;
    self
  }

  /// Sets the most children the client keeps at once.
  ///
  /// `follow_server_child_limits` may lower it. Defaults to 25.
  #[must_use]
  pub fn max_distributed_children(mut self, max_distributed_children: u32) -> Self {
    self.config.max_distributed_children = max_distributed_children;
    self
  }

  /// Sets whether to follow the server's limits on children.
  ///
  /// The client then lowers the child limit to what the server's speed ratio allows the client's
  /// upload speed, which is the speed divided by the ratio and by 100, takes no children below
  /// the server's minimum speed, and asks for its own statistics after login to learn the speed.
  /// If unset, `max_distributed_children` alone bounds the children. Defaults to `true`.
  #[must_use]
  pub fn follow_server_child_limits(mut self, follow_server_child_limits: bool) -> Self {
    self.config.follow_server_child_limits = follow_server_child_limits;
    self
  }

  /// Sets whether to cancel every running search when the server session ends.
  ///
  /// If unset, each search ends by its own timeout or limit, taking the responses peers still
  /// send. Defaults to `true`.
  #[must_use]
  pub fn cancel_searches_on_disconnect(mut self, cancel_searches_on_disconnect: bool) -> Self {
    self.config.cancel_searches_on_disconnect = cancel_searches_on_disconnect;
    self
  }

  /// Sets whether to check names against the official server's rules.
  ///
  /// The login username and the names of rooms to join are checked before anything is sent. A
  /// server with other rules needs this unset. Defaults to `true`.
  #[must_use]
  pub fn check_names(mut self, check_names: bool) -> Self {
    self.config.check_names = check_names;
    self
  }

  /// Sets the time a request waits for the reply that answers it.
  ///
  /// Fixed for the client's lifetime. Defaults to 5 seconds.
  #[must_use]
  pub fn request_timeout(mut self, request_timeout: Duration) -> Self {
    self.config.request_timeout = request_timeout;
    self
  }

  /// Sets the interval at which a ping is written to the server.
  ///
  /// The pings make a socket that stopped taking writes noticed. If unset, none is written,
  /// which leaves that to the connection's TCP keep-alive. The server needs no pings. Read at
  /// each login. Defaults to 60 seconds.
  #[must_use]
  pub fn server_ping_interval(mut self, server_ping_interval: impl Into<Option<Duration>>) -> Self {
    self.config.server_ping_interval = server_ping_interval.into();
    self
  }

  /// Sets the number of events the channel holds for its slowest subscriber.
  ///
  /// A subscriber gets [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged)
  /// once it is further behind. Fixed for the client's lifetime. Defaults to 4096.
  #[must_use]
  pub fn event_capacity(mut self, event_capacity: u32) -> Self {
    self.config.event_capacity = event_capacity;
    self
  }

  /// Sets the time a transfer waits for the peer to answer the transfer request.
  ///
  /// Applies to an upload, and to a download asked for with a transfer request. Defaults to 15
  /// seconds.
  #[must_use]
  pub fn transfer_response_timeout(mut self, transfer_response_timeout: Duration) -> Self {
    self.config.transfer_response_timeout = transfer_response_timeout;
    self
  }

  /// Sets the time a download waits for the uploader to open the transfer connection.
  ///
  /// The wait starts once the uploader's offer is accepted. Some clients take 20 to 30 seconds
  /// to fall back to a connection through the server. Defaults to 45 seconds.
  #[must_use]
  pub fn transfer_accept_timeout(mut self, transfer_accept_timeout: Duration) -> Self {
    self.config.transfer_accept_timeout = transfer_accept_timeout;
    self
  }

  /// Sets whether to tell a peer its place in the queue as soon as its download is queued.
  ///
  /// The place in the client's queue comes from [`Handler::place_in_queue`] and is sent as
  /// soon as a download the peer asked for is queued. If unset, the place is sent only when the
  /// peer asks. Defaults to `true`.
  #[must_use]
  pub fn send_place_when_queued(mut self, send_place_when_queued: bool) -> Self {
    self.config.send_place_when_queued = send_place_when_queued;
    self
  }

  /// Sets the time telling a peer that an upload to it ended may take.
  ///
  /// The notice is given up after that. Defaults to 5 seconds.
  #[must_use]
  pub fn upload_notice_timeout(mut self, upload_notice_timeout: Duration) -> Self {
    self.config.upload_notice_timeout = upload_notice_timeout;
    self
  }

  /// Sets the time the offer of a file a user sends unasked is held.
  ///
  /// The offer is held for the download the application starts for it. One not taken by then is
  /// refused, as its sender no longer waits for an answer. Defaults to 30 seconds.
  #[must_use]
  pub fn unasked_offer_timeout(mut self, unasked_offer_timeout: Duration) -> Self {
    self.config.unasked_offer_timeout = unasked_offer_timeout;
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
  pub fn server_connection(
    mut self,
    edit: impl FnOnce(ConnectionBuilder) -> ConnectionBuilder,
  ) -> Self {
    let connection = self.config.server_connection;
    self.config.server_connection = edit(ConnectionBuilder { connection }).connection;
    self
  }

  /// Edits the options of peer message connections, outbound and inbound once identified.
  ///
  /// Defaults to the defaults of [`Connection`].
  #[must_use]
  pub fn peer_connection(
    mut self,
    edit: impl FnOnce(ConnectionBuilder) -> ConnectionBuilder,
  ) -> Self {
    let connection = self.config.peer_connection;
    self.config.peer_connection = edit(ConnectionBuilder { connection }).connection;
    self
  }

  /// Edits the options of file transfer connections.
  ///
  /// Defaults to the defaults of [`Connection`].
  #[must_use]
  pub fn transfer_connection(
    mut self,
    edit: impl FnOnce(ConnectionBuilder) -> ConnectionBuilder,
  ) -> Self {
    let connection = self.config.transfer_connection;
    self.config.transfer_connection = edit(ConnectionBuilder { connection }).connection;
    self
  }

  /// Edits the options of an accepted connection until its first message says what it is for.
  ///
  /// Defaults to the defaults of [`Connection`].
  #[must_use]
  pub fn incoming_connection(
    mut self,
    edit: impl FnOnce(ConnectionBuilder) -> ConnectionBuilder,
  ) -> Self {
    let connection = self.config.incoming_connection;
    self.config.incoming_connection = edit(ConnectionBuilder { connection }).connection;
    self
  }

  /// Edits the options of parent and child connections in the distributed search tree.
  ///
  /// Defaults to the defaults of [`Connection`] with an inactivity timeout of 60 seconds.
  #[must_use]
  pub fn distributed_connection(
    mut self,
    edit: impl FnOnce(ConnectionBuilder) -> ConnectionBuilder,
  ) -> Self {
    let connection = self.config.distributed_connection;
    self.config.distributed_connection = edit(ConnectionBuilder { connection }).connection;
    self
  }

  /// Edits the bounds on remote input and on the work it can start.
  ///
  /// Defaults to the defaults of [`Limits`].
  #[must_use]
  pub fn limits(mut self, edit: impl FnOnce(LimitsBuilder) -> LimitsBuilder) -> Self {
    let limits = self.config.limits;
    self.config.limits = edit(LimitsBuilder { limits }).limits;
    self
  }

  /// Sets the handler that answers what peers ask of the client.
  ///
  /// Without one, peers get no answers and every file is refused as not shared.
  #[must_use]
  pub fn handler(mut self, handler: impl Handler) -> Self {
    self.config.handler = handler::Shared(std::sync::Arc::new(handler));
    self
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
  pub(crate) max_cached_search_responses: u32,
  pub(crate) search_response_cache_lifetime: Duration,
  pub(crate) max_cached_addresses: u32,
  pub(crate) address_cache_lifetime: Duration,
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
      max_cached_search_responses: 1000,
      search_response_cache_lifetime: Duration::from_secs(180),
      max_cached_addresses: 10_000,
      address_cache_lifetime: Duration::from_secs(60),
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
  /// Returns what the decoders accept from peers.
  pub fn decode(&self) -> soulseek_proto::Limits {
    self.decode
  }

  /// Returns the most outbound peer connection attempts in flight at once.
  pub fn max_concurrent_peer_connects(&self) -> u32 {
    self.max_concurrent_peer_connects
  }

  /// Returns the most peer message connections kept open at once.
  pub fn max_cached_peer_connections(&self) -> u32 {
    self.max_cached_peer_connections
  }

  /// Returns the most message connections one address may have open to the client at once.
  pub fn max_message_connections_per_address(&self) -> u32 {
    self.max_message_connections_per_address
  }

  /// Returns the most accepted connections at once that have not yet said what they are for.
  pub fn max_unidentified_connections(&self) -> u32 {
    self.max_unidentified_connections
  }

  /// Returns the most undelivered search responses kept for a late connection from the searcher.
  pub fn max_cached_search_responses(&self) -> u32 {
    self.max_cached_search_responses
  }

  /// Returns the time an undelivered search response is kept.
  pub fn search_response_cache_lifetime(&self) -> Duration {
    self.search_response_cache_lifetime
  }

  /// Returns the most user addresses remembered between connects.
  pub fn max_cached_addresses(&self) -> u32 {
    self.max_cached_addresses
  }

  /// Returns the time a user's address is remembered before the server is asked again.
  pub fn address_cache_lifetime(&self) -> Duration {
    self.address_cache_lifetime
  }

  /// Returns the time a would-be parent has to send its branch level, root and a search.
  pub fn distributed_parent_negotiation_timeout(&self) -> Duration {
    self.distributed_parent_negotiation_timeout
  }

  /// Returns the most searches from the distributed parent remembered by searcher and token.
  pub fn distributed_search_memory(&self) -> u32 {
    self.distributed_search_memory
  }

  /// Returns the time a search from the distributed parent is remembered.
  pub fn distributed_search_memory_lifetime(&self) -> Duration {
    self.distributed_search_memory_lifetime
  }

  /// Returns the time changes of the client's place in the distributed tree are coalesced.
  pub fn distributed_status_debounce(&self) -> Duration {
    self.distributed_status_debounce
  }

  /// Returns the age at which a status the server has is sent again without the debounce.
  pub fn distributed_status_max_age(&self) -> Duration {
    self.distributed_status_max_age
  }

  /// Returns the interval at which the server is asked for parent candidates again.
  pub fn distributed_parent_retry(&self) -> Duration {
    self.distributed_parent_retry
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
      "limits.max_cached_search_responses",
      self.max_cached_search_responses,
    )?;
    at_least_one("limits.max_cached_addresses", self.max_cached_addresses)?;
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

/// Builder of [`Limits`].
///
/// [`ConfigBuilder::limits`] hands one to the closure that edits the limits.
#[derive(Clone, Debug)]
pub struct LimitsBuilder {
  pub(crate) limits: Limits,
}

impl LimitsBuilder {
  /// Sets what the decoders accept from peers.
  ///
  /// These are soulseek-proto's own limits. A frame past its bound is skipped unread. Defaults
  /// to [`soulseek_proto::Limits::DEFAULT`], the limits as soulseek-proto sets them.
  #[must_use]
  pub fn decode(mut self, decode: soulseek_proto::Limits) -> Self {
    self.limits.decode = decode;
    self
  }

  /// Sets the most outbound peer connection attempts in flight at once.
  ///
  /// The attempts the client makes for itself and the ones it makes because a user asked the
  /// server to have the client connect are counted apart, each up to this many. Defaults to 50.
  #[must_use]
  pub fn max_concurrent_peer_connects(mut self, max_concurrent_peer_connects: u32) -> Self {
    self.limits.max_concurrent_peer_connects = max_concurrent_peer_connects;
    self
  }

  /// Sets the most peer message connections kept open at once.
  ///
  /// Defaults to 1000.
  #[must_use]
  pub fn max_cached_peer_connections(mut self, max_cached_peer_connections: u32) -> Self {
    self.limits.max_cached_peer_connections = max_cached_peer_connections;
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
    self.limits.max_message_connections_per_address = max_message_connections_per_address;
    self
  }

  /// Sets the most accepted connections at once that have not yet said what they are for.
  ///
  /// One more is closed on accept, so peers that connect and stay silent cannot use up file
  /// descriptors. Read when the listener starts. Defaults to 128.
  #[must_use]
  pub fn max_unidentified_connections(mut self, max_unidentified_connections: u32) -> Self {
    self.limits.max_unidentified_connections = max_unidentified_connections;
    self
  }

  /// Sets the most undelivered search responses kept for a late connection from the searcher.
  ///
  /// Defaults to 1000.
  #[must_use]
  pub fn max_cached_search_responses(mut self, max_cached_search_responses: u32) -> Self {
    self.limits.max_cached_search_responses = max_cached_search_responses;
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
    self.limits.search_response_cache_lifetime = search_response_cache_lifetime;
    self
  }

  /// Sets the most user addresses remembered between connects.
  ///
  /// Defaults to 10000.
  #[must_use]
  pub fn max_cached_addresses(mut self, max_cached_addresses: u32) -> Self {
    self.limits.max_cached_addresses = max_cached_addresses;
    self
  }

  /// Sets the time a user's address is remembered before the server is asked again.
  ///
  /// Defaults to 60 seconds.
  #[must_use]
  pub fn address_cache_lifetime(mut self, address_cache_lifetime: Duration) -> Self {
    self.limits.address_cache_lifetime = address_cache_lifetime;
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
    self.limits.distributed_parent_negotiation_timeout = distributed_parent_negotiation_timeout;
    self
  }

  /// Sets the most searches from the distributed parent remembered by searcher and token.
  ///
  /// A search that comes again while remembered is neither passed on to the children nor
  /// answered. 0 remembers none. Defaults to 1000.
  #[must_use]
  pub fn distributed_search_memory(mut self, distributed_search_memory: u32) -> Self {
    self.limits.distributed_search_memory = distributed_search_memory;
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
    self.limits.distributed_search_memory_lifetime = distributed_search_memory_lifetime;
    self
  }

  /// Sets the time changes of the client's place in the distributed tree are coalesced.
  ///
  /// The server is told after that. Defaults to 5 seconds.
  #[must_use]
  pub fn distributed_status_debounce(mut self, distributed_status_debounce: Duration) -> Self {
    self.limits.distributed_status_debounce = distributed_status_debounce;
    self
  }

  /// Sets the age at which a status the server has is sent again without the debounce.
  ///
  /// A status the server has had for this long is sent again when another change comes, without
  /// waiting out the debounce. Defaults to 300 seconds.
  #[must_use]
  pub fn distributed_status_max_age(mut self, distributed_status_max_age: Duration) -> Self {
    self.limits.distributed_status_max_age = distributed_status_max_age;
    self
  }

  /// Sets the interval at which the server is asked for parent candidates again.
  ///
  /// Applies to a client with neither parent nor branch. Defaults to 900 seconds.
  #[must_use]
  pub fn distributed_parent_retry(mut self, distributed_parent_retry: Duration) -> Self {
    self.limits.distributed_parent_retry = distributed_parent_retry;
    self
  }
}

fn invalid(field: &str, message: impl AsRef<str>) -> Error {
  Error::invalid_argument(format!("{field}: {}", message.as_ref()))
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
  use crate::error::{ErrorKind, Repr};

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
      listener: false,
      ..with(0, 1)
    };
    not_listening.validate().unwrap();
    assert!(matches!(
      with(0, 1).validate().as_ref().map_err(Error::repr),
      Err(Repr::InvalidArgument(message)) if message.starts_with("listen_port: must be between 1 and 65535")
    ));
    assert!(matches!(
      with(1, 0).validate().as_ref().map_err(Error::repr),
      Err(Repr::InvalidArgument(message)) if message.starts_with("listen_backlog: must be greater than or equal to 1")
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

    let off = ConfigBuilder::from(old.clone())
      .distributed_network(false)
      .config;
    assert!(off.reconnect_required(&old));
    assert!(!old.reconnect_required(&off), "turning it back on");

    let no_ping = ConfigBuilder::from(old.clone())
      .server_ping_interval(None)
      .config;
    assert!(no_ping.reconnect_required(&old));

    let no_children = ConfigBuilder::from(old.clone())
      .distributed_children(false)
      .config;
    assert!(no_children.reconnect_required(&old));

    let peer = ConfigBuilder::from(old.clone())
      .peer_connection(|connection| connection.connect_timeout(Duration::from_secs(3)))
      .config;
    assert!(!peer.reconnect_required(&old));

    let keepalive = ConfigBuilder::from(old.clone())
      .server_connection(|connection| {
        connection.keepalive(KeepAlive::new(
          Duration::from_secs(10),
          Duration::from_secs(2),
          5,
        ))
      })
      .config;
    assert!(keepalive.reconnect_required(&old));
  }

  #[test]
  fn the_server_connection_never_times_out_for_inactivity() {
    let old = Config::default();
    let new = ConfigBuilder::from(old.clone())
      .server_connection(|connection| connection.inactivity_timeout(Duration::from_secs(30)))
      .config;
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
      ConfigBuilder::from(old.clone()).listen_port(0).build(),
      Err(matched) if matched.kind() == ErrorKind::InvalidArgument
    ));
    assert!(matches!(
      ConfigBuilder::from(old.clone()).event_capacity(8).config.validate_change(&old).as_ref().map_err(Error::repr),
      Err(Repr::InvalidArgument(message)) if message.starts_with("event_capacity: is fixed")
    ));
    ConfigBuilder::from(old.clone())
      .listen_port(50_001)
      .config
      .validate_change(&old)
      .unwrap();
  }
}
