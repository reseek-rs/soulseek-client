//! The client's options: what it listens on, how each kind of connection behaves, the protocol
//! version it logs in with, the limits that bound remote input, and the application's resolvers.
//! Options are validated when the client is built and whenever a patch replaces them.

mod connection;

pub use connection::{ConnectionOptions, KeepAlive, ProxyCredentials, ProxyOptions};

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use soulseek_proto::{Limits, PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION};

use crate::error::ClientError;
use crate::resolvers::Resolvers;

/// Configuration of a [`Client`](crate::Client).
///
/// # Examples
///
/// ```
/// use soulseek_client::ClientOptions;
///
/// // a field left out keeps its default
/// let options = ClientOptions {
///   listen_port: 50_300,
///   enable_distributed_network: false,
///   ..ClientOptions::default()
/// };
/// options.validate()?;
/// # Ok::<(), soulseek_client::ClientError>(())
/// ```
#[derive(Clone, Debug)]
pub struct ClientOptions {
  /// Whether to accept connections from peers while connected.
  ///
  /// The listener accepts them on `listen_ip_address:listen_port`. Defaults to `true`.
  pub enable_listener: bool,
  /// Whether to keep the listener up between server sessions.
  ///
  /// Transfers under way then still take the connections peers open. If unset, the listener
  /// stops when the session ends. A change of the listener options while disconnected stops it
  /// until the next connect. Defaults to `false`.
  pub listen_while_disconnected: bool,
  /// Address the listener binds.
  ///
  /// Defaults to `0.0.0.0`, every interface.
  pub listen_ip_address: IpAddr,
  /// Port the listener binds.
  ///
  /// Must be 1 or more while the listener is on. Defaults to 50000.
  pub listen_port: u16,
  /// Port the server tells other users to connect to.
  ///
  /// If unset, it is `listen_port` while the listener is on and 0 while it is off. 0 says the
  /// client takes no direct connections. A port mapped from outside differs from the one bound.
  /// Unset by default.
  pub advertised_port: Option<u16>,
  /// TCP accept backlog of the listener.
  ///
  /// Defaults to [`i32::MAX`].
  pub listen_backlog: u32,
  /// Whether to look for a parent in the distributed search tree.
  ///
  /// The client then also passes searches on to children. Defaults to `true`.
  pub enable_distributed_network: bool,
  /// Whether a parent candidate must pass on a search before it counts.
  ///
  /// A search passed on shows that searches reach the candidate. On a quiet server none may come
  /// within the negotiation timeout. Defaults to `true`.
  pub parent_must_relay_search: bool,
  /// Whether other users may join the distributed search tree below the client.
  ///
  /// Defaults to `true`.
  pub accept_distributed_children: bool,
  /// Most children the client keeps at once.
  ///
  /// `follow_server_child_limits` may lower it. Defaults to 25.
  pub distributed_child_limit: u32,
  /// Whether to follow the server's limits on children.
  ///
  /// The client then lowers the child limit to what the server's speed ratio allows the client's
  /// upload speed, takes no children below the server's minimum speed, and asks for its own
  /// statistics after login to learn the speed. If unset, `distributed_child_limit` alone bounds
  /// the children. Defaults to `true`.
  pub follow_server_child_limits: bool,
  /// Whether to cancel every running search when the server session ends.
  ///
  /// If unset, each search ends by its own timeout or limit, taking the responses peers still
  /// send. Defaults to `true`.
  pub cancel_searches_on_disconnect: bool,
  /// Whether to check names against the official server's rules.
  ///
  /// The login username and the names of rooms to join are checked before anything is sent. A
  /// server with other rules needs this unset. Defaults to `true`.
  pub check_names: bool,
  /// Time a request waits for the reply that answers it.
  ///
  /// Fixed for the client's lifetime. Defaults to 5 seconds.
  pub message_timeout: Duration,
  /// Interval at which a ping is written to the server.
  ///
  /// The pings make a socket that stopped taking writes noticed. If unset, none is written,
  /// which leaves that to the connection's TCP keep-alive. The server needs no pings. Read at
  /// each login. Defaults to 60 seconds.
  pub server_ping_interval: Option<Duration>,
  /// Number of events the channel holds for its slowest subscriber.
  ///
  /// A subscriber gets [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged)
  /// once it is further behind. Fixed for the client's lifetime. Defaults to 4096.
  pub event_capacity: u32,
  /// Time a transfer waits for the peer to answer the transfer request.
  ///
  /// Applies to an upload, and to a download asked for with a transfer request. Defaults to 15
  /// seconds.
  pub transfer_response_timeout: Duration,
  /// Time a download waits for the uploader to open the transfer connection.
  ///
  /// The wait starts once the uploader's offer is accepted. Some clients take 20 to 30 seconds
  /// to fall back to a connection through the server. Defaults to 45 seconds.
  pub transfer_accept_timeout: Duration,
  /// Whether to tell a peer its place in the queue as soon as its download is queued.
  ///
  /// The place in the client's queue comes from [`Resolvers::place_in_queue`] and is sent as
  /// soon as a download the peer asked for is queued. If unset, the place is sent only when the
  /// peer asks. Defaults to `true`.
  pub send_place_when_queued: bool,
  /// Time telling a peer that an upload to it ended may take.
  ///
  /// The notice is given up after that. Defaults to 5 seconds.
  pub upload_notice_timeout: Duration,
  /// Time the offer of a file a user sends unasked is held.
  ///
  /// The offer is held for the download the application starts for it. One not taken by then is
  /// refused, as its sender no longer waits for an answer. Defaults to 30 seconds.
  pub unasked_offer_timeout: Duration,
  /// Options of the server connection.
  ///
  /// Its inactivity timeout is always off, since the server can be silent for a long time. The
  /// server never answers a ping either, so the connection's TCP keep-alive is how a dead server
  /// connection is noticed: after about `idle + probes * interval`. Defaults to the defaults of
  /// [`ConnectionOptions`] with no inactivity timeout and with keep-alive on: 10 seconds of
  /// silence, 2 seconds between probes, 3 probes.
  pub server_connection_options: ConnectionOptions,
  /// Options of peer message connections, outbound and inbound once identified.
  ///
  /// Defaults to the defaults of [`ConnectionOptions`].
  pub peer_connection_options: ConnectionOptions,
  /// Options of file transfer connections.
  ///
  /// Defaults to the defaults of [`ConnectionOptions`].
  pub transfer_connection_options: ConnectionOptions,
  /// Options of an accepted connection until its first message says what it is for.
  ///
  /// Defaults to the defaults of [`ConnectionOptions`].
  pub incoming_connection_options: ConnectionOptions,
  /// Options of parent and child connections in the distributed search tree.
  ///
  /// Defaults to the defaults of [`ConnectionOptions`] with an inactivity timeout of 60 seconds.
  pub distributed_connection_options: ConnectionOptions,
  /// Major version sent at login.
  ///
  /// Fixed for the client's lifetime. Defaults to [`PROTOCOL_MAJOR_VERSION`].
  pub protocol_major_version: u32,
  /// Minor version sent at login.
  ///
  /// Fixed for the client's lifetime. It tells client programs apart, so an application sends a
  /// number of its own. Defaults to soulseek-proto's, [`PROTOCOL_MINOR_VERSION`].
  pub protocol_minor_version: u32,
  /// Bounds on remote input and on the work it can start.
  ///
  /// Defaults to the defaults of [`ClientLimits`].
  pub limits: ClientLimits,
  /// Callbacks and caches of the application.
  ///
  /// Every resolver is unset by default.
  pub resolvers: Resolvers,
}

impl Default for ClientOptions {
  fn default() -> Self {
    Self {
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
      server_connection_options: ConnectionOptions {
        inactivity_timeout: None,
        keepalive: KeepAlive {
          enabled: true,
          idle: Duration::from_secs(10),
          interval: Duration::from_secs(2),
          probes: 3,
        },
        ..ConnectionOptions::default()
      },
      peer_connection_options: ConnectionOptions::default(),
      transfer_connection_options: ConnectionOptions::default(),
      incoming_connection_options: ConnectionOptions::default(),
      // a parent on a quiet server sends nothing for longer than other connections stay idle
      distributed_connection_options: ConnectionOptions {
        inactivity_timeout: Some(Duration::from_secs(60)),
        ..ConnectionOptions::default()
      },
      protocol_major_version: PROTOCOL_MAJOR_VERSION,
      protocol_minor_version: PROTOCOL_MINOR_VERSION,
      limits: ClientLimits::default(),
      resolvers: Resolvers::default(),
    }
  }
}

impl ClientOptions {
  /// the port the server is told other users reach the client on
  pub(crate) fn port_advertised(&self) -> u16 {
    let listening = if self.enable_listener {
      self.listen_port
    } else {
      0
    };
    self.advertised_port.unwrap_or(listening)
  }

  /// Checks every field, failing on the first one out of range.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::InvalidArgument`] if a field is out of range.
  pub fn validate(&self) -> Result<(), ClientError> {
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
    self
      .server_connection_options
      .validate("server_connection_options")?;
    self
      .peer_connection_options
      .validate("peer_connection_options")?;
    self
      .transfer_connection_options
      .validate("transfer_connection_options")?;
    self
      .incoming_connection_options
      .validate("incoming_connection_options")?;
    self
      .distributed_connection_options
      .validate("distributed_connection_options")?;
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

  /// the options the server connection is actually created with
  pub(crate) fn effective_server_connection_options(&self) -> ConnectionOptions {
    ConnectionOptions {
      inactivity_timeout: None,
      ..self.server_connection_options.clone()
    }
  }
}

/// Bounds on remote input and on the work it can make the client start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientLimits {
  /// What the decoders accept from peers.
  ///
  /// These are soulseek-proto's own limits. A frame past its bound is skipped unread. Defaults
  /// to [`Limits::DEFAULT`], the limits as soulseek-proto sets them.
  pub decode: Limits,
  /// Most outbound peer connection attempts in flight at once.
  ///
  /// Defaults to 50.
  pub max_concurrent_peer_connects: u32,
  /// Most peer message connections kept open at once.
  ///
  /// Defaults to 1000.
  pub max_cached_peer_connections: u32,
  /// Most message connections one address may have open to the client at once.
  ///
  /// Counts current, replaced and still arriving connections. One more is closed. A few users
  /// behind one NAT fit, a host posing as many users cannot fill the cache. Defaults to 8.
  pub max_message_connections_per_address: u32,
  /// Most accepted connections at once that have not yet said what they are for.
  ///
  /// One more is closed on accept, so peers that connect and stay silent cannot use up file
  /// descriptors. Read when the listener starts. Defaults to 128.
  pub max_unidentified_connections: u32,
  /// Most undelivered search responses kept for a late connection from the searcher.
  ///
  /// Defaults to 1000.
  pub search_response_cache_size: u32,
  /// Time an undelivered search response is kept.
  ///
  /// Defaults to 180 seconds.
  pub search_response_cache_lifetime: Duration,
  /// Most user addresses remembered between connects.
  ///
  /// Defaults to 10000.
  pub user_endpoint_cache_size: u32,
  /// Time a user's address is remembered before the server is asked again.
  ///
  /// Defaults to 60 seconds.
  pub user_endpoint_cache_lifetime: Duration,
  /// Time a would-be parent has to send its branch level, root and a search.
  ///
  /// Defaults to 5 seconds.
  pub distributed_parent_negotiation_timeout: Duration,
  /// Most searches from the distributed parent remembered by searcher and token.
  ///
  /// A search that comes again while remembered is neither passed on to the children nor
  /// answered. 0 remembers none. Defaults to 1000.
  pub distributed_search_memory: u32,
  /// Time a search from the distributed parent is remembered.
  ///
  /// Defaults to 60 seconds.
  pub distributed_search_memory_lifetime: Duration,
  /// Time changes of the client's place in the distributed tree are coalesced.
  ///
  /// The server is told after that. Defaults to 5 seconds.
  pub distributed_status_debounce: Duration,
  /// Age at which a status the server has is sent again without the debounce.
  ///
  /// A status the server has had for this long is sent again when another change comes, without
  /// waiting out the debounce. Defaults to 300 seconds.
  pub distributed_status_max_age: Duration,
  /// Interval at which the server is asked for parent candidates again.
  ///
  /// Applies to a client with neither parent nor branch. Defaults to 900 seconds.
  pub distributed_parent_retry: Duration,
}

impl Default for ClientLimits {
  fn default() -> Self {
    Self {
      decode: Limits::DEFAULT,
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

impl ClientLimits {
  fn validate(&self) -> Result<(), ClientError> {
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

fn invalid(field: &str, message: impl AsRef<str>) -> ClientError {
  ClientError::InvalidArgument(format!("{field}: {}", message.as_ref()))
}

fn invalid_in(name: &str, field: &str, message: impl AsRef<str>) -> ClientError {
  invalid(&format!("{name}.{field}"), message)
}

fn at_least_one(field: &str, value: u32) -> Result<(), ClientError> {
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
    let with = |listen_port, listen_backlog| ClientOptions {
      listen_port,
      listen_backlog,
      ..ClientOptions::default()
    };
    with(1, 1).validate().unwrap();
    // a client that does not listen binds nothing
    let not_listening = ClientOptions {
      enable_listener: false,
      ..with(0, 1)
    };
    not_listening.validate().unwrap();
    assert!(matches!(
      with(0, 1).validate(),
      Err(ClientError::InvalidArgument(message)) if message.starts_with("listen_port: must be between 1 and 65535")
    ));
    assert!(matches!(
      with(1, 0).validate(),
      Err(ClientError::InvalidArgument(message)) if message.starts_with("listen_backlog: must be greater than or equal to 1")
    ));
  }

  #[test]
  fn any_protocol_version_but_zero() {
    let with = |protocol_major_version, protocol_minor_version| ClientOptions {
      protocol_major_version,
      protocol_minor_version,
      ..ClientOptions::default()
    };
    with(1, 1).validate().unwrap();
    assert!(with(0, 1).validate().is_err());
    assert!(with(1, 0).validate().is_err());
  }
}
