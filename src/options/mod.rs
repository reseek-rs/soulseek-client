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

/// Everything that configures a [`Client`](crate::Client)
#[derive(Clone, Debug)]
pub struct ClientOptions {
  /// accept connections from peers on `listen_ip_address:listen_port` while connected
  pub enable_listener: bool,
  /// the address the listener binds; every interface by default
  pub listen_ip_address: IpAddr,
  /// the port the listener binds
  pub listen_port: u16,
  /// The port the server tells other users to connect to; unset, it is `listen_port`, whether
  /// or not we listen. A port mapped from outside differs from the one bound, and 0 says the
  /// client takes no direct connections.
  pub advertised_port: Option<u16>,
  /// TCP accept backlog of the listener
  pub listen_backlog: u32,
  /// look for a parent in the distributed search tree and pass searches on to children
  pub enable_distributed_network: bool,
  /// let other users join the distributed search tree below us
  pub accept_distributed_children: bool,
  /// most children we keep at once; `follow_server_child_limits` may lower it
  pub distributed_child_limit: u32,
  /// Lower the child limit to what the server's speed ratio allows our upload speed, take no
  /// children below its minimum speed, and ask for our own statistics after login to learn the
  /// speed. Unset, `distributed_child_limit` alone bounds the children.
  pub follow_server_child_limits: bool,
  /// drop a distributed search identical to the one received just before it
  pub deduplicate_search_requests: bool,
  /// Check the login username and the names of rooms to join against the official server's
  /// rules before anything is sent. A server with other rules needs this unset.
  pub check_names: bool,
  /// how long a request waits for the reply that answers it; fixed for the client's lifetime
  pub message_timeout: Duration,
  /// how often a ping is written to the server, so a socket that stopped taking writes is
  /// noticed; read at each login
  pub server_ping_interval: Duration,
  /// how many events the channel holds for its slowest subscriber, which gets
  /// `RecvError::Lagged` once it is further behind; fixed for the client's lifetime
  pub event_capacity: u32,
  /// how long an upload, or a download asked for with a transfer request, waits for the peer
  /// to answer the transfer request
  pub transfer_response_timeout: Duration,
  /// how long telling a peer that an upload to them ended may take before it is given up
  pub upload_notice_timeout: Duration,
  /// how long the offer of a file a user sends unasked is held for the download the application
  /// starts for it; one not taken by then is refused, as its sender no longer waits for an answer
  pub unasked_offer_timeout: Duration,
  /// the server connection. Its inactivity timeout is always off, since the server can be silent
  /// for a long time; the server never answers a ping either, so its TCP keep-alive is how a
  /// dead server connection is noticed: after about `idle + probes * interval`.
  pub server_connection_options: ConnectionOptions,
  /// peer message connections, outbound and inbound once identified
  pub peer_connection_options: ConnectionOptions,
  /// file transfer connections; the connect timeout also bounds how long a download waits for
  /// the uploader to connect to us
  pub transfer_connection_options: ConnectionOptions,
  /// an accepted connection until its first message says what it is for
  pub incoming_connection_options: ConnectionOptions,
  /// parent and child connections in the distributed search tree
  pub distributed_connection_options: ConnectionOptions,
  /// the major version sent at login; fixed for the client's lifetime
  pub protocol_major_version: u32,
  /// the minor version sent at login; fixed for the client's lifetime
  pub protocol_minor_version: u32,
  /// bounds on remote input and on the work it can start
  pub limits: ClientLimits,
  /// the application's callbacks and caches
  pub resolvers: Resolvers,
}

impl Default for ClientOptions {
  fn default() -> Self {
    Self {
      enable_listener: true,
      listen_ip_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
      listen_port: 50_000,
      advertised_port: None,
      listen_backlog: i32::MAX as u32,
      enable_distributed_network: true,
      accept_distributed_children: true,
      distributed_child_limit: 25,
      follow_server_child_limits: true,
      deduplicate_search_requests: true,
      check_names: true,
      message_timeout: Duration::from_secs(5),
      server_ping_interval: Duration::from_secs(60),
      event_capacity: 4096,
      transfer_response_timeout: Duration::from_secs(15),
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
      distributed_connection_options: ConnectionOptions::default(),
      protocol_major_version: PROTOCOL_MAJOR_VERSION,
      protocol_minor_version: PROTOCOL_MINOR_VERSION,
      limits: ClientLimits::default(),
      resolvers: Resolvers::default(),
    }
  }
}

impl ClientOptions {
  /// the port the server is told other users reach us on
  pub fn port_advertised(&self) -> u16 {
    self.advertised_port.unwrap_or(self.listen_port)
  }

  /// checks every field, failing on the first one out of range
  pub fn validate(&self) -> Result<(), ClientError> {
    // unless another is advertised, other users are told to connect to it, so the system
    // cannot pick it
    if self.listen_port == 0 {
      return Err(invalid("listen_port", "Must be between 1 and 65535"));
    }
    // the system takes the backlog as a positive signed number
    if !(1..=i32::MAX as u32).contains(&self.listen_backlog) {
      return Err(invalid(
        "listen_backlog",
        format!(
          "Must be greater than or equal to 1 and at most {} (specified: {})",
          i32::MAX,
          self.listen_backlog
        ),
      ));
    }
    if self.message_timeout.is_zero() {
      return Err(invalid("message_timeout", "Must be greater than zero"));
    }
    if self.server_ping_interval.is_zero() {
      return Err(invalid("server_ping_interval", "Must be greater than zero"));
    }
    at_least_one("event_capacity", self.event_capacity)?;
    if self.transfer_response_timeout.is_zero() {
      return Err(invalid(
        "transfer_response_timeout",
        "Must be greater than zero",
      ));
    }
    if self.upload_notice_timeout.is_zero() {
      return Err(invalid(
        "upload_notice_timeout",
        "Must be greater than zero",
      ));
    }
    if self.unasked_offer_timeout.is_zero() {
      return Err(invalid(
        "unasked_offer_timeout",
        "Must be greater than zero",
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
        "Must be greater than zero",
      ));
    }
    if self.protocol_minor_version == 0 {
      return Err(invalid(
        "protocol_minor_version",
        "Must be greater than zero",
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

/// Bounds on remote input and on the work it can make the client start
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientLimits {
  /// Largest browse response accepted, as its frame length. Every other peer message is bounded
  /// by what its own fields may need, and a frame past its bound is skipped unread.
  pub max_message_size: u32,
  /// outbound peer connection attempts in flight at once
  pub max_concurrent_peer_connects: u32,
  /// peer message connections kept open at once
  pub max_cached_peer_connections: u32,
  /// message connections one address may have open to us at once, current, replaced or still
  /// arriving; one more is closed. A few users behind one NAT fit, a host posing as many users
  /// cannot fill the cache.
  pub max_message_connections_per_address: u32,
  /// accepted connections that have not yet said what they are for, at once; one more is closed
  /// on accept, so peers that connect and stay silent cannot use up file descriptors. Read when
  /// the listener starts.
  pub max_unidentified_connections: u32,
  /// undelivered search responses kept for a late connection from the searcher
  pub search_response_cache_size: u32,
  /// how long an undelivered search response is kept
  pub search_response_cache_lifetime: Duration,
  /// user addresses remembered between connects
  pub user_endpoint_cache_size: u32,
  /// how long a user's address is remembered before the server is asked again
  pub user_endpoint_cache_lifetime: Duration,
  /// how long a would-be parent has to send its branch level, root and a search
  pub distributed_parent_negotiation_timeout: Duration,
}

impl Default for ClientLimits {
  fn default() -> Self {
    Self {
      max_message_size: 256 << 20,
      max_concurrent_peer_connects: 50,
      max_cached_peer_connections: 1000,
      max_message_connections_per_address: 8,
      max_unidentified_connections: 128,
      search_response_cache_size: 1000,
      search_response_cache_lifetime: Duration::from_secs(180),
      user_endpoint_cache_size: 10_000,
      user_endpoint_cache_lifetime: Duration::from_secs(60),
      distributed_parent_negotiation_timeout: Duration::from_secs(5),
    }
  }
}

impl ClientLimits {
  /// the frame and payload limits the decoders enforce
  pub fn frame_limits(&self) -> Limits {
    Limits {
      browse_message_bytes: self.max_message_size,
      ..Limits::DEFAULT
    }
  }

  fn validate(&self) -> Result<(), ClientError> {
    // a frame must at least hold its 4-byte code
    if self.max_message_size < 4 {
      return Err(invalid(
        "limits.max_message_size",
        "Must be at least 4 bytes",
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
        "Must be greater than zero",
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
    return Err(invalid(field, "Must be greater than or equal to 1"));
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
    assert!(matches!(
      with(0, 1).validate(),
      Err(ClientError::InvalidArgument(message)) if message.starts_with("listen_port: Must be between 1 and 65535")
    ));
    assert!(matches!(
      with(1, 0).validate(),
      Err(ClientError::InvalidArgument(message)) if message.starts_with("listen_backlog: Must be greater than or equal to 1")
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
