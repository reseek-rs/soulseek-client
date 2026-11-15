//! The options of one kind of connection: buffer sizes, the write queue, the connect and
//! inactivity timeouts, the SOCKS5 proxy and the socket settings.

use std::time::Duration;

use soulseek_proto::Secret;
use tokio::sync::Semaphore;

use super::invalid_in;
use crate::error::ClientError;

/// How one kind of connection reads, writes, times out and reaches its remote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionOptions {
  /// Size of a socket read, in bytes.
  ///
  /// Defaults to 16384.
  pub read_buffer_size: usize,
  /// Size of a socket write, in bytes.
  ///
  /// Defaults to 16384.
  pub write_buffer_size: usize,
  /// Most writes that may be waiting on the connection.
  ///
  /// One more closes the connection as dead. Defaults to 250.
  pub write_queue_size: u32,
  /// Deadline for the TCP connect and the proxy handshake together.
  ///
  /// On an accepted connection, it is the deadline for its first message to arrive. Defaults to
  /// 10 seconds.
  pub connect_timeout: Duration,
  /// Time the connection may go without a byte read or written before it is closed.
  ///
  /// `None` never closes it for inactivity. Defaults to 15 seconds.
  pub inactivity_timeout: Option<Duration>,
  /// Time a write may wait for the socket to take a byte before the connection is closed.
  ///
  /// `None` never closes it for a stalled write. Unset by default.
  pub write_timeout: Option<Duration>,
  /// SOCKS5 proxy through which outbound remotes are reached.
  ///
  /// Unset by default.
  pub proxy: Option<ProxyOptions>,
  /// TCP keep-alive on sockets made with these options.
  ///
  /// Defaults to the defaults of [`KeepAlive`].
  pub keepalive: KeepAlive,
  /// `TCP_MAXSEG` on sockets made with these options.
  ///
  /// Set where the platform lets it be set. Unset by default.
  pub max_segment_size: Option<u32>,
}

impl Default for ConnectionOptions {
  fn default() -> Self {
    Self {
      read_buffer_size: 16_384,
      write_buffer_size: 16_384,
      write_queue_size: 250,
      connect_timeout: Duration::from_secs(10),
      inactivity_timeout: Some(Duration::from_secs(15)),
      write_timeout: None,
      proxy: None,
      keepalive: KeepAlive::default(),
      max_segment_size: None,
    }
  }
}

impl ConnectionOptions {
  pub(super) fn validate(&self, name: &str) -> Result<(), ClientError> {
    if self.read_buffer_size == 0 {
      return Err(invalid_in(
        name,
        "read_buffer_size",
        "must be greater than zero",
      ));
    }
    if self.write_buffer_size == 0 {
      return Err(invalid_in(
        name,
        "write_buffer_size",
        "must be greater than zero",
      ));
    }
    // the write queue is a channel of this capacity, which tokio caps
    if self.write_queue_size == 0 || self.write_queue_size as usize > Semaphore::MAX_PERMITS {
      return Err(invalid_in(
        name,
        "write_queue_size",
        format!(
          "must be between 1 and {} (specified: {})",
          Semaphore::MAX_PERMITS,
          self.write_queue_size
        ),
      ));
    }
    if self.connect_timeout.is_zero() {
      return Err(invalid_in(
        name,
        "connect_timeout",
        "must be greater than zero",
      ));
    }
    if self
      .inactivity_timeout
      .is_some_and(|timeout| timeout.is_zero())
    {
      return Err(invalid_in(
        name,
        "inactivity_timeout",
        "must be greater than zero, or unset to disable it",
      ));
    }
    if self.write_timeout.is_some_and(|timeout| timeout.is_zero()) {
      return Err(invalid_in(
        name,
        "write_timeout",
        "must be greater than zero, or unset to disable it",
      ));
    }
    if let Some(proxy) = &self.proxy {
      proxy.validate(name)?;
    }
    self.keepalive.validate(&format!("{name}.keepalive"))
  }
}

/// TCP keep-alive settings for a socket.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeepAlive {
  /// Whether to send keep-alive probes.
  ///
  /// The other fields matter only when this is set. Defaults to `false`.
  pub enabled: bool,
  /// Silence before the first probe, in whole seconds.
  ///
  /// Defaults to zero.
  pub idle: Duration,
  /// Time between probes, in whole seconds.
  ///
  /// Defaults to zero.
  pub interval: Duration,
  /// Number of unanswered probes before the connection is dropped.
  ///
  /// Defaults to 0.
  pub probes: u32,
}

impl KeepAlive {
  pub(super) fn validate(&self, name: &str) -> Result<(), ClientError> {
    if !self.enabled {
      return Ok(());
    }
    if self.idle < Duration::from_secs(1) {
      return Err(invalid_in(name, "idle", "must be at least 1 second"));
    }
    if self.interval < Duration::from_secs(1) {
      return Err(invalid_in(name, "interval", "must be at least 1 second"));
    }
    if self.probes == 0 {
      return Err(invalid_in(name, "probes", "must be at least 1"));
    }
    Ok(())
  }
}

/// SOCKS5 proxy.
///
/// The address is resolved on every connect, so a changed DNS record is picked up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyOptions {
  /// Hostname or IP address.
  pub address: String,
  /// Port the proxy listens on.
  pub port: u16,
  /// Username and password for proxies that ask for them.
  ///
  /// `None` offers only no authentication.
  pub credentials: Option<ProxyCredentials>,
}

impl ProxyOptions {
  fn validate(&self, name: &str) -> Result<(), ClientError> {
    if self.address.trim().is_empty() {
      return Err(invalid_in(
        name,
        "proxy.address",
        "must not be empty or whitespace",
      ));
    }
    if let Some(credentials) = &self.credentials {
      // SOCKS5 sends each as a one-byte length and the bytes
      for (field, value) in [
        ("proxy.username", credentials.username.as_str()),
        ("proxy.password", credentials.password.expose()),
      ] {
        if !(1..=255).contains(&value.len()) {
          return Err(invalid_in(name, field, "must be between 1 and 255 bytes"));
        }
      }
    }
    Ok(())
  }
}

/// Username and password a SOCKS5 proxy asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyCredentials {
  /// Username, of 1 to 255 bytes.
  pub username: String,
  /// Password, of 1 to 255 bytes.
  ///
  /// Never logged.
  pub password: Secret,
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::options::ClientOptions;

  #[test]
  fn proxy_credentials_fit_socks5() {
    let mut options = ClientOptions::default();
    options.peer_connection_options.proxy = Some(ProxyOptions {
      address: "proxy".into(),
      port: 1080,
      credentials: Some(ProxyCredentials {
        username: "u".repeat(256),
        password: Secret::new("p"),
      }),
    });
    assert!(matches!(
      options.validate(),
      Err(ClientError::InvalidArgument(message)) if message.starts_with("peer_connection_options.proxy.username")
    ));
  }
}
