//! The options of one kind of connection: buffer sizes, the write queue, the connect and
//! inactivity timeouts, the SOCKS5 proxy and the socket settings.

use std::time::Duration;

use soulseek_proto::Secret;
use tokio::sync::Semaphore;

use super::invalid_in;
use crate::error::Error;

/// How one kind of connection reads, writes, times out and reaches its remote.
///
/// [`Config`](crate::Config) holds one for each kind of connection and edits it through a
/// closure, as [`Config::peer_connection`](crate::Config::peer_connection) does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connection {
  pub(crate) read_buffer_size: usize,
  pub(crate) write_buffer_size: usize,
  pub(crate) write_queue_size: u32,
  pub(crate) connect_timeout: Duration,
  pub(crate) inactivity_timeout: Option<Duration>,
  pub(crate) write_timeout: Option<Duration>,
  pub(crate) proxy: Option<Proxy>,
  pub(crate) keepalive: Option<KeepAlive>,
  pub(crate) max_segment_size: Option<u32>,
}

impl Default for Connection {
  fn default() -> Self {
    Self {
      read_buffer_size: 16_384,
      write_buffer_size: 16_384,
      write_queue_size: 250,
      connect_timeout: Duration::from_secs(10),
      inactivity_timeout: Some(Duration::from_secs(15)),
      write_timeout: None,
      proxy: None,
      keepalive: None,
      max_segment_size: None,
    }
  }
}

impl Connection {
  /// Sets the size of a socket read, in bytes.
  ///
  /// Defaults to 16384.
  #[must_use]
  pub fn read_buffer_size(mut self, read_buffer_size: usize) -> Self {
    self.read_buffer_size = read_buffer_size;
    self
  }

  /// Sets the size of a socket write, in bytes.
  ///
  /// Defaults to 16384.
  #[must_use]
  pub fn write_buffer_size(mut self, write_buffer_size: usize) -> Self {
    self.write_buffer_size = write_buffer_size;
    self
  }

  /// Sets the most writes that may be waiting on the connection.
  ///
  /// One more closes the connection as dead. Defaults to 250.
  #[must_use]
  pub fn write_queue_size(mut self, write_queue_size: u32) -> Self {
    self.write_queue_size = write_queue_size;
    self
  }

  /// Sets the deadline for the TCP connect and the proxy handshake together.
  ///
  /// On an accepted connection, it is the deadline for its first message to arrive. Defaults to
  /// 10 seconds.
  #[must_use]
  pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
    self.connect_timeout = connect_timeout;
    self
  }

  /// Sets the time the connection may go without a byte read or written before it is closed.
  ///
  /// `None` never closes it for inactivity. Defaults to 15 seconds.
  #[must_use]
  pub fn inactivity_timeout(mut self, inactivity_timeout: impl Into<Option<Duration>>) -> Self {
    self.inactivity_timeout = inactivity_timeout.into();
    self
  }

  /// Sets the time a write may wait for the socket to take a byte before the connection is closed.
  ///
  /// `None` never closes it for a stalled write. Unset by default.
  #[must_use]
  pub fn write_timeout(mut self, write_timeout: impl Into<Option<Duration>>) -> Self {
    self.write_timeout = write_timeout.into();
    self
  }

  /// Sets the SOCKS5 proxy through which outbound remotes are reached.
  ///
  /// Unset by default.
  #[must_use]
  pub fn proxy(mut self, proxy: impl Into<Option<Proxy>>) -> Self {
    self.proxy = proxy.into();
    self
  }

  /// Sets the TCP keep-alive on sockets made with these options.
  ///
  /// `None` sends no probes. Unset by default.
  #[must_use]
  pub fn keepalive(mut self, keepalive: impl Into<Option<KeepAlive>>) -> Self {
    self.keepalive = keepalive.into();
    self
  }

  /// Sets `TCP_MAXSEG` on sockets made with these options.
  ///
  /// Set where the platform lets it be set. Unset by default.
  #[must_use]
  pub fn max_segment_size(mut self, max_segment_size: impl Into<Option<u32>>) -> Self {
    self.max_segment_size = max_segment_size.into();
    self
  }

  pub(super) fn validate(&self, name: &str) -> Result<(), Error> {
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
    match &self.keepalive {
      Some(keepalive) => keepalive.validate(&format!("{name}.keepalive")),
      None => Ok(()),
    }
  }
}

/// TCP keep-alive probes on a socket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeepAlive {
  pub(crate) idle: Duration,
  pub(crate) interval: Duration,
  pub(crate) probes: u32,
}

impl KeepAlive {
  /// Creates keep-alive settings.
  ///
  /// The first probe goes out after `idle` of silence, the next ones `interval` apart, and the
  /// connection is dropped after `probes` of them go unanswered. Both times are taken in whole
  /// seconds and must be at least one second, and `probes` at least 1.
  #[must_use]
  pub fn new(idle: Duration, interval: Duration, probes: u32) -> Self {
    Self {
      idle,
      interval,
      probes,
    }
  }

  pub(super) fn validate(&self, name: &str) -> Result<(), Error> {
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
pub struct Proxy {
  pub(crate) address: String,
  pub(crate) port: u16,
  pub(crate) credentials: Option<ProxyCredentials>,
}

impl Proxy {
  /// Creates a proxy at a hostname or IP address and a port.
  ///
  /// It offers no authentication until [`Proxy::credentials`] sets some.
  #[must_use]
  pub fn new(address: impl Into<String>, port: u16) -> Self {
    Self {
      address: address.into(),
      port,
      credentials: None,
    }
  }

  /// Sets the username and password for a proxy that asks for them.
  ///
  /// Each is 1 to 255 bytes. The password is never logged.
  #[must_use]
  pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
    self.credentials = Some(ProxyCredentials {
      username: username.into(),
      password: Secret::new(password.into()),
    });
    self
  }

  fn validate(&self, name: &str) -> Result<(), Error> {
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

/// the username and password a SOCKS5 proxy asks for
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProxyCredentials {
  pub(crate) username: String,
  pub(crate) password: Secret,
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::Config;

  #[test]
  fn proxy_credentials_fit_socks5() {
    let mut options = Config::default();
    options.peer_connection.proxy = Some(Proxy {
      address: "proxy".into(),
      port: 1080,
      credentials: Some(ProxyCredentials {
        username: "u".repeat(256),
        password: Secret::new("p"),
      }),
    });
    assert!(matches!(
      options.validate(),
      Err(Error::InvalidArgument(message)) if message.starts_with("peer_connection.proxy.username")
    ));
  }
}
