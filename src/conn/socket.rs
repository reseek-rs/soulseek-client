//! Opening a TCP socket: the keep-alive and segment size settings applied as soon as a socket
//! exists, and the connect itself, direct or through a SOCKS5 proxy.

use std::io;
use std::net::SocketAddr;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::{TcpSocket, TcpStream, lookup_host};

use super::socks5::{self, Destination};
use crate::config::{Connection, KeepAlive, Proxy};
use crate::error::{ConnectionErrorKind, Error};

/// Applies the keep-alive and segment size options to a socket. A failure only degrades the
/// connection, so it is logged and the socket is used anyway.
pub(crate) fn configure(socket: SockRef<'_>, options: &Connection) {
  if let Some(options) = &options.keepalive
    && let Err(error) = socket.set_tcp_keepalive(&keepalive(options))
  {
    tracing::warn!(%error, "failed to set TCP keep-alive; performance is degraded");
  }
  if let Some(mss) = options.max_segment_size {
    set_max_segment_size(&socket, mss);
  }
}

fn keepalive(options: &KeepAlive) -> TcpKeepalive {
  let keepalive = TcpKeepalive::new()
    .with_time(options.idle)
    .with_interval(options.interval);
  #[cfg(any(
    target_os = "android",
    target_os = "freebsd",
    target_os = "linux",
    target_os = "macos",
    target_os = "netbsd",
    windows
  ))]
  let keepalive = keepalive.with_retries(options.probes);
  keepalive
}

#[cfg(unix)]
fn set_max_segment_size(socket: &SockRef<'_>, mss: u32) {
  if let Err(error) = socket.set_tcp_mss(mss) {
    tracing::warn!(%error, mss, "failed to set TCP_MAXSEG; performance is degraded");
  }
}

#[cfg(not(unix))]
fn set_max_segment_size(_socket: &SockRef<'_>, mss: u32) {
  tracing::warn!(mss, "TCP_MAXSEG cannot be set on this platform");
}

/// Connects to `remote`, through the proxy when the options name one. The caller bounds the
/// whole of it, proxy handshake included, with one deadline.
pub(crate) async fn connect(remote: SocketAddr, options: &Connection) -> Result<TcpStream, Error> {
  match &options.proxy {
    None => connect_direct(remote, options).await.map_err(|error| {
      Error::connection_from(ConnectionErrorKind::Other, error.to_string(), error)
    }),
    Some(proxy) => {
      // every address the protocol carries is IPv4; only a server name with no A record
      // resolves to anything else
      let SocketAddr::V4(destination) = remote else {
        return Err(Error::connection(
          ConnectionErrorKind::Proxy,
          format!("failed to connect to proxy: the destination {remote} is not an IPv4 address"),
        ));
      };
      connect_through(proxy, Destination::Address(destination), options).await
    }
  }
}

/// Connects to the host `name` through `proxy`, which resolves the name, so no lookup of it is
/// made here. Bounded by the caller like [`connect`].
pub(crate) async fn connect_named(
  proxy: &Proxy,
  name: &str,
  port: u16,
  options: &Connection,
) -> Result<TcpStream, Error> {
  connect_through(proxy, Destination::Name(name, port), options).await
}

async fn connect_direct(remote: SocketAddr, options: &Connection) -> io::Result<TcpStream> {
  let socket = match remote {
    SocketAddr::V4(_) => TcpSocket::new_v4()?,
    SocketAddr::V6(_) => TcpSocket::new_v6()?,
  };
  configure(SockRef::from(&socket), options);
  socket.connect(remote).await
}

async fn connect_through(
  proxy: &Proxy,
  destination: Destination<'_>,
  options: &Connection,
) -> Result<TcpStream, Error> {
  let proxy_error = |message: String| Error::connection(ConnectionErrorKind::Proxy, message);
  let proxy_address = resolve(&proxy.address, proxy.port)
    .await
    .map_err(|error| proxy_error(format!("failed to connect to proxy: {error}")))?;
  tracing::debug!(proxy = %proxy_address, ?destination, "connecting through proxy");
  let mut stream = connect_direct(proxy_address, options)
    .await
    .map_err(|error| {
      Error::connection_from(
        ConnectionErrorKind::Proxy,
        format!("failed to connect to proxy: {error}"),
        error,
      )
    })?;
  socks5::handshake(&mut stream, destination, proxy.credentials.as_ref()).await?;
  Ok(stream)
}

/// Resolves a host name or IP literal, preferring an IPv4 address and falling back to the first
/// IPv6 one when there is none.
pub(crate) async fn resolve(address: &str, port: u16) -> io::Result<SocketAddr> {
  if let Ok(ip) = address.parse() {
    return Ok(SocketAddr::new(ip, port));
  }
  let addresses: Vec<SocketAddr> = lookup_host((address, port)).await?.collect();
  prefer_ipv4(&addresses)
    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no addresses found"))
}

fn prefer_ipv4(addresses: &[SocketAddr]) -> Option<SocketAddr> {
  addresses
    .iter()
    .find(|address| address.is_ipv4())
    .or(addresses.first())
    .copied()
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use tokio::net::TcpListener;

  use super::*;
  use crate::config::Config;

  /// a socket connected with the options the server connection is made with
  async fn server_socket(options: Config) -> (TcpStream, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let options = options.effective_server_connection_options();
    let stream = connect(listener.local_addr().unwrap(), &options)
      .await
      .unwrap();
    (stream, listener)
  }

  #[tokio::test]
  async fn the_server_socket_gets_the_server_keepalive_and_segment_size() {
    let mut options = Config::default();
    options.server_connection.max_segment_size = Some(1200);
    let (stream, _listener) = server_socket(options).await;
    let socket = SockRef::from(&stream);
    assert!(socket.keepalive().unwrap());
    #[cfg(target_os = "linux")]
    {
      assert_eq!(
        socket.tcp_keepalive_time().unwrap(),
        Duration::from_secs(10)
      );
      assert_eq!(
        socket.tcp_keepalive_interval().unwrap(),
        Duration::from_secs(2)
      );
      assert_eq!(socket.tcp_keepalive_retries().unwrap(), 3);
      // the kernel reports the segment size it settled on, which can only be smaller
      assert!(socket.tcp_mss().unwrap() <= 1200);
    }
  }

  #[test]
  fn an_ipv4_address_wins_over_an_earlier_ipv6_one() {
    let v6: SocketAddr = "[::1]:2271".parse().unwrap();
    let v4: SocketAddr = "127.0.0.1:2271".parse().unwrap();
    assert_eq!(prefer_ipv4(&[v6, v4]), Some(v4));
    assert_eq!(prefer_ipv4(&[v6]), Some(v6));
    assert_eq!(prefer_ipv4(&[]), None);
  }
}
