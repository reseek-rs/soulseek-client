//! The SOCKS5 handshake on a socket already connected to the proxy: method selection, the
//! optional username/password step, and the CONNECT request to an IPv4 address or a host name.
//! Every reply is read by its exact length, so replies split across segments are handled.

use std::net::SocketAddrV4;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::config::ProxyCredentials;
use crate::error::Error;

const VERSION: u8 = 5;
const AUTH_VERSION: u8 = 1;
const NO_AUTHENTICATION: u8 = 0;
const USERNAME_PASSWORD: u8 = 2;
const NO_ACCEPTABLE_METHOD: u8 = 0xff;
const CONNECT: u8 = 1;
const IPV4: u8 = 1;
const DOMAIN: u8 = 3;
const IPV6: u8 = 4;

/// The reply of a proxy whose destination refused the connection.
const CONNECTION_REFUSED: u8 = 5;

/// Asks the proxy on `stream` to connect to `destination`. Failures are proxy errors; one whose
/// destination refused the connection carries a refused connect as its cause, as a direct
/// connect would.
pub(crate) async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
  stream: &mut S,
  destination: Destination<'_>,
  credentials: Option<&ProxyCredentials>,
) -> Result<(), Error> {
  negotiate(stream, destination, credentials)
    .await
    .map_err(|error| match error {
      Failure::Proxy(message) => proxy_error(message),
      Failure::Refused(message) => Error::connection_from(
        crate::error::ConnectionKind::Proxy,
        message,
        std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
      ),
      Failure::Io(error) => Error::connection_from(
        crate::error::ConnectionKind::Proxy,
        format!("failed to connect to proxy: {error}"),
        error,
      ),
    })
}

/// where the proxy is asked to connect
#[derive(Clone, Copy, Debug)]
pub(crate) enum Destination<'a> {
  Address(SocketAddrV4),
  /// a host name, which the proxy resolves, and a port
  Name(&'a str, u16),
}

enum Failure {
  Proxy(String),
  /// the destination refused the proxy's connection
  Refused(String),
  Io(std::io::Error),
}

impl From<std::io::Error> for Failure {
  fn from(error: std::io::Error) -> Self {
    Self::Io(error)
  }
}

fn proxy_error(message: String) -> Error {
  Error::connection(crate::error::ConnectionKind::Proxy, message)
}

async fn negotiate<S: AsyncRead + AsyncWrite + Unpin>(
  stream: &mut S,
  destination: Destination<'_>,
  credentials: Option<&ProxyCredentials>,
) -> Result<(), Failure> {
  let methods: &[u8] = match credentials {
    Some(_) => &[VERSION, 2, NO_AUTHENTICATION, USERNAME_PASSWORD],
    None => &[VERSION, 1, NO_AUTHENTICATION],
  };
  stream.write_all(methods).await?;

  let [version, method] = read_array(stream).await?;
  check_version(version)?;
  match method {
    NO_AUTHENTICATION => {}
    USERNAME_PASSWORD => {
      let Some(credentials) = credentials else {
        return Err(Failure::Proxy(
          "the proxy asks for credentials and none are set".into(),
        ));
      };
      authenticate(stream, credentials).await?;
    }
    NO_ACCEPTABLE_METHOD => {
      return Err(Failure::Proxy(
        "server does not support the specified authentication method(s)".into(),
      ));
    }
    other => {
      return Err(Failure::Proxy(format!(
        "unknown auth METHOD response from server: {other}"
      )));
    }
  }

  let mut request = vec![VERSION, CONNECT, 0];
  match destination {
    Destination::Address(address) => {
      request.push(IPV4);
      request.extend(address.ip().octets());
      request.extend(address.port().to_be_bytes());
    }
    Destination::Name(name, port) => {
      let Ok(len) = u8::try_from(name.len()) else {
        return Err(Failure::Proxy(format!(
          "the host name is {} bytes long; a SOCKS5 request holds 255",
          name.len()
        )));
      };
      request.extend([DOMAIN, len]);
      request.extend(name.as_bytes());
      request.extend(port.to_be_bytes());
    }
  }
  stream.write_all(&request).await?;

  let [version, reply, _reserved, address_type] = read_array(stream).await?;
  check_version(version)?;
  if reply != 0 {
    let message = format!("SOCKS connection failed: {}", reply_text(reply));
    return Err(if reply == CONNECTION_REFUSED {
      Failure::Refused(message)
    } else {
      Failure::Proxy(message)
    });
  }
  // the address the proxy bound for the client is read off the stream and not used
  let address_len = match address_type {
    IPV4 => 4,
    IPV6 => 16,
    DOMAIN => match stream.read_u8().await? {
      0xff => return Err(Failure::Proxy("invalid domain name".into())),
      len => usize::from(len),
    },
    other => {
      return Err(Failure::Proxy(format!(
        "unknown SOCKS Address type (expected: 1, 3 or 4, received: {other})"
      )));
    }
  };
  let mut bound = vec![0; address_len + 2];
  stream.read_exact(&mut bound).await?;
  Ok(())
}

async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
  stream: &mut S,
  credentials: &ProxyCredentials,
) -> Result<(), Failure> {
  let username = credentials.username.as_bytes();
  let password = credentials.password.expose().as_bytes();
  // option validation keeps both between 1 and 255 bytes, so each length fits its byte
  let mut request = vec![AUTH_VERSION, username.len() as u8];
  request.extend(username);
  request.push(password.len() as u8);
  request.extend(password);
  stream.write_all(&request).await?;

  let [version, status] = read_array(stream).await?;
  if version != AUTH_VERSION {
    return Err(Failure::Proxy(format!(
      "invalid authentication subnegotiation version (expected: {AUTH_VERSION}, received: {version})"
    )));
  }
  if status != 0 {
    return Err(Failure::Proxy(format!(
      "authentication failed: error code {status}"
    )));
  }
  Ok(())
}

async fn read_array<S: AsyncRead + Unpin, const N: usize>(
  stream: &mut S,
) -> Result<[u8; N], Failure> {
  let mut bytes = [0; N];
  stream.read_exact(&mut bytes).await?;
  Ok(bytes)
}

fn check_version(version: u8) -> Result<(), Failure> {
  if version != VERSION {
    return Err(Failure::Proxy(format!(
      "invalid SOCKS version (expected: {VERSION}, received: {version})"
    )));
  }
  Ok(())
}

fn reply_text(reply: u8) -> String {
  match reply {
    1 => "general SOCKS server failure".into(),
    2 => "connection not allowed by ruleset".into(),
    3 => "network unreachable".into(),
    4 => "host unreachable".into(),
    CONNECTION_REFUSED => "connection refused".into(),
    6 => "tTL expired".into(),
    7 => "command not supported".into(),
    8 => "address type not supported".into(),
    other => format!("unknown SOCKS error {other}"),
  }
}

#[cfg(test)]
mod tests {
  use std::net::Ipv4Addr;

  use soulseek_proto::Secret;
  use tokio::io::{DuplexStream, duplex};

  use super::*;

  const DESTINATION: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(10, 1, 2, 3), 2271);

  fn credentials() -> ProxyCredentials {
    ProxyCredentials {
      username: "user".into(),
      password: Secret::new("pass"),
    }
  }

  /// a proxy that expects `expect` and answers with `reply`, one byte per write so every read
  /// on the client side has to gather its bytes
  async fn proxy(mut stream: DuplexStream, exchanges: Vec<(Vec<u8>, Vec<u8>)>) {
    for (expect, reply) in exchanges {
      let mut received = vec![0; expect.len()];
      stream.read_exact(&mut received).await.unwrap();
      assert_eq!(received, expect);
      for byte in reply {
        stream.write_all(&[byte]).await.unwrap();
        tokio::task::yield_now().await;
      }
    }
  }

  fn connect_request() -> Vec<u8> {
    vec![5, 1, 0, 1, 10, 1, 2, 3, 0x08, 0xdf]
  }

  async fn run(
    credentials: Option<ProxyCredentials>,
    exchanges: Vec<(Vec<u8>, Vec<u8>)>,
  ) -> Result<(), Error> {
    let (mut client, server) = duplex(64);
    let proxy = tokio::spawn(proxy(server, exchanges));
    let destination = Destination::Address(DESTINATION);
    let result = handshake(&mut client, destination, credentials.as_ref()).await;
    drop(client);
    proxy.await.unwrap();
    result
  }

  fn proxy_message(result: Result<(), Error>) -> String {
    match result {
      Err(Error::Connection {
        kind: crate::error::ConnectionKind::Proxy,
        message,
        ..
      }) => message,
      other => panic!("expected a proxy error, got {other:?}"),
    }
  }

  #[tokio::test]
  async fn a_host_name_goes_to_the_proxy_to_resolve() {
    let mut request = vec![5, 1, 0, 3, 14];
    request.extend(b"server.example");
    request.extend([0x08, 0xdf]);
    let (mut client, server) = duplex(64);
    let exchanges = vec![
      (vec![5, 1, 0], vec![5, 0]),
      (request, vec![5, 0, 0, 1, 127, 0, 0, 1, 0x1f, 0x90]),
    ];
    let served = tokio::spawn(proxy(server, exchanges));
    let destination = Destination::Name("server.example", 2271);
    handshake(&mut client, destination, None).await.unwrap();
    drop(client);
    served.await.unwrap();

    // a name the request cannot hold is refused before it is written
    let long = "x".repeat(256);
    let (mut client, server) = duplex(64);
    let served = tokio::spawn(proxy(server, vec![(vec![5, 1, 0], vec![5, 0])]));
    let refused = handshake(&mut client, Destination::Name(&long, 2271), None).await;
    assert!(proxy_message(refused).starts_with("the host name is 256 bytes long"));
    drop(client);
    served.await.unwrap();
  }

  #[tokio::test]
  async fn connects_without_authentication_from_split_replies() {
    run(
      None,
      vec![
        (vec![5, 1, 0], vec![5, 0]),
        (
          connect_request(),
          vec![5, 0, 0, 1, 127, 0, 0, 1, 0x1f, 0x90],
        ),
      ],
    )
    .await
    .unwrap();
  }

  #[tokio::test]
  async fn authenticates_and_reads_a_domain_bound_address() {
    run(
      Some(credentials()),
      vec![
        (vec![5, 2, 0, 2], vec![5, 2]),
        ([&[1, 4][..], b"user", &[4], b"pass"].concat(), vec![1, 0]),
        (
          connect_request(),
          [&[5, 0, 0, 3, 5][..], b"proxy", &[0x1f, 0x90]].concat(),
        ),
      ],
    )
    .await
    .unwrap();
  }

  #[tokio::test]
  async fn refused_authentication() {
    let result = run(
      Some(credentials()),
      vec![
        (vec![5, 2, 0, 2], vec![5, 2]),
        ([&[1, 4][..], b"user", &[4], b"pass"].concat(), vec![1, 1]),
      ],
    )
    .await;
    assert_eq!(proxy_message(result), "authentication failed: error code 1");
  }

  #[tokio::test]
  async fn authentication_requested_without_credentials() {
    let result = run(None, vec![(vec![5, 1, 0], vec![5, 2])]).await;
    assert_eq!(
      proxy_message(result),
      "the proxy asks for credentials and none are set"
    );
  }

  #[tokio::test]
  async fn wrong_version_and_no_acceptable_method() {
    let result = run(None, vec![(vec![5, 1, 0], vec![4, 0])]).await;
    assert_eq!(
      proxy_message(result),
      "invalid SOCKS version (expected: 5, received: 4)"
    );
    let result = run(None, vec![(vec![5, 1, 0], vec![5, 0xff])]).await;
    assert_eq!(
      proxy_message(result),
      "server does not support the specified authentication method(s)"
    );
  }

  #[tokio::test]
  async fn connect_failure_names_the_reply() {
    let result = run(
      None,
      vec![
        (vec![5, 1, 0], vec![5, 0]),
        (connect_request(), vec![5, 5, 0, 1]),
      ],
    )
    .await;
    assert_eq!(
      proxy_message(result),
      "SOCKS connection failed: connection refused"
    );
  }

  #[tokio::test]
  async fn proxy_closing_early_is_a_proxy_error() {
    let result = run(None, vec![(vec![5, 1, 0], vec![5])]).await;
    assert!(proxy_message(result).starts_with("failed to connect to proxy: "));
  }
}
