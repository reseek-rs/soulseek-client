//! Connection requests relayed by a scripted server: we answer a peer's request by dialing it,
//! report the ones we cannot honor so the peer stops waiting, retry when a peer asks again as
//! older clients do, and give up our own request at once when the peer reports it cannot
//! connect.

use std::net::Ipv4Addr;
use std::time::Duration;

use crate::support::peer::{FakePeer, PeerSide};
use crate::support::{Session, free_port, listening_options, options};
use soulseek_client::ClientError;
use soulseek_client::testing::Engine;
use soulseek_proto::init::{PeerInit, PierceFirewall};
use soulseek_proto::server::{
  CannotConnectRequest, CannotConnectResponse, ConnectToPeerRequest, ConnectToPeerResponse,
};
use soulseek_proto::{InitMessage, PeerConnectionType, PeerMessage, ServerRequest, ServerResponse};
use tokio_util::sync::CancellationToken;

fn request(
  username: &str,
  connection_type: PeerConnectionType,
  port: u32,
  token: u32,
) -> ServerResponse {
  ServerResponse::ConnectToPeer(ConnectToPeerResponse {
    username: username.into(),
    connection_type,
    ip: Ipv4Addr::LOCALHOST,
    port,
    token,
    privileged: false,
    obfuscation: None,
  })
}

async fn cannot_connect(session: &mut Session) -> CannotConnectRequest {
  session
    .server
    .request_matching(|request| match request {
      ServerRequest::CannotConnect(report) => Some(report),
      _ => None,
    })
    .await
}

#[tokio::test]
async fn a_peer_we_cannot_dial_back_is_told_so() {
  let mut session = Session::start().await;
  let closed_port = u32::from(free_port());
  session
    .server
    .send(&request("erin", PeerConnectionType::Peer, closed_port, 31))
    .await;
  assert_eq!(
    cannot_connect(&mut session).await,
    CannotConnectRequest {
      token: 31,
      username: "erin".into()
    }
  );
}

#[tokio::test]
async fn transfer_and_child_requests_we_do_not_take_are_refused_at_once() {
  let mut session = Session::start().await;
  let peer = FakePeer::start().await;
  // no download waits for a file from this user
  session
    .server
    .send(&request(
      "stranger",
      PeerConnectionType::Transfer,
      peer.port(),
      32,
    ))
    .await;
  assert_eq!(cannot_connect(&mut session).await.token, 32);
  // we have neither a parent nor a branch, so we take no children
  session
    .server
    .send(&request(
      "child",
      PeerConnectionType::Distributed,
      peer.port(),
      33,
    ))
    .await;
  assert_eq!(cannot_connect(&mut session).await.token, 33);
}

#[tokio::test]
async fn a_peer_asking_again_after_a_failure_is_dialed_again() {
  let mut session = Session::start().await;
  session
    .server
    .send(&request(
      "frank",
      PeerConnectionType::Peer,
      u32::from(free_port()),
      34,
    ))
    .await;
  assert_eq!(cannot_connect(&mut session).await.token, 34);
  // an older client retries its request, this time from where it listens
  let frank = FakePeer::start().await;
  session
    .server
    .send(&request(
      "frank",
      PeerConnectionType::Peer,
      frank.port(),
      35,
    ))
    .await;
  let mut connection = frank.accept().await;
  assert_eq!(
    connection.read_init().await,
    InitMessage::PierceFirewall(PierceFirewall { token: 35 })
  );
  connection.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    connection.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  assert_eq!(session.client.message_connections()[0].0, "frank");
}

#[tokio::test]
async fn a_request_from_a_peer_we_are_connected_to_is_ignored() {
  let mut session = Session::with_options(listening_options()).await;
  let mut grace = session.peer("grace").await;
  grace.send(&PeerMessage::InfoRequest.encode()).await;
  grace.read_peer().await;
  let elsewhere = FakePeer::start().await;
  session
    .server
    .send(&request(
      "grace",
      PeerConnectionType::Peer,
      elsewhere.port(),
      36,
    ))
    .await;
  // neither dialed nor refused, and grace's own connection, from the address the server relays
  // her request with, is hers
  assert!(
    tokio::time::timeout(Duration::from_millis(300), elsewhere.accept())
      .await
      .is_err()
  );
  session.settle().await;
  assert_eq!(
    session.client.message_connection_address("grace"),
    Some(grace.stream.local_addr().unwrap())
  );
}

#[tokio::test]
async fn a_request_from_a_user_a_stranger_named_first_is_answered() {
  let mut session = Session::with_options(listening_options()).await;
  let mut stranger =
    PeerSide::connect_from(Ipv4Addr::new(127, 0, 0, 2), session.listen_address()).await;
  let introduce = InitMessage::PeerInit(PeerInit {
    username: "grace".into(),
    connection_type: PeerConnectionType::Peer,
    token: 0,
  });
  stranger.send(&introduce.encode()).await;
  stranger.send(&PeerMessage::InfoRequest.encode()).await;
  stranger.read_peer().await;
  // grace herself asks the server to have us connect
  let grace = FakePeer::start().await;
  session
    .server
    .send(&request(
      "grace",
      PeerConnectionType::Peer,
      grace.port(),
      37,
    ))
    .await;
  let mut dialed = grace.accept().await;
  assert_eq!(
    dialed.read_init().await,
    InitMessage::PierceFirewall(PierceFirewall { token: 37 })
  );
  dialed.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    dialed.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  assert_eq!(
    session.client.message_connection_address("grace"),
    Some(grace.address())
  );
}

#[tokio::test]
async fn a_user_introducing_itself_from_where_its_request_came_while_we_dial_is_taken() {
  // a proxy that takes every connection we dial and never answers it, so ours stays dialing
  let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let proxy_address = proxy.local_addr().unwrap();
  let (dialing, mut dialed) = tokio::sync::mpsc::channel(1);
  let stall = tokio::spawn(async move {
    let (socket, _) = proxy.accept().await.unwrap();
    dialing.send(()).await.unwrap();
    std::future::pending::<()>().await;
    drop(socket);
  });
  let mut options = listening_options();
  options.peer_connection_options.proxy = Some(soulseek_client::ProxyOptions {
    address: proxy_address.ip().to_string(),
    port: proxy_address.port(),
    credentials: None,
  });
  let mut session = Session::with_options(options).await;
  session
    .server
    .send(&request("heidi", PeerConnectionType::Peer, 2234, 38))
    .await;
  dialed.recv().await.unwrap();
  // heidi reaches us first, from the address the server relayed her request with
  let mut heidi = session.peer("heidi").await;
  heidi.send(&PeerMessage::InfoRequest.encode()).await;
  heidi.read_peer().await;
  assert_eq!(
    session.client.message_connection_address("heidi"),
    Some(heidi.stream.local_addr().unwrap())
  );
  stall.abort();
}

#[tokio::test]
async fn our_request_ends_at_once_when_the_peer_cannot_connect() {
  let mut options = options();
  options.peer_connection_options.connect_timeout = Duration::from_secs(5);
  let mut session = Session::with_options(options).await;
  let client = session.client.clone();
  let unreachable = std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, free_port()));
  let started = tokio::time::Instant::now();
  let connecting = tokio::spawn(async move {
    client
      .get_or_add_message_connection("dave", Some(unreachable), &CancellationToken::new())
      .await
  });
  let token = session
    .server
    .request_matching(|request| match request {
      ServerRequest::ConnectToPeer(ConnectToPeerRequest {
        token, username, ..
      }) if username == "dave" => Some(token),
      _ => None,
    })
    .await;
  session
    .server
    .send(&ServerResponse::CannotConnect(CannotConnectResponse {
      token,
      username: None,
    }))
    .await;
  let error = connecting.await.unwrap().unwrap_err();
  assert!(matches!(error, ClientError::Connection { .. }), "{error:?}");
  assert!(
    error
      .to_string()
      .starts_with("Failed to establish a direct or indirect message connection to dave"),
    "{error}"
  );
  assert!(started.elapsed() < Duration::from_secs(4));
}
