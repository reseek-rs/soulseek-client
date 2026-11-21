//! Changing options while logged in to soulfind: the listener moves to a new port and the server
//! hands the new one out, a port the system picked is the one handed out, a taken port keeps the
//! old listener and options, and while disconnected nothing needs a new session.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use crate::support::soulfind::{self, Soulfind, User};
use crate::support::{free_port, options, user_key};
use soulseek_client::Config;
use soulseek_client::ErrorKind;
use soulseek_client::config::ConfigBuilder;
use soulseek_proto::server::{GetPeerAddressRequest, GetPeerAddressResponse};
use soulseek_proto::{ServerCode, ServerRequest};
use tokio::net::TcpStream;

fn local(port: u16) -> SocketAddr {
  SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

async fn listening_on(server: &Arc<Soulfind>, port: u16) -> User {
  server
    .login(
      "moving",
      Config::builder()
        .listen_address(Ipv4Addr::LOCALHOST.into())
        .listen_port(port),
    )
    .await
}

fn listen_port(port: u16) -> impl FnOnce(ConfigBuilder) -> ConfigBuilder {
  move |config: ConfigBuilder| config.listen_port(port)
}

/// the port the server gives out for `user`, as another user asks for it
async fn advertised_port(server: &Arc<Soulfind>, user: &User) -> u32 {
  let asker = server.login("asker", options()).await;
  let address: GetPeerAddressResponse = asker
    .ask(
      user_key(ServerCode::GetPeerAddress, &user.username),
      ServerRequest::GetPeerAddress(GetPeerAddressRequest {
        username: user.username.clone(),
      }),
    )
    .await;
  address.port
}

#[tokio::test]
async fn the_listener_moves_to_a_new_port_and_the_server_hands_it_out() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let old = free_port();
  let user = listening_on(&server, old).await;
  let new = free_port();
  let applied = user.client.reconfigure(listen_port(new)).await.unwrap();
  assert!(applied.is_complete(), "{applied:?}");
  assert!(TcpStream::connect(local(new)).await.is_ok());
  assert!(TcpStream::connect(local(old)).await.is_err());
  assert_eq!(user.client.listen_address().unwrap().port(), new);
  assert_eq!(advertised_port(&server, &user).await, u32::from(new));
}

#[tokio::test]
async fn a_listen_port_of_zero_is_picked_by_the_system_and_handed_out() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let user = listening_on(&server, 0).await;
  let bound = user.client.listen_address().unwrap().port();
  assert_ne!(bound, 0);
  assert!(TcpStream::connect(local(bound)).await.is_ok());
  assert_eq!(advertised_port(&server, &user).await, u32::from(bound));
  // a listener kept between sessions keeps its port, and the next session hands it out again
  user
    .client
    .reconfigure(|config: ConfigBuilder| config.listen_while_disconnected(true))
    .await
    .unwrap();
  user.client.disconnect().await;
  server
    .connect(&user.client, &user.username, soulfind::PASSWORD)
    .await
    .unwrap();
  assert_eq!(user.client.listen_address().unwrap().port(), bound);
  assert_eq!(advertised_port(&server, &user).await, u32::from(bound));
  // bound again for a new backlog, the system picks again and the server is told
  let applied = user
    .client
    .reconfigure(|config: ConfigBuilder| config.listen_backlog(7))
    .await
    .unwrap();
  assert!(applied.is_complete(), "{applied:?}");
  let moved = user.client.listen_address().unwrap().port();
  assert_ne!(moved, 0);
  assert_eq!(advertised_port(&server, &user).await, u32::from(moved));
}

#[tokio::test]
async fn an_advertised_port_is_the_one_the_server_hands_out() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let bound = free_port();
  let user = listening_on(&server, bound).await;
  let advertise = |port| move |config: ConfigBuilder| config.advertised_port(port);
  user.client.reconfigure(advertise(Some(0))).await.unwrap();
  assert_eq!(advertised_port(&server, &user).await, 0);
  // the listener stays where it was bound
  assert!(TcpStream::connect(local(bound)).await.is_ok());
  user.client.reconfigure(advertise(None)).await.unwrap();
  assert_eq!(advertised_port(&server, &user).await, u32::from(bound));
}

#[tokio::test]
async fn a_listener_kept_between_sessions_serves_the_next_one() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let port = free_port();
  let user = server
    .login(
      "staying",
      Config::builder()
        .listen_address(Ipv4Addr::LOCALHOST.into())
        .listen_port(port)
        .listen_while_disconnected(true),
    )
    .await;
  user.client.disconnect().await;
  assert!(TcpStream::connect(local(port)).await.is_ok());
  // the next session takes it as it is, where binding the port again would fail
  server
    .connect(&user.client, &user.username, soulfind::PASSWORD)
    .await
    .unwrap();
  assert!(TcpStream::connect(local(port)).await.is_ok());
  // a change of where to listen, made between sessions, stops the one kept
  user.client.disconnect().await;
  assert!(TcpStream::connect(local(port)).await.is_ok());
  let moved = free_port();
  user.client.reconfigure(listen_port(moved)).await.unwrap();
  assert!(TcpStream::connect(local(port)).await.is_err());
  // the next session listens where the options say, and turned off, the listener goes with it
  server
    .connect(&user.client, &user.username, soulfind::PASSWORD)
    .await
    .unwrap();
  assert!(TcpStream::connect(local(moved)).await.is_ok());
  user
    .client
    .reconfigure(move |config: ConfigBuilder| config.listen_while_disconnected(false))
    .await
    .unwrap();
  user.client.disconnect().await;
  assert!(TcpStream::connect(local(moved)).await.is_err());
}

#[tokio::test]
async fn a_taken_port_keeps_the_old_listener_and_options() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let old = free_port();
  let user = listening_on(&server, old).await;
  let taken = std::net::TcpListener::bind(local(0)).unwrap();
  let new = taken.local_addr().unwrap().port();
  let applied = user.client.reconfigure(listen_port(new)).await.unwrap();
  let error = applied.listener_error.expect("the listener stays");
  assert!(error.kind() == ErrorKind::Listen, "{error:?}");
  assert_eq!(user.client.listen_address().unwrap().port(), old);
  assert!(TcpStream::connect(local(old)).await.is_ok());
  assert_eq!(advertised_port(&server, &user).await, u32::from(old));
}

#[tokio::test]
async fn rebinding_the_same_port_for_a_new_backlog() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let port = free_port();
  let user = listening_on(&server, port).await;
  let applied = user
    .client
    .reconfigure(move |config: ConfigBuilder| config.listen_backlog(256))
    .await
    .unwrap();
  assert!(applied.is_complete(), "{applied:?}");
  assert!(TcpStream::connect(local(port)).await.is_ok());
}

#[tokio::test]
async fn nothing_needs_a_reconnect_while_disconnected() {
  let client = soulseek_client::Client::new(options().build().unwrap());
  let off = move |config: ConfigBuilder| config.distributed_network(false);
  let applied = client.reconfigure(off).await.unwrap();
  assert!(applied.is_complete(), "{applied:?}");
}
