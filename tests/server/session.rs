//! Connecting, logging in and disconnecting against soulfind: the state transitions and their
//! events, a refused login, being kicked by a second login with the client's name, and a server
//! connection that stays up however long it is idle.

use std::time::Duration;

use crate::support::soulfind::{self, PASSWORD};
use crate::support::{disconnected, drain, free_port, next_event, options, transitions, user_key};
use soulseek_client::testing::Engine;
use soulseek_client::{Client, Error, Event};
use soulseek_proto::server::{GetPeerAddressRequest, GetPeerAddressResponse, GetStatusRequest};
use soulseek_proto::{ServerCode, ServerRequest};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn login_raises_its_transitions_before_anything_the_server_sends_after_it() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let username = soulfind::username("login");
  let client = Client::new(server.aimed(options())).unwrap();
  let mut events = client.events();
  server.connect(&client, &username, PASSWORD).await.unwrap();

  assert_eq!(client.state(), soulseek_client::session::State::LoggedIn);
  assert_eq!(client.username(), Some(username));
  assert_eq!(
    client.server_address(),
    Some(server.address().ip().to_string())
  );
  assert_eq!(client.server_endpoint(), Some(server.address()));
  let info = client.server_info();
  assert_eq!(info.is_supporter, Some(false));
  assert!(info.own_public_ip.is_some());

  // soulfind sends the room list right after the login reply
  let mut seen = Vec::new();
  next_event(&mut events, |event| {
    let done = matches!(event, Event::Room(soulseek_client::room::Event::List(_)));
    seen.push(event);
    done.then_some(())
  })
  .await;
  let order: Vec<&str> = seen
    .iter()
    .map(|event| match event {
      Event::Session(soulseek_client::session::Event::StateChanged { .. }) => "StateChanged",
      Event::Session(soulseek_client::session::Event::Connected { .. }) => "Connected",
      Event::Session(soulseek_client::session::Event::LoggedIn { .. }) => "LoggedIn",
      Event::Session(soulseek_client::session::Event::ServerInfo(_)) => "ServerInfoReceived",
      Event::Room(soulseek_client::room::Event::List(_)) => "RoomListReceived",
      other => panic!("unexpected {other:?}"),
    })
    .collect();
  assert_eq!(
    order,
    [
      "StateChanged",
      "StateChanged",
      "Connected",
      "StateChanged",
      "ServerInfoReceived",
      "StateChanged",
      "LoggedIn",
      "RoomListReceived"
    ]
  );
  assert_eq!(
    transitions(&seen),
    [
      (
        soulseek_client::session::State::Disconnected,
        soulseek_client::session::State::Connecting,
        "Connecting".into()
      ),
      (
        soulseek_client::session::State::Connecting,
        soulseek_client::session::State::Connected,
        format!("connected to {}", server.address())
      ),
      (
        soulseek_client::session::State::Connected,
        soulseek_client::session::State::LoggingIn,
        "logging in".into()
      ),
      (
        soulseek_client::session::State::LoggingIn,
        soulseek_client::session::State::LoggedIn,
        "logged in".into()
      ),
    ]
  );
}

#[tokio::test]
async fn a_client_that_does_not_listen_advertises_only_the_port_it_is_told_to() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let port = free_port();
  let alice = server
    .login("port", options().advertised_port(Some(port)))
    .await;
  let bob = server.login("asker", options()).await;
  let address: GetPeerAddressResponse = bob
    .ask(
      user_key(ServerCode::GetPeerAddress, &alice.username),
      ServerRequest::GetPeerAddress(GetPeerAddressRequest {
        username: alice.username.clone(),
      }),
    )
    .await;
  assert_eq!(address.port, u32::from(port));
  assert_eq!(Some(address.ip), alice.client.server_info().own_public_ip);
  // bob was told no port, so he advertises that he takes no direct connections
  let address: GetPeerAddressResponse = alice
    .ask(
      user_key(ServerCode::GetPeerAddress, &bob.username),
      ServerRequest::GetPeerAddress(GetPeerAddressRequest {
        username: bob.username.clone(),
      }),
    )
    .await;
  assert_eq!(address.port, 0);
}

#[tokio::test]
async fn connecting_twice_is_refused() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let alice = server.login("twice", options()).await;
  let error = server
    .connect(&alice.client, &alice.username, PASSWORD)
    .await
    .unwrap_err();
  assert!(
    matches!(&error, Error::InvalidOperation(message) if message == "the client is already connected")
  );
  assert_eq!(
    alice.client.state(),
    soulseek_client::session::State::LoggedIn
  );
}

#[tokio::test]
async fn a_wrong_password_is_rejected_with_the_server_reason() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let registered = server.login("password", options()).await;
  registered.client.disconnect().await;

  let client = Client::new(server.aimed(options())).unwrap();
  let mut events = client.events();
  let error = server
    .connect(&client, &registered.username, "not the password")
    .await
    .unwrap_err();
  assert!(
    matches!(&error, Error::LoginRejected(message) if message == "the server rejected login attempt: INVALIDPASS"),
    "{error:?}"
  );
  let (message, cause) = disconnected(&drain(&mut events));
  assert_eq!(message, "the server rejected login attempt: INVALIDPASS");
  assert!(matches!(cause, Some(Error::LoginRejected(_))));
  assert_eq!(
    client.state(),
    soulseek_client::session::State::Disconnected
  );
  assert_eq!(client.username(), None);
}

#[tokio::test]
async fn a_second_login_with_our_name_kicks_us() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let mut first = server.login("kicked", options()).await;
  let second = Client::new(server.aimed(options())).unwrap();
  server
    .connect(&second, &first.username, PASSWORD)
    .await
    .unwrap();
  first
    .event(|event| {
      matches!(
        event,
        Event::Session(soulseek_client::session::Event::Kicked)
      )
      .then_some(())
    })
    .await;
  let (message, cause) = first
    .event(|event| match event {
      Event::Session(soulseek_client::session::Event::Disconnected { message, error, .. }) => {
        Some((message, error))
      }
      _ => None,
    })
    .await;
  assert_eq!(message, "kicked from server");
  assert!(matches!(cause, Some(Error::Kicked)));
  assert_eq!(
    first.client.state(),
    soulseek_client::session::State::Disconnected
  );
  assert_eq!(second.state(), soulseek_client::session::State::LoggedIn);
}

#[tokio::test]
async fn disconnecting_fails_the_server_waits() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let mut alice = server.login("waits", options()).await;
  let server_wait = alice.client.waiter().wait::<()>(
    user_key(ServerCode::GetStatus, "nobody asked"),
    None,
    CancellationToken::new(),
  );

  alice.client.disconnect_because("done").await;

  let error = server_wait.await.unwrap_err();
  assert_eq!(
    error.to_string(),
    "server connection disconnected: done",
    "{error:?}"
  );
  assert_eq!(
    error.connection_kind(),
    Some(soulseek_client::error::ConnectionKind::Other)
  );
  assert_eq!(disconnected(&drain(&mut alice.events)).0, "done");
}

#[tokio::test]
async fn disconnecting_twice_raises_disconnected_once() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let mut alice = server.login("twice", options()).await;
  alice.client.disconnect().await;
  alice.client.disconnect().await;
  let events = drain(&mut alice.events);
  assert_eq!(disconnected(&events).0, "client disconnected");
  assert_eq!(alice.client.username(), None);
  assert_eq!(alice.client.server_address(), None);
  assert_eq!(alice.client.server_endpoint(), None);
}

#[tokio::test]
async fn a_refused_connection_is_a_connect_error() {
  let port = free_port();
  let client = Client::new(options().server("127.0.0.1", port)).unwrap();
  let mut events = client.events();
  let error = client.connect("nobody", PASSWORD).await.unwrap_err();
  let Error::Connect { message, source } = &error else {
    panic!("expected a connect error, got {error:?}");
  };
  assert!(
    message.starts_with(&format!(
      "failed to connect: failed to connect to 127.0.0.1:{port}: "
    )),
    "{message}"
  );
  let cause = source
    .as_deref()
    .and_then(|source| source.downcast_ref::<Error>());
  assert_eq!(
    cause.and_then(Error::connection_kind),
    Some(soulseek_client::error::ConnectionKind::Other),
    "{error:?}"
  );
  let events = drain(&mut events);
  assert_eq!(
    transitions(&events)
      .iter()
      .map(|(_, state, _)| *state)
      .collect::<Vec<_>>(),
    [
      soulseek_client::session::State::Connecting,
      soulseek_client::session::State::Disconnecting,
      soulseek_client::session::State::Disconnected
    ]
  );
  assert!(matches!(
    disconnected(&events).1,
    Some(Error::Connect { .. })
  ));
}

/// The clock is paused only while nothing waits on the socket: a paused clock jumps to the next
/// timer whenever the runtime is idle, which can be before loopback data is seen.
#[tokio::test]
async fn the_server_connection_never_times_out() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let alice = server.login("idle", options()).await;
  tokio::time::pause();
  tokio::time::sleep(Duration::from_secs(3600)).await;
  tokio::time::resume();
  assert_eq!(
    alice.client.state(),
    soulseek_client::session::State::LoggedIn
  );
  let status: soulseek_proto::server::UserStatus = alice
    .ask(
      user_key(ServerCode::GetStatus, &alice.username),
      ServerRequest::GetStatus(GetStatusRequest {
        username: alice.username.clone(),
      }),
    )
    .await;
  assert_eq!(status.username, alice.username);
}
