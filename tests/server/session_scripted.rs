//! The session against a scripted server, for what soulfind cannot show: the bytes the client
//! writes at login and after it, a login refused with a detail, a server that never answers the
//! login or closes the connection, canceling a login in progress, the write-liveness ping, and
//! the configuration re-sent when the options change.

use std::time::Duration;

use crate::support::{
  FakeServer, Logs, PASSWORD, Session, USERNAME, disconnected, drain, listening_options,
  login_success, next_event, options, poll_once, transitions,
};
use soulseek_client::{
  Client, ClientError, ClientOptions, ClientOptionsPatch, ClientState, ConnectionErrorKind, Event,
};
use soulseek_proto::server::{
  AcceptChildren, BranchLevel, BranchRoot, GetUserStatsRequest, HaveNoParents, LoginResponse,
  SetListenPort,
};
use soulseek_proto::{
  PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION, ServerRequest, ServerResponse,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn connect(
  client: &Client,
  server: &FakeServer,
  cancel: CancellationToken,
) -> JoinHandle<Result<(), ClientError>> {
  let client = client.clone();
  let address = server.address();
  tokio::spawn(async move {
    client
      .connect(
        &address.ip().to_string(),
        address.port(),
        USERNAME,
        PASSWORD,
        &cancel,
      )
      .await
  })
}

#[tokio::test]
async fn a_server_name_is_left_to_the_proxy_to_resolve() {
  use tokio::io::{AsyncReadExt, AsyncWriteExt};

  let fake = FakeServer::start().await;
  let target = fake.address();
  // a SOCKS5 proxy that takes any host name for the scripted server and says which it got
  let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let proxy_address = proxy.local_addr().unwrap();
  let relaying = tokio::spawn(async move {
    let (mut inbound, _) = proxy.accept().await.unwrap();
    let mut greeting = [0; 3];
    inbound.read_exact(&mut greeting).await.unwrap();
    inbound.write_all(&[5, 0]).await.unwrap();
    let mut head = [0; 5];
    inbound.read_exact(&mut head).await.unwrap();
    assert_eq!(head[3], 3, "a domain address");
    let mut name = vec![0; usize::from(head[4]) + 2];
    inbound.read_exact(&mut name).await.unwrap();
    name.truncate(name.len() - 2);
    inbound
      .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
      .await
      .unwrap();
    let mut outbound = tokio::net::TcpStream::connect(target).await.unwrap();
    tokio::spawn(async move {
      // the test ends with the sockets still open
      let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
    });
    String::from_utf8(name).unwrap()
  });
  let mut options = options();
  options.server_connection_options.proxy = Some(soulseek_client::ProxyOptions {
    address: proxy_address.ip().to_string(),
    port: proxy_address.port(),
    credentials: None,
  });
  let client = Client::new(options).unwrap();
  // a name no resolver knows: looked up here, the connect would fail
  let connecting = tokio::spawn({
    let client = client.clone();
    async move {
      client
        .connect(
          "server.invalid",
          2271,
          USERNAME,
          PASSWORD,
          &CancellationToken::new(),
        )
        .await
    }
  });
  let mut server = fake.accept().await;
  server.read_login().await;
  server.send(&crate::support::login_success()).await;
  connecting.await.unwrap().unwrap();
  assert_eq!(relaying.await.unwrap(), "server.invalid");
  assert_eq!(client.server_address().as_deref(), Some("server.invalid"));
  assert_eq!(client.server_endpoint(), None);
}

#[tokio::test]
async fn the_login_and_the_configuration_sent_after_it() {
  let fake = FakeServer::start().await;
  let client = Client::new(options()).unwrap();
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;

  let (login, port) = server.read_login().await;
  assert_eq!(login.username, USERNAME);
  assert_eq!(login.password.expose(), PASSWORD);
  assert_eq!(login.major_version, PROTOCOL_MAJOR_VERSION);
  assert_eq!(login.minor_version, PROTOCOL_MINOR_VERSION);
  assert_eq!(login.hash.expose(), "bb6de2fa07892f34b0e1f62df88f3384");
  // these options do not listen, so no port is advertised
  assert_eq!(port, 0);
  assert_eq!(client.state(), ClientState::LoggingIn);
  server.send(&crate::support::login_success()).await;

  assert_eq!(
    server.read_configuration().await,
    [
      ServerRequest::BranchLevel(BranchLevel { level: 0 }),
      ServerRequest::BranchRoot(BranchRoot {
        root: USERNAME.into()
      }),
      ServerRequest::AcceptChildren(AcceptChildren { accept: false }),
      ServerRequest::HaveNoParents(HaveNoParents {
        have_no_parents: true
      }),
      ServerRequest::GetUserStats(GetUserStatsRequest {
        username: USERNAME.into()
      }),
    ]
  );
  connecting.await.unwrap().unwrap();
  assert_eq!(
    client.server_info().own_public_ip,
    Some(crate::support::PUBLIC_IP)
  );
}

/// The client's upload speed bounds the children it takes, so a client that takes none does not
/// ask for it
#[tokio::test]
async fn a_client_that_takes_no_children_does_not_ask_for_its_speed() {
  not_asked(ClientOptions {
    accept_distributed_children: false,
    ..options()
  })
  .await;
}

#[tokio::test]
async fn a_client_that_sets_the_servers_child_advice_aside_does_not_ask_for_its_speed() {
  not_asked(ClientOptions {
    follow_server_child_limits: false,
    ..options()
  })
  .await;
}

#[tokio::test]
async fn following_the_servers_child_advice_again_asks_for_our_speed_at_once() {
  let fake = FakeServer::start().await;
  let client = Client::new(ClientOptions {
    follow_server_child_limits: false,
    ..options()
  })
  .unwrap();
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.accept_login().await;
  for _ in 0..4 {
    server.request().await;
  }
  connecting.await.unwrap().unwrap();
  client
    .reconfigure_options(ClientOptionsPatch {
      follow_server_child_limits: Some(true),
      ..ClientOptionsPatch::default()
    })
    .await
    .unwrap();
  let asked = server
    .request_matching(|request| match request {
      ServerRequest::GetUserStats(stats) => Some(stats.username),
      _ => None,
    })
    .await;
  assert_eq!(asked, USERNAME);
}

#[tokio::test]
async fn a_client_shut_down_refuses_what_it_is_asked() {
  let session = Session::start().await;
  session.client.shutdown().await;
  let error = session.client.get_user_endpoint("bob").await.unwrap_err();
  assert!(matches!(error, ClientError::Shutdown), "{error:?}");
  let error = session
    .client
    .connect(
      "127.0.0.1",
      1,
      USERNAME,
      PASSWORD,
      &CancellationToken::new(),
    )
    .await
    .unwrap_err();
  assert!(matches!(error, ClientError::Shutdown), "{error:?}");
}

/// a client with `options` logs in and sends its distributed status, and nothing after it
async fn not_asked(options: ClientOptions) {
  let fake = FakeServer::start().await;
  let client = Client::new(options).unwrap();
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.accept_login().await;
  for _ in 0..4 {
    server.request().await;
  }
  connecting.await.unwrap().unwrap();
  client.set_shared_counts(1, 2).await.unwrap();
  assert!(
    matches!(
      server.request().await,
      ServerRequest::SharedFoldersAndFiles(_)
    ),
    "the next request is ours"
  );
}

async fn rejected_login(response: LoginResponse) -> ClientError {
  let fake = FakeServer::start().await;
  let client = Client::new(options()).unwrap();
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  server.send(&ServerResponse::Login(response)).await;
  connecting.await.unwrap().unwrap_err()
}

#[tokio::test]
async fn a_rejected_login_appends_the_detail() {
  let error = rejected_login(LoginResponse::Failure {
    reason: "INVALIDUSERNAME".into(),
    detail: Some("Nick too long.".into()),
  })
  .await;
  assert!(
    matches!(&error, ClientError::LoginRejected(message) if message == "The server rejected login attempt: INVALIDUSERNAME: Nick too long.")
  );
}

#[tokio::test]
async fn no_login_reply_is_a_login_error_of_its_own() {
  let fake = FakeServer::start().await;
  let client = Client::new(ClientOptions {
    message_timeout: Duration::from_millis(300),
    ..options()
  })
  .unwrap();
  let mut events = client.subscribe();
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  let error = connecting.await.unwrap().unwrap_err();
  assert!(
    matches!(&error, ClientError::LoginNoResponse(message) if message == "the server did not answer the login in time"),
    "{error:?}"
  );
  let (_, cause) = disconnected(&drain(&mut events));
  assert!(matches!(cause, Some(ClientError::LoginNoResponse(_))));
  server.closed().await;
}

#[tokio::test]
async fn the_server_closing_during_login_is_a_connect_error() {
  let fake = FakeServer::start().await;
  let client = Client::new(options()).unwrap();
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  drop(server);
  let error = connecting.await.unwrap().unwrap_err();
  assert!(matches!(error, ClientError::Connect { .. }), "{error:?}");
}

#[tokio::test]
async fn canceling_the_connect_while_logging_in() {
  let fake = FakeServer::start().await;
  let client = Client::new(options()).unwrap();
  let mut events = client.subscribe();
  let cancel = CancellationToken::new();
  let connecting = connect(&client, &fake, cancel.clone());
  let mut server = fake.accept().await;
  server.read_login().await;
  cancel.cancel();
  let error = connecting.await.unwrap().unwrap_err();
  assert!(matches!(error, ClientError::Canceled), "{error:?}");
  assert!(matches!(
    disconnected(&drain(&mut events)).1,
    Some(ClientError::Canceled)
  ));
  assert_eq!(client.state(), ClientState::Disconnected);
  server.closed().await;
}

#[tokio::test]
async fn a_connect_with_its_token_canceled_already_begins_nothing() {
  let fake = FakeServer::start().await;
  let client = Client::new(listening_options()).unwrap();
  let mut events = client.subscribe();
  let address = fake.address();
  let cancel = CancellationToken::new();
  cancel.cancel();
  let connected = client
    .connect(
      &address.ip().to_string(),
      address.port(),
      USERNAME,
      PASSWORD,
      &cancel,
    )
    .await;
  assert!(
    matches!(connected, Err(ClientError::Canceled)),
    "{connected:?}"
  );
  let raised = drain(&mut events);
  assert!(raised.is_empty(), "{raised:?}");
  let reached = tokio::time::timeout(Duration::from_millis(200), fake.accept()).await;
  assert!(reached.is_err(), "the server took a connection");
}

#[tokio::test]
async fn disconnecting_cancels_a_connect_in_progress() {
  let fake = FakeServer::start().await;
  let client = Client::new(options()).unwrap();
  let mut events = client.subscribe();
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  client.disconnect(Some("user asked"), None).await;
  let error = connecting.await.unwrap().unwrap_err();
  assert!(matches!(error, ClientError::Canceled), "{error:?}");
  let (message, cause) = disconnected(&drain(&mut events));
  assert_eq!(message, "user asked");
  assert!(cause.is_none());
  assert_eq!(client.state(), ClientState::Disconnected);
  server.closed().await;
}

#[tokio::test]
async fn a_connect_queued_behind_another_login_is_refused_whatever_it_asks() {
  let fake = FakeServer::start().await;
  let client = Client::new(listening_options()).unwrap();
  let first = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  server.send(&login_success()).await;
  first.await.unwrap().unwrap();

  // both connects pass their first check while the disconnect stops the listener, so the second
  // waits for the first to log in
  let address = fake.address();
  let host = address.ip().to_string();
  let cancel = CancellationToken::new();
  let serve = async {
    let mut server = fake.accept().await;
    server.read_login().await;
    server.send(&login_success()).await;
    server
  };
  let ((), first, second, _server) = tokio::join!(
    client.disconnect(None, None),
    client.connect(&host, address.port(), USERNAME, PASSWORD, &cancel),
    client.connect(&host, address.port(), USERNAME, PASSWORD, &cancel),
    serve,
  );
  first.unwrap();
  // the same server, name and password make it no less a second connect
  assert!(
    matches!(&second, Err(ClientError::InvalidOperation(message)) if message == "The client is already connected"),
    "{second:?}"
  );
  assert_eq!(client.username().as_deref(), Some(USERNAME));
  drop(server);
}

#[tokio::test]
async fn a_disconnect_while_a_connect_waits_for_the_last_session_to_end_cancels_that_connect() {
  let session = Session::with_options(listening_options()).await;
  let client = &session.client;
  let mut events = client.subscribe();
  let address = session.fake.address();
  let host = address.ip().to_string();
  let cancel = CancellationToken::new();
  // the connect starts while the first disconnect stops the listener and waits for it to end;
  // the second disconnect is asked for after the connect
  let connecting = tokio::time::timeout(
    Duration::from_secs(2),
    client.connect(&host, address.port(), USERNAME, PASSWORD, &cancel),
  );
  let ((), connected, ()) = tokio::join!(
    client.disconnect(None, None),
    connecting,
    client.disconnect(None, None),
  );
  let states: Vec<ClientState> = transitions(&drain(&mut events))
    .into_iter()
    .map(|(_, state, _)| state)
    .collect();
  assert!(
    matches!(connected, Ok(Err(ClientError::Canceled))),
    "{connected:?}, through {states:?}"
  );
  assert_eq!(client.state(), ClientState::Disconnected);
}

#[tokio::test]
async fn a_second_disconnect_returns_once_the_one_under_way_has_ended() {
  let session = Session::with_options(listening_options()).await;
  let client = &session.client;
  let second = async {
    client.disconnect(None, None).await;
    client.state()
  };
  // the first is polled first and waits for the listener to stop
  let ((), after_second) = tokio::join!(client.disconnect(None, None), second);
  assert_eq!(after_second, ClientState::Disconnected);
}

#[tokio::test]
async fn a_connect_queued_behind_a_login_that_fails_waits_and_connects_as_itself() {
  let fake = FakeServer::start().await;
  let client = Client::new(listening_options()).unwrap();
  let first = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  server.send(&login_success()).await;
  first.await.unwrap().unwrap();

  // both connects pass their first check while the disconnect stops the listener; the first's
  // login is refused, and its session has ended by the time the second takes its turn
  let address = fake.address();
  let host = address.ip().to_string();
  let cancel = CancellationToken::new();
  let serve = async {
    let mut refused = fake.accept().await;
    refused.read_login().await;
    refused
      .send(&ServerResponse::Login(LoginResponse::Failure {
        reason: "INVALIDPASS".into(),
        detail: None,
      }))
      .await;
    let mut queued = fake.accept().await;
    let (login, _) = queued.read_login().await;
    queued.send(&login_success()).await;
    (refused, queued, login.username)
  };
  let ((), refused, queued, (_refused, _queued, name)) = tokio::join!(
    client.disconnect(None, None),
    client.connect(&host, address.port(), USERNAME, PASSWORD, &cancel),
    client.connect(&host, address.port(), "someone_else", PASSWORD, &cancel),
    serve,
  );
  assert!(
    matches!(refused, Err(ClientError::LoginRejected(_))),
    "{refused:?}"
  );
  queued.unwrap();
  assert_eq!(name, "someone_else");
  assert_eq!(client.username().as_deref(), Some("someone_else"));
  drop(server);
}

#[tokio::test]
async fn a_disconnect_cancels_a_connect_still_waiting_for_the_lock() {
  let fake = FakeServer::start().await;
  let client = Client::new(listening_options()).unwrap();
  let first = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  server.send(&login_success()).await;
  first.await.unwrap().unwrap();

  // both connects pass their first check while the disconnect stops the listener; the first
  // logs in and is disconnected while the second still waits for the lock it holds
  let address = fake.address();
  let host = address.ip().to_string();
  let cancel = CancellationToken::new();
  let serve = async {
    let mut joined = fake.accept().await;
    joined.read_login().await;
    joined.send(&login_success()).await;
    joined
  };
  let joined_then_ended = async {
    let joined = client
      .connect(&host, address.port(), USERNAME, PASSWORD, &cancel)
      .await;
    client.disconnect(None, None).await;
    joined
  };
  let ((), joined, queued, _joined) = tokio::join!(
    client.disconnect(None, None),
    joined_then_ended,
    client.connect(&host, address.port(), "someone_else", PASSWORD, &cancel),
    serve,
  );
  joined.unwrap();
  assert!(matches!(queued, Err(ClientError::Canceled)), "{queued:?}");
  assert_eq!(client.state(), ClientState::Disconnected);
  drop(server);
}

#[tokio::test]
async fn a_connect_dropped_while_another_waits_leaves_a_session_the_other_can_take() {
  let fake = FakeServer::start().await;
  let client = Client::new(listening_options()).unwrap();
  let first = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.read_login().await;
  server.send(&login_success()).await;
  first.await.unwrap().unwrap();

  let address = fake.address();
  let host = address.ip().to_string();
  let cancel = CancellationToken::new();
  let serve = async {
    let mut hung = fake.accept().await;
    hung.read_login().await;
    // never answers; the queued connect's session is answered
    let mut second = fake.accept().await;
    let (login, _) = second.read_login().await;
    second.send(&login_success()).await;
    (hung, second, login.username)
  };
  let dropped = tokio::time::timeout(
    Duration::from_millis(300),
    client.connect(&host, address.port(), "dropped", PASSWORD, &cancel),
  );
  let ((), timed_out, queued, (_hung, _second, name)) = tokio::join!(
    client.disconnect(None, None),
    dropped,
    client.connect(&host, address.port(), "queued", PASSWORD, &cancel),
    serve,
  );
  assert!(timed_out.is_err());
  queued.unwrap();
  assert_eq!(name, "queued");
  drop(server);
}

#[tokio::test]
async fn a_disconnect_dropped_while_the_listener_stops_ends_disconnected_and_can_connect_again() {
  let session = Session::with_options(listening_options()).await;
  let client = session.client.clone();
  let mut disconnecting = Box::pin(client.disconnect(None, None));
  // polled once, it waits for the listener's task to end
  assert!(poll_once(&mut disconnecting).await.is_pending());
  drop(disconnecting);
  let ended = tokio::time::timeout(Duration::from_secs(2), async {
    while client.state() != ClientState::Disconnected {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await;
  assert!(ended.is_ok(), "the client stayed {:?}", client.state());

  let fake = FakeServer::start().await;
  let connecting = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.accept_login().await;
  server.read_configuration().await;
  connecting.await.unwrap().unwrap();
}

/// What a dropped connect does today once it has logged in: the session it made is ended as a
/// canceled attempt. Whether a logged in session should end with the call that made it is not
/// settled; this holds the behavior still until it is.
#[tokio::test]
async fn a_connect_dropped_while_sending_the_configuration_ends_the_logged_in_session_as_canceled()
{
  let fake = FakeServer::start().await;
  let client = Client::new(options()).unwrap();
  let mut events = client.subscribe();
  let address = fake.address();
  let host = address.ip().to_string();
  let cancel = CancellationToken::new();
  let mut connecting = Box::pin(client.connect(&host, address.port(), USERNAME, PASSWORD, &cancel));
  let mut server = tokio::select! {
    server = fake.accept() => server,
    connected = &mut connecting => panic!("connected without a server: {connected:?}"),
  };
  tokio::select! {
    _ = server.read_login() => {}
    connected = &mut connecting => panic!("connected without an answer: {connected:?}"),
  }
  server.send(&login_success()).await;
  // polled until it has logged in, which is when it writes the configuration
  while client.state() != ClientState::LoggedIn {
    assert!(poll_once(&mut connecting).await.is_pending());
    tokio::time::sleep(Duration::from_millis(1)).await;
  }
  drop(connecting);
  let (message, error) = next_event(&mut events, |event| match event {
    Event::Disconnected { message, error, .. } => Some((message, error)),
    _ => None,
  })
  .await;
  assert_eq!(message, "connect dropped");
  assert!(matches!(error, Some(ClientError::Canceled)), "{error:?}");
  server.closed().await;
}

#[test]
fn a_connect_dropped_outside_the_runtime_does_not_make_a_queued_connect_panic() {
  let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();
  let session = runtime.block_on(Session::with_options(listening_options()));
  let client = session.client.clone();
  let address = session.fake.address();
  let host = address.ip().to_string();
  let cancel = CancellationToken::new();
  let mut dropped = Box::pin(client.connect(&host, address.port(), "dropped", PASSWORD, &cancel));
  let (queued, _hung) = runtime.block_on(async {
    // both connects start while the disconnect stops the listener: the first takes the lock,
    // the second waits for it
    let mut disconnecting = Box::pin(client.disconnect(None, None));
    assert!(poll_once(&mut disconnecting).await.is_pending());
    assert!(poll_once(&mut dropped).await.is_pending());
    let mut queued = Box::pin({
      let (client, host, cancel) = (client.clone(), host.clone(), cancel.clone());
      async move {
        client
          .connect(&host, address.port(), "queued", PASSWORD, &cancel)
          .await
      }
    });
    assert!(poll_once(&mut queued).await.is_pending());
    let queued = tokio::spawn(queued);
    disconnecting.await;
    // the server takes the login and never answers it
    let hang = async {
      let mut hung = session.fake.accept().await;
      hung.read_login().await;
      hung
    };
    let (timed_out, hung) = tokio::join!(
      tokio::time::timeout(Duration::from_millis(300), &mut dropped),
      hang
    );
    assert!(timed_out.is_err());
    (queued, hung)
  });
  drop(dropped);
  let joined = runtime.block_on(async {
    let serve = async {
      let mut second = session.fake.accept().await;
      second.accept_login().await;
      second.discard().await;
    };
    tokio::select! {
      joined = queued => joined,
      () = serve => unreachable!("the server reads until the test ends"),
    }
  });
  assert!(joined.is_ok(), "the queued connect panicked: {joined:?}");
}

#[tokio::test]
async fn the_server_closing_the_connection_ends_the_session() {
  let mut session = Session::start().await;
  drain(&mut session.events);
  let address = session.fake.address();
  drop(session.server.stream);
  let (message, cause) = next_event(&mut session.events, |event| match event {
    Event::Disconnected { message, error, .. } => Some((message, error)),
    _ => None,
  })
  .await;
  assert_eq!(message, "Read error: Remote connection closed");
  let cause = cause.expect("the read error is the cause");
  assert_eq!(cause.connection_kind(), Some(ConnectionErrorKind::Read));
  assert!(
    cause
      .to_string()
      .starts_with(&format!("Failed to read 4 bytes from {address}")),
    "{cause}"
  );
  assert_eq!(session.client.state(), ClientState::Disconnected);
  assert_eq!(session.client.username(), None);
  assert_eq!(session.client.server_address(), None);
  assert_eq!(session.client.server_endpoint(), None);
}

/// A session that ends with an error is the application's to tell of, as it handles the
/// `Disconnected` event: a disconnect it asked for with its reason, and a session lost on its own
#[tokio::test]
async fn a_session_ended_by_an_error_is_told_at_debug() {
  let logs = Logs::capture();
  let asked = Session::start().await;
  let reason = ClientError::client("the application is shutting down", None);
  asked
    .client
    .disconnect(Some("Shutting down"), Some(reason))
    .await;
  let mut lost = Session::start().await;
  drain(&mut lost.events);
  drop(lost.server.stream);
  next_event(&mut lost.events, |event| {
    matches!(event, Event::Disconnected { .. }).then_some(())
  })
  .await;
  let ended = logs.lines_with(&["DEBUG", "disconnected from server"]);
  assert_eq!(ended.len(), 2, "{}", logs.text());
  assert!(ended[0].contains("Shutting down"), "{}", ended[0]);
  assert!(ended[1].contains("Read error"), "{}", ended[1]);
}

/// A connect that fails is the caller's to tell of, and to try again: the client tells of it at
/// debug, the connect, its listener and the disconnect it causes; a session that logs in tells
/// of its listener at info
#[tokio::test]
async fn a_failed_connect_is_told_at_debug_and_a_logged_in_listener_at_info() {
  let logs = Logs::capture();
  let fake = FakeServer::start().await;
  let client = Client::new(listening_options()).unwrap();
  let closed = std::net::TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap();
  for _ in 0..2 {
    client
      .connect(
        "127.0.0.1",
        closed.port(),
        USERNAME,
        PASSWORD,
        &CancellationToken::new(),
      )
      .await
      .unwrap_err();
  }
  // a server that closes during the login
  let cut = connect(&client, &fake, CancellationToken::new());
  drop(fake.accept().await);
  cut.await.unwrap().unwrap_err();
  let told = |level: &str| {
    [
      "connecting to server",
      "listening for peer connections",
      "disconnected from server",
    ]
    .iter()
    .map(|line| logs.lines_with(&[level, line]).len())
    .collect::<Vec<_>>()
  };
  assert_eq!(told("DEBUG"), [3, 6, 3], "{}", logs.text());
  assert_eq!(told("INFO"), [0, 0, 0], "{}", logs.text());
  assert_eq!(told("WARN"), [0, 0, 0], "{}", logs.text());

  let logging_in = connect(&client, &fake, CancellationToken::new());
  let mut server = fake.accept().await;
  server.accept_login().await;
  server.read_configuration().await;
  logging_in.await.unwrap().unwrap();
  client.disconnect(None, None).await;
  let serving = |level: &str| {
    [
      "listening for peer connections",
      "stopped listening for peer connections",
    ]
    .iter()
    .map(|line| logs.lines_with(&[level, line]).len())
    .collect::<Vec<_>>()
  };
  assert_eq!(serving("INFO"), [2, 1], "{}", logs.text());
}

/// The clock is paused only while nothing waits on the socket: a paused clock jumps to the next
/// timer whenever the runtime is idle, which can be before loopback data is seen.
#[tokio::test]
async fn the_server_is_pinged_each_minute() {
  let mut session = Session::start().await;
  tokio::time::pause();
  tokio::time::sleep(Duration::from_secs(59)).await;
  tokio::time::resume();
  let marker = ServerRequest::Ping;
  assert!(
    tokio::time::timeout(Duration::from_millis(100), session.server.frame())
      .await
      .is_err(),
    "nothing before the first minute"
  );
  tokio::time::pause();
  tokio::time::sleep(Duration::from_secs(2)).await;
  tokio::time::resume();
  let ping = session.server.frame().await;
  assert_eq!(ServerRequest::decode(&ping).unwrap(), marker);
}

#[tokio::test]
async fn a_busy_listen_port_fails_the_connect_before_the_server_is_contacted() {
  let fake = FakeServer::start().await;
  let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let client = Client::new(ClientOptions {
    enable_listener: true,
    listen_ip_address: [127, 0, 0, 1].into(),
    listen_port: taken.local_addr().unwrap().port(),
    ..ClientOptions::default()
  })
  .unwrap();
  let error = connect(&client, &fake, CancellationToken::new())
    .await
    .unwrap()
    .unwrap_err();
  assert!(matches!(error, ClientError::Listen { .. }), "{error:?}");
  assert_eq!(client.state(), ClientState::Disconnected);
  // nothing reached the server
  let accepted = tokio::time::timeout(Duration::from_millis(200), fake.accept()).await;
  assert!(accepted.is_err());
}

#[tokio::test]
async fn a_reconfigure_resends_the_configuration_and_the_status_only_when_it_changed() {
  let mut session = Session::start().await;
  let port = crate::support::free_port();
  session
    .client
    .reconfigure_options(ClientOptionsPatch {
      advertised_port: Some(Some(port)),
      ..ClientOptionsPatch::default()
    })
    .await
    .unwrap();
  let configuration = |port: u16| {
    ServerRequest::SetListenPort(SetListenPort {
      port: u32::from(port),
      obfuscation: None,
    })
  };
  // the distributed status has not changed, so it is not sent again
  assert_eq!(session.server.request().await, configuration(port));

  session
    .client
    .reconfigure_options(ClientOptionsPatch {
      enable_distributed_network: Some(false),
      ..ClientOptionsPatch::default()
    })
    .await
    .unwrap();
  let mut sent = Vec::new();
  for _ in 0..5 {
    sent.push(session.server.request().await);
  }
  assert_eq!(sent[0], configuration(port));
  assert_eq!(
    sent[1..],
    [
      ServerRequest::BranchLevel(BranchLevel { level: 0 }),
      ServerRequest::BranchRoot(BranchRoot {
        root: USERNAME.into()
      }),
      ServerRequest::AcceptChildren(AcceptChildren { accept: false }),
      ServerRequest::HaveNoParents(HaveNoParents {
        have_no_parents: false
      }),
    ]
  );
}
