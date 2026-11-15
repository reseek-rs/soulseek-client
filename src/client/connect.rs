//! Connecting and logging in: the argument checks, resolving the server, starting the listener,
//! the server connection, the login and its outcomes, and the configuration messages sent once
//! logged in.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use soulseek_proto::frame::FrameKind;
use soulseek_proto::server::{LoginRequest, LoginResponse, SetListenPort};
use soulseek_proto::{Secret, ServerCode, ServerRequest, validate_login_username};
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::Client;
use super::disconnect::Cause;
use super::session::{Edge, Link, Stage};
use crate::conn::{Conn, ConnKind, MessageConn, ReadEvent, resolve};
use crate::error::{ClientError, ConnectionErrorKind};
use crate::inbound;
use crate::listener::Listener;
use crate::lock;
use crate::log::LogLimiter;
use crate::options::{ClientOptions, ProxyOptions};
use crate::state::ClientState;
use crate::waiter::WaitKey;

static UNIDENTIFIED_FULL: LogLimiter = LogLimiter::new();

/// where the server connection is made
#[derive(Clone, Debug)]
enum Server {
  /// at the address the server's name resolved to
  At(SocketAddr),
  /// through `proxy`, found at `at`, which is given the server's name to resolve
  Named { at: SocketAddr, proxy: ProxyOptions },
}

impl Client {
  /// Connects to the server at `address:port` and logs in. The listener starts first, so a busy
  /// listen port fails the connect before the server is contacted. `cancel`, or a call to
  /// [`Client::disconnect`] made after this one began, ends it with `Canceled`, whether it is
  /// under way or still waiting for an earlier session to end. A call that finds a session logged
  /// in by another call with the same server, username and password joins it, and is refused
  /// otherwise. Any failure after the state changed leaves the client disconnected, with a
  /// `Disconnected` event naming the error.
  pub async fn connect(
    &self,
    address: &str,
    port: u16,
    username: &str,
    password: &str,
    cancel: &CancellationToken,
  ) -> Result<(), ClientError> {
    validate_arguments(address, username, password, self.options().check_names)?;
    self.inner.check_not_shut_down()?;
    self.check_not_connected()?;
    let attempt = Attempt {
      caller: cancel,
      token: self.inner.session().connects.child_token(),
    };
    let connecting = self.connect_attempt(address, port, username, password, &attempt);
    tokio::pin!(connecting);
    // the caller's token first, so a call made with it canceled ends before it begins anything
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        attempt.token.cancel();
        connecting.await
      }
      result = &mut connecting => result,
    }
  }

  /// the connect, canceled through `attempt`
  async fn connect_attempt(
    &self,
    address: &str,
    port: u16,
    username: &str,
    password: &str,
    attempt: &Attempt<'_>,
  ) -> Result<(), ClientError> {
    let options = self.inner.options();
    // a name goes to the proxy to resolve when there is one, so no lookup of it is made here
    let proxy = options
      .server_connection_options
      .proxy
      .as_ref()
      .filter(|_| address.parse::<IpAddr>().is_err());
    let (host, host_port) = match proxy {
      Some(proxy) => (proxy.address.as_str(), proxy.port),
      None => (address, port),
    };
    let remote = tokio::select! {
      biased;
      () = attempt.token.cancelled() => return Err(ClientError::Canceled),
      resolved = resolve(host, host_port) => resolved.map_err(|error| {
        ClientError::Address(format!("Failed to resolve address '{host}': {error}"))
      })?,
    };
    let to = match proxy {
      Some(proxy) => Server::Named {
        at: remote,
        proxy: proxy.clone(),
      },
      None => Server::At(remote),
    };
    let _connecting = tokio::select! {
      biased;
      () = attempt.token.cancelled() => return Err(ClientError::Canceled),
      guard = self.inner.state_lock.lock() => guard,
    };
    // declared after the lock so it drops first: a queued connect never sees this one's session
    // half made, unless it was dropped outside a runtime
    let mut unfinished = Unfinished {
      client: self,
      armed: false,
    };
    let mut state = self.inner.state.subscribe();
    loop {
      let left_behind = {
        let mut session = self.inner.session();
        // transitions publish the state under the session lock, so a change after this read is
        // one `changed` sees
        state.borrow_and_update();
        match &session.stage {
          // the previous session is still being torn down; this waits for it below
          Stage::Disconnecting { .. } => false,
          // another connect logged in while this one waited for the lock
          Stage::LoggedIn {
            link,
            username: current,
            password: current_password,
            ..
          } => {
            return joined(
              link,
              current,
              current_password,
              address,
              port,
              username,
              password,
            );
          }
          Stage::Disconnected { .. } => {
            // a disconnect cancels under the session lock, so one asked for while this call
            // waited is seen here, as is a cancel of the caller's made before now, and no
            // session begins
            attempt.check()?;
            self
              .inner
              .transition(&mut session, Edge::Connect, "Connecting".into(), None)
              .expect("a disconnected session takes the connect edge");
            unfinished.armed = true;
            break;
          }
          // connects take turns under the lock, so a session part made while this one holds it
          // belongs to a connect dropped outside a runtime, where it could not end it
          Stage::Connecting { .. } | Stage::Connected { .. } | Stage::LoggingIn { .. } => true,
        }
      };
      if left_behind {
        tracing::debug!("a session whose connect was dropped is ended before connecting");
        if let Some(teardown) = self.inner.begin_disconnect(
          Some("connect dropped"),
          Some(ClientError::Canceled),
          Cause::Abandoned,
        ) {
          drop(teardown.spawn(self));
        }
      }
      tokio::select! {
        biased;
        () = attempt.token.cancelled() => return Err(ClientError::Canceled),
        // the client holds the sender for as long as this call can run
        _ = state.changed() => {}
      }
    }
    // the caller tells of its connects and of how they fail, the retries it makes included
    tracing::debug!(address, port, ?to, username, "connecting to server");
    let result = self
      .connect_session(&options, (address, port), to, username, password, attempt)
      .await;
    let Err(error) = result else {
      unfinished.armed = false;
      return Ok(());
    };
    let error = if attempt.canceled() {
      ClientError::Canceled
    } else {
      connect_error(error)
    };
    self
      .end_session(
        Some(&error.to_string()),
        Some(error.clone()),
        Cause::Abandoned,
      )
      .await;
    Err(error)
  }

  /// refuses a connect while a session is under way; a disconnect under way is waited for
  fn check_not_connected(&self) -> Result<(), ClientError> {
    match self.state() {
      ClientState::Disconnected | ClientState::Disconnecting => Ok(()),
      state => Err(connect_refused(state)),
    }
  }

  async fn connect_session(
    &self,
    options: &Arc<ClientOptions>,
    (address, port): (&str, u16),
    to: Server,
    username: &str,
    password: &str,
    attempt: &Attempt<'_>,
  ) -> Result<(), ClientError> {
    // one kept from the last session serves this one if it is bound as the options ask
    let bound = SocketAddr::new(options.listen_ip_address, options.listen_port);
    let kept = lock(&self.inner.kept_listener)
      .take_if(|kept| !options.enable_listener || !kept.is_bound_as(bound, options.listen_backlog));
    if let Some(stale) = kept {
      stale.stop().await;
    }
    if options.enable_listener {
      let kept = lock(&self.inner.kept_listener).take();
      let listener = match kept {
        Some(listener) => listener,
        None => self.start_listener(options, false)?,
      };
      let mut session = self.inner.session();
      attempt.check()?;
      let Stage::Connecting { listener: place } = &mut session.stage else {
        return Err(ended_while_connecting(session.stage.state()));
      };
      *place = Some(listener);
    }

    let kind = ConnKind::OUTBOUND | ConnKind::DIRECT;
    let connection_options = options.effective_server_connection_options();
    let conn = match &to {
      Server::Named { at, proxy } => {
        Conn::connect_named(
          *at,
          proxy,
          address,
          port,
          kind,
          connection_options,
          &attempt.token,
        )
        .await?
      }
      Server::At(remote) => {
        Conn::connect(*remote, None, kind, connection_options, &attempt.token).await?
      }
    };
    let server = conn.into_message(FrameKind::Server, options.limits.decode)?;
    {
      let mut session = self.inner.session();
      let link = Link {
        server: server.clone(),
        address: address.to_owned(),
        port,
        ip_endpoint: match &to {
          Server::At(remote) => Some(*remote),
          Server::Named { .. } => None,
        },
      };
      let reached = attempt.check().and_then(|()| {
        self
          .inner
          .transition(
            &mut session,
            Edge::Reach(link),
            match &to {
              Server::At(remote) => format!("Connected to {remote}"),
              Server::Named { .. } => format!("Connected to {address}:{port}"),
            },
            None,
          )
          .map(drop)
          .map_err(ended_while_connecting)
      });
      if let Err(error) = reached {
        server
          .handle()
          .disconnect(error.to_string(), Some(error.clone()));
        return Err(error);
      }
    }
    tracing::debug!(?to, "connected to server");
    self.start_server_reading(&server);

    self.login(username, password, options, attempt).await?;
    self.send_configuration_messages().await?;
    if options.enable_distributed_network
      && options.accept_distributed_children
      && options.follow_server_child_limits
    {
      self.ask_own_speed(username);
    }
    spawn_ping(server, options.server_ping_interval);
    Ok(())
  }

  /// Asks the server for our own statistics on a task of its own: our upload speed bounds how
  /// many distributed children we take and whether we take any, and the reply is the only way
  /// the client learns it.
  fn ask_own_speed(&self, username: &str) {
    let client = self.clone();
    let username = username.to_owned();
    tokio::spawn(
      async move {
        if let Err(error) = client.get_user_statistics(&username).await {
          tracing::debug!(%error, "our own statistics not read; the child limit stays as set");
        }
      }
      .in_current_span(),
    );
  }

  async fn login(
    &self,
    username: &str,
    password: &str,
    options: &ClientOptions,
    attempt: &Attempt<'_>,
  ) -> Result<(), ClientError> {
    {
      let mut session = self.inner.session();
      attempt.check()?;
      self
        .inner
        .transition(
          &mut session,
          Edge::LogIn {
            username: username.to_owned(),
            password: Secret::new(password),
          },
          "Logging in".into(),
          None,
        )
        .map_err(ended_while_connecting)?;
    }
    let reply = self.inner.waiter.wait::<LoginResponse>(
      WaitKey::Server(ServerCode::Login),
      None,
      attempt.token.clone(),
    );
    // the listen port goes in the same write, so other users learn it as soon as we appear
    self
      .write_server_messages(&[
        ServerRequest::Login(LoginRequest::new(
          username.to_owned(),
          password.to_owned(),
          options.protocol_major_version,
          options.protocol_minor_version,
        )),
        ServerRequest::SetListenPort(SetListenPort {
          port: u32::from(options.port_advertised()),
          obfuscation: None,
        }),
      ])
      .await?;
    match reply.await {
      Ok(LoginResponse::Success { greeting, ip, .. }) => {
        // the dispatcher logged the session in as it read the reply, unless a disconnect came
        // first
        attempt.check()?;
        let state = self.state();
        if !state.is_logged_in() {
          return Err(ended_while_connecting(state));
        }
        tracing::info!(username, public_ip = %ip, "logged in");
        tracing::debug!(greeting, "server greeting");
        Ok(())
      }
      Ok(LoginResponse::Failure { reason, detail }) => {
        let reason = match detail {
          Some(detail) => format!("{reason}: {detail}"),
          None => reason,
        };
        Err(ClientError::LoginRejected(format!(
          "The server rejected login attempt: {reason}"
        )))
      }
      // the login was written and the connection is still up, or the disconnect would have
      // failed the wait first
      Err(ClientError::Timeout(_)) => Err(ClientError::LoginNoResponse(
        "the server did not answer the login in time".into(),
      )),
      Err(error) => Err(error),
    }
  }

  /// Tells the server our listen port and our place in the distributed tree; sent after login
  /// and after the options change.
  pub(super) async fn send_configuration_messages(&self) -> Result<(), ClientError> {
    let options = self.inner.options();
    self
      .write_server_message(&ServerRequest::SetListenPort(SetListenPort {
        port: u32::from(options.port_advertised()),
        obfuscation: None,
      }))
      .await?;
    self.update_distributed_status(false).await;
    Ok(())
  }

  /// Binds the listener on the configured address, for a session that is `serving` when it has
  /// logged in already. Accepted connections are classified on a task of their own and handed
  /// on, at most `limits.max_unidentified_connections` of them at once.
  pub(super) fn start_listener(
    &self,
    options: &ClientOptions,
    serving: bool,
  ) -> Result<Listener, ClientError> {
    let client = self.downgrade();
    let limit = options.limits.max_unidentified_connections;
    let unidentified = Arc::new(Semaphore::new(limit as usize));
    Listener::start(
      SocketAddr::new(options.listen_ip_address, options.listen_port),
      options.listen_backlog,
      serving,
      move |stream, remote| {
        // the client is gone; dropping the stream closes it
        let Some(client) = client.upgrade() else {
          return;
        };
        let Ok(counted) = unidentified.clone().try_acquire_owned() else {
          if let Some(suppressed) = UNIDENTIFIED_FULL.allow() {
            tracing::debug!(
              %remote,
              limit,
              suppressed,
              "too many connections not identified yet, closed on accept"
            );
          }
          return;
        };
        let options = client.options().incoming_connection_options.clone();
        let conn = Conn::accepted(stream, remote, options);
        let span = conn.handle().span().clone();
        let lifetime = client.cancellation().clone();
        let client = client.downgrade();
        // a client shut down drops the connection with the classification
        tokio::spawn(
          async move {
            tokio::select! {
              () = lifetime.cancelled() => {}
              () = async {
                let inbound = inbound::classify(conn).await;
                if let Some(inbound) = inbound
                  && let Some(client) = client.upgrade()
                {
                  client.route_inbound(inbound, counted).await;
                }
              } => {}
            }
          }
          .instrument(span),
        );
      },
    )
  }

  /// Hands every server frame to the dispatcher, and ends the session when the connection
  /// closes on its own.
  fn start_server_reading(&self, server: &MessageConn) {
    let inner = Arc::downgrade(&self.inner);
    server.start_reading(move |event| {
      if let ReadEvent::Frame(frame) = event
        && let Some(inner) = inner.upgrade()
      {
        Client { inner }.dispatch_server_frame(&frame);
      }
    });
    let inner = Arc::downgrade(&self.inner);
    let handle = server.handle().clone();
    let span = handle.span().clone();
    tokio::spawn(
      async move {
        let reason = handle.disconnected().await;
        if let Some(inner) = inner.upgrade() {
          Client { inner }
            .server_disconnected(handle.id(), reason)
            .await;
        }
      }
      .instrument(span),
    );
  }
}

/// Writes a ping every `every` without waiting for an answer, so a socket that stopped taking
/// writes is noticed; stops when the connection closes.
fn spawn_ping(server: MessageConn, every: Duration) {
  let span = server.handle().span().clone();
  tokio::spawn(
    async move {
      loop {
        tokio::select! {
          () = server.handle().cancellation().cancelled() => return,
          () = sleep_until(crate::later(Instant::now(), every)) => {}
        }
        if let Err(error) = server.send(ServerRequest::Ping.encode()) {
          tracing::debug!(%error, "write-liveness ping not sent");
          return;
        }
        tracing::trace!("write-liveness ping sent");
      }
    }
    .instrument(span),
  );
}

/// `check_names` holds the username to the official server's rules; without it the username
/// only has to be there
fn validate_arguments(
  address: &str,
  username: &str,
  password: &str,
  check_names: bool,
) -> Result<(), ClientError> {
  let invalid = |message: &str| Err(ClientError::InvalidArgument(message.into()));
  if address.trim().is_empty() {
    return invalid("Address must not be empty or only whitespace");
  }
  if username.is_empty() {
    return invalid("Username must not be empty");
  }
  if check_names && let Err(error) = validate_login_username(username) {
    return invalid(&format!("Invalid username: {error}"));
  }
  if password.is_empty() {
    return invalid("Password must not be empty");
  }
  Ok(())
}

/// Ends the session a `connect` began when the call is dropped before it returns, as a
/// timeout around it does, so the session does not stay connecting or logging in with nothing
/// moving it on
struct Unfinished<'a> {
  client: &'a Client,
  /// the connect began a session and has not logged in
  armed: bool,
}

impl Drop for Unfinished<'_> {
  fn drop(&mut self) {
    if !self.armed {
      return;
    }
    // dropped outside a runtime, as after a `block_on` returns, nothing could run the teardown;
    // a connect already waiting for the lock ends the session instead, else a disconnect must,
    // since a new connect is refused while the session stays connecting
    if tokio::runtime::Handle::try_current().is_err() {
      return;
    }
    if let Some(teardown) = self.client.inner.begin_disconnect(
      Some("connect dropped"),
      Some(ClientError::Canceled),
      Cause::Abandoned,
    ) {
      drop(teardown.spawn(self.client));
    }
  }
}

/// Joins the session logged in over `link` as `current` with `current_password` when a connect
/// for `address:port` as `username` with `password` would have made the same one; refuses it
/// otherwise, since that connect would report a session it did not make
fn joined(
  link: &Link,
  current: &str,
  current_password: &Secret,
  address: &str,
  port: u16,
  username: &str,
  password: &str,
) -> Result<(), ClientError> {
  let refused = |message: &str| Err(ClientError::InvalidOperation(message.into()));
  if current != username {
    return refused("The client is already connected as another user");
  }
  if link.address != address || link.port != port {
    return refused("The client is already connected to another server");
  }
  if current_password.expose() != password {
    return refused("The client is already connected with another password");
  }
  Ok(())
}

/// What cancels one connect: the caller's token, and `token`, a child of the session's connects,
/// which a disconnect asked for after the connect began cancels. The caller's cancel reaches
/// `token` when the connect is next polled, and at once wherever the connect decides to go on.
struct Attempt<'a> {
  caller: &'a CancellationToken,
  token: CancellationToken,
}

impl Attempt<'_> {
  /// whether the connect was canceled, by its caller or a disconnect; a caller's cancel is passed
  /// on to `token` here, so whatever waits on it ends too
  fn canceled(&self) -> bool {
    if self.caller.is_cancelled() {
      self.token.cancel();
    }
    self.token.is_cancelled()
  }

  /// fails when the connect was canceled
  fn check(&self) -> Result<(), ClientError> {
    if self.canceled() {
      return Err(ClientError::Canceled);
    }
    Ok(())
  }
}

/// the error of a connect whose session a disconnect moved on to `state`
fn ended_while_connecting(state: ClientState) -> ClientError {
  ClientError::connection(
    ConnectionErrorKind::Other,
    format!("the server session ended while connecting (state: {state})"),
  )
}

/// the refusal of a connect while the session is in `state`
fn connect_refused(state: ClientState) -> ClientError {
  let message = if state.is_transitioning() {
    "A connection is already in the process of being established"
  } else {
    "The client is already connected"
  };
  ClientError::InvalidOperation(message.into())
}

/// what a failed connect reports: its own outcomes as they are, anything else as a connect
/// failure wrapping the cause
fn connect_error(error: ClientError) -> ClientError {
  match error {
    ClientError::LoginRejected(_)
    | ClientError::LoginNoResponse(_)
    | ClientError::Canceled
    | ClientError::Timeout(_)
    | ClientError::Listen { .. } => error,
    error => ClientError::Connect {
      message: format!("Failed to connect: {error}"),
      source: Some(Arc::new(error)),
    },
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn a_connect_its_caller_canceled_begins_no_session_before_it_is_polled_again() {
    let client = Client::new(ClientOptions {
      enable_listener: false,
      ..ClientOptions::default()
    })
    .unwrap();
    let mut events = client.subscribe();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
      .unwrap()
      .local_addr()
      .unwrap()
      .port();
    // canceled after the connect last looked at the caller's token, so its own token has not
    // heard of it
    let caller = CancellationToken::new();
    caller.cancel();
    let attempt = Attempt {
      caller: &caller,
      token: client.inner.session().connects.child_token(),
    };
    let ended = client
      .connect_attempt("127.0.0.1", port, "alice", "secret", &attempt)
      .await;
    assert!(matches!(ended, Err(ClientError::Canceled)), "{ended:?}");
    assert!(events.try_recv().is_err(), "no session began");
  }

  fn rejected(address: &str, username: &str, password: &str) -> String {
    match validate_arguments(address, username, password, true) {
      Err(ClientError::InvalidArgument(message)) => message,
      other => panic!("expected an invalid argument, got {other:?}"),
    }
  }

  #[test]
  fn arguments_are_checked_before_anything_is_sent() {
    validate_arguments("server", "alice", "secret", true).unwrap();
    assert!(rejected(" ", "alice", "secret").starts_with("Address must not"));
    assert_eq!(
      rejected("server", "", "secret"),
      "Username must not be empty"
    );
    assert_eq!(
      rejected("server", " alice", "secret"),
      "Invalid username: username starts or ends with a space"
    );
    assert_eq!(
      rejected("server", "server", "secret"),
      "Invalid username: username is reserved"
    );
    // a server with rules of its own takes the name as given
    validate_arguments("server", " alice", "secret", false).unwrap();
    assert!(validate_arguments("server", "", "secret", false).is_err());
    assert_eq!(
      rejected("server", "alice", ""),
      "Password must not be empty"
    );
  }
}
