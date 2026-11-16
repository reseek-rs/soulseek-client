//! Connecting and logging in: the argument checks, resolving the server, starting the listener,
//! the server connection, the login and its outcomes, and the configuration messages sent once
//! logged in.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use soulseek_proto::frame::FrameKind;
use soulseek_proto::server::{LoginRequest, LoginResponse, SetListenPort};
use soulseek_proto::{ServerCode, ServerRequest, validate_login_username};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::Client;
use super::disconnect::Cause;
use super::session::{Edge, Link, Stage};
use crate::config::{Config, Proxy};
use crate::conn::{Conn, ConnKind, MessageConn, ReadEvent, resolve};
#[cfg(doc)]
use crate::error::ErrorKind;
use crate::error::{Error, Repr};
use crate::inbound;
use crate::limit::Limiter;
use crate::listener::Listener;
use crate::lock;
use crate::log::LogLimiter;
use crate::waiter::WaitKey;

static UNIDENTIFIED_FULL: LogLimiter = LogLimiter::new();

/// where the server connection is made
#[derive(Clone, Debug)]
enum Server {
  /// at the address the server's name resolved to
  At(SocketAddr),
  /// through `proxy`, found at `at`, which is given the server's name to resolve
  Named { at: SocketAddr, proxy: Proxy },
}

impl Client {
  /// Connects to the configured server and logs in.
  ///
  /// The server is the one at [`Config::server_host`] and [`Config::server_port`].
  /// The listener starts first, so a busy listen port fails the connect before the server is
  /// contacted. A call to [`Client::disconnect`] made after this one began ends this call,
  /// whether it is under way or still waiting for an earlier session to end. Any
  /// failure after the state changed leaves the client disconnected, with an
  /// [`session::Event::Disconnected`](crate::session::Event::Disconnected) naming the error.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if the configured server host is empty or only
  /// whitespace, if the username or the password is empty, or if [`Config::check_names`] is
  /// set and the username is not a valid login name.
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down.
  ///
  /// Returns [`ErrorKind::InvalidOperation`] if the call is made while a session exists or
  /// is being made, whatever the call asks for.
  ///
  /// Returns [`ErrorKind::Canceled`] if [`Client::disconnect`] is called after this call began.
  ///
  /// Returns [`ErrorKind::Address`] if the address to connect to cannot be resolved.
  ///
  /// Returns [`ErrorKind::Listen`] if the listener cannot bind its address.
  ///
  /// Returns [`ErrorKind::LoginRejected`] if the server rejects the login, and
  /// [`ErrorKind::LoginNoResponse`] if the server does not answer the login in time.
  ///
  /// Returns [`ErrorKind::Timeout`] if a step of the connect times out, and the error of the failed
  /// step, led by "failed to connect", for any other failure.
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use soulseek_client::{Client, Error};
  ///
  /// # async fn run(client: Client) -> Result<(), Error> {
  /// client.connect("username", "password").await?;
  /// # Ok(())
  /// # }
  /// ```
  pub async fn connect(&self, username: &str, password: &str) -> Result<(), Error> {
    self.inner.check_not_shut_down()?;
    self.check_not_connected()?;
    let attempt = Attempt {
      token: self.inner.session().connects.child_token(),
    };
    self.connect_attempt(username, password, &attempt).await
  }

  /// the connect, canceled through `attempt`
  async fn connect_attempt(
    &self,
    username: &str,
    password: &str,
    attempt: &Attempt,
  ) -> Result<(), Error> {
    let _connecting = tokio::select! {
      biased;
      () = attempt.token.cancelled() => return Err(Error::canceled()),
      guard = self.inner.state_lock.lock() => guard,
    };
    // read under the lock, so a reconfigure is either wholly before this connect or after it
    let options = self.inner.options();
    let (address, port) = (options.server_host.as_str(), options.server_port);
    validate_arguments(address, username, password, options.check_names)?;
    // a name goes to the proxy to resolve when there is one, so no lookup of it is made here
    let proxy = options
      .server_connection
      .proxy
      .as_ref()
      .filter(|_| address.parse::<IpAddr>().is_err());
    let (host, host_port) = match proxy {
      Some(proxy) => (proxy.address.as_str(), proxy.port),
      None => (address, port),
    };
    let remote = tokio::select! {
      biased;
      () = attempt.token.cancelled() => return Err(Error::canceled()),
      resolved = resolve(host, host_port) => resolved.map_err(|error| {
        Error::address(format!("failed to resolve address '{host}': {error}"))
      })?,
    };
    let to = match proxy {
      Some(proxy) => Server::Named {
        at: remote,
        proxy: proxy.clone(),
      },
      None => Server::At(remote),
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
          Stage::LoggedIn { .. } => return Err(connect_refused(crate::session::State::LoggedIn)),
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
          Some(Error::canceled()),
          Cause::Abandoned,
        ) {
          drop(teardown.spawn(self));
        }
      }
      tokio::select! {
        biased;
        () = attempt.token.cancelled() => return Err(Error::canceled()),
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
      Error::canceled()
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
  fn check_not_connected(&self) -> Result<(), Error> {
    match self.state() {
      crate::session::State::Disconnected | crate::session::State::Disconnecting => Ok(()),
      state => Err(connect_refused(state)),
    }
  }

  async fn connect_session(
    &self,
    options: &Arc<Config>,
    (address, port): (&str, u16),
    to: Server,
    username: &str,
    password: &str,
    attempt: &Attempt,
  ) -> Result<(), Error> {
    if options.listener {
      // one kept from the last session serves this one when it is bound as the options say
      let kept = lock(&self.inner.kept_listener).take();
      let (listener, was_kept) = match kept {
        Some(kept) if kept.bound_as(options) => (kept, true),
        stale => {
          if let Some(stale) = stale {
            stale.stop().await;
          }
          (self.start_listener(options, false)?, false)
        }
      };
      let placed = {
        let mut session = self.inner.session();
        match (attempt.check(), &mut session.stage) {
          (Ok(()), Stage::Connecting { listener: place }) => {
            *place = Some(listener);
            Ok(())
          }
          (Err(error), _) => Err((error, listener)),
          (Ok(()), stage) => Err((ended_while_connecting(stage.state()), listener)),
        }
      };
      if let Err((error, listener)) = placed {
        // a kept listener goes back for the next connect
        if was_kept {
          *lock(&self.inner.kept_listener) = Some(listener);
        }
        return Err(error);
      }
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
        host: address.to_owned(),
        address: match &to {
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
              Server::At(remote) => format!("connected to {remote}"),
              Server::Named { .. } => format!("connected to {address}:{port}"),
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
    // the listen port went out with the login
    self.update_distributed_status(false).await;
    if options.distributed_network
      && options.distributed_children
      && options.follow_server_child_limits
    {
      self.ask_own_speed(username);
    }
    if let Some(every) = options.server_ping_interval {
      spawn_ping(server, every);
    }
    Ok(())
  }

  /// Asks the server for the client's own statistics on a task of its own: the client's upload
  /// speed bounds how many distributed children it takes and whether it takes any, and the
  /// reply is the only way the client learns it.
  pub(super) fn ask_own_speed(&self, username: &str) {
    let client = self.clone();
    let username = username.to_owned();
    tokio::spawn(
      async move {
        if let Err(error) = client.users().statistics(&username).await {
          tracing::debug!(%error, "the client's own statistics not read; the child limit stays as set");
        }
      }
      .in_current_span(),
    );
  }

  async fn login(
    &self,
    username: &str,
    password: &str,
    options: &Config,
    attempt: &Attempt,
  ) -> Result<(), Error> {
    {
      let mut session = self.inner.session();
      attempt.check()?;
      self
        .inner
        .transition(
          &mut session,
          Edge::LogIn {
            username: username.to_owned(),
          },
          "logging in".into(),
          None,
        )
        .map_err(ended_while_connecting)?;
    }
    let reply = self.inner.waiter.wait::<LoginResponse>(
      WaitKey::Server(ServerCode::Login),
      None,
      attempt.token.clone(),
    );
    // the listen port goes in the same write, so other users learn it as soon as the client
    // appears
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
      Ok(LoginResponse::Failure { reason, detail }) => Err(Error::login_rejected(reason, detail)),
      // the login was written and the connection is still up, or the disconnect would have
      // failed the wait first
      Err(error) if error.is_timeout() => Err(Error::login_no_response(
        "the server did not answer the login in time".into(),
      )),
      Err(error) => Err(error),
    }
  }

  /// Tells the server the client's listen port and its place in the distributed tree, after the
  /// options change.
  pub(super) async fn send_configuration_messages(&self) -> Result<(), Error> {
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
  /// on, at most `limits.max_unidentified_connections` of them at once, as the option reads at
  /// each accept.
  pub(super) fn start_listener(&self, options: &Config, serving: bool) -> Result<Listener, Error> {
    let client = self.downgrade();
    let unidentified = Limiter::new(options.limits.max_unidentified_connections);
    Listener::start(
      SocketAddr::new(options.listen_address, options.listen_port),
      options.listen_backlog,
      serving,
      move |stream, remote| {
        // the client is gone; dropping the stream closes it
        let Some(client) = client.upgrade() else {
          return;
        };
        let limit = client.options().limits.max_unidentified_connections;
        let Some(counted) = unidentified.try_acquire(limit) else {
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
        let options = client.options().incoming_connection.clone();
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
) -> Result<(), Error> {
  let invalid = |message: &str| Err(Error::invalid_argument(message.into()));
  if address.trim().is_empty() {
    return invalid("address must not be empty or only whitespace");
  }
  if username.is_empty() {
    return invalid("username must not be empty");
  }
  if check_names && let Err(error) = validate_login_username(username) {
    return invalid(&format!("invalid username: {error}"));
  }
  if password.is_empty() {
    return invalid("password must not be empty");
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
      Some(Error::canceled()),
      Cause::Abandoned,
    ) {
      drop(teardown.spawn(self.client));
    }
  }
}

/// What cancels one connect: `token`, a child of the session's connects, which a disconnect
/// asked for after the connect began cancels
struct Attempt {
  token: CancellationToken,
}

impl Attempt {
  /// whether a disconnect canceled the connect
  fn canceled(&self) -> bool {
    self.token.is_cancelled()
  }

  /// fails when the connect was canceled
  fn check(&self) -> Result<(), Error> {
    if self.canceled() {
      return Err(Error::canceled());
    }
    Ok(())
  }
}

/// the error of a connect whose session a disconnect moved on to `state`
fn ended_while_connecting(state: crate::session::State) -> Error {
  Error::connection(
    crate::error::ConnectionKind::Other,
    format!("the server session ended while connecting (state: {state})"),
  )
}

/// the refusal of a connect while the session is in `state`
fn connect_refused(state: crate::session::State) -> Error {
  let message = if state.is_transitioning() {
    "a connection is already in the process of being established"
  } else {
    "the client is already connected"
  };
  Error::invalid_operation(message.into())
}

/// what a failed connect reports: its own outcomes as they are, anything else led by the
/// failure to connect
fn connect_error(error: Error) -> Error {
  match error.repr() {
    Repr::LoginRejected { .. }
    | Repr::LoginNoResponse(_)
    | Repr::Canceled
    | Repr::Timeout(_)
    | Repr::Listen { .. } => error,
    _ => error.context("failed to connect"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn a_connect_a_disconnect_canceled_begins_no_session() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
      .unwrap()
      .local_addr()
      .unwrap()
      .port();
    let client = Client::new(Config {
      listener: false,
      server_host: "127.0.0.1".into(),
      server_port: port,
      ..Config::default()
    });
    let mut events = client.events();
    let attempt = Attempt {
      token: client.inner.session().connects.child_token(),
    };
    attempt.token.cancel();
    let ended = client.connect_attempt("alice", "secret", &attempt).await;
    assert!(
      matches!(&ended, Err(matched) if matched.is_canceled()),
      "{ended:?}"
    );
    let raised = std::pin::pin!(events.next())
      .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
    assert!(raised.is_pending(), "no session began");
  }

  fn rejected(address: &str, username: &str, password: &str) -> String {
    match validate_arguments(address, username, password, true)
      .as_ref()
      .map_err(Error::repr)
    {
      Err(Repr::InvalidArgument(message)) => message.clone(),
      other => panic!("expected an invalid argument, got {other:?}"),
    }
  }

  #[test]
  fn arguments_are_checked_before_anything_is_sent() {
    validate_arguments("server", "alice", "secret", true).unwrap();
    assert!(rejected(" ", "alice", "secret").starts_with("address must not"));
    assert_eq!(
      rejected("server", "", "secret"),
      "username must not be empty"
    );
    assert_eq!(
      rejected("server", " alice", "secret"),
      "invalid username: username starts or ends with a space"
    );
    assert_eq!(
      rejected("server", "server", "secret"),
      "invalid username: username is reserved"
    );
    // a server with rules of its own takes the name as given
    validate_arguments("server", " alice", "secret", false).unwrap();
    assert!(validate_arguments("server", "", "secret", false).is_err());
    assert_eq!(
      rejected("server", "alice", ""),
      "password must not be empty"
    );
  }
}
