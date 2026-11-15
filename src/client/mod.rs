//! The client: one server session at a time (connect, log in, disconnect), the listener that
//! runs alongside it, the server message dispatcher, the operations that ask the server for
//! something, and the parts that live across sessions: connections to other users, the
//! distributed search tree, the search responder, the waiter and the tokens.

mod account;
mod connect;
mod disconnect;
mod endpoint;
mod inbound;
mod interests;
mod messaging;
mod private_rooms;
mod reconfigure;
mod request;
mod rooms;
mod server;
mod session;
mod users;

pub(crate) use request::{failed, non_blank};

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};

use session::{Edge, Released, Stage};

use soulseek_proto::ServerRequest;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

use crate::browse::FolderRequests;
use crate::config::Config;
use crate::conn::MessageConn;
use crate::distributed::DistributedNetwork;
use crate::error::Error;
use crate::events::{Event, Events};
use crate::listener::Listener;
use crate::lock;
use crate::peers::PeerConnections;
use crate::responder::SearchResponder;
use crate::search::Searches;

use crate::token::TokenGenerator;
use crate::transfer::Registry;
use crate::waiter::Waiter;

/// Soulseek client.
///
/// Clones share one client.
///
/// # Examples
///
/// ```no_run
/// use soulseek_client::{Client, Config, Error};
///
/// # async fn run() -> Result<(), Error> {
/// let client = Client::new(Config::new())?;
/// client.connect("username", "password").await?;
/// println!("session state: {}", client.state());
///
/// client.disconnect().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
  inner: Arc<Inner>,
}

/// shows the session's state and who it is logged in as
impl std::fmt::Debug for Client {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Client")
      .field("state", &self.state())
      .field("username", &self.username())
      .finish_non_exhaustive()
  }
}

/// A client that does not keep it alive, for the tasks it starts
#[derive(Clone)]
pub(crate) struct WeakClient {
  inner: Weak<Inner>,
}

impl WeakClient {
  pub(crate) fn upgrade(&self) -> Option<Client> {
    self.inner.upgrade().map(|inner| Client { inner })
  }
}

struct Inner {
  /// replaced whole, so even a poisoned lock holds a complete value
  options: RwLock<Arc<Config>>,
  /// written only while `session` is locked, so transitions and their events stay in order
  state: watch::Sender<crate::session::State>,
  session: Mutex<Session>,
  /// serializes `connect` and `reconfigure_options`
  state_lock: tokio::sync::Mutex<()>,
  server_info: Mutex<crate::session::ServerInfo>,
  waiter: Waiter,
  tokens: TokenGenerator,
  events: broadcast::Sender<Event>,
  peers: Arc<PeerConnections>,
  distributed: DistributedNetwork,
  responder: SearchResponder,
  endpoints: endpoint::Endpoints,
  transfers: Arc<Registry>,
  searches: Searches,
  folder_requests: FolderRequests,
  /// the listener kept from the last session, between sessions
  kept_listener: Mutex<Option<Listener>>,
  /// how many connections peers opened to the client's listener so far
  inbound: watch::Sender<u64>,
  /// canceled when the client is shut down or dropped
  lifetime: CancellationToken,
}

/// The server session: which one it is, where it stands with what exists there, and how the
/// last one ended
struct Session {
  /// counts the sessions begun; the current or last one's number
  number: u64,
  stage: Stage,
  /// how the latest session to end ended, kept across the next one
  last_end: Option<crate::session::End>,
  /// the parent of every `connect` call's token from the call on, replaced as a disconnect the
  /// application asks for cancels it; a call can go on after the login and can wait for its
  /// turn before any session begins, so this is not part of a stage
  connects: CancellationToken,
}

impl Default for Session {
  fn default() -> Self {
    Self {
      number: 0,
      stage: Stage::Disconnected { reached: false },
      last_end: None,
      connects: CancellationToken::new(),
    }
  }
}

impl Client {
  /// Creates a disconnected client with `options`.
  ///
  /// The client starts tasks of its own, so it must be created inside a tokio runtime. The tasks
  /// stop when the client is shut down or the last clone is dropped. A handler that holds a
  /// clone, as one that starts uploads does, keeps the client from being dropped, so such a
  /// client stops only at [`Client::shutdown`].
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if the options do not validate.
  ///
  /// # Panics
  ///
  /// Panics if called outside a tokio runtime.
  pub fn new(options: Config) -> Result<Self, Error> {
    Self::with_tokens(options, TokenGenerator::new(None))
  }

  /// [`Client::new`] whose tokens count from `first`, for tests that need to know them
  #[cfg(test)]
  pub(crate) fn counting_from(options: Config, first: u32) -> Result<Self, Error> {
    Self::with_tokens(options, TokenGenerator::new(Some(first)))
  }

  fn with_tokens(options: Config, tokens: TokenGenerator) -> Result<Self, Error> {
    options.validate()?;
    let (events, _) = broadcast::channel(options.event_capacity as usize);
    let (distributed, commands) = DistributedNetwork::new(
      options.limits.distributed_search_memory,
      options.limits.distributed_search_memory_lifetime,
    );
    let waiter = Waiter::new(options.message_timeout);
    let inner = Inner {
      peers: Arc::new(PeerConnections::new(
        options.limits.max_concurrent_peer_connects,
        waiter.clone(),
      )),
      waiter,
      tokens,
      responder: SearchResponder::new(
        options.limits.search_response_cache_size,
        options.limits.search_response_cache_lifetime,
      ),
      endpoints: endpoint::Endpoints::new(
        options.limits.user_endpoint_cache_size,
        options.limits.user_endpoint_cache_lifetime,
      ),
      transfers: Arc::new(Registry::new()),
      searches: Searches::default(),
      folder_requests: FolderRequests::default(),
      options: RwLock::new(Arc::new(options)),
      state: watch::Sender::new(crate::session::State::Disconnected),
      session: Mutex::new(Session::default()),
      state_lock: tokio::sync::Mutex::new(()),
      kept_listener: Mutex::new(None),
      server_info: Mutex::new(crate::session::ServerInfo::default()),
      events,
      distributed,
      inbound: watch::Sender::default(),
      lifetime: CancellationToken::new(),
    };
    let client = Self {
      inner: Arc::new(inner),
    };
    DistributedNetwork::run(client.downgrade(), commands);
    client.start_endpoint_lock_cleanup();
    Ok(client)
  }

  /// Returns where the client is in its server session.
  pub fn state(&self) -> crate::session::State {
    *self.inner.state.borrow()
  }

  /// Returns the address the listener is bound to.
  ///
  /// `None` while no listener runs.
  pub fn listen_address(&self) -> Option<SocketAddr> {
    let running = self
      .inner
      .session()
      .stage
      .listener()
      .and_then(|listener| listener.as_ref().map(Listener::address));
    running.or_else(|| {
      lock(&self.inner.kept_listener)
        .as_ref()
        .map(Listener::address)
    })
  }

  /// Returns the count of inbound peer connections, to read and to follow.
  ///
  /// The number counts the connections peers opened to the client's listener since the client
  /// was created. Every one, a peer answering a connection request the client had the server
  /// relay included, shows that other users can reach the listener.
  pub fn inbound_connections(&self) -> crate::session::InboundConnections {
    crate::session::InboundConnections::new(self.inner.inbound.subscribe())
  }

  /// Returns the server session as it stands.
  ///
  /// The session's number, state, username and the latest end are read together.
  pub fn session(&self) -> crate::session::Status {
    let session = self.inner.session();
    crate::session::Status {
      number: session.number,
      state: session.stage.state(),
      reached: session.stage.reached(),
      username: session.stage.username().map(str::to_owned),
      last_end: session.last_end.clone(),
    }
  }

  /// Returns the server address as given to [`Client::connect`].
  ///
  /// The address is present while connected.
  pub fn server_address(&self) -> Option<String> {
    let session = self.inner.session();
    session.stage.link().map(|link| link.address.clone())
  }

  /// Returns the server endpoint the address resolved to.
  ///
  /// The endpoint is present while connected. A name that a proxy resolved has none here: the
  /// server's address is then the proxy's to know.
  pub fn server_endpoint(&self) -> Option<SocketAddr> {
    let session = self.inner.session();
    session.stage.link().and_then(|link| link.endpoint)
  }

  /// Returns the client's username.
  ///
  /// The username is present while logged in.
  pub fn username(&self) -> Option<String> {
    let session = self.inner.session();
    session.stage.username().map(str::to_owned)
  }

  /// Returns the tuning values the server has sent so far.
  pub fn server_info(&self) -> crate::session::ServerInfo {
    lock(&self.inner.server_info).clone()
  }

  /// the options in effect
  pub(crate) fn options(&self) -> Arc<Config> {
    self.inner.options()
  }

  /// Returns what the client knows of and asks about other users.
  pub fn users(&self) -> crate::user::Users<'_> {
    crate::user::Users { client: self }
  }

  /// Returns the client's chat rooms.
  pub fn rooms(&self) -> crate::room::Rooms<'_> {
    crate::room::Rooms { client: self }
  }

  /// Returns the client's interests and the server's recommendations.
  pub fn interests(&self) -> crate::interests::Interests<'_> {
    crate::interests::Interests { client: self }
  }

  /// Returns the account the client is logged in with.
  pub fn account(&self) -> crate::account::Account<'_> {
    crate::account::Account { client: self }
  }

  /// Returns the client's place in the distributed search tree.
  pub fn distributed(&self) -> crate::distributed::Distributed<'_> {
    crate::distributed::Distributed { client: self }
  }

  /// Returns a stream of every event from now on.
  ///
  /// The client holds [`Config::event_capacity`] events for its slowest stream. One further
  /// behind gets an [`Event::Lagged`] with the number it missed and goes on from the oldest
  /// event still held.
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use soulseek_client::{Client, Event, session};
  ///
  /// # async fn run(client: Client) {
  /// let mut events = client.events();
  /// while let Some(event) = events.next().await {
  ///   match event {
  ///     Event::PrivateMessage(message) => {
  ///       println!("{}: {}", message.username, message.message);
  ///     }
  ///     Event::Session(session::Event::Disconnected { .. }) => break,
  ///     _ => {}
  ///   }
  /// }
  /// # }
  /// ```
  pub fn events(&self) -> Events {
    Events::new(self.inner.events.subscribe())
  }

  /// the table that matches replies to the requests waiting for them
  pub(crate) fn waiter(&self) -> &Waiter {
    &self.inner.waiter
  }

  /// the next token of the client's count, for a transfer or a browse; a search reserves its
  /// own with [`Client::reserve_search_token`]
  pub(crate) fn next_token(&self) -> u32 {
    self.inner.tokens.next()
  }

  /// Returns every user with an established message connection.
  ///
  /// Each user is paired with the address of its connection.
  pub fn message_connections(&self) -> Vec<(String, SocketAddr)> {
    self.inner.peers.message_connections()
  }

  /// Returns the address of the message connection to `username`.
  ///
  /// The connection is the established one the client's requests to `username` go on, if there
  /// is one.
  pub fn message_connection_address(&self, username: &str) -> Option<SocketAddr> {
    self
      .inner
      .peers
      .cached(username)
      .map(|conn| conn.handle().remote())
  }

  /// raises `event` to every subscriber
  pub(crate) fn emit(&self, event: Event) {
    self.inner.emit(event);
  }

  /// canceled when the client is shut down or dropped; long-lived tasks stop with it
  pub(crate) fn cancellation(&self) -> &CancellationToken {
    &self.inner.lifetime
  }

  pub(crate) fn downgrade(&self) -> WeakClient {
    WeakClient {
      inner: Arc::downgrade(&self.inner),
    }
  }

  pub(crate) fn peers(&self) -> &PeerConnections {
    &self.inner.peers
  }

  pub(crate) fn peers_arc(&self) -> Arc<PeerConnections> {
    self.inner.peers.clone()
  }

  pub(crate) fn network(&self) -> &DistributedNetwork {
    &self.inner.distributed
  }

  pub(crate) fn responder(&self) -> &SearchResponder {
    &self.inner.responder
  }

  pub(crate) fn transfer_registry(&self) -> &Arc<Registry> {
    &self.inner.transfers
  }

  pub(crate) fn searches(&self) -> &Searches {
    &self.inner.searches
  }

  pub(crate) fn folder_requests(&self) -> &FolderRequests {
    &self.inner.folder_requests
  }

  pub(crate) fn tokens(&self) -> &TokenGenerator {
    &self.inner.tokens
  }

  pub(crate) fn check_not_shut_down(&self) -> Result<(), Error> {
    self.inner.check_not_shut_down()
  }

  /// The client's username, for the operation `verb` that needs it; fails with
  /// `InvalidOperation` naming `verb` while not logged in.
  pub(crate) fn own_username(&self, verb: &str) -> Result<String, Error> {
    self.require_logged_in(verb)?;
    // a disconnect between the two reads leaves no username
    self
      .username()
      .ok_or_else(|| request::not_logged_in(verb, self.state()))
  }

  /// Writes one message to the server.
  ///
  /// Waits until the message is on the socket.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidOperation`] if the client has no connection to the server,
  /// or if that connection is closing. Returns [`Error::Connection`] if the connection's
  /// write queue is full or the write fails.
  pub async fn write_server_message(&self, message: &ServerRequest) -> Result<(), Error> {
    self
      .write_server_messages(std::slice::from_ref(message))
      .await
  }

  /// Writes several messages to the server as one write, so they reach it together, and waits
  /// until they are on the socket.
  pub(crate) async fn write_server_messages(
    &self,
    messages: &[ServerRequest],
  ) -> Result<(), Error> {
    let server = self.inner.server()?;
    let bytes = messages.iter().flat_map(ServerRequest::encode).collect();
    server.write(bytes).await?;
    for message in messages {
      tracing::debug!(
        code = message.code().code(),
        name = message.code().name(),
        "server message sent"
      );
    }
    Ok(())
  }

  /// Queues a message to the server without waiting for the write, for acknowledgments,
  /// probes and reports whose failure only matters to the connection itself.
  pub(crate) fn send_server_message(&self, message: &ServerRequest) {
    let result = self
      .inner
      .server()
      .and_then(|server| server.send(message.encode()));
    let (code, name) = (message.code().code(), message.code().name());
    match result {
      Ok(()) => tracing::debug!(code, name, "server message sent"),
      Err(error) => tracing::debug!(code, name, %error, "server message not sent"),
    }
  }

  /// Disconnects and stops the client for good.
  ///
  /// Every transfer is canceled, every wait is canceled, every connection to another user is
  /// closed and every task tied to the client stops. Later calls fail with
  /// [`Error::Shutdown`].
  pub async fn shutdown(&self) {
    self
      .end_session(
        Some("client is shutting down"),
        Some(Error::Shutdown),
        disconnect::Cause::Requested,
      )
      .await;
    // shut down first, so the transfers this cancels do not tell their peers
    self.inner.lifetime.cancel();
    self.stop_kept_listener().await;
    self.inner.transfers.cancel_all();
    self.inner.peers.remove_and_close_all();
    self.inner.waiter.cancel_all();
  }
}

impl Client {
  /// stops the listener kept between sessions, if there is one
  pub(super) async fn stop_kept_listener(&self) {
    let kept = lock(&self.inner.kept_listener).take();
    if let Some(listener) = kept {
      listener.stop().await;
    }
  }
}

impl Inner {
  fn options(&self) -> Arc<Config> {
    self
      .options
      .read()
      .unwrap_or_else(std::sync::PoisonError::into_inner)
      .clone()
  }

  fn set_options(&self, options: Arc<Config>) {
    *self
      .options
      .write()
      .unwrap_or_else(std::sync::PoisonError::into_inner) = options;
  }

  fn session(&self) -> MutexGuard<'_, Session> {
    lock(&self.session)
  }

  fn server(&self) -> Result<MessageConn, Error> {
    let session = self.session();
    match session.stage.link() {
      Some(link) => Ok(link.server.clone()),
      None => Err(Error::InvalidOperation(format!(
        "cannot send on a connection that is not connected (state: {})",
        session.stage.state()
      ))),
    }
  }

  fn check_not_shut_down(&self) -> Result<(), Error> {
    if self.lifetime.is_cancelled() {
      return Err(Error::Shutdown);
    }
    Ok(())
  }

  fn emit(&self, event: Event) {
    // no subscribers is a normal state, not an error
    let _ = self.events.send(event);
  }

  /// Takes `edge` and raises its events, or refuses it and changes nothing. The caller holds
  /// the session lock, which keeps transitions and their events in one order. This is the one
  /// place the session's stage and the published state change.
  fn transition(
    &self,
    session: &mut Session,
    edge: Edge,
    message: String,
    error: Option<Error>,
  ) -> Result<Released, crate::session::State> {
    let released = session.stage.advance(edge)?;
    let state = session.stage.state();
    let previous_state = self.state.send_replace(state);
    tracing::debug!(%previous_state, %state, message, "client state changed");
    self.emit(Event::Session(crate::session::Event::StateChanged {
      previous_state,
      state,
      message: message.clone(),
      error: error.clone(),
    }));
    match &session.stage {
      Stage::Connecting { .. } => session.number += 1,
      Stage::Connected { .. } => self.emit(Event::Session(crate::session::Event::Connected {
        session: session.number,
      })),
      Stage::LoggedIn { username, .. } => {
        self.emit(Event::Session(crate::session::Event::LoggedIn {
          session: session.number,
          username: username.clone(),
        }))
      }
      Stage::Disconnected { .. } => {
        let at = std::time::SystemTime::now();
        session.last_end = Some(crate::session::End {
          session: session.number,
          message: message.clone(),
          error: error.clone(),
          at,
        });
        self.emit(Event::Session(crate::session::Event::Disconnected {
          session: session.number,
          message,
          error,
          at,
        }));
      }
      Stage::LoggingIn { .. } | Stage::Disconnecting { .. } => {}
    }
    Ok(released)
  }

  fn merge_server_info(&self, merge: impl FnOnce(&mut crate::session::ServerInfo)) {
    let info = {
      let mut info = lock(&self.server_info);
      merge(&mut info);
      info.clone()
    };
    self.emit(Event::Session(crate::session::Event::ServerInfo(info)));
  }
}

impl Drop for Inner {
  fn drop(&mut self) {
    self.lifetime.cancel();
    self.peers.remove_and_close_all();
    let session = self
      .session
      .get_mut()
      .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(link) = session.stage.link() {
      link
        .server
        .handle()
        .disconnect("client is shutting down", Some(Error::Shutdown));
    }
  }
}
