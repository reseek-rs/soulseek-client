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
use crate::conn::MessageConn;
use crate::distributed::DistributedNetwork;
use crate::error::ClientError;
use crate::events::Event;
use crate::listener::Listener;
use crate::lock;
use crate::options::ClientOptions;
use crate::peers::PeerConnections;
use crate::responder::SearchResponder;
use crate::search::Searches;
use crate::state::{ClientState, ServerInfo, SessionEnd, SessionStatus};
use crate::token::TokenGenerator;
use crate::transfers::Transfers;
use crate::waiter::Waiter;

/// The server the library connects to when the application names none
pub const DEFAULT_SERVER_ADDRESS: &str = "server.slsknet.org";
/// The server port the library connects to when the application names none
pub const DEFAULT_SERVER_PORT: u16 = 2271;

/// A Soulseek client. Clones share one client.
#[derive(Clone)]
pub struct Client {
  inner: Arc<Inner>,
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
  options: RwLock<Arc<ClientOptions>>,
  /// written only while `session` is locked, so transitions and their events stay in order
  state: watch::Sender<ClientState>,
  session: Mutex<Session>,
  /// serializes `connect` and `reconfigure_options`
  state_lock: tokio::sync::Mutex<()>,
  server_info: Mutex<ServerInfo>,
  waiter: Waiter,
  tokens: TokenGenerator,
  events: broadcast::Sender<Event>,
  peers: Arc<PeerConnections>,
  distributed: DistributedNetwork,
  responder: SearchResponder,
  endpoints: endpoint::Endpoints,
  transfers: Arc<Transfers>,
  searches: Searches,
  folder_requests: FolderRequests,
  /// the listener kept from the last session, between sessions
  kept_listener: Mutex<Option<Listener>>,
  /// how many connections peers opened to our listener so far
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
  last_end: Option<SessionEnd>,
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
  /// A disconnected client with `options`. Fails when the options do not validate. The client
  /// starts tasks of its own, so it must be made inside a tokio runtime; they stop when it is
  /// shut down or the last clone is dropped.
  pub fn new(options: ClientOptions) -> Result<Self, ClientError> {
    Self::with_tokens(options, TokenGenerator::new(None))
  }

  /// [`Client::new`] whose tokens count from `first`, for tests that need to know them
  #[cfg(test)]
  pub(crate) fn counting_from(options: ClientOptions, first: u32) -> Result<Self, ClientError> {
    Self::with_tokens(options, TokenGenerator::new(Some(first)))
  }

  fn with_tokens(options: ClientOptions, tokens: TokenGenerator) -> Result<Self, ClientError> {
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
      transfers: Arc::new(Transfers::new()),
      searches: Searches::default(),
      folder_requests: FolderRequests::default(),
      options: RwLock::new(Arc::new(options)),
      state: watch::Sender::new(ClientState::Disconnected),
      session: Mutex::new(Session::default()),
      state_lock: tokio::sync::Mutex::new(()),
      kept_listener: Mutex::new(None),
      server_info: Mutex::new(ServerInfo::default()),
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

  /// where the client is in its server session
  pub fn state(&self) -> ClientState {
    *self.inner.state.borrow()
  }

  /// How many connections peers opened to our listener since the client was made, followed as
  /// they come. Every one, a peer answering a connection request we had the server relay
  /// included, shows that other users can reach the listener.
  pub fn inbound_connections(&self) -> watch::Receiver<u64> {
    self.inner.inbound.subscribe()
  }

  /// The server session's number, state, username and the latest end, read together.
  pub fn session(&self) -> SessionStatus {
    let session = self.inner.session();
    SessionStatus {
      number: session.number,
      state: session.stage.state(),
      reached: session.stage.reached(),
      username: session.stage.username().map(str::to_owned),
      last_end: session.last_end.clone(),
    }
  }

  /// the server address as given to `connect`, while connected
  pub fn server_address(&self) -> Option<String> {
    let session = self.inner.session();
    session.stage.link().map(|link| link.address.clone())
  }

  /// The server endpoint the address resolved to, while connected. A name a proxy resolved has
  /// none here: the server's address is then the proxy's to know.
  pub fn server_endpoint(&self) -> Option<SocketAddr> {
    let session = self.inner.session();
    session.stage.link().and_then(|link| link.ip_endpoint)
  }

  /// our username, while logged in
  pub fn username(&self) -> Option<String> {
    let session = self.inner.session();
    session.stage.username().map(str::to_owned)
  }

  /// whether the session is logged in as `username` with `password`, which a password changed
  /// on the server while logged in becomes
  pub fn logged_in_as(&self, username: &str, password: &str) -> bool {
    let session = self.inner.session();
    match &session.stage {
      Stage::LoggedIn {
        username: current,
        password: current_password,
        ..
      } => current == username && current_password.expose() == password,
      _ => false,
    }
  }

  /// the tuning values the server has sent so far
  pub fn server_info(&self) -> ServerInfo {
    lock(&self.inner.server_info).clone()
  }

  /// the options in effect
  pub fn options(&self) -> Arc<ClientOptions> {
    self.inner.options()
  }

  /// a receiver of every event from now on
  pub fn subscribe(&self) -> broadcast::Receiver<Event> {
    self.inner.events.subscribe()
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

  /// every user with an established message connection, and its address
  pub fn message_connections(&self) -> Vec<(String, SocketAddr)> {
    self.inner.peers.message_connections()
  }

  /// the address of the established message connection our requests to `username` go on, if
  /// there is one
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

  pub(crate) fn distributed(&self) -> &DistributedNetwork {
    &self.inner.distributed
  }

  pub(crate) fn responder(&self) -> &SearchResponder {
    &self.inner.responder
  }

  pub(crate) fn transfers(&self) -> &Arc<Transfers> {
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

  pub(crate) fn check_not_shut_down(&self) -> Result<(), ClientError> {
    self.inner.check_not_shut_down()
  }

  /// Our username, for the operation `verb` that needs it; fails with `InvalidOperation`
  /// naming `verb` while not logged in.
  pub(crate) fn own_username(&self, verb: &str) -> Result<String, ClientError> {
    self.require_logged_in(verb)?;
    // a disconnect between the two reads leaves no username
    self
      .username()
      .ok_or_else(|| request::not_logged_in(verb, self.state()))
  }

  /// writes one message to the server and waits until it is on the socket
  pub async fn write_server_message(&self, message: &ServerRequest) -> Result<(), ClientError> {
    self
      .write_server_messages(std::slice::from_ref(message))
      .await
  }

  /// Writes several messages to the server as one write, so they reach it together, and waits
  /// until they are on the socket.
  pub(crate) async fn write_server_messages(
    &self,
    messages: &[ServerRequest],
  ) -> Result<(), ClientError> {
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

  /// Disconnects and stops the client for good: every transfer is canceled, every wait is
  /// canceled, every connection to another user is closed and every task tied to the client
  /// stops. Later calls fail with `Shutdown`.
  pub async fn shutdown(&self) {
    self
      .disconnect(Some("Client is shutting down"), Some(ClientError::Shutdown))
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
  fn options(&self) -> Arc<ClientOptions> {
    self
      .options
      .read()
      .unwrap_or_else(std::sync::PoisonError::into_inner)
      .clone()
  }

  fn set_options(&self, options: Arc<ClientOptions>) {
    *self
      .options
      .write()
      .unwrap_or_else(std::sync::PoisonError::into_inner) = options;
  }

  fn session(&self) -> MutexGuard<'_, Session> {
    lock(&self.session)
  }

  fn server(&self) -> Result<MessageConn, ClientError> {
    let session = self.session();
    match session.stage.link() {
      Some(link) => Ok(link.server.clone()),
      None => Err(ClientError::InvalidOperation(format!(
        "Cannot send on a connection that is not connected (state: {})",
        session.stage.state()
      ))),
    }
  }

  fn check_not_shut_down(&self) -> Result<(), ClientError> {
    if self.lifetime.is_cancelled() {
      return Err(ClientError::Shutdown);
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
    error: Option<ClientError>,
  ) -> Result<Released, ClientState> {
    let released = session.stage.advance(edge)?;
    let state = session.stage.state();
    let previous_state = self.state.send_replace(state);
    tracing::debug!(%previous_state, %state, message, "client state changed");
    self.emit(Event::StateChanged {
      previous_state,
      state,
      message: message.clone(),
      error: error.clone(),
    });
    match &session.stage {
      Stage::Connecting { .. } => session.number += 1,
      Stage::Connected { .. } => self.emit(Event::Connected {
        session: session.number,
      }),
      Stage::LoggedIn { username, .. } => self.emit(Event::LoggedIn {
        session: session.number,
        username: username.clone(),
      }),
      Stage::Disconnected { .. } => {
        let at = std::time::SystemTime::now();
        session.last_end = Some(SessionEnd {
          session: session.number,
          message: message.clone(),
          error: error.clone(),
          at,
        });
        self.emit(Event::Disconnected {
          session: session.number,
          message,
          error,
          at,
        });
      }
      Stage::LoggingIn { .. } | Stage::Disconnecting { .. } => {}
    }
    Ok(released)
  }

  fn merge_server_info(&self, merge: impl FnOnce(&mut ServerInfo)) {
    let info = {
      let mut info = lock(&self.server_info);
      merge(&mut info);
      info.clone()
    };
    self.emit(Event::ServerInfoReceived(info));
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
        .disconnect("Client is shutting down", Some(ClientError::Shutdown));
    }
  }
}
