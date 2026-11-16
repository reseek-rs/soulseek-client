//! The client's searches. A search goes to the server in the form its scope needs as soon as it is
//! called; peers that have matches answer over a message connection, naming the search by its
//! token. A search takes the responses its options let through until none came for a while or it
//! has enough. Disconnecting from the server cancels every search.

mod responses;
mod state;

pub(crate) use state::Options;
pub use state::{Outcome, Search, State};

use std::collections::{HashMap, HashSet};
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use soulseek_proto::peer::{File, SearchResponse};
use soulseek_proto::server::{FileSearchRequest, RoomSearch, UserSearch, WishlistSearch};
use soulseek_proto::{SearchQuery, ServerRequest};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::Instrument;

use crate::client::{Client, failed, non_blank};
use crate::error::Error;
use crate::lock;
use crate::token::TokenGenerator;
use responses::Counts;

/// Recipients of a search.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Scope {
  /// Every user, through the distributed tree.
  Network,
  /// Every user, from the wishlist.
  ///
  /// The server allows a wishlist search only at its wishlist interval.
  Wishlist,
  /// Members of the named rooms, with one request per room.
  Rooms(Vec<String>),
  /// The named users.
  Users(Vec<String>),
}

impl Scope {
  fn validate(&self) -> Result<(), Error> {
    match self {
      Self::Network | Self::Wishlist => Ok(()),
      Self::Rooms(rooms) if rooms.is_empty() => Err(Error::InvalidArgument(
        "a room search needs at least one room".into(),
      )),
      Self::Rooms(rooms) => rooms
        .iter()
        .try_for_each(|room| non_blank("room name", room)),
      Self::Users(usernames) if usernames.is_empty() => Err(Error::InvalidArgument(
        "a user search needs at least one username".into(),
      )),
      Self::Users(usernames) => usernames
        .iter()
        .try_for_each(|username| non_blank("username", username)),
    }
  }

  /// the requests that send `text` with `token` to this scope
  fn requests(&self, token: u32, text: &str) -> Vec<ServerRequest> {
    let query = text.to_owned();
    match self {
      Self::Network => vec![ServerRequest::FileSearch(FileSearchRequest {
        token,
        query,
      })],
      Self::Wishlist => vec![ServerRequest::WishlistSearch(WishlistSearch {
        token,
        query,
      })],
      Self::Rooms(rooms) => rooms
        .iter()
        .map(|room| {
          ServerRequest::RoomSearch(RoomSearch {
            room: room.clone(),
            token,
            query: query.clone(),
          })
        })
        .collect(),
      Self::Users(usernames) => usernames
        .iter()
        .map(|username| {
          ServerRequest::UserSearch(UserSearch {
            username: username.clone(),
            token,
            query: query.clone(),
          })
        })
        .collect(),
    }
  }
}

/// One search while it runs
struct Running {
  token: u32,
  /// the text as it is sent
  text: String,
  query: SearchQuery,
  scope: Scope,
  options: Options,
  counts: Mutex<Counts>,
  /// where the responses taken go, until the search has ended; a response is counted and handed
  /// over under this lock, so a search that ends has handed over every response it took
  responses: Mutex<Option<mpsc::Sender<SearchResponse>>>,
  /// set once the counts hold how the search ends, to wake the search
  ended: watch::Sender<bool>,
}

impl Running {
  fn snapshot(&self, counts: &Counts) -> Search {
    Search {
      text: self.text.clone(),
      query: self.query.clone(),
      scope: self.scope.clone(),
      token: self.token,
      state: counts.state,
      response_count: counts.responses,
      file_count: counts.files,
      locked_file_count: counts.locked_files,
    }
  }

  /// Decides with the locked `counts` that the search ends with `outcome`, unless how it ends
  /// was decided already, and returns how it ends. Every end is decided under the counts' lock,
  /// so no response is taken once it is and a response taken first counts against the timeout.
  fn decide(&self, counts: &mut Counts, outcome: Outcome) -> Outcome {
    let decided = *counts.outcome.get_or_insert(outcome);
    self.ended.send_replace(true);
    decided
  }

  /// [`Running::decide`], taking the counts' lock
  fn end(&self, outcome: Outcome) -> Outcome {
    self.decide(&mut lock(&self.counts), outcome)
  }

  /// Waits for the search to end, by its timeout, its longest duration, a limit a response
  /// reached, a disconnect or `cancel`, then ends it.
  async fn run(&self, cancel: &CancellationToken) {
    let mut ended = self.ended.subscribe();
    let latest = self.options.maximum_duration.map(deadline_after);
    let outcome = loop {
      let deadline = lock(&self.counts).deadline;
      tokio::select! {
        biased;
        () = cancel.cancelled() => break self.end(Outcome::Canceled),
        // decided already, so this returns how
        () = was_ended(&mut ended) => break self.end(Outcome::Canceled),
        () = at(latest) => break self.end(Outcome::MaximumDurationReached),
        () = sleep_until(deadline) => {
          let mut counts = lock(&self.counts);
          // a response taken meanwhile moved the deadline
          if counts.deadline <= Instant::now() {
            break self.decide(&mut counts, Outcome::TimedOut);
          }
        }
      }
    };
    self.finish(outcome);
    let snapshot = self.snapshot(&lock(&self.counts));
    tracing::info!(
      state = %snapshot.state,
      responses = snapshot.response_count,
      files = snapshot.file_count,
      "search completed"
    );
  }

  /// Completes the search with `outcome` and ends its responses. A response being taken is
  /// handed over first.
  fn finish(&self, outcome: Outcome) {
    let ended = lock(&self.responses).take();
    self.set_state(State::Completed(outcome));
    drop(ended);
  }

  /// Moves the search to `state`. Only the search's own task moves it, along the edges
  /// [`State::leads_to`] allows.
  fn set_state(&self, state: State) {
    let mut counts = lock(&self.counts);
    let previous = counts.state;
    assert!(
      previous.leads_to(state),
      "a search does not move from {previous} to {state}"
    );
    counts.state = state;
    tracing::debug!(%previous, %state, "search state changed");
  }
}

/// What the client keeps for its searches
#[derive(Default)]
pub(crate) struct Searches {
  running: Mutex<HashMap<u32, Arc<Running>>>,
  /// tokens handed out for searches not started yet, which no other search and no ticket takes
  /// meanwhile; locked after `running` when both are held
  reserved: Mutex<HashSet<u32>>,
}

/// removes a search from the table when it ends
struct Registration {
  client: crate::client::WeakClient,
  token: u32,
}

impl Drop for Registration {
  fn drop(&mut self) {
    if let Some(client) = self.client.upgrade() {
      client.searches().running().remove(&self.token);
    }
  }
}

impl Searches {
  fn running(&self) -> MutexGuard<'_, HashMap<u32, Arc<Running>>> {
    lock(&self.running)
  }

  fn get(&self, token: u32) -> Option<Arc<Running>> {
    self.running().get(&token).cloned()
  }

  /// whether a search of the client's with `token` is running
  pub(crate) fn is_running(&self, token: u32) -> bool {
    self.running().contains_key(&token)
  }

  /// whether any search of the client's is running
  pub(crate) fn any_running(&self) -> bool {
    !self.running().is_empty()
  }

  /// Runs `decide` with the running and reserved searches held, telling it whether a token is
  /// one of theirs, so no search takes a token while it decides on one
  pub(crate) fn holding_tokens<R>(&self, decide: impl FnOnce(&dyn Fn(u32) -> bool) -> R) -> R {
    let running = self.running();
    let reserved = lock(&self.reserved);
    decide(&|token| running.contains_key(&token) || reserved.contains(&token))
  }

  /// Draws a token no search runs or is reserved under and `ticketed` does not claim, and
  /// reserves it
  fn reserve(&self, tokens: &TokenGenerator, ticketed: impl Fn(u32) -> bool) -> u32 {
    let running = self.running();
    let mut reserved = lock(&self.reserved);
    let token = tokens.next_free(|token| {
      running.contains_key(&token) || reserved.contains(&token) || ticketed(token)
    });
    reserved.insert(token);
    token
  }

  /// Takes the `reserved` token, or a fresh one, for `search`, in one step so two callers
  /// cannot both take one token. A fresh token is never one `ticketed` says is a connection
  /// request's ticket: the search sends it to every user.
  fn register(
    &self,
    tokens: &TokenGenerator,
    reserved: Option<Token>,
    ticketed: impl Fn(u32) -> bool,
    search: impl FnOnce(u32) -> Running,
  ) -> Arc<Running> {
    let mut running = self.running();
    let token = match &reserved {
      Some(reserved) => reserved.token,
      None => {
        let reserved = lock(&self.reserved);
        tokens.next_free(|token| {
          running.contains_key(&token) || reserved.contains(&token) || ticketed(token)
        })
      }
    };
    let search = Arc::new(search(token));
    running.insert(token, search.clone());
    // the reservation lets go only once the search holds the token, under the same lock
    drop(reserved);
    search
  }

  /// cancels every search, for when the server session ends
  pub(crate) fn cancel_all(&self) {
    let running: Vec<_> = self.running().values().cloned().collect();
    if !running.is_empty() {
      tracing::debug!(count = running.len(), "searches canceled");
    }
    for search in running {
      search.end(Outcome::Canceled);
    }
  }
}

/// Search token reserved with [`Client::reserve_search_token`].
///
/// The token is kept from other searches and from connection requests until the search started
/// with it ends, or until this value is dropped unused.
pub struct Token {
  client: Client,
  token: u32,
}

impl std::fmt::Debug for Token {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_tuple("Token").field(&self.token).finish()
  }
}

impl Token {
  /// Returns the reserved token.
  ///
  /// The search started with this value goes out under it.
  pub fn token(&self) -> u32 {
    self.token
  }
}

impl Drop for Token {
  fn drop(&mut self) {
    lock(&self.client.searches().reserved).remove(&self.token);
  }
}

/// Search that has not started yet.
///
/// Made by [`Client::search`]. Its setters say who the search goes to and what bounds it, and
/// awaiting it sends it and returns its [`Responses`].
///
/// # Errors
///
/// Awaiting it returns [`Error::InvalidArgument`] if the text has no term to search for (after
/// dropping the terms of one character when asked to), if the scope names no room or user or
/// names a blank one, or if the timeout is zero. Returns [`Error::Shutdown`] if the client has
/// been shut down. Returns [`Error::InvalidOperation`] if the client is not logged in. Returns
/// [`Error::Timeout`], or an [`Error::Operation`] wrapping the cause, if the request could not
/// be sent.
#[must_use = "a search is sent when it is awaited"]
pub struct Request<'a> {
  client: &'a Client,
  text: String,
  verbatim: bool,
  scope: Scope,
  options: Options,
  token: Option<Token>,
}

impl Request<'_> {
  /// Sets who the search goes to.
  ///
  /// Defaults to [`Scope::Network`].
  pub fn scope(mut self, scope: Scope) -> Self {
    self.scope = scope;
    self
  }

  /// Sends the text exactly as given.
  ///
  /// By default the search sends the terms and exclusions it parsed from the text, in its own
  /// order and spacing.
  pub fn verbatim(mut self) -> Self {
    self.verbatim = true;
    self
  }

  /// Sets the token the search goes out under.
  ///
  /// `token` is one reserved with [`Client::reserve_search_token`].
  pub fn token(mut self, token: Token) -> Self {
    self.token = Some(token);
    self
  }

  /// Sets the time without a response taken after which the search ends.
  ///
  /// Defaults to 15 seconds.
  pub fn timeout(mut self, timeout: Duration) -> Self {
    self.options.search_timeout = timeout;
    self
  }

  /// Sets the time after the search was sent at which it ends.
  ///
  /// The search ends then however many responses still come, as
  /// [`Outcome::MaximumDurationReached`]. If unset, only the timeout and the limits end it.
  /// Unset by default.
  pub fn maximum_duration(mut self, duration: impl Into<Option<Duration>>) -> Self {
    self.options.maximum_duration = duration.into();
    self
  }

  /// Sets whether to take one response from each user and drop any further one.
  ///
  /// Defaults to `true`.
  pub fn one_response_per_user(mut self, one: bool) -> Self {
    self.options.one_response_per_user = one;
    self
  }

  /// Sets the number of responses taken at which the search ends.
  ///
  /// Defaults to 250.
  pub fn response_limit(mut self, limit: u32) -> Self {
    self.options.response_limit = limit;
    self
  }

  /// Sets the number of downloadable files at which the search ends.
  ///
  /// Counts the files the responses taken hold. Defaults to 25000.
  pub fn file_limit(mut self, limit: u32) -> Self {
    self.options.file_limit = limit;
    self
  }

  /// Sets whether to apply the filters.
  ///
  /// If unset, every response is taken. Defaults to `true`.
  pub fn filter_responses(mut self, filter: bool) -> Self {
    self.options.filter_responses = filter;
    self
  }

  /// Sets the fewest files a response may hold, locked ones included.
  ///
  /// A response with fewer is dropped. Defaults to 1.
  pub fn minimum_response_file_count(mut self, count: u32) -> Self {
    self.options.minimum_response_file_count = count;
    self
  }

  /// Sets the most uploads a responding peer may have queued.
  ///
  /// A response from a peer with more is dropped. A peer that does not know its queue length,
  /// by [`peer_count`](crate::peer_count), is kept. Defaults to [`u32::MAX`].
  pub fn maximum_peer_queue_length(mut self, length: u32) -> Self {
    self.options.maximum_peer_queue_length = length;
    self
  }

  /// Sets the slowest upload speed a responding peer may have, in bytes per second.
  ///
  /// A response from a slower peer is dropped. Defaults to 0.
  pub fn minimum_peer_upload_speed(mut self, speed: u32) -> Self {
    self.options.minimum_peer_upload_speed = speed;
    self
  }

  /// Sets whether to leave terms of one character out of the query.
  ///
  /// Defaults to `false`.
  pub fn remove_single_character_terms(mut self, remove: bool) -> Self {
    self.options.remove_single_character_search_terms = remove;
    self
  }

  /// Sets a filter that keeps a response only when it returns `true`.
  ///
  /// Runs after the other filters. Unset by default.
  pub fn response_filter(
    mut self,
    keep: impl Fn(&SearchResponse) -> bool + Send + Sync + 'static,
  ) -> Self {
    self.options.response_filter = Some(Arc::new(keep));
    self
  }

  /// Sets a filter applied to both file lists of a response.
  ///
  /// The response is then counted again. Unset by default.
  pub fn file_filter(mut self, keep: impl Fn(&File) -> bool + Send + Sync + 'static) -> Self {
    self.options.file_filter = Some(Arc::new(keep));
    self
  }
}

impl<'a> IntoFuture for Request<'a> {
  type Output = Result<Responses, Error>;
  type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

  fn into_future(self) -> Self::IntoFuture {
    Box::pin(self.client.start_search(
      self.text,
      self.verbatim,
      self.scope,
      self.options,
      self.token,
    ))
  }
}

/// shows the search; the filters in its options have nothing to print
impl std::fmt::Debug for Request<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Request")
      .field("text", &self.text)
      .field("scope", &self.scope)
      .field("options", &self.options)
      .field("token", &self.token.as_ref().map(Token::token))
      .finish()
  }
}

/// Responses of a running search.
///
/// Made by awaiting a [`Request`]. It yields each response as the search takes it and ends when
/// the search does: at its timeout, at a limit, or when the server session ends. Dropping it
/// cancels the search.
pub struct Responses {
  search: Arc<Running>,
  responses: mpsc::Receiver<SearchResponse>,
  /// cancels the search when this is dropped
  _cancel: DropGuard,
}

impl Responses {
  /// Returns the next response, or `None` once the search has ended.
  ///
  /// # Cancel safety
  ///
  /// This method is cancel safe. A response not returned stays for the next call.
  pub async fn next(&mut self) -> Option<SearchResponse> {
    self.responses.recv().await
  }

  /// Returns the search as it is now.
  ///
  /// Once the responses have ended, its state says how the search ended.
  pub fn search(&self) -> Search {
    self.search.snapshot(&lock(&self.search.counts))
  }

  /// Waits for the search to end and returns it with every response not yet taken out.
  ///
  /// The responses are in the order they arrived.
  pub async fn all(mut self) -> (Search, Vec<SearchResponse>) {
    let mut responses = Vec::new();
    while let Some(response) = self.next().await {
      responses.push(response);
    }
    (self.search(), responses)
  }
}

impl Stream for Responses {
  type Item = SearchResponse;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<SearchResponse>> {
    self.responses.poll_recv(cx)
  }
}

impl std::fmt::Debug for Responses {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Responses")
      .field("search", &self.search())
      .finish_non_exhaustive()
  }
}

impl Client {
  /// Reserves a token for a search to start later.
  ///
  /// The search is started with [`Request::token`]. The reservation serves an
  /// application that records the search first: no search runs under the token, no connection
  /// request's ticket holds it, and neither takes it while it is reserved or the search started
  /// with it runs.
  pub fn reserve_search_token(&self) -> Token {
    let token = self
      .searches()
      .reserve(self.tokens(), |token| self.ticket_held(token));
    Token {
      client: self.clone(),
      token,
    }
  }

  /// Searches for files shared by other users.
  ///
  /// Returns the search to set up and await. See [`Request`] for what bounds it and for the
  /// errors awaiting it returns.
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use std::time::Duration;
  ///
  /// use soulseek_client::{Client, Error};
  ///
  /// # async fn run(client: Client) -> Result<(), Error> {
  /// let mut responses = client
  ///   .search("artist album")
  ///   .timeout(Duration::from_secs(10))
  ///   .await?;
  /// while let Some(response) = responses.next().await {
  ///   println!("{} has {} files", response.username, response.files.len());
  /// }
  /// println!("ended as {}", responses.search().state);
  /// # Ok(())
  /// # }
  /// ```
  pub fn search(&self, text: impl Into<String>) -> Request<'_> {
    Request {
      client: self,
      text: text.into(),
      verbatim: false,
      scope: Scope::Network,
      options: Options::default(),
      token: None,
    }
  }

  /// checks and sends a search, and starts the task that ends it
  async fn start_search(
    &self,
    text: String,
    verbatim: bool,
    scope: Scope,
    options: Options,
    token: Option<Token>,
  ) -> Result<Responses, Error> {
    let (search_text, query) = checked(text, verbatim, &scope, &options)?;
    self.require_logged_in(&format!("search for {search_text}"))?;
    // a search takes no more responses than its limit, so none ever waits for room
    let room = (options.response_limit as usize).clamp(1, Semaphore::MAX_PERMITS);
    let (taken, responses) = mpsc::channel(room);
    let ticketed = |token| self.ticket_held(token);
    let search = self
      .searches()
      .register(self.tokens(), token, ticketed, |token| Running {
        token,
        text: search_text.clone(),
        query,
        scope,
        options,
        counts: Mutex::new(Counts::default()),
        responses: Mutex::new(Some(taken)),
        ended: watch::channel(false).0,
      });
    let registration = Registration {
      client: self.downgrade(),
      token: search.token,
    };
    let span = tracing::info_span!("search", token = search.token);
    self
      .send_search(&search, &search_text)
      .instrument(span.clone())
      .await?;
    let cancel = CancellationToken::new();
    tokio::spawn({
      let (search, cancel) = (search.clone(), cancel.clone());
      async move {
        search.run(&cancel).await;
        drop(registration);
      }
      .instrument(span)
    });
    Ok(Responses {
      search,
      responses,
      _cancel: cancel.drop_guard(),
    })
  }

  /// Sends the search and has it take responses. An error ends it.
  async fn send_search(&self, search: &Arc<Running>, text: &str) -> Result<(), Error> {
    // another task can end the search while it is being sent: a disconnect, or, once it takes
    // responses, a limit a response reached
    let mut ended = search.ended.subscribe();
    let requests = search.scope.requests(search.token, text);
    // an answer can come back as soon as the request is on its way, before the write is seen to
    // end, so the search takes responses from here
    search.set_state(State::InProgress);
    let sent = tokio::select! {
      biased;
      // ended meanwhile, by a disconnect or by a limit an early answer reached
      () = was_ended(&mut ended) => None,
      sent = self.write_server_messages(&requests) => Some(sent),
    };
    match sent {
      Some(Ok(())) => {
        tracing::info!(text, scope = ?search.scope, "search sent");
        // the timeout counts from the send, however long the write waited
        lock(&search.counts).deadline = deadline_after(search.options.search_timeout);
      }
      Some(Err(error)) => {
        if search.end(Outcome::Errored) == Outcome::Errored {
          let error = match error {
            error @ Error::Timeout(_) => error,
            error => failed(&format!("search for {text}"), error),
          };
          search.finish(Outcome::Errored);
          tracing::info!(%error, "search ended");
          self.searches().running().remove(&search.token);
          return Err(error);
        }
        // a limit an early answer reached ended it first; its task ends with that
        tracing::debug!(%error, "search ended before its request failed to send");
      }
      // its task ends at once with how it ended
      None => tracing::debug!("search ended while its request was sent"),
    }
    Ok(())
  }

  /// Sends a search for `text` with `token` to everyone in `scope`, without waiting for
  /// responses. A search of several users is one request per user, written together.
  #[cfg(feature = "testing")]
  pub(crate) async fn send_search_request(
    &self,
    scope: &Scope,
    token: u32,
    text: &str,
  ) -> Result<(), Error> {
    non_blank("search text", text)?;
    scope.validate()?;
    let verb = format!("send search request with token {token}");
    self.require_logged_in(&verb)?;
    self
      .write_server_messages(&scope.requests(token, text))
      .await
      .map_err(|error| failed(&verb, error))?;
    tracing::debug!(token, ?scope, "search request sent");
    Ok(())
  }

  /// hands `response` to the search it names, which takes it if its options let it through
  pub(crate) fn search_response(&self, response: SearchResponse) {
    let Some(search) = self.searches().get(response.token) else {
      tracing::trace!(
        token = response.token,
        "search response for no active search, dropped"
      );
      return;
    };
    let response = match responses::filter(&search.options, response) {
      Ok(response) => response,
      Err(reason) => {
        return tracing::trace!(token = search.token, reason, "search response dropped");
      }
    };
    // taking a response and handing it over are one step, so a search that ends waits for the
    // responses it took
    let responses = lock(&search.responses);
    let Some(taken) = responses.as_ref() else {
      return tracing::trace!(
        token = search.token,
        "search response after the search ended, dropped"
      );
    };
    let response = match responses::accept(&search, response) {
      Ok(response) => response,
      Err(reason) => {
        return tracing::trace!(token = search.token, reason, "search response dropped");
      }
    };
    tracing::trace!(
      username = response.username,
      files = response.files.len(),
      "search response taken"
    );
    // the channel holds as many responses as the search takes; a full one means the caller let
    // go of them
    if taken.try_send(response).is_err() {
      tracing::trace!(
        token = search.token,
        "search response not handed over; nobody reads them"
      );
    }
  }
}

/// the instant `duration` from now
pub(super) fn deadline_after(duration: Duration) -> Instant {
  crate::later(Instant::now(), duration)
}

/// resolves at `instant`, or never without one
async fn at(instant: Option<Instant>) {
  match instant {
    Some(instant) => sleep_until(instant).await,
    None => std::future::pending().await,
  }
}

/// Resolves once how the search ends was decided, by another task (a disconnect, or a limit a
/// response reached) or by the search's own. The value's read guard is let go before this
/// returns.
async fn was_ended(ended: &mut watch::Receiver<bool>) {
  if ended.wait_for(|ended| *ended).await.is_err() {
    // the search holds the sender, so this cannot end without a value
    std::future::pending::<()>().await;
  }
}

/// The text a search sends and its terms, once the search is known to be one that can be sent:
/// `text` as given when `verbatim`, else as its terms and exclusions spell it
fn checked(
  text: String,
  verbatim: bool,
  scope: &Scope,
  options: &Options,
) -> Result<(String, SearchQuery), Error> {
  let (text, query) = if verbatim {
    non_blank("search text", &text)?;
    let query = SearchQuery::parse(&text);
    (text, query)
  } else {
    let query = search_query(&text, options)?;
    (query.search_text(), query)
  };
  scope.validate()?;
  if options.search_timeout.is_zero() {
    return Err(Error::InvalidArgument(
      "the search timeout must be greater than zero".into(),
    ));
  }
  Ok((text, query))
}

/// The query `text` makes, with short terms dropped when the options say so; it needs a term
fn search_query(text: &str, options: &Options) -> Result<SearchQuery, Error> {
  let mut query = SearchQuery::parse(text);
  if query.search_text().trim().is_empty() {
    return Err(Error::InvalidArgument(
      "the search text must not be empty".into(),
    ));
  }
  if query.terms.is_empty() {
    return Err(Error::InvalidArgument(
      "the search must contain at least one non-exclusion term".into(),
    ));
  }
  if options.remove_single_character_search_terms {
    query.terms.retain(|term| term.chars().count() > 1);
    if query.terms.is_empty() {
      return Err(Error::InvalidArgument(
        "the search must contain at least one non-exclusion term with length greater than 1".into(),
      ));
    }
  }
  Ok(query)
}

#[cfg(test)]
pub(crate) mod tests {

  use soulseek_proto::PeerConnectionType;

  use super::*;
  use crate::config::Config;

  fn search(token: u32) -> Running {
    Running {
      token,
      text: "daft punk".into(),
      query: search_query("daft punk", &Options::default()).unwrap(),
      scope: Scope::Network,
      options: Options::default(),
      counts: Mutex::new(Counts::default()),
      responses: Mutex::new(None),
      ended: watch::channel(false).0,
    }
  }

  /// a search running under `token` until the returned guard is dropped
  pub(crate) fn running(client: &Client, token: u32) -> impl Sized + '_ {
    let searches = client.searches();
    lock(&searches.reserved).insert(token);
    let reserved = Token {
      client: client.clone(),
      token,
    };
    let tokens = TokenGenerator::new(None);
    searches.register(&tokens, Some(reserved), |_| false, search);
    Registration {
      client: client.downgrade(),
      token,
    }
  }

  #[tokio::test]
  async fn a_fresh_search_token_is_never_a_connection_requests_ticket() {
    let client = Client::new(Config {
      listener: false,
      ..Config::default()
    })
    .unwrap();
    let ticket = client.issue_ticket("bob", PeerConnectionType::Peer, Duration::from_secs(10));
    let held = ticket.token();
    let ticketed = |token| client.ticket_held(token);
    // the count reaching the ticket passes it by
    let counted = TokenGenerator::new(Some(held));
    let search = client.searches().register(&counted, None, ticketed, search);
    assert_ne!(search.token, held);
  }

  #[tokio::test]
  async fn a_reserved_token_is_clear_of_tickets_and_kept_from_them_and_other_searches() {
    let client = Client::counting_from(
      Config {
        listener: false,
        ..Config::default()
      },
      100,
    )
    .unwrap();
    let timeout = Duration::from_secs(10);
    // a ticket drawing `token`, then `instead`
    let ticket_on = |token: u32, instead: u32| {
      let mut draws = [token, instead].into_iter();
      client.issue_ticket_from("bob", PeerConnectionType::Peer, timeout, || {
        draws.next().unwrap()
      })
    };
    // a ticket holds the next token the count gives
    let ticket = ticket_on(100, 1);
    let reserved = client.reserve_search_token();
    let token = reserved.token();
    assert_eq!(token, 101, "the ticket's token is passed by");
    assert_eq!(
      ticket_on(token, 7).token(),
      7,
      "a ticket passes a reserved token by"
    );
    let searches = client.searches();
    let counted = TokenGenerator::new(Some(token));
    let other = searches.register(&counted, None, |_| false, search);
    assert_ne!(other.token, token, "another search passes it by");
    let search = searches.register(&counted, Some(reserved), |_| false, search);
    assert_eq!(search.token, token);
    let started = Registration {
      client: client.downgrade(),
      token,
    };
    assert_eq!(ticket_on(token, 8).token(), 8, "nor while the search runs");
    drop(started);
    assert_eq!(
      ticket_on(token, 9).token(),
      token,
      "free once the search ended"
    );
    drop(ticket);
    // a reservation that dropped unused leaves its token free
    let unused = client.reserve_search_token().token();
    assert_eq!(ticket_on(unused, 10).token(), unused);
  }

  #[test]
  fn short_terms_are_kept_unless_the_options_drop_them() {
    let options = Options::default();
    assert_eq!(search_query("a b", &options).unwrap().search_text(), "a b");
    let drop_short = Options {
      remove_single_character_search_terms: true,
      ..Options::default()
    };
    let query = search_query("a  daft\tpunk -live", &drop_short).unwrap();
    assert_eq!(query.search_text(), "daft punk -live");
    assert!(search_query("a b", &drop_short).is_err());
    assert!(search_query("-live -bootleg", &options).is_err());
    assert!(search_query("   ", &options).is_err());
  }

  #[test]
  fn a_verbatim_request_sends_its_text_as_given() {
    let text = "-live  \"daft punk\" -live";
    let options = Options::default();
    let sent = |text: &str, verbatim| checked(text.into(), verbatim, &Scope::Network, &options);
    assert_ne!(sent(text, false).unwrap().0, text);
    assert_eq!(sent(text, true).unwrap().0, text);
    // only exclusions is a search the server takes, so it is not refused here
    assert!(sent("-live", true).is_ok());
    assert!(sent("  ", true).is_err());
  }

  #[test]
  fn a_deadline_too_far_off_to_name_is_still_one() {
    assert!(deadline_after(Duration::MAX) > Instant::now() + Duration::from_secs(3600));
  }

  #[test]
  fn a_request_is_checked_before_anything_is_sent() {
    let refused = |text: &str, scope: Scope, options: Options| {
      matches!(
        checked(text.into(), false, &scope, &options),
        Err(Error::InvalidArgument(_))
      )
    };
    let options = Options::default;
    let (text, _) = checked("daft punk".into(), false, &Scope::Network, &options()).unwrap();
    assert_eq!(text, "daft punk");
    assert!(refused("   ", Scope::Network, options()), "no text");
    assert!(
      refused("-live -bootleg", Scope::Network, options()),
      "only exclusions"
    );
    let drop_short = Options {
      remove_single_character_search_terms: true,
      ..options()
    };
    assert!(
      refused("a b", Scope::Network, drop_short),
      "only short terms"
    );
    for scope in [
      Scope::Rooms(Vec::new()),
      Scope::Rooms(vec![" ".into()]),
      Scope::Users(Vec::new()),
      Scope::Users(vec!["alice".into(), "".into()]),
    ] {
      assert!(refused("daft punk", scope.clone(), options()), "{scope:?}");
    }
    let no_wait = Options {
      search_timeout: Duration::ZERO,
      ..options()
    };
    assert!(refused("daft punk", Scope::Network, no_wait), "no timeout");
  }

  #[test]
  fn a_user_search_is_one_request_per_user() {
    let scope = Scope::Users(vec!["alice".into(), "bob".into()]);
    let requests = scope.requests(7, "daft punk");
    assert_eq!(requests.len(), 2);
  }
}

/// Something that happened to a search of another user's that the client answered.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// The client sent its results for another user's search.
  ResponseDelivered(Delivery),
  /// Results the client kept for an unreachable searcher were given up.
  ResponseDeliveryFailed(Delivery),
}

/// The client's answer to another user's search, and whom it was for.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Delivery {
  /// User who searched.
  pub username: String,
  /// Token of the searcher.
  pub token: u32,
  /// Query as the searcher sent it.
  pub query: String,
  /// What the client sent.
  pub reply: std::sync::Arc<crate::handler::SearchReply>,
}
