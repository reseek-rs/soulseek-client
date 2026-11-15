//! The client's searches. A search goes to the server in the form its scope needs as soon as it is
//! called; peers that have matches answer over a message connection, naming the search by its
//! token. A search takes the responses its options let through until none came for a while or it
//! has enough. Disconnecting from the server cancels every search.

mod responses;
mod state;

pub use state::{
  FileFilter, ResponseFilter, Search, SearchOptions, SearchOutcome, SearchResponseReceived,
  SearchState, SearchStateChanged,
};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use soulseek_proto::peer::SearchResponse;
use soulseek_proto::server::{FileSearchRequest, RoomSearch, UserSearch, WishlistSearch};
use soulseek_proto::{SearchQuery, ServerRequest};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::client::{Client, failed, non_blank};
use crate::error::Error;
use crate::lock;
use crate::token::TokenGenerator;
use responses::{Accepted, Counts};

/// Recipients of a search.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SearchScope {
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

impl SearchScope {
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

/// The handler a search hands its responses to
type Handler = Box<dyn FnMut(SearchResponse) + Send>;

/// One search while it runs
struct Running {
  token: u32,
  /// the text as it is sent
  text: String,
  query: SearchQuery,
  scope: SearchScope,
  options: SearchOptions,
  counts: Mutex<Counts>,
  handler: Mutex<Handler>,
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
  fn decide(&self, counts: &mut Counts, outcome: SearchOutcome) -> SearchOutcome {
    let decided = *counts.outcome.get_or_insert(outcome);
    self.ended.send_replace(true);
    decided
  }

  /// [`Running::decide`], taking the counts' lock
  fn end(&self, outcome: SearchOutcome) -> SearchOutcome {
    self.decide(&mut lock(&self.counts), outcome)
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

/// removes a search from the table when its operation ends
struct Registration<'a> {
  searches: &'a Searches,
  token: u32,
}

impl Drop for Registration<'_> {
  fn drop(&mut self) {
    self.searches.running().remove(&self.token);
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
    reserved: Option<SearchToken>,
    ticketed: impl Fn(u32) -> bool,
    search: impl FnOnce(u32) -> Running,
  ) -> (Registration<'_>, Arc<Running>) {
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
    (
      Registration {
        searches: self,
        token,
      },
      search,
    )
  }

  /// cancels every search, for when the server session ends
  pub(crate) fn cancel_all(&self) {
    let running: Vec<_> = self.running().values().cloned().collect();
    if !running.is_empty() {
      tracing::debug!(count = running.len(), "searches canceled");
    }
    for search in running {
      search.end(SearchOutcome::Canceled);
    }
  }
}

/// Search token reserved with [`Client::reserve_search_token`].
///
/// The token is kept from other searches and from connection requests until the search started
/// with it ends, or until this value is dropped unused.
pub struct SearchToken {
  client: Client,
  token: u32,
}

impl std::fmt::Debug for SearchToken {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_tuple("SearchToken").field(&self.token).finish()
  }
}

impl SearchToken {
  /// Returns the reserved token.
  ///
  /// The search started with this value goes out under it.
  pub fn token(&self) -> u32 {
    self.token
  }
}

impl Drop for SearchToken {
  fn drop(&mut self) {
    lock(&self.client.searches().reserved).remove(&self.token);
  }
}

/// Search that has been checked and is ready to start.
///
/// Holds the text the search sends, who it goes to, what bounds it, and the token it goes out
/// under.
pub struct SearchRequest {
  /// the text as it is sent
  text: String,
  query: SearchQuery,
  scope: SearchScope,
  options: SearchOptions,
  token: Option<SearchToken>,
}

impl SearchRequest {
  /// Creates a search for `text` under a fresh token.
  ///
  /// The search goes to `scope` with `options`.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if the text has no term to search for (after
  /// dropping the terms of one character when the options say so), if the scope names no room
  /// or user or names a blank one, or if the timeout is zero.
  ///
  /// # Examples
  ///
  /// ```
  /// use soulseek_client::{SearchOptions, SearchRequest, SearchScope};
  ///
  /// let options = SearchOptions::default();
  /// let request = SearchRequest::new("artist album", SearchScope::Network, options.clone())?;
  /// println!("searching for {}", request.text());
  ///
  /// // a text with no term to search for is refused
  /// assert!(SearchRequest::new(" ", SearchScope::Network, options).is_err());
  /// # Ok::<(), soulseek_client::Error>(())
  /// ```
  pub fn new(text: &str, scope: SearchScope, options: SearchOptions) -> Result<Self, Error> {
    let query = search_query(text, &options)?;
    Self::checked(query.search_text(), query, scope, options)
  }

  /// Creates a search that sends `text` exactly as given.
  ///
  /// [`SearchRequest::new`] sends the terms and exclusions it parsed instead, in its own order
  /// and spacing.
  ///
  /// # Errors
  ///
  /// Returns [`Error::InvalidArgument`] if the text is empty or only whitespace, or under
  /// the conditions [`SearchRequest::new`] gives for the scope and the timeout.
  pub fn verbatim(text: &str, scope: SearchScope, options: SearchOptions) -> Result<Self, Error> {
    non_blank("search text", text)?;
    Self::checked(text.to_owned(), SearchQuery::parse(text), scope, options)
  }

  fn checked(
    text: String,
    query: SearchQuery,
    scope: SearchScope,
    options: SearchOptions,
  ) -> Result<Self, Error> {
    scope.validate()?;
    if options.search_timeout.is_zero() {
      return Err(Error::InvalidArgument(
        "the search timeout must be greater than zero".into(),
      ));
    }
    Ok(Self {
      text,
      query,
      scope,
      options,
      token: None,
    })
  }

  /// Sets the token the search goes out under.
  ///
  /// `token` is one reserved with [`Client::reserve_search_token`].
  pub fn with_token(mut self, token: SearchToken) -> Self {
    self.token = Some(token);
    self
  }

  /// Returns the terms and exclusions of the text.
  pub fn query(&self) -> &SearchQuery {
    &self.query
  }

  /// Returns the text as it is sent.
  pub fn text(&self) -> &str {
    &self.text
  }
}

/// shows the search; the callbacks in its options have nothing to print
impl std::fmt::Debug for SearchRequest {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SearchRequest")
      .field("text", &self.text)
      .field("scope", &self.scope)
      .field("options", &self.options)
      .field("token", &self.token.as_ref().map(SearchToken::token))
      .finish()
  }
}

impl Client {
  /// Reserves a token for a search to start later.
  ///
  /// The search is started with [`SearchRequest::with_token`]. The reservation serves an
  /// application that records the search first: no search runs under the token, no connection
  /// request's ticket holds it, and neither takes it while it is reserved or the search started
  /// with it runs.
  pub fn reserve_search_token(&self) -> SearchToken {
    let token = self
      .searches()
      .reserve(self.tokens(), |token| self.ticket_held(token));
    SearchToken {
      client: self.clone(),
      token,
    }
  }

  /// Starts a search and returns it once it has ended.
  ///
  /// Returns the search as it ended, with every response it took in the order they arrived. How
  /// it ended is its state: a timeout, a limit, or canceled, by the caller or by a disconnect.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Shutdown`] if the client has been shut down. Returns
  /// [`Error::InvalidOperation`] if the client is not logged in. Returns
  /// [`Error::Timeout`], or a [`Error::Operation`] wrapping the cause, if the request
  /// could not be sent.
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use soulseek_client::{
  ///   CancellationToken, Client, Error, SearchOptions, SearchRequest, SearchScope,
  /// };
  ///
  /// # async fn run(client: Client) -> Result<(), Error> {
  /// let request =
  ///   SearchRequest::new("artist album", SearchScope::Network, SearchOptions::default())?;
  /// let (search, responses) = client.search(request, &CancellationToken::new()).await?;
  /// println!("{} responses, ended as {}", responses.len(), search.state);
  /// # Ok(())
  /// # }
  /// ```
  pub async fn search(
    &self,
    request: SearchRequest,
    cancel: &CancellationToken,
  ) -> Result<(Search, Vec<SearchResponse>), Error> {
    let responses = Arc::new(Mutex::new(Vec::new()));
    let collect = {
      let responses = responses.clone();
      move |response| lock(&responses).push(response)
    };
    let search = self.search_with(request, collect, cancel).await?;
    let responses = std::mem::take(&mut *lock(&responses));
    Ok((search, responses))
  }

  /// Starts a search, handing each response to `handler`.
  ///
  /// Each response is handed over as it is taken. Otherwise behaves as [`Client::search`],
  /// which collects the responses.
  ///
  /// # Errors
  ///
  /// Fails under the conditions [`Client::search`] gives.
  pub async fn search_with(
    &self,
    request: SearchRequest,
    handler: impl FnMut(SearchResponse) + Send + 'static,
    cancel: &CancellationToken,
  ) -> Result<Search, Error> {
    let SearchRequest {
      text: search_text,
      query,
      scope,
      options,
      token,
    } = request;
    self.require_logged_in(&format!("search for {search_text}"))?;
    let searches = self.searches();
    let ticketed = |token| self.ticket_held(token);
    let (_registration, search) =
      searches.register(self.tokens(), token, ticketed, |token| Running {
        token,
        text: search_text.clone(),
        query,
        scope,
        options,
        counts: Mutex::new(Counts::default()),
        handler: Mutex::new(Box::new(handler)),
        ended: watch::channel(false).0,
      });
    let span = tracing::info_span!("search", token = search.token);
    self
      .run_search(&search, &search_text, cancel)
      .instrument(span)
      .await
  }

  async fn run_search(
    &self,
    search: &Arc<Running>,
    text: &str,
    cancel: &CancellationToken,
  ) -> Result<Search, Error> {
    // another task can end the search while it is being sent: a disconnect, or, once it takes
    // responses, a limit a response reached
    let mut ended = search.ended.subscribe();
    let requests = search.scope.requests(search.token, text);
    // an answer can come back as soon as the request is on its way, before the write is seen to
    // end, so the search takes responses from here
    self.set_search_state(search, SearchState::InProgress);
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
        if search.end(SearchOutcome::Errored) == SearchOutcome::Errored {
          let error = match error {
            error @ Error::Timeout(_) => error,
            error => failed(&format!("search for {text}"), error),
          };
          return Err(self.end_search(search, SearchOutcome::Errored, error));
        }
        // a limit an early answer reached ended it first; the loop below ends with that
        tracing::debug!(%error, "search ended before its request failed to send");
      }
      // the loop below ends at once with how it ended
      None => tracing::debug!("search ended while its request was sent"),
    }
    let latest = search.options.maximum_duration.map(deadline_after);
    let outcome = loop {
      let deadline = lock(&search.counts).deadline;
      tokio::select! {
        biased;
        () = cancel.cancelled() => break search.end(SearchOutcome::Canceled),
        // decided already, so this returns how
        () = was_ended(&mut ended) => break search.end(SearchOutcome::Canceled),
        () = at(latest) => break search.end(SearchOutcome::MaximumDurationReached),
        () = sleep_until(deadline) => {
          let mut counts = lock(&search.counts);
          // a response taken meanwhile moved the deadline
          if counts.deadline <= Instant::now() {
            break search.decide(&mut counts, SearchOutcome::TimedOut);
          }
        }
      }
    };
    // a response taken before the end is handed over before the search returns
    drop(lock(&search.handler));
    self.set_search_state(search, SearchState::Completed(outcome));
    let snapshot = search.snapshot(&lock(&search.counts));
    tracing::info!(
      state = %snapshot.state,
      responses = snapshot.response_count,
      files = snapshot.file_count,
      "search completed"
    );
    Ok(snapshot)
  }

  /// ends `search` with `outcome`, or with how it was decided to end first, and returns
  /// `error`, which the caller fails with
  fn end_search(&self, search: &Running, outcome: SearchOutcome, error: Error) -> Error {
    let state = SearchState::Completed(search.end(outcome));
    self.set_search_state(search, state);
    tracing::info!(%state, %error, "search ended");
    error
  }

  /// Moves `search` to `state`, calling its callback and then raising the event. Only the
  /// search's own task moves it, along the edges [`SearchState::leads_to`] allows.
  fn set_search_state(&self, search: &Running, state: SearchState) {
    let (previous, snapshot) = {
      let mut counts = lock(&search.counts);
      let previous = counts.state;
      assert!(
        previous.leads_to(state),
        "a search does not move from {previous} to {state}"
      );
      counts.state = state;
      (previous, search.snapshot(&counts))
    };
    tracing::debug!(%previous, %state, "search state changed");
    if let Some(callback) = &search.options.state_changed {
      callback(previous, &snapshot);
    }
    self.emit(crate::Event::Search(crate::search::Event::StateChanged {
      previous_state: previous,
      search: snapshot,
    }));
  }

  /// Sends a search for `text` with `token` to everyone in `scope`, without waiting for
  /// responses. A search of several users is one request per user, written together.
  #[cfg(feature = "testing")]
  pub(crate) async fn send_search_request(
    &self,
    scope: &SearchScope,
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
    let mut handler = lock(&search.handler);
    let Accepted { snapshot, response } = match responses::accept(&search, response) {
      Ok(accepted) => accepted,
      Err(reason) => {
        return tracing::trace!(token = search.token, reason, "search response dropped");
      }
    };
    tracing::trace!(
      username = response.username,
      files = response.files.len(),
      "search response taken"
    );
    handler(response.as_ref().clone());
    if let Some(received) = &search.options.response_received {
      received(&snapshot, &response);
    }
    self.emit(crate::Event::Search(
      crate::search::Event::ResponseReceived {
        search: snapshot,
        response,
      },
    ));
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

/// The query `text` makes, with short terms dropped when the options say so; it needs a term
fn search_query(text: &str, options: &SearchOptions) -> Result<SearchQuery, Error> {
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
      query: search_query("daft punk", &SearchOptions::default()).unwrap(),
      scope: SearchScope::Network,
      options: SearchOptions::default(),
      counts: Mutex::new(Counts::default()),
      handler: Mutex::new(Box::new(|_| {})),
      ended: watch::channel(false).0,
    }
  }

  /// a search running under `token` until the returned guard is dropped
  pub(crate) fn running(client: &Client, token: u32) -> impl Sized + '_ {
    let searches = client.searches();
    lock(&searches.reserved).insert(token);
    let reserved = SearchToken {
      client: client.clone(),
      token,
    };
    let tokens = TokenGenerator::new(None);
    let (registration, _) = searches.register(&tokens, Some(reserved), |_| false, search);
    registration
  }

  #[tokio::test]
  async fn a_fresh_search_token_is_never_a_connection_requests_ticket() {
    let client = Client::new(Config {
      enable_listener: false,
      ..Config::default()
    })
    .unwrap();
    let ticket = client.issue_ticket("bob", PeerConnectionType::Peer, Duration::from_secs(10));
    let held = ticket.token();
    let ticketed = |token| client.ticket_held(token);
    // the count reaching the ticket passes it by
    let counted = TokenGenerator::new(Some(held));
    let (_registration, search) = client.searches().register(&counted, None, ticketed, search);
    assert_ne!(search.token, held);
  }

  #[tokio::test]
  async fn a_reserved_token_is_clear_of_tickets_and_kept_from_them_and_other_searches() {
    let client = Client::counting_from(
      Config {
        enable_listener: false,
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
    let (_other, other) = searches.register(&counted, None, |_| false, search);
    assert_ne!(other.token, token, "another search passes it by");
    let (started, search) = searches.register(&counted, Some(reserved), |_| false, search);
    assert_eq!(search.token, token);
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
    let options = SearchOptions::default();
    assert_eq!(search_query("a b", &options).unwrap().search_text(), "a b");
    let drop_short = SearchOptions {
      remove_single_character_search_terms: true,
      ..SearchOptions::default()
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
    let options = SearchOptions::default;
    let parsed = SearchRequest::new(text, SearchScope::Network, options()).unwrap();
    assert_ne!(parsed.text(), text);
    let verbatim = SearchRequest::verbatim(text, SearchScope::Network, options()).unwrap();
    assert_eq!(verbatim.text(), text);
    // only exclusions is a search the server takes, so it is not refused here
    assert!(SearchRequest::verbatim("-live", SearchScope::Network, options()).is_ok());
    assert!(SearchRequest::verbatim("  ", SearchScope::Network, options()).is_err());
  }

  #[test]
  fn a_deadline_too_far_off_to_name_is_still_one() {
    assert!(deadline_after(Duration::MAX) > Instant::now() + Duration::from_secs(3600));
  }

  #[test]
  fn a_request_is_checked_before_anything_is_sent() {
    let refused = |text: &str, scope: SearchScope, options: SearchOptions| {
      matches!(
        SearchRequest::new(text, scope, options),
        Err(Error::InvalidArgument(_))
      )
    };
    let options = SearchOptions::default;
    let request = SearchRequest::new("daft punk", SearchScope::Network, options()).unwrap();
    assert_eq!(request.query().search_text(), "daft punk");
    assert!(refused("   ", SearchScope::Network, options()), "no text");
    assert!(
      refused("-live -bootleg", SearchScope::Network, options()),
      "only exclusions"
    );
    let drop_short = SearchOptions {
      remove_single_character_search_terms: true,
      ..options()
    };
    assert!(
      refused("a b", SearchScope::Network, drop_short),
      "only short terms"
    );
    for scope in [
      SearchScope::Rooms(Vec::new()),
      SearchScope::Rooms(vec![" ".into()]),
      SearchScope::Users(Vec::new()),
      SearchScope::Users(vec!["alice".into(), "".into()]),
    ] {
      assert!(refused("daft punk", scope.clone(), options()), "{scope:?}");
    }
    let no_wait = SearchOptions {
      search_timeout: Duration::ZERO,
      ..options()
    };
    assert!(
      refused("daft punk", SearchScope::Network, no_wait),
      "no timeout"
    );
  }

  #[test]
  fn a_user_search_is_one_request_per_user() {
    let scope = SearchScope::Users(vec!["alice".into(), "bob".into()]);
    let requests = scope.requests(7, "daft punk");
    assert_eq!(requests.len(), 2);
  }
}

/// Something that happened to a search: one of the client's own, or one it answered.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// The client sent its results for another user's search.
  ResponseDelivered(Delivery),
  /// Results the client kept for an unreachable searcher were given up.
  ResponseDeliveryFailed(Delivery),
  /// One of the client's searches moved from one state to another.
  StateChanged {
    /// State left.
    previous_state: SearchState,
    /// Search as it is now.
    search: Search,
  },
  /// One of the client's searches took a response.
  ///
  /// The event may come just after the search completed.
  ResponseReceived {
    /// Search as it is now.
    search: Search,
    /// Response as it was taken.
    response: std::sync::Arc<SearchResponse>,
  },
}

/// The client's answer to another user's search, and whom it was for.
#[derive(Clone, Debug)]
pub struct Delivery {
  /// User who searched.
  pub username: String,
  /// Token of the searcher.
  pub token: u32,
  /// Query as the searcher sent it.
  pub query: String,
  /// What the client sent.
  pub search_response: std::sync::Arc<crate::handler::SearchReply>,
}
