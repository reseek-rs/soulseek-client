//! Answering other users' searches. The application's handler finds what matches; the answer
//! goes to the searcher over a message connection, dialed directly and requested through the
//! server at once. An answer that cannot be delivered is kept for a few minutes under the token
//! of that connection request, so it still reaches a searcher who acts on the request late;
//! answers that waited on one attempt are kept together under its request, and one that comes to
//! be kept after the searcher's late connection took the others goes out on that connection. A
//! request is known as the searcher's from the moment an answer waits on its attempt, so a late
//! connection that comes before any answer is kept under it is taken too. One connection answers
//! a request; another with its token is not taken.
//! The limits bound how many searches from the server or the distributed tree are answered at
//! once, and how long the handler has to answer one.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use soulseek_proto::PeerMessage;
use tracing::Instrument;

use crate::cache::BoundedCache;
use crate::client::Client;
use crate::conn::MessageConn;
use crate::error::Error;
use crate::events::Event;
use crate::handler::SearchReply;
use crate::limit::Limiter;
use crate::log::LogLimiter;
use crate::peers::{Answering, Unreached};

static ANSWERING_FULL: LogLimiter = LogLimiter::new();

/// An answer the client could not deliver, kept under the token of the connection request it
/// rode on
#[derive(Clone)]
struct Undelivered {
  username: String,
  /// the searcher's token
  token: u32,
  query: String,
  reply: Arc<SearchReply>,
  /// the client's username when it answered, which the reply goes out under
  own: String,
}

/// A kept answer's place in the cache: the connection request it rode on, and the order it was
/// kept in
type KeptAt = (u32, u64);

/// What the client keeps for answering searches
pub(crate) struct SearchResponder {
  /// the answers the client could not deliver, one entry each, so the cache's bound counts
  /// answers
  undelivered: Mutex<BoundedCache<KeptAt, Undelivered>>,
  /// numbers the answers kept, in order
  next_kept: AtomicU64,
  /// connection requests the searcher refused while answers waited on them, so those answers
  /// are not kept; forgotten once the last of them is done
  refused: Mutex<BoundedCache<u32, ()>>,
  /// connection requests whose searcher connected late and took the answers kept under them,
  /// so an answer that comes to be kept after goes out at once; taken with `undelivered` held,
  /// and forgotten once the last answer waiting on the searcher is done
  arrived: Mutex<BoundedCache<u32, ()>>,
  /// the connection requests whose attempts answers wait on, with their searcher; forgotten
  /// once the last answer waiting on the searcher is done
  riding: Mutex<BoundedCache<u32, String>>,
  /// connection requests a connection has answered, awaited or late, so no other connection
  /// is taken for them while answers may still be kept under them; a full cache never drops
  /// one that answers wait on or are kept under to make room
  answered: Mutex<BoundedCache<u32, ()>>,
  /// the answers waiting for a connection to each searcher
  waiting: Mutex<HashMap<String, Waiters>>,
  /// the searches from the server or the distributed tree being answered
  answering: Limiter,
}

/// The answers waiting for a connection to one searcher.
#[derive(Default)]
struct Waiters {
  /// how many
  count: usize,
  /// the connection requests their attempts made while under way, each attempt that replaced
  /// another adding its own
  requests: Vec<u32>,
}

impl SearchResponder {
  /// a responder that keeps at most `cache_size` undelivered answers, each for `lifetime`
  pub(crate) fn new(cache_size: u32, lifetime: Duration) -> Self {
    Self {
      undelivered: Mutex::new(BoundedCache::new(cache_size as usize, lifetime)),
      next_kept: AtomicU64::new(0),
      refused: Mutex::new(BoundedCache::new(cache_size as usize, lifetime)),
      arrived: Mutex::new(BoundedCache::new(cache_size as usize, lifetime)),
      riding: Mutex::new(BoundedCache::new(cache_size as usize, lifetime)),
      answered: Mutex::new(BoundedCache::new(cache_size as usize, lifetime)),
      waiting: Mutex::new(HashMap::new()),
      answering: Limiter::new(0),
    }
  }

  fn undelivered(&self) -> std::sync::MutexGuard<'_, BoundedCache<KeptAt, Undelivered>> {
    crate::lock(&self.undelivered)
  }

  /// Keeps `answer` under the connection request `request`, for a searcher who acts on it
  /// late. Each answer counts against the bound, and the cache makes room by dropping the one
  /// kept longest ago. Each also keeps its own age: one kept later under the same request does
  /// not lengthen the wait of those kept before it. When the searcher's late connection for
  /// `request` took the answers kept before, `answer` is given back to go out on it.
  fn keep(&self, request: u32, answer: Undelivered) -> Option<Undelivered> {
    let mut undelivered = self.undelivered();
    if crate::lock(&self.arrived).get(&request).is_some() {
      return Some(answer);
    }
    let number = self.next_kept.fetch_add(1, Ordering::Relaxed);
    undelivered.insert((request, number), answer);
    None
  }

  /// takes out the answers kept under the connection request `request`, in the order they were
  /// kept
  fn take(&self, request: u32) -> Vec<Undelivered> {
    Self::taken(&mut self.undelivered(), request)
  }

  /// [`SearchResponder::take`] for the searcher's late connection answering `request`, which
  /// the answers kept under it after this go out on too
  fn take_arrived(&self, request: u32) -> Vec<Undelivered> {
    let mut undelivered = self.undelivered();
    crate::lock(&self.arrived).insert(request, ());
    Self::taken(&mut undelivered, request)
  }

  fn taken(undelivered: &mut BoundedCache<KeptAt, Undelivered>, request: u32) -> Vec<Undelivered> {
    let mut taken = undelivered.remove_where(|(rode_on, _)| *rode_on == request);
    taken.sort_by_key(|((_, number), _)| *number);
    taken.into_iter().map(|(_, answer)| answer).collect()
  }

  /// whether an answer of the client's waits for a connection to `username`
  pub(crate) fn answers_waiting_for(&self, username: &str) -> bool {
    crate::lock(&self.waiting).contains_key(username)
  }

  /// the searcher, whom answers wait to reach, cannot connect to the client for the connection
  /// request `token` still being tried
  pub(crate) fn refuse(&self, token: u32) {
    crate::lock(&self.refused).insert(token, ());
  }

  /// counts an answer waiting for a connection to `username` until the returned guard is dropped
  fn wait_for(&self, username: &str) -> Waiting<'_> {
    crate::lock(&self.waiting)
      .entry(username.to_owned())
      .or_default()
      .count += 1;
    Waiting {
      responder: self,
      username: username.to_owned(),
    }
  }

  /// Takes the connection request `response_token` for the searcher answers ride on it for,
  /// answers kept under it or waiting on its attempt, and returns that searcher. A request is
  /// taken once.
  pub(crate) fn claim(&self, response_token: u32) -> Option<String> {
    let undelivered = self.undelivered();
    if crate::lock(&self.answered).peek(&response_token).is_some() {
      return None;
    }
    let searcher = undelivered
      .find(|(rode_on, _)| *rode_on == response_token)
      .map(|kept| kept.username.clone())
      .or_else(|| crate::lock(&self.riding).get(&response_token).cloned())?;
    self.change_answered(&undelivered, |marks, held| {
      marks.insert_unless_held(response_token, (), held);
    });
    Some(searcher)
  }

  /// a connection answered the connection request `token`, which no other connection is taken
  /// for from now on
  pub(crate) fn answered(&self, token: u32) {
    let undelivered = self.undelivered();
    self.change_answered(&undelivered, |marks, held| {
      marks.insert_unless_held(token, (), held);
    });
  }

  /// Changes the answered marks with `change`, which is told what it must not drop to make
  /// room: the requests answers are kept under, in `undelivered`, or wait on the attempts of.
  /// Every change to the marks comes here, with `undelivered` held.
  fn change_answered(
    &self,
    undelivered: &BoundedCache<KeptAt, Undelivered>,
    change: impl FnOnce(&mut BoundedCache<u32, ()>, &dyn Fn(&u32) -> bool),
  ) {
    let mut filed: HashSet<u32> = undelivered.keys().map(|(rode_on, _)| *rode_on).collect();
    for waiters in crate::lock(&self.waiting).values() {
      filed.extend(&waiters.requests);
    }
    change(&mut crate::lock(&self.answered), &|token| {
      filed.contains(token)
    });
  }

  /// whether anything is still filed under the connection request `token`: answers riding on
  /// its attempt or kept under it, its refusal, its late connection or its answer
  pub(crate) fn holds(&self, token: u32) -> bool {
    self
      .undelivered()
      .find(|(rode_on, _)| *rode_on == token)
      .is_some()
      || crate::lock(&self.riding).peek(&token).is_some()
      || crate::lock(&self.arrived).peek(&token).is_some()
      || crate::lock(&self.refused).peek(&token).is_some()
      || crate::lock(&self.answered).peek(&token).is_some()
  }

  /// follows a change of the cache bound and of how long an undelivered answer is kept
  pub(crate) fn resize(&self, cache_size: u32, lifetime: Duration) {
    let mut undelivered = self.undelivered();
    undelivered.set_capacity(cache_size as usize);
    undelivered.set_ttl(lifetime);
    for marks in [&self.refused, &self.arrived] {
      let mut marks = crate::lock(marks);
      marks.set_capacity(cache_size as usize);
      marks.set_ttl(lifetime);
    }
    let mut riding = crate::lock(&self.riding);
    riding.set_capacity(cache_size as usize);
    riding.set_ttl(lifetime);
    drop(riding);
    self.change_answered(&undelivered, |marks, held| {
      marks.set_ttl(lifetime);
      marks.set_capacity_unless_held(cache_size as usize, held);
    });
  }
}

/// One answer waiting for a connection to its searcher. The last one for a searcher to be done
/// forgets every request the answers to them rode on: the attempts, their refusals and their
/// late connections.
struct Waiting<'a> {
  responder: &'a SearchResponder,
  username: String,
}

impl Drop for Waiting<'_> {
  fn drop(&mut self) {
    let mut waiting = crate::lock(&self.responder.waiting);
    let waiters = waiting
      .get_mut(&self.username)
      .expect("a waiting answer is counted");
    waiters.count -= 1;
    if waiters.count > 0 {
      return;
    }
    let requests = std::mem::take(&mut waiters.requests);
    waiting.remove(&self.username);
    for request in requests {
      crate::lock(&self.responder.refused).remove(&request);
      crate::lock(&self.responder.arrived).remove(&request);
      crate::lock(&self.responder.riding).remove(&request);
    }
  }
}

impl Waiting<'_> {
  /// The answer waits on the attempt that had the server relay the connection request
  /// `request`, which a connection answering it late is known by from now on.
  fn rides_on(&self, request: u32) {
    let mut waiting = crate::lock(&self.responder.waiting);
    let requests = &mut waiting
      .get_mut(&self.username)
      .expect("a waiting answer is counted")
      .requests;
    if !requests.contains(&request) {
      requests.push(request);
    }
    crate::lock(&self.responder.riding).insert(request, self.username.clone());
  }
}

impl Client {
  /// Returns the number of undelivered search answers.
  ///
  /// These are the answers to other users' searches that are kept now because their searcher could
  /// not be reached yet.
  pub fn undelivered_search_responses(&self) -> usize {
    self.responder().undelivered().len()
  }

  /// Answers another user's search on a task of its own, unless as many as the limits allow
  /// are being answered already.
  pub(crate) fn respond_to_search(&self, username: String, token: u32, query: String) {
    let limit = self.options().limits.max_concurrent_search_answers;
    let Some(answering) = self.responder().answering.try_acquire(limit) else {
      if let Some(suppressed) = ANSWERING_FULL.allow() {
        tracing::debug!(
          limit,
          suppressed,
          "too many searches being answered, search dropped"
        );
      }
      self.dropped(username, token, query, crate::search::DropReason::Busy);
      return;
    };
    let client = self.clone();
    let span = tracing::debug_span!("search_request", username, token);
    tokio::spawn(
      async move {
        client.try_respond(&username, token, &query).await;
        drop(answering);
      }
      .instrument(span),
    );
  }

  /// tells the application `username`'s search went unanswered
  fn dropped(
    &self,
    username: String,
    token: u32,
    query: String,
    reason: crate::search::DropReason,
  ) {
    let dropped = crate::search::Dropped {
      username,
      token,
      query,
      reason,
    };
    self.emit(Event::SearchReply(crate::search::ReplyEvent::Dropped(
      dropped,
    )));
  }

  /// Resolves an answer to `username`'s search and delivers it; whether it was delivered
  pub(crate) async fn try_respond(&self, username: &str, token: u32, query: &str) -> bool {
    let Some((reply, own)) = self.resolve_search(username, token, query).await else {
      return false;
    };
    let reply = Arc::new(reply);
    if self.username().as_deref() == Some(username) {
      return self.deliver_to_self(username, token, query, reply);
    }
    let address = match self.users().address(username).await {
      Ok(address) => address,
      Err(error) => {
        tracing::debug!(query, %error, "failed to send search response");
        self.not_delivered(username, token, query, reply);
        return false;
      }
    };
    let waiting = self.responder().wait_for(username);
    let reached = self
      .reach(
        username,
        Some(address),
        // on a task of its own, so only the client's shutdown stops it
        self.cancellation(),
        |request| {
          waiting.rides_on(request);
        },
      )
      .await;
    let conn = match reached {
      Ok(conn) => conn,
      Err(Unreached { error, request }) => {
        let delivery = || {
          Event::SearchReply(crate::search::ReplyEvent::NotDelivered(
            crate::search::Delivery {
              username: username.to_owned(),
              token,
              query: query.to_owned(),
              reply: reply.clone(),
            },
          ))
        };
        // an attempt that made no request of its own, such as one answering the searcher's
        // request to the client, leaves the searcher nothing to act on late
        let Some(request) = request else {
          tracing::debug!(%error, "the searcher cannot be reached; search response dropped");
          self.emit(delivery());
          return false;
        };
        if crate::lock(&self.responder().refused)
          .get(&request)
          .is_some()
        {
          tracing::debug!(response_token = request, %error, "the searcher cannot connect; search response dropped");
          self.emit(delivery());
          return false;
        }
        let answer = Undelivered {
          username: username.to_owned(),
          token,
          query: query.to_owned(),
          reply,
          own,
        };
        return self.keep_undelivered(request, answer, &error).await;
      }
    };
    drop(waiting);
    self
      .deliver(
        &conn,
        &mut Answering::default(),
        (username, token, query),
        reply,
        &own,
      )
      .await
  }

  /// The client's answer to a search it sent itself on purpose goes straight to that search: a
  /// connection to its own listener would stand for both ends of one user's connection
  fn deliver_to_self(
    &self,
    username: &str,
    token: u32,
    query: &str,
    reply: Arc<SearchReply>,
  ) -> bool {
    let response = match reply.plain() {
      SearchReply::Matches(matches) => matches.response(username, token),
      encoded => {
        let limits = self.options().limits.decode;
        match PeerMessage::decode_with(&encoded.frame(username, token), &limits) {
          Ok(PeerMessage::SearchResponse(response)) => response,
          Ok(other) => {
            tracing::warn!(
              code = other.code().code(),
              "encoded search reply is another message"
            );
            self.not_delivered(username, token, query, reply);
            return false;
          }
          // the decoder has logged what is wrong with it
          Err(_) => {
            self.not_delivered(username, token, query, reply);
            return false;
          }
        }
      }
    };
    self.search_response(response);
    tracing::debug!(
      query,
      "search response delivered to the client's own search"
    );
    self.emit(Event::SearchReply(crate::search::ReplyEvent::Delivered(
      crate::search::Delivery {
        username: username.to_owned(),
        token,
        query: query.to_owned(),
        reply,
      },
    )));
    true
  }

  /// Answers a search a peer sent on its message connection, on that connection
  pub(crate) async fn respond_on(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    username: &str,
    token: u32,
    query: &str,
  ) {
    if let Some((reply, own)) = self.resolve_search(username, token, query).await {
      self
        .deliver(
          conn,
          answering,
          (username, token, query),
          Arc::new(reply),
          &own,
        )
        .await;
    }
  }

  /// Keeps an answer the searcher could not be reached for, failing with `error`, under the
  /// connection request `request`; when the searcher's late connection for it took the answers
  /// kept before, it goes out on that connection now. Whether it was delivered.
  async fn keep_undelivered(&self, request: u32, answer: Undelivered, error: &Error) -> bool {
    let Some(answer) = self.responder().keep(request, answer) else {
      tracing::debug!(
        response_token = request,
        %error,
        "failed to connect to deliver search results; cached response for potential delayed delivery"
      );
      return false;
    };
    self.respond_kept(request, answer).await
  }

  /// A searcher acted late on a connection request that carried answers the client could not
  /// deliver, and has just connected; the kept answers go out on that connection, as do those
  /// kept under it from now on
  pub(crate) async fn try_respond_cached(&self, response_token: u32) {
    let kept = self.responder().take_arrived(response_token);
    for kept in kept {
      self.respond_kept(response_token, kept).await;
    }
  }

  /// sends one kept answer on the searcher's connection; whether it went out
  async fn respond_kept(&self, response_token: u32, kept: Undelivered) -> bool {
    let delivery = crate::search::Delivery {
      username: kept.username.clone(),
      token: kept.token,
      query: kept.query.clone(),
      reply: kept.reply.clone(),
    };
    let Some(conn) = self.peers().cached(&kept.username) else {
      tracing::debug!(
        username = kept.username,
        response_token,
        "no connection to deliver the kept search response on"
      );
      self.emit(Event::SearchReply(crate::search::ReplyEvent::NotDelivered(
        delivery,
      )));
      return false;
    };
    let remote = conn.handle().remote();
    if !kept.reply.may_reach(remote.ip()) {
      tracing::debug!(
        username = kept.username,
        %remote,
        "kept search response confined to another address; not delivered"
      );
      self.emit(Event::SearchReply(crate::search::ReplyEvent::NotDelivered(
        delivery,
      )));
      return false;
    }
    match conn.write(kept.reply.frame(&kept.own, kept.token)).await {
      Ok(()) => {
        tracing::debug!(
          username = kept.username,
          token = kept.token,
          "kept search response delivered"
        );
        self.emit(Event::SearchReply(crate::search::ReplyEvent::Delivered(
          delivery,
        )));
        true
      }
      Err(error) => {
        tracing::debug!(username = kept.username, %error, "kept search response not delivered");
        self.emit(Event::SearchReply(crate::search::ReplyEvent::NotDelivered(
          delivery,
        )));
        false
      }
    }
  }

  /// The searcher answers were kept for cannot connect to the client; the answers are given up
  pub(crate) fn try_discard(&self, response_token: u32) -> bool {
    let kept = self.responder().take(response_token);
    let discarded = !kept.is_empty();
    for kept in kept {
      tracing::debug!(
        username = kept.username,
        token = kept.token,
        response_token,
        "kept search response discarded"
      );
      self.emit(Event::SearchReply(crate::search::ReplyEvent::NotDelivered(
        crate::search::Delivery {
          username: kept.username,
          token: kept.token,
          query: kept.query,
          reply: kept.reply,
        },
      )));
    }
    discarded
  }

  /// Asks the application for an answer, returned with the client's username as it is now,
  /// which the answer goes out under. `None` when there is none to send, or the client is not
  /// logged in.
  async fn resolve_search(
    &self,
    username: &str,
    token: u32,
    query: &str,
  ) -> Option<(SearchReply, String)> {
    let own = self.username()?;
    // the client's own searches come back to it from the network; only a search of its own
    // shares is answered
    if own == username && !self.searches_own_shares(token) {
      tracing::trace!(token, "own search not answered");
      return None;
    }
    let search = crate::handler::SearchRequest {
      username: username.to_owned(),
      token,
      query: query.to_owned(),
    };
    let options = self.options();
    let answered = tokio::time::timeout(
      options.limits.search_answer_timeout,
      options.handler.search(self, &search),
    )
    .await;
    let Ok(reply) = answered else {
      tracing::debug!(query, "the handler did not answer the search in time");
      let reason = crate::search::DropReason::TimedOut;
      self.dropped(username.to_owned(), token, query.to_owned(), reason);
      return None;
    };
    let reply = reply?;
    // an empty answer is never sent; an encoded one always is
    let files = reply.file_count();
    if files == Some(0) {
      return None;
    }
    tracing::debug!(files, query, "resolved files for search");
    Some((reply, own))
  }

  /// sends `reply`, under the client's username `own`, to `username`'s search `token` for
  /// `query`
  async fn deliver(
    &self,
    conn: &MessageConn,
    answering: &mut Answering,
    (username, token, query): (&str, u32, &str),
    reply: Arc<SearchReply>,
    own: &str,
  ) -> bool {
    let remote = conn.handle().remote();
    if !reply.may_reach(remote.ip()) {
      tracing::debug!(
        username,
        %remote,
        query,
        "search response confined to another address; not delivered"
      );
      self.emit(Event::SearchReply(crate::search::ReplyEvent::NotDelivered(
        crate::search::Delivery {
          username: username.to_owned(),
          token,
          query: query.to_owned(),
          reply,
        },
      )));
      return false;
    }
    match answering.send_frame(conn, reply.frame(own, token)).await {
      Ok(()) => {
        tracing::debug!(query, "search response delivered");
        self.emit(Event::SearchReply(crate::search::ReplyEvent::Delivered(
          crate::search::Delivery {
            username: username.to_owned(),
            token,
            query: query.to_owned(),
            reply,
          },
        )));
        true
      }
      Err(error) => {
        tracing::debug!(query, %error, "failed to send search response");
        self.not_delivered(username, token, query, reply);
        false
      }
    }
  }

  /// tells the application its answer to `username`'s search did not go out
  fn not_delivered(&self, username: &str, token: u32, query: &str, reply: Arc<SearchReply>) {
    let delivery = crate::search::Delivery {
      username: username.to_owned(),
      token,
      query: query.to_owned(),
      reply,
    };
    self.emit(Event::SearchReply(crate::search::ReplyEvent::NotDelivered(
      delivery,
    )));
  }
}

#[cfg(test)]
mod tests {
  use soulseek_proto::frame::{self, FrameKind};
  use soulseek_proto::server::{CannotConnectResponse, LoginResponse};
  use soulseek_proto::{Limits, Secret, ServerRequest, ServerResponse};
  use tokio::io::{AsyncReadExt, AsyncWriteExt};
  use tokio::net::{TcpListener, TcpStream};
  use tokio_util::sync::CancellationToken;

  use super::*;
  use crate::config::Config;

  const KEEP: Duration = Duration::from_secs(180);

  /// the next message the client wrote to `server` that `pick` accepts, skipping the others
  async fn request<T>(
    server: &mut TcpStream,
    buffer: &mut Vec<u8>,
    pick: impl Fn(ServerRequest) -> Option<T>,
  ) -> T {
    loop {
      while let Some((_, consumed)) =
        frame::split(FrameKind::Server, buffer, &Limits::DEFAULT).unwrap()
      {
        let frame: Vec<u8> = buffer.drain(..consumed).collect();
        if let Some(picked) = pick(ServerRequest::decode(&frame).unwrap()) {
          return picked;
        }
      }
      let mut chunk = [0; 4096];
      let read = server.read(&mut chunk).await.unwrap();
      assert!(read > 0, "the client closed the connection");
      buffer.extend_from_slice(&chunk[..read]);
    }
  }

  /// A client logged in as alice to a server played by the test, with what the server has read
  /// from it and not yet taken.
  async fn logged_in() -> (Client, TcpStream, Vec<u8>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let client = Client::new(Config {
      listener: false,
      server_host: "127.0.0.1".into(),
      server_port: port,
      ..Config::default()
    });
    let mut buffer = Vec::new();
    let connecting = client.connect("alice", "hunter2");
    let serving = async {
      let (mut server, _) = listener.accept().await.unwrap();
      request(&mut server, &mut buffer, |request| {
        matches!(request, ServerRequest::Login(_)).then_some(())
      })
      .await;
      let accepted = ServerResponse::Login(LoginResponse::Success {
        greeting: String::new(),
        ip: std::net::Ipv4Addr::LOCALHOST,
        hash: Secret::new("0123456789abcdef0123456789abcdef"),
        is_supporter: false,
      });
      server.write_all(&accepted.encode()).await.unwrap();
      server
    };
    let (connected, server) = tokio::join!(connecting, serving);
    connected.unwrap();
    (client, server, buffer)
  }

  #[tokio::test]
  async fn a_refused_connection_request_that_carried_no_answer_leaves_nothing_behind() {
    let (client, mut server, mut buffer) = logged_in().await;
    let cancel = CancellationToken::new();
    // nothing listens where bob is said to be, and bob refuses the request the server relays
    let nowhere = std::net::TcpListener::bind("127.0.0.1:0")
      .unwrap()
      .local_addr()
      .unwrap();
    let asking = client.message_connection("bob", Some(nowhere), &cancel);
    let refusing = async {
      let token = request(&mut server, &mut buffer, |request| match request {
        ServerRequest::ConnectToPeer(request) => Some(request.token),
        _ => None,
      })
      .await;
      let refusal = ServerResponse::CannotConnect(CannotConnectResponse {
        token,
        username: None,
      });
      server.write_all(&refusal.encode()).await.unwrap();
    };
    let (asked, ()) = tokio::join!(asking, refusing);
    assert!(asked.is_err());
    assert_eq!(crate::lock(&client.responder().refused).len(), 0);
  }

  /// Wakes a task, noting first whether the connection request `token` is refused by then.
  struct Woken {
    client: crate::client::WeakClient,
    token: u32,
    refused: std::sync::Mutex<Option<bool>>,
    told: tokio::sync::Notify,
  }

  impl std::task::Wake for Woken {
    fn wake(self: Arc<Self>) {
      self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
      let client = self.client.upgrade().expect("the test holds the client");
      let refused = crate::lock(&client.responder().refused)
        .get(&self.token)
        .is_some();
      crate::lock(&self.refused).get_or_insert(refused);
      self.told.notify_one();
    }
  }

  #[tokio::test]
  async fn a_request_the_searcher_cannot_act_on_is_refused_before_its_attempt_is_woken() {
    use crate::config::Connection;
    use soulseek_proto::PeerConnectionType;
    use std::future::Future;

    let (client, mut server, mut buffer) = logged_in().await;
    // an answer to carol's search waits on the attempt to reach her
    let _waiting = client.responder().wait_for("carol");
    let cancel = CancellationToken::new();
    let ticket = client.issue_ticket("carol", PeerConnectionType::Peer, Duration::from_secs(30));
    let token = ticket.token();
    let mut soliciting = Box::pin(client.solicit(ticket, Connection::default(), &cancel));
    tokio::select! {
      biased;
      _ = &mut soliciting => panic!("the attempt ended before the server had its request"),
      () = request(&mut server, &mut buffer, |request| {
        matches!(request, ServerRequest::ConnectToPeer(_)).then_some(())
      }) => {}
    }
    // from here the attempt is woken through a waker that looks at the refusals first
    let woken = Arc::new(Woken {
      client: client.downgrade(),
      token,
      refused: std::sync::Mutex::new(None),
      told: tokio::sync::Notify::new(),
    });
    let waker = std::task::Waker::from(woken.clone());
    let polled = soliciting
      .as_mut()
      .poll(&mut std::task::Context::from_waker(&waker));
    assert!(polled.is_pending());
    let refusal = ServerResponse::CannotConnect(CannotConnectResponse {
      token,
      username: Some("carol".into()),
    });
    server.write_all(&refusal.encode()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), woken.told.notified())
      .await
      .expect("the attempt was never woken");
    assert_eq!(
      *crate::lock(&woken.refused),
      Some(true),
      "woken before the request was refused, so an answer could be kept under it"
    );
  }

  /// carol's answer to the search `token`, as a kept answer
  fn for_carol(token: u32) -> Undelivered {
    let reply = SearchReply::Matches(crate::handler::Matches::new(Vec::new()));
    Undelivered {
      username: "carol".into(),
      token,
      query: "hit".into(),
      reply: Arc::new(reply),
      own: "alice".into(),
    }
  }

  #[tokio::test]
  async fn an_answer_kept_after_the_searchers_late_connection_took_the_others_goes_out_on_it() {
    use crate::config::Connection;
    use crate::peers::Via;

    let client = Client::new(Config {
      listener: false,
      ..Config::default()
    });
    let request = 7;
    let unreached = Error::timeout("carol".into());
    assert!(
      !client
        .keep_undelivered(request, for_carol(1), &unreached)
        .await
    );
    // carol acts on the request late: her connection takes what was kept under it
    let (conn, mut carol) = crate::conn::tests::pair(Connection::default()).await;
    client
      .add_or_update_message_connection("carol", conn, Via::Indirect.kind(), true, None)
      .unwrap();
    client.try_respond_cached(request).await;
    // the answer that shared the attempt is kept only now
    assert!(
      client
        .keep_undelivered(request, for_carol(2), &unreached)
        .await
    );
    assert_eq!(client.undelivered_search_responses(), 0);
    assert_eq!(answers_read(&mut carol, 2).await, [1, 2]);
  }

  /// What carol's message connection `carol` reads, as the tokens of the search answers on it,
  /// once `count` came.
  async fn answers_read(carol: &mut TcpStream, count: usize) -> Vec<u32> {
    let mut buffer = Vec::new();
    let mut tokens = Vec::new();
    while tokens.len() < count {
      if let Some((_, consumed)) = frame::split(FrameKind::Peer, &buffer, &Limits::DEFAULT).unwrap()
      {
        let frame: Vec<u8> = buffer.drain(..consumed).collect();
        match PeerMessage::decode_with(&frame, &Limits::DEFAULT).unwrap() {
          PeerMessage::SearchResponse(response) => tokens.push(response.token),
          other => panic!("{other:?}"),
        }
        continue;
      }
      let mut chunk = [0; 4096];
      let read = carol.read(&mut chunk).await.unwrap();
      assert!(read > 0, "the connection closed after {tokens:?}");
      buffer.extend_from_slice(&chunk[..read]);
    }
    tokens
  }

  #[tokio::test]
  async fn a_searcher_who_acts_late_before_any_answer_is_kept_is_known_and_gets_it() {
    use crate::config::Connection;
    use crate::peers::Via;

    let client = Client::new(Config {
      listener: false,
      ..Config::default()
    });
    let nowhere = std::net::TcpListener::bind("127.0.0.1:0")
      .unwrap()
      .local_addr()
      .unwrap();
    let waiting = client.responder().wait_for("carol");
    // the attempt fails at once, since the client is not logged in
    let reached = client
      .reach(
        "carol",
        Some(nowhere),
        &CancellationToken::new(),
        |request| {
          waiting.rides_on(request);
        },
      )
      .await;
    let request = reached.err().unwrap().request.unwrap();
    // carol acts on the request now, before the answer is kept: what the connection is taken as
    let searcher = client.responder().claim(request);
    assert_eq!(searcher.as_deref(), Some("carol"));
    let (conn, mut carol) = crate::conn::tests::pair(Connection::default()).await;
    client
      .add_or_update_message_connection("carol", conn, Via::Indirect.kind(), true, None)
      .unwrap();
    client.try_respond_cached(request).await;
    let unreached = Error::timeout("carol".into());
    assert!(
      client
        .keep_undelivered(request, for_carol(1), &unreached)
        .await
    );
    assert_eq!(answers_read(&mut carol, 1).await, [1]);
    drop(waiting);
  }

  #[test]
  fn the_requests_of_replaced_attempts_are_forgotten_with_the_last_answer_waiting() {
    let responder = SearchResponder::new(10, KEEP);
    let riding = |responder: &SearchResponder| crate::lock(&responder.riding).len();
    let first = responder.wait_for("carol");
    // the attempt on request 1 was replaced by one on request 2
    first.rides_on(1);
    first.rides_on(2);
    let second = responder.wait_for("carol");
    second.rides_on(3);
    drop(second);
    assert_eq!(riding(&responder), 3, "an answer still waits on them");
    drop(first);
    assert_eq!(riding(&responder), 0);
  }

  #[tokio::test]
  async fn a_request_is_claimed_for_its_searcher_once() {
    let responder = SearchResponder::new(3, KEEP);
    let waiting = responder.wait_for("carol");
    waiting.rides_on(7);
    assert_eq!(responder.claim(7).as_deref(), Some("carol"));
    // someone else connecting with the same token is not taken for carol
    assert_eq!(responder.claim(7), None);
    drop(waiting);
    assert_eq!(responder.claim(7), None);
    // carol answered 8 in time, but the connection failed after, so the answer is kept under it
    responder.answered(8);
    assert!(responder.keep(8, for_carol(1)).is_none());
    assert_eq!(responder.claim(8), None);
  }

  #[tokio::test]
  async fn a_request_is_held_while_anything_is_filed_under_it() {
    let responder = SearchResponder::new(10, KEEP);
    assert!(responder.keep(1, for_carol(1)).is_none());
    let waiting = responder.wait_for("carol");
    waiting.rides_on(2);
    responder.answered(3);
    assert!(responder.take_arrived(4).is_empty());
    responder.refuse(5);
    for (token, filed) in [
      (1, "a kept answer"),
      (2, "an answer riding on its attempt"),
      (3, "its answer"),
      (4, "its late connection"),
      (5, "its refusal"),
    ] {
      assert!(responder.holds(token), "{filed} holds {token}");
    }
    assert!(!responder.holds(6));
    drop(waiting);
  }

  #[tokio::test]
  async fn an_answered_request_an_answer_is_kept_under_stays_answered_past_the_bound() {
    let responder = SearchResponder::new(3, KEEP);
    // carol answered 8 in time, but the connection failed after, so the answer is kept under it
    responder.answered(8);
    assert!(responder.keep(8, for_carol(1)).is_none());
    // and 9 while an answer still waits on its attempt
    let waiting = responder.wait_for("carol");
    waiting.rides_on(9);
    responder.answered(9);
    // more requests are answered since than the bound holds
    for token in 10..20 {
      responder.answered(token);
    }
    assert_eq!(responder.claim(8), None);
    assert_eq!(responder.claim(9), None);
    drop(waiting);
  }

  #[tokio::test]
  async fn a_request_claimed_since_does_not_drop_the_mark_of_one_an_answer_is_kept_under() {
    let responder = SearchResponder::new(3, KEEP);
    responder.answered(8);
    assert!(responder.keep(8, for_carol(1)).is_none());
    // late connections take more requests than the bound holds
    let waiting = responder.wait_for("dave");
    for token in 10..14 {
      waiting.rides_on(token);
      assert_eq!(responder.claim(token).as_deref(), Some("dave"));
    }
    assert_eq!(responder.claim(8), None);
    drop(waiting);
  }

  #[tokio::test]
  async fn a_request_the_searcher_answered_while_awaited_is_not_answered_again() {
    use crate::config::Connection;

    let (client, mut server, mut buffer) = logged_in().await;
    let nowhere = std::net::TcpListener::bind("127.0.0.1:0")
      .unwrap()
      .local_addr()
      .unwrap();
    let cancel = CancellationToken::new();
    // an answer to carol's search waits on the attempt to reach her
    let waiting = client.responder().wait_for("carol");
    let mut reaching = Box::pin(client.reach("carol", Some(nowhere), &cancel, |request| {
      waiting.rides_on(request);
    }));
    let request = tokio::select! {
      biased;
      _ = &mut reaching => panic!("the attempt ended before the server had its request"),
      token = request(&mut server, &mut buffer, |request| match request {
        ServerRequest::ConnectToPeer(asked) => Some(asked.token),
        _ => None,
      }) => token,
    };
    let (answered, _carol) = crate::conn::tests::pair(Connection::default()).await;
    let carol = answered.handle().remote();
    client.pierced(request, answered).await.unwrap();
    let reached = reaching.await.ok().unwrap();
    assert_eq!(reached.handle().remote(), carol);
    // someone else answers the same request while the answer still waits on carol
    let (stranger, _stranger) = crate::conn::tests::pair(Connection::default()).await;
    let refused = stranger.handle().clone();
    client.pierced(request, stranger).await.unwrap();
    assert!(
      refused
        .disconnected()
        .await
        .message
        .starts_with("unknown PierceFirewall attempt")
    );
    assert_eq!(client.users().connection_address("carol"), Some(carol));
    drop(waiting);
    // a later answer finds the connection made; the request it was made with is done
    let mut ridden = Vec::new();
    let later = client.responder().wait_for("carol");
    let again = client
      .reach("carol", None, &cancel, |request| {
        ridden.push(request);
        later.rides_on(request);
      })
      .await;
    assert_eq!(again.ok().unwrap().handle().remote(), carol);
    assert!(ridden.is_empty());
  }

  #[tokio::test]
  async fn answers_kept_under_one_request_count_against_the_bound_one_by_one() {
    let responder = SearchResponder::new(3, KEEP);
    for token in 0..5 {
      assert!(responder.keep(7, for_carol(token)).is_none());
    }
    assert_eq!(responder.undelivered().len(), 3);
    let taken: Vec<u32> = responder.take(7).iter().map(|kept| kept.token).collect();
    assert_eq!(taken.len(), 3);
    assert!(taken.is_sorted(), "taken in the order kept: {taken:?}");
    assert_eq!(responder.undelivered().len(), 0);
  }
}
