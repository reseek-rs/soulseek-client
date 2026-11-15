//! The client's searches between clients on soulfind: every scope, the states a search goes
//! through, the limits and filters on what it takes, the client's own search reaching its own
//! answers, and a disconnect canceling one under the token it reserved.

use std::sync::Arc;
use std::time::Duration;

use crate::support::PATIENCE;
use crate::support::search::SearchOptions;
use crate::support::soulfind::{self, Soulfind, User};
use soulseek_client::Event;
use soulseek_client::handler::SearchReply;
use soulseek_client::search::{Outcome, Scope, Search, State};
use soulseek_proto::RoomType;
use soulseek_proto::peer::{File, SearchResponse};

/// a resolver that answers every search as `username` with `files` files
fn answering(username: String, files: usize) -> crate::support::handler::SearchResolver {
  Arc::new(move |_, token: u32, _: String| {
    let response = SearchResponse {
      username: username.clone(),
      token,
      files: (0..files)
        .map(|n| File {
          code: 1,
          filename: format!("@@music\\match {n}.flac").as_str().into(),
          size: 1000,
          extension: "flac".into(),
          attributes: Vec::new(),
        })
        .collect(),
      has_free_upload_slot: true,
      upload_speed: 5000,
      queue_length: 0,
      locked_files: Vec::new(),
    };
    Box::pin(async move { Some(SearchReply::Response(response)) })
  })
}

/// a user that answers every search with `files` files
async fn responder(server: &Arc<Soulfind>, prefix: &str, files: usize) -> User {
  let username = soulfind::username(prefix);
  let mut options = soulfind::listening();
  options = options.handler({
    let mut resolvers = crate::support::answering();
    resolvers.search_response = Some(answering(username.clone(), files));
    resolvers
  });
  server.login_as(username, options).await
}

/// options for a search that ends soon after its last response
fn quick() -> SearchOptions {
  SearchOptions {
    search_timeout: Duration::from_millis(1500),
    ..SearchOptions::default()
  }
}

async fn search(
  searcher: &User,
  scope: Scope,
  options: SearchOptions,
) -> (Search, Vec<SearchResponse>) {
  tokio::time::timeout(
    PATIENCE,
    crate::support::search::run(&searcher.client, "flac match", scope, options, None),
  )
  .await
  .expect("the search ends")
  .unwrap()
}

#[tokio::test]
async fn network_and_room_searches_take_the_answers_and_end_when_they_stop() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let responder = responder(&server, "responder", 2).await;
  // the searcher answers too, and its own search reaches its resolver like any other
  let mut searcher = responder_like(&server).await;
  let room = soulfind::username("searched");
  for user in [&responder, &searcher] {
    user
      .client
      .rooms()
      .join(&room, RoomType::Public)
      .await
      .unwrap();
  }
  let scopes = [Scope::Network, Scope::Rooms(vec![room.clone()])];
  for scope in scopes {
    let (done, responses) = search(&searcher, scope.clone(), quick()).await;
    assert_eq!(done.state, State::Completed(Outcome::TimedOut), "{scope:?}");
    // the server is shared with other tests, whose users answer a network search as well
    let mut ours: Vec<_> = responses
      .iter()
      .map(|response| response.username.as_str())
      .filter(|&from| from == responder.username || from == searcher.username)
      .collect();
    ours.sort_unstable();
    let mut both = [responder.username.as_str(), searcher.username.as_str()];
    both.sort_unstable();
    assert_eq!(ours, both, "{scope:?}");
    let files: usize = responses.iter().map(|response| response.files.len()).sum();
    assert_eq!(
      (done.response_count, done.file_count),
      (responses.len() as u64, files as u64)
    );
    assert_eq!(done.query.search_text(), "flac match");
  }
  // the client asking only itself reaches its own answers too
  let (done, responses) = search(
    &searcher,
    Scope::Users(vec![searcher.username.clone()]),
    quick(),
  )
  .await;
  assert_eq!(done.response_count, 1);
  assert_eq!(responses[0].username, searcher.username);
  searcher
    .event(|event| {
      matches!(
        event,
        Event::Search(soulseek_client::search::Event::ResponseDelivered(_))
      )
      .then_some(())
    })
    .await;
}

/// a searcher that answers every search with one file of its own
async fn responder_like(server: &Arc<Soulfind>) -> User {
  responder(server, "searcher", 1).await
}

#[tokio::test]
async fn a_wishlist_search_goes_out_like_a_network_search() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let responder = responder(&server, "responder", 1).await;
  let searcher = server.login("searcher", soulfind::listening()).await;
  let (done, responses) = search(&searcher, Scope::Wishlist, quick()).await;
  assert_eq!(done.state, State::Completed(Outcome::TimedOut));
  assert!(
    responses
      .iter()
      .any(|response| response.username == responder.username)
  );
}

#[tokio::test]
async fn a_search_ends_once_it_has_enough_responses_or_files() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let _first = responder(&server, "responder", 5).await;
  let _second = responder(&server, "responder", 5).await;
  let searcher = server.login("searcher", soulfind::listening()).await;
  let options = SearchOptions {
    response_limit: 1,
    search_timeout: Duration::from_secs(30),
    ..SearchOptions::default()
  };
  let (done, responses) = search(&searcher, Scope::Network, options).await;
  assert_eq!(done.state, State::Completed(Outcome::ResponseLimitReached));
  assert_eq!(responses.len(), 1);
  let options = SearchOptions {
    file_limit: 3,
    search_timeout: Duration::from_secs(30),
    ..SearchOptions::default()
  };
  let (done, _) = search(&searcher, Scope::Network, options).await;
  assert_eq!(done.state, State::Completed(Outcome::FileLimitReached));
  assert!(done.file_count >= 3);
}

#[tokio::test]
async fn filtered_out_responses_are_not_taken() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let responder = responder(&server, "responder", 2).await;
  let searcher = server.login("searcher", soulfind::listening()).await;
  let (_, responses) = search(&searcher, Scope::Network, quick()).await;
  assert!(
    responses
      .iter()
      .any(|response| response.username == responder.username),
    "the responder answers when nothing filters it"
  );
  let mut options = quick();
  options.minimum_peer_upload_speed = 1_000_000;
  let (done, responses) = search(&searcher, Scope::Network, options).await;
  // no user on the server uploads this fast
  assert!(responses.is_empty());
  assert_eq!(done.response_count, 0);
}

#[tokio::test]
async fn a_search_ends_at_its_maximum_duration_whatever_its_timeout() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let searcher = server.login("searcher", soulfind::listening()).await;
  let options = SearchOptions {
    search_timeout: Duration::from_secs(600),
    maximum_duration: Some(Duration::from_millis(300)),
    ..SearchOptions::default()
  };
  let (done, _) = search(&searcher, Scope::Network, options).await;
  assert_eq!(
    done.state,
    State::Completed(Outcome::MaximumDurationReached)
  );
}

#[tokio::test]
async fn a_search_outlives_the_session_when_the_options_say_so() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let mut options = soulfind::listening();
  options = options.cancel_searches_on_disconnect(false);
  let searcher = server.login("searcher", options).await;
  let responses = quick()
    .on(searcher.client.search("outlives"))
    .await
    .unwrap();
  searcher.client.disconnect(None, None).await;
  let (search, _) = tokio::time::timeout(PATIENCE, responses.all())
    .await
    .unwrap();
  assert_eq!(search.state, State::Completed(Outcome::TimedOut));
}

#[tokio::test]
async fn a_disconnect_cancels_a_running_search() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let searcher = server.login("searcher", soulfind::listening()).await;
  let reserved = searcher.client.reserve_search_token();
  let token = reserved.token();
  let options = SearchOptions {
    search_timeout: Duration::from_secs(60),
    ..SearchOptions::default()
  };
  let responses = options
    .on(searcher.client.search("long wait"))
    .token(reserved)
    .await
    .unwrap();
  searcher.client.disconnect(None, None).await;
  let (search, _) = tokio::time::timeout(PATIENCE, responses.all())
    .await
    .unwrap();
  assert_eq!(
    (search.token, search.state),
    (token, State::Completed(Outcome::Canceled))
  );
}
