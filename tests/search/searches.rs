//! Our searches between clients on soulfind: every scope, the states a search goes through, the
//! limits and filters on what it takes, the one way our own search reaches our own answers, and
//! a disconnect canceling one under the token it reserved.

use std::sync::Arc;
use std::time::Duration;

use crate::support::soulfind::{self, Soulfind, User};
use crate::support::{PATIENCE, next_event};
use soulseek_client::{
  ClientError, Event, Search, SearchOptions, SearchOutcome, SearchReply, SearchRequest,
  SearchResponseResolver, SearchScope, SearchState,
};
use soulseek_proto::peer::{File, SearchResponse};
use soulseek_proto::{RoomType, SearchQuery};
use tokio_util::sync::CancellationToken;

/// a resolver that answers every search as `username` with `files` files
fn answering(username: String, files: usize) -> SearchResponseResolver {
  Arc::new(move |_, token: u32, _: SearchQuery| {
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
    Box::pin(async move { Ok(Some(SearchReply::Response(response))) })
  })
}

/// a user that answers every search with `files` files
async fn responder(server: &Arc<Soulfind>, prefix: &str, files: usize) -> User {
  let username = soulfind::username(prefix);
  let mut options = soulfind::listening();
  options.resolvers.search_response = Some(answering(username.clone(), files));
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
  scope: SearchScope,
  options: SearchOptions,
) -> (Search, Vec<SearchResponse>) {
  tokio::time::timeout(
    PATIENCE,
    searcher.client.search(
      SearchRequest::new("flac match", scope, options).unwrap(),
      &CancellationToken::new(),
    ),
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
  // the searcher answers too, but never its own search unless it asks itself
  let mut searcher = responder_like(&server).await;
  let room = soulfind::username("searched");
  for user in [&responder, &searcher] {
    user
      .client
      .join_room(&room, RoomType::Public)
      .await
      .unwrap();
  }
  let scopes = [SearchScope::Network, SearchScope::Rooms(vec![room.clone()])];
  for scope in scopes {
    let mut events = searcher.client.subscribe();
    let (done, responses) = search(&searcher, scope.clone(), quick()).await;
    assert_eq!(
      done.state,
      SearchState::Completed(SearchOutcome::TimedOut),
      "{scope:?}"
    );
    // the server is shared with other tests, whose users answer a network search as well
    let ours: Vec<_> = responses
      .iter()
      .map(|response| response.username.as_str())
      .filter(|&from| from == responder.username || from == searcher.username)
      .collect();
    assert_eq!(ours, [responder.username.as_str()], "{scope:?}");
    let files: usize = responses.iter().map(|response| response.files.len()).sum();
    assert_eq!(
      (done.response_count, done.file_count),
      (responses.len() as u64, files as u64)
    );
    assert_eq!(done.query.search_text(), "flac match");
    let states = search_states(&mut events).await;
    assert_eq!(
      states,
      [
        SearchState::InProgress,
        SearchState::Completed(SearchOutcome::TimedOut),
      ]
    );
  }
  // asking ourselves on purpose reaches our own answers
  let (done, responses) = search(
    &searcher,
    SearchScope::Users(vec![searcher.username.clone()]),
    quick(),
  )
  .await;
  assert_eq!(done.response_count, 1);
  assert_eq!(responses[0].username, searcher.username);
  searcher
    .event(|event| matches!(event, Event::SearchResponseDelivered(_)).then_some(()))
    .await;
}

/// a searcher that answers every search with one file of its own
async fn responder_like(server: &Arc<Soulfind>) -> User {
  responder(server, "searcher", 1).await
}

/// the states of the next search, until it completed
async fn search_states(events: &mut tokio::sync::broadcast::Receiver<Event>) -> Vec<SearchState> {
  let mut states = Vec::new();
  loop {
    let state = next_event(events, |event| match event {
      Event::SearchStateChanged { search, .. } => Some(search.state),
      _ => None,
    })
    .await;
    states.push(state);
    if state.is_terminal() {
      return states;
    }
  }
}

#[tokio::test]
async fn a_wishlist_search_goes_out_like_a_network_search() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let responder = responder(&server, "responder", 1).await;
  let searcher = server.login("searcher", soulfind::listening()).await;
  let (done, responses) = search(&searcher, SearchScope::Wishlist, quick()).await;
  assert_eq!(done.state, SearchState::Completed(SearchOutcome::TimedOut));
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
  let (done, responses) = search(&searcher, SearchScope::Network, options).await;
  assert_eq!(
    done.state,
    SearchState::Completed(SearchOutcome::ResponseLimitReached)
  );
  assert_eq!(responses.len(), 1);
  let options = SearchOptions {
    file_limit: 3,
    search_timeout: Duration::from_secs(30),
    ..SearchOptions::default()
  };
  let (done, _) = search(&searcher, SearchScope::Network, options).await;
  assert_eq!(
    done.state,
    SearchState::Completed(SearchOutcome::FileLimitReached)
  );
  assert!(done.file_count >= 3);
}

#[tokio::test]
async fn filtered_out_responses_are_not_taken() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let responder = responder(&server, "responder", 2).await;
  let searcher = server.login("searcher", soulfind::listening()).await;
  let (_, responses) = search(&searcher, SearchScope::Network, quick()).await;
  assert!(
    responses
      .iter()
      .any(|response| response.username == responder.username),
    "the responder answers when nothing filters it"
  );
  let mut options = quick();
  options.minimum_peer_upload_speed = 1_000_000;
  let (done, responses) = search(&searcher, SearchScope::Network, options).await;
  // no user on the server uploads this fast
  assert!(responses.is_empty());
  assert_eq!(done.response_count, 0);
}

#[tokio::test]
async fn a_disconnect_cancels_a_running_search() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let searcher = server.login("searcher", soulfind::listening()).await;
  let mut events = searcher.client.subscribe();
  let reserved = searcher.client.reserve_search_token();
  let token = reserved.token();
  let running = tokio::spawn({
    let client = searcher.client.clone();
    async move {
      let options = SearchOptions {
        search_timeout: Duration::from_secs(60),
        ..SearchOptions::default()
      };
      client
        .search(
          SearchRequest::new("long wait", SearchScope::Network, options)
            .unwrap()
            .with_token(reserved),
          &CancellationToken::new(),
        )
        .await
    }
  });
  next_event(&mut events, |event| match event {
    Event::SearchStateChanged { search, .. } if search.state == SearchState::InProgress => Some(()),
    _ => None,
  })
  .await;
  searcher.client.disconnect(None, None).await;
  let result = tokio::time::timeout(PATIENCE, running)
    .await
    .unwrap()
    .unwrap();
  assert!(matches!(result, Err(ClientError::Cancelled)), "{result:?}");
  let ended = next_event(&mut events, |event| match event {
    Event::SearchStateChanged { search, .. } if search.state.is_terminal() => {
      Some((search.token, search.state))
    }
    _ => None,
  })
  .await;
  assert_eq!(
    ended,
    (token, SearchState::Completed(SearchOutcome::Cancelled))
  );
}
