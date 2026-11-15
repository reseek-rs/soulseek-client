//! A search whose request is still being written to a scripted server: an answer that comes
//! back meanwhile is taken, as the server can pass the request on before the write is seen to
//! end.

use std::time::Duration;

use crate::support::{FakeServer, Session, listening_options};
use soulseek_client::{
  Event, SearchOptions, SearchOutcome, SearchRequest, SearchScope, SearchState,
};
use soulseek_proto::PeerMessage;
use soulseek_proto::peer::SearchResponse;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn an_answer_that_comes_while_the_request_is_written_is_taken() {
  let mut options = listening_options();
  options = options.server_connection(|connection| connection.write_queue_size(4096));
  let mut session = Session::on(FakeServer::with_receive_buffer(4096).await, options).await;
  // the server stops reading, so every later write to it waits, the search's too
  session.stall_server_writes();
  let reserved = session.client.reserve_search_token();
  let token = reserved.token();
  let searching = {
    let client = session.client.clone();
    tokio::spawn(async move {
      let options = SearchOptions {
        response_limit: 1,
        filter_responses: false,
        search_timeout: Duration::from_secs(600),
        ..SearchOptions::default()
      };
      let scope = SearchScope::Network;
      let cancel = CancellationToken::new();
      client
        .search(
          SearchRequest::new("animals", scope, options)
            .unwrap()
            .with_token(reserved),
          &cancel,
        )
        .await
    })
  };
  session
    .event(|event| match event {
      Event::Search(soulseek_client::search::Event::StateChanged { search, .. })
        if search.state == SearchState::InProgress =>
      {
        Some(())
      }
      _ => None,
    })
    .await;
  let mut bob = session.peer("bob").await;
  let answer = PeerMessage::SearchResponse(SearchResponse {
    username: "bob".into(),
    token,
    files: Vec::new(),
    has_free_upload_slot: true,
    upload_speed: 0,
    queue_length: 0,
    locked_files: Vec::new(),
  });
  bob.send(&answer.encode()).await;
  // taken while the write still waits
  let taken = session
    .event(|event| match event {
      Event::Search(soulseek_client::search::Event::ResponseReceived { search, .. }) => {
        Some(search.response_count)
      }
      _ => None,
    })
    .await;
  assert_eq!(taken, 1);
  // the limit ends the search at once, its request still waiting to be written
  let (search, responses) = searching.await.unwrap().unwrap();
  assert_eq!(
    search.state,
    SearchState::Completed(SearchOutcome::ResponseLimitReached)
  );
  assert_eq!(responses.len(), 1);
}

#[tokio::test]
async fn a_verbatim_search_goes_out_as_it_was_given() {
  let mut session = Session::start().await;
  let text = "-live  \"daft punk\"";
  let searching = tokio::spawn({
    let client = session.client.clone();
    async move {
      let options = SearchOptions {
        search_timeout: Duration::from_millis(200),
        ..SearchOptions::default()
      };
      let request = SearchRequest::verbatim(text, SearchScope::Network, options).unwrap();
      client.search(request, &CancellationToken::new()).await
    }
  });
  let sent = session
    .server
    .request_matching(|request| match request {
      soulseek_proto::ServerRequest::FileSearch(search) => Some(search.query),
      _ => None,
    })
    .await;
  assert_eq!(sent, text);
  let (search, _) = searching.await.unwrap().unwrap();
  assert_eq!(search.text, text);
}
