//! A search whose request is still being written to a scripted server: an answer that comes
//! back meanwhile is taken, as the server can pass the request on before the write is seen to
//! end.

use std::time::Duration;

use crate::support::{FakeServer, Session, listening_options};
use soulseek_client::search::{Outcome, State};
use soulseek_proto::PeerMessage;
use soulseek_proto::peer::SearchResponse;

#[tokio::test]
async fn an_answer_that_comes_while_the_request_is_written_is_taken() {
  let mut options = listening_options();
  options = options.server_connection(|connection| connection.write_queue_size(4096));
  let session = Session::on(FakeServer::with_receive_buffer(4096).await, options).await;
  // the server stops reading, so every later write to it waits, the search's too
  session.stall_server_writes();
  let reserved = session.client.reserve_search_token();
  let token = reserved.token();
  let searching = {
    let client = session.client.clone();
    tokio::spawn(async move {
      let responses = client
        .search("animals")
        .response_limit(1)
        .filter_responses(false)
        .idle_timeout(Duration::from_secs(600))
        .token(reserved)
        .await?;
      Ok::<_, soulseek_client::Error>(responses.all().await)
    })
  };
  // the search runs until its request waits to be written, taking responses by then
  tokio::task::yield_now().await;
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
  // the limit ends the search at once, its request still waiting to be written
  let (search, responses) = searching.await.unwrap().unwrap();
  assert_eq!(
    search.state,
    State::Completed(Outcome::ResponseLimitReached)
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
      let request = client
        .search(text)
        .idle_timeout(Duration::from_millis(200))
        .verbatim();
      Ok::<_, soulseek_client::Error>(request.await?.all().await)
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
