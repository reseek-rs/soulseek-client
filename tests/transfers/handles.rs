//! What a caller holds of a transfer, against a scripted peer: the changes a handle reports, an
//! awaited download or upload dropped before it ends, a download waiting on the server when the
//! client disconnects or shuts down, a transfer found by its token, and requests that can be
//! moved to a task.

use std::future::IntoFuture;

use crate::support::scripted::{BOB, SIZE, Scripted, offer, send_file};
use crate::support::transfers::{file, scratch, states};
use crate::support::{PATIENCE, poll_once};
use soulseek_client::ErrorKind;
use soulseek_client::transfer::{Direction, Handle, Outcome, State};
use soulseek_proto::ServerRequest;

/// holds for a request whose future can go on a task of its own
fn spawnable<R>()
where
  R: IntoFuture + Send + 'static,
  R::IntoFuture: Send + 'static,
{
}

#[test]
fn a_request_can_be_awaited_on_a_task_of_its_own() {
  spawnable::<soulseek_client::transfer::Download<soulseek_client::transfer::Destination>>();
  spawnable::<soulseek_client::transfer::Upload<soulseek_client::transfer::Source>>();
  spawnable::<soulseek_client::search::Request>();
  spawnable::<soulseek_client::user::Browse>();
  spawnable::<soulseek_client::user::Request<soulseek_proto::peer::UserInfo>>();
}

#[tokio::test]
async fn the_changes_of_a_transfer_start_with_it_as_it_is_and_end_once_it_has() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\followed.mp3";
  let (mut running, mut bob) = scripted.queued_download(filename).await;
  let mut changes = running.handle.changes();
  let first = changes.next().await.expect("the transfer as it is now");
  assert_eq!(first.state, State::QueuedRemotely);
  assert_eq!(first.token, running.handle.transfer().token);

  offer(&mut bob, filename, 61, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(61).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let mut seen = Vec::new();
  let followed = async {
    while let Some(change) = changes.next().await {
      seen.push(change);
    }
  };
  tokio::time::timeout(PATIENCE, followed)
    .await
    .expect("the changes end with the transfer");
  // a slow reader skips states, never the last one, and nothing comes after it
  let ended: Vec<bool> = seen
    .iter()
    .map(|change| change.state.is_terminal())
    .collect();
  assert_eq!(ended.iter().filter(|&&ended| ended).count(), 1, "{ended:?}");
  let last = seen.last().unwrap();
  assert_eq!(last.state, State::Completed(Outcome::Succeeded));
  assert_eq!(last.bytes_transferred, SIZE);
  assert!(
    seen
      .windows(2)
      .all(|pair| pair[0].bytes_transferred <= pair[1].bytes_transferred)
  );

  // changes asked for once the transfer has ended are the transfer as it ended, and no more
  let done = (&mut running.handle).await.unwrap();
  let mut after = running.handle.changes();
  let only = after.next().await.expect("the transfer as it ended");
  assert_eq!((only.state, only.token), (done.state, done.token));
  assert!(after.next().await.is_none());
}

#[tokio::test]
async fn the_first_state_of_a_new_transfer_is_one_change() {
  let mut scripted = Scripted::start().await;
  let handle = scripted
    .client()
    .download(BOB, "@@share\\first.mp3")
    .size(SIZE)
    .to_file(scratch().join("first"))
    .start()
    .await
    .unwrap();
  // read before the download's task has run
  let mut changes = handle.changes();
  let first = changes.next().await.unwrap();
  assert_eq!(first.state, State::QueuedLocally);
  // the task has reported its first state once it looks up bob's address
  scripted
    .session
    .server
    .request_matching(|request| matches!(request, ServerRequest::GetPeerAddress(_)).then_some(()))
    .await;
  let mut next = std::pin::pin!(changes.next());
  assert!(
    poll_once(&mut next).await.is_pending(),
    "the same state again"
  );
}

/// a download started from `scripted`'s client once it waits for the server to give bob's
/// address, which the server never does
async fn waiting_on_the_server(scripted: &mut Scripted) -> Handle {
  let handle = scripted
    .client()
    .download(BOB, "@@share\\waiting.mp3")
    .size(SIZE)
    .to_file(scratch().join("waiting"))
    .start()
    .await
    .unwrap();
  scripted
    .session
    .server
    .request_matching(|request| matches!(request, ServerRequest::GetPeerAddress(_)).then_some(()))
    .await;
  handle
}

#[tokio::test]
async fn a_shutdown_cancels_a_download_waiting_on_the_server() {
  let mut scripted = Scripted::start().await;
  let handle = waiting_on_the_server(&mut scripted).await;
  scripted.client().shutdown().await;
  let error = handle.await.unwrap_err();
  assert!(error.is_canceled(), "{error:?}");
}

#[tokio::test]
async fn a_disconnect_fails_a_download_waiting_on_the_server() {
  let mut scripted = Scripted::start().await;
  let handle = waiting_on_the_server(&mut scripted).await;
  scripted.client().disconnect().await;
  let error = handle.await.unwrap_err();
  assert_eq!(
    error.kind(),
    ErrorKind::Connection(soulseek_client::error::ConnectionKind::Other),
    "{error:?}"
  );
  assert!(
    error
      .to_string()
      .starts_with("failed to retrieve address for user bob: "),
    "{error}"
  );
}

#[tokio::test]
async fn an_awaited_download_dropped_before_it_ends_is_canceled() {
  let mut scripted = Scripted::start().await;
  let client = scripted.client().clone();
  let mut events = client.events();
  let destination = scratch().join("dropped");
  let mut pending = client
    .download(BOB, "@@share\\dropped.mp3")
    .size(SIZE)
    .to_file(&destination)
    .into_future();
  // polled until the download looks up bob's address, which the server never gives
  let server = &mut scripted.session.server;
  tokio::select! {
    biased;
    ended = &mut pending => panic!("the download ended: {ended:?}"),
    () = server.request_matching(|request| {
      matches!(request, ServerRequest::GetPeerAddress(_)).then_some(())
    }) => {}
  }
  assert_eq!(client.transfers().downloads().len(), 1);
  drop(pending);
  let (_, last) = states(&mut events, Direction::Download).await;
  assert_eq!(last.state, State::Completed(Outcome::Canceled));
  let forgotten = tokio::time::timeout(PATIENCE, async {
    while !client.transfers().downloads().is_empty() {
      tokio::task::yield_now().await;
    }
  })
  .await;
  assert!(forgotten.is_ok(), "the client still tracks the download");
  assert!(!destination.exists(), "nothing was opened");
}

#[tokio::test]
async fn an_awaited_upload_dropped_before_it_ends_is_canceled() {
  let mut scripted = Scripted::start().await;
  let client = scripted.client().clone();
  let mut events = client.events();
  let source = file(&scratch(), "dropped", SIZE as usize);
  let mut pending = client
    .upload(BOB, "@@share\\dropped.mp3")
    .from_file(&source)
    .into_future();
  // polled until the upload looks up bob's address, which the server never gives
  let server = &mut scripted.session.server;
  tokio::select! {
    biased;
    ended = &mut pending => panic!("the upload ended: {ended:?}"),
    () = server.request_matching(|request| {
      matches!(request, ServerRequest::GetPeerAddress(_)).then_some(())
    }) => {}
  }
  assert_eq!(client.transfers().uploads().len(), 1);
  drop(pending);
  let (_, last) = states(&mut events, Direction::Upload).await;
  assert_eq!(last.state, State::Completed(Outcome::Canceled));
  let forgotten = tokio::time::timeout(PATIENCE, async {
    while !client.transfers().uploads().is_empty() {
      tokio::task::yield_now().await;
    }
  })
  .await;
  assert!(forgotten.is_ok(), "the client still tracks the upload");
}

#[tokio::test]
async fn a_transfer_found_by_its_token_can_be_followed_and_canceled() {
  let mut scripted = Scripted::start().await;
  let handle = waiting_on_the_server(&mut scripted).await;
  let token = handle.transfer().token;
  let found = scripted
    .client()
    .transfers()
    .get(token)
    .expect("the download is found by its token");
  assert_eq!(found.transfer().token, token);
  let mut changes = found.clone().changes();
  assert_eq!(changes.next().await.unwrap().state, State::QueuedLocally);
  found.cancel();
  let error = handle.await.unwrap_err();
  assert!(error.is_canceled(), "{error:?}");
  // the last change is the end, and an ended transfer is not found
  let mut last = None;
  while let Some(change) = changes.next().await {
    last = Some(change.state);
  }
  assert_eq!(last, Some(State::Completed(Outcome::Canceled)));
  assert!(scripted.client().transfers().get(token).is_none());
}
