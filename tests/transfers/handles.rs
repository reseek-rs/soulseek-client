//! What a caller holds of a transfer, against a scripted peer: the changes a handle reports, and
//! a download dropped before it has a handle.

use std::future::IntoFuture;

use crate::support::PATIENCE;
use crate::support::scripted::{BOB, SIZE, Scripted, offer, send_file};
use crate::support::transfers::{scratch, states};
use soulseek_client::transfer::{Direction, Outcome, State};
use soulseek_proto::ServerRequest;

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
async fn a_download_dropped_before_it_has_a_handle_is_canceled() {
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
    started = &mut pending => panic!("the download has a handle: {started:?}"),
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
