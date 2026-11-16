//! Downloads between two clients on soulfind, the uploader queueing every request and starting
//! the upload when the test says: asked for with `QueueDownload` or with a transfer request,
//! queued and then started, resumed from an offset, returned early once queued, and told its
//! place in the queue.

use soulseek_client::ErrorKind;
use soulseek_client::transfer::{Download, Handle, RequestStyle};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::support::PATIENCE;
use crate::support::soulfind::{self, User};
use crate::support::transfers::{self, Asked, asked, content, file, once_queued, scratch, states};
use soulseek_client::Event;

const SIZE: usize = 300_000;
const REMOTE: &str = "@@share\\album\\01 song.flac";

/// a downloader and an uploader that queues every request the downloader makes
async fn pair(server: &Arc<soulfind::Soulfind>) -> (User, User, Asked) {
  let (queueing, requests) = transfers::queueing();
  let options = soulfind::listening().handler(queueing);
  let uploader = server.login("up", options).await;
  let downloader = server.login("down", soulfind::listening()).await;
  (downloader, uploader, requests)
}

/// a download of the file by `downloader` from `uploader`, to `destination`
fn download(downloader: &User, uploader: &User, destination: &Path) -> Download {
  downloader
    .client
    .download(&uploader.username, REMOTE)
    .size(SIZE as u64)
    .to_file(destination)
}

/// that download once the uploader has it queued, or the error it ended with before
async fn queued_download(
  downloader: &User,
  uploader: &User,
  destination: &Path,
) -> Result<Handle, soulseek_client::Error> {
  once_queued(download(downloader, uploader, destination).start().await).await
}

/// starts the upload of `path` the downloader asked for
async fn serve(uploader: &User, requests: &mut Asked, path: std::path::PathBuf, start: bool) {
  let (username, filename) = asked(requests).await;
  assert_eq!(filename.as_str(), REMOTE);
  if start {
    let upload = uploader.client.upload(username, filename).from_file(path);
    upload.start().await.expect("the upload starts");
  }
}

#[tokio::test]
async fn a_queued_download_goes_through_every_state_and_arrives_whole() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (mut downloader, mut uploader, mut requests) = pair(&server).await;
  let dir = scratch();
  let source = file(&dir, "source.flac", SIZE);
  let destination = dir.join("destination.flac");
  let download = queued_download(&downloader, &uploader, &destination)
    .await
    .unwrap();
  serve(&uploader, &mut requests, source, true).await;
  let (downloaded, transfer) = states(
    &mut downloader.events,
    soulseek_client::transfer::Direction::Download,
  )
  .await;
  assert_eq!(downloaded, transfers::download());
  assert_eq!(transfer.bytes_transferred, SIZE as u64);
  assert!(transfer.remote_token.is_some());
  let (uploaded, _) = states(
    &mut uploader.events,
    soulseek_client::transfer::Direction::Upload,
  )
  .await;
  assert_eq!(uploaded, transfers::upload());
  let finished = download.await.unwrap();
  assert_eq!(
    finished.state,
    soulseek_client::transfer::State::Completed(soulseek_client::transfer::Outcome::Succeeded)
  );
  assert_eq!(std::fs::read(destination).unwrap(), content(SIZE));
  assert!(downloader.client.transfers().downloads().is_empty());
}

#[tokio::test]
async fn a_download_asked_with_a_transfer_request_is_queued_by_the_answer() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (mut downloader, uploader, mut requests) = pair(&server).await;
  let dir = scratch();
  let source = file(&dir, "source.flac", SIZE);
  let asking = download(&downloader, &uploader, &dir.join("destination.flac"))
    .request_style(RequestStyle::TransferRequest);
  let download = once_queued(asking.start().await).await.unwrap();
  serve(&uploader, &mut requests, source, true).await;
  let (downloaded, _) = states(
    &mut downloader.events,
    soulseek_client::transfer::Direction::Download,
  )
  .await;
  assert_eq!(downloaded, transfers::download());
  download.await.unwrap();
  assert_eq!(
    std::fs::read(dir.join("destination.flac")).unwrap(),
    content(SIZE)
  );
}

#[tokio::test]
async fn a_resumed_download_keeps_what_it_has_and_fetches_the_rest() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (mut downloader, mut uploader, mut requests) = pair(&server).await;
  let dir = scratch();
  let source = file(&dir, "source.flac", SIZE);
  let destination = dir.join("destination.flac");
  // the first 100 000 bytes arrived before, and a stray tail after them goes
  let mut partial = content(100_000);
  partial.extend([0xee; 5000]);
  std::fs::write(&destination, partial).unwrap();
  let resuming = download(&downloader, &uploader, &destination).start_offset(100_000);
  let download = once_queued(resuming.start().await).await.unwrap();
  serve(&uploader, &mut requests, source, true).await;
  let (_, uploaded) = states(
    &mut uploader.events,
    soulseek_client::transfer::Direction::Upload,
  )
  .await;
  assert_eq!(uploaded.start_offset, 100_000);
  assert_eq!(uploaded.bytes_transferred, SIZE as u64);
  let (_, downloaded) = states(
    &mut downloader.events,
    soulseek_client::transfer::Direction::Download,
  )
  .await;
  assert_eq!(downloaded.start_offset, 100_000);
  assert_eq!(downloaded.bytes_transferred, SIZE as u64);
  download.await.unwrap();
  assert_eq!(std::fs::read(destination).unwrap(), content(SIZE));
}

#[tokio::test]
async fn a_resume_from_a_file_shorter_than_the_offset_is_refused_before_asking() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (downloader, uploader, mut requests) = pair(&server).await;
  let dir = scratch();
  let destination = dir.join("destination.flac");
  std::fs::write(&destination, content(10)).unwrap();
  let error = download(&downloader, &uploader, &destination)
    .start_offset(100)
    .await
    .unwrap_err();
  assert!(
    error.kind() == ErrorKind::InvalidArgument
      && error.to_string() == "local file is shorter than the start offset",
    "{error:?}"
  );
  assert!(requests.try_recv().is_err(), "the peer was never asked");
}

#[tokio::test]
async fn a_download_waits_in_the_queue_and_hears_its_place_until_the_upload_starts() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (queueing, mut requests) = transfers::queueing();
  let options = soulfind::listening().handler(queueing.at_place(2));
  let uploader = server.login("up", options).await;
  let mut downloader = server.login("down", soulfind::listening()).await;
  let dir = scratch();
  let source = file(&dir, "source.flac", SIZE);
  // returns once the uploader has the request queued
  let task = queued_download(&downloader, &uploader, &dir.join("destination.flac"))
    .await
    .unwrap();
  let place = downloader
    .event(|event| match event {
      Event::Transfer(soulseek_client::transfer::Event::PlaceInQueue {
        filename, place, ..
      }) if filename.as_str() == REMOTE => Some(place),
      _ => None,
    })
    .await;
  assert_eq!(place, 2);
  serve(&uploader, &mut requests, source.clone(), false).await;
  let asked = downloader
    .client
    .transfers()
    .place_in_queue(&uploader.username, REMOTE)
    .await
    .unwrap();
  assert_eq!(asked, 2);
  // still queued at the uploader while it has not started
  tokio::time::sleep(Duration::from_millis(300)).await;
  let queued = downloader.client.transfers().downloads();
  assert_eq!(queued.len(), 1);
  assert_eq!(
    queued[0].state,
    soulseek_client::transfer::State::QueuedRemotely
  );
  let upload = uploader
    .client
    .upload(&downloader.username, REMOTE)
    .from_file(source);
  upload.await.unwrap();
  let done = tokio::time::timeout(PATIENCE, task).await.unwrap().unwrap();
  assert_eq!(done.bytes_transferred, SIZE as u64);
}

#[tokio::test]
async fn a_download_canceled_in_the_queue_refuses_the_upload_offered_later() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (downloader, mut uploader, mut requests) = pair(&server).await;
  let dir = scratch();
  let source = file(&dir, "source.flac", SIZE);
  let task = queued_download(&downloader, &uploader, &dir.join("destination.flac"))
    .await
    .unwrap();
  serve(&uploader, &mut requests, source.clone(), false).await;
  task.cancel();
  let canceled = task.await.unwrap_err();
  assert!(canceled.is_canceled());
  assert!(!dir.join("destination.flac").exists(), "nothing was opened");
  let upload = uploader
    .client
    .upload(&downloader.username, REMOTE)
    .from_file(source);
  let refused = upload.await.unwrap_err();
  assert!(
    refused.kind() == ErrorKind::TransferRejected && refused.reason() == Some("Cancelled"),
    "{refused:?}"
  );
  let (uploaded, _) = states(
    &mut uploader.events,
    soulseek_client::transfer::Direction::Upload,
  )
  .await;
  assert_eq!(
    uploaded.last(),
    Some(
      &(soulseek_client::transfer::State::Completed(soulseek_client::transfer::Outcome::Rejected))
    )
  );
}

#[tokio::test]
async fn a_peer_that_shares_nothing_refuses_the_download() {
  let Some(server) = soulfind::server() else {
    return;
  };
  // no upload queue at all, so nothing is shared
  let uploader = server.login("up", soulfind::listening()).await;
  let downloader = server.login("down", soulfind::listening()).await;
  for style in [RequestStyle::Queue, RequestStyle::TransferRequest] {
    let destination = scratch().join("destination.flac");
    let asking = download(&downloader, &uploader, &destination).request_style(style);
    let error = asking.await.unwrap_err();
    assert!(
      error.kind() == ErrorKind::TransferRejected && error.reason() == Some("File not shared."),
      "{style:?}: {error:?}"
    );
  }
}
