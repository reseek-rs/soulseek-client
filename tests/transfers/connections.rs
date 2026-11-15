//! Transfer connections between two clients on soulfind: when the downloader cannot be dialed,
//! the uploader has the server relay its request and the downloader dials it; a download waits
//! no longer than the accept timeout, and a relayed request nothing expects is refused.

use std::time::Duration;

use crate::support::soulfind;
use crate::support::{Logs, PATIENCE};
use soulseek_client::Error;
use soulseek_client::testing::{ConnKind, Engine};
use soulseek_proto::RemoteFilename;
use tokio_util::sync::CancellationToken;

fn file() -> RemoteFilename {
  "@@music\\artist\\01 intro.flac".into()
}

#[tokio::test]
async fn an_uploader_that_cannot_dial_the_download_asks_the_server_to_have_it_dial() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let downloader = server.login("down", soulfind::unreachable()).await;
  let uploader = server.login("up", soulfind::listening()).await;
  let incoming = downloader.client.await_transfer_connection(
    &uploader.username,
    file(),
    42,
    &CancellationToken::new(),
  );
  let endpoint = uploader
    .client
    .users()
    .address(&downloader.username)
    .await
    .unwrap();
  let mut outgoing = uploader
    .client
    .get_transfer_connection(
      &downloader.username,
      endpoint,
      42,
      &CancellationToken::new(),
    )
    .await
    .unwrap();
  assert_eq!(
    outgoing.handle().kind(),
    ConnKind::OUTBOUND | ConnKind::INDIRECT
  );
  let mut received = tokio::time::timeout(PATIENCE, incoming)
    .await
    .unwrap()
    .unwrap();
  assert_eq!(
    received.handle().kind(),
    ConnKind::INBOUND | ConnKind::INDIRECT
  );
  received.write_all(&7u64.to_le_bytes()).await.unwrap();
  assert_eq!(outgoing.read_exact(8).await.unwrap(), 7u64.to_le_bytes());
}

#[tokio::test]
async fn a_download_waits_no_longer_than_the_accept_timeout() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let mut options = soulfind::listening();
  options = options.transfer_accept_timeout(Duration::from_millis(200));
  let downloader = server.login("down", options).await;
  let error = downloader
    .client
    .await_transfer_connection("nobody", file(), 43, &CancellationToken::new())
    .await
    .unwrap_err();
  assert!(
    matches!(&error, Error::Timeout(message) if message.starts_with(
      "failed to establish a direct or indirect transfer connection to nobody with remote token 43"
    )),
    "{error:?}"
  );
}

#[tokio::test]
async fn a_relayed_transfer_request_nothing_expects_is_refused() {
  let logs = Logs::capture();
  let Some(server) = soulfind::server() else {
    return;
  };
  let downloader = server.login("down", soulfind::unreachable()).await;
  let mut uploader_options = soulfind::listening();
  uploader_options = uploader_options
    .transfer_connection(|connection| connection.connect_timeout(Duration::from_secs(5)));
  let uploader = server.login("up", uploader_options).await;
  let endpoint = uploader
    .client
    .users()
    .address(&downloader.username)
    .await
    .unwrap();
  let started = tokio::time::Instant::now();
  let error = uploader
    .client
    .get_transfer_connection(
      &downloader.username,
      endpoint,
      44,
      &CancellationToken::new(),
    )
    .await
    .unwrap_err();
  assert!(matches!(error, Error::Connection { .. }), "{error:?}");
  // the downloader said it would not connect, so the uploader did not wait out its timeout
  assert!(started.elapsed() < Duration::from_secs(4));
  assert_eq!(
    logs
      .lines_with(&["unexpected transfer request; ignored"])
      .len(),
    1
  );
}
