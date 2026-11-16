//! Uploads to a scripted downloader, for what a real downloader does not do on demand: refusing,
//! never answering, asking to resume past the end or with Soulseek NS's broken offset, closing
//! early or never, the upload's slot and source misbehaving, and the upload canceled at
//! each stage, with what the downloader is told afterwards.

use soulseek_client::ErrorKind;
use soulseek_client::transfer::{Handle, Slot, Source, Transfer, Upload, UploadNotice};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::support::peer::PeerSide;
use crate::support::scripted::{BOB, Scripted};
use crate::support::transfers::{self, content, file, once_queued, scratch, states};
use crate::support::{Logs, PATIENCE, listening_options};
use soulseek_client::{Error, Event};
use soulseek_proto::peer::{
  QueueDownload, TransferRequest, TransferResponse, UploadDenied, UploadFailed,
};
use soulseek_proto::server::{GetPeerAddressRequest, GetPeerAddressResponse};
use soulseek_proto::{PeerMessage, ServerRequest, ServerResponse};
use tokio::io::AsyncWriteExt;

const SIZE: usize = 50_000;

/// An upload to bob
struct Running {
  /// the upload's handle, or the error of an upload that ended as it was queued
  started: Result<Handle, Error>,
  source: PathBuf,
}

impl Running {
  fn handle(&self) -> &Handle {
    self.started.as_ref().expect("the upload is queued")
  }

  async fn result(self) -> Result<Transfer, Error> {
    tokio::time::timeout(PATIENCE, async { self.started?.await })
      .await
      .expect("the upload ends")
  }
}

/// starts uploading a file of [`SIZE`] bytes to bob as `filename`, the upload shaped by `shape`
async fn upload(
  scripted: &Scripted,
  filename: &str,
  shape: impl FnOnce(Upload<Source>) -> Upload<Source>,
) -> Running {
  let source = file(&scratch(), "upload", SIZE);
  let upload = scripted.client().upload(BOB, filename).from_file(&source);
  Running {
    started: once_queued(shape(upload).start().await).await,
    source,
  }
}

/// bob reads the client's offer to send `filename` and returns its token
async fn offered(bob: &mut PeerSide, filename: &str) -> u32 {
  let PeerMessage::TransferRequest(TransferRequest::Upload {
    token,
    filename: offered,
    file_size,
  }) = bob.read_peer().await
  else {
    panic!("an upload offer");
  };
  assert_eq!(offered.as_str(), filename);
  assert_eq!(file_size, Some(SIZE as u64));
  token
}

async fn allow(bob: &mut PeerSide, token: u32) {
  bob
    .send(
      &PeerMessage::TransferResponse(TransferResponse::Allowed {
        token,
        file_size: None,
      })
      .encode(),
    )
    .await;
}

/// bob takes the upload offered as `filename` up to the transfer connection, which it returns
async fn accepted(scripted: &mut Scripted, bob: &mut PeerSide, filename: &str) -> PeerSide {
  let token = offered(bob, filename).await;
  allow(bob, token).await;
  let (transfer, named) = scripted.accept_transfer().await;
  assert_eq!(named, token);
  transfer
}

/// bob asks to start at `start` and reads the rest of the file
async fn receive(transfer: &mut PeerSide, start: u64) {
  transfer.send(&start.to_le_bytes()).await;
  let rest = transfer.raw(SIZE - start as usize).await;
  assert_eq!(rest, content(SIZE)[start as usize..]);
}

/// an upload to bob up to the point bob has the offer, with bob's message connection
async fn offered_upload(scripted: &mut Scripted, filename: &str) -> (Running, PeerSide) {
  let running = upload(scripted, filename, |upload| upload).await;
  let bob = scripted.connected().await;
  (running, bob)
}

/// `upload` with a governor that lets the first 1000 bytes through and nothing after, so the
/// upload stays in progress
fn first_chunk_only(upload: Upload<Source>) -> Upload<Source> {
  let granted = AtomicU32::new(0);
  upload.governor(move |_, wanted| {
    let first = granted.fetch_add(1, Ordering::SeqCst) == 0;
    async move {
      if first {
        wanted.min(1000)
      } else {
        std::future::pending().await
      }
    }
  })
}

fn told_failed(message: PeerMessage, filename: &str) -> bool {
  message
    == PeerMessage::UploadFailed(UploadFailed {
      filename: filename.into(),
    })
}

fn told_canceled(message: PeerMessage, filename: &str) -> bool {
  message
    == PeerMessage::UploadDenied(UploadDenied {
      filename: filename.into(),
      reason: "Cancelled".into(),
    })
}

#[tokio::test]
async fn an_upload_ends_when_the_downloader_closes_after_the_last_byte() {
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\a.mp3").await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\a.mp3").await;
  receive(&mut transfer, 0).await;
  let started = tokio::time::Instant::now();
  drop(transfer);
  let done = running.result().await.unwrap();
  assert!(
    started.elapsed() < Duration::from_secs(2),
    "no linger once closed"
  );
  assert_eq!(done.bytes_transferred, SIZE as u64);
  assert_eq!(done.closed_by_peer, Some(true));
  assert!(
    bob.quiet_for(Duration::from_millis(200)).await,
    "nothing to tell"
  );
}

#[tokio::test]
async fn a_downloader_that_refuses_ends_the_upload_untold_and_one_that_never_answers_hears_it_failed()
 {
  // a refusal, "Queued" included, since a downloader may not queue the uploader
  for reason in ["Queued", "Cancelled"] {
    let mut scripted = Scripted::start().await;
    let (running, mut bob) = offered_upload(&mut scripted, "@@share\\r.mp3").await;
    let token = offered(&mut bob, "@@share\\r.mp3").await;
    bob
      .send(
        &PeerMessage::TransferResponse(TransferResponse::Denied {
          token,
          message: reason.into(),
        })
        .encode(),
      )
      .await;
    let error = running.result().await.unwrap_err();
    assert!(
      error.kind() == ErrorKind::TransferRejected && error.reason() == Some(reason),
      "{error:?}"
    );
    // told it failed, it would queue the file it just refused
    assert!(
      bob.quiet_for(Duration::from_millis(200)).await,
      "a refusal is not answered"
    );
  }

  // no answer
  let mut options = listening_options();
  options = options.transfer_response_timeout(Duration::from_millis(200));
  let mut scripted = Scripted::with_options(options).await;
  let mut events = scripted.client().events();
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\t.mp3").await;
  offered(&mut bob, "@@share\\t.mp3").await;
  assert!(matches!(running.result().await.unwrap_err(), matched if matched.is_timeout()));
  let (_, last) = states(&mut events, soulseek_client::transfer::Direction::Upload).await;
  assert_eq!(
    last.state,
    soulseek_client::transfer::State::Completed(soulseek_client::transfer::Outcome::TimedOut)
  );
  assert!(told_failed(bob.read_peer().await, "@@share\\t.mp3"));
}

#[tokio::test]
async fn a_downloader_that_has_the_whole_file_gets_nothing_more() {
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\whole.mp3").await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\whole.mp3").await;
  receive(&mut transfer, SIZE as u64).await;
  drop(transfer);
  assert_eq!(
    running.result().await.unwrap().bytes_transferred,
    SIZE as u64
  );
}

#[tokio::test]
async fn an_offset_past_the_end_fails_the_upload_and_the_broken_one_is_logged_once() {
  let logs = Logs::capture();
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\past.mp3").await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\past.mp3").await;
  transfer.send(&(SIZE as u64 + 1).to_le_bytes()).await;
  let error = running.result().await.unwrap_err();
  assert!(
    error.kind() == ErrorKind::Transfer
      && error
        .to_string()
        .starts_with("transfer error: requested start offset of"),
    "{error:?}"
  );
  assert!(told_failed(bob.read_peer().await, "@@share\\past.mp3"));
  // the transfer connection is closed for that reason, not left to its inactivity timeout
  logs
    .wait_for(&["connection closed", "requested start offset of"])
    .await;

  // Soulseek NS resuming a file over 2 GiB, twice
  for name in ["@@share\\ns1.mp3", "@@share\\ns2.mp3"] {
    let running = upload(&scripted, name, |upload| upload).await;
    let mut transfer = accepted(&mut scripted, &mut bob, name).await;
    transfer.send(&u64::MAX.to_le_bytes()).await;
    assert!(
      matches!(running.result().await.unwrap_err(), matched if matched.kind() == ErrorKind::Transfer)
    );
    assert!(told_failed(bob.read_peer().await, name));
  }
  assert_eq!(
    logs
      .lines_with(&["peer sent an invalid resume offset", "bob"])
      .len(),
    1
  );
}

#[tokio::test]
async fn a_failed_upload_tells_the_downloader_which_may_queue_the_file_again_at_once() {
  let (queueing, mut requests) = transfers::queueing();
  let mut scripted = Scripted::with_options(listening_options().handler(queueing)).await;
  let running = upload(&scripted, "@@share\\shrunk.mp3", |upload| upload).await;
  let mut bob = scripted.connected().await;
  let source = running.source.clone();
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\shrunk.mp3").await;
  std::fs::write(&source, content(SIZE / 2)).unwrap();
  transfer.send(&0u64.to_le_bytes()).await;
  let error = running.result().await.unwrap_err();
  assert!(error.kind() == ErrorKind::Transfer, "{error:?}");
  // told as soon as the upload's record is gone, so asking again finds no duplicate
  assert!(told_failed(bob.read_peer().await, "@@share\\shrunk.mp3"));
  bob
    .send(
      &PeerMessage::QueueDownload(QueueDownload {
        filename: "@@share\\shrunk.mp3".into(),
      })
      .encode(),
    )
    .await;
  let (username, filename) = transfers::asked(&mut requests).await;
  scripted
    .client()
    .upload(username, filename)
    .from_file(source)
    .start()
    .await
    .unwrap();
}

/// A close before the start offset, a reset included, may be the network's as well as the
/// downloader's, which the socket does not tell apart: the downloader is told the upload failed,
/// which one that let the file go ignores
#[tokio::test]
async fn a_downloader_that_closes_before_its_start_offset_is_told_the_upload_failed() {
  // how many of the offset's eight bytes it sends first, and whether it resets
  for (sent, reset) in [(0, false), (3, false), (0, true)] {
    let mut scripted = Scripted::start().await;
    let (running, mut bob) = offered_upload(&mut scripted, "@@share\\early.mp3").await;
    let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\early.mp3").await;
    if sent > 0 {
      transfer.send(&0u64.to_le_bytes()[..sent]).await;
    }
    if reset {
      socket2::SockRef::from(&transfer.stream)
        .set_linger(Some(Duration::ZERO))
        .unwrap();
    }
    drop(transfer);
    let error = running.result().await.unwrap_err();
    assert_eq!(
      (error.to_string(), error.kind()),
      (
        "transfer failed: the downloader closed the connection".to_string(),
        ErrorKind::Connection(soulseek_client::error::ConnectionKind::RemoteClosed)
      ),
      "sent {sent}, reset {reset}"
    );
    assert!(
      told_failed(bob.read_peer().await, "@@share\\early.mp3"),
      "sent {sent}, reset {reset}"
    );
  }
}

/// A close or a reset mid-file may be the network's as well as the downloader's: the downloader
/// is told the upload failed, so it asks again unless it let the file go
#[tokio::test]
async fn a_downloader_that_closes_or_resets_mid_file_is_told_the_upload_failed() {
  for reset in [false, true] {
    let mut scripted = Scripted::start().await;
    let running = upload(&scripted, "@@share\\left.mp3", first_chunk_only).await;
    let mut bob = scripted.connected().await;
    let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\left.mp3").await;
    transfer.send(&0u64.to_le_bytes()).await;
    assert_eq!(transfer.raw(1000).await, content(1000));
    if reset {
      // with no linger the close resets the connection, as a close with unread bytes does
      socket2::SockRef::from(&transfer.stream)
        .set_linger(Some(Duration::ZERO))
        .unwrap();
    }
    drop(transfer);
    let error = running.result().await.unwrap_err();
    assert_eq!(
      error.to_string(),
      "transfer failed: the downloader closed the connection",
      "reset: {reset}"
    );
    assert_eq!(
      error.kind(),
      ErrorKind::Connection(soulseek_client::error::ConnectionKind::RemoteClosed)
    );
    assert!(
      told_failed(bob.read_peer().await, "@@share\\left.mp3"),
      "reset: {reset}"
    );
  }
}

/// Every byte written, the upload stands however the downloader closes: a reset is how one that
/// read everything and closes abortively ends it, as well as one that left bytes unread
#[tokio::test]
async fn a_downloader_that_resets_after_the_last_byte_leaves_the_upload_standing() {
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\reset.mp3", |upload| upload).await;
  let mut changes = running.handle().changes();
  let mut bob = scripted.connected().await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\reset.mp3").await;
  transfer.send(&0u64.to_le_bytes()).await;
  assert_eq!(transfer.raw(SIZE - 10).await, content(SIZE - 10));
  // every byte is written, so the upload waits for the downloader to close; it resets instead,
  // its last bytes unread
  let written = async { while changes.next().await.unwrap().bytes_transferred < SIZE as u64 {} };
  tokio::time::timeout(PATIENCE, written)
    .await
    .expect("every byte is written");
  socket2::SockRef::from(&transfer.stream)
    .set_linger(Some(Duration::ZERO))
    .unwrap();
  drop(transfer);
  let done = running.result().await.unwrap();
  assert_eq!(done.bytes_transferred, SIZE as u64);
  assert!(bob.quiet_for(Duration::from_millis(200)).await);
}

#[tokio::test]
async fn an_upload_that_fails_after_the_server_session_was_lost_still_tells_the_downloader() {
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\outage.mp3").await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\outage.mp3").await;
  // the server goes away without the application asking to disconnect
  scripted.session.server.stream.shutdown().await.unwrap();
  scripted
    .session
    .event(|event| {
      matches!(
        event,
        Event::Session(soulseek_client::session::Event::Disconnected { .. })
      )
      .then_some(())
    })
    .await;
  std::fs::write(&running.source, content(SIZE / 2)).unwrap();
  transfer.send(&0u64.to_le_bytes()).await;
  let error = running.result().await.unwrap_err();
  assert!(error.kind() == ErrorKind::Transfer, "{error:?}");
  assert!(told_failed(bob.read_peer().await, "@@share\\outage.mp3"));
}

#[tokio::test]
async fn a_downloader_that_never_closes_is_closed_after_the_linger_and_the_upload_stands() {
  let logs = Logs::capture();
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\linger.mp3", |upload| {
    upload.linger(Duration::from_millis(200))
  })
  .await;
  let mut bob = scripted.connected().await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\linger.mp3").await;
  receive(&mut transfer, 0).await;
  // kept open; the client closes it, and says the downloader did not
  transfer.closed().await;
  let done = running.result().await.unwrap();
  assert_eq!(
    (done.bytes_transferred, done.closed_by_peer),
    (SIZE as u64, Some(false))
  );
  assert_eq!(
    logs
      .lines_with(&["exceeding the maximum linger time"])
      .len(),
    1
  );

  // canceling during the linger leaves a delivered upload succeeded, and the peer untold
  let running = upload(&scripted, "@@share\\linger2.mp3", |upload| {
    upload.linger(Duration::from_secs(30))
  })
  .await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\linger2.mp3").await;
  receive(&mut transfer, 0).await;
  tokio::time::sleep(Duration::from_millis(100)).await;
  running.handle().cancel();
  assert!(running.result().await.unwrap().state.succeeded());
  assert!(bob.quiet_for(Duration::from_millis(200)).await);
}

#[tokio::test]
async fn an_upload_canceled_at_any_stage_tells_the_downloader_to_drop_it() {
  // waiting for the application's slot
  let mut scripted = Scripted::start().await;
  let waiting = upload(&scripted, "@@share\\waiting.mp3", |upload| {
    // never admits the upload: the wait is dropped when the upload is canceled
    upload.slot(|_| std::future::pending::<Slot>())
  })
  .await;
  waiting.handle().cancel();
  // the first message bob reads, so no offer came before it
  let mut bob = scripted.connected().await;
  assert!(told_canceled(bob.read_peer().await, "@@share\\waiting.mp3"));
  assert!(matches!(waiting.result().await.unwrap_err(), matched if matched.is_canceled()));

  // waiting for the downloader's answer
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\asked.mp3").await;
  offered(&mut bob, "@@share\\asked.mp3").await;
  running.handle().cancel();
  assert!(matches!(running.result().await.unwrap_err(), matched if matched.is_canceled()));
  assert!(told_canceled(bob.read_peer().await, "@@share\\asked.mp3"));

  // waiting for the downloader to say where to start
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\init.mp3").await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\init.mp3").await;
  running.handle().cancel();
  assert!(matches!(running.result().await.unwrap_err(), matched if matched.is_canceled()));
  transfer.closed().await;
  assert!(told_canceled(bob.read_peer().await, "@@share\\init.mp3"));

  // with bytes moving, held back by a governor that allows nothing after the first chunk
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\moving.mp3", first_chunk_only).await;
  let mut bob = scripted.connected().await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\moving.mp3").await;
  transfer.send(&0u64.to_le_bytes()).await;
  assert_eq!(transfer.raw(1000).await, content(1000));
  running.handle().cancel();
  let error = running.result().await.unwrap_err();
  assert!(error.is_canceled(), "{error:?}");
  transfer.closed().await;
  assert!(told_canceled(bob.read_peer().await, "@@share\\moving.mp3"));
}

#[tokio::test]
async fn the_upload_decides_what_its_peer_is_told_when_it_ends() {
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\banned.mp3", |upload| {
    upload.ended(|_, error| {
      assert!(error.is_canceled(), "{error:?}");
      Some(UploadNotice::Denied("Banned".into()))
    })
  })
  .await;
  let mut bob = scripted.connected().await;
  offered(&mut bob, "@@share\\banned.mp3").await;
  running.handle().cancel();
  running.result().await.unwrap_err();
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::UploadDenied(UploadDenied {
      filename: "@@share\\banned.mp3".into(),
      reason: "Banned".into(),
    })
  );

  // a cancel the peer is to take as a failure, so it asks again
  let running = upload(&scripted, "@@share\\later.mp3", |upload| {
    upload.ended(|_, _| Some(UploadNotice::Failed))
  })
  .await;
  offered(&mut bob, "@@share\\later.mp3").await;
  running.handle().cancel();
  running.result().await.unwrap_err();
  assert!(told_failed(bob.read_peer().await, "@@share\\later.mp3"));

  // one that is told nothing reads only what comes next
  let running = upload(&scripted, "@@share\\quiet.mp3", |upload| {
    upload.ended(|_, _| Some(UploadNotice::Nothing))
  })
  .await;
  offered(&mut bob, "@@share\\quiet.mp3").await;
  running.handle().cancel();
  running.result().await.unwrap_err();
  scripted
    .client()
    .transfers()
    .fail(BOB, "@@share\\next.mp3")
    .await;
  assert!(told_failed(bob.read_peer().await, "@@share\\next.mp3"));

  // one that leaves it to the client gets the client's notice for a cancel
  let running = upload(&scripted, "@@share\\default.mp3", |upload| {
    upload.ended(|_, _| None)
  })
  .await;
  offered(&mut bob, "@@share\\default.mp3").await;
  running.handle().cancel();
  running.result().await.unwrap_err();
  assert!(told_canceled(bob.read_peer().await, "@@share\\default.mp3"));
}

#[tokio::test]
async fn an_upload_lingers_however_long_it_is_told_to() {
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\patient.mp3", |upload| {
    upload.linger(Duration::MAX)
  })
  .await;
  let mut bob = scripted.connected().await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\patient.mp3").await;
  receive(&mut transfer, 0).await;
  drop(transfer);
  assert_eq!(running.result().await.unwrap().closed_by_peer, Some(true));
}

/// a slot guard that runs `0` when the upload drops it
struct Released(Box<dyn FnMut() + Send>);

impl Drop for Released {
  fn drop(&mut self) {
    (self.0)();
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_slot_is_back_before_the_peer_is_told() {
  let released = Arc::new(AtomicU32::new(0));
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\gone.mp3", |upload| {
    let released = released.clone();
    // handing the slot back takes a moment, in which a peer told first would have heard already
    upload.slot(move |_| {
      let released = released.clone();
      async move {
        Slot::granted(Released(Box::new(move || {
          std::thread::sleep(Duration::from_millis(300));
          released.fetch_add(1, Ordering::SeqCst);
        })))
      }
    })
  })
  .await;
  let mut bob = scripted.connected().await;
  offered(&mut bob, "@@share\\gone.mp3").await;
  running.handle().cancel();
  assert!(told_canceled(bob.read_peer().await, "@@share\\gone.mp3"));
  assert_eq!(
    released.load(Ordering::SeqCst),
    1,
    "the peer heard before the slot was back"
  );
  assert!(matches!(running.result().await, Err(matched) if matched.is_canceled()));
}

#[tokio::test]
async fn the_slot_policy_fails_cancels_or_admits_the_upload_and_gets_the_slot_back_once() {
  // a slot policy that fails gives no guard to hold
  let released = Arc::new(AtomicU32::new(0));
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\noslot.mp3", |upload| {
    upload.slot(|_| async { Slot::Fail("queue on fire".into()) })
  })
  .await;
  // bob is told the upload failed, which lets a downloader ask again
  let mut bob = scripted.connected().await;
  assert!(told_failed(bob.read_peer().await, "@@share\\noslot.mp3"));
  let error = running.result().await.unwrap_err();
  assert!(
    error.kind() == ErrorKind::Transfer
      && error
        .to_string()
        .starts_with("transfer error: failed to acquire an upload slot"),
    "{error:?}"
  );

  // one that cancels ends the upload canceled, and bob is told to drop it
  let running = upload(&scripted, "@@share\\canceled.mp3", |upload| {
    upload.slot(|_| async { Slot::Cancel })
  })
  .await;
  assert!(told_canceled(
    bob.read_peer().await,
    "@@share\\canceled.mp3"
  ));
  assert!(matches!(running.result().await, Err(matched) if matched.is_canceled()));

  // one that admits it gets the slot back once the upload ends
  let running = upload(&scripted, "@@share\\slot.mp3", |upload| {
    let released = released.clone();
    upload.slot(move |_| {
      let released = released.clone();
      async move {
        Slot::granted(Released(Box::new(move || {
          released.fetch_add(1, Ordering::SeqCst);
        })))
      }
    })
  })
  .await;
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\slot.mp3").await;
  receive(&mut transfer, 0).await;
  drop(transfer);
  running.result().await.unwrap();
  assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_source_that_changed_or_ends_early_fails_the_upload() {
  // the file shrank while the upload waited
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = offered_upload(&mut scripted, "@@share\\shrunk.mp3").await;
  std::fs::write(&running.source, content(SIZE / 2)).unwrap();
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\shrunk.mp3").await;
  transfer.send(&0u64.to_le_bytes()).await;
  let error = running.result().await.unwrap_err();
  assert!(
    error.kind() == ErrorKind::Transfer
      && error
        .to_string()
        .starts_with("transfer error: file changed size"),
    "{error:?}"
  );
  assert!(told_failed(bob.read_peer().await, "@@share\\shrunk.mp3"));

  // a stream that ends before the size it claimed
  let short = scripted
    .client()
    .upload(BOB, "@@share\\short.mp3")
    .from_source(SIZE as u64, |_| async {
      Ok(std::io::Cursor::new(content(SIZE - 10)))
    })
    .start()
    .await
    .unwrap();
  let mut transfer = accepted(&mut scripted, &mut bob, "@@share\\short.mp3").await;
  transfer.send(&0u64.to_le_bytes()).await;
  let error = short.await.unwrap_err();
  assert!(
    error.kind() == ErrorKind::Transfer
      && error
        .to_string()
        .starts_with("transfer error: input ended prematurely"),
    "{error:?}"
  );
}

#[tokio::test]
async fn an_upload_is_queued_at_once_and_once_per_file() {
  let scripted = Scripted::start().await;
  let source = file(&scratch(), "upload", SIZE);
  let upload = || {
    scripted
      .client()
      .upload(BOB, "@@share\\q.mp3")
      .from_file(&source)
  };
  let task = upload().start().await.unwrap();
  assert_eq!(
    scripted.client().transfers().uploads()[0].state,
    soulseek_client::transfer::State::QueuedLocally
  );
  assert!(matches!(
    upload().start().await,
    Err(matched) if matched.kind() == ErrorKind::DuplicateTransfer
  ));
  drop(task);
  let missing = scripted
    .client()
    .upload(BOB, "@@share\\m.mp3")
    .from_file(scratch().join("nope"));
  assert!(
    matches!(missing.start().await, Err(matched) if matched.kind() == ErrorKind::FileNotFound)
  );
}

#[tokio::test]
async fn an_upload_to_a_user_who_is_offline_fails_without_telling_anyone() {
  let mut scripted = Scripted::start().await;
  let running = upload(&scripted, "@@share\\off.mp3", |upload| upload).await;
  scripted
    .session
    .server
    .request_matching(|request| match request {
      ServerRequest::GetPeerAddress(GetPeerAddressRequest { username }) if username == BOB => {
        Some(())
      }
      _ => None,
    })
    .await;
  scripted
    .session
    .server
    .send(&ServerResponse::GetPeerAddress(GetPeerAddressResponse {
      username: BOB.into(),
      ip: std::net::Ipv4Addr::UNSPECIFIED,
      port: 0,
      obfuscation: None,
    }))
    .await;
  let started = tokio::time::Instant::now();
  let error = running.result().await.unwrap_err();
  assert!(error.kind() == ErrorKind::UserOffline, "{error:?}");
  assert!(
    started.elapsed() < Duration::from_secs(2),
    "no lookup for a courtesy"
  );
}
