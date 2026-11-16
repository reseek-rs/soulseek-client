//! Downloads from a scripted peer, for what a real uploader does not say on demand: refusing
//! with any reason, reporting a failure at each stage, naming another size, sending at once,
//! sending what does not decode; and how the download asks: governed, with fresh tokens, once per
//! file.

use soulseek_client::ErrorKind;
use soulseek_client::config::ConfigBuilder;
use soulseek_client::handler::Handler;
use soulseek_client::transfer::{Destination, Download, RequestStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::support::peer::{FakePeer, PeerSide};
use crate::support::scripted::{BOB, SIZE, Scripted, Starting, offer, queued, send_file};
use crate::support::transfers::{self, content, reached, scratch, states};
use crate::support::{Logs, PATIENCE, Session, listening_options};
use soulseek_client::testing::Engine;
use soulseek_client::{Error, Event, Peer};
use soulseek_proto::peer::{
  PlaceInQueueRequest, PlaceInQueueResponse, QueueDownload, TransferRequest, TransferResponse,
  UploadDenied, UploadFailed, UserInfo,
};
use soulseek_proto::server::GetPeerAddressResponse;
use soulseek_proto::{PeerCode, PeerMessage, ServerRequest, ServerResponse};
use tokio_util::sync::CancellationToken;

fn rejected(reason: &str) -> impl Fn(&Error) -> bool + '_ {
  move |error| error.kind() == ErrorKind::TransferRejected && error.reason() == Some(reason)
}

fn reported_failed(error: &Error) -> bool {
  error.kind() == ErrorKind::TransferFailed
    && error.to_string() == "download reported as failed by remote client"
}

#[tokio::test]
async fn a_refusal_with_any_reason_rejects_the_download() {
  let mut scripted = Scripted::start().await;
  // the reason is passed on as it is and never matched, so a few stand for every one
  let reasons = ["Cancelled", "File not shared.", ""];
  let mut bob = None;
  for (n, reason) in reasons.into_iter().enumerate() {
    let filename = format!("@@share\\refused {n}.mp3");
    let running = scripted.download(&filename, |download| download);
    // the first download connects; the rest find the connection cached
    if bob.is_none() {
      bob = Some(scripted.connected().await);
    }
    let side = bob.as_mut().unwrap();
    queued(side, &filename).await;
    side
      .send(
        &PeerMessage::UploadDenied(UploadDenied {
          filename: filename.as_str().into(),
          reason: reason.into(),
        })
        .encode(),
      )
      .await;
    let error = running.result().await.unwrap_err();
    assert!(rejected(reason)(&error), "{reason}: {error:?}");
    let message = scripted
      .session
      .event(|event| match event {
        Event::Transfer(soulseek_client::transfer::Event::DownloadDenied { message, .. }) => {
          Some(message)
        }
        _ => None,
      })
      .await;
    assert_eq!(message, reason);
  }
}

#[tokio::test]
async fn a_transfer_request_answered_with_a_refusal_rejects_the_download_and_queued_waits() {
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\a.mp3", |download| {
    download.request_style(RequestStyle::TransferRequest)
  });
  let mut bob = scripted.connected().await;
  let PeerMessage::TransferRequest(asked) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  assert!(
    matches!(asked, TransferRequest::Download { .. }),
    "a request to download, which carries no size"
  );
  bob
    .send(
      &PeerMessage::TransferResponse(TransferResponse::Denied {
        token: asked.token(),
        message: "Too many megabytes".into(),
      })
      .encode(),
    )
    .await;
  assert!(rejected("Too many megabytes")(
    &running.result().await.unwrap_err()
  ));

  // "Queued", however spelled, queues the download, which then waits for the offer
  let running = scripted.download("@@share\\b.mp3", |download| {
    download.request_style(RequestStyle::TransferRequest)
  });
  let PeerMessage::TransferRequest(asked) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  bob
    .send(
      &PeerMessage::TransferResponse(TransferResponse::Denied {
        token: asked.token(),
        message: "queued.".into(),
      })
      .encode(),
    )
    .await;
  let mut events = scripted.client().events();
  let states = tokio::time::timeout(PATIENCE, async {
    loop {
      let downloads = scripted.client().transfers().downloads();
      if downloads[0].state == soulseek_client::transfer::State::QueuedRemotely {
        return downloads;
      }
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
  assert_eq!(states.len(), 1);
  offer(&mut bob, "@@share\\b.mp3", 77, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(77).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  transfer.closed().await;
  let done = running.result().await.unwrap();
  assert_eq!(done.remote_token, Some(77));
  let (_, last) =
    transfers::states(&mut events, soulseek_client::transfer::Direction::Download).await;
  assert!(last.state.succeeded());
}

#[tokio::test]
async fn a_peer_that_sends_at_once_is_dialed_with_our_own_token() {
  let mut scripted = Scripted::start().await;
  let mut events = scripted.client().events();
  let running = scripted.download("@@share\\now.mp3", |download| {
    download.request_style(RequestStyle::TransferRequest)
  });
  let mut bob = scripted.connected().await;
  let PeerMessage::TransferRequest(asked) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  bob
    .send(
      &PeerMessage::TransferResponse(TransferResponse::Allowed {
        token: asked.token(),
        file_size: Some(SIZE),
      })
      .encode(),
    )
    .await;
  let (mut transfer, token) = scripted.accept_transfer().await;
  assert_eq!(token, asked.token());
  send_file(&mut transfer, 0, SIZE as usize).await;
  transfer.closed().await;
  let destination = running.destination.clone();
  let done = running.result().await.unwrap();
  assert_eq!(done.remote_token, None);
  assert_eq!(std::fs::read(destination).unwrap(), content(SIZE as usize));
  let (states, _) = states(&mut events, soulseek_client::transfer::Direction::Download).await;
  assert_eq!(states, transfers::download());
}

#[tokio::test]
async fn a_failure_reported_at_any_stage_ends_the_download_at_once() {
  let failed = |filename: &str| {
    PeerMessage::UploadFailed(UploadFailed {
      filename: filename.into(),
    })
    .encode()
  };

  // waiting in bob's queue
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = scripted.queued_download("@@share\\q.mp3").await;
  bob.send(&failed("@@share\\q.mp3")).await;
  assert!(reported_failed(&running.result().await.unwrap_err()));

  // waiting for bob to connect, which takes far longer to time out than this test waits
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = scripted.queued_download("@@share\\i.mp3").await;
  offer(&mut bob, "@@share\\i.mp3", 5, Some(SIZE)).await;
  bob.send(&failed("@@share\\i.mp3")).await;
  let error = running.result().await.unwrap_err();
  assert!(reported_failed(&error), "{error:?}");

  // with bytes moving
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = scripted.queued_download("@@share\\p.mp3").await;
  offer(&mut bob, "@@share\\p.mp3", 6, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(6).await;
  send_file(&mut transfer, 0, 1000).await;
  // a failure is taken before bytes still unread, so bob sends it once all 1000 are written
  reached(scripted.client(), 1000).await;
  bob.send(&failed("@@share\\p.mp3")).await;
  let destination = running.destination.clone();
  let error = running.result().await.unwrap_err();
  assert!(reported_failed(&error), "{error:?}");
  // what arrived is kept for a resume
  let kept = std::fs::read(destination).unwrap();
  assert_eq!(kept, content(1000));
  let downloads = scripted.client().transfers().downloads();
  assert!(downloads.is_empty());
}

#[tokio::test]
async fn a_refusal_while_the_request_awaits_its_answer_ends_the_download_at_once() {
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\r.mp3", |download| {
    download.request_style(RequestStyle::TransferRequest)
  });
  let mut bob = scripted.connected().await;
  let _ = bob.read_peer().await;
  bob
    .send(
      &PeerMessage::UploadDenied(UploadDenied {
        filename: "@@share\\r.mp3".into(),
        reason: "Banned".into(),
      })
      .encode(),
    )
    .await;
  let started = tokio::time::Instant::now();
  assert!(rejected("Banned")(&running.result().await.unwrap_err()));
  assert!(
    started.elapsed() < Duration::from_secs(5),
    "not the response timeout"
  );
}

#[tokio::test]
async fn the_size_the_uploader_names_is_checked_against_ours() {
  // another size aborts
  let mut scripted = Scripted::start().await;
  let mut events = scripted.client().events();
  let (running, mut bob) = scripted.queued_download("@@share\\big.mp3").await;
  bob
    .send(
      &PeerMessage::TransferRequest(TransferRequest::Upload {
        token: 9,
        filename: "@@share\\big.mp3".into(),
        file_size: Some(SIZE + 1),
      })
      .encode(),
    )
    .await;
  let error = running.result().await.unwrap_err();
  assert!(error.size_mismatch() == Some((SIZE, SIZE + 1)), "{error:?}");
  let (_, last) = states(&mut events, soulseek_client::transfer::Direction::Download).await;
  assert_eq!(
    last.state,
    soulseek_client::transfer::State::Completed(soulseek_client::transfer::Outcome::Aborted)
  );

  // the download can settle on the uploader's size
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\settled.mp3", |download| {
    download
      .size(SIZE - 10)
      .size_mismatch(|_, remote| Some(remote))
  });
  let mut bob = scripted.connected().await;
  queued(&mut bob, "@@share\\settled.mp3").await;
  offer(&mut bob, "@@share\\settled.mp3", 11, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(11).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let done = running.result().await.unwrap();
  assert_eq!((done.size, done.bytes_transferred), (SIZE, SIZE));

  // a download that names no size takes the uploader's
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\any.mp3", |download| download.size(None));
  let mut bob = scripted.connected().await;
  queued(&mut bob, "@@share\\any.mp3").await;
  offer(&mut bob, "@@share\\any.mp3", 12, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(12).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let done = running.result().await.unwrap();
  assert_eq!((done.size, done.bytes_transferred), (SIZE, SIZE));

  // with no size from either side there is nothing to download to
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\none.mp3", |download| download.size(None));
  let mut bob = scripted.connected().await;
  queued(&mut bob, "@@share\\none.mp3").await;
  bob
    .send(
      &PeerMessage::TransferRequest(TransferRequest::Upload {
        token: 13,
        filename: "@@share\\none.mp3".into(),
        file_size: None,
      })
      .encode(),
    )
    .await;
  let error = running.result().await.unwrap_err();
  assert!(
    error.kind() == ErrorKind::Transfer
      && error
        .to_string()
        .starts_with("transfer error: the uploader named no size"),
    "{error:?}"
  );
}

/// A download queued at its peer goes on when the session ends: with the listener kept, the
/// offer that comes on the message connection still open is accepted without the server
#[tokio::test]
async fn an_offer_that_comes_between_sessions_is_taken_on_the_open_connection() {
  let mut options = listening_options();
  options = options.listen_while_disconnected(true);
  let mut scripted = Scripted::with_options(options).await;
  let (running, mut bob) = scripted.queued_download("@@share\\later.mp3").await;
  scripted.client().disconnect().await;
  offer(&mut bob, "@@share\\later.mp3", 7, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(7).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  assert!(running.result().await.is_ok());
}

/// options whose every timeout is too long to add to the clock
fn endless() -> ConfigBuilder {
  let forever = |connection: soulseek_client::config::ConnectionBuilder| {
    connection
      .connect_timeout(Duration::MAX)
      .inactivity_timeout(Duration::MAX)
  };
  let mut options = listening_options()
    .peer_connection(forever)
    .transfer_connection(forever)
    .incoming_connection(forever);
  options = options.request_timeout(Duration::MAX);
  options = options.transfer_response_timeout(Duration::MAX);
  options = options.transfer_accept_timeout(Duration::MAX);
  options = options.upload_notice_timeout(Duration::MAX);
  options = options.limits(|limits| limits.address_cache_lifetime(Duration::MAX));
  options
}

#[tokio::test]
async fn a_download_runs_with_every_timeout_too_long_to_add_to_the_clock() {
  let mut scripted = Scripted::with_options(endless()).await;
  let filename = "@@share\\forever.mp3";
  let running = scripted.download(filename, |download| download.queued_timeout(Duration::MAX));
  let mut bob = scripted.connected().await;
  queued(&mut bob, filename).await;
  offer(&mut bob, filename, 31, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(31).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  assert_eq!(running.result().await.unwrap().bytes_transferred, SIZE);
}

#[tokio::test]
async fn a_place_in_queue_is_asked_for_a_file_we_do_not_download() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\elsewhere.mp3";
  let asking = tokio::spawn({
    let client = scripted.client().clone();
    async move { client.transfers().place_in_queue(BOB, filename).await }
  });
  let mut bob = scripted.connected().await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::PlaceInQueueRequest(PlaceInQueueRequest {
      filename: filename.into()
    })
  );
  let place = PeerMessage::PlaceInQueueResponse(PlaceInQueueResponse {
    filename: filename.into(),
    place: 4,
  });
  bob.send(&place.encode()).await;
  assert_eq!(asking.await.unwrap().unwrap(), 4);
}

#[tokio::test]
async fn a_second_offer_of_a_file_already_coming_is_refused_and_the_first_goes_on() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\twice.mp3";
  let (running, mut bob) = scripted.queued_download(filename).await;
  offer(&mut bob, filename, 21, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(21).await;
  // the download took the first offer and waits for none; the uploader is told so
  let again = PeerMessage::TransferRequest(TransferRequest::Upload {
    token: 22,
    filename: filename.into(),
    file_size: Some(SIZE),
  });
  bob.send(&again.encode()).await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::TransferResponse(TransferResponse::Denied {
      token: 22,
      message: "Cancelled".into(),
    })
  );
  send_file(&mut transfer, 0, SIZE as usize).await;
  assert_eq!(running.result().await.unwrap().bytes_transferred, SIZE);
}

#[tokio::test]
async fn a_transfer_message_that_does_not_decode_fails_what_waits_for_it() {
  // a refusal whose reason is cut short still names the file
  let mut scripted = Scripted::start().await;
  let (running, mut bob) = scripted.queued_download("@@share\\x.mp3").await;
  let filename = "@@share\\x.mp3";
  let mut payload = (filename.len() as u32).to_le_bytes().to_vec();
  payload.extend(filename.as_bytes());
  payload.extend([100, 0, 0, 0, b'n', b'o']);
  let mut frame = ((payload.len() + 4) as u32).to_le_bytes().to_vec();
  frame.extend(50u32.to_le_bytes());
  frame.extend(payload);
  bob.send(&frame).await;
  let error = running.result().await.unwrap_err();
  assert!(
    matches!(error.kind(), ErrorKind::Message(_))
      && error.to_string() == "peer sent an unparseable UploadDenied",
    "{error:?}"
  );
}

#[tokio::test]
async fn a_governor_that_allows_nothing_yet_is_asked_again() {
  let mut scripted = Scripted::start().await;
  let asked = Arc::new(AtomicU32::new(0));
  let reported = Arc::new(AtomicU32::new(0));
  let running = scripted.download("@@share\\paced.mp3", {
    let (asked, reported) = (asked.clone(), reported.clone());
    move |download| {
      download
        .governor(move |_, wanted| {
          // nothing the first time
          let first = asked.fetch_add(1, Ordering::SeqCst) == 0;
          async move { if first { 0 } else { wanted } }
        })
        .reporter(move |_, chunk| {
          reported.fetch_add(chunk.moved, Ordering::SeqCst);
        })
    }
  });
  let mut bob = scripted.connected().await;
  queued(&mut bob, "@@share\\paced.mp3").await;
  offer(&mut bob, "@@share\\paced.mp3", 19, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(19).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  assert_eq!(running.result().await.unwrap().bytes_transferred, SIZE);
  assert!(asked.load(Ordering::SeqCst) > 1);
  assert_eq!(u64::from(reported.load(Ordering::SeqCst)), SIZE);
}

#[tokio::test]
async fn the_clients_reporter_is_told_unless_the_download_has_its_own() {
  let everywhere = Arc::new(AtomicU32::new(0));
  let options = listening_options().transfer_reporter({
    let everywhere = everywhere.clone();
    move |_, chunk| {
      everywhere.fetch_add(chunk.moved, Ordering::SeqCst);
    }
  });
  let mut scripted = Scripted::with_options(options).await;
  let running = scripted.download("@@share\\plain.mp3", |download| download);
  let mut bob = scripted.connected().await;
  queued(&mut bob, "@@share\\plain.mp3").await;
  offer(&mut bob, "@@share\\plain.mp3", 31, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(31).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  running.result().await.unwrap();
  assert_eq!(u64::from(everywhere.load(Ordering::SeqCst)), SIZE);

  // a download with a reporter of its own leaves the client's untold
  let own = Arc::new(AtomicU32::new(0));
  let running = scripted.download("@@share\\own.mp3", {
    let own = own.clone();
    move |download| {
      download.reporter(move |_, chunk| {
        own.fetch_add(chunk.moved, Ordering::SeqCst);
      })
    }
  });
  queued(&mut bob, "@@share\\own.mp3").await;
  offer(&mut bob, "@@share\\own.mp3", 32, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(32).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  running.result().await.unwrap();
  assert_eq!(u64::from(own.load(Ordering::SeqCst)), SIZE);
  assert_eq!(u64::from(everywhere.load(Ordering::SeqCst)), SIZE);
}

#[tokio::test]
async fn the_clients_governor_is_asked_unless_the_download_has_its_own() {
  let everywhere = Arc::new(AtomicU32::new(0));
  let options = listening_options().transfer_governor({
    let everywhere = everywhere.clone();
    move |_, wanted| {
      everywhere.fetch_add(wanted, Ordering::SeqCst);
      async move { wanted }
    }
  });
  let mut scripted = Scripted::with_options(options).await;
  let running = scripted.download("@@share\\plain.mp3", |download| download);
  let mut bob = scripted.connected().await;
  queued(&mut bob, "@@share\\plain.mp3").await;
  offer(&mut bob, "@@share\\plain.mp3", 31, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(31).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  running.result().await.unwrap();
  assert_eq!(u64::from(everywhere.load(Ordering::SeqCst)), SIZE);

  // a download with a governor of its own leaves the client's unasked
  let own = Arc::new(AtomicU32::new(0));
  let running = scripted.download("@@share\\own.mp3", {
    let own = own.clone();
    move |download| {
      download.governor(move |_, wanted| {
        own.fetch_add(wanted, Ordering::SeqCst);
        async move { wanted }
      })
    }
  });
  queued(&mut bob, "@@share\\own.mp3").await;
  offer(&mut bob, "@@share\\own.mp3", 32, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(32).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  running.result().await.unwrap();
  assert_eq!(u64::from(own.load(Ordering::SeqCst)), SIZE);
  assert_eq!(u64::from(everywhere.load(Ordering::SeqCst)), SIZE);
}

#[tokio::test]
async fn a_fresh_token_skips_one_a_transfer_holds() {
  let scripted = Scripted::start().await;
  // the token the counter hands out next is taken by hand first
  let next = scripted.client().next_token() + 1;
  let held = scripted.download("@@share\\held.mp3", move |download| download.token(next));
  tokio::time::timeout(PATIENCE, async {
    while scripted.client().transfers().downloads().is_empty() {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  let fresh = scripted.download("@@share\\fresh.mp3", |download| download);
  let tokens = tokio::time::timeout(PATIENCE, async {
    loop {
      let mut tokens: Vec<_> = scripted
        .client()
        .transfers()
        .downloads()
        .iter()
        .map(|transfer| transfer.token)
        .collect();
      if tokens.len() == 2 {
        tokens.sort_unstable();
        return tokens;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  assert_eq!(tokens, [next, next + 1]);
  held.started().await.handle.cancel();
  fresh.started().await.handle.cancel();
}

#[tokio::test]
async fn one_download_per_file_and_one_transfer_per_token() {
  let mut scripted = Scripted::start().await;
  let (_running, _bob) = scripted.queued_download("@@share\\once.mp3").await;
  let again = scripted
    .client()
    .download(BOB, "@@share\\once.mp3")
    .size(SIZE)
    .to_file(scratch().join("again"))
    .await;
  assert!(matches!(&again, Err(matched) if matched.kind() == ErrorKind::DuplicateTransfer));
  let token = scripted.client().transfers().downloads()[0].token;
  let other = scripted
    .client()
    .download(BOB, "@@share\\other.mp3")
    .size(SIZE)
    .token(token)
    .to_file(scratch().join("other"))
    .await;
  let taken = other.unwrap_err();
  assert_eq!(taken.kind(), ErrorKind::DuplicateToken);
  assert_eq!(
    taken.to_string(),
    format!("token {token} is already in use")
  );
}

#[tokio::test]
async fn an_offer_that_comes_while_the_download_still_looks_up_its_peer_is_allowed() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\early.mp3";
  let _running = scripted.download(filename, |download| download);
  // the download is registered and waits for bob's address
  let server = &mut scripted.session.server;
  server
    .request_matching(|request| matches!(request, ServerRequest::GetPeerAddress(_)).then_some(()))
    .await;
  // bob still has the file queued from before, and offers it now
  let mut bob = scripted.session.peer(BOB).await;
  let offered = PeerMessage::TransferRequest(TransferRequest::Upload {
    token: 77,
    filename: filename.into(),
    file_size: Some(SIZE),
  });
  bob.send(&offered.encode()).await;
  // the client has handled the offer by the time it answers a request sent after it
  bob.send(&PeerMessage::InfoRequest.encode()).await;
  let allowed = PeerMessage::TransferResponse(TransferResponse::Allowed {
    token: 77,
    file_size: None,
  });
  let mut answered = false;
  loop {
    match bob.read_peer().await {
      PeerMessage::InfoResponse(_) => break,
      answer => {
        assert_eq!(answer, allowed);
        answered = true;
      }
    }
  }
  scripted
    .session
    .server
    .send(&ServerResponse::GetPeerAddress(GetPeerAddressResponse {
      username: BOB.into(),
      ip: std::net::Ipv4Addr::LOCALHOST,
      port: scripted.bob.port(),
      obfuscation: None,
    }))
    .await;
  let in_time = tokio::time::timeout(Duration::from_secs(2), async {
    while !answered {
      answered = bob.read_peer().await == allowed;
    }
  })
  .await;
  assert!(in_time.is_ok(), "the offer was not answered");
}

#[tokio::test]
async fn a_download_whose_offer_came_early_is_handed_back_before_it_ends() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\handed.mp3";
  let starting = scripted.download(filename, |download| download);
  let server = &mut scripted.session.server;
  server
    .request_matching(|request| matches!(request, ServerRequest::GetPeerAddress(_)).then_some(()))
    .await;
  // bob still has the file queued from before, and offers it before the download asks
  let mut bob = scripted.session.peer(BOB).await;
  let offered = PeerMessage::TransferRequest(TransferRequest::Upload {
    token: 78,
    filename: filename.into(),
    file_size: Some(SIZE),
  });
  bob.send(&offered.encode()).await;
  scripted
    .session
    .server
    .send(&ServerResponse::GetPeerAddress(GetPeerAddressResponse {
      username: BOB.into(),
      ip: std::net::Ipv4Addr::LOCALHOST,
      port: scripted.bob.port(),
      obfuscation: None,
    }))
    .await;
  // the download took the offer and waits for the file, which bob has not begun to send
  let running = tokio::time::timeout(Duration::from_secs(5), starting.queued())
    .await
    .expect("the download is handed back while it runs");
  let mut transfer = scripted.connect_for_transfer(78).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let done = running.result().await.unwrap();
  assert_eq!(done.bytes_transferred, SIZE);
}

/// Downloads from bob, each named long enough that bob, who does not read, soon leaves the
/// connection full: the later ones take it and wait their turn to ask behind writes that cannot
/// end. Each is shaped by `shape`.
fn filling(
  scripted: &Scripted,
  shape: fn(Download<Destination>) -> Download<Destination>,
) -> (Vec<String>, Vec<Starting>) {
  let names: Vec<String> = (0..300)
    .map(|n| format!("@@share\\{n:03} {}.flac", "x".repeat(60_000)))
    .collect();
  let running = names
    .iter()
    .map(|filename| scripted.download(filename, shape))
    .collect();
  (names, running)
}

/// Reads frames from `side` until `pick` takes one.
async fn until<T>(
  side: &mut crate::support::peer::PeerSide,
  mut pick: impl FnMut(PeerMessage) -> Option<T>,
) -> T {
  loop {
    if let Some(picked) = pick(side.read_peer().await) {
      return picked;
    }
  }
}

/// Downloads that took their connection to bob and wait their turn to ask find it closed when
/// it comes, or close under the write they are in: each asks on a new connection, and none
/// fails.
#[tokio::test]
async fn a_request_whose_connection_closed_under_it_goes_on_a_new_one() {
  let mut scripted = Scripted::start().await;
  let (names, _running) = filling(&scripted, |download| download);
  let mut bob = scripted.connected().await;
  queued(&mut bob, &names[0]).await;
  drop(bob);
  let mut again = scripted.accept_message().await;
  let last = names.last().unwrap();
  until(&mut again, |message| match message {
    PeerMessage::QueueDownload(asked) => (asked.filename.as_str() == last.as_str()).then_some(()),
    _ => None,
  })
  .await;
  // a download that ended is not among the client's any more
  let running = scripted.client().transfers().downloads().len();
  assert_eq!(running, names.len(), "downloads failed with the connection");
}

/// Bob takes the first transfer request of the downloads filling the connection and closes it;
/// every download whose request goes out anew on the new connection keeps running, and the
/// first of them takes the answer bob gives there.
async fn transfer_requests_sent_anew() {
  let mut scripted = Scripted::start().await;
  let (names, _running) = filling(&scripted, |download| {
    download.request_style(RequestStyle::TransferRequest)
  });
  let mut bob = scripted.connected().await;
  let PeerMessage::TransferRequest(_) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  drop(bob);
  let mut again = scripted.accept_message().await;
  let mut asked = Vec::new();
  let mut anew = Vec::new();
  let last = names.last().unwrap();
  until(&mut again, |message| match message {
    PeerMessage::TransferRequest(request) => {
      asked.push(request.token());
      anew.push(
        names
          .iter()
          .position(|n| n.as_str() == request.filename().as_str())
          .unwrap(),
      );
      (request.filename().as_str() == last.as_str()).then_some(())
    }
    _ => None,
  })
  .await;
  // a download that ended is not among the client's any more
  let running = scripted.client().transfers().downloads();
  let ended: Vec<usize> = anew
    .iter()
    .copied()
    .filter(|&n| !running.iter().any(|t| t.filename.as_str() == names[n]))
    .collect();
  assert!(ended.is_empty(), "downloads asked anew failed: {ended:?}");
  // the first asked anew held the turn as the connection closed, its write held up by the full
  // connection, so the close failed its first wait and the answer comes to the renewed one
  let queued = PeerMessage::TransferResponse(TransferResponse::Denied {
    token: asked[0],
    message: "Queued".into(),
  });
  again.send(&queued.encode()).await;
  let answered = tokio::time::timeout(PATIENCE, async {
    loop {
      let remote = scripted
        .client()
        .transfers()
        .downloads()
        .into_iter()
        .any(|t| {
          t.token == asked[0] && t.state == soulseek_client::transfer::State::QueuedRemotely
        });
      if remote {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await;
  assert!(
    answered.is_ok(),
    "the answer on the new connection was not taken"
  );
}

/// In the transfer request style the answer is waited for on a request sent anew: the close of
/// the connection it first went to failed the wait registered for the first, and the one made
/// again before the request goes anew takes the peer's answer on the new connection.
#[tokio::test]
async fn a_transfer_request_sent_anew_is_answered_on_the_new_connection() {
  transfer_requests_sent_anew().await;
}

/// [`a_transfer_request_sent_anew_is_answered_on_the_new_connection`] with the close handled on
/// another worker than the download's, for the order of removing the closed connection and
/// failing its user's waits: done apart, the failing could reach a wait made again after the
/// removal. This catches that split only when the connection's own watcher removes the entry,
/// not when the download finds it closed first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_request_sent_anew_keeps_its_wait_whichever_worker_closes() {
  transfer_requests_sent_anew().await;
}

/// waits until `holds` says yes, looking again each time the other tasks have had a turn
async fn eventually(what: &str, holds: impl Fn() -> bool) {
  let reached = tokio::time::timeout(PATIENCE, async {
    while !holds() {
      tokio::task::yield_now().await;
    }
  })
  .await;
  assert!(reached.is_ok(), "{what}");
}

/// Reads frames from `side` until the client's `QueueDownload` for `filename`, skipping the
/// others whole however long they are.
async fn skip_to_queued(side: &mut PeerSide, filename: &str) {
  loop {
    let prefix = side.raw(4).await;
    let len = u32::from_le_bytes(prefix.clone().try_into().unwrap());
    let body = side.raw(len as usize).await;
    if u32::from_le_bytes(body[..4].try_into().unwrap()) != PeerCode::QueueDownload.code() {
      continue;
    }
    let frame = [prefix, body].concat();
    if let PeerMessage::QueueDownload(asked) = PeerMessage::decode(&frame).unwrap()
      && asked.filename.as_str() == filename
    {
      return;
    }
  }
}

/// A download that took bob's connection before bob reconnected past the ones kept asks on the
/// current connection when the old one refuses its request. Bob does not read the user info
/// answers on the first connection, so the request ahead waits there while bob reconnects three
/// times and the client closes the first once its queued writes are out. Bob then reads up to
/// the request ahead, and the next request comes while the close waits behind a write that does
/// not end.
#[tokio::test]
async fn a_request_refused_on_a_superseded_connection_goes_on_the_current_one() {
  // more than a loopback socket pair holds while bob does not read
  const PICTURE: usize = 16 << 20;
  let logs = Logs::capture();
  /// answers user info with a picture of [`PICTURE`] bytes, and counts the answers
  struct BigPicture(Arc<AtomicU32>);
  impl Handler for BigPicture {
    async fn user_info(&self, _client: &soulseek_client::Client, _peer: &Peer) -> Option<UserInfo> {
      self.0.fetch_add(1, Ordering::SeqCst);
      Some(UserInfo {
        description: String::new(),
        picture: Some(vec![0; PICTURE]),
        upload_slots: 1,
        queue_length: 0,
        has_free_upload_slot: true,
        upload_permitted: None,
      })
    }
  }
  let answers = Arc::new(AtomicU32::new(0));
  let options = listening_options().handler(BigPicture(answers.clone()));
  let mut scripted = Scripted {
    session: Session::with_options(options).await,
    bob: FakePeer::with_receive_buffer(64 << 10).await,
  };
  let first = "@@share\\first.flac";
  let _first = scripted.download(first, |download| download);
  let mut superseded = scripted.connected().await;
  queued(&mut superseded, first).await;
  let conn = scripted
    .client()
    .get_or_add_message_connection(BOB, None, &CancellationToken::new())
    .await
    .unwrap();
  // an answer is queued in the poll that resolved it, and the writer stalls on the first
  superseded.send(&PeerMessage::InfoRequest.encode()).await;
  eventually("the first answer stalls the writer", || {
    answers.load(Ordering::SeqCst) == 1 && conn.write_queue_depth() == 0
  })
  .await;
  let ahead = "@@share\\ahead.flac";
  let _ahead = scripted.download(ahead, |download| download);
  eventually("the request ahead is queued", || {
    conn.write_queue_depth() == 1
  })
  .await;
  let behind = "@@share\\behind.flac";
  let mut behind_ended = Box::pin(scripted.download(behind, |download| download).result());
  logs
    .wait_for(&["retrieved cached message connection", "behind.flac"])
    .await;
  superseded.send(&PeerMessage::InfoRequest.encode()).await;
  eventually("the second answer is queued", || {
    answers.load(Ordering::SeqCst) == 2 && conn.write_queue_depth() == 2
  })
  .await;
  // each from where the server says bob is, so each replaces the one before; bob keeps them
  // all open, so the first is pushed out of the ones kept
  let mut reconnected = Vec::new();
  for _ in 0..3 {
    let side = scripted.session.peer(BOB).await;
    let from = side.stream.local_addr().unwrap();
    eventually("bob's new connection is taken", || {
      scripted.client().users().connection_address(BOB) == Some(from)
    })
    .await;
    reconnected.push(side);
  }
  let current = reconnected.last_mut().unwrap();
  skip_to_queued(&mut superseded, ahead).await;
  tokio::select! {
    ended = &mut behind_ended => panic!("the download ended: {ended:?}"),
    asked = current.read_peer() => assert_eq!(
      asked,
      PeerMessage::QueueDownload(QueueDownload { filename: behind.into() })
    ),
  }
}

/// Bob reads a transfer request whole and closes the connection without answering: the
/// request may not have been read by the peer, so it goes once more on a new connection, where
/// bob's answer is taken, and the download goes on.
#[tokio::test]
async fn a_transfer_request_written_whole_before_a_close_goes_once_more() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\closed.mp3";
  let running = scripted.download(filename, |download| {
    download.request_style(RequestStyle::TransferRequest)
  });
  let mut bob = scripted.connected().await;
  let PeerMessage::TransferRequest(first) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  drop(bob);
  let mut again = scripted.accept_message().await;
  let PeerMessage::TransferRequest(asked) = again.read_peer().await else {
    panic!("the transfer request once more");
  };
  assert_eq!(asked, first);
  let queued = PeerMessage::TransferResponse(TransferResponse::Denied {
    token: asked.token(),
    message: "Queued".into(),
  });
  again.send(&queued.encode()).await;
  eventually("the answer on the new connection was taken", || {
    scripted.client().transfers().downloads().iter().any(|t| {
      t.token == asked.token() && t.state == soulseek_client::transfer::State::QueuedRemotely
    })
  })
  .await;
  let running = running.queued().await;
  assert!(!running.handle.transfer().state.is_terminal());
}

/// A place question bob reads and leaves unanswered fails with `NoResponse`. One that waits on
/// bob's address, which the server never gives, was never written and fails with `Timeout`.
#[tokio::test]
async fn an_unanswered_place_question_differs_from_one_never_written() {
  let options = || listening_options().request_timeout(Duration::from_millis(500));
  let mut scripted = Scripted::with_options(options()).await;
  let filename = "@@share\\placed.mp3";
  let (_running, mut bob) = scripted.queued_download(filename).await;
  let asking = scripted.client().transfers().place_in_queue(BOB, filename);
  let (asked, read) = tokio::join!(asking.into_future(), bob.read_peer());
  assert!(
    matches!(read, PeerMessage::PlaceInQueueRequest(_)),
    "{read:?}"
  );
  assert!(
    matches!(&asked, Err(matched) if matched.kind() == ErrorKind::NoResponse),
    "{asked:?}"
  );

  let mut unaddressed = Scripted::with_options(options()).await;
  let filename = "@@share\\unplaced.mp3";
  let _waiting = unaddressed.download(filename, |download| download);
  // the download is registered and waits for bob's address, which never comes
  unaddressed
    .session
    .server
    .request_matching(|request| matches!(request, ServerRequest::GetPeerAddress(_)).then_some(()))
    .await;
  let asked = unaddressed
    .client()
    .transfers()
    .place_in_queue(BOB, filename)
    .await;
  assert!(
    matches!(&asked, Err(matched) if matched.is_timeout()),
    "{asked:?}"
  );
}

/// The acceptance of an offer goes on a new connection when the one it was written to closes.
/// Bob reads nothing, so the requests fill the connection; once the client has stopped getting
/// requests out, bob offers the first file there, closes once the download takes the offer, and
/// the file arrives.
#[tokio::test]
async fn an_offers_acceptance_whose_connection_closed_goes_on_a_new_one() {
  let mut scripted = Scripted::start().await;
  let mut events = scripted.client().events();
  let (names, mut running) = filling(&scripted, |download| download);
  let mut bob = scripted.connected().await;
  // the requests written so far; none for a while means the connection is full, since bob
  // reads none of them. A window too short leaves room for the acceptance, whose write then ends
  // and is not sent anew, so the test fails; it cannot pass falsely
  let quiet = Duration::from_secs(1);
  loop {
    let requested = tokio::time::timeout(quiet, async {
      loop {
        match events.next().await {
          Some(Ok(Event::Transfer(soulseek_client::transfer::Event::StateChanged {
            transfer,
            ..
          })))
            if transfer.state == soulseek_client::transfer::State::Requested =>
          {
            return;
          }
          Some(Err(lagged)) => panic!("{lagged}"),
          Some(Ok(_)) => {}
          None => panic!("the client is gone"),
        }
      }
    })
    .await;
    if requested.is_err() {
      break;
    }
  }
  let offered = PeerMessage::TransferRequest(TransferRequest::Upload {
    token: 7,
    filename: names[0].as_str().into(),
    file_size: Some(SIZE),
  });
  bob.send(&offered.encode()).await;
  tokio::time::timeout(PATIENCE, async {
    loop {
      match events.next().await {
        Some(Ok(Event::Transfer(soulseek_client::transfer::Event::StateChanged {
          transfer,
          ..
        })))
          if transfer.state == soulseek_client::transfer::State::Initializing =>
        {
          return;
        }
        Some(Err(lagged)) => panic!("{lagged}"),
        Some(Ok(_)) => {}
        None => panic!("the client is gone"),
      }
    }
  })
  .await
  .expect("the offer is taken");
  drop(bob);
  let mut again = scripted.accept_message().await;
  until(&mut again, |message| match message {
    PeerMessage::TransferResponse(TransferResponse::Allowed { token: 7, .. }) => Some(()),
    _ => None,
  })
  .await;
  let mut transfer = scripted.connect_for_transfer(7).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let first = running.remove(0);
  assert!(first.result().await.is_ok());
}

#[tokio::test]
async fn downloads_started_in_order_are_asked_of_the_peer_in_that_order() {
  let mut scripted = Scripted::start().await;
  let album: Vec<String> = (1..=20)
    .map(|track| format!("@@share\\album\\{track:02}.flac"))
    .collect();
  let _running: Vec<_> = album
    .iter()
    .map(|filename| scripted.download(filename, |download| download))
    .collect();
  let mut bob = scripted.connected().await;
  let mut asked = Vec::new();
  for _ in &album {
    let PeerMessage::QueueDownload(request) = bob.read_peer().await else {
      panic!("a queue request");
    };
    asked.push(request.filename.to_string());
  }
  assert_eq!(asked, album);
}

/// `UploadFailed` names only the file, so one bob sends about an attempt that already ended here
/// cannot be told from one about the attempt queued after it. Today it fails the newer attempt;
/// this holds that still.
#[tokio::test]
async fn a_failure_reported_for_an_attempt_that_ended_fails_the_attempt_queued_after_it() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\again.mp3";
  let (first, mut bob) = scripted.queued_download(filename).await;
  offer(&mut bob, filename, 21, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(21).await;
  send_file(&mut transfer, 0, 1000).await;
  drop(transfer);
  assert!(first.result().await.is_err());
  let second = scripted.download(filename, |download| download);
  queued(&mut bob, filename).await;
  // bob reports the first attempt's broken transfer only now
  let failed = PeerMessage::UploadFailed(UploadFailed {
    filename: filename.into(),
  });
  bob.send(&failed.encode()).await;
  assert!(reported_failed(&second.result().await.unwrap_err()));
}

/// The uploader cannot have failed a download that has not asked for its file yet, so a failure
/// it reports then is about an earlier download of the same file and leaves this one running.
#[tokio::test]
async fn a_failure_reported_before_the_download_asked_for_its_file_leaves_it_running() {
  let mut scripted = Scripted::start().await;
  // the download looks up bob's address, which the server has not given yet, so it has not
  // asked bob; bob reaches the client on a connection of his own
  let filename = "@@share\\second.mp3";
  let waiting = scripted.download(filename, |download| download);
  let mut early = crate::support::peer::PeerSide::connect(scripted.session.listen_address()).await;
  let init = soulseek_proto::InitMessage::PeerInit(soulseek_proto::init::PeerInit {
    username: BOB.into(),
    connection_type: soulseek_proto::PeerConnectionType::Peer,
    token: 0,
  });
  early.send(&init.encode()).await;
  let failed = PeerMessage::UploadFailed(UploadFailed {
    filename: filename.into(),
  });
  early.send(&failed.encode()).await;
  // the client has handled the failure by the time it answers a request sent after it
  early.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    early.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  // asked on bob's own connection once his address is known
  scripted.give_address().await;
  queued(&mut early, filename).await;
  offer(&mut early, filename, 23, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(23).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let done = waiting.result().await.unwrap();
  assert_eq!(done.bytes_transferred, SIZE);
}

#[tokio::test]
async fn an_offer_on_a_connection_from_another_address_than_the_servers_is_taken() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\wanted.flac";
  let (running, mut bob) = scripted.queued_download(filename).await;
  // bob reaches the client again from an address the server does not give for him, as through
  // a router that rewrites where connections come from
  let mut again = PeerSide::connect_from(
    std::net::Ipv4Addr::new(127, 0, 0, 2),
    scripted.session.listen_address(),
  )
  .await;
  let introduce = soulseek_proto::InitMessage::PeerInit(soulseek_proto::init::PeerInit {
    username: BOB.into(),
    connection_type: soulseek_proto::PeerConnectionType::Peer,
    token: 0,
  });
  again.send(&introduce.encode()).await;
  scripted.give_address().await;
  let offered = PeerMessage::TransferRequest(TransferRequest::Upload {
    token: 42,
    filename: filename.into(),
    file_size: Some(SIZE),
  });
  again.send(&offered.encode()).await;
  // the offer is taken, and the answer goes where the client's requests to bob go
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::TransferResponse(TransferResponse::Allowed {
      token: 42,
      file_size: None,
    })
  );
  let mut transfer = scripted.connect_for_transfer(42).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let destination = running.destination.clone();
  let done = running.result().await.unwrap();
  assert_eq!(done.remote_token, Some(42));
  assert_eq!(std::fs::read(destination).unwrap(), content(SIZE as usize));
}

/// Downloads from one user connect side by side and wait for each other only to ask, so an
/// album from a user nobody can reach fails together after one connection attempt.
#[tokio::test]
async fn downloads_from_a_peer_nobody_can_reach_fail_together() {
  let mut options = crate::support::listening_options();
  options =
    options.peer_connection(|connection| connection.connect_timeout(Duration::from_millis(500)));
  let mut scripted = Scripted::with_options(options).await;
  let started = tokio::time::Instant::now();
  let running: Vec<_> = (1..=5)
    .map(|track| scripted.download(&format!("@@share\\gone\\{track}.flac"), |download| download))
    .collect();
  let ended = async {
    for download in running {
      assert!(download.result().await.is_err());
    }
  };
  // nothing listens where bob is said to be, and bob never acts on the relayed request
  let nowhere = crate::support::free_port();
  let server = &mut scripted.session.server;
  let lookups = async {
    loop {
      server
        .request_matching(|request| {
          matches!(request, ServerRequest::GetPeerAddress(_)).then_some(())
        })
        .await;
      server
        .send(&ServerResponse::GetPeerAddress(GetPeerAddressResponse {
          username: BOB.into(),
          ip: std::net::Ipv4Addr::LOCALHOST,
          port: u32::from(nowhere),
          obfuscation: None,
        }))
        .await;
    }
  };
  tokio::select! {
    () = ended => {}
    () = lookups => unreachable!("the server answers every lookup"),
  }
  assert!(
    started.elapsed() < Duration::from_secs(2),
    "took {:?}",
    started.elapsed()
  );
}
