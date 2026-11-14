//! Downloads from a scripted peer, for what a real uploader does not say on demand: refusing
//! with any reason, reporting a failure at each stage, naming another size, sending at once,
//! sending what does not decode; and how the download asks: governed, with fresh tokens, once per
//! file.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::support::peer::{FakePeer, PeerSide};
use crate::support::scripted::{BOB, SIZE, Scripted, offer, queued, send_file};
use crate::support::transfers::{self, content, reached, scratch, states};
use crate::support::{Logs, PATIENCE, Session, listening_options};
use soulseek_client::{
  ClientError, DownloadOutput, DownloadRequest, Event, RequestStyle, TransferOutcome, TransferState,
};
use soulseek_proto::peer::{
  QueueDownload, TransferRequest, TransferResponse, UploadDenied, UploadFailed, UserInfo,
};
use soulseek_proto::server::GetPeerAddressResponse;
use soulseek_proto::{PeerCode, PeerMessage, ServerRequest, ServerResponse, TransferDirection};
use tokio_util::sync::CancellationToken;

fn rejected(reason: &str) -> impl Fn(&ClientError) -> bool + '_ {
  move |error| matches!(error, ClientError::TransferRejected(given) if given == reason)
}

fn reported_failed(error: &ClientError) -> bool {
  matches!(error, ClientError::TransferReportedFailed(message) if message == "Download reported as failed by remote client")
}

#[tokio::test]
async fn a_refusal_with_any_reason_rejects_the_download() {
  let mut scripted = Scripted::start().await;
  // the reason is passed on as it is and never matched, so a few stand for every one
  let reasons = ["Cancelled", "File not shared.", ""];
  let mut bob = None;
  for (n, reason) in reasons.into_iter().enumerate() {
    let filename = format!("@@share\\refused {n}.mp3");
    let running = scripted.download(&filename, |_| {});
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
        Event::DownloadDenied { message, .. } => Some(message),
        _ => None,
      })
      .await;
    assert_eq!(message, reason);
  }
}

#[tokio::test]
async fn a_transfer_request_answered_with_a_refusal_rejects_the_download_and_queued_waits() {
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\a.mp3", |request| {
    request.options.request_style = RequestStyle::TransferRequest;
  });
  let mut bob = scripted.connected().await;
  let PeerMessage::TransferRequest(asked) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  assert_eq!(asked.direction, TransferDirection::Download);
  assert_eq!(asked.file_size, None, "a download request carries no size");
  bob
    .send(
      &PeerMessage::TransferResponse(TransferResponse::Denied {
        token: asked.token,
        message: "Too many megabytes".into(),
      })
      .encode(),
    )
    .await;
  assert!(rejected("Too many megabytes")(
    &running.result().await.unwrap_err()
  ));

  // "Queued", however spelled, queues the download, which then waits for the offer
  let running = scripted.download("@@share\\b.mp3", |request| {
    request.options.request_style = RequestStyle::TransferRequest;
  });
  let PeerMessage::TransferRequest(asked) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  bob
    .send(
      &PeerMessage::TransferResponse(TransferResponse::Denied {
        token: asked.token,
        message: "queued.".into(),
      })
      .encode(),
    )
    .await;
  let mut events = scripted.client().subscribe();
  let states = tokio::time::timeout(PATIENCE, async {
    loop {
      let downloads = scripted.client().downloads();
      if downloads[0].state == TransferState::QueuedRemotely {
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
  let (_, last) = transfers::states(&mut events, TransferDirection::Download).await;
  assert!(last.state.succeeded());
}

#[tokio::test]
async fn a_peer_that_sends_at_once_is_dialed_with_our_own_token() {
  let mut scripted = Scripted::start().await;
  let mut events = scripted.client().subscribe();
  let running = scripted.download("@@share\\now.mp3", |request| {
    request.options.request_style = RequestStyle::TransferRequest;
  });
  let mut bob = scripted.connected().await;
  let PeerMessage::TransferRequest(asked) = bob.read_peer().await else {
    panic!("a transfer request");
  };
  bob
    .send(
      &PeerMessage::TransferResponse(TransferResponse::Allowed {
        token: asked.token,
        file_size: Some(SIZE),
      })
      .encode(),
    )
    .await;
  let (mut transfer, token) = scripted.accept_transfer().await;
  assert_eq!(token, asked.token);
  send_file(&mut transfer, 0, SIZE as usize).await;
  transfer.closed().await;
  let destination = running.destination.clone();
  let done = running.result().await.unwrap();
  assert_eq!(done.remote_token, None);
  assert_eq!(std::fs::read(destination).unwrap(), content(SIZE as usize));
  let (states, _) = states(&mut events, TransferDirection::Download).await;
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
  let mut events = scripted.client().subscribe();
  let (running, mut bob) = scripted.queued_download("@@share\\p.mp3").await;
  offer(&mut bob, "@@share\\p.mp3", 6, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(6).await;
  send_file(&mut transfer, 0, 1000).await;
  // a failure is taken before bytes still unread, so bob sends it once all 1000 are written
  reached(&mut events, 1000).await;
  bob.send(&failed("@@share\\p.mp3")).await;
  let destination = running.destination.clone();
  let error = running.result().await.unwrap_err();
  assert!(reported_failed(&error), "{error:?}");
  // what arrived is kept for a resume
  let kept = std::fs::read(destination).unwrap();
  assert_eq!(kept, content(1000));
  let downloads = scripted.client().downloads();
  assert!(downloads.is_empty());
}

#[tokio::test]
async fn a_refusal_while_the_request_awaits_its_answer_ends_the_download_at_once() {
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\r.mp3", |request| {
    request.options.request_style = RequestStyle::TransferRequest;
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
  let mut events = scripted.client().subscribe();
  let (running, mut bob) = scripted.queued_download("@@share\\big.mp3").await;
  bob
    .send(
      &PeerMessage::TransferRequest(TransferRequest {
        direction: TransferDirection::Upload,
        token: 9,
        filename: "@@share\\big.mp3".into(),
        file_size: Some(SIZE + 1),
      })
      .encode(),
    )
    .await;
  let error = running.result().await.unwrap_err();
  assert!(
    matches!(error, ClientError::SizeMismatch { local: SIZE, remote, .. } if remote == SIZE + 1),
    "{error:?}"
  );
  let (_, last) = states(&mut events, TransferDirection::Download).await;
  assert_eq!(
    last.state,
    TransferState::Completed(TransferOutcome::Aborted)
  );

  // a resolver can settle on the uploader's size
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\settled.mp3", |request| {
    request.size = SIZE - 10;
    request.options.size_mismatch_resolver = Some(Arc::new(|_, remote| Ok(remote)));
  });
  let mut bob = scripted.connected().await;
  queued(&mut bob, "@@share\\settled.mp3").await;
  offer(&mut bob, "@@share\\settled.mp3", 11, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(11).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  let done = running.result().await.unwrap();
  assert_eq!((done.size, done.bytes_transferred), (SIZE, SIZE));
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
    matches!(&error, ClientError::Message { message, .. } if message == "peer sent an unparseable UploadDenied"),
    "{error:?}"
  );
}

#[tokio::test]
async fn a_governor_that_allows_nothing_yet_is_asked_again() {
  let mut scripted = Scripted::start().await;
  let asked = Arc::new(AtomicU32::new(0));
  let reported = Arc::new(AtomicU32::new(0));
  let running = scripted.download("@@share\\paced.mp3", |request| {
    let asked = asked.clone();
    request.options.governor = Some(Arc::new(move |_, wanted, _| {
      // nothing the first time
      let first = asked.fetch_add(1, Ordering::SeqCst) == 0;
      Box::pin(async move { if first { 0 } else { wanted } })
    }));
    let reported = reported.clone();
    request.options.reporter = Some(Arc::new(move |_, _, _, moved| {
      reported.fetch_add(moved, Ordering::SeqCst);
    }));
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
async fn a_fresh_token_skips_one_a_transfer_holds() {
  let scripted = Scripted::start().await;
  // the token the counter hands out next is taken by hand first
  let next = scripted.client().next_token() + 1;
  let held = scripted.download("@@share\\held.mp3", |request| {
    request.token = Some(next);
  });
  tokio::time::timeout(PATIENCE, async {
    while scripted.client().downloads().is_empty() {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  let fresh = scripted.download("@@share\\fresh.mp3", |_| {});
  let tokens = tokio::time::timeout(PATIENCE, async {
    loop {
      let mut tokens: Vec<_> = scripted
        .client()
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
  held.cancel.cancel();
  fresh.cancel.cancel();
}

#[tokio::test]
async fn one_download_per_file_and_one_transfer_per_token() {
  let mut scripted = Scripted::start().await;
  let (_running, _bob) = scripted.queued_download("@@share\\once.mp3").await;
  let again = scripted
    .client()
    .enqueue_download(
      DownloadRequest::new(
        BOB,
        "@@share\\once.mp3",
        SIZE,
        DownloadOutput::File(scratch().join("again")),
      ),
      &CancellationToken::new(),
    )
    .await;
  assert!(matches!(again, Err(ClientError::DuplicateTransfer { .. })));
  let token = scripted.client().downloads()[0].token;
  let mut request = DownloadRequest::new(
    BOB,
    "@@share\\other.mp3",
    SIZE,
    DownloadOutput::File(scratch().join("other")),
  );
  request.token = Some(token);
  assert!(matches!(
    scripted
      .client()
      .enqueue_download(request, &CancellationToken::new())
      .await,
    Err(ClientError::DuplicateToken(taken)) if taken == token
  ));
}

#[tokio::test]
async fn an_offer_that_comes_while_the_download_still_looks_up_its_peer_is_allowed() {
  let mut scripted = Scripted::start().await;
  let filename = "@@share\\early.mp3";
  let _running = scripted.download(filename, |_| {});
  // the download is registered and waits for bob's address
  let server = &mut scripted.session.server;
  server
    .request_matching(|request| matches!(request, ServerRequest::GetPeerAddress(_)).then_some(()))
    .await;
  // bob still has the file queued from before, and offers it now
  let mut bob = scripted.session.peer(BOB).await;
  let offered = PeerMessage::TransferRequest(TransferRequest {
    direction: TransferDirection::Upload,
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

/// Downloads from bob, each named long enough that bob, who does not read, soon leaves the
/// connection full: the later ones take it and wait their turn to ask behind writes that cannot
/// end. Each is shaped by `shape`.
fn filling(
  scripted: &Scripted,
  shape: impl Fn(&mut DownloadRequest),
) -> (Vec<String>, Vec<crate::support::scripted::Downloading>) {
  let names: Vec<String> = (0..300)
    .map(|n| format!("@@share\\{n:03} {}.flac", "x".repeat(60_000)))
    .collect();
  let running = names
    .iter()
    .map(|filename| scripted.download(filename, &shape))
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
  let (names, running) = filling(&scripted, |_| {});
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
  let ended = running.iter().filter(|d| d.task.is_finished()).count();
  assert_eq!(ended, 0, "downloads failed with the connection");
}

/// Bob takes the first transfer request of the downloads filling the connection and closes it;
/// every download whose request goes out anew on the new connection keeps running, and the
/// first of them takes the answer bob gives there. A request whose write ended before the close
/// is not sent anew, and the close fails its wait as before.
async fn transfer_requests_sent_anew() {
  let mut scripted = Scripted::start().await;
  let (names, running) = filling(&scripted, |request| {
    request.options.request_style = RequestStyle::TransferRequest;
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
      asked.push(request.token);
      anew.push(
        names
          .iter()
          .position(|n| n.as_str() == request.filename.as_str())
          .unwrap(),
      );
      (request.filename.as_str() == last.as_str()).then_some(())
    }
    _ => None,
  })
  .await;
  let ended: Vec<usize> = anew
    .iter()
    .copied()
    .filter(|&n| running[n].task.is_finished())
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
        .downloads()
        .into_iter()
        .any(|t| t.token == asked[0] && t.state == TransferState::QueuedRemotely);
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
  let answers = Arc::new(AtomicU32::new(0));
  let mut options = listening_options();
  let counted = answers.clone();
  options.resolvers.user_info_resolver = Some(Arc::new(move |_, _| {
    counted.fetch_add(1, Ordering::SeqCst);
    Box::pin(async {
      Ok(UserInfo {
        description: String::new(),
        picture: Some(vec![0; PICTURE]),
        upload_slots: 1,
        queue_length: 0,
        has_free_upload_slot: true,
        upload_permitted: None,
      })
    })
  }));
  let mut scripted = Scripted {
    session: Session::with_options(options).await,
    bob: FakePeer::with_receive_buffer(64 << 10).await,
  };
  let first = "@@share\\first.flac";
  let _first = scripted.download(first, |_| {});
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
  let _ahead = scripted.download(ahead, |_| {});
  eventually("the request ahead is queued", || {
    conn.write_queue_depth() == 1
  })
  .await;
  let behind = "@@share\\behind.flac";
  let mut behind_task = scripted.download(behind, |_| {}).task;
  logs
    .wait_for(&["Retrieved cached message connection", "behind.flac"])
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
      scripted.client().message_connection_address(BOB) == Some(from)
    })
    .await;
    reconnected.push(side);
  }
  let current = reconnected.last_mut().unwrap();
  skip_to_queued(&mut superseded, ahead).await;
  tokio::select! {
    ended = &mut behind_task => panic!("the download ended: {ended:?}"),
    asked = current.read_peer() => assert_eq!(
      asked,
      PeerMessage::QueueDownload(QueueDownload { filename: behind.into() })
    ),
  }
}

/// The acceptance of an offer goes on a new connection when the one it was written to closes.
/// Bob reads nothing, so the requests fill the connection; once the client has stopped getting
/// requests out, bob offers the first file there, closes once the download takes the offer, and
/// the file arrives.
#[tokio::test]
async fn an_offers_acceptance_whose_connection_closed_goes_on_a_new_one() {
  use tokio::sync::broadcast::error::RecvError;

  let mut scripted = Scripted::start().await;
  let mut events = scripted.client().subscribe();
  let (names, mut running) = filling(&scripted, |_| {});
  let mut bob = scripted.connected().await;
  // the requests written so far; none for a while means the connection is full, since bob
  // reads none of them. A window too short leaves room for the acceptance, whose write then ends
  // and is not sent anew, so the test fails; it cannot pass falsely
  let quiet = Duration::from_secs(1);
  loop {
    let requested = tokio::time::timeout(quiet, async {
      loop {
        match events.recv().await {
          Ok(Event::TransferStateChanged { transfer, .. })
            if transfer.state == TransferState::Requested =>
          {
            return;
          }
          Ok(_) | Err(RecvError::Lagged(_)) => {}
          Err(RecvError::Closed) => panic!("the client is gone"),
        }
      }
    })
    .await;
    if requested.is_err() {
      break;
    }
  }
  let offered = PeerMessage::TransferRequest(TransferRequest {
    direction: TransferDirection::Upload,
    token: 7,
    filename: names[0].as_str().into(),
    file_size: Some(SIZE),
  });
  bob.send(&offered.encode()).await;
  tokio::time::timeout(PATIENCE, async {
    loop {
      match events.recv().await {
        Ok(Event::TransferStateChanged { transfer, .. })
          if transfer.state == TransferState::Initializing =>
        {
          return;
        }
        Ok(_) | Err(RecvError::Lagged(_)) => {}
        Err(RecvError::Closed) => panic!("the client is gone"),
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
    .map(|filename| scripted.download(filename, |_| {}))
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
  let second = scripted.download(filename, |_| {});
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
  // asked bob; bob reaches us on a connection of his own
  let filename = "@@share\\second.mp3";
  let waiting = scripted.download(filename, |_| {});
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

/// Downloads from one user connect side by side and wait for each other only to ask, so an
/// album from a user nobody can reach fails together after one connection attempt.
#[tokio::test]
async fn downloads_from_a_peer_nobody_can_reach_fail_together() {
  let mut options = crate::support::listening_options();
  options.peer_connection_options.connect_timeout = Duration::from_millis(500);
  let mut scripted = Scripted::with_options(options).await;
  let started = tokio::time::Instant::now();
  let running: Vec<_> = (1..=5)
    .map(|track| scripted.download(&format!("@@share\\gone\\{track}.flac"), |_| {}))
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
