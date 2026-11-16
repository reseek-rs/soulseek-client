//! What the client does with each peer message, sent by a scripted peer on a message connection:
//! answers from the application's handler, refusals, and what a hostile or vanishing peer
//! causes.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::support::peer::{FakePeer, PeerSide};
use crate::support::{Answering, Logs, PATIENCE, Session, listening_options, poll_once};
use soulseek_client::handler::{BrowseReply, EncodedFrame, Upload, UploadRequest};
use soulseek_client::search::{Outcome, State};
use soulseek_client::testing::{Engine, IncomingBrowse, WaitKey};
use soulseek_client::{Error, Handler, Peer};
use soulseek_proto::init::PeerInit;
use soulseek_proto::peer::*;
use soulseek_proto::{
  InitMessage, Limits, PeerCode, PeerConnectionType, PeerMessage, RemoteFilename, ServerRequest,
};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Answers browse and user info as [`Answering`] does, says every file is third in the queue,
/// and decides an upload by the file's name
struct Serving;

impl Handler for Serving {
  async fn browse(&self, peer: &Peer) -> Option<BrowseReply> {
    Answering.browse(peer).await
  }

  async fn user_info(&self, peer: &Peer) -> Option<UserInfo> {
    Answering.user_info(peer).await
  }

  async fn upload(&self, _peer: &Peer, request: &UploadRequest) -> Upload {
    match request.filename.as_str() {
      name if name.contains("banned") => Upload::Deny("Banned".into()),
      name if name.contains("away") => Upload::Later,
      _ => Upload::Queue,
    }
  }

  async fn place_in_queue(&self, _peer: &Peer, _filename: &RemoteFilename) -> Option<u32> {
    Some(3)
  }
}

/// answers only a place in queue request
struct PlaceOnly;

impl Handler for PlaceOnly {
  async fn place_in_queue(&self, _peer: &Peer, _filename: &RemoteFilename) -> Option<u32> {
    Some(3)
  }
}

/// answers a place in queue request, and tells the receiver which peer asked
struct Places(mpsc::UnboundedSender<Peer>);

impl Handler for Places {
  async fn place_in_queue(&self, peer: &Peer, _filename: &RemoteFilename) -> Option<u32> {
    // the test may have stopped listening
    let _ = self.0.send(peer.clone());
    Some(1)
  }
}

/// answers every browse with a listing far longer than the sockets hold, and counts them
struct EndlessListing(Arc<AtomicUsize>);

impl Handler for EndlessListing {
  async fn browse(&self, _peer: &Peer) -> Option<BrowseReply> {
    self.0.fetch_add(1, Ordering::SeqCst);
    let len = 1 << 30;
    Some(BrowseReply::Encoded(EncodedFrame {
      len,
      reader: Box::pin(tokio::io::repeat(0).take(len)),
    }))
  }

  async fn user_info(&self, peer: &Peer) -> Option<UserInfo> {
    Answering.user_info(peer).await
  }
}

async fn session() -> Session {
  Session::with_options(listening_options().handler(Serving)).await
}

/// a scripted peer named bob, connected and known to the client once it answered a request
async fn bob(session: &Session) -> PeerSide {
  let mut bob = session.peer("bob").await;
  bob.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    bob.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  bob
}

/// bob, whose connection the client takes as his once the server gives his address, as it does
/// when it has something to ask him; later connections from there are his at once
async fn bobs_own(session: &mut Session) -> PeerSide {
  let bob = bob(session).await;
  let client = session.client.clone();
  let taking = tokio::spawn(async move {
    client
      .get_or_add_message_connection("bob", None, &CancellationToken::new())
      .await
  });
  session
    .give_address("bob", SocketAddr::from((Ipv4Addr::LOCALHOST, 2234)))
    .await;
  let taken = taking.await.unwrap().unwrap();
  assert_eq!(taken.handle().remote(), bob.stream.local_addr().unwrap());
  bob
}

/// bob asks for his place in the queue, which the handler always answers
async fn place_asked(bob: &mut PeerSide) -> PeerMessage {
  let filename: RemoteFilename = "@@music\\fine.mp3".into();
  bob
    .send(&PeerMessage::PlaceInQueueRequest(PlaceInQueueRequest { filename }).encode())
    .await;
  bob.read_peer().await
}

#[tokio::test]
async fn a_browse_or_user_info_request_the_handler_does_not_implement_gets_no_reply() {
  let session = Session::with_options(listening_options().handler(PlaceOnly)).await;
  let mut bob = session.peer("bob").await;
  bob.send(&PeerMessage::BrowseRequest.encode()).await;
  bob.send(&PeerMessage::InfoRequest.encode()).await;
  // the next thing bob reads is the answer to what he asks after them
  assert!(matches!(
    place_asked(&mut bob).await,
    PeerMessage::PlaceInQueueResponse(_)
  ));
}

#[tokio::test]
async fn a_browse_or_user_info_the_handler_has_no_answer_for_sends_nothing() {
  struct NoAnswer;
  impl Handler for NoAnswer {
    async fn browse(&self, _peer: &Peer) -> Option<BrowseReply> {
      None
    }

    async fn user_info(&self, _peer: &Peer) -> Option<UserInfo> {
      None
    }

    async fn place_in_queue(&self, _peer: &Peer, _filename: &RemoteFilename) -> Option<u32> {
      Some(3)
    }
  }
  let session = Session::with_options(listening_options().handler(NoAnswer)).await;
  let mut bob = session.peer("bob").await;
  bob.send(&PeerMessage::BrowseRequest.encode()).await;
  bob.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    place_asked(&mut bob).await,
    PeerMessage::PlaceInQueueResponse(_)
  ));
}

#[tokio::test]
async fn a_queued_download_hears_its_place_only_on_asking_when_the_option_is_off() {
  let session = Session::with_options(
    listening_options()
      .handler(Serving)
      .send_place_when_queued(false),
  )
  .await;
  let mut bob = bob(&session).await;
  let queue = |filename: &str| {
    PeerMessage::QueueDownload(QueueDownload {
      filename: filename.into(),
    })
    .encode()
  };
  // queued without a word; the next thing bob reads answers what he sends after it
  bob.send(&queue("@@music\\fine.mp3")).await;
  bob.send(&queue("@@music\\banned.mp3")).await;
  assert!(matches!(
    bob.read_peer().await,
    PeerMessage::UploadDenied(_)
  ));
  assert!(matches!(
    place_asked(&mut bob).await,
    PeerMessage::PlaceInQueueResponse(_)
  ));
}

#[tokio::test]
async fn queueing_a_download_is_answered_with_the_place_or_the_refusal() {
  let session = session().await;
  let mut bob = bob(&session).await;
  let queue = |filename: &str| {
    PeerMessage::QueueDownload(QueueDownload {
      filename: filename.into(),
    })
    .encode()
  };
  bob.send(&queue("@@music\\fine.mp3")).await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::PlaceInQueueResponse(PlaceInQueueResponse {
      filename: "@@music\\fine.mp3".into(),
      place: 3,
    })
  );
  bob.send(&queue("@@music\\banned.mp3")).await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::UploadDenied(UploadDenied {
      filename: "@@music\\banned.mp3".into(),
      reason: "Banned".into(),
    })
  );
  // a file that cannot be queued now is failed, so the peer asks again
  bob.send(&queue("@@music\\away.mp3")).await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::UploadFailed(UploadFailed {
      filename: "@@music\\away.mp3".into(),
    })
  );
}

#[tokio::test]
async fn the_upload_handler_is_told_which_request_the_peer_made() {
  struct LegacyTokens(mpsc::UnboundedSender<Option<u32>>);
  impl Handler for LegacyTokens {
    async fn user_info(&self, peer: &Peer) -> Option<UserInfo> {
      Answering.user_info(peer).await
    }

    async fn upload(&self, _peer: &Peer, request: &UploadRequest) -> Upload {
      // the test may have stopped listening
      let _ = self.0.send(request.legacy_token);
      Upload::Queue
    }
  }
  let (asked, mut asks) = mpsc::unbounded_channel();
  let options = listening_options().handler(LegacyTokens(asked));
  let session = Session::with_options(options).await;
  let mut bob = bob(&session).await;
  let filename: RemoteFilename = "@@music\\fine.mp3".into();
  let legacy = PeerMessage::TransferRequest(TransferRequest::Download {
    token: 6,
    filename: filename.clone(),
  });
  bob.send(&legacy.encode()).await;
  bob
    .send(&PeerMessage::QueueDownload(QueueDownload { filename }).encode())
    .await;
  assert_eq!(asks.recv().await.unwrap(), Some(6));
  assert_eq!(asks.recv().await.unwrap(), None);
}

#[tokio::test]
async fn a_legacy_download_request_the_handler_refuses_is_denied_with_its_reason() {
  let session = session().await;
  let mut bob = bob(&session).await;
  bob
    .send(
      &PeerMessage::TransferRequest(TransferRequest::Download {
        token: 6,
        filename: "@@music\\banned.mp3".into(),
      })
      .encode(),
    )
    .await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::TransferResponse(TransferResponse::Denied {
      token: 6,
      message: "Banned".into()
    })
  );
  // the one refusal is the whole answer: what bob reads next answers what he asks next
  assert!(matches!(
    place_asked(&mut bob).await,
    PeerMessage::PlaceInQueueResponse(_)
  ));
}

#[tokio::test]
async fn a_legacy_download_request_that_cannot_be_queued_now_is_queued_then_failed() {
  let session = session().await;
  let mut bob = bob(&session).await;
  bob
    .send(
      &PeerMessage::TransferRequest(TransferRequest::Download {
        token: 8,
        filename: "@@music\\away.mp3".into(),
      })
      .encode(),
    )
    .await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::TransferResponse(TransferResponse::Denied {
      token: 8,
      message: "Queued".into()
    })
  );
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::UploadFailed(UploadFailed {
      filename: "@@music\\away.mp3".into(),
    })
  );
}

// an offer for a file the client does download goes to that download; the transfer suites
// cover it
#[tokio::test]
async fn an_upload_offer_for_a_file_we_do_not_download_is_refused() {
  let session = session().await;
  let mut bob = bob(&session).await;
  let offer = |token, filename: &str| {
    PeerMessage::TransferRequest(TransferRequest::Upload {
      token,
      filename: filename.into(),
      file_size: Some(4_000_000),
    })
    .encode()
  };
  bob.send(&offer(7, "@@music\\unknown.mp3")).await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::TransferResponse(TransferResponse::Denied {
      token: 7,
      message: "Cancelled".into()
    })
  );
}

#[tokio::test]
async fn an_invalid_browse_response_fails_the_browse() {
  let mut session = session().await;
  let mut bob = bobs_own(&mut session).await;
  // a browse follows its response from the header on, and waits for the whole of it
  let _header = session.client.waiter().wait::<IncomingBrowse>(
    WaitKey::BrowseResponseConnection {
      username: "bob".into(),
    },
    None,
    CancellationToken::new(),
  );
  let response = session.client.waiter().wait::<Arc<BrowseResponse>>(
    WaitKey::Peer {
      code: PeerCode::BrowseResponse,
      username: "bob".into(),
    },
    None,
    CancellationToken::new(),
  );
  bob
    .send(&[9, 0, 0, 0, 5, 0, 0, 0, b'n', b'o', b'p', b'e', b'!'])
    .await;
  let error = response.await.unwrap_err();
  assert!(
    matches!(&error, Error::Message { kind: soulseek_client::error::MessageKind::Read, message, .. } if message == "the peer returned an invalid browse response"),
    "{error:?}"
  );
}

/// `message` encoded, its zlib stream cut short before its checksum
fn cut_short(message: PeerMessage) -> Vec<u8> {
  let mut frame = message.encode();
  frame.truncate(frame.len() - 4);
  let len = frame.len() as u32 - 4;
  frame[..4].copy_from_slice(&len.to_le_bytes());
  frame
}

/// a frame of `code` whose payload is `len` zero bytes
fn zeros(code: PeerCode, len: usize) -> Vec<u8> {
  let mut frame = (len as u32 + 4).to_le_bytes().to_vec();
  frame.extend(code.code().to_le_bytes());
  frame.resize(frame.len() + len, 0);
  frame
}

#[tokio::test]
async fn responses_nobody_asked_for_are_dropped_before_inflating() {
  let logs = Logs::capture();
  let session = session().await;
  // anyone can connect and give any name; nothing was asked of this one and no search runs
  let mut mallory = session.peer("mallory").await;
  let mut frames = cut_short(PeerMessage::BrowseResponse(BrowseResponse {
    directories: Vec::new(),
    locked_directories: Vec::new(),
  }));
  // answers of a mebibyte each, which reading in would have held for as long as they took
  let unasked = [
    PeerCode::FolderContentsResponse,
    PeerCode::InfoResponse,
    PeerCode::SearchResponse,
  ];
  for code in unasked {
    frames.extend(zeros(code, 1 << 20));
  }
  frames.extend(PeerMessage::InfoRequest.encode());
  mallory.send(&frames).await;
  // the request after them is answered, so they were passed over whole
  assert!(matches!(
    mallory.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  logs
    .wait_for(&["browse response nobody follows", "skipped"])
    .await;
  for code in unasked {
    assert_eq!(
      logs
        .lines_with(&["frame skipped", &format!("code={}", code.code())])
        .len(),
      1,
      "{code:?} is skipped unread: {}",
      logs.text()
    );
  }
  // reading any of them would have found it cut short or malformed
  assert!(
    logs.lines_with(&["message rejected"]).is_empty(),
    "{}",
    logs.text()
  );
}

#[tokio::test]
async fn ignored_and_unknown_messages_leave_the_connection_up() {
  let logs = Logs::capture();
  let session = session().await;
  let mut bob = bob(&session).await;
  let mut frames = PeerMessage::UploadQueueNotification.encode();
  // a code no message has
  frames.extend([4, 0, 0, 0, 99, 0, 0, 0]);
  frames.extend(PeerMessage::InfoRequest.encode());
  bob.send(&frames).await;
  assert!(matches!(
    bob.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  assert_eq!(
    logs
      .lines_with(&["unhandled peer message", "code=99"])
      .len(),
    1
  );
  assert!(
    logs
      .lines_with(&["unhandled peer message", "code=52"])
      .is_empty()
  );
}

#[tokio::test]
async fn a_frame_longer_than_any_message_closes_the_connection() {
  let logs = Logs::capture();
  let session = session().await;
  let mut bob = bob(&session).await;
  bob.send(&u32::MAX.to_le_bytes()).await;
  bob.closed().await;
  // a peer can send this as often as it reconnects, so it is no warning, and the close says why
  logs
    .wait_for(&["connection closed", "message length out of range"])
    .await;
  assert!(logs.lines_with(&[" WARN "]).is_empty(), "{}", logs.text());
}

#[tokio::test]
async fn a_frame_longer_than_its_message_is_skipped_unread() {
  let logs = Logs::capture();
  let session = session().await;
  let mut bob = bob(&session).await;
  // a queue request just over its bound, then a request on the same connection
  let len = Limits::DEFAULT.peer_message_bytes(PeerCode::QueueDownload) + 1;
  let mut frames = len.to_le_bytes().to_vec();
  frames.extend(PeerCode::QueueDownload.code().to_le_bytes());
  frames.extend(vec![0; len as usize - 4]);
  frames.extend(PeerMessage::InfoRequest.encode());
  bob.send(&frames).await;
  assert!(matches!(
    bob.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  assert_eq!(
    logs.lines_with(&["frame skipped", "code=43"]).len(),
    1,
    "{}",
    logs.text()
  );
}

#[tokio::test]
async fn a_replaced_connection_closing_never_takes_its_replacement_with_it() {
  let logs = Logs::capture();
  let mut session = session().await;
  let mut first = bobs_own(&mut session).await;
  let mut second = bob(&session).await;
  // the replaced connection is still read and answered on until bob closes it
  first.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    first.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  drop(first);
  logs
    .wait_for(&["connection closed", "remote connection closed"])
    .await;
  // the replacement still answers, and it is the one the client hands out
  second.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    second.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  let connections = session.client.message_connections();
  assert_eq!(connections.len(), 1, "{connections:?}");
  assert_eq!(connections[0].1, second.stream.local_addr().unwrap());
  let cached = session
    .client
    .get_or_add_message_connection("bob", None, &CancellationToken::new())
    .await
    .unwrap();
  assert_eq!(
    cached.handle().remote(),
    second.stream.local_addr().unwrap()
  );
}

#[tokio::test]
async fn a_wait_fails_when_its_connection_closes_though_another_request_finds_it_closed_first() {
  let mut session = session().await;
  let _bob = bobs_own(&mut session).await;
  let client = &session.client;
  let cancel = CancellationToken::new();
  let conn = client
    .get_or_add_message_connection("bob", None, &cancel)
    .await
    .unwrap();
  let wait = client.waiter().wait::<UserInfo>(
    WaitKey::Peer {
      code: PeerCode::InfoResponse,
      username: "bob".into(),
    },
    Some(PATIENCE * 6),
    CancellationToken::new(),
  );
  assert!(conn.handle().disconnect("remote connection closed", None));
  // another request looks for the connection before anything else has seen it close
  let mut another = Box::pin(client.get_or_add_message_connection("bob", None, &cancel));
  assert!(poll_once(&mut another).await.is_pending());
  let failed = tokio::time::timeout(Duration::from_secs(2), wait).await;
  let error = failed
    .expect("the wait ends when the connection closes")
    .unwrap_err();
  assert_eq!(
    error.to_string(),
    "the message connection to bob closed: remote connection closed"
  );
}

/// an address on loopback other than the one the scripted peers use
const ELSEWHERE: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 2);

/// bob's message connection, which the client dialed at `known` after asking the server
async fn dialed_bob(session: &mut Session, known: &FakePeer) -> PeerSide {
  let client = session.client.clone();
  let dialing = tokio::spawn(async move {
    client
      .get_or_add_message_connection("bob", None, &CancellationToken::new())
      .await
  });
  session.give_address("bob", known.address()).await;
  let side = known.accept().await;
  let dialed = dialing.await.unwrap().unwrap();
  assert_eq!(dialed.handle().remote(), known.address());
  side
}

/// a scripted peer at `from` that connected to the client for messages as `username`
async fn introduced_from(session: &Session, from: Ipv4Addr, username: &str) -> PeerSide {
  let mut side = PeerSide::connect_from(from, session.listen_address()).await;
  let introduce = InitMessage::PeerInit(PeerInit {
    username: username.into(),
    connection_type: PeerConnectionType::Peer,
    token: 0,
  });
  side.send(&introduce.encode()).await;
  side
}

/// `side` is read and answered on
async fn answered(side: &mut PeerSide) {
  side.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    side.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
}

#[tokio::test]
async fn a_connection_naming_a_user_from_an_address_the_server_does_not_give_is_kept_beside() {
  let logs = Logs::capture();
  let mut session = session().await;
  let known = FakePeer::start().await;
  let mut bob = dialed_bob(&mut session, &known).await;
  assert!(matches!(bob.read_init().await, InitMessage::PeerInit(_)));
  let mut other = introduced_from(&session, ELSEWHERE, "bob").await;
  // the address cached for bob is not the newcomer's, so the server is asked again, and still
  // gives the one the client dialed
  session.give_address("bob", known.address()).await;
  answered(&mut other).await;
  logs
    .wait_for(&["address the server does not give", "bob", "127.0.0.2"])
    .await;
  assert_eq!(
    session.client.message_connection_address("bob"),
    Some(known.address())
  );
  // what the client writes to bob still goes to the connection it dialed
  let current = session
    .client
    .get_or_add_message_connection("bob", None, &CancellationToken::new())
    .await
    .unwrap();
  current
    .write(PeerMessage::InfoRequest.encode())
    .await
    .unwrap();
  assert!(matches!(bob.read_peer().await, PeerMessage::InfoRequest));
}

#[tokio::test]
async fn a_handler_is_told_whether_the_peers_name_is_confirmed() {
  let (asked, mut peers) = mpsc::unbounded_channel();
  let options = listening_options().handler(Places(asked));
  let mut session = Session::with_options(options).await;
  let known = FakePeer::start().await;
  let mut bob = dialed_bob(&mut session, &known).await;
  assert!(matches!(bob.read_init().await, InitMessage::PeerInit(_)));
  // the connection the client dialed at the address the server gave is bob's
  place_asked(&mut bob).await;
  let dialed = peers.recv().await.unwrap();
  assert_eq!((dialed.username.as_str(), dialed.confirmed), ("bob", true));
  // one naming bob from an address the server does not give is only said to be
  let mut other = introduced_from(&session, ELSEWHERE, "bob").await;
  session.give_address("bob", known.address()).await;
  place_asked(&mut other).await;
  let claimed = peers.recv().await.unwrap();
  assert_eq!(
    (claimed.username.as_str(), claimed.confirmed),
    ("bob", false)
  );
  assert_eq!(claimed.address.ip(), ELSEWHERE);
}

#[tokio::test]
async fn a_peer_arriving_between_sessions_is_served_as_unconfirmed() {
  let (asked, mut peers) = mpsc::unbounded_channel();
  let options = listening_options()
    .listen_while_disconnected(true)
    .handler(Places(asked));
  let session = Session::with_options(options).await;
  session.client.disconnect().await;
  // no server says where bob is, so his name is only what he says
  let mut bob = session.peer("bob").await;
  assert!(matches!(
    place_asked(&mut bob).await,
    PeerMessage::PlaceInQueueResponse(_)
  ));
  let peer = peers.recv().await.unwrap();
  assert_eq!((peer.username.as_str(), peer.confirmed), ("bob", false));
}

#[tokio::test]
async fn a_connection_from_the_address_the_server_now_gives_replaces_the_open_one() {
  let mut session = session().await;
  let known = FakePeer::start().await;
  let _bob = dialed_bob(&mut session, &known).await;
  let mut moved = introduced_from(&session, ELSEWHERE, "bob").await;
  // bob's address changed since the client looked it up, and the server says so
  session
    .give_address("bob", SocketAddr::from((ELSEWHERE, known.address().port())))
    .await;
  answered(&mut moved).await;
  let from = moved.stream.local_addr().unwrap();
  assert_eq!(session.client.message_connection_address("bob"), Some(from));
}

#[tokio::test]
async fn a_user_on_a_new_address_with_nothing_open_is_served_without_asking() {
  let mut session = session().await;
  let mut bob = introduced_from(&session, ELSEWHERE, "bob").await;
  answered(&mut bob).await;
  // nothing says the newcomer is bob, so what the client asks bob never goes to it
  assert_eq!(session.client.message_connection_address("bob"), None);
  // the server hears nothing about bob before a request sent after he was served
  let marker = ServerRequest::CheckPrivileges;
  session.client.write_server_message(&marker).await.unwrap();
  assert_eq!(session.server.request().await, marker);
}

#[tokio::test]
async fn a_stranger_first_under_a_name_never_carries_our_requests() {
  let mut session = session().await;
  let mut stranger = introduced_from(&session, ELSEWHERE, "bob").await;
  answered(&mut stranger).await;
  // asking something of bob looks him up and dials where the server says he is
  let known = FakePeer::start().await;
  let mut bob = dialed_bob(&mut session, &known).await;
  assert!(matches!(bob.read_init().await, InitMessage::PeerInit(_)));
  assert!(stranger.quiet_for(Duration::from_millis(200)).await);
}

#[tokio::test]
async fn a_user_from_the_address_already_given_is_taken_at_once() {
  let mut session = session().await;
  let client = session.client.clone();
  let looking = tokio::spawn(async move { client.users().address("bob").await });
  let given = SocketAddr::from((ELSEWHERE, 2234));
  session.give_address("bob", given).await;
  assert_eq!(looking.await.unwrap().unwrap(), given);
  let mut bob = introduced_from(&session, ELSEWHERE, "bob").await;
  answered(&mut bob).await;
  let from = bob.stream.local_addr().unwrap();
  assert_eq!(session.client.message_connection_address("bob"), Some(from));
  assert!(asked_nothing_more(&mut session).await);
}

#[tokio::test]
async fn connections_nobody_confirmed_are_bounded_like_the_cache() {
  let mut options = listening_options();
  options = options.limits(|limits| limits.max_cached_peer_connections(2));
  let session = Session::with_options(options).await;
  let mut sides = Vec::new();
  for name in ["dave", "erin", "frank"] {
    let mut side = session.peer(name).await;
    answered(&mut side).await;
    sides.push(side);
  }
  // the idlest makes room, and the others are still answered on
  sides[0].closed().await;
  for side in &mut sides[1..] {
    answered(side).await;
  }
}

#[tokio::test]
async fn a_peer_reconnecting_over_and_over_keeps_two_replaced_connections() {
  let mut session = session().await;
  let mut sides = vec![bobs_own(&mut session).await];
  // the server's answer is cached, so the rest are not asked about
  for _ in 0..4 {
    sides.push(bob(&session).await);
  }
  // the oldest ones are closed as newer ones replace them
  for oldest in &mut sides[..2] {
    oldest.closed().await;
  }
  // the two replaced last are still answered on, and the newest is the current one
  for kept in &mut sides[2..] {
    kept.send(&PeerMessage::InfoRequest.encode()).await;
    assert!(matches!(
      kept.read_peer().await,
      PeerMessage::InfoResponse(_)
    ));
  }
  let connections = session.client.message_connections();
  assert_eq!(connections.len(), 1, "{connections:?}");
  assert_eq!(connections[0].1, sides[4].stream.local_addr().unwrap());
}

#[tokio::test]
async fn one_address_posing_as_many_users_cannot_evict_other_connections() {
  let mut options = listening_options();
  // room in the cache for the client's connection to carol, and beside it for the eight one
  // address may open to the client
  options = options.limits(|limits| limits.max_cached_peer_connections(9));
  let session = Session::with_options(options).await;
  let carol = FakePeer::start().await;
  let address = SocketAddr::from((Ipv4Addr::LOCALHOST, u16::try_from(carol.port()).unwrap()));
  session
    .client
    .get_or_add_message_connection("carol", Some(address), &CancellationToken::new())
    .await
    .unwrap();
  let mut ours = carol.accept().await;
  assert!(matches!(ours.read_init().await, InitMessage::PeerInit(_)));
  // each busier than the one to carol, so that one is the idlest of all
  let mut posers = Vec::new();
  for n in 1..=8 {
    let mut poser = session.peer(&format!("poser{n}")).await;
    poser.send(&PeerMessage::InfoRequest.encode()).await;
    assert!(matches!(
      poser.read_peer().await,
      PeerMessage::InfoResponse(_)
    ));
    posers.push(poser);
  }
  session.peer("poser9").await.closed().await;
  ours.send(&PeerMessage::InfoRequest.encode()).await;
  assert!(matches!(
    ours.read_peer().await,
    PeerMessage::InfoResponse(_)
  ));
  // the posers are answered, and none of them is taken as the user it names
  let connected: Vec<String> = session
    .client
    .message_connections()
    .into_iter()
    .map(|(username, _)| username)
    .collect();
  assert_eq!(connected, ["carol"]);
}

/// Waits until `count` connections from `from` naming `username` were identified. An identified
/// connection is counted against its address in the same turn, before it waits for anything.
async fn identified(logs: &Logs, from: Ipv4Addr, username: &str, count: usize) {
  let name = format!("username=\"{username}\"");
  let address = format!("remote={from}:");
  let reached = tokio::time::timeout(PATIENCE, async {
    while logs
      .lines_with(&["inbound connection identified", &address, &name])
      .len()
      < count
    {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await;
  assert!(reached.is_ok(), "{count} from {from} as {username}");
}

/// Whether the next lookup or privileges check the server reads is the privileges check this
/// writes, so no lookup was asked for before it.
async fn asked_nothing_more(session: &mut Session) -> bool {
  let marker = ServerRequest::CheckPrivileges;
  session.client.write_server_message(&marker).await.unwrap();
  session
    .server
    .request_matching(|request| match request {
      ServerRequest::GetPeerAddress(_) => Some(false),
      ServerRequest::CheckPrivileges => Some(true),
      _ => None,
    })
    .await
}

#[tokio::test]
async fn connections_waiting_for_the_servers_word_count_against_their_address() {
  let logs = Logs::capture();
  let crowded = Ipv4Addr::new(127, 0, 0, 3);
  let mut session = session().await;
  let known = FakePeer::start().await;
  let _bob = dialed_bob(&mut session, &known).await;
  // eight claim bob from an address the cached one is not, and wait for a lookup never answered
  let mut waiting = Vec::new();
  for _ in 0..8 {
    waiting.push(introduced_from(&session, crowded, "bob").await);
  }
  identified(&logs, crowded, "bob", 8).await;
  introduced_from(&session, crowded, "carol")
    .await
    .closed()
    .await;
}

#[tokio::test]
async fn connections_waiting_for_the_servers_word_count_as_unidentified() {
  let logs = Logs::capture();
  let mut session = session().await;
  let known = FakePeer::start().await;
  let _bob = dialed_bob(&mut session, &known).await;
  // 128 claim bob, eight from each of sixteen addresses, and wait for a lookup never answered
  let mut waiting = Vec::new();
  for host in 1..=16 {
    let from = Ipv4Addr::new(127, 0, 1, host);
    for _ in 0..8 {
      waiting.push(introduced_from(&session, from, "bob").await);
    }
    identified(&logs, from, "bob", 8).await;
  }
  PeerSide::connect_from(Ipv4Addr::new(127, 0, 1, 17), session.listen_address())
    .await
    .closed()
    .await;
}

#[tokio::test]
async fn a_host_claiming_an_open_user_from_elsewhere_again_costs_no_second_lookup() {
  let mut session = session().await;
  let known = FakePeer::start().await;
  let _bob = dialed_bob(&mut session, &known).await;
  let mut first = introduced_from(&session, ELSEWHERE, "bob").await;
  session.give_address("bob", known.address()).await;
  answered(&mut first).await;
  let mut again = introduced_from(&session, ELSEWHERE, "bob").await;
  answered(&mut again).await;
  assert!(asked_nothing_more(&mut session).await);
  // what the client writes to bob still goes to the connection it dialed
  assert_eq!(
    session.client.message_connection_address("bob"),
    Some(known.address())
  );
}

#[tokio::test]
async fn a_host_claiming_an_open_user_the_server_says_is_offline_costs_one_lookup() {
  let mut session = session().await;
  let known = FakePeer::start().await;
  let _bob = dialed_bob(&mut session, &known).await;
  let mut first = introduced_from(&session, ELSEWHERE, "bob").await;
  session
    .give_address("bob", SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
    .await;
  answered(&mut first).await;
  for _ in 0..3 {
    let mut again = introduced_from(&session, ELSEWHERE, "bob").await;
    answered(&mut again).await;
  }
  assert!(asked_nothing_more(&mut session).await);
  // what the client writes to bob still goes to the connection it dialed
  assert_eq!(
    session.client.message_connection_address("bob"),
    Some(known.address())
  );
}

#[tokio::test]
async fn connections_kept_beside_never_push_out_the_users_own_replaced_ones() {
  let mut session = session().await;
  let known = FakePeer::start().await;
  let mut dialed = dialed_bob(&mut session, &known).await;
  assert!(matches!(dialed.read_init().await, InitMessage::PeerInit(_)));
  // bob reconnects from where the server says he is, so the one the client dialed is kept as
  // replaced
  let mut reconnected = session.peer("bob").await;
  answered(&mut reconnected).await;
  // a host elsewhere claims bob three times; asked once, the server gives bob's address
  let mut impostors = vec![introduced_from(&session, ELSEWHERE, "bob").await];
  session.give_address("bob", known.address()).await;
  answered(&mut impostors[0]).await;
  for _ in 0..2 {
    let mut next = introduced_from(&session, ELSEWHERE, "bob").await;
    answered(&mut next).await;
    impostors.push(next);
  }
  // the oldest of them makes room, and bob's own replaced connection is still answered on
  impostors[0].closed().await;
  answered(&mut dialed).await;
}

#[tokio::test]
async fn messages_past_the_handler_bound_wait_instead_of_being_dropped() {
  // user info answered only once the test lets it, so every handler stays busy
  struct Held {
    release: Arc<tokio::sync::Semaphore>,
    asked: Arc<AtomicUsize>,
  }
  impl Handler for Held {
    async fn user_info(&self, _peer: &Peer) -> Option<UserInfo> {
      self.asked.fetch_add(1, Ordering::SeqCst);
      self.release.acquire().await.unwrap().forget();
      Some(UserInfo {
        description: String::new(),
        picture: None,
        upload_slots: 1,
        queue_length: 0,
        has_free_upload_slot: true,
        upload_permitted: None,
      })
    }
  }
  let release = Arc::new(tokio::sync::Semaphore::new(0));
  let asked = Arc::new(AtomicUsize::new(0));
  let options = listening_options()
    // every answer is queued at once when the handlers are released
    .peer_connection(|connection| connection.write_queue_size(1000))
    .handler(Held {
      release: release.clone(),
      asked: asked.clone(),
    });
  let session = Session::with_options(options).await;
  let mut bob = session.peer("bob").await;
  let requests = 300;
  let frames: Vec<u8> = (0..requests)
    .flat_map(|_| PeerMessage::InfoRequest.encode())
    .collect();
  bob.send(&frames).await;
  // 16 messages, the most one connection may have handled, are handled at once
  tokio::time::timeout(PATIENCE, async {
    while asked.load(Ordering::SeqCst) < 16 {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
  tokio::time::sleep(Duration::from_millis(100)).await;
  assert_eq!(asked.load(Ordering::SeqCst), 16);
  release.add_permits(requests);
  for _ in 0..requests {
    assert!(matches!(
      bob.read_peer().await,
      PeerMessage::InfoResponse(_)
    ));
  }
}

#[tokio::test]
async fn peers_reading_their_replies_at_a_trickle_leave_other_peers_requests_handled() {
  let asked = Arc::new(AtomicUsize::new(0));
  // a listing far longer than the sockets hold, so its write waits for the peer to read
  let options = listening_options().handler(EndlessListing(asked.clone()));
  let session = Session::with_options(options).await;
  // 16 peers with 16 requests each have 256 messages handled at once, which is the bound
  let cancel = CancellationToken::new();
  for n in 0..16 {
    let peer = FakePeer::start().await;
    let name = format!("slow{n}");
    let dialed = session
      .client
      .get_or_add_message_connection(&name, Some(peer.address()), &cancel);
    let accepted = async {
      let mut side = peer.accept().await;
      side.read_init().await;
      side
    };
    let (dialed, mut side) = tokio::join!(dialed, accepted);
    dialed.unwrap();
    side
      .send(&PeerMessage::BrowseRequest.encode().repeat(16))
      .await;
    tokio::spawn(async move {
      // a byte now and then keeps the connection from counting as idle
      while side.stream.read_exact(&mut [0]).await.is_ok() {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
    });
  }
  tokio::time::timeout(PATIENCE, async {
    while asked.load(Ordering::SeqCst) < 256 {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("every request of the slow peers is being answered");
  let mut carol = session.peer("carol").await;
  carol.send(&PeerMessage::InfoRequest.encode()).await;
  let answered = tokio::time::timeout(Duration::from_secs(2), carol.read_peer()).await;
  assert!(
    answered.is_ok(),
    "carol's request waits behind the slow readers"
  );
}

#[tokio::test]
async fn a_peer_that_does_not_read_has_no_more_answers_pending_than_its_connection_handles() {
  let asked = Arc::new(AtomicUsize::new(0));
  let options = listening_options()
    // room for every answer in the write queue, so only the handler bound can hold them back
    .peer_connection(|connection| connection.write_queue_size(1000))
    .handler(EndlessListing(asked.clone()));
  let session = Session::with_options(options).await;
  let mut bob = session.peer("bob").await;
  bob
    .send(&PeerMessage::BrowseRequest.encode().repeat(40))
    .await;
  // 16, the most one connection may have handled, are answered and wait to be written
  tokio::time::timeout(PATIENCE, async {
    while asked.load(Ordering::SeqCst) < 16 {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
  tokio::time::sleep(Duration::from_millis(100)).await;
  assert_eq!(asked.load(Ordering::SeqCst), 16);
}

#[tokio::test]
async fn queue_requests_on_one_connection_reach_the_handler_in_the_order_sent() {
  struct SlowFirst(mpsc::UnboundedSender<RemoteFilename>);
  impl Handler for SlowFirst {
    async fn upload(&self, _peer: &Peer, request: &UploadRequest) -> Upload {
      // what the application looks up before it queues takes longer for the first file
      let lookup = if request.filename.as_str().ends_with("01.flac") {
        50
      } else {
        1
      };
      tokio::time::sleep(Duration::from_millis(lookup)).await;
      // the test may have stopped listening
      let _ = self.0.send(request.filename.clone());
      Upload::Queue
    }
  }
  let (queued, mut order) = mpsc::unbounded_channel();
  let session = Session::with_options(listening_options().handler(SlowFirst(queued))).await;
  let mut bob = session.peer("bob").await;
  let album: Vec<RemoteFilename> = (1..=20)
    .map(|track| format!("@@music\\album\\{track:02}.flac").into())
    .collect();
  let frames: Vec<u8> = album
    .iter()
    .flat_map(|filename| {
      PeerMessage::QueueDownload(QueueDownload {
        filename: filename.clone(),
      })
      .encode()
    })
    .collect();
  bob.send(&frames).await;
  let mut reached = Vec::new();
  for _ in &album {
    let filename = tokio::time::timeout(PATIENCE, order.recv())
      .await
      .expect("every request reaches the handler");
    reached.extend(filename);
  }
  assert_eq!(reached, album);
}

#[tokio::test]
async fn a_search_takes_one_response_per_user_and_only_under_its_connections_name() {
  let session = session().await;
  let mut bob = bob(&session).await;
  let reserved = session.client.reserve_search_token();
  let token = reserved.token();
  let responses = session
    .client
    .search("daft punk")
    .timeout(Duration::from_millis(500))
    .response_limit(2)
    .token(reserved)
    .await
    .unwrap();
  let answer = |username: &str| {
    PeerMessage::SearchResponse(SearchResponse {
      username: username.into(),
      token,
      files: vec![File {
        code: 1,
        filename: "@@music\\around the world.mp3".into(),
        size: 4_000_000,
        extension: "mp3".into(),
        attributes: Vec::new(),
      }],
      has_free_upload_slot: true,
      upload_speed: 1000,
      queue_length: 0,
      locked_files: Vec::new(),
    })
    .encode()
  };
  // counting the misnamed one or bob's second would reach the response limit
  let mut frames = answer("mallory");
  frames.extend(answer("bob"));
  frames.extend(answer("bob"));
  bob.send(&frames).await;
  let (search, responses) = tokio::time::timeout(PATIENCE, responses.all())
    .await
    .unwrap();
  assert_eq!(search.state, State::Completed(Outcome::TimedOut));
  assert_eq!(responses.len(), 1, "{responses:?}");
  assert_eq!(responses[0].username, "bob");
}
