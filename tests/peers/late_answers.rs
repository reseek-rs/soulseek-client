//! A scripted peer answering, late, the connection request the server relayed for the client:
//! dialing it directly already won, but the peer connects to the client anyway with the
//! request's token. For messages, the answering connection replaces the one the client dialed,
//! which takes over again if the peer closes the answering one; for a transfer it is refused,
//! since the transfer runs on the connection that won; after the time the request was awaited
//! for, it is unknown.

use std::time::Duration;

use crate::support::peer::PeerSide;
use crate::support::scripted::{BOB, SIZE, Scripted, offer, queued, send_file};
use crate::support::{Logs, PATIENCE, listening_options};
use soulseek_client::{Config, RequestStyle};
use soulseek_proto::init::PierceFirewall;
use soulseek_proto::peer::TransferResponse;
use soulseek_proto::server::ConnectToPeerRequest;
use soulseek_proto::{InitMessage, PeerConnectionType, PeerMessage, ServerRequest};

/// the token of the next request the client has the server relay to bob for `connection_type`
async fn relayed(scripted: &mut Scripted, connection_type: PeerConnectionType) -> u32 {
  scripted
    .session
    .server
    .request_matching(|request| match request {
      ServerRequest::ConnectToPeer(ConnectToPeerRequest {
        token,
        username,
        connection_type: asked,
      }) if username == BOB && asked == connection_type => Some(token),
      _ => None,
    })
    .await
}

/// bob connects to the client and answers the relayed request `token`
async fn answer(scripted: &Scripted, token: u32) -> PeerSide {
  let mut side = PeerSide::connect(scripted.session.listen_address()).await;
  side
    .send(&InitMessage::PierceFirewall(PierceFirewall { token }).encode())
    .await;
  side
}

/// waits until the client's message connection to bob is the one bob holds as `side`
async fn current(scripted: &Scripted, side: &PeerSide) {
  let local = side.stream.local_addr().unwrap();
  tokio::time::timeout(PATIENCE, async {
    while scripted.client().message_connections() != [(BOB.to_owned(), local)] {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("the connection becomes the current one");
}

#[tokio::test]
async fn a_late_answer_replaces_the_dialed_connection_and_carries_the_offer() {
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\late.mp3", |_| {});
  let mut dialed = scripted.connected().await;
  queued(&mut dialed, "@@share\\late.mp3").await;
  // bob acts on the relayed request after all, counts that connection as its connection to the
  // client and drops the one the client dialed
  let token = relayed(&mut scripted, PeerConnectionType::Peer).await;
  let mut answered = answer(&scripted, token).await;
  current(&scripted, &answered).await;
  drop(dialed);
  offer(&mut answered, "@@share\\late.mp3", 77, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(77).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  transfer.closed().await;
  let done = running.result().await.unwrap();
  assert_eq!(done.remote_token, Some(77));
  current(&scripted, &answered).await;
}

#[tokio::test]
async fn a_late_answer_its_peer_closes_leaves_the_dialed_connection_in_use() {
  let mut scripted = Scripted::start().await;
  let running = scripted.download("@@share\\kept.mp3", |_| {});
  let mut dialed = scripted.connected().await;
  queued(&mut dialed, "@@share\\kept.mp3").await;
  let token = relayed(&mut scripted, PeerConnectionType::Peer).await;
  let answered = answer(&scripted, token).await;
  current(&scripted, &answered).await;
  // this bob keeps the connection the client dialed and closes the one it answered with; the
  // download still waiting for its offer is not failed for it
  drop(answered);
  current(&scripted, &dialed).await;
  offer(&mut dialed, "@@share\\kept.mp3", 79, Some(SIZE)).await;
  let mut transfer = scripted.connect_for_transfer(79).await;
  send_file(&mut transfer, 0, SIZE as usize).await;
  transfer.closed().await;
  let done = running.result().await.unwrap();
  assert_eq!(done.remote_token, Some(79));
}

#[tokio::test]
async fn a_request_answered_late_is_not_answered_again() {
  let logs = Logs::capture();
  let mut scripted = Scripted::start().await;
  let _running = scripted.download("@@share\\late.mp3", |_| {});
  let mut dialed = scripted.connected().await;
  queued(&mut dialed, "@@share\\late.mp3").await;
  let token = relayed(&mut scripted, PeerConnectionType::Peer).await;
  let answered = answer(&scripted, token).await;
  current(&scripted, &answered).await;
  // someone else who saw the token connects with it too
  let mut again = answer(&scripted, token).await;
  again.closed().await;
  logs
    .wait_for(&[&format!(
      "unknown PierceFirewall attempt with token {token}"
    )])
    .await;
  current(&scripted, &answered).await;
}

#[tokio::test]
async fn an_answer_after_the_request_was_awaited_for_is_refused() {
  let logs = Logs::capture();
  let mut options: Config = listening_options();
  options =
    options.peer_connection(|connection| connection.connect_timeout(Duration::from_millis(300)));
  let mut scripted = Scripted::with_options(options).await;
  let _running = scripted.download("@@share\\late.mp3", |_| {});
  let mut dialed = scripted.connected().await;
  queued(&mut dialed, "@@share\\late.mp3").await;
  let token = relayed(&mut scripted, PeerConnectionType::Peer).await;
  // the request would have been given up by now even if dialing had failed
  tokio::time::sleep(Duration::from_millis(400)).await;
  let mut answered = answer(&scripted, token).await;
  answered.closed().await;
  logs
    .wait_for(&[
      "connection closed",
      &format!("unknown PierceFirewall attempt with token {token}"),
    ])
    .await;
  current(&scripted, &dialed).await;
}

#[tokio::test]
async fn a_late_answer_to_a_transfer_request_is_refused() {
  let logs = Logs::capture();
  let mut scripted = Scripted::start().await;
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
        token: asked.token(),
        file_size: Some(SIZE),
      })
      .encode(),
    )
    .await;
  let (mut transfer, _) = scripted.accept_transfer().await;
  let token = relayed(&mut scripted, PeerConnectionType::Transfer).await;
  let mut answered = answer(&scripted, token).await;
  answered.closed().await;
  logs
    .wait_for(&[
      "connection closed",
      "transfer connection request already answered",
    ])
    .await;
  // the transfer runs on the connection that won, and the message connection stays as it was
  send_file(&mut transfer, 0, SIZE as usize).await;
  transfer.closed().await;
  running.result().await.unwrap();
  current(&scripted, &bob).await;
}
