//! Files a user sends unasked: between two clients on soulfind, the receiver's application is
//! asked, and either starts a download that takes the held offer or refuses it; from a scripted
//! peer, an offer of a file whose offer is held is refused, and an offer nobody takes is refused
//! once its time is up.

use soulseek_client::handler::{self, Handler};
use soulseek_client::transfer::{Handle, RequestStyle};
use std::sync::Arc;

use std::time::Duration;

use crate::support::peer::PeerSide;
use crate::support::scripted::{self, BOB, Scripted, send_file};
use crate::support::soulfind::{self, User};
use crate::support::transfers::{content, file, finish, scratch};
use crate::support::{PATIENCE, listening_options};
use soulseek_client::{Error, Peer};
use soulseek_proto::peer::{TransferRequest, TransferResponse};
use soulseek_proto::{PeerMessage, RemoteFilename};
use tokio::sync::{Notify, mpsc};

const SIZE: usize = 200_000;
const REMOTE: &str = "@@share\\gift\\01 song.flac";

/// An offer the receiver's application was asked about
type Offer = (String, RemoteFilename, u64);

/// Takes every file offered unasked and hands each offer to a receiver, which is where the test
/// starts the download
struct Taking(mpsc::UnboundedSender<Offer>);

impl Handler for Taking {
  async fn unasked_offer(
    &self,
    peer: &Peer,
    filename: &RemoteFilename,
    size: u64,
  ) -> handler::Offer {
    // the test may have stopped listening; the offer is taken either way
    let _ = self.0.send((peer.username.clone(), filename.clone(), size));
    handler::Offer::Taken
  }
}

/// sends the file at `path` to `receiver` unasked, as `sender`'s upload
async fn send(sender: &User, receiver: &User, path: std::path::PathBuf) -> Handle {
  let upload = sender.client.upload(&receiver.username, REMOTE);
  upload.from_file(path).await.expect("the upload is queued")
}

#[tokio::test]
async fn a_file_sent_unasked_lands_once_the_application_takes_it() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (offered, mut offers) = mpsc::unbounded_channel();
  let options = soulfind::listening().handler(Taking(offered));
  let receiver = server.login("receiver", options).await;
  let sender = server.login("sender", soulfind::listening()).await;
  let dir = scratch();
  let upload = send(&sender, &receiver, file(&dir, "source.flac", SIZE)).await;
  let (username, filename, size) = tokio::time::timeout(PATIENCE, offers.recv())
    .await
    .expect("the receiver is asked about the file")
    .unwrap();
  assert_eq!(
    (username.as_str(), filename.as_str(), size),
    (sender.username.as_str(), REMOTE, SIZE as u64)
  );
  let destination = dir.join("received.flac");
  let taking = receiver
    .client
    .download(&username, filename)
    .size(size)
    .request_style(RequestStyle::Offered)
    .to_file(&destination);
  let received = finish(taking).await.unwrap();
  assert_eq!(received.bytes_transferred, SIZE as u64);
  assert_eq!(std::fs::read(destination).unwrap(), content(SIZE));
  upload.await.expect("the sender's upload succeeds");
}

#[tokio::test]
async fn a_file_the_application_refuses_is_refused_to_its_sender() {
  let Some(server) = soulfind::server() else {
    return;
  };
  struct Refusing;
  impl Handler for Refusing {
    async fn unasked_offer(&self, _: &Peer, _: &RemoteFilename, _: u64) -> handler::Offer {
      handler::Offer::Refused("not a buddy".into())
    }
  }
  let options = soulfind::listening().handler(Refusing);
  let receiver = server.login("receiver", options).await;
  let sender = server.login("sender", soulfind::listening()).await;
  let dir = scratch();
  let refused = send(&sender, &receiver, file(&dir, "source.flac", SIZE))
    .await
    .await
    .unwrap_err();
  assert!(
    matches!(&refused, Error::TransferRejected(reason) if reason == "not a buddy"),
    "{refused:?}"
  );
  // the refused offer is not held, so a download cannot take it afterwards
  let taking = receiver
    .client
    .download(&sender.username, REMOTE)
    .size(SIZE as u64)
    .request_style(RequestStyle::Offered)
    .to_file(dir.join("received.flac"));
  let error = finish(taking).await.unwrap_err();
  assert!(matches!(error, Error::InvalidOperation(_)), "{error:?}");
}

/// bob offers to send `filename` of `size` bytes unasked, with `token`
async fn bob_offers(bob: &mut PeerSide, filename: &str, token: u32, size: u64) {
  let offer = PeerMessage::TransferRequest(TransferRequest::Upload {
    token,
    filename: filename.into(),
    file_size: Some(size),
  });
  bob.send(&offer.encode()).await;
}

/// the client's refusal of bob's offer with `token`
fn refused(token: u32) -> PeerMessage {
  PeerMessage::TransferResponse(TransferResponse::Denied {
    token,
    message: "Cancelled".into(),
  })
}

#[tokio::test]
async fn an_offer_of_a_file_whose_offer_is_held_is_refused() {
  const FILE: &str = "@@share\\gift.flac";
  /// tells the receiver the size offered, and takes the offer once the test lets it
  struct Deciding {
    asked: mpsc::UnboundedSender<u64>,
    decided: Arc<Notify>,
  }
  impl Handler for Deciding {
    async fn unasked_offer(&self, _: &Peer, _: &RemoteFilename, size: u64) -> handler::Offer {
      // the test may have stopped listening; the offer is decided either way
      let _ = self.asked.send(size);
      self.decided.notified().await;
      handler::Offer::Taken
    }
  }
  let (asked, mut asks) = mpsc::unbounded_channel();
  let decide = Arc::new(Notify::new());
  let options = listening_options().handler(Deciding {
    asked,
    decided: decide.clone(),
  });
  let mut scripted = Scripted::with_options(options).await;
  let mut bob = scripted.session.peer(BOB).await;
  bob_offers(&mut bob, FILE, 1, scripted::SIZE).await;
  let size = tokio::time::timeout(PATIENCE, asks.recv()).await.unwrap();
  assert_eq!(size, Some(scripted::SIZE));
  // the same file again, far larger, while the first offer is being decided
  bob_offers(&mut bob, FILE, 2, scripted::SIZE * 1000).await;
  assert_eq!(bob.read_peer().await, refused(2));
  decide.notify_one();
  let running = scripted.download(FILE, |download| {
    download.request_style(RequestStyle::Offered)
  });
  // the download looks bob up, then answers on the connection bob offered on
  scripted.give_address().await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::TransferResponse(TransferResponse::Allowed {
      token: 1,
      file_size: None,
    })
  );
  let mut transfer = scripted.connect_for_transfer(1).await;
  send_file(&mut transfer, 0, scripted::SIZE as usize).await;
  let done = running.result().await.unwrap();
  assert_eq!(
    (done.remote_token, done.bytes_transferred),
    (Some(1), scripted::SIZE)
  );
  assert!(
    asks.try_recv().is_err(),
    "the second offer is never asked about"
  );
}

#[tokio::test]
async fn an_offer_nobody_takes_is_refused_once_its_time_is_up() {
  /// says it took the offer and starts no download
  struct Idle;
  impl Handler for Idle {
    async fn unasked_offer(&self, _: &Peer, _: &RemoteFilename, _: u64) -> handler::Offer {
      handler::Offer::Taken
    }
  }
  let options = listening_options()
    .unasked_offer_timeout(Duration::from_millis(200))
    .handler(Idle);
  let scripted = Scripted::with_options(options).await;
  let mut bob = scripted.session.peer(BOB).await;
  bob_offers(&mut bob, "@@share\\untaken.flac", 7, scripted::SIZE).await;
  let answer = tokio::time::timeout(PATIENCE, bob.read_peer())
    .await
    .expect("the offer is answered");
  assert_eq!(answer, refused(7));
}
