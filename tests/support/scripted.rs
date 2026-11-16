//! A client on the scripted server whose transfers go to a scripted peer, for the transfer paths
//! two real clients never take: refusals with every reason, failures, peers that lie or vanish
//! at each stage, and canceling at each stage.

use soulseek_client::config::ConfigBuilder;
use soulseek_client::transfer::{Download, Handle, Transfer};
use std::path::PathBuf;

use soulseek_client::{Client, Error};
use soulseek_proto::init::{FileTransferInit, PeerInit};
use soulseek_proto::peer::{QueueDownload, TransferRequest, TransferResponse};
use soulseek_proto::{InitMessage, PeerConnectionType, PeerMessage};
use tokio::task::JoinHandle;

use super::peer::{FakePeer, PeerSide};
use super::transfers::{content, once_queued, scratch};
use super::{PATIENCE, Session, USERNAME, listening_options};

/// the scripted peer's name
pub const BOB: &str = "bob";

/// A listening client on the scripted server and bob, a scripted peer the server gives out the
/// address of
pub struct Scripted {
  pub session: Session,
  pub bob: FakePeer,
}

impl Scripted {
  pub async fn start() -> Self {
    Self::with_options(listening_options()).await
  }

  pub async fn with_options(options: ConfigBuilder) -> Self {
    Self {
      session: Session::with_options(options).await,
      bob: FakePeer::start().await,
    }
  }

  pub fn client(&self) -> &Client {
    &self.session.client
  }

  /// answers the client's next lookup of bob's address with the scripted peer's
  pub async fn give_address(&mut self) {
    self.session.give_address(BOB, self.bob.address()).await;
  }

  /// gives the client bob's address and returns the message connection it then opens to bob
  pub async fn connected(&mut self) -> PeerSide {
    self.give_address().await;
    self.accept_message().await
  }

  /// the next message connection the client opens to bob, past its introduction
  pub async fn accept_message(&self) -> PeerSide {
    let mut side = self.bob.accept().await;
    let InitMessage::PeerInit(PeerInit {
      username,
      connection_type,
      ..
    }) = side.read_init().await
    else {
      panic!("the client introduces itself");
    };
    assert_eq!(
      (username.as_str(), connection_type),
      (USERNAME, PeerConnectionType::Peer)
    );
    side
  }

  /// the next transfer connection the client opens to bob, and the token it names
  pub async fn accept_transfer(&self) -> (PeerSide, u32) {
    let mut side = self.bob.accept().await;
    let InitMessage::PeerInit(PeerInit {
      connection_type, ..
    }) = side.read_init().await
    else {
      panic!("the client introduces itself");
    };
    assert_eq!(connection_type, PeerConnectionType::Transfer);
    let token = FileTransferInit::decode(&side.raw(4).await).unwrap().token;
    (side, token)
  }

  /// bob opens a transfer connection to the client for the transfer the client named `token`
  pub async fn connect_for_transfer(&self, token: u32) -> PeerSide {
    let mut side = PeerSide::connect(self.session.listen_address()).await;
    let mut init = InitMessage::PeerInit(PeerInit {
      username: BOB.into(),
      connection_type: PeerConnectionType::Transfer,
      token: 0,
    })
    .encode();
    init.extend(FileTransferInit { token }.encode());
    side.send(&init).await;
    side
  }
}

/// the size of the files bob sends
pub const SIZE: u64 = 50_000;

/// A download from bob that bob may not have the request of yet
pub struct Starting {
  /// ends with the download's handle once the download is tracked
  handle: JoinHandle<Result<Handle, Error>>,
  pub destination: PathBuf,
}

impl Starting {
  /// the download as soon as it is tracked, whatever bob has of it
  pub async fn started(self) -> Downloading {
    let handle = self.handle.await.unwrap().expect("the download starts");
    Downloading {
      handle,
      destination: self.destination,
    }
  }

  /// the download once bob has the request
  pub async fn queued(self) -> Downloading {
    let started = self.handle.await.unwrap();
    let handle = tokio::time::timeout(PATIENCE, once_queued(started))
      .await
      .expect("bob has the request")
      .expect("the download is queued");
    Downloading {
      handle,
      destination: self.destination,
    }
  }

  pub async fn result(self) -> Result<Transfer, Error> {
    tokio::time::timeout(PATIENCE, async { self.handle.await.unwrap()?.await })
      .await
      .expect("the download ends")
  }
}

/// A download from bob that bob has the request of
pub struct Downloading {
  pub handle: Handle,
  pub destination: PathBuf,
}

impl Downloading {
  pub async fn result(self) -> Result<Transfer, Error> {
    tokio::time::timeout(PATIENCE, self.handle)
      .await
      .expect("the download ends")
  }
}

impl Scripted {
  /// starts downloading `filename`, of [`SIZE`] bytes unless `shape` says otherwise, from bob
  pub fn download(
    &self,
    filename: &str,
    shape: impl FnOnce(Download) -> Download + Send + 'static,
  ) -> Starting {
    let destination = scratch().join("download");
    let client = self.client().clone();
    let filename = filename.to_owned();
    let handle = tokio::spawn({
      let destination = destination.clone();
      async move {
        let download = client
          .download(BOB, filename.as_str())
          .size(SIZE)
          .to_file(destination);
        shape(download).start().await
      }
    });
    Starting {
      handle,
      destination,
    }
  }

  /// a download from bob queued at bob, with bob's side of the message connection
  pub async fn queued_download(&mut self, filename: &str) -> (Downloading, PeerSide) {
    let starting = self.download(filename, |download| download);
    let mut bob = self.connected().await;
    queued(&mut bob, filename).await;
    (starting.queued().await, bob)
  }
}

/// bob's side reads the client's `QueueDownload` for `filename`
pub async fn queued(bob: &mut PeerSide, filename: &str) {
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::QueueDownload(QueueDownload {
      filename: filename.into(),
    })
  );
}

/// bob offers to start sending `filename` with token `token` and reads the client's acceptance
pub async fn offer(bob: &mut PeerSide, filename: &str, token: u32, size: Option<u64>) {
  bob
    .send(
      &PeerMessage::TransferRequest(TransferRequest::Upload {
        token,
        filename: filename.into(),
        file_size: size,
      })
      .encode(),
    )
    .await;
  assert_eq!(
    bob.read_peer().await,
    PeerMessage::TransferResponse(TransferResponse::Allowed {
      token,
      file_size: None,
    })
  );
}

/// bob reads the start offset the client asks for, checks it is `start`, and sends `len` bytes
/// of the file from there on the transfer connection
pub async fn send_file(transfer: &mut PeerSide, start: u64, len: usize) {
  let offset = u64::from_le_bytes(transfer.raw(8).await.try_into().unwrap());
  assert_eq!(offset, start);
  let file = content(SIZE as usize);
  transfer
    .send(&file[start as usize..start as usize + len])
    .await;
}
