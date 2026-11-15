//! A handler made of closures, one per question, for tests that answer one or two of them. A
//! question with no closure gets the handler's default answer.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use soulseek_client::DistributedParent;
use soulseek_client::handler::{
  BrowseReply, Handler, Offer, Peer, Search, SearchReply, Upload, UploadRequest,
};
use soulseek_proto::peer::{Directory, UserInfo};
use soulseek_proto::{PeerConnectionType, RemoteFilename};

pub type Answer<T> = Pin<Box<dyn Future<Output = T> + Send>>;
/// answers a question about one thing
pub type Asked<A, T> = Arc<dyn Fn(A) -> Answer<T> + Send + Sync>;
/// answers a question about two things
pub type Asked2<A, B, T> = Arc<dyn Fn(A, B) -> Answer<T> + Send + Sync>;
/// answers a search, given the searcher, the token and the query
pub type SearchResolver =
  Arc<dyn Fn(String, u32, String) -> Answer<Option<SearchReply>> + Send + Sync>;
/// decides a file offered unasked, given who offers it, its name and its size
pub type OfferResolver = Arc<dyn Fn(Peer, RemoteFilename, u64) -> Answer<Offer> + Send + Sync>;

/// decides a connection request the server relays
pub type ConnectionDecision = Arc<dyn Fn(&Peer, PeerConnectionType) -> bool + Send + Sync>;
/// picks a distributed parent among the candidates
pub type ParentChoice = Arc<dyn Fn(&[DistributedParent]) -> Option<usize> + Send + Sync>;
/// decides whether a peer may become a distributed child
pub type ChildDecision = Arc<dyn Fn(&Peer) -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub struct Resolvers {
  pub search_response: Option<SearchResolver>,
  pub browse_response: Option<Asked<Peer, Option<BrowseReply>>>,
  pub folder_contents: Option<Asked2<Peer, RemoteFilename, Option<Vec<Directory>>>>,
  pub user_info: Option<Asked<Peer, Option<UserInfo>>>,
  pub upload_request: Option<Asked2<Peer, UploadRequest, Upload>>,
  pub place_in_queue: Option<Asked2<Peer, RemoteFilename, Option<u32>>>,
  pub unasked_offer: Option<OfferResolver>,
  pub connection_request: Option<ConnectionDecision>,
  pub distributed_parent: Option<ParentChoice>,
  pub distributed_child: Option<ChildDecision>,
}

/// the handler's default answers
struct Defaults;

impl Handler for Defaults {}

impl Handler for Resolvers {
  async fn search(&self, search: &Search) -> Option<SearchReply> {
    match &self.search_response {
      Some(answer) => answer(search.username.clone(), search.token, search.query.clone()).await,
      None => Defaults.search(search).await,
    }
  }

  async fn browse(&self, peer: &Peer) -> Option<BrowseReply> {
    match &self.browse_response {
      Some(answer) => answer(peer.clone()).await,
      None => Defaults.browse(peer).await,
    }
  }

  async fn folder(&self, peer: &Peer, directory: &RemoteFilename) -> Option<Vec<Directory>> {
    match &self.folder_contents {
      Some(answer) => answer(peer.clone(), directory.clone()).await,
      None => Defaults.folder(peer, directory).await,
    }
  }

  async fn user_info(&self, peer: &Peer) -> Option<UserInfo> {
    match &self.user_info {
      Some(answer) => answer(peer.clone()).await,
      None => Defaults.user_info(peer).await,
    }
  }

  async fn upload(&self, peer: &Peer, request: &UploadRequest) -> Upload {
    match &self.upload_request {
      Some(answer) => answer(peer.clone(), request.clone()).await,
      None => Defaults.upload(peer, request).await,
    }
  }

  async fn place_in_queue(&self, peer: &Peer, filename: &RemoteFilename) -> Option<u32> {
    match &self.place_in_queue {
      Some(answer) => answer(peer.clone(), filename.clone()).await,
      None => Defaults.place_in_queue(peer, filename).await,
    }
  }

  async fn unasked_offer(&self, peer: &Peer, filename: &RemoteFilename, size: u64) -> Offer {
    match &self.unasked_offer {
      Some(answer) => answer(peer.clone(), filename.clone(), size).await,
      None => Defaults.unasked_offer(peer, filename, size).await,
    }
  }

  fn accept_connection(&self, peer: &Peer, connection_type: PeerConnectionType) -> bool {
    match &self.connection_request {
      Some(answer) => answer(peer, connection_type),
      None => Defaults.accept_connection(peer, connection_type),
    }
  }

  fn choose_parent(&self, candidates: &[DistributedParent]) -> Option<usize> {
    match &self.distributed_parent {
      Some(answer) => answer(candidates),
      None => Defaults.choose_parent(candidates),
    }
  }

  fn accept_child(&self, peer: &Peer) -> bool {
    match &self.distributed_child {
      Some(answer) => answer(peer),
      None => Defaults.accept_child(peer),
    }
  }
}
