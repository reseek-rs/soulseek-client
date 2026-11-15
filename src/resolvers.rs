//! The callbacks the application supplies so the client can answer peers: share listings,
//! folder contents, user info, search results, upload queueing and files users offer unasked.
//! They are part of the client options; the peer dispatcher and the search responder call them.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use soulseek_proto::peer::{BrowseResponse, Directory, SearchResponse, UserInfo};
use soulseek_proto::{PeerConnectionType, RemoteFilename};

use crate::conn::EncodedFrame;
use crate::error::ClientError;
use crate::events::DistributedParent;

/// Boxed future of the answer a resolver returns.
pub type ResolverFuture<T> = Pin<Box<dyn Future<Output = Result<T, ClientError>> + Send>>;

/// Peer a request came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
  /// Name the peer gave.
  pub username: String,
  /// Address its connection comes from.
  pub endpoint: SocketAddr,
  /// Whether the name is the user's.
  ///
  /// It is when the server gives this address for the user, or the connection answers a request
  /// of the client's. Otherwise the name is only what the peer said.
  pub confirmed: bool,
}

/// Resolver for the answer to a search a user sent the client.
///
/// Called with `(username, token, query)`. Returns the client's answer, or `None` when the
/// client has nothing that matches. The response names the client, in `username`, and the
/// search, with the `token` the resolver is given. A search of the client's own reaches the
/// resolver like any other, under the client's own username, and its answer goes to that search.
/// `query` is the text as the searcher sent it.
/// [`SearchQuery::parse`](soulseek_proto::SearchQuery::parse) splits it into terms and
/// exclusions.
pub type SearchResponseResolver =
  Arc<dyn Fn(String, u32, String) -> ResolverFuture<Option<SearchReply>> + Send + Sync>;

/// Resolver for the share listing sent to a peer that browses the client.
///
/// Called with `(peer)`. Returns the client's share listing, or `None` to send no reply.
pub type BrowseResponseResolver =
  Arc<dyn Fn(Peer) -> ResolverFuture<Option<BrowseReply>> + Send + Sync>;

/// Resolver for the contents of a folder a peer asks for.
///
/// Called with `(peer, token, folder)`. Returns that folder and every folder below it, or `None`
/// to send no reply.
pub type FolderContentsResolver =
  Arc<dyn Fn(Peer, u32, RemoteFilename) -> ResolverFuture<Option<Vec<Directory>>> + Send + Sync>;

/// Resolver for the user info the client shows a peer.
///
/// Called with `(peer)`. Returns the user info shown to that peer, or `None` to send no reply.
/// The info is sent as the application made it.
pub type UserInfoResolver = Arc<dyn Fn(Peer) -> ResolverFuture<Option<UserInfo>> + Send + Sync>;

/// Request of a peer to upload it a file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadAsk {
  /// File as the peer names it.
  pub filename: RemoteFilename,
  /// Token of a request made with the legacy transfer request.
  ///
  /// The peer expects a later offer to answer such a request. `None` for a request to queue the
  /// file. Either is answered as queued.
  pub legacy_token: Option<u32>,
}

/// What the application does with a peer's request to upload it a file.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UploadDecision {
  /// The application took the request.
  ///
  /// It sends the file when it chooses with [`Client::upload`](crate::Client::upload), which runs
  /// the upload on a task of its own.
  Queue,
  /// Refused, with the reason the peer is told as written.
  Deny(String),
  /// Not now.
  ///
  /// The peer is told the upload failed, which is its cue to ask again.
  Later,
}

/// Resolver for a peer's request that the client upload it a file.
///
/// Called with `(peer, ask)`. An error is logged and the peer is told the upload failed, as for
/// [`UploadDecision::Later`].
pub type UploadRequestResolver =
  Arc<dyn Fn(Peer, UploadAsk) -> ResolverFuture<UploadDecision> + Send + Sync>;

/// Resolver for a file a user offers to send that the client did not ask for.
///
/// Called with `(peer, filename, size)`. The client holds the offer while the resolver runs.
/// Succeeding means the application started a download of the file with
/// [`RequestStyle::Offered`](crate::RequestStyle::Offered), which takes the offer. Failing
/// refuses it. The reason of a [`ClientError::TransferRejected`] is what the sender is told. Any
/// other error is logged and the sender is told "Cancelled".
pub type UnaskedOfferResolver =
  Arc<dyn Fn(Peer, RemoteFilename, u64) -> ResolverFuture<()> + Send + Sync>;

/// Resolver for the place of a file in the client's upload queue.
///
/// Called with `(peer, filename)`. Returns the 1-based place of that file in the queue, or
/// `None` to send no reply.
pub type PlaceInQueueResolver =
  Arc<dyn Fn(Peer, RemoteFilename) -> ResolverFuture<Option<u32>> + Send + Sync>;

/// Resolver for a connection request the server relays.
///
/// Called with `(peer, connection_type)`. The server relays a user's request that the client
/// connect to them, and names the user and the address, so the peer is confirmed. Returning
/// `false` refuses: the server is told the client cannot connect and nothing is dialed. A user
/// who cannot reach the client's listener depends on these requests, for the transfers of files
/// the client asked them for as well.
pub type ConnectionRequestResolver = Arc<dyn Fn(&Peer, PeerConnectionType) -> bool + Send + Sync>;

/// Resolver that picks the client's distributed parent.
///
/// Called with `(candidates)`. Returns the index of the one to adopt as the client's distributed
/// parent, or `None` to adopt none of them. The candidates are those of the server's offer that
/// answered as a parent must, nearest the top of its branch first. The resolver runs on the
/// tree's own task, so it must not wait.
pub type DistributedParentResolver =
  Arc<dyn Fn(&[DistributedParent]) -> Option<usize> + Send + Sync>;

/// Resolver that decides whether a peer may become a distributed child.
///
/// Called with `(peer)`. Returns whether the peer may join the distributed search tree below
/// the client. It is asked while a slot is free. The resolver runs on the tree's own task, so it
/// must not wait.
pub type DistributedChildResolver = Arc<dyn Fn(&Peer) -> bool + Send + Sync>;

/// Answer of the client to another user's search.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SearchReply {
  /// Files that matched, which the client encodes.
  Response(SearchResponse),
  /// Complete [`SearchResponse`] frame the application encoded itself, sent as it is.
  Encoded(Vec<u8>),
  /// Answer meant for the searcher at the address `to` only.
  ///
  /// On a connection from any other address it is not sent, since a peer can give any name when
  /// it connects.
  Confined {
    /// Answer itself.
    reply: Box<SearchReply>,
    /// Only address the answer may go to.
    to: IpAddr,
  },
}

impl SearchReply {
  /// Returns the number of files the reply offers, locked ones included.
  ///
  /// Returns `None` for an encoded reply, whose count is unknown.
  pub fn file_count(&self) -> Option<usize> {
    match self {
      Self::Response(response) => Some(response.files.len() + response.locked_files.len()),
      Self::Encoded(_) => None,
      Self::Confined { reply, .. } => reply.file_count(),
    }
  }

  /// the answer without the address it is confined to: a `Response` or an `Encoded`
  pub(crate) fn plain(&self) -> &SearchReply {
    match self {
      Self::Confined { reply, .. } => reply.plain(),
      other => other,
    }
  }

  /// whether the answer may go out on a connection from `remote`; an IPv4 address and the same
  /// address mapped into IPv6, as a dual-stack listener sees it, are one address
  pub(crate) fn may_reach(&self, remote: IpAddr) -> bool {
    match self {
      Self::Confined { reply, to } => {
        to.to_canonical() == remote.to_canonical() && reply.may_reach(remote)
      }
      _ => true,
    }
  }

  /// the complete frame to write
  pub(crate) fn frame(&self) -> Vec<u8> {
    match self {
      Self::Response(response) => {
        soulseek_proto::PeerMessage::SearchResponse(response.clone()).encode()
      }
      Self::Encoded(frame) => frame.clone(),
      Self::Confined { reply, .. } => reply.frame(),
    }
  }
}

/// Share listing of the client for a peer that browses it.
#[derive(Debug)]
#[non_exhaustive]
pub enum BrowseReply {
  /// Listing, which the client encodes.
  Response(BrowseResponse),
  /// Complete [`BrowseResponse`] frame the application encoded ahead of time.
  ///
  /// The frame is streamed as it is.
  Encoded(EncodedFrame),
}

/// Every callback the application can supply.
///
/// Those that answer a peer return a future and take what they are asked about by value. Those
/// the client decides something with on the spot are plain functions that borrow it. An unset
/// resolver falls back to the behavior each one documents.
#[derive(Clone, Default)]
pub struct Resolvers {
  /// Resolver for answers to searches.
  ///
  /// If unset, searches are never answered. Unset by default.
  pub search_response: Option<SearchResponseResolver>,
  /// Resolver for share listings.
  ///
  /// If unset, browse requests get no reply. Unset by default.
  pub browse_response: Option<BrowseResponseResolver>,
  /// Resolver for folder contents.
  ///
  /// If unset, folder requests get no reply. Unset by default.
  pub folder_contents: Option<FolderContentsResolver>,
  /// Resolver for user info.
  ///
  /// If unset, user info requests get no reply. Unset by default.
  pub user_info: Option<UserInfoResolver>,
  /// Resolver for upload requests.
  ///
  /// If unset, every request is refused with "File not shared." Unset by default.
  pub upload_request: Option<UploadRequestResolver>,
  /// Resolver for places in the upload queue.
  ///
  /// If unset, place requests get no reply. Unset by default.
  pub place_in_queue: Option<PlaceInQueueResolver>,
  /// Resolver for files offered unasked.
  ///
  /// If unset, every file a user offers unasked is refused. Unset by default.
  pub unasked_offer: Option<UnaskedOfferResolver>,
  /// Resolver for relayed connection requests.
  ///
  /// If unset, every connection request the server relays is dialed. Unset by default.
  pub connection_request: Option<ConnectionRequestResolver>,
  /// Resolver that picks the distributed parent.
  ///
  /// If unset, the candidate nearest the top of its branch becomes the client's distributed
  /// parent. Unset by default.
  pub distributed_parent: Option<DistributedParentResolver>,
  /// Resolver that admits distributed children.
  ///
  /// If unset, every peer may become a distributed child while a slot is free. Unset by default.
  pub distributed_child: Option<DistributedChildResolver>,
}

/// shows which callbacks are set; the callbacks themselves have nothing to print
impl fmt::Debug for Resolvers {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Resolvers")
      .field("search_response", &self.search_response.is_some())
      .field("browse_response", &self.browse_response.is_some())
      .field("folder_contents", &self.folder_contents.is_some())
      .field("user_info", &self.user_info.is_some())
      .field("upload_request", &self.upload_request.is_some())
      .field("place_in_queue", &self.place_in_queue.is_some())
      .field("unasked_offer", &self.unasked_offer.is_some())
      .field("connection_request", &self.connection_request.is_some())
      .field("distributed_parent", &self.distributed_parent.is_some())
      .field("distributed_child", &self.distributed_child.is_some())
      .finish()
  }
}
