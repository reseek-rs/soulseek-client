//! The callbacks the application supplies so the client can answer peers: share listings,
//! folder contents, user info, search results, upload queueing and files users offer unasked.
//! They are part of the client options; the peer dispatcher and the search responder call them.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use soulseek_proto::RemoteFilename;
use soulseek_proto::peer::{BrowseResponse, Directory, SearchResponse, UserInfo};

use crate::conn::EncodedFrame;
use crate::error::ClientError;

/// What a resolver returns: a boxed future of its answer
pub type ResolverFuture<T> = Pin<Box<dyn Future<Output = Result<T, ClientError>> + Send>>;

/// The peer a request came from
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
  /// the name the peer gave
  pub username: String,
  /// where its connection comes from
  pub endpoint: SocketAddr,
  /// Whether the name is the user's: the server gives this address for the user, or the
  /// connection answers a request of ours. Otherwise the name is only what the peer said.
  pub confirmed: bool,
}

/// `(username, token, query)` to our answer to a search a user sent us, or `None` when we have
/// nothing that matches. A search of our own reaches the resolver like any other, under our own
/// username, and its answer goes to that search. `query` is the text as the searcher sent it;
/// [`SearchQuery::parse`](soulseek_proto::SearchQuery::parse) splits it into terms and exclusions.
pub type SearchResponseResolver =
  Arc<dyn Fn(String, u32, String) -> ResolverFuture<Option<SearchReply>> + Send + Sync>;

/// `(peer)` to our share listing for a peer that browses us, or `None` to send no reply
pub type BrowseResponseResolver =
  Arc<dyn Fn(Peer) -> ResolverFuture<Option<BrowseReply>> + Send + Sync>;

/// `(peer, token, folder)` to that folder and every folder below it, or `None` to send no reply
pub type FolderContentsResolver =
  Arc<dyn Fn(Peer, u32, RemoteFilename) -> ResolverFuture<Option<Vec<Directory>>> + Send + Sync>;

/// `(peer)` to the user info we show that peer, or `None` to send no reply. The info is sent as
/// the application made it.
pub type UserInfoResolver = Arc<dyn Fn(Peer) -> ResolverFuture<Option<UserInfo>> + Send + Sync>;

/// What the application does with a peer's request to upload it a file
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UploadDecision {
  /// The application took the request, and sends the file when it chooses with
  /// [`Client::upload`](crate::Client::upload), which runs the upload on a task of its own.
  Queue,
  /// refused, with the reason the peer is told as written
  Deny(String),
  /// not now: the peer is told the upload failed, which is its cue to ask again
  Later,
}

/// `(peer, filename)`: a peer asks us to upload it a file. An error is logged and the peer is
/// told the upload failed, as for [`UploadDecision::Later`].
pub type UploadRequestResolver =
  Arc<dyn Fn(Peer, RemoteFilename) -> ResolverFuture<UploadDecision> + Send + Sync>;

/// `(peer, filename, size)`: a user offers to send us a file we did not ask for, and the client
/// holds the offer while this runs. Succeeding means the application started a download of the
/// file with [`RequestStyle::Offered`](crate::RequestStyle::Offered), which takes the offer;
/// failing refuses it. The reason of a [`ClientError::TransferRejected`] is what the sender is
/// told; any other error is logged and the sender is told "Cancelled".
pub type UnaskedOfferResolver =
  Arc<dyn Fn(Peer, RemoteFilename, u64) -> ResolverFuture<()> + Send + Sync>;

/// `(peer, filename)` to the 1-based place of that file in our upload queue, or `None` to send
/// no reply
pub type PlaceInQueueResolver =
  Arc<dyn Fn(Peer, RemoteFilename) -> ResolverFuture<Option<u32>> + Send + Sync>;

/// Our answer to another user's search
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SearchReply {
  /// the files that matched, which the client encodes
  Response(SearchResponse),
  /// a complete `SearchResponse` frame the application encoded itself, sent as it is
  Encoded(Vec<u8>),
  /// an answer meant for the searcher at the address `to` only: on a connection from any
  /// other address it is not sent, since a peer can give any name when it connects
  Confined {
    /// the answer itself
    reply: Box<SearchReply>,
    /// the only address it may go to
    to: IpAddr,
  },
}

impl SearchReply {
  /// the files the reply offers, locked ones included; unknown for an encoded reply
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

/// Our share listing for a peer that browses us
#[derive(Debug)]
#[non_exhaustive]
pub enum BrowseReply {
  /// the listing, which the client encodes
  Response(BrowseResponse),
  /// a complete `BrowseResponse` frame the application encoded ahead of time, streamed as it is
  Encoded(EncodedFrame),
}

/// Every callback the application can supply. An unset resolver falls back to the behavior
/// each one documents.
#[derive(Clone, Default)]
pub struct Resolvers {
  /// unset: searches are never answered
  pub search_response: Option<SearchResponseResolver>,
  /// unset: browse requests get no reply
  pub browse_response: Option<BrowseResponseResolver>,
  /// unset: folder requests get no reply
  pub folder_contents: Option<FolderContentsResolver>,
  /// unset: user info requests get no reply
  pub user_info: Option<UserInfoResolver>,
  /// unset: every request is refused with "File not shared."
  pub upload_request: Option<UploadRequestResolver>,
  /// unset: place requests get no reply
  pub place_in_queue: Option<PlaceInQueueResolver>,
  /// unset: every file a user offers unasked is refused
  pub unasked_offer: Option<UnaskedOfferResolver>,
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
      .finish()
  }
}
