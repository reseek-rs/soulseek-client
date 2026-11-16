//! What the application supplies so the client can answer peers: share listings, folder
//! contents, user info, search results, upload queueing, files users offer unasked, and the few
//! decisions the client leaves to it. The peer dispatcher, the search responder and the
//! distributed tree call the handler.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use soulseek_proto::peer::{BrowseResponse, Directory, SearchResponse, UserInfo};
use soulseek_proto::{PeerConnectionType, RemoteFilename};

pub use crate::conn::EncodedFrame;

/// what a peer is told when nothing queues uploads
const NOT_SHARED: &str = "File not shared.";
/// what the sender of an unasked file is told when nothing takes it
pub(crate) const NOT_TAKEN: &str = "Cancelled";

/// Peer a request came from.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Peer {
  /// Name the peer gave.
  pub username: String,
  /// Address its connection comes from.
  pub address: SocketAddr,
  /// Whether the name is the user's.
  ///
  /// It is when the server gives this address for the user, or the connection answers a request
  /// of the client's. Otherwise the name is only what the peer said. The crate root says what an
  /// unconfirmed peer is still served.
  pub confirmed: bool,
}

/// Search another user made.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Search {
  /// User who searches.
  ///
  /// A search of the client's own reaches the handler like any other, under the client's own
  /// username.
  pub username: String,
  /// Token the searcher gave the search, which the answer carries.
  pub token: u32,
  /// Text as the searcher sent it.
  ///
  /// [`SearchQuery::parse`](soulseek_proto::SearchQuery::parse) splits it into terms and
  /// exclusions.
  pub query: String,
}

/// Request of a peer to upload it a file.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct UploadRequest {
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
pub enum Upload {
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

/// What the application does with a file a user offers to send unasked.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Offer {
  /// The application started a download of the file, which takes the offer.
  ///
  /// The download is one with [`RequestStyle::Offered`](crate::transfer::RequestStyle::Offered).
  Taken,
  /// Refused, with the reason the sender is told as written.
  Refused(String),
}

/// Answers of the application to what peers ask of the client.
///
/// Every method has a default that gives no answer, so an application implements only what it
/// serves. The methods that answer a peer are asynchronous and may run at the same time for
/// different peers. The ones the client decides something with on the spot are plain functions,
/// called on the client's own tasks, so they must not wait.
///
/// The handler is part of the [`Config`](crate::Config), set with
/// [`Config::handler`](crate::Config::handler). One that holds a clone of the
/// [`Client`](crate::Client), as one that starts uploads does, keeps the client from being
/// dropped, so such a client stops only at [`Client::shutdown`](crate::Client::shutdown).
///
/// # Examples
///
/// ```
/// use soulseek_client::handler::{Handler, Peer, Upload, UploadRequest};
///
/// struct NothingShared;
///
/// impl Handler for NothingShared {
///   // refuses every file a peer asks for; the other methods keep their defaults
///   async fn upload(&self, _peer: &Peer, _request: &UploadRequest) -> Upload {
///     Upload::Deny("Not sharing".into())
///   }
/// }
/// ```
pub trait Handler: Send + Sync + 'static {
  /// Answers a search another user made.
  ///
  /// Returns the client's answer, or `None` when the client has nothing that matches. The
  /// response names the client, in `username`, and the search, with its token. An answer with
  /// no files is not sent. Returns `None` by default.
  fn search(&self, search: &Search) -> impl Future<Output = Option<SearchReply>> + Send {
    let _ = search;
    async { None }
  }

  /// Returns the share listing sent to a peer that browses the client.
  ///
  /// `None` sends no reply. Returns `None` by default.
  fn browse(&self, peer: &Peer) -> impl Future<Output = Option<BrowseReply>> + Send {
    let _ = peer;
    async { None }
  }

  /// Returns the contents of a folder a peer asks for.
  ///
  /// The answer is that folder and every folder below it. `None` sends no reply. Returns `None`
  /// by default.
  fn folder(
    &self,
    peer: &Peer,
    directory: &RemoteFilename,
  ) -> impl Future<Output = Option<Vec<Directory>>> + Send {
    let _ = (peer, directory);
    async { None }
  }

  /// Returns the user info the client shows a peer.
  ///
  /// The info is sent as the application made it. `None` sends no reply. Returns `None` by
  /// default.
  fn user_info(&self, peer: &Peer) -> impl Future<Output = Option<UserInfo>> + Send {
    let _ = peer;
    async { None }
  }

  /// Decides a peer's request that the client upload it a file.
  ///
  /// Refuses the file as not shared by default.
  fn upload(&self, peer: &Peer, request: &UploadRequest) -> impl Future<Output = Upload> + Send {
    let _ = (peer, request);
    async { Upload::Deny(NOT_SHARED.into()) }
  }

  /// Returns the place of a file in the client's upload queue.
  ///
  /// The place is 1-based. `None` sends no reply. Returns `None` by default.
  fn place_in_queue(
    &self,
    peer: &Peer,
    filename: &RemoteFilename,
  ) -> impl Future<Output = Option<u32>> + Send {
    let _ = (peer, filename);
    async { None }
  }

  /// Decides a file of `size` bytes a user offers to send that the client did not ask for.
  ///
  /// The client holds the offer while this runs. Refuses it by default.
  fn unasked_offer(
    &self,
    peer: &Peer,
    filename: &RemoteFilename,
    size: u64,
  ) -> impl Future<Output = Offer> + Send {
    let _ = (peer, filename, size);
    async { Offer::Refused(NOT_TAKEN.into()) }
  }

  /// Returns whether to act on a connection request the server relays.
  ///
  /// The server relays a user's request that the client connect to them, and names the user
  /// and the address, so the peer is confirmed. Returning `false` refuses: the server is told
  /// the client cannot connect and nothing is dialed. A user who cannot reach the client's
  /// listener depends on these requests, for the transfers of files the client asked them for
  /// as well. Returns `true` by default.
  fn accept_connection(&self, peer: &Peer, connection_type: PeerConnectionType) -> bool {
    let _ = (peer, connection_type);
    true
  }

  /// Picks the client's distributed parent.
  ///
  /// Returns the index of the candidate to adopt, or `None` to adopt none of them. The
  /// candidates are those of the server's offer that answered as a parent must, nearest the top
  /// of its branch first. Returns the first by default.
  fn choose_parent(&self, candidates: &[crate::distributed::Parent]) -> Option<usize> {
    (!candidates.is_empty()).then_some(0)
  }

  /// Returns whether a peer may join the distributed search tree below the client.
  ///
  /// It is asked while a slot is free. Returns `true` by default.
  fn accept_child(&self, peer: &Peer) -> bool {
    let _ = peer;
    true
  }
}

/// the handler of a client whose application set none
pub(crate) struct Unhandled;

impl Handler for Unhandled {}

type Answer<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// [`Handler`] with its futures boxed, which a client can hold without naming the
/// application's type
pub(crate) trait AnyHandler: Send + Sync + 'static {
  fn search<'a>(&'a self, search: &'a Search) -> Answer<'a, Option<SearchReply>>;
  fn browse<'a>(&'a self, peer: &'a Peer) -> Answer<'a, Option<BrowseReply>>;
  fn folder<'a>(
    &'a self,
    peer: &'a Peer,
    directory: &'a RemoteFilename,
  ) -> Answer<'a, Option<Vec<Directory>>>;
  fn user_info<'a>(&'a self, peer: &'a Peer) -> Answer<'a, Option<UserInfo>>;
  fn upload<'a>(&'a self, peer: &'a Peer, request: &'a UploadRequest) -> Answer<'a, Upload>;
  fn place_in_queue<'a>(
    &'a self,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
  ) -> Answer<'a, Option<u32>>;
  fn unasked_offer<'a>(
    &'a self,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
    size: u64,
  ) -> Answer<'a, Offer>;
  fn accept_connection(&self, peer: &Peer, connection_type: PeerConnectionType) -> bool;
  fn choose_parent(&self, candidates: &[crate::distributed::Parent]) -> Option<usize>;
  fn accept_child(&self, peer: &Peer) -> bool;
}

impl<H: Handler> AnyHandler for H {
  fn search<'a>(&'a self, search: &'a Search) -> Answer<'a, Option<SearchReply>> {
    Box::pin(Handler::search(self, search))
  }

  fn browse<'a>(&'a self, peer: &'a Peer) -> Answer<'a, Option<BrowseReply>> {
    Box::pin(Handler::browse(self, peer))
  }

  fn folder<'a>(
    &'a self,
    peer: &'a Peer,
    directory: &'a RemoteFilename,
  ) -> Answer<'a, Option<Vec<Directory>>> {
    Box::pin(Handler::folder(self, peer, directory))
  }

  fn user_info<'a>(&'a self, peer: &'a Peer) -> Answer<'a, Option<UserInfo>> {
    Box::pin(Handler::user_info(self, peer))
  }

  fn upload<'a>(&'a self, peer: &'a Peer, request: &'a UploadRequest) -> Answer<'a, Upload> {
    Box::pin(Handler::upload(self, peer, request))
  }

  fn place_in_queue<'a>(
    &'a self,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
  ) -> Answer<'a, Option<u32>> {
    Box::pin(Handler::place_in_queue(self, peer, filename))
  }

  fn unasked_offer<'a>(
    &'a self,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
    size: u64,
  ) -> Answer<'a, Offer> {
    Box::pin(Handler::unasked_offer(self, peer, filename, size))
  }

  fn accept_connection(&self, peer: &Peer, connection_type: PeerConnectionType) -> bool {
    Handler::accept_connection(self, peer, connection_type)
  }

  fn choose_parent(&self, candidates: &[crate::distributed::Parent]) -> Option<usize> {
    Handler::choose_parent(self, candidates)
  }

  fn accept_child(&self, peer: &Peer) -> bool {
    Handler::accept_child(self, peer)
  }
}

/// the application's handler as the configuration holds it
#[derive(Clone)]
pub(crate) struct Shared(pub(crate) Arc<dyn AnyHandler>);

impl Default for Shared {
  fn default() -> Self {
    Self(Arc::new(Unhandled))
  }
}

/// the handler itself has nothing to print
impl fmt::Debug for Shared {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("Handler")
  }
}

impl std::ops::Deref for Shared {
  type Target = dyn AnyHandler;

  fn deref(&self) -> &Self::Target {
    &*self.0
  }
}

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
