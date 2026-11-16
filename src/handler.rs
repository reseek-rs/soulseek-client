//! What the application supplies so the client can answer peers.
//!
//! The handler gives share listings, folder contents, user info, search results, upload
//! queueing, what to do with files users offer unasked, and the few decisions the client leaves
//! to the application. The peer dispatcher, the search responder and the distributed tree call
//! the handler.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use soulseek_proto::peer::{BrowseResponse, Directory, File, SearchResponse, UserInfo};
use soulseek_proto::{PeerConnectionType, RemoteFilename};

use crate::client::Client;
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
pub struct SearchRequest {
  /// User who searches.
  ///
  /// A search of the client's own reaches the handler only when it searches the client's own
  /// shares, as a [`Scope::Users`](crate::search::Scope::Users) search naming the client does.
  pub username: String,
  /// Token the searcher gave the search.
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
pub enum UploadReply {
  /// The application took the request.
  ///
  /// It sends the file when it chooses with [`Client::upload`](crate::Client::upload).
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
pub enum OfferReply {
  /// The application started a download of the file, which takes the offer.
  ///
  /// The download is one with [`RequestStyle::Offered`](crate::transfer::RequestStyle::Offered).
  Taken,
  /// Refused, with the reason the sender is told as written.
  Refused(String),
}

/// Answers of the application to what peers ask of the client.
///
/// Every method has a default, stated on the method, so an application implements only what it
/// serves. The methods that answer a peer are asynchronous and may run at the same time for
/// different peers. The ones the client decides something with on the spot are plain functions,
/// called on the client's own tasks, so they must not wait.
///
/// The handler is part of the [`Config`](crate::Config), set with
/// [`ConfigBuilder::handler`](crate::config::ConfigBuilder::handler). The asynchronous methods
/// are given the client they answer for, which a handler that starts uploads starts them
/// with.
///
/// # Examples
///
/// ```
/// use soulseek_client::Client;
/// use soulseek_client::handler::{Handler, Peer, UploadReply, UploadRequest};
///
/// struct NothingShared;
///
/// impl Handler for NothingShared {
///   // refuses every file a peer asks for; the other methods keep their defaults
///   async fn upload(&self, _client: &Client, _peer: &Peer, _request: &UploadRequest) -> UploadReply {
///     UploadReply::Deny("Not sharing".into())
///   }
/// }
/// ```
pub trait Handler: Send + Sync + 'static {
  /// Answers a search another user made.
  ///
  /// Returns the client's answer, or `None` when the client has nothing that matches. An
  /// answer with no files is not sent. An [`SearchReply::Encoded`] answer names the client and
  /// the search's token itself; the client fills both in for [`SearchReply::Matches`]. Returns
  /// `None` by default.
  fn search(
    &self,
    client: &Client,
    search: &SearchRequest,
  ) -> impl Future<Output = Option<SearchReply>> + Send {
    let _ = (client, search);
    async { None }
  }

  /// Returns the share listing sent to a peer that browses the client.
  ///
  /// `None` sends no reply. Returns `None` by default.
  fn browse(
    &self,
    client: &Client,
    peer: &Peer,
  ) -> impl Future<Output = Option<BrowseReply>> + Send {
    let _ = (client, peer);
    async { None }
  }

  /// Returns the contents of a folder a peer asks for.
  ///
  /// The answer is that folder and every folder below it. `None` sends no reply. Returns `None`
  /// by default.
  fn folder(
    &self,
    client: &Client,
    peer: &Peer,
    directory: &RemoteFilename,
  ) -> impl Future<Output = Option<Vec<Directory>>> + Send {
    let _ = (client, peer, directory);
    async { None }
  }

  /// Returns the user info the client shows a peer.
  ///
  /// The info is sent as the application made it. `None` sends no reply. Returns `None` by
  /// default.
  fn user_info(
    &self,
    client: &Client,
    peer: &Peer,
  ) -> impl Future<Output = Option<UserInfo>> + Send {
    let _ = (client, peer);
    async { None }
  }

  /// Decides a peer's request that the client upload it a file.
  ///
  /// Refuses the file as not shared by default.
  fn upload(
    &self,
    client: &Client,
    peer: &Peer,
    request: &UploadRequest,
  ) -> impl Future<Output = UploadReply> + Send {
    let _ = (client, peer, request);
    async { UploadReply::Deny(NOT_SHARED.into()) }
  }

  /// Returns the place of a file in the client's upload queue.
  ///
  /// The place is 1-based. `None` sends no reply. Returns `None` by default.
  fn place_in_queue(
    &self,
    client: &Client,
    peer: &Peer,
    filename: &RemoteFilename,
  ) -> impl Future<Output = Option<u32>> + Send {
    let _ = (client, peer, filename);
    async { None }
  }

  /// Decides a file a user offers to send that the client did not ask for.
  ///
  /// `size` is the file's size in bytes. The client holds the offer while this runs. Refuses it
  /// by default.
  fn unasked_offer(
    &self,
    client: &Client,
    peer: &Peer,
    filename: &RemoteFilename,
    size: u64,
  ) -> impl Future<Output = OfferReply> + Send {
    let _ = (client, peer, filename, size);
    async { OfferReply::Refused(NOT_TAKEN.into()) }
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
  fn search<'a>(
    &'a self,
    client: &'a Client,
    search: &'a SearchRequest,
  ) -> Answer<'a, Option<SearchReply>>;
  fn browse<'a>(&'a self, client: &'a Client, peer: &'a Peer) -> Answer<'a, Option<BrowseReply>>;
  fn folder<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    directory: &'a RemoteFilename,
  ) -> Answer<'a, Option<Vec<Directory>>>;
  fn user_info<'a>(&'a self, client: &'a Client, peer: &'a Peer) -> Answer<'a, Option<UserInfo>>;
  fn upload<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    request: &'a UploadRequest,
  ) -> Answer<'a, UploadReply>;
  fn place_in_queue<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
  ) -> Answer<'a, Option<u32>>;
  fn unasked_offer<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
    size: u64,
  ) -> Answer<'a, OfferReply>;
  fn accept_connection(&self, peer: &Peer, connection_type: PeerConnectionType) -> bool;
  fn choose_parent(&self, candidates: &[crate::distributed::Parent]) -> Option<usize>;
  fn accept_child(&self, peer: &Peer) -> bool;
}

impl<H: Handler> AnyHandler for H {
  fn search<'a>(
    &'a self,
    client: &'a Client,
    search: &'a SearchRequest,
  ) -> Answer<'a, Option<SearchReply>> {
    Box::pin(Handler::search(self, client, search))
  }

  fn browse<'a>(&'a self, client: &'a Client, peer: &'a Peer) -> Answer<'a, Option<BrowseReply>> {
    Box::pin(Handler::browse(self, client, peer))
  }

  fn folder<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    directory: &'a RemoteFilename,
  ) -> Answer<'a, Option<Vec<Directory>>> {
    Box::pin(Handler::folder(self, client, peer, directory))
  }

  fn user_info<'a>(&'a self, client: &'a Client, peer: &'a Peer) -> Answer<'a, Option<UserInfo>> {
    Box::pin(Handler::user_info(self, client, peer))
  }

  fn upload<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    request: &'a UploadRequest,
  ) -> Answer<'a, UploadReply> {
    Box::pin(Handler::upload(self, client, peer, request))
  }

  fn place_in_queue<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
  ) -> Answer<'a, Option<u32>> {
    Box::pin(Handler::place_in_queue(self, client, peer, filename))
  }

  fn unasked_offer<'a>(
    &'a self,
    client: &'a Client,
    peer: &'a Peer,
    filename: &'a RemoteFilename,
    size: u64,
  ) -> Answer<'a, OfferReply> {
    Box::pin(Handler::unasked_offer(self, client, peer, filename, size))
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
  /// Files that matched, which the client encodes with its own username and the search's
  /// token.
  Matches(Matches),
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
      Self::Matches(matches) => Some(matches.files.len() + matches.locked_files.len()),
      Self::Encoded(_) => None,
      Self::Confined { reply, .. } => reply.file_count(),
    }
  }

  /// the answer without the address it is confined to: a `Matches` or an `Encoded`
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

  /// the complete frame to write, from the client `username` for the search `token`
  pub(crate) fn frame(&self, username: &str, token: u32) -> Vec<u8> {
    match self {
      Self::Matches(matches) => {
        soulseek_proto::PeerMessage::SearchResponse(matches.response(username, token)).encode()
      }
      Self::Encoded(frame) => frame.clone(),
      Self::Confined { reply, .. } => reply.frame(username, token),
    }
  }
}

/// Files of the client's that match a search.
///
/// Made with [`Matches::new`]. The client answers with them under its own username and the
/// search's token.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Matches {
  /// Files that matched.
  pub files: Vec<File>,
  /// Files that matched and are shared only with selected users.
  pub locked_files: Vec<File>,
  /// Whether an upload slot is free now.
  pub has_free_upload_slot: bool,
  /// Upload speed, in bytes per second.
  pub upload_speed: u32,
  /// Number of uploads queued.
  pub queue_length: u32,
}

impl Matches {
  /// Creates matches of `files`.
  ///
  /// They have no locked files, a free upload slot, an upload speed of 0 and an empty queue
  /// until set otherwise.
  pub fn new(files: Vec<File>) -> Self {
    Self {
      files,
      locked_files: Vec::new(),
      has_free_upload_slot: true,
      upload_speed: 0,
      queue_length: 0,
    }
  }

  /// Sets the files that matched and are shared only with selected users.
  #[must_use]
  pub fn locked_files(mut self, files: Vec<File>) -> Self {
    self.locked_files = files;
    self
  }

  /// Sets whether an upload slot is free now.
  #[must_use]
  pub fn free_upload_slot(mut self, free: bool) -> Self {
    self.has_free_upload_slot = free;
    self
  }

  /// Sets the upload speed, in bytes per second.
  #[must_use]
  pub fn upload_speed(mut self, speed: u32) -> Self {
    self.upload_speed = speed;
    self
  }

  /// Sets the number of uploads queued.
  #[must_use]
  pub fn queue_length(mut self, length: u32) -> Self {
    self.queue_length = length;
    self
  }

  /// the response the client sends, as `username`, for the search `token`
  pub(crate) fn response(&self, username: &str, token: u32) -> SearchResponse {
    SearchResponse {
      username: username.to_owned(),
      token,
      files: self.files.clone(),
      has_free_upload_slot: self.has_free_upload_slot,
      upload_speed: self.upload_speed,
      queue_length: self.queue_length,
      locked_files: self.locked_files.clone(),
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
