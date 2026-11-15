//! What the probe shares: one file, found by a search for words of its name, listed when a peer
//! browses, and uploaded at once to whoever asks for it.

use std::path::PathBuf;
use std::sync::Arc;

use soulseek_client::soulseek_proto::SearchQuery;
use soulseek_client::soulseek_proto::peer::{
  BrowseResponse, Directory, File, SearchResponse, UserInfo,
};
use soulseek_client::{
  BrowseReply, CancellationToken, Client, Peer, ResolverFuture, Resolvers, SearchReply, UploadAsk,
  UploadDecision, UploadInput, UploadRequest,
};

use crate::report;

/// the file's name on disk, in the work directory
pub const FILE_NAME: &str = "probe song.flac";
/// the folder and the file as other clients see them
pub const REMOTE_FOLDER: &str = "@@probe\\Probe Artist\\Probe Album";
pub const REMOTE_NAME: &str = "@@probe\\Probe Artist\\Probe Album\\probe song.flac";
/// what the probe says about itself
pub const DESCRIPTION: &str = "soulseek-client interop probe";

/// the resolvers of a client that shares the file at `path`, which is `size` bytes long
pub fn resolvers(client: &Client, path: PathBuf, size: u64) -> Resolvers {
  let file = File {
    code: 1,
    filename: REMOTE_NAME.into(),
    size,
    extension: "flac".into(),
    attributes: Vec::new(),
  };
  // a listing names a file without its folder
  let listed = File {
    filename: FILE_NAME.into(),
    ..file.clone()
  };
  let (answering, uploading) = (client.clone(), client.clone());
  Resolvers {
    search_response: Some(Arc::new(move |_searcher, token, query| {
      let terms = SearchQuery::parse(&query).terms;
      let name = REMOTE_NAME.to_lowercase();
      let matches =
        !terms.is_empty() && terms.iter().all(|term| name.contains(&term.to_lowercase()));
      let reply = matches.then(|| {
        SearchReply::Response(SearchResponse {
          username: answering.username().unwrap_or_default(),
          token,
          files: vec![file.clone()],
          has_free_upload_slot: true,
          upload_speed: 1000,
          queue_length: 0,
          locked_files: Vec::new(),
        })
      });
      Box::pin(async move { Ok(reply) }) as ResolverFuture<_>
    })),
    browse_response: Some(Arc::new(move |_peer: Peer| {
      let listing = BrowseResponse {
        directories: vec![Directory {
          name: REMOTE_FOLDER.into(),
          files: vec![listed.clone()],
        }],
        locked_directories: Vec::new(),
      };
      Box::pin(async move { Ok(Some(BrowseReply::Response(listing))) }) as ResolverFuture<_>
    })),
    user_info: Some(Arc::new(|_peer: Peer| {
      Box::pin(async move {
        Ok(Some(UserInfo {
          description: DESCRIPTION.into(),
          picture: None,
          upload_slots: 1,
          queue_length: 0,
          has_free_upload_slot: true,
          upload_permitted: None,
        }))
      }) as ResolverFuture<_>
    })),
    upload_request: Some(Arc::new(move |peer: Peer, ask: UploadAsk| {
      let (client, path) = (uploading.clone(), path.clone());
      Box::pin(async move {
        if ask.filename.as_str() != REMOTE_NAME {
          return Ok(UploadDecision::Deny("File not shared.".into()));
        }
        // the upload runs on a task of its own; this only takes the request
        tokio::spawn(async move {
          let upload =
            UploadRequest::new(peer.username.clone(), ask.filename, UploadInput::File(path));
          let result = client.upload(upload, &CancellationToken::new()).await;
          report(
            &peer.username,
            "upload to the peer",
            result
              .map(|transfer| {
                format!(
                  "{} bytes sent, {}",
                  transfer.bytes_transferred, transfer.state
                )
              })
              .map_err(|error| format!("{error:?}")),
          );
        });
        Ok(UploadDecision::Queue)
      }) as ResolverFuture<_>
    })),
    ..Resolvers::default()
  }
}
