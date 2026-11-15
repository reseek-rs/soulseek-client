//! What the probe shares: one file, found by a search for words of its name, listed when a peer
//! browses, and uploaded at once to whoever asks for it.

use std::path::PathBuf;

use soulseek_client::handler::{BrowseReply, Handler, Peer, Search, SearchReply, Upload};
use soulseek_client::soulseek_proto::SearchQuery;
use soulseek_client::soulseek_proto::peer::{
  BrowseResponse, Directory, File, SearchResponse, UserInfo,
};
use soulseek_client::{CancellationToken, Client, UploadInput, UploadRequest};

use crate::report;

/// the file's name on disk, in the work directory
pub const FILE_NAME: &str = "probe song.flac";
/// the folder and the file as other clients see them
pub const REMOTE_FOLDER: &str = "@@probe\\Probe Artist\\Probe Album";
pub const REMOTE_NAME: &str = "@@probe\\Probe Artist\\Probe Album\\probe song.flac";
/// what the probe says about itself
pub const DESCRIPTION: &str = "soulseek-client interop probe";

/// Shares the file at `path` on behalf of `client`
pub struct Sharing {
  client: Client,
  path: PathBuf,
  /// the file as a search names it
  file: File,
}

impl Sharing {
  /// shares the file at `path`, which is `size` bytes long
  pub fn new(client: &Client, path: PathBuf, size: u64) -> Self {
    Self {
      client: client.clone(),
      path,
      file: File {
        code: 1,
        filename: REMOTE_NAME.into(),
        size,
        extension: "flac".into(),
        attributes: Vec::new(),
      },
    }
  }
}

impl Handler for Sharing {
  async fn search(&self, search: &Search) -> Option<SearchReply> {
    let terms = SearchQuery::parse(&search.query).terms;
    let name = REMOTE_NAME.to_lowercase();
    let matches = !terms.is_empty() && terms.iter().all(|term| name.contains(&term.to_lowercase()));
    matches.then(|| {
      SearchReply::Response(SearchResponse {
        username: self.client.username().unwrap_or_default(),
        token: search.token,
        files: vec![self.file.clone()],
        has_free_upload_slot: true,
        upload_speed: 1000,
        queue_length: 0,
        locked_files: Vec::new(),
      })
    })
  }

  async fn browse(&self, _peer: &Peer) -> Option<BrowseReply> {
    // a listing names a file without its folder
    let listed = File {
      filename: FILE_NAME.into(),
      ..self.file.clone()
    };
    Some(BrowseReply::Response(BrowseResponse {
      directories: vec![Directory {
        name: REMOTE_FOLDER.into(),
        files: vec![listed],
      }],
      locked_directories: Vec::new(),
    }))
  }

  async fn user_info(&self, _peer: &Peer) -> Option<UserInfo> {
    Some(UserInfo {
      description: DESCRIPTION.into(),
      picture: None,
      upload_slots: 1,
      queue_length: 0,
      has_free_upload_slot: true,
      upload_permitted: None,
    })
  }

  async fn upload(&self, peer: &Peer, request: &soulseek_client::handler::UploadRequest) -> Upload {
    if request.filename.as_str() != REMOTE_NAME {
      return Upload::Deny("File not shared.".into());
    }
    let (client, path) = (self.client.clone(), self.path.clone());
    let (username, filename) = (peer.username.clone(), request.filename.clone());
    // the upload runs on a task of its own; this only takes the request
    tokio::spawn(async move {
      let upload = UploadRequest::new(username.clone(), filename, UploadInput::File(path));
      let result = client.upload(upload, &CancellationToken::new()).await;
      report(
        &username,
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
    Upload::Queue
  }
}
