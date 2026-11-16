//! What the probe shares: one file, found by a search for words of its name, listed when a peer
//! browses, and uploaded at once to whoever asks for it.

use std::path::PathBuf;

use soulseek_client::Client;
use soulseek_client::handler::{
  BrowseReply, Handler, Matches, Peer, SearchReply, SearchRequest, UploadReply,
};
use soulseek_client::soulseek_proto::SearchQuery;
use soulseek_client::soulseek_proto::peer::{BrowseResponse, Directory, File, UserInfo};

use crate::report;

/// the file's name on disk, in the work directory
pub const FILE_NAME: &str = "probe song.flac";
/// the folder and the file as other clients see them
pub const REMOTE_FOLDER: &str = "@@probe\\Probe Artist\\Probe Album";
pub const REMOTE_NAME: &str = "@@probe\\Probe Artist\\Probe Album\\probe song.flac";
/// what the probe says about itself
pub const DESCRIPTION: &str = "soulseek-client interop probe";

/// Shares the file at `path`
pub struct Sharing {
  path: PathBuf,
  /// the file as a search names it
  file: File,
}

impl Sharing {
  /// shares the file at `path`, which is `size` bytes long
  pub fn new(path: PathBuf, size: u64) -> Self {
    Self {
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
  async fn search(&self, _client: &Client, search: &SearchRequest) -> Option<SearchReply> {
    let terms = SearchQuery::parse(&search.query).terms;
    let name = REMOTE_NAME.to_lowercase();
    let matches = !terms.is_empty() && terms.iter().all(|term| name.contains(&term.to_lowercase()));
    matches.then(|| SearchReply::Matches(Matches::new(vec![self.file.clone()]).upload_speed(1000)))
  }

  async fn browse(&self, _client: &Client, _peer: &Peer) -> Option<BrowseReply> {
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

  async fn user_info(&self, _client: &Client, _peer: &Peer) -> Option<UserInfo> {
    Some(UserInfo {
      description: DESCRIPTION.into(),
      picture: None,
      upload_slots: 1,
      queue_length: 0,
      has_free_upload_slot: true,
      upload_permitted: None,
    })
  }

  async fn upload(
    &self,
    client: &Client,
    peer: &Peer,
    request: &soulseek_client::handler::UploadRequest,
  ) -> UploadReply {
    if request.filename.as_str() != REMOTE_NAME {
      return UploadReply::Deny("File not shared.".into());
    }
    let (client, path) = (client.clone(), self.path.clone());
    let (username, filename) = (peer.username.clone(), request.filename.clone());
    // the upload runs on a task of its own; this only takes the request
    tokio::spawn(async move {
      let result = client
        .upload(username.clone(), filename)
        .from_file(path)
        .await;
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
    UploadReply::Queue
  }
}
