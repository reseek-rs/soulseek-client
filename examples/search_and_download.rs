//! Two clients on one server: one shares a file from memory, the other searches for it and
//! downloads it. Run against a server that takes new accounts, such as soulfind:
//!
//! ```text
//! cargo run --example search_and_download -- 127.0.0.1 2242
//! ```

use std::sync::Arc;
use std::time::Duration;

use soulseek_client::soulseek_proto::peer::{File, SearchResponse};
use soulseek_client::{
  CancellationToken, Client, ClientError, ClientOptions, ClientOptionsPatch, DownloadOutput,
  DownloadRequest, ResolverFuture, Resolvers, SearchOptions, SearchReply, SearchRequest,
  SearchScope, UploadInput, UploadRequest, UploadSource,
};

/// what the sharer shares
const SHARED: &str = "@@music\\example\\song.flac";
const CONTENT: &[u8] = b"not really a flac file, but the bytes move all the same";

#[tokio::main]
async fn main() -> Result<(), ClientError> {
  // the client's own log lines
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();
  let mut args = std::env::args().skip(1);
  let host = args.next().unwrap_or_else(|| "127.0.0.1".into());
  let port: u16 = args
    .next()
    .map_or(2242, |port| port.parse().expect("a port"));
  let suffix = std::process::id();
  let cancel = CancellationToken::new();

  // the sharer listens, so the fetcher can reach it
  let sharer = Client::new(ClientOptions {
    listen_port: 50_101,
    ..ClientOptions::default()
  })?;
  // its resolvers start uploads, so they take the client once it exists
  sharer
    .reconfigure_options(ClientOptionsPatch {
      resolvers: sharing(&sharer),
      ..ClientOptionsPatch::default()
    })
    .await?;
  let sharer_name = format!("sharer{suffix}");
  sharer
    .connect(&host, port, &sharer_name, "password", &cancel)
    .await?;

  let fetcher = Client::new(ClientOptions {
    enable_listener: false,
    ..ClientOptions::default()
  })?;
  fetcher
    .connect(
      &host,
      port,
      &format!("fetcher{suffix}"),
      "password",
      &cancel,
    )
    .await?;

  let options = SearchOptions {
    search_timeout: Duration::from_secs(2),
    ..SearchOptions::default()
  };
  let request = SearchRequest::new("song", SearchScope::Users(vec![sharer_name]), options)?;
  let (_, responses) = fetcher.search(request, &cancel).await?;
  let response = responses.first().expect("the sharer answers");
  let file = &response.files[0];
  println!(
    "found {} ({} bytes) at {}",
    file.filename, file.size, response.username
  );

  let into = std::env::temp_dir().join(format!("search_and_download{suffix}.flac"));
  let download = DownloadRequest::new(
    response.username.clone(),
    file.filename.clone(),
    file.size,
    DownloadOutput::File(into.clone()),
  );
  let done = fetcher.download(download, &cancel).await?;
  println!(
    "downloaded {} bytes into {}",
    done.bytes_transferred,
    into.display()
  );
  assert_eq!(std::fs::read(&into).expect("the file arrived"), CONTENT);
  std::fs::remove_file(&into).expect("the file can go");

  fetcher.disconnect(None, None).await;
  sharer.disconnect(None, None).await;
  Ok(())
}

/// The resolvers of a client that shares `SHARED`: a search for anything in its name is
/// answered with it, and a request for it is sent at once.
fn sharing(client: &Client) -> Resolvers {
  let file = File {
    code: 1,
    filename: SHARED.into(),
    size: CONTENT.len() as u64,
    extension: "flac".into(),
    attributes: Vec::new(),
  };
  let (client, answering) = (client.clone(), client.clone());
  Resolvers {
    // the response names us, not the user who searched
    search_response: Some(Arc::new(move |_searcher, token, query| {
      let matches = query
        .terms
        .iter()
        .all(|term| SHARED.to_lowercase().contains(&term.to_lowercase()));
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
    upload_request: Some(Arc::new(move |username, _, filename| {
      let client = client.clone();
      Box::pin(async move {
        if filename.as_str() != SHARED {
          return Err(ClientError::UploadRefused("File not shared.".into()));
        }
        let input = UploadInput::Stream {
          size: CONTENT.len() as u64,
          open: Box::new(|_| {
            Box::pin(async { Ok(Box::new(std::io::Cursor::new(CONTENT)) as Box<dyn UploadSource>) })
          }),
        };
        // the upload runs on a task of its own; this only takes the request
        tokio::spawn(async move {
          let upload = UploadRequest::new(username, filename, input);
          if let Err(error) = client.upload(upload, &CancellationToken::new()).await {
            eprintln!("upload failed: {error}");
          }
        });
        Ok(())
      }) as ResolverFuture<_>
    })),
    ..Resolvers::default()
  }
}
