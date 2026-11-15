# soulseek-client

[![crates.io](https://img.shields.io/crates/v/soulseek-client)](https://crates.io/crates/soulseek-client)
[![docs.rs](https://img.shields.io/docsrs/soulseek-client)](https://docs.rs/soulseek-client)
[![ci](https://github.com/reseek-rs/soulseek-client/actions/workflows/ci.yml/badge.svg)](https://github.com/reseek-rs/soulseek-client/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/soulseek-client)](#license)

A Soulseek client library on tokio: the server session, peer connections (direct and through the
server's relay), downloads and uploads, searches, browsing, user info, the distributed search
tree, answering other users' searches and the listener that accepts peers. The wire format comes
from [`soulseek-proto`](https://github.com/reseek-rs/soulseek-proto).

The client offers actions and reports what happens as events. Policy is left to the application:
who to reconnect to and when, buddies and ban lists, what to share, how many uploads run at once
and what to answer a peer. The client takes each of these from the caller, as an option or as a
resolver it asks, and decides none of them itself. Limits that bound what remote input can make
the client hold stay in the client.

## Usage

```toml
[dependencies]
soulseek-client = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use soulseek_client::{
  CancellationToken, Client, ClientError, ClientOptions, DEFAULT_SERVER_ADDRESS,
  DEFAULT_SERVER_PORT, DownloadOutput, DownloadRequest, SearchOptions, SearchRequest, SearchScope,
};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
  let cancel = CancellationToken::new();
  let client = Client::new(ClientOptions::default())?;
  client
    .connect(DEFAULT_SERVER_ADDRESS, DEFAULT_SERVER_PORT, "username", "password", &cancel)
    .await?;

  // the search ends at its timeout or a limit, with every response it took
  let request = SearchRequest::new("some artist flac", SearchScope::Network, SearchOptions::default())?;
  let (_search, responses) = client.search(request, &cancel).await?;

  let found = responses
    .iter()
    .find_map(|response| Some((response, response.files.first()?)));
  if let Some((response, file)) = found {
    let download = DownloadRequest::new(
      response.username.clone(),
      file.filename.clone(),
      file.size,
      DownloadOutput::File("download.flac".into()),
    );
    let transfer = client.download(download, &cancel).await?;
    println!("downloaded {} bytes", transfer.bytes_transferred);
  }

  client.disconnect(None, None).await;
  Ok(())
}
```

The protocol's types, such as the `SearchResponse` a search resolver returns, are re-exported as
`soulseek_client::soulseek_proto` at the version the client is built on. Take them from there
rather than from a separate `soulseek-proto` dependency: at another version its types have the
same names and do not match.

`examples/search_and_download.rs` runs two clients against a local server, one sharing a file and
the other searching for it and downloading it.

## Events and resolvers

`Client::subscribe` returns a broadcast receiver of `Event`s: session state, private and room
messages, user status, search responses, transfer state and progress. What a peer asks of the
client (a search, a browse, user info, a file) is answered by the application through the
`Resolvers` in `ClientOptions`.

The client logs through `tracing` and installs no subscriber.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT)
at your option. Unless you explicitly state otherwise, any contribution you submit for inclusion
in this crate, as defined in the Apache-2.0 license, is dual licensed as above, without any
additional terms or conditions.
