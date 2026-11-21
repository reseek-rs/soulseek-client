# soulseek-client

[![crates.io](https://img.shields.io/crates/v/soulseek-client)](https://crates.io/crates/soulseek-client)
[![docs.rs](https://img.shields.io/docsrs/soulseek-client)](https://docs.rs/soulseek-client)
[![ci](https://github.com/reseek-rs/soulseek-client/actions/workflows/ci.yml/badge.svg)](https://github.com/reseek-rs/soulseek-client/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/soulseek-client)](#license)

A Soulseek client library on tokio. The wire format comes from
[`soulseek-proto`](https://github.com/reseek-rs/soulseek-proto).

## Features

- Server session: connect, log in, disconnect
- Peer connections, direct or through the server's relay, and a listener that accepts peers
- Searches, with responses as a stream
- Downloads and uploads, to and from files or the application's own sinks and sources
- Browsing users and fetching their user info
- Chat rooms, interests, private messages and the account
- The distributed search tree
- Answers to other users' searches, browses and file requests, through a handler
- A SOCKS5 proxy per kind of connection
- Bounds on what remote input can make the client hold
- Events as one stream, or one stream per topic

## Usage

```toml
[dependencies]
soulseek-client = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use soulseek_client::{Client, Config, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
  let client = Client::new(Config::default());
  client.connect("username", "password").await?;

  // the search ends once responses stop coming or at a limit
  let mut responses = client.search("some artist flac").await?;
  while let Some(response) = responses.next().await {
    let Some(file) = response.files.first() else {
      continue;
    };
    let transfer = client
      .download(response.username.clone(), file.filename.clone())
      .size(file.size)
      .to_file("download.flac")
      .await?;
    println!("downloaded {} bytes", transfer.bytes_transferred);
    break;
  }

  client.disconnect().await;
  Ok(())
}
```

### Client and handles

`Client` holds the session and the operations: `connect`, `search`, `download`, `upload`. Each
topic has a handle:

| Handle | Description |
|---|---|
| `client.users()` | user status and statistics, browsing, user info, private messages |
| `client.rooms()` | public and private rooms |
| `client.interests()` | likes, hates, recommendations and similar users |
| `client.account()` | status, privileges, password and shared counts |
| `client.transfers()` | the downloads and uploads under way |
| `client.distributed()` | the client's place in the distributed search tree |

Settings go on an operation before it is awaited. A search returns a stream of responses; a
transfer started with `start` returns a handle:

```rust,no_run
use std::time::Duration;

use soulseek_client::{Client, Error};

async fn follow(client: &Client) -> Result<(), Error> {
  let idle = Duration::from_secs(5);
  let mut responses = client.search("text").idle_timeout(idle).await?;
  let download = client.download("username", "@@share\\song.flac").to_file("song.flac");
  let handle = download.start().await?;

  while let Some(response) = responses.next().await {
    println!("{} has {} files", response.username, response.files.len());
  }
  handle.cancel();
  Ok(())
}
```

### Events

- `client.events()` streams every `Event`, tagged by topic
- A topic's handle streams its own events: `client.rooms().events()`

### Handler

Requests from other users go to the `Handler` set in `Config`:

| Method | Request | Default |
|---|---|---|
| `search` | another user's search | no reply |
| `browse` | the share listing | no reply |
| `folder` | a folder's contents | no reply |
| `user_info` | user info | no reply |
| `upload` | a file | refused as not shared |
| `place_in_queue` | a file's place in the queue | no reply |
| `unasked_offer` | a file sent unasked | refused |
| `accept_connection` | a connection the server asks for | accepted |
| `choose_parent` | a distributed parent | the first candidate |
| `accept_child` | a distributed child | accepted |

- A peer names itself when it connects; `Peer::confirmed` says whether the name is known to be
  the user's

### Configuration

- `Config` holds the handler, the listener, the proxies and the timers
- `config::Limits` bounds what a peer can send, with `soulseek-proto`'s decode limits as
  `Limits::decode`
- Protocol types come from `soulseek_client::soulseek_proto`, re-exported at the version the
  client is built on; the same types from another version of `soulseek-proto` are different
  types

## Crate features

| Feature | Default | Description |
|---|---|---|
| `serde` | off | `Serialize` and `Deserialize` for the plain data types |

- Covered types include `Config`, the events and `search::Search`; the
  [crate documentation](https://docs.rs/soulseek-client) lists them all
- A type that holds an `Error` is `Serialize` only
- A serialized `Config` includes a proxy's password

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT)
at your option. Unless you explicitly state otherwise, any contribution you submit for inclusion
in this crate, as defined in the Apache-2.0 license, is dual licensed as above, without any
additional terms or conditions.
