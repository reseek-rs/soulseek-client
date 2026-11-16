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
handler it asks, and decides none of them itself. Limits that bound what remote input can make
the client hold stay in the client.

## Usage

```toml
[dependencies]
soulseek-client = "0.2"
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

The protocol's types, such as the `SearchResponse` a search answer holds, are re-exported as
`soulseek_client::soulseek_proto` at the version the client is built on. An application takes
them from there rather than from a separate `soulseek-proto` dependency: at another version its
types have the same names and do not match.

## Layout

`Client` holds the session and what starts something of its own: `connect`, `search`,
`download` and `upload`. Everything else is on a handle for its topic: `client.users()`,
`client.rooms()`, `client.interests()`, `client.account()`, `client.transfers()` and
`client.distributed()`. An operation with settings of its own returns a value to set up and
await, and one that runs for a while gives something to follow it with: a search its stream of
responses, a transfer started with `start` its handle.

## Events and the handler

`Client::events` returns a stream of `Event`s, grouped by topic: the session, users, rooms,
searches, transfers, the distributed network, and private messages. A topic's handle gives a
stream of its events alone, as `client.rooms().events()` does. What a peer asks of the
client (a search, a browse, user info, a file) is answered by the application through the
`Handler` it sets in `Config`. A method it does not implement says nothing for the application:
a search, a browse or a user info request gets no reply, and a request for a file is refused as
not shared.
Other methods let the application choose its parent in the distributed search tree, decide who
joins below it, and refuse a connection the server asks it to make.

The bounds on what a peer can send and the timers an application is likely to tune are settings
of `Config` and `config::Limits`, with soulseek-proto's own decode limits as `Limits::decode`.

The client logs through `tracing` and installs no subscriber.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT)
at your option. Unless you explicitly state otherwise, any contribution you submit for inclusion
in this crate, as defined in the Apache-2.0 license, is dual licensed as above, without any
additional terms or conditions.
