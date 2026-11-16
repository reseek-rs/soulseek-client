//! Drives `soulseek-client` against other Soulseek clients logged in to one local server.
//!
//! For each peer named on the command line the probe searches, browses, asks for user info and
//! folder contents, and downloads files the peer shares, comparing every byte with the peer's
//! own copy. It shares one file of its own, so a peer told to do so can search, browse and
//! download in the other direction. Each step prints a `PASS` or `FAIL` line and a failed step
//! does not stop the run; the exit code says whether any failed.
//!
//! `run.sh` starts the server and the peers and runs this program. See `README.md`.

mod probe;
mod sharing;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use soulseek_client::{Client, Config, Error, Event};

const USAGE: &str = "usage: interop-driver --server-port PORT --work DIR [--name NAME] \
                     [--listen-port PORT | --no-listener] [--basic] [--linger SECONDS] \
                     [--command-peer PEER] PEER=SHARE_DIR...";

static FAILED: AtomicU32 = AtomicU32::new(0);

/// the port the probe listens on unless told otherwise
const LISTEN_PORT: u16 = 50_411;

/// prints the outcome of one step and counts a failure
pub fn report(peer: &str, step: &str, result: Result<String, String>) {
  match result {
    Ok(detail) => println!("PASS  {peer:<8} {step:<24} {detail}"),
    Err(detail) => {
      FAILED.fetch_add(1, Ordering::Relaxed);
      println!("FAIL  {peer:<8} {step:<24} {detail}");
    }
  }
}

/// A client under test: its username and the files it shares, by file name
pub struct Peer {
  pub name: String,
  pub files: HashMap<String, PathBuf>,
}

impl Peer {
  fn new(name: &str, share: &Path) -> Self {
    let mut files = HashMap::new();
    collect(share, &mut files);
    Self {
      name: name.to_owned(),
      files,
    }
  }
}

/// every file under `directory`, by its name
fn collect(directory: &Path, into: &mut HashMap<String, PathBuf>) {
  let entries = std::fs::read_dir(directory)
    .unwrap_or_else(|error| panic!("cannot read {}: {error}", directory.display()));
  for entry in entries.flatten() {
    let path = entry.path();
    if path.is_dir() {
      collect(&path, into);
    } else {
      into.insert(entry.file_name().to_string_lossy().into_owned(), path);
    }
  }
}

struct Arguments {
  server_port: u16,
  /// `None` runs the probe without a listener, so peers reach it only through the server
  listen_port: Option<u16>,
  work: PathBuf,
  name: String,
  /// stop after the steps every peer gets, leaving out the long transfers
  basic: bool,
  /// how long to stay connected afterwards, serving what peers ask for
  linger: Duration,
  /// a peer that runs commands sent to it as private messages
  command_peer: Option<String>,
  peers: Vec<Peer>,
}

impl Arguments {
  fn parse() -> Result<Self, String> {
    let mut server_port = None;
    let mut listen_port = Some(LISTEN_PORT);
    let mut work = None;
    let mut name = "probe".to_owned();
    let mut basic = false;
    let mut linger = Duration::ZERO;
    let mut command_peer = None;
    let mut peers = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
      let mut value = || args.next().ok_or_else(|| format!("{arg} needs a value"));
      match arg.as_str() {
        "--server-port" => server_port = Some(number(&value()?)?),
        "--listen-port" => listen_port = Some(number(&value()?)?),
        "--no-listener" => listen_port = None,
        "--work" => work = Some(PathBuf::from(value()?)),
        "--name" => name = value()?,
        "--basic" => basic = true,
        "--linger" => linger = Duration::from_secs(number(&value()?)?),
        "--command-peer" => command_peer = Some(value()?),
        peer => {
          let (name, share) = peer
            .split_once('=')
            .ok_or_else(|| format!("unexpected argument {peer}"))?;
          peers.push(Peer::new(name, Path::new(share)));
        }
      }
    }
    Ok(Self {
      server_port: server_port.ok_or("--server-port is required")?,
      listen_port,
      work: work.ok_or("--work is required")?,
      name,
      basic,
      linger,
      command_peer,
      peers,
    })
  }
}

fn number<T: std::str::FromStr>(text: &str) -> Result<T, String> {
  text.parse().map_err(|_| format!("{text} is not a number"))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  let arguments = match Arguments::parse() {
    Ok(arguments) => arguments,
    Err(error) => {
      eprintln!("{error}\n{USAGE}");
      std::process::exit(2);
    }
  };
  // the client's own log lines, at debug unless RUST_LOG says otherwise
  tracing_subscriber::fmt()
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "soulseek_client=debug,soulseek_proto=info".into()),
    )
    .with_writer(std::io::stderr)
    .with_ansi(false)
    .init();

  let shared = arguments.work.join(sharing::FILE_NAME);
  let size = std::fs::metadata(&shared)
    .unwrap_or_else(|error| panic!("cannot read {}: {error}", shared.display()))
    .len();
  let config = Config::builder()
    .server("127.0.0.1", arguments.server_port)
    .listener(arguments.listen_port.is_some())
    .listen_port(arguments.listen_port.unwrap_or(LISTEN_PORT))
    .handler(sharing::Sharing::new(shared, size));
  let client = Client::new(config.build().unwrap());
  tokio::spawn(follow(client.events()));

  client.connect(&arguments.name, "probe-password").await?;
  client.account().set_shared_counts(1, 1).await?;
  println!(
    "connected as {} ({}), sharing {} ({size} bytes)",
    arguments.name,
    if arguments.listen_port.is_some() {
      "listening"
    } else {
      "no listener"
    },
    sharing::REMOTE_NAME
  );

  for peer in &arguments.peers {
    let listing = probe::basic(&client, peer, &arguments.work).await;
    if let (Some(listing), false) = (listing, arguments.basic) {
      probe::transfers(&client, peer, &listing, &arguments.work).await;
    }
  }
  if let Some(peer) = &arguments.command_peer {
    probe::commands(&client, peer, size).await;
  }

  if !arguments.linger.is_zero() {
    println!("serving peers for {} seconds", arguments.linger.as_secs());
    tokio::time::sleep(arguments.linger).await;
  }
  let running = sharing::UPLOADS_RUNNING.load(Ordering::Relaxed);
  if running > 0 {
    report(
      &arguments.name,
      "uploads ended",
      Err(format!("{running} upload(s) still running at exit")),
    );
  }
  client.disconnect().await;
  let failed = FAILED.load(Ordering::Relaxed);
  println!("{} done, {failed} step(s) failed", arguments.name);
  std::process::exit(i32::from(failed > 0));
}

/// prints the private messages peers send and, on stderr, how each transfer moves
async fn follow(mut events: soulseek_client::Events) {
  while let Some(event) = events.next().await {
    let event = match event {
      Ok(event) => event,
      Err(lagged) => {
        eprintln!("{lagged}");
        continue;
      }
    };
    match event {
      Event::PrivateMessage(message) => report(
        &message.username,
        "message to the probe",
        Ok(format!("{:?}", message.message)),
      ),
      Event::Transfer(soulseek_client::transfer::Event::StateChanged { transfer, .. }) => {
        eprintln!(
          "transfer {:?} {} {}: {}",
          transfer.direction, transfer.username, transfer.filename, transfer.state
        );
      }
      _ => {}
    }
  }
}
