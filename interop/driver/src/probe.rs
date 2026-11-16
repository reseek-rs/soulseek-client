//! The steps run against each peer. A peer shares, under any folders, the files `run.sh` makes
//! for it: `NAME song.flac` and `NAME second.flac`, three `NAME big ?.flac`, `NAME huge.flac`,
//! and many small ones.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use soulseek_client::ErrorKind;
use soulseek_client::search::Scope;
use soulseek_client::soulseek_proto::RemoteFilename;
use soulseek_client::soulseek_proto::peer::BrowseResponse;
use soulseek_client::transfer::{Download, Handle, RequestStyle, State};
use soulseek_client::{Client, Error};

use crate::{Peer, report, sharing};

/// how long one download may take before the step fails
const PATIENCE: Duration = Duration::from_secs(120);

/// A file as a peer lists it: its full remote name and its size
type Listed = (RemoteFilename, u64);

/// The steps that need no long transfer: status, address, a private message, searches, browse,
/// user info, folder contents, a place in the queue and one small download. Returns the peer's
/// listing.
pub async fn basic(client: &Client, peer: &Peer, work: &Path) -> Option<BrowseResponse> {
  let name = peer.name.as_str();
  let song = format!("{name} song.flac");
  let debug = |error: Error| format!("{error:?}");

  report(
    name,
    "status",
    client
      .users()
      .status(name)
      .await
      .map(|status| format!("{:?}", status.presence))
      .map_err(debug),
  );
  report(
    name,
    "address",
    client
      .users()
      .address(name)
      .await
      .map(|address| address.to_string())
      .map_err(debug),
  );

  let own = client.username().unwrap_or_default();
  report(
    name,
    "private message",
    client
      .users()
      .message(name, &format!("hello {name}, from {own}"))
      .await
      .map(|()| "sent".into())
      .map_err(debug),
  );

  // a search of the one user, then of everyone
  let mut found = None;
  for (step, scope) in [
    ("search of the user", Scope::Users(vec![name.to_owned()])),
    ("search of the network", Scope::Network),
  ] {
    let searched = async {
      let responses = client
        .search(format!("{name} song"))
        .scope(scope)
        .idle_timeout(Duration::from_secs(4))
        .await?;
      Ok::<_, Error>(responses.all().await)
    };
    let result = searched.await.map_err(debug).and_then(|(_, responses)| {
      let file = responses
        .iter()
        .filter(|response| response.username == name)
        .flat_map(|response| &response.files)
        .find(|file| file.filename.as_str().ends_with(&song))
        .ok_or_else(|| {
          format!(
            "{} responses, none from {name} with {song}",
            responses.len()
          )
        })?;
      found.get_or_insert((file.filename.clone(), file.size));
      Ok(format!("{} ({} bytes)", file.filename, file.size))
    });
    report(name, step, result);
  }

  let listing = client.users().browse(name).await;
  let folder = listing.as_ref().ok().and_then(|listing| {
    let holds = |file: &soulseek_client::soulseek_proto::peer::File| file.filename.as_str() == song;
    listing
      .directories
      .iter()
      .find(|directory| directory.files.iter().any(holds))
      .map(|directory| directory.name.clone())
  });
  report(
    name,
    "browse",
    match (&listing, &folder) {
      (Ok(listing), Some(folder)) => {
        let files: usize = listing
          .directories
          .iter()
          .map(|directory| directory.files.len())
          .sum();
        if files == peer.files.len() {
          Ok(format!(
            "{} folders, {files} files, {song} in {folder}",
            listing.directories.len()
          ))
        } else {
          Err(format!("{files} files listed, {} shared", peer.files.len()))
        }
      }
      (Ok(_), None) => Err(format!("{song} is not in the listing")),
      (Err(error), _) => Err(format!("{error:?}")),
    },
  );

  report(
    name,
    "user info",
    client
      .users()
      .info(name)
      .await
      .map(|info| {
        format!(
          "description {:?}, {} slots, queue of {}",
          info.description, info.upload_slots, info.queue_length
        )
      })
      .map_err(debug),
  );

  if let Some(folder) = folder {
    let contents = client.users().folder(name, folder).await;
    report(
      name,
      "folder contents",
      contents.map_err(debug).and_then(|folders| {
        let files: Vec<_> = folders.iter().flat_map(|folder| &folder.files).collect();
        if files.iter().any(|file| file.filename.as_str() == song) {
          Ok(format!("{} files", files.len()))
        } else {
          Err(format!("{song} is not among {} files", files.len()))
        }
      }),
    );
  }

  let Some(file) = found else {
    report(
      name,
      "download",
      Err("no search response to download from".into()),
    );
    return listing.ok();
  };
  report(
    name,
    "place of a file not asked",
    match client
      .transfers()
      .place_in_queue(name, file.0.clone())
      .await
    {
      // neither slskd nor Nicotine+ answers for a file that is not in its queue
      Err(matched) if matched.kind() == ErrorKind::NoResponse => Ok("no answer".into()),
      Ok(place) => Err(format!("answered {place:?} for a file that is not queued")),
      Err(error) => Err(format!("{error:?}")),
    },
  );
  let into = work.join(format!("{name} song.download"));
  report(
    name,
    "download",
    fetch(client, peer, file, &into, 0, |download| download).await,
  );
  listing.ok()
}

/// The longer transfers: a download asked for with a transfer request, three at once that wait
/// in the peer's queue, one canceled part way and resumed, and one the peer dies under, resumed
/// once the peer is back.
pub async fn transfers(client: &Client, peer: &Peer, listing: &BrowseResponse, work: &Path) {
  let name = peer.name.as_str();
  let mut listed: HashMap<&str, Listed> = HashMap::new();
  for directory in &listing.directories {
    for file in &directory.files {
      let full = format!("{}\\{}", directory.name, file.filename);
      listed.insert(file.filename.as_str(), (full.as_str().into(), file.size));
    }
  }
  let pick = |file: &str| {
    listed
      .get(file)
      .cloned()
      .ok_or_else(|| format!("{file} is not in the listing"))
  };

  let step = "download by request";
  match pick(&format!("{name} second.flac")) {
    Ok(file) => {
      let into = work.join(format!("{name} second.download"));
      let fetched = fetch(client, peer, file, &into, 0, |download| {
        download.request_style(RequestStyle::TransferRequest)
      });
      report(name, step, fetched.await);
    }
    Err(missing) => report(name, step, Err(missing)),
  }

  // three at once from a peer that sends one at a time, slowed so the others are seen waiting
  let mut together = Vec::new();
  for letter in ["a", "b", "c"] {
    let step = format!("three at once ({letter})");
    let file = match pick(&format!("{name} big {letter}.flac")) {
      Ok(file) => file,
      Err(missing) => {
        report(name, &step, Err(missing));
        continue;
      }
    };
    let into = work.join(format!("{name} big {letter}.download"));
    together.push(async move {
      let slowed = |download| slow(download, 5);
      report(
        name,
        &step,
        fetch(client, peer, file, &into, 0, slowed).await,
      );
    });
  }
  let queue = async {
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let waiting = client
      .transfers()
      .downloads()
      .into_iter()
      .find(|transfer| transfer.username == name && transfer.state == State::QueuedRemotely);
    let Some(waiting) = waiting else {
      return report(
        name,
        "queued at the peer",
        Err("no download waits in the peer's queue".into()),
      );
    };
    report(name, "queued at the peer", Ok(waiting.filename.to_string()));
    report(
      name,
      "place of a queued file",
      client
        .transfers()
        .place_in_queue(name, waiting.filename)
        .await
        .map(|place| format!("{place:?}"))
        .map_err(|error| format!("{error:?}")),
    );
  };
  tokio::join!(join_all(together), queue);

  let file = match pick(&format!("{name} huge.flac")) {
    Ok(file) => file,
    Err(missing) => return report(name, "cancel part way", Err(missing)),
  };
  let into = work.join(format!("{name} huge.download"));

  // canceled part way, then taken up from the bytes on disk
  let (ended, on_disk) = interrupted(client, peer, &file, &into, Handle::cancel).await;
  let part_way = on_disk > 0 && on_disk < file.1;
  report(
    name,
    "cancel part way",
    match ended {
      Some(Err(error)) if error.is_canceled() && part_way => Ok(format!("{on_disk} bytes on disk")),
      other => Err(format!("{on_disk} bytes on disk, ended with {other:?}")),
    },
  );
  if part_way {
    // the peer needs a moment to let the canceled transfer go
    tokio::time::sleep(Duration::from_secs(2)).await;
    report(
      name,
      "resume",
      fetch(client, peer, file.clone(), &into, on_disk, |download| {
        download
      })
      .await,
    );
  }

  // the peer is killed part way and started again: run.sh does both when the marker appears
  let marker = work.join(format!("restart-{name}"));
  let kill = |_: &Handle| std::fs::write(&marker, b"").expect("the restart marker can be written");
  let (ended, on_disk) = interrupted(client, peer, &file, &into, kill).await;
  let part_way = on_disk > 0 && on_disk < file.1;
  report(
    name,
    "peer killed mid file",
    match ended {
      Some(Err(error)) if part_way => Ok(format!("{on_disk} bytes on disk, ended with {error}")),
      None if part_way => Ok(format!(
        "{on_disk} bytes on disk, the download did not end once interrupted"
      )),
      other => Err(format!("{on_disk} bytes on disk, ended with {other:?}")),
    },
  );
  let back = async {
    loop {
      tokio::time::sleep(Duration::from_secs(1)).await;
      if client.users().info(name).await.is_ok() {
        break;
      }
    }
  };
  let returned = tokio::time::timeout(Duration::from_secs(90), back)
    .await
    .is_ok();
  report(
    name,
    "peer back",
    if returned {
      Ok("answering again".into())
    } else {
      Err("not answering 90 seconds later".into())
    },
  );
  if returned && part_way {
    // the peer may still be scanning its shares, and refuses the file until it is done
    let mut result = Err("not tried".to_owned());
    for _ in 0..10 {
      result = fetch(client, peer, file.clone(), &into, on_disk, |download| {
        download
      })
      .await;
      if result.is_ok() {
        break;
      }
      tokio::time::sleep(Duration::from_secs(3)).await;
    }
    report(name, "resume after restart", result);
  }
}

/// Tells `peer`, a Nicotine+ running the plugin in `nicotine-plugin/`, to browse the probe, ask
/// for its user info, search it and download its file. `run.sh` checks what the peer got.
pub async fn commands(client: &Client, peer: &str, size: u64) {
  let commands = [
    "!browse".to_owned(),
    "!info".to_owned(),
    "!search probe song".to_owned(),
    format!("!get {}|{size}", sharing::REMOTE_NAME),
  ];
  for command in commands {
    let sent = client.users().message(peer, &command).await;
    report(
      peer,
      "command",
      sent.map(|()| command).map_err(|error| format!("{error:?}")),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
  }
}

/// downloads `file` into `into` from byte `offset`, as `shape` sets it up, and compares what
/// arrived with the peer's own copy
async fn fetch(
  client: &Client,
  peer: &Peer,
  file: Listed,
  into: &Path,
  offset: u64,
  shape: impl FnOnce(Download) -> Download,
) -> Result<String, String> {
  let local = local_copy(peer, &file.0)?;
  if offset == 0 {
    // a file left by an earlier step must not count as this one's
    let _ = std::fs::remove_file(into);
  }
  let download = shape(
    client
      .download(peer.name.as_str(), file.0)
      .size(file.1)
      .start_offset(offset)
      .to_file(into),
  );
  let transfer = tokio::time::timeout(PATIENCE, download)
    .await
    .map_err(|_| format!("not done within {} seconds", PATIENCE.as_secs()))?
    .map_err(|error| format!("{error:?}"))?;
  let got = std::fs::read(into).map_err(|error| format!("the output cannot be read: {error}"))?;
  let want = std::fs::read(&local).map_err(|error| format!("{}: {error}", local.display()))?;
  if got != want {
    return Err(format!(
      "{} bytes arrived and {} were expected, or they differ",
      got.len(),
      want.len()
    ));
  }
  Ok(match offset {
    0 => format!("{} bytes match, {}", got.len(), transfer.state),
    _ => format!(
      "{} bytes match, from byte {offset}, {}",
      got.len(),
      transfer.state
    ),
  })
}

/// Starts a slowed download of `file` into `into`, runs `interrupt` once two megabytes arrived,
/// and returns how the download ended, `None` when it did not end once interrupted, and how
/// many bytes it left on disk
async fn interrupted(
  client: &Client,
  peer: &Peer,
  file: &Listed,
  into: &Path,
  interrupt: impl FnOnce(&Handle),
) -> (Option<Result<(), Error>>, u64) {
  let _ = std::fs::remove_file(into);
  let download = client
    .download(peer.name.as_str(), file.0.clone())
    .size(file.1)
    .to_file(into);
  let ended = async {
    let mut download = match slow(download, 2).start().await {
      Ok(download) => download,
      Err(error) => return Some(Err(error)),
    };
    let mut changes = download.changes();
    let progress = async {
      while let Some(transfer) = changes.next().await {
        if transfer.bytes_transferred >= 2_000_000 {
          break;
        }
      }
    };
    tokio::select! {
      ended = &mut download => Some(ended),
      () = progress => {
        interrupt(&download);
        tokio::time::timeout(PATIENCE, download).await.ok()
      }
    }
  };
  let ended = ended.await;
  let on_disk = std::fs::metadata(into).map_or(0, |metadata| metadata.len());
  (ended.map(|ended| ended.map(|_| ())), on_disk)
}

/// the peer's own copy of the file it lists as `remote`
fn local_copy(peer: &Peer, remote: &RemoteFilename) -> Result<std::path::PathBuf, String> {
  let name = remote.as_str().rsplit('\\').next().unwrap_or_default();
  peer
    .files
    .get(name)
    .cloned()
    .ok_or_else(|| format!("{name} is not a file the peer shares"))
}

/// `download` let through 16 KiB every `pause` milliseconds
fn slow(download: Download, pause: u64) -> Download {
  download.governor(move |_, wanted| async move {
    tokio::time::sleep(Duration::from_millis(pause)).await;
    wanted.min(16 * 1024)
  })
}

/// runs the futures together, to the end of each
async fn join_all(futures: Vec<impl Future<Output = ()>>) {
  let mut futures: Vec<_> = futures.into_iter().map(Box::pin).collect();
  std::future::poll_fn(|context| {
    futures.retain_mut(|future| future.as_mut().poll(context).is_pending());
    if futures.is_empty() {
      std::task::Poll::Ready(())
    } else {
      std::task::Poll::Pending
    }
  })
  .await;
}
