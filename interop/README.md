# Interoperability session

Runs `soulseek-client` against two other Soulseek clients, [slskd](https://github.com/slskd/slskd)
(built on Soulseek.NET) and [Nicotine+](https://nicotine-plus.org), all logged in to one local
[soulfind](https://github.com/soulfind-dev/soulfind) server. The crate's own test suites cover
its behaviour against soulfind and scripted peers; this session checks that real clients agree
with it.

```sh
just interop
```

The session takes about five minutes and prints one `PASS` or `FAIL` line per step. It exits
with a failure if any step failed. No gate runs it.

## Isolation

`run.sh` moves itself into a new network namespace before it starts anything. The namespace has
a loopback device and a default route into a dummy device that drops what it is sent, and the
script stops if an outside address can be reached. No client in the session can connect to the
Soulseek network, whatever its configuration says.

The namespace needs unprivileged user namespaces, which most Linux distributions allow. The
session does not run on macOS.

## What it checks

A probe built on the crate (`driver/`) logs in and, for each of the two peers:

- asks the server for the peer's status and address;
- sends the peer a private message;
- searches the peer alone and then the whole network for a file the peer shares;
- browses the peer's 3,006 files and checks that each is listed;
- asks for the peer's user info and for the contents of one folder;
- asks for the place in the queue of a file it has not asked for, which no peer answers;
- downloads a file by queueing it and another with a transfer request;
- downloads three files at once from a peer with one upload slot, checks that one waits in the
  peer's queue, and asks for its place there;
- cancels a download part way and resumes it from the bytes on disk;
- has the peer killed part way through a download, waits for it to come back, and resumes.

Every downloaded file is compared byte for byte with the peer's own copy.

The probe shares one file. While it stays connected, each peer is told to browse it, ask for
its user info, search it, and download the file, and the script checks what the peer got. slskd
is told over its HTTP API and also sends the probe a private message. Nicotine+ runs headless
with the plugin in `nicotine-plugin/`, which does what a private message from the probe asks.

The session then runs a second probe without a listener, so that every connection to it goes
through the server's relay, and repeats the short steps and the peers' requests.

## Layout

- `run.sh` isolates the session, makes the shares, starts soulfind and the peers, runs the
  probes and checks what the peers received.
- `driver/` is the probe, a program of its own that depends on the crate by path.
- `nicotine-plugin/` is the Nicotine+ plugin.
- `run/` holds the shares, downloads and logs of the last session. It is made anew each time
  and is ignored by git.

When a step fails, `run/probe.log` and `run/shy.log` hold the crate's log at the debug level,
and `run/slskd.log`, `run/nicotine.log` and `run/soulfind.log` hold the other side's.

## Versions

The clients come from the nixpkgs revision in `flake.lock`, through the `interop` dev shell.
`run.sh` only needs `soulfind`, `slskd`, `nicotine`, `cargo`, `curl`, `ip` and `unshare` on
`PATH`, so it also runs outside Nix where those are installed.
