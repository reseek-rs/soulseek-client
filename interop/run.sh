#!/usr/bin/env bash
# Runs soulseek-client against slskd and Nicotine+ on a local soulfind and prints one PASS or
# FAIL line per step. Everything runs in a network namespace whose only way out is a device that
# drops what it is sent, so no client here can reach the Soulseek network.
#
# Needs soulfind, slskd, nicotine, cargo, curl, ip and unshare on PATH; `nix develop .#interop`
# provides them. See README.md.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
run="$here/run"

server_port=2242
probe_port=50411
slskd_port=50300
slskd_http_port=5030
nicotine_port=2250
slskd_api="http://127.0.0.1:$slskd_http_port/api/v0"

if [[ -z "${INTEROP_ISOLATED:-}" ]]; then
  for tool in soulfind slskd nicotine cargo curl ip unshare; do
    if ! command -v "$tool" > /dev/null; then
      echo "$tool is not on PATH; run this inside \`nix develop .#interop\`" >&2
      exit 1
    fi
  done
  cargo build --quiet --manifest-path "$here/driver/Cargo.toml"
  rm -rf "$run"
  mkdir -p "$run"
  # root inside a new user namespace, which is enough to set up the new network namespace
  INTEROP_ISOLATED=1 exec unshare --map-root-user --net "${BASH_SOURCE[0]}"
fi

driver="$here/driver/target/debug/interop-driver"
failed=0
cd "$run"

result() { # pass|fail, who, step, detail
  local verdict=FAIL
  if [[ "$1" == pass ]]; then
    verdict=PASS
  else
    failed=1
  fi
  printf '%s  %-8s %-24s %s\n' "$verdict" "$2" "$3" "${4:-}"
}

isolate() {
  ip link set lo up
  # soulfind connects a socket to an outside address at startup to learn its own, and gives
  # that address out for every peer. A default route into a dummy device lets the connect
  # succeed and gives the peers an address to be reached on, while packets sent out are dropped
  ip link add dummy0 type dummy
  ip addr add 10.99.0.1/24 dev dummy0
  ip link set dummy0 up
  ip route add default dev dummy0
  if timeout 2 bash -c 'exec 3<>/dev/tcp/1.1.1.1/53' 2> /dev/null; then
    echo "the namespace reaches the outside; stopping" >&2
    exit 1
  fi
}

# the files each peer shares: two small ones, three that take a moment, one large enough to
# interrupt, and many tiny ones in many folders so the listing is a large message
make_shares() {
  local peer album many folder file
  for peer in slskd nicotine; do
    album="share-$peer/${peer^} Artist/${peer^} Album"
    mkdir -p "$album"
    head -c 300000 /dev/urandom > "$album/$peer song.flac"
    head -c 300000 /dev/urandom > "$album/$peer second.flac"
    for file in a b c; do
      head -c 8000000 /dev/urandom > "$album/$peer big $file.flac"
    done
    head -c 30000000 /dev/urandom > "$album/$peer huge.flac"
    for folder in $(seq 60); do
      many="share-$peer/${peer^} Artist/many $folder"
      mkdir -p "$many"
      for file in $(seq 50); do
        echo "$peer $folder $file" > "$many/$peer track $folder-$file.mp3"
      done
    done
  done
  mkdir -p work
  head -c 300000 /dev/urandom > "work/probe song.flac"
}
shared_files=3006

start_soulfind() {
  mkdir -p soulfind
  soulfind -p "$server_port" -d "$run/soulfind/soulfind.db" > soulfind.log 2>&1 &
  echo $! > soulfind.pid
  local _
  for _ in $(seq 100); do
    if (exec 3<> "/dev/tcp/127.0.0.1/$server_port") 2> /dev/null; then
      return
    fi
    sleep 0.1
  done
  echo "soulfind did not start; see $run/soulfind.log" >&2
  exit 1
}

# one upload slot, so downloads asked for together wait in its queue
start_slskd() {
  mkdir -p slskd/app slskd/downloads slskd/incomplete
  slskd --app-dir "$run/slskd/app" --no-https --no-auth --http-port "$slskd_http_port" \
    --slsk-address 127.0.0.1 --slsk-port "$server_port" \
    --slsk-username slskd --slsk-password slskd-password \
    --slsk-listen-port "$slskd_port" --upload-slots 1 \
    --shared "$run/share-slskd" --downloads "$run/slskd/downloads" \
    --incomplete "$run/slskd/incomplete" >> slskd.log 2>&1 &
  echo $! > slskd.pid
  # a peer is killed on purpose part way, which the shell would otherwise announce
  disown
}

# headless, with the plugin that does what a private message asks, and one upload slot
start_nicotine() {
  mkdir -p nicotine/data/plugins nicotine/downloads nicotine/incomplete
  cp -r "$here/nicotine-plugin" nicotine/data/plugins/interop
  cat > nicotine/config << EOF
[server]
server = ('127.0.0.1', $server_port)
login = nicotine
passw = nicotine-password
portrange = ($nicotine_port, $nicotine_port)
auto_connect_startup = True
upnp = False

[transfers]
shared = [('nicotine-share', '$run/share-nicotine')]
downloaddir = $run/nicotine/downloads
incompletedir = $run/nicotine/incomplete
rescanonstartup = True
useupslots = True
uploadslots = 1

[plugins]
enabled = ['interop']
EOF
  nicotine --headless -c "$run/nicotine/config" -u "$run/nicotine/data" >> nicotine.log 2>&1 &
  echo $! > nicotine.pid
  disown
}

# the driver asks for a peer to be killed and started again by leaving a marker in work/
restart_on_request() {
  while sleep 0.2; do
    if [[ -e work/restart-slskd ]]; then
      kill -9 "$(< slskd.pid)" 2> /dev/null || true
      sleep 2
      start_slskd
      rm -f work/restart-slskd
    fi
    if [[ -e work/restart-nicotine ]]; then
      kill -9 "$(< nicotine.pid)" 2> /dev/null || true
      sleep 2
      start_nicotine
      rm -f work/restart-nicotine
    fi
  done
}

stop_everything() {
  local pid_file pid _
  for pid_file in watcher.pid slskd.pid nicotine.pid soulfind.pid; do
    if [[ -e "$pid_file" ]]; then
      pid="$(< "$pid_file")"
      kill "$pid" 2> /dev/null || true
      # slskd takes a few seconds to shut down
      for _ in $(seq 100); do
        kill -0 "$pid" 2> /dev/null || break
        sleep 0.1
      done
      kill -9 "$pid" 2> /dev/null || true
    fi
  done
}

wait_for_peers() {
  local _ state='' shares=''
  for _ in $(seq 120); do
    state="$(curl -s "$slskd_api/server" || true)"
    shares="$(curl -s "$slskd_api/shares" || true)"
    if [[ "$state" == *'"isLoggedIn":true'* && "$shares" == *"\"files\":$shared_files"* ]]; then
      break
    fi
    sleep 0.5
  done
  if [[ "$state" != *'"isLoggedIn":true'* ]]; then
    echo "slskd did not log in; see $run/slskd.log" >&2
    exit 1
  fi
  for _ in $(seq 120); do
    if grep -q "Rescan complete" nicotine.log && grep -q "INTEROP plugin loaded" nicotine.log; then
      # logged in a moment after its shares are scanned
      sleep 3
      return
    fi
    sleep 0.5
  done
  echo "Nicotine+ did not start; see $run/nicotine.log" >&2
  exit 1
}

# runs the driver as `name` with the given flags, and returns once it has probed the peers and
# stays connected to serve them
start_probe() { # name, driver flags...
  local name="$1" _
  shift
  "$driver" --server-port "$server_port" --work "$run/work" --name "$name" \
    --command-peer nicotine "$@" \
    "slskd=$run/share-slskd" "nicotine=$run/share-nicotine" \
    > "$name.out" 2> "$name.log" &
  probe_pid=$!
  for _ in $(seq 1200); do
    if grep -q "^serving peers" "$name.out" 2> /dev/null; then
      return
    fi
    # it ended early
    kill -0 "$probe_pid" 2> /dev/null || return 0
    sleep 0.5
  done
}

# what slskd is told to do with the probe over its HTTP API while the probe stays connected
slskd_requests() { # the probe's name
  local name="$1" size file search=7b0c3c0e-1111-4222-8333-444455556666
  size="$(stat -c %s "work/probe song.flac")"
  file='@@probe\\Probe Artist\\Probe Album\\probe song.flac'
  rm -rf slskd/downloads/*
  curl -s -o "slskd-$name-browse.json" "$slskd_api/users/$name/browse"
  curl -s -o "slskd-$name-info.json" "$slskd_api/users/$name/info"
  curl -s -o /dev/null -X POST -H 'Content-Type: application/json' \
    -d "\"hello $name, from slskd\"" "$slskd_api/conversations/$name"
  curl -s -o /dev/null -X DELETE "$slskd_api/searches/$search"
  curl -s -o /dev/null -X POST -H 'Content-Type: application/json' \
    -d "{\"id\":\"$search\",\"searchText\":\"probe song\"}" "$slskd_api/searches"
  curl -s -o /dev/null -X POST -H 'Content-Type: application/json' \
    -d "[{\"filename\":\"$file\",\"size\":$size}]" \
    "$slskd_api/transfers/downloads/$name"
  # a search takes slskd fifteen seconds to end
  sleep 20
  curl -s -o "slskd-$name-search.json" "$slskd_api/searches/$search/responses"
  curl -s -o "slskd-$name-messages.json" "$slskd_api/conversations/$name/messages"
}

contains() { # who, step, file, pattern
  if grep -q -- "$4" "$3" 2> /dev/null; then
    result pass "$1" "$2"
  else
    result fail "$1" "$2" "$(head -c 300 "$3" 2> /dev/null | tr '\n' ' ')"
  fi
}

saved() { # who, the probe's name, downloads folder
  local got
  got="$(find "$3" -name 'probe song.flac' -print -quit)"
  if [[ -n "$got" ]] && cmp -s "$got" "work/probe song.flac"; then
    result pass "$1" "download from $2" "the bytes saved match"
  else
    result fail "$1" "download from $2" "saved: ${got:-nothing}"
  fi
}

# what each peer got from the probe, once the probe is done
peer_checks() { # the probe's name
  local name="$1"
  contains slskd "browse of $name" "slskd-$name-browse.json" 'probe song.flac'
  contains slskd "info of $name" "slskd-$name-info.json" 'soulseek-client interop probe'
  contains slskd "search finds $name" "slskd-$name-search.json" 'probe song.flac'
  contains slskd "message from $name" "slskd-$name-messages.json" "from $name"
  saved slskd "$name" slskd/downloads
  # the plugin's log lines since this probe's first command
  local log="nicotine-$name.log"
  sed -n "/INTEROP command from $name: browse/,\$p" nicotine.log > "$log"
  contains nicotine "browse of $name" "$log" "INTEROP browsed $name: 1 folders, 1 files"
  contains nicotine "info of $name" "$log" "INTEROP info $name: 'soulseek-client interop probe'"
  contains nicotine "search finds $name" "$log" "INTEROP found $name: .*probe song.flac"
  saved nicotine "$name" nicotine/downloads
  rm -rf nicotine/downloads/* nicotine/incomplete/*
}

probe() { # name, driver flags...
  local name="$1"
  start_probe "$@"
  slskd_requests "$name"
  wait "$probe_pid" || failed=1
  cat "$name.out"
  peer_checks "$name"
}

isolate
# for a run that stops early; one that ends stops everything itself
trap stop_everything EXIT
make_shares
start_soulfind
start_slskd
start_nicotine
restart_on_request &
echo $! > watcher.pid
wait_for_peers

echo "--- a probe that peers can dial"
probe probe --listen-port "$probe_port" --linger 35
echo "--- a probe without a listener, reached only through the server"
probe shy --no-listener --basic --linger 35

if [[ "$failed" == 0 ]]; then
  echo "every step passed"
else
  echo "some steps failed; the logs are in $run"
fi
stop_everything
trap - EXIT
exit "$failed"
