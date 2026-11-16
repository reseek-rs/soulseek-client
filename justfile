default:
    @just --list

# cargo nextest without and with `serde`, then the doc examples nextest does not run
test:
    cargo nextest run
    cargo nextest run --all-features
    cargo test --all-features --doc
    @[ -n "$({{ just_executable() }} --justfile {{ justfile() }} soulfind-bin)" ] || echo "soulfind not found at SOULFIND_BIN or on PATH: the soulfind suites were skipped"

# formatting, clippy without and with `serde`, and the docs as docs.rs builds them, then the
# same for the interop harness. `__testing` turns `missing_docs` off, so clippy also runs on the
# library without it
lint:
    cargo fmt --all --check
    cargo clippy --all-targets -- -D warnings
    cargo clippy --all-features --all-targets -- -D warnings
    cargo clippy -- -D warnings
    cargo clippy --features serde -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
    cargo fmt --check --manifest-path interop/driver/Cargo.toml
    cargo clippy --manifest-path interop/driver/Cargo.toml -- -D warnings
    shellcheck interop/run.sh

# the library alone on the manifest's rust-version, without and with `serde`
msrv:
    CARGO_TARGET_DIR=target/msrv nix develop .#msrv -c cargo build --locked
    CARGO_TARGET_DIR=target/msrv nix develop .#msrv -c cargo build --locked --features serde

# the gate for every push; ci.yml runs it
ci: lint test msrv

# the crate against slskd and Nicotine+ on a local soulfind, cut off from the network; no gate
# runs it. See interop/README.md
interop:
    nix develop .#interop -c ./interop/run.sh

# package and build the crate as crates.io would, without uploading; release.yml runs it
publish-check:
    cargo publish --dry-run

# prints the soulfind to test against: SOULFIND_BIN, else soulfind on PATH, else nothing, which
# the callers take as a skip; with SOULFIND_REQUIRED=1 a missing soulfind fails instead
[private]
soulfind-bin:
    #!/usr/bin/env bash
    set -euo pipefail
    soulfind="${SOULFIND_BIN:-$(command -v soulfind || true)}"
    if [ -z "$soulfind" ] && [ "${SOULFIND_REQUIRED:-}" = 1 ]; then
      echo "SOULFIND_REQUIRED=1 but soulfind is neither at SOULFIND_BIN nor on PATH" >&2
      exit 1
    fi
    echo "$soulfind"
