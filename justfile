default:
    @just --list

# cargo nextest, then the doc examples nextest does not run
test:
    cargo nextest run --all-features
    cargo test --all-features --doc
    @[ -n "$({{ just_executable() }} --justfile {{ justfile() }} soulfind-bin)" ] || echo "soulfind not found at SOULFIND_BIN or on PATH: the soulfind suites were skipped"

# formatting and clippy
lint:
    cargo fmt --all --check
    cargo clippy --all-features --all-targets -- -D warnings

# the library alone on the manifest's rust-version
msrv:
    CARGO_TARGET_DIR=target/msrv nix develop .#msrv -c cargo build --locked

# the gate for every push; ci.yml runs it
ci: lint test msrv

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
