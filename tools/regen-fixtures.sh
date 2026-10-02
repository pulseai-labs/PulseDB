#!/usr/bin/env bash
# Regenerate ALL real prior-release golden fixtures + their provenance manifests.
#
# Builds the three workspace-OUTSIDE generator crates against the PUBLISHED
# pulsehive-db =0.7.0 / =0.5.1 / =0.4.0 (resolved from crates.io), runs them to write the
# frozen `.redb` blobs into tests/fixtures/, strips the derived HNSW sidecar dir,
# and layers mechanical provenance onto each manifest via finalize_manifest.py.
#
# ISOLATION (audit C9): each generator is its own workspace root (empty
# [workspace] in its Cargo.toml) and is built ONLY via an explicit
# `--manifest-path`, so the production root Cargo.toml / lockfile / build graph
# are never touched. The build target dir defaults OUTSIDE the repo so the
# worktree stays clean; override with CARGO_TARGET_DIR.
#
# NOTE: regenerating produces NEW UUIDv7 ids + timestamps (reproducible-with-drift)
# and therefore a NEW blob_sha256 — the committed blob + manifest are a matched,
# FROZEN pair. This script is committed for PROVENANCE and is NOT run in CI.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$here" rev-parse --show-toplevel)"
fixtures_dir="$repo_root/tests/fixtures"
mkdir -p "$fixtures_dir"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/pulsedb-fixgen-target}"
git_commit="$(git -C "$repo_root" rev-parse HEAD)"
rustc_version="$(rustc --version)"
cargo_version="$(cargo --version)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

gen_one() {
  local crate_dir="$1" version="$2"
  local manifest="$here/$crate_dir/Cargo.toml"
  local blob="$fixtures_dir/real-v${version}.redb"
  local content="$tmp/real-v${version}.content.json"
  local out_manifest="$fixtures_dir/real-v${version}.manifest.json"
  local lock="$here/$crate_dir/Cargo.lock"

  echo ">> [$version] building + running $crate_dir (target: $CARGO_TARGET_DIR)"
  cargo run --release --manifest-path "$manifest" -- "$blob" "$content"

  # The HNSW index is rebuilt from redb on every open (issue #18) — never freeze it.
  rm -rf "${blob}.hnsw"

  echo ">> [$version] finalizing manifest"
  python3 "$here/finalize_manifest.py" \
    "$content" "$blob" "$lock" "$out_manifest" \
    "$git_commit" "$rustc_version" "$cargo_version"
}

gen_one "fixture-gen-v0_7_0" "0.7.0"
gen_one "fixture-gen-v0_5_1" "0.5.1"
gen_one "fixture-gen-v0_4_0" "0.4.0"

echo ">> done. Frozen fixtures + manifests:"
ls -l "$fixtures_dir"/real-v0.7.0.redb "$fixtures_dir"/real-v0.5.1.redb "$fixtures_dir"/real-v0.4.0.redb \
      "$fixtures_dir"/real-v0.7.0.manifest.json "$fixtures_dir"/real-v0.5.1.manifest.json "$fixtures_dir"/real-v0.4.0.manifest.json
