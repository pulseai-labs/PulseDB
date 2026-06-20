# Contributing to PulseDB

Thanks for your interest in PulseDB. This is a pre-1.0 project under active development.

## Ground rules

- **Be respectful and constructive.**
- **Never commit secrets** or private/customer data — see [PUBLIC_BOUNDARY.md](PUBLIC_BOUNDARY.md). Secret scanning + push protection are enabled on this repo.
- **Security issues**: do **not** open a public issue — follow [SECURITY.md](SECURITY.md).

## Licensing of contributions

By submitting a contribution (PR, patch, etc.), you agree it is provided under the **AGPL-3.0-only** license and may also be offered by the maintainers under PulseDB's commercial license — see [LICENSING.md](LICENSING.md). Only contribute code you have the right to license this way.

## Development setup

```bash
git clone https://github.com/pulseai-labs/PulseDB.git
cd PulseDB
cargo build
cargo test
```

Feature flags: `builtin-embeddings`, `sync`, `sync-http`, `sync-websocket` (test the ones your change touches, e.g. `cargo test --features sync`).

## Before opening a PR

Run the same gates CI enforces:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test --doc
```

- Keep PRs focused; describe the change and motivation.
- Add tests for new behavior; update `CHANGELOG.md` under `[Unreleased]`.
- **Do not** include AI-assistant trailers in commit messages (a `commit-msg` hook rejects `Co-Authored-By`/AI-generated markers).

## CI & review

PRs run a full matrix (format/clippy, tests across OS × feature sets, MSRV, coverage, security audit) and must pass before merge. `main` is protected: changes land via PR, force-pushes and deletions are blocked.

## Releases

Releases are maintainer-driven: a `v*` tag triggers a tag-gated, manually-approved crates.io publish. Contributors do not need to touch versioning.
