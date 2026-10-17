# Changelog

All notable changes to gatereach are listed here. Versions follow [Semantic Versioning](https://semver.org/), and while the major version is 0, a minor release may change behavior.

## 0.1.1 - 2026-10-17

### Changed

- Release binaries are built with `-trimpath` and carry the commit time, so they hold no build machine paths

### Security

- The launcher fetches over HTTPS only and checks the build attestation when `gh` is logged in
- GoReleaser is pinned and the release job doesn't use the Go cache

### Fixed

- The launcher finds a matching build later on PATH even when its own `bin/` comes first, and never runs one from the working folder
- Two copies of the launcher on PATH no longer ask each other for their version without end
- A relative `GATEREACH_HOME` counts from the working folder, so the launcher's `go install` accepts it

## 0.1.0 - 2026-10-15

First release.

### Added

- Saved connection aliases in `conns.toml`, with secrets only as `keychain:` or `env:` references
- MySQL, Redis, DynamoDB and Kafka through pure Go drivers in one static binary
- Teleport `tsh` tunnels that open on demand and stay until `close`, and `discover tsh` to list reachable databases
- A resident session per alias that holds one connection over a private Unix socket, so repeated queries skip the setup cost
- Best-effort read-only checks, one MySQL statement per request, and `--write` for an explicit write
- `--follow` by polling, or a consumer for Kafka
- `--out` writes a Frictionless Data Package with Arrow or CSV segments
- A Claude Code plugin with one skill and a launcher that fetches the release binary and checks its checksum
