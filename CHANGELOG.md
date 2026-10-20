# Changelog

All notable changes to gatereach are listed here. Versions follow [Semantic Versioning](https://semver.org/), and while the major version is 0, a minor release may change behavior.

## 0.2.1

### Fixed

- Optional versioned JSON output exposes connection discovery and query outcomes without parsing human messages

## 0.2.0 - 2026-10-17

### Added

- Athena through the AWS SDK, with `--workgroup`, `--output-location` and `--catalog` on `add`
- `--max` on `q` caps what the source reads over the whole command in its own unit: bytes scanned for Athena, read capacity units for DynamoDB, rows examined for MySQL, reply elements for Redis and records for Kafka. stderr reports what was read, and reaching the cap is a normal end
- `--timeout` on `q` caps each run in seconds. MySQL also gets `max_execution_time`, so the server ends a slow read-only SELECT by itself

### Changed

- Help, messages and code comments are in English
- `discover tsh` reports a tsh that can't start as `tsh db ls couldn't run` instead of asking you to log in
- `add` refuses `--host` and `--port` for DynamoDB and Athena instead of dropping them, and tells a missing `--client` apart from an unknown one
- A local build reports its version as `dev`, so it never passes for a release
- `--write` needs `write = true` on the alias, which only you can set by editing conns.toml
- Each run has a default timeout: 30 s for MySQL, 10 s for Redis and 60 s for DynamoDB. `--timeout 0` turns it off
- `--interval` is at least 1 second, and a session runs at most 4 follows
- `env:` secrets must name a `GATEREACH_` variable
- The count on stderr says `(1 row)` for a single record
- tsh tunnels start with new arguments, so 0.2.0 doesn't see a tunnel 0.1 opened. Run `gatereach close all` before you update

### Security

- Read-only MySQL accepts only SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, TABLE and VALUES, and refuses a WITH that leads a write, locking reads, `INTO OUTFILE` and lock or sleep functions
- Read-only Redis bounds whole-collection and range reads by `--limit`, and caps SCAN COUNT at 1000
- Read-only Athena refuses `system.query` passthrough
- `add` refuses a secret another alias uses for a different host, needs `--tls` to send a MySQL or Redis secret to a remote host, and refuses flag-like or control characters in `--host`, `--user` and `--target`
- Secret references are checked when added, and a malformed one is never echoed in errors
- A tunnel port held by another user is refused before any password is sent
- tsh gets `--` before the target
- `--out` refuses symlinks and creates files exclusively. The descriptor reads `complete: false` until the run ends
- `--file` reads only a regular file of at most 1 MiB, and refuses a FIFO without blocking
- Control and bidi characters are escaped in JSON output, errors and `discover`
- Aliases in the status lines of `q`, `open` and `close` have their control characters escaped

### Fixed

- `keychain:` or `env:` with no name is refused
- Duplicate column names no longer collide with a column already named like a suffix, so `id id id_2` becomes `id id_3 id_2`
- A MySQL query cancelled near its end can't kill the next request's query
- `--out` pointing at a file fails before any tunnel opens
- Athena's read-only check handles every line ending in comments
- A positive `--timeout` or `--interval` under a nanosecond is refused instead of turning the limit off
- A MySQL query that returns right on the cancel still gets `KILL QUERY`
- A failed CSV write says `csv write failed` like Arrow does
- A bad flag value prints its error once and exits 2
- A failed sink write is reported once instead of twice
- A signal that lands after a run finished no longer marks it interrupted
- `add` refuses the alias `all`, `--target` without `--tunnel` and an explicit `--port 0`
- `--timeout` and `--interval` refuse 9223372036 seconds and more, as the message says
- A session stuck writing to a reader that stopped reading ends on `close` instead of holding its connection
- A record line cut inside nested JSON is caught as truncated
- `close` waits for the tunnel to free its port, so an `open` right after it starts a new one
- A grandchild that ignores SIGTERM gets SIGKILL even after the tunnel process exits
- An unknown Kafka topic is reported as `no such kafka topic` instead of a lookup failure
- A Kafka sample ends at a compaction gap at the end of a partition instead of waiting forever
- `--timeout` only rewrites deadline errors, so an unrelated error past the deadline keeps its text
- A stopped Athena query's scan counts toward `--max`
- An empty CSV run writes an empty file instead of a blank line
- `close` stops a session right away even while a connection hasn't sent its request yet
- A driver socket timeout right at the run deadline reports `query ran past --timeout`

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
