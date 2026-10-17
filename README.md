<h1 align="center">gatereach</h1>

<p align="center">Let your agent query the databases it can't reach on its own, through saved aliases, read only by default</p>

<p align="center">
  <a href="https://github.com/jeon-jihyeon/gatereach/blob/main/LICENSE"><img alt="MIT" src="https://img.shields.io/badge/license-MIT-blue"></a>
</p>

The data an agent needs often sits behind a Teleport tunnel, a password in your Keychain, or an AWS profile. Left alone, an agent improvises: it pastes credentials into commands, leaves tunnels open on random ports, and parses five different CLI output formats. gatereach gives it one command instead.

- Connections are saved once as aliases. Passwords stay in the macOS Keychain or an environment variable, and the config file only holds a reference
- Tunnels open on demand and stay open until you close them, and a session per alias holds the database connection, so the next query skips the setup
- MySQL, Redis, DynamoDB, Kafka and Athena all come out as one JSON object per record, with NULL kept apart from the string "NULL"
- Writes are blocked unless the alias allows them in the config and you pass `--write`
- Results can be saved as a [Frictionless Data Package](https://datapackage.org/) with Arrow or CSV data, so any Data Package reader can pick them up

## Quickstart

gatereach runs on macOS and Linux as one binary with the database drivers built in. On first run the plugin's launcher downloads the release for your platform over HTTPS and checks it against the release checksums, and against its build attestation with `gh attestation verify` when `gh` is installed and logged in. It builds it with Go when the download isn't possible. When neither works, it runs a `gatereach` on your PATH that reports the same version. The binary lives in `~/.gatereach`, or in `GATEREACH_HOME` when it's set. A relative `GATEREACH_HOME` counts from the working folder.

Claude Code:

```
/plugin marketplace add jeon-jihyeon/gatereach
/plugin install gatereach@gatereach
```

The plugin puts `gatereach` on the agent's PATH and ships a skill that tells the agent when and how to use it. Then ask in plain words, like "how many orders are pending in orders-staging?"

Terminal:

```
go install github.com/jeon-jihyeon/gatereach/cmd/gatereach@latest
gatereach ls
```

Or download an archive from the [releases](https://github.com/jeon-jihyeon/gatereach/releases) and put `gatereach` on your PATH.

## Prerequisites

| For | Needs |
|---|---|
| Teleport tunnels | `tsh`, logged in, or Teleport Connect |
| DynamoDB and Athena | AWS credentials from a profile or the default chain, as the AWS CLI finds them |
| `keychain:` secrets | macOS and its `security` command |

MySQL, Redis, DynamoDB, Kafka and Athena clients are built in, so no database client needs to be installed. `tsh` can be set in the config file when it isn't on PATH.

## Configuration

Connections live in `$XDG_CONFIG_HOME/gatereach/conns.toml`, or `~/.config/gatereach/conns.toml` when `XDG_CONFIG_HOME` isn't set. `GATEREACH_CONFIG` points to another file. `gatereach add` appends to it and leaves your comments alone.

```toml
# MySQL through a Teleport tunnel
[conns.orders-staging]
client = "mysql"
host = "127.0.0.1"
port = 26344
user = "viewer"
database = "orders"

[conns.orders-staging.tunnel]
type = "tsh"
target = "orders-staging"

# Redis reached directly
[conns.cache-local]
client = "redis"
host = "127.0.0.1"
port = 6379
secret = "keychain:cache-local"

# DynamoDB has no host or port
[conns.events-ddb]
client = "dynamodb"
profile = "dev"
region = "us-west-2"

# Athena needs a workgroup and has no host or port
[conns.acme-athena]
client = "athena"
profile = "dev"
region = "us-east-1"
workgroup = "primary"
database = "acme"
output_location = "s3://example-bucket/athena-results/"

# tsh path and home, only when the defaults don't fit
# home sets TELEPORT_HOME, so tsh uses the profile Teleport Connect keeps
[tunnels.tsh]
home = "~/Library/Application Support/Teleport Connect/tsh"
```

[conns.example.toml](conns.example.toml) has every key, Kafka with SASL included.

A secret is a reference, never the password:

- `keychain:NAME` reads the macOS Keychain item whose service is `NAME`, for example one made with `security add-generic-password -s NAME -a "$USER" -w`
- `env:GATEREACH_NAME` reads an environment variable. The name must start with `GATEREACH_`, so a reference can't pull an unrelated variable such as a cloud token out of the environment

The value goes straight into the built-in driver in memory, so it never shows up in `ps`, a file or a log.

### Allowing writes

`--write` works only on an alias whose connection says `write = true`. Add it by hand to the connections you mean to write to:

```toml
[conns.orders-dev]
client = "mysql"
port = 3306
user = "app"
write = true
```

Without it, `--write` fails with `orders-dev doesn't allow --write`. Keep it off for anything an agent shouldn't change.

## Commands

| Command | Does |
|---|---|
| `gatereach ls` | Lists aliases with client, address or AWS profile, user, tunnel, whether the port answers and whether a session is live |
| `gatereach q ALIAS QUERY` | Runs a query, opening the tunnel and starting the session first if needed. `gatereach q ALIAS --file PATH` reads the query from a file instead |
| `gatereach open ALIAS` | Opens the tunnel and starts the session without querying |
| `gatereach close ALIAS` or `close all` | Ends the session and closes tunnels gatereach opened. Tunnels another program opened are left alone |
| `gatereach discover tsh` | Lists Teleport databases you can reach, marking which ones are `new` and which are already `added` |
| `gatereach add ALIAS ...` | Appends a connection to the config file |

What a query looks like per source:

| Client | Query |
|---|---|
| `mysql` | one SQL statement |
| `redis` | one Redis command such as `HGETALL user:1` |
| `dynamodb` | a PartiQL statement |
| `kafka` | a topic name, or `topics` for topic metadata |
| `athena` | one SQL statement |

A file is read only when you name it with `--file`, so a query is never mistaken for a path. Put `--` before a query that starts with a dash, like `gatereach q ALIAS -- "-x"`.

`q` flags:

| Flag | Does |
|---|---|
| `--file PATH` | Reads the query from a file instead of the argument |
| `--limit N` | Caps records per run, 1000 by default |
| `--max AMOUNT` | Caps what the source reads over the whole command, in its own unit. See [Budget and timeout](#budget-and-timeout) |
| `--timeout S` | Seconds each run may take, fractions allowed. 30 for MySQL, 10 for Redis and 60 for DynamoDB by default. 0 means no limit |
| `--write` | Lifts the read-only guard on an alias with `write = true`. Kafka refuses it |
| `--follow` | Keeps printing new records until stopped |
| `--interval S` | Seconds between polls with `--follow`, 5 by default and at least 1 |
| `--key FIELD` | Field that identifies a record when polling, so a changed row isn't printed twice |
| `--out DIR` | Saves a Data Package into a new or empty folder |
| `--format arrow` or `csv` | Data file format for `--out`, `arrow` by default |

`add` flags:

| Flag | Does |
|---|---|
| `--client` | `mysql`, `redis`, `dynamodb`, `kafka` or `athena`, required |
| `--host`, `--port` | Address, `127.0.0.1` by default. A direct connection needs `--port` |
| `--tunnel tsh --target NAME` | Reaches the database through `tsh proxy db`. Needs `--user`, and picks a fixed port between 20000 and 31999 from the target and user unless you pass `--port` |
| `--user`, `--database` | Database user, and the default database for MySQL or Athena |
| `--secret` | `keychain:NAME` or `env:GATEREACH_NAME` |
| `--tls` | TLS for MySQL, Redis and Kafka. Kafka with a secret always uses TLS |
| `--profile`, `--region` | AWS profile and region for DynamoDB and Athena |
| `--workgroup` | Athena workgroup, required for Athena |
| `--output-location` | Athena result location, an `s3://` URL. Left out, the workgroup's own setting applies |
| `--catalog` | Athena data catalog, `AwsDataCatalog` by default |

```
gatereach discover tsh
gatereach add orders-staging --client mysql --user viewer --tunnel tsh --target orders-staging
gatereach add cache-local --client redis --port 6379 --secret keychain:cache-local
gatereach add events-kafka --client kafka --host broker.example.com --port 9092 --user api-key --secret env:GATEREACH_EVENTS_KAFKA
gatereach add events-ddb --client dynamodb --profile dev --region us-west-2
gatereach add acme-athena --client athena --profile dev --region us-east-1 --workgroup primary --database acme
```

`discover` lists MySQL and Redis databases with their client and shows `-` for protocols gatereach can't query. `add` takes a tunnel for any client but DynamoDB and Athena.

A tunnel and a session keep running after gatereach exits. The session holds one connection per alias and serves queries over a Unix socket in a folder only your user can open. So the next query skips the TLS and login round trips, and Teleport records one session instead of one per query.

Their logs go to `logs/ALIAS-HASH.log` and `logs/ALIAS-HASH.session.log` next to the config file. `ALIAS` is the alias with every run of characters other than letters, digits, `.`, `-` and `_` replaced by one `_`. `HASH` is the alias's crc32, so two aliases never share a log.

## Output

Without `--out`, each record is one JSON line on stdout as it arrives, and stderr ends with the count. It reads `(1 row)` for one record, and `(N rows, stopped by a failure)` when the run failed:

```
$ gatereach q orders-staging "SELECT id, status, paid_at FROM orders LIMIT 2"
{"id":1,"status":"paid","paid_at":"2026-01-02 10:00:00"}
{"id":2,"status":"pending","paid_at":null}
(2 rows)
```

How each source maps to records:

| Client | Record |
|---|---|
| `mysql` | One row, typed by column: integers and floats as numbers, JSON columns parsed, DECIMAL kept as a string for its precision, dates and times as the server's text, binary columns as base64 |
| `redis` | A map reply is one record, an array gives `index` and `value` per element, and anything else is one `value` |
| `dynamodb` | One item with type tags removed. Numbers stay strings to keep their precision |
| `kafka` | One message with topic, partition, offset, timestamp, key, value and headers |
| `athena` | One row: integer columns as numbers, booleans as `true` or `false`, NULL as `null`, and everything else, decimals, floats and dates included, as the text Athena returns |

With `--out DIR`, stdout prints only the path of `DIR/datapackage.json`, not the records:

```
DIR/datapackage.json   descriptor with the source, query, time and a typed Table Schema per file
DIR/result.arrows      Arrow IPC stream, written in batches of up to 1000 records or every second
DIR/result-2.arrows    started when records change shape, so nothing is dropped
```

- `--format arrow` writes an Arrow IPC stream. Columns with nested or mixed values use the canonical `arrow.json` extension type, and the source and query are also in the schema metadata, so the file still says where it came from on its own
- `--format csv` writes `result.csv` with `\N` as NULL, declared in the schema's `missingValues`. A string that is literally `\N` reads back as NULL too, so use Arrow when that matters. Nested values are JSON text. In a column that mixes strings and other kinds, strings are written as they are, so the string `"1"` and the number `1` look the same in CSV while Arrow keeps them apart
- CSV cells keep the source's values exactly and aren't sanitized against spreadsheet formulas. A cell that starts with `=`, `+`, `-` or `@` may run as a formula in a spreadsheet, so take care opening a CSV of untrusted data that way
- The `gatereach` key in `datapackage.json` records the alias, client, user, query, limit, flags, start time, and whether the run is `complete`. The descriptor is written with `"complete": false` when the run starts and replaced in one rename at the end, so a folder that's still being written or was cut off by a crash never looks complete
- If the query fails, or a run without `--follow` is interrupted, gatereach still writes what it received, sets `"complete": false` with the cause in `error`, and exits non-zero, 130 for an interrupt. Stopping a `--follow` run with Ctrl-C or SIGTERM is a normal end, exits 0 and stays complete
- If writing the files fails, the package is still marked incomplete and gatereach exits non-zero
- Saved results are private to your user: the folder is created with mode 0700 and each file with 0600
- `--out` refuses a symlink, and every file is created new, so a link planted in the folder can't redirect the results

## Following

`--follow` keeps the query going until you stop it.

- Kafka keeps its consumer open: it first prints up to `--limit` recent records from each partition, then every new message
- Every other source, and Kafka `topics`, reruns the query every `--interval` seconds with `--limit` applied to each run, and prints only records it hasn't seen, compared by `--key` or by the whole record
- DynamoDB and Athena bill each rerun by what it reads, so narrow the query to a key or a partition before following it. `--max` puts a cap on the total

## Budget and timeout

`--max` caps what the source reads over the whole command, every `--follow` run included. Each source counts in its own unit and reports it once the work is done, so the run that reaches the cap still prints what it paid for, and nothing runs after it.

| Client | `--max` counts | Form |
|---|---|---|
| `athena` | bytes scanned, failed queries included | bytes, or `KB`, `MB`, `GB` or `TB` in powers of 1000, such as `10GB` |
| `dynamodb` | read capacity units, per page | a number above 0, such as `2.5` |
| `mysql` | rows examined by the query | a whole number above 0, such as `100000` |
| `redis` | values in each reply, counting every element of an array and every value of a map | a whole number above 0, such as `1000` |
| `kafka` | records | a whole number above 0, such as `1000` |

stderr ends with what the source read:

```
$ gatereach q acme-athena "SELECT id FROM events WHERE day = '2026-01-02'" --max 10GB
{"id":1}
{"id":2}
(2 rows)
acme-athena read 1.2 GB of --max 10 GB
```

Reaching the cap is a normal end. gatereach prints `ALIAS stopped at --max: read USED of MAX`, exits 0 and keeps an `--out` package complete.

- MySQL reads the query's `ROWS_EXAMINED` from `performance_schema.events_statements_history` right after it, so it needs MySQL 8.0.16 or later and an account that can read that table. Otherwise the query fails with `mysql --max needs performance_schema statement history`
- A bad form fails before any tunnel opens, such as `athena --max takes bytes scanned, such as 10GB`

`--timeout S` caps each run, each `--follow` poll included. MySQL defaults to 30 seconds, Redis to 10 and DynamoDB to 60, and `--timeout 0` turns the cap off. A run past it fails with `query ran past --timeout`. Athena stops the query so it stops scanning. MySQL also sets `max_execution_time`, so the server ends a read-only SELECT by itself. A followed Kafka topic has no runs, so the timeout doesn't apply to it.

## Read-only guard

Without `--write`, gatereach blocks writes it can recognize:

| Client | Guard |
|---|---|
| `mysql` | Accepts only `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`, `TABLE` and `VALUES`. Refuses locking reads such as `FOR UPDATE`, file writes such as `INTO OUTFILE`, and lock or sleep functions. `EXPLAIN ANALYZE` runs its statement, so it's allowed only on a `SELECT`, `WITH` or `TABLE`. Takes one statement per query. In the same round trip, it first rolls back any transaction an earlier query left open and sets `transaction_read_only = ON` again, so a statement that turns it off or a `START TRANSACTION READ WRITE` can't carry over to the next query |
| `redis` | Allows only read commands such as `GET`, `HGETALL`, `SCAN`, `XRANGE` and `INFO`. `KEYS` is left out, since it can stall the server |
| `dynamodb` | Allows only statements starting with `SELECT` |
| `kafka` | Only consumes |
| `athena` | Allows only statements starting with `SELECT`, `WITH`, `SHOW`, `DESCRIBE` or `EXPLAIN`, after any comments or parentheses. `EXPLAIN ANALYZE` runs its statement, so it's allowed only on a `SELECT` or `WITH`, with or without a `FORMAT` or `VERBOSE` option |

With `--write`, MySQL skips the rollback, so a transaction can span several `--write` queries on the same alias. The next query without `--write` rolls it back.

MySQL's one-statement check reads the query both with and without backslash escapes, so a server mode like `NO_BACKSLASH_ESCAPES` or ANSI quoting can't hide a second statement. That means a string with a backslash-escaped quote before a semicolon, like `'it\'s; done'`, is refused too. Write the quote doubled instead: `'it''s; done'`.

This is a guard against mistakes, not a security boundary. A stored procedure, a function with side effects, a `TEMPORARY` table, or a command it doesn't know about can still write. Only a read-only database account guarantees no writes, so use one where you can.

Limits are best effort too. MySQL caps each statement with `sql_select_limit` and stops reading at `--limit`, and DynamoDB and Athena stop paging there. Athena still runs the whole query and bills the data it scans, so add a `LIMIT` or a partition filter to cut the scan. `--max` can only stop the work that comes next, since a source reports its cost after the work is done. Redis bounds whole-collection and range reads such as `HGETALL`, `SMEMBERS` and `LRANGE` by `--limit`; use `HSCAN`, `SSCAN` or `ZSCAN` to page through the rest. A Kafka topic name reads up to `--limit` recent records sampled across partitions, not strictly the newest of the whole topic.

## Reading the results elsewhere

A saved folder is a plain Data Package v2, so any tool that reads Data Packages, Arrow or CSV can open it without gatereach:

```python
import pyarrow.ipc as ipc
table = ipc.open_stream("DIR/result.arrows").read_all()
```

```python
import polars as pl
df = pl.read_ipc_stream("DIR/result.arrows")
```

```sql
INSTALL nanoarrow FROM community;
LOAD nanoarrow;
FROM read_arrow('DIR/result.arrows');
```

pandas reads it through pyarrow, `table.to_pandas()`. Read every resource `datapackage.json` lists, since a stream that changed shape is split across files, and check `complete` before treating it as the whole answer.

## Notes

- Tunnels are tracked by their process command line and sessions by their socket, with no state file, so a tunnel Teleport Connect or another program opened on the same port shows up as held by another process
- When a query finds the connection dropped, the session connects again and runs that query once more. It then starts without the session variables or temporary tables of earlier queries, and stderr says so

## License

MIT
