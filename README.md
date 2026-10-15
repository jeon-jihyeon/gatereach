<h1 align="center">gatereach</h1>

<p align="center">Let your agent query the databases it can't reach on its own, through saved aliases, read only by default</p>

<p align="center">
  <a href="https://github.com/jeon-jihyeon/gatereach/blob/main/LICENSE"><img alt="MIT" src="https://img.shields.io/badge/license-MIT-blue"></a>
</p>

The data an agent needs often sits behind a Teleport tunnel, a password in your Keychain, or an AWS profile. Left alone, an agent improvises: it pastes credentials into commands, leaves tunnels open on random ports, and parses five different CLI output formats. gatereach gives it one command instead.

- Connections are saved once as aliases. Passwords stay in the macOS Keychain or an environment variable, and the config file only holds a reference
- Tunnels open on demand and stay open until you close them, and a session per alias holds the database connection, so the next query skips the setup
- MySQL, Redis, DynamoDB and Kafka all come out as one JSON object per record, with NULL kept apart from the string "NULL"
- Writes are blocked unless you pass `--write`
- Results can be saved as a [Frictionless Data Package](https://datapackage.org/) with Arrow or CSV data, so any Data Package reader can pick them up

## Quickstart

gatereach runs on macOS and Linux as one binary with the database drivers built in. On first run the plugin's launcher downloads the release for your platform and checks it against the release checksums, or builds it with Go when the download isn't possible. The binary lives in `~/.gatereach`, or in `GATEREACH_HOME` when it's set.

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
| DynamoDB | an AWS profile, as the AWS CLI would use it |
| `keychain:` secrets | macOS and its `security` command |

MySQL, Redis, DynamoDB and Kafka clients are built in, so no database client needs to be installed. `tsh` can be set in the config file when it isn't on PATH.

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
region = "ap-northeast-2"

# tsh path and home, only when the defaults don't fit
# home sets TELEPORT_HOME, so tsh uses the profile Teleport Connect keeps
[tunnels.tsh]
home = "~/Library/Application Support/Teleport Connect/tsh"
```

[conns.example.toml](conns.example.toml) has every key, Kafka with SASL included.

A secret is a reference, never the password:

- `keychain:NAME` reads the macOS Keychain item whose service is `NAME`, for example one made with `security add-generic-password -s NAME -a "$USER" -w`
- `env:VAR` reads an environment variable

The value goes straight into the built-in driver in memory, so it never shows up in `ps`, a file or a log.

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

A file is read only when you name it with `--file`, so a query is never mistaken for a path. Put `--` before a query that starts with a dash, like `gatereach q ALIAS -- "-x"`.

`q` flags:

| Flag | Does |
|---|---|
| `--file PATH` | Reads the query from a file instead of the argument |
| `--limit N` | Caps records per run, 1000 by default |
| `--write` | Lifts the read-only guard. Kafka refuses it |
| `--follow` | Keeps printing new records until stopped |
| `--interval S` | Seconds between polls with `--follow`, 5 by default |
| `--key FIELD` | Field that identifies a record when polling, so a changed row isn't printed twice |
| `--out DIR` | Saves a Data Package into a new or empty folder |
| `--format arrow` or `csv` | Data file format for `--out`, `arrow` by default |

`add` flags:

| Flag | Does |
|---|---|
| `--client` | `mysql`, `redis`, `dynamodb` or `kafka`, required |
| `--host`, `--port` | Address, `127.0.0.1` by default. A direct connection needs `--port` |
| `--tunnel tsh --target NAME` | Reaches the database through `tsh proxy db`. Needs `--user`, and picks a fixed port between 20000 and 31999 from the target and user unless you pass `--port` |
| `--user`, `--database` | Database user and MySQL default database |
| `--secret` | `keychain:NAME` or `env:VAR` |
| `--tls` | TLS for MySQL, Redis and Kafka. Kafka with a secret always uses TLS |
| `--profile`, `--region` | AWS profile and region for DynamoDB |

```
gatereach discover tsh
gatereach add orders-staging --client mysql --user viewer --tunnel tsh --target orders-staging
gatereach add cache-local --client redis --port 6379 --secret keychain:cache-local
gatereach add events-kafka --client kafka --host broker.example.com --port 9092 --user api-key --secret env:EVENTS_KAFKA_SECRET
gatereach add events-ddb --client dynamodb --profile dev --region ap-northeast-2
```

`discover` lists MySQL and Redis databases with their client and shows `-` for protocols gatereach can't query. `add` takes a tunnel for any client but DynamoDB.

A tunnel and a session keep running after gatereach exits. The session holds one connection per alias and serves queries over a Unix socket in a folder only your user can open, so the next query skips the TLS and login round trips, and Teleport records one session instead of one per query. Their logs go to `logs/ALIAS.log` and `logs/ALIAS.session.log` next to the config file.

## Output

Without `--out`, each record is one JSON line on stdout as it arrives, and stderr ends with the count:

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

With `--out DIR`, stdout prints only the path of `DIR/datapackage.json`, not the records:

```
DIR/datapackage.json   descriptor with the source, query, time and a typed Table Schema per file
DIR/result.arrows      Arrow IPC stream, written in batches of up to 1000 records or every second
DIR/result-2.arrows    started when records change shape, so nothing is dropped
```

- `--format arrow` writes an Arrow IPC stream. Columns with nested or mixed values use the canonical `arrow.json` extension type, and the source and query are also in the schema metadata, so the file still says where it came from on its own
- `--format csv` writes `result.csv` with `\N` as NULL, declared in the schema's `missingValues`. A string that is literally `\N` reads back as NULL too, so use Arrow when that matters. Nested values are JSON text. In a column that mixes strings and other kinds, strings are written as they are, so the string `"1"` and the number `1` look the same in CSV while Arrow keeps them apart
- The `gatereach` key in `datapackage.json` records the alias, client, user, query, limit, flags, start time, and whether the run is `complete`
- If the query fails, or a run without `--follow` is interrupted, gatereach still writes what it received, sets `"complete": false` with the cause in `error`, and exits non-zero, 130 for an interrupt. Stopping a `--follow` run with Ctrl-C or SIGTERM is a normal end, exits 0 and stays complete
- If writing the files fails, the package is still marked incomplete and gatereach exits non-zero
- Saved results are private to your user: the folder is created with mode 0700 and each file with 0600

## Following

`--follow` keeps the query going until you stop it.

- Kafka keeps its consumer open: it first prints up to `--limit` recent records from each partition, then every new message
- Every other source, and Kafka `topics`, reruns the query every `--interval` seconds with `--limit` applied to each run, and prints only records it hasn't seen, compared by `--key` or by the whole record

## Read-only guard

Without `--write`, gatereach blocks writes it can recognize:

| Client | Guard |
|---|---|
| `mysql` | Takes one statement per query. In the same round trip, it first rolls back any transaction an earlier query left open and sets `transaction_read_only = ON` again, so a statement that turns it off or a `START TRANSACTION READ WRITE` can't carry over to the next query |
| `redis` | Allows only read commands such as `GET`, `HGETALL`, `SCAN`, `XRANGE` and `INFO`. `KEYS` is left out, since it can stall the server |
| `dynamodb` | Allows only statements starting with `SELECT` |
| `kafka` | Only consumes |

With `--write`, MySQL skips the rollback, so a transaction can span several `--write` queries on the same alias. The next query without `--write` rolls it back.

The one statement check reads the query both with and without backslash escapes, so a server mode like `NO_BACKSLASH_ESCAPES` or ANSI quoting can't hide a second statement. That means a string with a backslash-escaped quote before a semicolon, like `'it\'s; done'`, is refused too. Write the quote doubled instead: `'it''s; done'`.

This is a guard against mistakes, not a security boundary. A stored procedure, a function with side effects, a `TEMPORARY` table, or a command it doesn't know about can still write. Only a read-only database account guarantees no writes, so use one where you can.

Limits are best effort too. MySQL caps each statement with `sql_select_limit` and stops reading at `--limit`, and DynamoDB stops paging there. Redis cuts the reply only after it arrives, so a huge `HGETALL` still loads the server; prefer `HSCAN`, `SSCAN` or a bounded range. A Kafka topic name reads up to `--limit` recent records sampled across partitions, not strictly the newest of the whole topic.

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

- gatereach prints its own messages in Korean for now
- Tunnels are tracked by their process command line and sessions by their socket, with no state file, so a tunnel Teleport Connect or another program opened on the same port shows up as held by another process
- When a query finds the connection dropped, the session connects again and runs that query once more. It then starts without the session variables or temporary tables of earlier queries, and stderr says so

## License

MIT
