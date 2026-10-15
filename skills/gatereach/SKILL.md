---
name: gatereach
description: Query MySQL, Redis, DynamoDB or Kafka that sit behind tunnels or credentials, through saved connection aliases, opening tunnels such as Teleport on demand. Use when the user asks to query or inspect a database, open or close a DB connection, list connections, add a new one, or save query results as a file.
license: MIT
compatibility: The launcher fetches a checksum-verified release binary on first run, or builds it with Go when the release can't be fetched. Database drivers are built in. Teleport tunnels need tsh, and keychain secrets need macOS security.
---

# gatereach

Every data source action goes through the `gatereach` command on PATH. Never call tsh or a database client such as mysql, redis-cli, aws dynamodb or kcat directly, and never edit the config file by hand. gatereach prints its own messages in Korean; the meaning of each one you'll meet is given below

## Query

1. Run `gatereach ls` to see the aliases. Columns are alias, client, host and port or AWS profile, user, tunnel type, tunnel `open`, `closed` or `-` when there's no port to check, and session `live` or `-`
2. Pick the alias from the user's words. If two aliases fit, ask instead of guessing
3. Run `gatereach q <alias> "<query>"`. A closed tunnel opens by itself, and a session process starts that holds the connection, so the next query on the alias skips the connection setup. Both stay after the command ends
   - To read the query from a file, run `gatereach q <alias> --file <path>`. A bare argument is always the query, never a path
   - Put `--` before a query that starts with a dash: `gatereach q <alias> -- "<query>"`
   - mysql: one SQL statement. Send several statements as separate queries. Inside a string, escape a quote by doubling it (`'it''s'`), since a backslash-escaped quote before a semicolon is refused
   - redis: one redis-cli command such as `HGETALL user:1`. Use SCAN instead of KEYS
   - dynamodb: a PartiQL SELECT
   - kafka: a topic name to read up to N recent records sampled across its partitions (not strictly the newest N of the whole topic), or `topics` for metadata
4. Without `--out`, stdout gets one JSON object per record as it arrives and stderr ends with a count such as `(12 rows)`, or `(12 rows, 실패로 중단)` when a failure cut it short. A NULL is JSON `null`, so it never mixes with the string "NULL"
5. MySQL values come typed: integers and floats as numbers, JSON columns parsed, and DECIMAL, dates and times as strings. DynamoDB numbers stay strings to keep their precision
6. Queries return at most 1000 records. Pass `--limit N` for a different cap. Redis cuts the reply after it arrives, so a huge `HGETALL`, `SMEMBERS` or `LRANGE k 0 -1` still loads the server; prefer HSCAN, SSCAN or a bounded range
7. Without `--write`, gatereach blocks writes on a best-effort basis: Redis allows only read commands, DynamoDB only statements starting with SELECT, Kafka only consumes, and MySQL takes one statement at a time and, before every query, rolls back any transaction an earlier query left open and sets the session read only again. This is a guard against mistakes, not a guarantee; only a read-only DB account such as `viewer` guarantees no writes, so prefer such an account
8. Pass `--write` only when the user asks for that exact write. Kafka refuses `--write`. With `--write`, MySQL skips the rollback, so a transaction you start stays open across `--write` queries on that alias. The next query without `--write` rolls it back, so commit first

## Following

- `--follow` keeps printing new records until stopped
- Kafka keeps its consumer running: it first prints up to `--limit` recent records from each partition, then every new message as it lands. `--interval` doesn't apply
- Every other source, and Kafka `topics`, reruns the query every `--interval` seconds, 5 by default, with `--limit` applied to each run, and prints only records it hasn't seen
- Pass `--key <field>` when a record has an id, so a changed row isn't printed again. Without it the whole record is compared
- Run a follow in the background and stop it when the user asks. Ctrl-C or SIGTERM ends a follow normally and still saves `--out` as complete

## Saving results

- `gatereach q <alias> "<query>" --out <new folder>` saves a Frictionless Data Package: data written in batches as records arrive, and `datapackage.json` with the source, query and time. The folder must be new or empty
- With `--out`, stdout prints only the path of `datapackage.json`, not the records. Read the result from the files
- `--format arrow`, the default, writes an Arrow IPC stream `result.arrows`. pyarrow opens it with `pyarrow.ipc.open_stream`, Polars with `pl.read_ipc_stream`, and DuckDB after `INSTALL nanoarrow FROM community; LOAD nanoarrow;`. pandas needs pyarrow first. The source and query also sit in its schema metadata
- Columns of objects or arrays hold JSON text, as the `arrow.json` extension type in Arrow and as JSON text cells in CSV
- A column that mixes kinds, such as strings and numbers, gets type `any`. Arrow stores every value of it as JSON, so the string `"1"` and the number `1` stay apart. CSV writes its strings as they are and the other values as JSON, so `"1"` and `1` look the same; use arrow when that matters
- `--format csv` writes `result.csv` with a typed schema in `datapackage.json`, for spreadsheets and tools that only read CSV. In CSV a NULL is `\N`, so a string value `\N` also reads back as NULL; use arrow when that matters
- When records change shape mid-stream, a new file such as `result-2.arrows` starts so nothing is dropped. Read every resource the descriptor lists
- If the query fails or a run without `--follow` is interrupted, gatereach keeps what it received, writes `"complete": false` and the cause as `error` under the `gatereach` key of `datapackage.json`, prints `실패로 중단되어 받은 데까지만 저장: <path>` (stopped by a failure, saved only what arrived) to stderr, and exits non-zero. Treat such a package as partial and say so when citing it
- The folder is created with mode 0700 and every file with 0600, so only the user can read the results. Don't loosen that when sharing; copy what the user wants shared
- Use it when the user wants to keep, share or cite the result, or when a result is too large to read in the conversation

## Connections

- Close only when the user asks: `gatereach close <alias>` or `gatereach close all`. It ends the session and closes the tunnel. Only tunnels gatereach opened are closed; `gatereach가 연 터널이 아니라 유지` means another program opened it, so it stays
- `gatereach open <alias>` opens the tunnel and starts the session ahead of a query
- To add a Teleport database, run `gatereach discover tsh`. Columns are `new` or `added`, name, client, allowed users and target. Pick a `new` row whose client is `mysql` or `redis`; a `-` client is a protocol gatereach can't query. Then
  `gatereach add <alias> --client <client> --user <allowed user> --tunnel tsh --target <target>`
  - The tunnel listens on 127.0.0.1 at a fixed port between 20000 and 31999 derived from the target and user. Pass `--port` only to pick one yourself
- To add a direct connection, `gatereach add <alias> --client <client> --host <host> --port <port>`. `--host` defaults to 127.0.0.1
- Add `--database <name>` for a MySQL default database, and `--tls` for TLS to MySQL, Redis or Kafka. Kafka with a secret always uses TLS
- Kafka with a password uses SASL_SSL and PLAIN and needs `--user`: `gatereach add <alias> --client kafka --host <broker> --port <port> --user <key> --secret keychain:<item>`
- DynamoDB has no host or port: `gatereach add <alias> --client dynamodb --profile <aws profile> --region <region>`
- Store a password only as a reference: `--secret keychain:<item>` for a macOS Keychain item found by service name, or `--secret env:<VAR>`

## Failures

Tunnels:

- `열기 실패` (open failed): the tunnel exited early or its port didn't open in 20 s. Show the tunnel's error lines that gatereach prints. The full tunnel log is at `logs/<alias>.log` next to the config file. If they point to login, tell the user to log in to the tunnel, for example `tsh login` or Teleport Connect
- `터널 실행 실패` (tunnel couldn't run): the tunnel program itself couldn't start, usually because tsh isn't on PATH. Tell the user to install tsh or set `bin` under `[tunnels.tsh]`
- `닫혀 있고 터널 설정 없음` (closed and no tunnel configured): a direct connection's port doesn't answer. The server or the user's own tunnel is down; tell the user
- `다른 프로세스가 연 포트 사용` (port held by another process): another program such as Teleport Connect holds that port, so gatereach can't confirm it reaches the alias's target. Tell the user when results look like a different database
- `프로세스 목록 조회 실패` (process list failed): `ps` failed. Show the message to the user
- `tsh db ls 실패` (tsh db ls failed): the user needs to log in to Teleport
- `지원하지 않는 터널` (unsupported tunnel): only `tsh` exists

Sessions:

- `세션 시작 실패` (session failed to start): show the lines gatereach prints, often a `연결 실패` (connect failed) from the database or a secret error below. The full log is at `logs/<alias>.session.log` next to the config file. `세션 실행 실패` (session couldn't run) means gatereach couldn't launch itself
- `세션 연결 실패` (couldn't reach the session): the session went away between start and query. Run the query again
- `세션이 응답 중에 끊김` (session dropped mid-response): the session exited or was closed while answering. Run the query again; if it repeats, check `logs/<alias>.session.log`
- `세션 응답 해석 실패` (couldn't parse the session's reply): a gatereach version mismatch between the session and the command. Run `gatereach close <alias>` and query again
- `세션 사용 중` (session busy): another query on the same alias held the connection for 30 s. Wait for it, or run the queries one after another
- `재연결 실패` (reconnect failed): the connection dropped and connecting again failed, often because the tunnel closed or a login expired. Check with `gatereach ls` and tell the user
- `연결이 끊겨 다시 맺음` (reconnected after a drop): the query ran on a new connection, so session variables and temporary tables from earlier queries are gone
- `<alias> 세션에 <other alias> 요청` (a request for another alias): the socket answered for a different alias, so nothing ran. Run `gatereach close all` and query again
- `세션 종료 실패` (session wouldn't stop): it ignored the shutdown request and SIGTERM. Tell the user
- `세션 폴더가 실제 폴더가 아님`, `세션 폴더 소유자가 현재 사용자가 아님`, `세션 폴더 권한이 0700이 아님` or `세션 소켓의 상대가 현재 사용자가 아님` (the socket folder is a link, owned by someone else or open to others, or the socket belongs to another user): gatereach refuses to send queries there. Tell the user and don't change the folder yourself

Queries:

- `알 수 없는 별칭` (unknown alias): show `gatereach ls` and ask which one
- `읽기 전용 모드에서 막힌 명령` (command blocked in read-only mode) or `읽기 전용 모드에서는 SELECT만 허용` (only SELECT in read-only mode): the query writes. Rewrite it as a read, or ask the user before adding `--write`
- `Cannot execute statement in a READ ONLY transaction`: the MySQL server itself refused a write in read-only mode. Same as above
- `kafka는 읽기만 지원` (kafka only reads): drop `--write`
- `MySQL은 한 번에 한 문장만 실행` (MySQL runs one statement at a time): split the statements into separate queries. If there's only one, look for a backslash-escaped quote before a semicolon inside a string and double the quote instead
- `여러 문장이 실행됨` (several statements ran): more than one statement reached the server and only the first result was printed. Split the query
- `빈 명령` (empty command) or `닫히지 않은 따옴표` (unclosed quote): fix the Redis command
- `redis 오류 응답` (redis error reply): the server's own error follows. Fix the command
- `kafka 토픽 없음` (no such topic) or `조회 실패` (lookup failed) for a topic: check the name with the `topics` query
- `중단됨` (interrupted): a query without `--follow` was stopped and exited 130. What arrived is partial
- `--limit은 1 이상` (limit must be 1 or more), `--interval은 0보다 커야 함` (interval must be above 0), `지원하지 않는 형식` (unsupported format) or `사용법` (usage): fix the flags
- `비어 있지 않은 폴더` (folder not empty): pick a new folder for `--out`
- `arrow 쓰기 실패` (arrow write failed): saving failed, usually a full disk. The package is marked incomplete

Connections and config:

- `Keychain 항목 없음` (no Keychain item) or `환경변수 없음` (no environment variable): the secret reference points nowhere. Ask the user to create the item or set the variable; never ask for the password itself
- `지원하지 않는 secret 참조` (unsupported secret reference): only `keychain:` and `env:` work
- `kafka secret을 쓰려면 user 필요` (a Kafka secret needs a user): the connection needs `user`
- `연결 설정에 없는 키` (keys missing from the connection): the alias lacks a key its client needs, such as `user` for MySQL. Tell the user which key to add to that connection
- `지원하지 않는 client` (unsupported client): the connection's `client` isn't mysql, redis, dynamodb or kafka
- `설정 파일 읽기 실패` (couldn't read the config file): the TOML is broken. Show the error to the user; don't edit the file yourself
- `이미 있는 별칭` (alias exists), `다른 연결이 쓰는 포트` (port in use by another connection), `터널 연결은 --target과 --user 필요` (a tunnel needs --target and --user), `터널 없는 연결은 --port 필요` (a direct connection needs --port), `--client 필요` (--client required), `터널 없이 연결` (connects without a tunnel), `고정 포트 범위 소진` (no fixed port left) or `설정 값으로 쓸 수 없는 문자` (a character the config can't hold): fix the `add` flags or pick another alias
- `터널 닫기 실패` (couldn't close the tunnel): `close` went on with the other aliases. Show the message
