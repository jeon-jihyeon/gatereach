package clients

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

const mysqlKillWait = 5 * time.Second

type mysqlClient struct {
	conn     config.Conn
	password string
	cfg      *mysql.Config
	db       *sql.DB
	// One pinned connection so session variables carry over between requests
	sess *sql.Conn
	// The pinned connection's CONNECTION_ID that KILL QUERY targets
	id int64
	// An earlier request set max_execution_time so the next one resets it
	timed bool
}

func newMySQL(conn config.Conn, password string) Client {
	return &mysqlClient{conn: conn, password: password}
}

// Allows one statement and without --write only a read that can't lock or stall the shared server
// 1. A proxy such as Teleport runs several statements on its own connection to the server so the client refuses them
// 2. Both parses are checked so the verdict holds when sql_mode turns off backslash escapes
// 3. Write blocking itself comes from the session settings sent before every request
// 4. A read-only transaction still allows locking reads and lock or sleep functions so they're refused here
func mysqlCheck(text string, write bool) error {
	if multipleStatements(text, true) || multipleStatements(text, false) {
		return errors.New("MySQL runs one statement at a time\nrun the statements after a semicolon separately")
	}
	if write {
		return nil
	}
	if err := mysqlReadOnly(mysqlTokens(text, true)); err != nil {
		return err
	}
	return mysqlReadOnly(mysqlTokens(text, false))
}

// Leading keywords read-only mode allows
var mysqlReads = map[string]bool{
	"SELECT": true, "WITH": true, "SHOW": true, "DESC": true, "DESCRIBE": true, "EXPLAIN": true,
	"TABLE": true, "VALUES": true,
}

// Statements EXPLAIN ANALYZE runs that read only
var mysqlAnalyzed = map[string]bool{"SELECT": true, "WITH": true, "TABLE": true}

// Statements a WITH clause can lead besides SELECT
var mysqlWithWrites = map[string]bool{"UPDATE": true, "DELETE": true, "INSERT": true, "REPLACE": true}

// Word sequences that lock rows or write a server file
var mysqlPhrases = []struct {
	words  []string
	reason string
}{
	{[]string{"INTO", "OUTFILE"}, "it writes a file on the server"},
	{[]string{"INTO", "DUMPFILE"}, "it writes a file on the server"},
	{[]string{"FOR", "UPDATE"}, "it locks the rows it reads"},
	{[]string{"FOR", "SHARE"}, "it locks the rows it reads"},
	{[]string{"LOCK", "IN", "SHARE", "MODE"}, "it locks the rows it reads"},
}

// Functions that hold the connection or a lock or burn server time
var mysqlHeavy = map[string]bool{
	"SLEEP": true, "BENCHMARK": true, "GET_LOCK": true, "RELEASE_LOCK": true, "RELEASE_ALL_LOCKS": true,
	"IS_FREE_LOCK": true, "IS_USED_LOCK": true, "WAIT_FOR_EXECUTED_GTID_SET": true, "MASTER_POS_WAIT": true,
	"SOURCE_POS_WAIT": true,
}

func mysqlRefuses(thing, reason string) error {
	return fmt.Errorf("read-only mode refuses %s: %s\nuse --write to run it anyway", thing, reason)
}

// 1. The first keyword after any parentheses decides the statement
// 2. EXPLAIN ANALYZE runs its statement so that statement must read too
// 3. Phrases and functions count anywhere in the code
func mysqlReadOnly(toks []sqlToken) error {
	i := skipOpen(toks, 0)
	first := tokenWordAt(toks, i)
	if !mysqlReads[first] {
		return errors.New("read-only mode allows only SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, TABLE and VALUES\n" +
			"use --write to write")
	}
	with := first == "WITH"
	if first == "EXPLAIN" || first == "DESC" || first == "DESCRIBE" {
		j := skipFormat(toks, i+1)
		if tokenWordAt(toks, j) == "ANALYZE" {
			run := tokenWordAt(toks, skipOpen(toks, skipFormat(toks, j+1)))
			if !mysqlAnalyzed[run] {
				return errors.New("read-only mode allows EXPLAIN ANALYZE only on SELECT, WITH or TABLE\n" +
					"use --write to run it anyway")
			}
			with = run == "WITH"
		}
	}
	for k, t := range toks {
		// MySQL lets a WITH clause lead an UPDATE or DELETE
		if with && t.kind == tokenWord && mysqlWithWrites[t.text] {
			return errors.New("read-only mode allows WITH only before SELECT\nuse --write to write")
		}
		for _, p := range mysqlPhrases {
			if hasPhrase(toks[k:], p.words) {
				return mysqlRefuses(strings.Join(p.words, " "), p.reason)
			}
		}
		// A quoted name followed by a parenthesis still calls the built-in function
		named := t.kind == tokenWord || t.kind == tokenQuoted
		if named && mysqlHeavy[t.text] && k+1 < len(toks) && toks[k+1] == (sqlToken{"(", tokenSymbol}) {
			return mysqlRefuses(t.text+"()", "it can hold locks or load the server")
		}
	}
	return nil
}

func tokenWordAt(toks []sqlToken, i int) string {
	if i < len(toks) && toks[i].kind == tokenWord {
		return toks[i].text
	}
	return ""
}

func skipOpen(toks []sqlToken, i int) int {
	for i < len(toks) && toks[i] == (sqlToken{"(", tokenSymbol}) {
		i++
	}
	return i
}

// Skips a `FORMAT = name` option of EXPLAIN
func skipFormat(toks []sqlToken, i int) int {
	if tokenWordAt(toks, i) == "FORMAT" && i+2 < len(toks) && toks[i+1] == (sqlToken{"=", tokenSymbol}) {
		return i + 3
	}
	return i
}

func hasPhrase(toks []sqlToken, words []string) bool {
	if len(toks) < len(words) {
		return false
	}
	for i, w := range words {
		if tokenWordAt(toks, i) != w {
			return false
		}
	}
	return true
}

type tokenKind int

const (
	tokenWord   tokenKind = iota // keyword or plain identifier
	tokenQuoted                  // name in backticks or double quotes
	tokenString                  // single quoted string
	tokenSymbol                  // any other character
)

type sqlToken struct {
	// Upper-cased word or quoted name
	text string
	kind tokenKind
}

// Code tokens of one statement without its comments
// 1. Executable comments run so their content counts as code
// 2. Their opener with its version number and their closing `*/` are left out
// 3. Quotes follow skipQuoted so both parses split the text as multipleStatements does
func mysqlTokens(s string, backslash bool) []sqlToken {
	var toks []sqlToken
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c <= ' ':
			i++
		case c == '\'' || c == '"' || c == '`':
			end := skipQuoted(s, i, backslash)
			kind := tokenQuoted
			if c == '\'' {
				kind = tokenString
			}
			name := strings.ReplaceAll(s[i+1:end], string([]byte{c, c}), string(c))
			toks = append(toks, sqlToken{strings.ToUpper(name), kind})
			i = end + 1
		case c == '#' || (c == '-' && strings.HasPrefix(s[i:], "--") && (i+2 == len(s) || s[i+2] <= ' ')):
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case strings.HasPrefix(s[i:], "/*!") || strings.HasPrefix(s[i:], "/*M!"):
			i += strings.Index(s[i:], "!") + 1
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
		case strings.HasPrefix(s[i:], "/*+"):
			i += 3
		case strings.HasPrefix(s[i:], "/*"):
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return toks
			}
			i += 2 + end + 2
		case strings.HasPrefix(s[i:], "*/"):
			i += 2
		case mysqlWordByte(c):
			j := i
			for j < len(s) && mysqlWordByte(s[j]) {
				j++
			}
			toks = append(toks, sqlToken{strings.ToUpper(s[i:j]), tokenWord})
			i = j
		default:
			toks = append(toks, sqlToken{string(c), tokenSymbol})
			i++
		}
	}
	return toks
}

// Unquoted identifiers take letters, digits, `_`, `$` and any non-ASCII byte
func mysqlWordByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '$' || c >= 0x80
}

// Whether another statement follows a semicolon outside quotes and comments
// Executable comments the server runs count as code
// They start with `/*!` or `/*+` or `/*M!`
func multipleStatements(s string, backslash bool) bool {
	ended := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			if ended {
				return true
			}
			i = skipQuoted(s, i, backslash)
		case c == '#' || (c == '-' && strings.HasPrefix(s[i:], "--") && (i+2 == len(s) || s[i+2] <= ' ')):
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && strings.HasPrefix(s[i:], "/*"):
			rest := s[i+2:]
			if strings.HasPrefix(rest, "!") || strings.HasPrefix(rest, "+") || strings.HasPrefix(rest, "M!") {
				if ended {
					return true
				}
				i++
				continue
			}
			end := strings.Index(rest, "*/")
			if end < 0 {
				return false
			}
			i += 2 + end + 1
		case c == '*' && strings.HasPrefix(s[i:], "*/"):
			i++
		case c == ';':
			ended = true
		case c > ' ':
			if ended {
				return true
			}
		}
	}
	return false
}

// Index of the closing quote
// 1. A doubled quote stays inside the string
// 2. With backslash the character after a backslash stays inside too
func skipQuoted(s string, i int, backslash bool) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			if backslash && q != '`' {
				j++
			}
		case q:
			if j+1 < len(s) && s[j+1] == q {
				j++
				continue
			}
			return j
		}
	}
	return len(s)
}

func (m *mysqlClient) Connect(ctx context.Context) error {
	cfg := mysql.NewConfig()
	cfg.User = m.conn.User
	cfg.Passwd = m.password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(m.conn.Host, strconv.Itoa(m.conn.Port))
	cfg.DBName = m.conn.Database
	// 1. Multiple statements send the session settings and the query in one round trip
	// 2. mysqlCheck limits the user's query to one statement
	// 3. LOAD DATA LOCAL is blocked so the server can't read client files
	// 4. The query text goes as is with no value interpolation
	cfg.MultiStatements = true
	cfg.AllowAllFiles = false
	cfg.InterpolateParams = false
	if m.conn.TLS {
		cfg.TLSConfig = "true"
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	sess, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return err
	}
	var id int64
	if err := sess.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		_ = sess.Close()
		_ = db.Close()
		return err
	}
	m.cfg, m.db, m.sess, m.id = cfg, db, sess, id
	return nil
}

func (m *mysqlClient) Close() error {
	if m.sess != nil {
		_ = m.sess.Close()
	}
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// ER_QUERY_TIMEOUT the server returns for a SELECT that ran past max_execution_time
const erQueryTimeout = 3024

// The newest finished statement of this connection is the query that ran just before
const mysqlRowsExamined = "SELECT ROWS_EXAMINED FROM performance_schema.events_statements_history " +
	"WHERE THREAD_ID = PS_CURRENT_THREAD_ID() ORDER BY EVENT_ID DESC LIMIT 1"

// Session settings sent before every request
// 1. Read-only rolls back a transaction an earlier request opened so START TRANSACTION READ WRITE can't get around it
// 2. The variables are set on every request to undo what an earlier request changed
// 3. Session scope is used since a transaction scoped READ ONLY doesn't survive an implicit commit
// 4. --write skips ROLLBACK so a transaction can span requests
// 5. max_execution_time lets the server end a read-only SELECT by itself and 0 turns it off
// 6. It's sent only with a timeout or to reset an earlier one since MariaDB has no such variable
func mysqlStatement(q Query, reset bool) string {
	settings := fmt.Sprintf("sql_select_limit = %d", q.Limit)
	if q.Timeout > 0 || reset {
		settings += fmt.Sprintf(", max_execution_time = %d", mysqlMillis(q.Timeout))
	}
	if q.Write {
		return fmt.Sprintf("SET SESSION transaction_read_only = OFF, %s;\n%s", settings, q.Text)
	}
	return fmt.Sprintf("ROLLBACK;\nSET SESSION transaction_read_only = ON, %s;\n%s", settings, q.Text)
}

// Rounds up so a timeout under a millisecond never turns the limit off
func mysqlMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

func (m *mysqlClient) Read(ctx context.Context, q Query, emit Emit) error {
	return mysqlError(m.read(ctx, q, emit), q.Timeout)
}

// The server ending a SELECT at the max_execution_time of --timeout reads as the same timeout Run reports
// Without --timeout the limit came from the query's own hint so the server's message stays
func mysqlError(err error, timeout time.Duration) error {
	var me *mysql.MySQLError
	if timeout > 0 && errors.As(err, &me) && me.Number == erQueryTimeout {
		return timeoutError(timeout)
	}
	return err
}

func (m *mysqlClient) read(ctx context.Context, q Query, emit Emit) error {
	if m.sess == nil {
		return ErrDisconnected
	}
	// On cancel the driver only closes the connection and the server keeps running the query
	// 1. KILL QUERY over another connection stops the server side too
	// 2. Read waits for a started kill so it can't land on the next request's query
	// 3. A query that returns on the cancel itself can beat the AfterFunc so Read kills then
	killed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(killed)
		m.kill()
	})
	defer func() {
		if !stop() {
			<-killed
		} else if ctx.Err() != nil {
			m.kill()
		}
	}()
	stmt := mysqlStatement(q, m.timed)
	// A failed round trip may still have set the timeout so it's reset next time
	m.timed = m.timed || q.Timeout > 0
	rows, err := m.sess.QueryContext(ctx, stmt)
	if disconnected(err) {
		return ErrDisconnected
	}
	if err != nil {
		return err
	}
	m.timed = q.Timeout > 0
	err = queryResult(rows, q.Limit, emit)
	// Frees the held connection for the statement that measures
	rows.Close()
	if err != nil || q.Use == nil {
		return err
	}
	// The rows are already out so the amount only decides whether the next run happens
	examined, err := m.rowsExamined(ctx)
	if err != nil {
		return fmt.Errorf("mysql --max needs performance_schema statement history: %w", err)
	}
	return q.Use(examined)
}

// The settings statements return no columns so the query's result is the first that has some
// Several statements that slipped past the check are reported as an error
// instead of silently dropping their results
func queryResult(rows *sql.Rows, limit int, emit Emit) error {
	for {
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		if len(cols) > 0 {
			break
		}
		if !rows.NextResultSet() {
			return rows.Err()
		}
	}
	if err := readRows(rows, limit, emit); err != nil {
		return err
	}
	// NextResultSet skips the rows left past limit
	if rows.NextResultSet() {
		return errors.New("several statements ran: only the first result was printed")
	}
	return rows.Err()
}

// A statement of its own after the round trip
// 1. The driver skips results without columns so a statement appended to an UPDATE would pass for its result
// 2. A query ending in a line comment would swallow an appended statement
// 3. Its own event is still running so the newest finished one is the query
func (m *mysqlClient) rowsExamined(ctx context.Context) (float64, error) {
	var n int64
	err := m.sess.QueryRowContext(ctx, mysqlRowsExamined).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("no statement recorded for this connection")
	}
	return float64(n), err
}

// Only a drop the driver noticed before sending the statement is retried
// 1. `driver.ErrBadConn`: a dead connection found before writing
// 2. `sql.ErrConnDone`: an already closed connection
// `mysql.ErrInvalidConn` also shows up after sending so retrying it could run a write twice
func disconnected(err error) bool {
	return errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone)
}

// SHOW ignores sql_select_limit so the row count is capped here too
func readRows(rows *sql.Rows, limit int, emit Emit) error {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	cols := make([]string, len(colTypes))
	dbTypes := make([]string, len(colTypes))
	for i, t := range colTypes {
		cols[i], dbTypes[i] = t.Name(), t.DatabaseTypeName()
	}
	names := record.UniqueNames(cols)
	raw := make([]sql.RawBytes, len(colTypes))
	ptrs := make([]any, len(colTypes))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for n := 0; n < limit && rows.Next(); n++ {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		values := make([]any, len(raw))
		for i, b := range raw {
			values[i] = mysqlValue(dbTypes[i], b)
		}
		if err := emit(record.New(names, values)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (m *mysqlClient) kill() {
	if m.cfg == nil || m.id == 0 {
		return
	}
	db, err := sql.Open("mysql", m.cfg.FormatDSN())
	if err != nil {
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), mysqlKillWait)
	defer cancel()
	_, _ = db.ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", m.id))
}

// JSON value per column type
// 1. Integers are numbers
// 2. BIGINT UNSIGNED past int64 is a string
// 3. DECIMAL is a string to keep its precision
// 4. FLOAT and DOUBLE are numbers
// 5. JSON is the parsed value
// 6. Binary columns are base64 strings
// 7. Everything else including date and time types is the server's text
func mysqlValue(typ string, b sql.RawBytes) any {
	if b == nil {
		return nil
	}
	s := string(b)
	typ = strings.TrimPrefix(typ, "UNSIGNED ")
	switch typ {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "YEAR":
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
		return s
	case "FLOAT", "DOUBLE":
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		return s
	case "JSON":
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return s
		}
		return record.Normalize(v)
	case "BINARY", "VARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BIT", "GEOMETRY":
		return base64.StdEncoding.EncodeToString(b)
	}
	return s
}
