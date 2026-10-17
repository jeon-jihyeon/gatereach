package clients

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// Only a connection that dropped before sending is retried so a write never runs twice
func TestMySQLDisconnected(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		err  error
		want bool
	}{
		{"dead before writing", driver.ErrBadConn, true},
		{"wrapped dead before writing", fmt.Errorf("query: %w", driver.ErrBadConn), true},
		{"connection already closed", sql.ErrConnDone, true},
		{"dropped after the statement was sent", mysql.ErrInvalidConn, false},
		{"server error", errors.New("Error 1146"), false},
		{"no error", nil, false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, disconnected(tc.err))
		})
	}
}

func TestMySQLCheck(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		sql  string
		want bool
	}{
		{"select 1", false},
		{"select 1;", false},
		{"select 1;  -- done\n", false},
		{"select 1; /* done */", false},
		{"select ';' as s", false},
		{"select 'it''s; fine'", false},
		{"select `a;b` from t", false},
		{"select 1 -- ; delete from t", false},
		{"select 1 # ; delete", false},
		{"select 1; select 2", true},
		{"select 1;select 2", true},
		{"select 1; /*! delete from t */", true},
		{"select 1; /*+ SET_VAR(sort_buffer_size = 1M) */", true},
		{"select 1; /*M! delete from t */", true},
		{"select /*+ MAX_EXECUTION_TIME(1000) */ 1", false},
		{"select 1; # done", false},
		{"select 1; 'x'", true},
		{"select 1 /* ; */; delete from t", true},
		{"select 1 --; delete from t", true},
		{`select '\\'; delete from t`, true},
		{`select 'a\'; delete from t; -- '`, true},
		{`select "a\"; b"`, true},
	}
	for _, tc := range tcs {
		t.Run(tc.sql, func(t *testing.T) {
			t.Parallel()
			got := mysqlCheck(tc.sql, false) != nil
			assert.Equal(t, tc.want, got)
		})
	}
}

// Without --write only a query that can't lock or stall the shared server passes
func TestMySQLCheckReadOnly(t *testing.T) {
	t.Parallel()
	const (
		keyword = "read-only mode allows only SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, TABLE and VALUES\nuse --write to write"
		analyze = "read-only mode allows EXPLAIN ANALYZE only on SELECT, WITH or TABLE\nuse --write to run it anyway"
		sleep   = "read-only mode refuses SLEEP(): it can hold locks or load the server\nuse --write to run it anyway"
		update  = "read-only mode refuses FOR UPDATE: it locks the rows it reads\nuse --write to run it anyway"
		outfile = "read-only mode refuses INTO OUTFILE: it writes a file on the server\nuse --write to run it anyway"
		with    = "read-only mode allows WITH only before SELECT\nuse --write to write"
	)
	type args struct {
		sql   string
		write bool
	}
	tcs := []struct {
		name string
		args args
		want string
	}{
		{"select", args{"select * from t", false}, ""},
		{"with", args{"WITH x AS (SELECT 1) SELECT * FROM x", false}, ""},
		{"with leading a delete", args{"WITH x AS (SELECT 1) DELETE FROM t", false}, with},
		{"with leading an update", args{"with x as (select 1) update t set a = 1", false}, with},
		{"explain analyze of a with delete", args{"EXPLAIN ANALYZE WITH x AS (SELECT 1) DELETE FROM t", false}, with},
		{"with naming a quoted update column", args{"WITH x AS (SELECT 1 AS `update`) SELECT * FROM x", false}, ""},
		{"with leading a delete with --write", args{"WITH x AS (SELECT 1) DELETE FROM t", true}, ""},
		{"show", args{"show tables", false}, ""},
		{"desc", args{"desc t", false}, ""},
		{"describe", args{"DESCRIBE t", false}, ""},
		{"explain of a write runs nothing", args{"EXPLAIN DELETE FROM t", false}, ""},
		{"table", args{"TABLE t", false}, ""},
		{"values", args{"VALUES ROW(1, 2)", false}, ""},
		{"parentheses before the keyword", args{"((select 1) union (select 2))", false}, ""},
		{"comments before the keyword", args{"-- note\n# more\n/* why */ select 1", false}, ""},
		{"executable comment with a version", args{"/*!50000 SELECT */ 1", false}, ""},
		{"delete", args{"DELETE FROM t", false}, keyword},
		{"set", args{"SET @x = 1", false}, keyword},
		{"call", args{"CALL p()", false}, keyword},
		{"do", args{"DO SLEEP(1)", false}, keyword},
		{"a word that starts like SELECT", args{"SELECTX 1", false}, keyword},
		{"an empty query", args{" -- only a comment", false}, keyword},
		{"a keyword hidden in an executable comment", args{"/*!50000 DELETE */ FROM t", false}, keyword},
		{"a keyword inside a plain comment is skipped", args{"/* select */ delete from t", false}, keyword},
		{"explain analyze of a select", args{"EXPLAIN ANALYZE SELECT * FROM t", false}, ""},
		{"explain analyze with a format", args{"explain analyze format = tree (select 1)", false}, ""},
		{"explain analyze of a delete", args{"EXPLAIN ANALYZE DELETE FROM t", false}, analyze},
		{"desc analyze of an update", args{"DESC FORMAT=TREE ANALYZE UPDATE t SET a = 1", false}, analyze},
		{"sleep", args{"select sleep(1)", false}, sleep},
		{"sleep in any case", args{"SELECT Sleep(1)", false}, sleep},
		{"sleep with a space before the parenthesis", args{"select sleep (1)", false}, sleep},
		{"sleep with a comment before the parenthesis", args{"select sleep/* x */(1)", false}, sleep},
		{"sleep in an executable comment", args{"select 1 /*!50000 SLEEP(10)*/", false}, sleep},
		{"sleep in a MariaDB executable comment", args{"select 1 /*M!100000 sleep(10) */", false}, sleep},
		{"sleep in a where clause", args{"select * from t where a = 1 and sleep(1) = 0", false}, sleep},
		{"sleep called by a quoted name", args{"select `sleep`(1)", false}, sleep},
		{"a column named sleep", args{"select `sleep` from t", false}, ""},
		{"a plain column named sleep", args{"select sleep from t", false}, ""},
		{"sleep in a string", args{"select 'sleep(1)'", false}, ""},
		{"sleep in a comment", args{"select 1 -- sleep(1)", false}, ""},
		{"sleep in a plain block comment", args{"select 1 /* sleep(1) */", false}, ""},
		{"a name that ends in sleep", args{"select nosleep(1)", false}, ""},
		{"a string that hides sleep only without backslash escapes", args{`select 'a\' , sleep(1) -- '`, false}, sleep},
		{"benchmark", args{"select benchmark(1000000, md5('x'))", false},
			"read-only mode refuses BENCHMARK(): it can hold locks or load the server\nuse --write to run it anyway"},
		{"get_lock", args{"select get_lock('acme', 10)", false},
			"read-only mode refuses GET_LOCK(): it can hold locks or load the server\nuse --write to run it anyway"},
		{"release_all_locks", args{"select release_all_locks()", false},
			"read-only mode refuses RELEASE_ALL_LOCKS(): it can hold locks or load the server\nuse --write to run it anyway"},
		{"source_pos_wait", args{"select source_pos_wait('log.1', 4)", false},
			"read-only mode refuses SOURCE_POS_WAIT(): it can hold locks or load the server\nuse --write to run it anyway"},
		{"wait_for_executed_gtid_set", args{"select wait_for_executed_gtid_set('x')", false},
			"read-only mode refuses WAIT_FOR_EXECUTED_GTID_SET(): it can hold locks or load the server\n" +
				"use --write to run it anyway"},
		{"for update", args{"select * from t for update", false}, update},
		{"for update across a comment", args{"select * from t for /* x */ update", false}, update},
		{"for update in a subquery", args{"select * from (select * from t for update) x", false}, update},
		{"for share", args{"select * from t FOR SHARE", false},
			"read-only mode refuses FOR SHARE: it locks the rows it reads\nuse --write to run it anyway"},
		{"lock in share mode", args{"select * from t lock in share mode", false},
			"read-only mode refuses LOCK IN SHARE MODE: it locks the rows it reads\nuse --write to run it anyway"},
		{"for update in a string", args{"select 'for update' from t", false}, ""},
		{"a column named update", args{"select `update` from t", false}, ""},
		{"into outfile", args{"select * from t into outfile '/tmp/x'", false}, outfile},
		{"into dumpfile", args{"select a from t INTO DUMPFILE '/tmp/x'", false},
			"read-only mode refuses INTO DUMPFILE: it writes a file on the server\nuse --write to run it anyway"},
		{"into a variable", args{"select a into @x from t", false}, ""},
		{"into outfile in an executable comment", args{"select * from t /*! into outfile '/tmp/x' */", false}, outfile},
		{"--write allows a delete", args{"DELETE FROM t", true}, ""},
		{"--write allows sleep", args{"select sleep(1)", true}, ""},
		{"--write allows for update", args{"select * from t for update", true}, ""},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, errText(mysqlCheck(tc.args.sql, tc.args.write)))
		})
	}
}

func TestMySQLStatement(t *testing.T) {
	t.Parallel()
	type args struct {
		q     Query
		reset bool
	}
	tcs := []struct {
		name string
		args args
		want string
	}{
		{"read only rolls back and sets the session", args{Query{Text: "select 1", Limit: 5}, false},
			"ROLLBACK;\nSET SESSION transaction_read_only = ON, sql_select_limit = 5;\nselect 1"},
		{"write keeps an open transaction", args{Query{Text: "COMMIT", Write: true, Limit: 5}, false},
			"SET SESSION transaction_read_only = OFF, sql_select_limit = 5;\nCOMMIT"},
		{"a timeout becomes max_execution_time",
			args{Query{Text: "select 1", Limit: 5, Timeout: 1500 * time.Millisecond}, false},
			"ROLLBACK;\nSET SESSION transaction_read_only = ON, sql_select_limit = 5, max_execution_time = 1500;\nselect 1"},
		{"a timeout under a millisecond rounds up", args{Query{Text: "select 1", Limit: 5, Timeout: time.Microsecond}, false},
			"ROLLBACK;\nSET SESSION transaction_read_only = ON, sql_select_limit = 5, max_execution_time = 1;\nselect 1"},
		{"an earlier timeout is reset", args{Query{Text: "select 1", Limit: 5}, true},
			"ROLLBACK;\nSET SESSION transaction_read_only = ON, sql_select_limit = 5, max_execution_time = 0;\nselect 1"},
		{"a budget adds no statement",
			args{Query{Text: "select 1", Limit: 5, Use: func(float64) error { return nil }}, false},
			"ROLLBACK;\nSET SESSION transaction_read_only = ON, sql_select_limit = 5;\nselect 1"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, mysqlStatement(tc.args.q, tc.args.reset))
		})
	}
}

func TestMySQLError(t *testing.T) {
	t.Parallel()
	interrupted := &mysql.MySQLError{Number: 3024, Message: "Query execution was interrupted"}
	other := &mysql.MySQLError{Number: 1146, Message: "Table 'acme.t' doesn't exist"}
	tcs := []struct {
		name    string
		err     error
		timeout time.Duration
		want    string
	}{
		{"max_execution_time reads as the timeout", interrupted, 2 * time.Second, "query ran past --timeout 2s"},
		{"a wrapped 3024 too", fmt.Errorf("read: %w", interrupted), 2 * time.Second, "query ran past --timeout 2s"},
		{"a 3024 from the query's own hint stays", interrupted, 0, interrupted.Error()},
		{"another server error stays", other, 2 * time.Second, other.Error()},
		{"no error stays none", nil, 2 * time.Second, ""},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, errText(mysqlError(tc.err, tc.timeout)))
		})
	}
}

func TestMySQLValue(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		typ  string
		raw  sql.RawBytes
		want any
	}{
		{"int", "INT", sql.RawBytes("42"), int64(42)},
		{"unsigned past int64", "UNSIGNED BIGINT", sql.RawBytes("18446744073709551615"), "18446744073709551615"},
		{"decimal keeps precision", "DECIMAL", sql.RawBytes("12.30"), "12.30"},
		{"double", "DOUBLE", sql.RawBytes("1.5"), 1.5},
		{"json parsed", "JSON", sql.RawBytes(`{"a":1,"b":[1.5]}`), map[string]any{"a": int64(1), "b": []any{1.5}}},
		{"binary as base64", "VARBINARY", sql.RawBytes{0xff, 0x00}, "/wA="},
		{"datetime as text", "DATETIME", sql.RawBytes("2026-10-15 10:00:00"), "2026-10-15 10:00:00"},
		{"the string NULL", "VARCHAR", sql.RawBytes("NULL"), "NULL"},
		{"null", "VARCHAR", nil, nil},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, mysqlValue(tc.typ, tc.raw))
		})
	}
}

// The rows go out first and the measuring statement runs only with --max
func TestMySQLRead(t *testing.T) {
	t.Parallel()
	type args struct {
		limit int
		max   bool
		// What Use returns
		useErr error
	}
	type want struct {
		rows int
		used []float64
		err  string
	}
	tcs := []struct {
		name string
		args args
		init func(m sqlmock.Sqlmock)
		want want
	}{
		{"without --max nothing is measured", args{limit: 2}, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2))
		}, want{rows: 2}},
		{"--max takes the rows examined", args{limit: 2, max: true}, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			m.ExpectQuery("ROWS_EXAMINED").WillReturnRows(sqlmock.NewRows([]string{"r"}).AddRow(42))
		}, want{rows: 1, used: []float64{42}}},
		{"the paid rows come out before the budget", args{limit: 2, max: true, useErr: ErrBudget}, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2))
			m.ExpectQuery("ROWS_EXAMINED").WillReturnRows(sqlmock.NewRows([]string{"r"}).AddRow(42))
		}, want{rows: 2, used: []float64{42}, err: ErrBudget.Error()}},
		{"a measuring statement that fails ends after the rows", args{limit: 2, max: true}, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			m.ExpectQuery("ROWS_EXAMINED").WillReturnError(errors.New("Error 1142"))
		}, want{rows: 1, err: "mysql --max needs performance_schema statement history: Error 1142"}},
		{"--max without statement history fails after the rows", args{limit: 2, max: true}, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			m.ExpectQuery("ROWS_EXAMINED").WillReturnRows(sqlmock.NewRows([]string{"r"}))
		}, want{rows: 1, err: "mysql --max needs performance_schema statement history: " +
			"no statement recorded for this connection"}},
		{"a failed query measures nothing", args{limit: 2, max: true}, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT 1").WillReturnError(errors.New("Error 1146"))
		}, want{err: "Error 1146"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, m, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			tc.init(m)
			c := &mysqlClient{sess: mustConn(t, db)}
			var used []float64
			q := Query{Text: "SELECT 1", Limit: tc.args.limit}
			if tc.args.max {
				q.Use = func(amount float64) error { used = append(used, amount); return tc.args.useErr }
			}
			rows := 0
			err = c.Read(context.Background(), q, func(record.Record) error { rows++; return nil })
			assert.Equal(t, tc.want, want{rows: rows, used: used, err: errText(err)})
			assert.NoError(t, m.ExpectationsWereMet())
		})
	}
}

// A cancel sends KILL QUERY over another connection and Read returns only after it ends
func TestMySQLReadCancel(t *testing.T) {
	t.Parallel()
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	m.ExpectQuery("SELECT SLEEP").WillDelayFor(time.Minute).WillReturnRows(sqlmock.NewRows([]string{"n"}))
	// The kill's connection lands here and is dropped so the kill fails fast
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	dialed := make(chan struct{}, 1)
	go func() {
		if conn, err := ln.Accept(); err == nil {
			dialed <- struct{}{}
			_ = conn.Close()
		}
	}()
	cfg := mysql.NewConfig()
	cfg.Net, cfg.Addr = "tcp", ln.Addr().String()
	c := &mysqlClient{sess: mustConn(t, db), cfg: cfg, id: 7}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = c.Read(ctx, Query{Text: "SELECT SLEEP(60)", Limit: 1}, func(record.Record) error { return nil })
	assert.Error(t, err)
	assert.Len(t, dialed, 1)
}

func mustConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	c, err := db.Conn(context.Background())
	require.NoError(t, err)
	return c
}
