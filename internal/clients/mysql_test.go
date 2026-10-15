package clients

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

// 보내기 전에 끊긴 연결만 재시도해 쓰기가 두 번 실행되지 않음
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

func TestMySQLStatement(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		q    Query
		want string
	}{
		{"read only rolls back and sets the session", Query{Text: "select 1", Limit: 5},
			"ROLLBACK;\nSET SESSION transaction_read_only = ON, sql_select_limit = 5;\nselect 1"},
		{"write keeps an open transaction", Query{Text: "COMMIT", Write: true, Limit: 5},
			"SET SESSION transaction_read_only = OFF, sql_select_limit = 5;\nCOMMIT"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, mysqlStatement(tc.q))
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
