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

const killWait = 5 * time.Second

type mysqlClient struct {
	conn     config.Conn
	password string
	cfg      *mysql.Config
	db       *sql.DB
	// 세션 변수를 요청 사이에 이어 가려고 고정한 연결 하나
	sess *sql.Conn
	id   int64
}

func newMySQL(conn config.Conn, password string) Client {
	return &mysqlClient{conn: conn, password: password}
}

// 문장은 한 번에 하나만
// Teleport 같은 프록시는 서버에 따로 붙어 여러 문장을 실행하므로 클라이언트에서 거부
// 백슬래시 이스케이프가 꺼진 sql_mode에서도 같은 판정이 나오도록 두 해석 모두 검사
// 읽기 전용 자체는 요청마다 앞에 붙이는 세션 설정이 담당
func mysqlCheck(text string, _ bool) error {
	if multipleStatements(text, true) || multipleStatements(text, false) {
		return errors.New("MySQL은 한 번에 한 문장만 실행\n세미콜론 뒤 문장은 따로 실행")
	}
	return nil
}

// 따옴표와 주석 밖의 세미콜론 뒤에 다른 문장이 있는지
// 서버가 실행하는 실행형 주석 안은 코드로 봄
// 대상: `/*!` 시작과 `/*+` 시작과 `/*M!` 시작
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

// 따옴표 문자열의 끝 위치
// 겹친 따옴표는 문자열 안
// backslash면 백슬래시 다음 문자도 문자열 안
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
	// 1. 세션 설정과 쿼리를 한 번의 왕복으로 보내려고 여러 문장 허용
	// 2. 사용자 쿼리는 mysqlCheck가 한 문장으로 제한
	// 3. 서버가 클라이언트 파일을 읽어 가는 LOAD DATA LOCAL 차단
	// 4. 쿼리 텍스트를 그대로 보내 값 치환 없음
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
	if err := sess.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&m.id); err != nil {
		_ = sess.Close()
		_ = db.Close()
		return err
	}
	m.cfg, m.db, m.sess = cfg, db, sess
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

// 요청 앞에 붙이는 세션 설정
// 1. 읽기 전용이면 앞선 요청이 연 트랜잭션을 ROLLBACK해 START TRANSACTION READ WRITE 우회 차단
// 2. 세션 변수를 요청마다 다시 걸어 앞선 요청이 바꾼 값을 되돌림
// 3. 트랜잭션 단위 READ ONLY는 암묵적 커밋 문장 뒤에 남지 않아 세션 단위 사용
// 4. --write는 트랜잭션을 여러 요청에 걸쳐 쓸 수 있게 ROLLBACK 없음
func mysqlStatement(q Query) string {
	if q.Write {
		return fmt.Sprintf("SET SESSION transaction_read_only = OFF, sql_select_limit = %d;\n%s", q.Limit, q.Text)
	}
	return fmt.Sprintf("ROLLBACK;\nSET SESSION transaction_read_only = ON, sql_select_limit = %d;\n%s", q.Limit, q.Text)
}

func (m *mysqlClient) Read(ctx context.Context, q Query, emit Emit) error {
	if m.sess == nil {
		return ErrDisconnected
	}
	// 드라이버는 컨텍스트 취소 때 연결만 닫고 서버 쿼리는 계속 실행
	// 다른 연결로 KILL QUERY를 보내 서버 쪽도 멈춤
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			m.kill()
		case <-done:
		}
	}()
	rows, err := m.sess.QueryContext(ctx, mysqlStatement(q))
	if disconnected(err) {
		return ErrDisconnected
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	// 앞의 설정 문장은 열이 없는 결과라 열이 있는 결과까지 넘김
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
	if err := readRows(rows, q.Limit, emit); err != nil {
		return err
	}
	// 검사를 빠져나간 여러 문장이 실행됐으면 결과를 버리지 않고 오류로 알림
	if rows.NextResultSet() {
		return errors.New("여러 문장이 실행됨: 첫 문장 결과만 출력")
	}
	return rows.Err()
}

// 드라이버가 문장을 보내기 전에 끊김을 알아챈 경우만 재시도 대상
// 1. `driver.ErrBadConn`: 쓰기 전에 죽은 연결을 발견
// 2. `sql.ErrConnDone`: 이미 닫힌 연결
// `mysql.ErrInvalidConn`은 문장을 보낸 뒤에도 나와 재시도하면 쓰기가 두 번 실행될 수 있음
func disconnected(err error) bool {
	return errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone)
}

// SHOW는 sql_select_limit을 따르지 않아 행 수도 직접 제한
func readRows(rows *sql.Rows, limit int, emit Emit) error {
	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	cols := make([]string, len(types))
	for i, t := range types {
		cols[i] = t.Name()
	}
	names := record.UniqueNames(cols)
	raw := make([]sql.RawBytes, len(types))
	ptrs := make([]any, len(types))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for n := 0; n < limit && rows.Next(); n++ {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		values := make([]any, len(raw))
		for i, b := range raw {
			values[i] = mysqlValue(types[i].DatabaseTypeName(), b)
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
	ctx, cancel := context.WithTimeout(context.Background(), killWait)
	defer cancel()
	_, _ = db.ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", m.id))
}

// 열 타입별 JSON 값
// 1. 정수는 숫자
// 2. int64를 넘는 BIGINT UNSIGNED는 문자열
// 3. DECIMAL은 정밀도 보존을 위해 문자열
// 4. FLOAT와 DOUBLE은 숫자
// 5. JSON은 해석한 값
// 6. 바이너리 열은 base64 문자열
// 7. 날짜 시간 타입을 포함한 나머지는 서버가 준 텍스트
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
