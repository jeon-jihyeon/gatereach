// Package session은 별칭마다 소스 연결을 쥐고 Unix 소켓으로 요청을 받는 상주 프로세스
package session

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/proc"
	"github.com/jeon-jihyeon/gatereach/internal/record"
	"github.com/jeon-jihyeon/gatereach/internal/secrets"
)

const (
	startWait = 20 * time.Second
	startTick = 100 * time.Millisecond
	// 단건 조회가 쥔 연결의 차례를 기다리는 시간
	busyWait = 30 * time.Second
	// shutdown 요청 뒤 소켓이 닫히길 기다리는 시간
	stopWait = 5 * time.Second
	stopTick = 100 * time.Millisecond
	dialWait = time.Second
	// macOS sun_path 104바이트에서 끝 NUL을 뺀 길이
	maxSocketPath = 103
	// 한 줄 레코드의 최대 크기
	maxLine = 256 << 20
)

type Op string

const (
	OpQuery    Op = ""         // 쿼리
	OpShutdown Op = "shutdown" // 세션 종료
)

// 요청 한 줄
type Request struct {
	Op Op `json:"op,omitempty"`
	// 세션을 띄운 바이너리의 버전과 다르면 거부해 새 바이너리가 세션을 이어받음
	Version string `json:"version,omitempty"`
	// 소켓 이름이 겹쳐도 다른 별칭의 세션에 쿼리가 가지 않도록 대조
	Alias    string        `json:"alias"`
	Query    string        `json:"query,omitempty"`
	Write    bool          `json:"write,omitempty"`
	Limit    int           `json:"limit,omitempty"`
	Follow   bool          `json:"follow,omitempty"`
	Interval time.Duration `json:"interval,omitempty"`
	Key      string        `json:"key,omitempty"`
}

// 응답의 마지막 줄
type End struct {
	Rows  int    `json:"rows"`
	Error string `json:"error,omitempty"`
	// 끊긴 연결을 다시 맺어 세션 변수와 임시 테이블이 초기화됨
	Reconnected bool `json:"reconnected,omitempty"`
	// 세션 버전이 요청과 달라 아무것도 실행하지 않음
	Outdated bool `json:"outdated,omitempty"`
}

// 세션이 다른 버전의 바이너리로 떠 있음
// 호출자가 세션을 다시 띄운 뒤 요청을 다시 보냄
var ErrOutdated = errors.New("세션 버전이 다름")

// 클라이언트를 거치지 않은 요청도 막는 값 범위
func (r Request) check() error {
	if r.Limit < 1 {
		return errors.New("limit은 1 이상")
	}
	if r.Follow && r.Interval <= 0 {
		return errors.New("interval은 0보다 커야 함")
	}
	return nil
}

// 레코드 줄은 `{"record":...}`
// 그 밖의 줄은 End
const recordPrefix = `{"record":`

// 설정 폴더 아래 run
// 경로가 길어 소켓 이름이 sun_path 한도를 넘으면 `/tmp` 아래 사용자와 설정 폴더별 폴더
func Dir() string {
	d := filepath.Join(config.Dir(), "run")
	if len(d)+len("/00000000.sock") <= maxSocketPath {
		return d
	}
	return fmt.Sprintf("/tmp/gatereach-%d-%08x", os.Getuid(), crc32.ChecksumIEEE([]byte(config.Dir())))
}

// 별칭마다 소켓 하나
// 다른 별칭이 같은 파일 이름이 되지 않도록 별칭 해시를 덧붙임
// 경로가 sun_path 한도를 넘으면 해시만 사용
func SocketPath(alias string) string {
	p := filepath.Join(Dir(), config.FileKey(alias)+".sock")
	if len(p) <= maxSocketPath {
		return p
	}
	return filepath.Join(Dir(), config.AliasHash(alias)+".sock")
}

func lockPath(alias string) string {
	return filepath.Join(Dir(), filepath.Base(SocketPath(alias))+".lock")
}

// 소켓 폴더 생성과 검사
// 거부 대상
// 1. 심볼릭 링크이거나 폴더가 아닌 경로
// 2. 현재 사용자가 아닌 소유자
// 3. 그룹이나 다른 사용자 권한
func ensureDir() error {
	d := Dir()
	if err := os.MkdirAll(d, 0o700); err != nil {
		return err
	}
	return checkDir(d)
}

func checkDir(d string) error {
	fi, err := os.Lstat(d)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return fmt.Errorf("세션 폴더가 실제 폴더가 아님: %s", d)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != os.Getuid() {
		return fmt.Errorf("세션 폴더 소유자가 현재 사용자가 아님: %s", d)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("세션 폴더 권한이 0700이 아님: %s", d)
	}
	return nil
}

// 폴더와 소켓 반대편이 모두 현재 사용자 것일 때만 연결
// 가로챈 폴더나 다른 사용자의 가짜 세션에 쿼리를 보내지 않음
func Dial(alias string) (*net.UnixConn, error) {
	if err := checkDir(Dir()); err != nil {
		return nil, err
	}
	c, err := net.DialTimeout("unix", SocketPath(alias), dialWait)
	if err != nil {
		return nil, err
	}
	uc := c.(*net.UnixConn)
	uid, err := peerUID(uc)
	if err != nil || uid != os.Getuid() {
		_ = uc.Close()
		return nil, fmt.Errorf("%s 세션 소켓의 상대가 현재 사용자가 아님", alias)
	}
	return uc, nil
}

func Alive(alias string) bool {
	c, err := Dial(alias)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// 세션이 없으면 띄움
// 같은 별칭을 동시에 띄우지 않도록 잠금 파일에 flock
// 잠금은 프로세스가 끝나면 커널이 풀어 상태 파일이 아님
func Ensure(alias string) (started bool, err error) {
	if Alive(alias) {
		return false, nil
	}
	if err := ensureDir(); err != nil {
		return false, err
	}
	unlock, err := proc.Lock(lockPath(alias))
	if err != nil {
		return false, err
	}
	defer unlock()
	if Alive(alias) {
		return false, nil
	}
	// 죽은 세션이 남긴 소켓 정리
	if err := os.Remove(SocketPath(alias)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, start(alias, startWait)
}

// 실행 파일 자신을 serve로 띄우고 소켓이 연결을 받을 때까지 대기
func start(alias string, wait time.Duration) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	path := config.LogPath(alias, ".session.log")
	err = proc.Spawn([]string{exe, "serve", alias}, os.Environ(), path, func() bool { return Alive(alias) }, wait, startTick)
	var exit *proc.ExitError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &exit):
		return fmt.Errorf("%s 세션 시작 실패: %s\n%s\n로그: %s", alias, exit, proc.Tail(path, 5), path)
	case errors.Is(err, proc.ErrTimeout):
		return fmt.Errorf("%s 세션 시작 실패: %s 안에 연결되지 않음\n%s\n로그: %s", alias, wait, proc.Tail(path, 5), path)
	}
	return fmt.Errorf("%s 세션 실행 실패: %w", alias, err)
}

// shutdown 뒤 wait 안에 소켓이 사라지지 않으면 소켓 반대편 프로세스에 SIGTERM
// 폴더나 소켓이 없거나 연결이 거부되면 세션이 없어 false
// 그 밖의 연결 실패는 세션이 남아 있을 수 있어 오류
func Stop(alias string) (bool, error) {
	return stop(alias, stopWait)
}

func stop(alias string, wait time.Duration) (bool, error) {
	c, err := Dial(alias)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%s 세션 종료 실패: %w", alias, err)
	}
	pid, _ := peerPID(c)
	deadline := time.Now().Add(wait)
	_ = json.NewEncoder(c).Encode(Request{Op: OpShutdown, Alias: alias})
	_ = c.SetReadDeadline(deadline)
	_, _ = io.Copy(io.Discard, c)
	_ = c.Close()
	if waitGone(alias, deadline) {
		return true, nil
	}
	if pid > 0 {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return false, err
		}
	}
	if !waitGone(alias, time.Now().Add(wait)) {
		return false, fmt.Errorf("%s 세션 종료 실패", alias)
	}
	return true, nil
}

func waitGone(alias string, deadline time.Time) bool {
	for ; time.Now().Before(deadline); time.Sleep(stopTick) {
		if !Alive(alias) {
			return true
		}
	}
	return false
}

// 요청 하나를 보내고 레코드 줄마다 그 JSON을 넘김
// ctx가 끝나면 연결을 닫아 세션이 쿼리를 취소
func Query(ctx context.Context, req Request, emit func(line []byte) error) (End, error) {
	c, err := Dial(req.Alias)
	if err != nil {
		return End{}, fmt.Errorf("%s 세션 연결 실패: %w", req.Alias, err)
	}
	defer func() { _ = c.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		return End{}, err
	}
	sc := bufio.NewScanner(c)
	sc.Buffer(make([]byte, 64*1024), maxLine)
	for sc.Scan() {
		line := sc.Bytes()
		if !bytes.HasPrefix(line, []byte(recordPrefix)) {
			var end End
			if err := json.Unmarshal(line, &end); err != nil {
				return End{}, fmt.Errorf("%s 세션 응답 해석 실패: %w", req.Alias, err)
			}
			switch {
			case end.Outdated:
				return end, fmt.Errorf("%w: %s", ErrOutdated, end.Error)
			case end.Error != "":
				return end, errors.New(end.Error)
			}
			return end, nil
		}
		// 레코드 줄은 접두어 뒤에 값과 닫는 중괄호가 있어야 함
		if len(line) <= len(recordPrefix)+1 || line[len(line)-1] != '}' {
			return End{}, fmt.Errorf("%s 세션 응답 해석 실패: 잘린 레코드 줄", req.Alias)
		}
		if err := emit(line[len(recordPrefix) : len(line)-1]); err != nil {
			return End{}, err
		}
	}
	if ctx.Err() != nil {
		return End{}, ctx.Err()
	}
	if err := sc.Err(); err != nil {
		return End{}, err
	}
	return End{}, fmt.Errorf("%s 세션이 응답 중에 끊김", req.Alias)
}

// 상주 세션 본체
type server struct {
	alias string
	// 세션을 띄운 바이너리의 버전
	version  string
	conn     config.Conn
	kind     clients.Kind
	password string
	ln       *net.UnixListener

	// 단건 조회가 쓰는 연결 하나
	// 크기 1 채널이 차례를 정하고 client 교체도 차례 안에서만 일어남
	turn   chan struct{}
	client clients.Client
	// 차례를 기다리는 최대 시간
	busy time.Duration

	quit     chan struct{}
	quitOnce sync.Once
}

// 연결을 먼저 맺은 뒤 소켓을 열어 소켓이 보이면 곧 준비 완료
// 비밀값은 여기서 한 번만 해석해 재연결과 follow 연결이 재사용
func Serve(ctx context.Context, alias, version string, conn config.Conn) error {
	return serve(ctx, alias, version, conn, busyWait)
}

func serve(ctx context.Context, alias, version string, conn config.Conn, busy time.Duration) error {
	kind, err := clients.Get(conn)
	if err != nil {
		return err
	}
	if err := ensureDir(); err != nil {
		return err
	}
	password, err := secrets.Resolve(ctx, conn.Secret)
	if err != nil {
		return err
	}
	client := kind.New(conn, password)
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("%s 연결 실패: %w", alias, err)
	}
	s := &server{
		alias: alias, version: version, conn: conn, kind: kind, password: password, client: client,
		busy: busy, turn: make(chan struct{}, 1), quit: make(chan struct{}),
	}
	defer func() { _ = s.client.Close() }()
	old := syscall.Umask(0o077)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: SocketPath(alias), Net: "unix"})
	syscall.Umask(old)
	if err != nil {
		return err
	}
	ln.SetUnlinkOnClose(true)
	s.ln = ln
	fmt.Printf("%s 세션 준비 %s\n", alias, SocketPath(alias))
	return s.loop(ctx)
}

// shutdown이나 종료 신호가 오면 진행 중인 요청도 취소해 follow가 세션을 붙잡지 않음
// Accept가 다른 이유로 실패해도 요청을 먼저 취소한 뒤 기다림
func (s *server) loop(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-s.quit:
		}
		cancel()
		_ = s.ln.Close()
	}()
	for {
		c, err := s.ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		uid, err := peerUID(c)
		if err != nil || uid != os.Getuid() {
			_ = c.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handle(ctx, c)
		}()
	}
}

func (s *server) shutdown() {
	s.quitOnce.Do(func() { close(s.quit) })
}

func (s *server) handle(parent context.Context, c *net.UnixConn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return
	}
	var req Request
	if err := json.Unmarshal(line, &req); err != nil {
		writeEnd(c, End{Error: "잘못된 요청: " + err.Error()})
		return
	}
	if req.Alias != s.alias {
		writeEnd(c, End{Error: fmt.Sprintf("%s 세션에 %s 요청", s.alias, req.Alias)})
		return
	}
	// 다른 버전의 바이너리도 이전 세션을 멈출 수 있게 버전보다 먼저 처리
	if req.Op == OpShutdown {
		writeEnd(c, End{})
		s.shutdown()
		return
	}
	if req.Version != s.version {
		writeEnd(c, End{Error: fmt.Sprintf("%s 세션 버전 %s와 요청 버전 %s가 다름", s.alias, s.version, req.Version), Outdated: true})
		return
	}
	// 요청을 보낸 쪽이 연결을 닫으면 쿼리 취소
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() {
		_, _ = io.Copy(io.Discard, r)
		cancel()
	}()
	w := bufio.NewWriter(c)
	rows := 0
	emit := func(rec record.Record) error {
		_, _ = w.WriteString(recordPrefix)
		_, _ = w.Write(rec.JSON())
		_, _ = w.WriteString("}\n")
		rows++
		// follow는 레코드가 드물어 바로 내보내고 단건 조회는 버퍼가 찰 때만
		if req.Follow {
			return w.Flush()
		}
		return nil
	}
	end := End{}
	q := clients.Query{Text: req.Query, Write: req.Write, Limit: req.Limit}
	if err := req.check(); err != nil {
		end.Error = err.Error()
	} else if err := s.kind.Check(q.Text, q.Write); err != nil {
		end.Error = err.Error()
	} else if req.Follow {
		end.Error = errText(ctx, s.follow(ctx, q, clients.Watching{Interval: req.Interval, Key: req.Key}, emit))
	} else {
		reconnected, err := s.read(ctx, q, emit)
		end.Reconnected = reconnected
		end.Error = errText(ctx, err)
	}
	end.Rows = rows
	if ctx.Err() == nil {
		_ = w.Flush()
		writeEnd(c, end)
	}
}

func errText(ctx context.Context, err error) string {
	if err == nil || ctx.Err() != nil {
		return ""
	}
	return err.Error()
}

// 단건 조회는 쥔 연결을 차례로 사용
// 끊긴 연결은 다시 맺고 한 번 재시도
// 다시 맺기에 실패하면 이전 클라이언트를 그대로 두어 다음 요청이 다시 시도
func (s *server) read(ctx context.Context, q clients.Query, emit clients.Emit) (bool, error) {
	select {
	case s.turn <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	case <-time.After(s.busy):
		return false, fmt.Errorf("%s 세션 사용 중", s.alias)
	}
	defer func() { <-s.turn }()
	err := s.client.Read(ctx, q, emit)
	if !errors.Is(err, clients.ErrDisconnected) {
		return false, err
	}
	fresh := s.kind.New(s.conn, s.password)
	if err := fresh.Connect(ctx); err != nil {
		return false, fmt.Errorf("%s 재연결 실패: %w", s.alias, err)
	}
	_ = s.client.Close()
	s.client = fresh
	return true, s.client.Read(ctx, q, emit)
}

// follow는 수명 동안 자기 연결을 따로 써 단건 조회를 막지 않음
func (s *server) follow(ctx context.Context, q clients.Query, w clients.Watching, emit clients.Emit) error {
	c := s.kind.New(s.conn, s.password)
	if err := c.Connect(ctx); err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	return clients.Watch(ctx, c, q, w, emit)
}

func writeEnd(c net.Conn, end End) {
	_ = json.NewEncoder(c).Encode(end)
}
