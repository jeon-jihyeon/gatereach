package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/record"
	"github.com/jeon-jihyeon/gatereach/internal/session"
	"github.com/jeon-jihyeon/gatereach/internal/sinks"
	"github.com/jeon-jihyeon/gatereach/internal/tunnels"
	"github.com/jeon-jihyeon/gatereach/internal/version"
)

const (
	DefaultLimit    = 1000
	DefaultInterval = 5 * time.Second
)

type queryOptions struct {
	alias    string
	text     string
	write    bool
	limit    int
	out      string
	format   string
	follow   bool
	interval time.Duration
	key      string
}

// 쿼리는 위치 인자나 --file 중 하나
// 파일은 명시할 때만 읽어 쿼리와 같은 이름의 파일이 끼어들지 않음
func (a *app) parseQuery(args []string) (queryOptions, error) {
	fs := newFlags("q", a.stderr)
	var o queryOptions
	file := fs.String("file", "", "쿼리를 담은 파일")
	fs.BoolVar(&o.write, "write", false, "읽기 전용 해제")
	fs.IntVar(&o.limit, "limit", DefaultLimit, "한 번 조회의 최대 레코드 수")
	fs.StringVar(&o.out, "out", "", "결과를 Data Package 폴더로 저장")
	fs.StringVar(&o.format, "format", "arrow", formatHelp())
	fs.BoolVar(&o.follow, "follow", false, "종료할 때까지 새 레코드를 계속 출력")
	seconds := fs.Float64("interval", DefaultInterval.Seconds(), "폴링 주기 초")
	fs.StringVar(&o.key, "key", "", "폴링 중복 판별 필드\n없으면 레코드 전체")
	pos, err := parse(fs, args)
	if err != nil {
		return o, err
	}
	switch {
	case *file != "" && len(pos) == 1:
		b, err := os.ReadFile(*file)
		if err != nil {
			return o, err
		}
		o.alias, o.text = pos[0], string(b)
	case *file == "" && len(pos) == 2:
		o.alias, o.text = pos[0], pos[1]
	default:
		return o, usageError("q")
	}
	if o.limit < 1 {
		return o, errors.New("--limit은 1 이상")
	}
	if _, ok := sinks.Formats[o.format]; !ok {
		return o, fmt.Errorf("지원하지 않는 형식: %s", o.format)
	}
	if *seconds <= 0 {
		return o, errors.New("--interval은 0보다 커야 함")
	}
	o.interval = time.Duration(*seconds * float64(time.Second))
	return o, nil
}

func (a *app) query(args []string) error {
	o, err := a.parseQuery(args)
	if err != nil {
		return err
	}
	interrupted, err := a.runQuery(o)
	return exitFor(o, interrupted, err)
}

// follow를 Ctrl-C나 SIGTERM으로 멈추면 정상 종료
// 단건 조회를 멈추면 중단으로 기록하고 130
// 출력 닫기 실패는 중단보다 먼저 알려 1로 끝냄
func exitFor(o queryOptions, interrupted bool, err error) error {
	switch {
	case err != nil:
		return err
	case interrupted && !o.follow:
		return exitCode{code: exitInterrupted, msg: "중단됨"}
	}
	return nil
}

// 소스는 세션이 맡고 출력은 Sink가 맡음
// 둘을 잇는 차례는 여기에만 둠
// 터널과 세션보다 쓰기 차단 규칙을 먼저 확인
func (a *app) runQuery(o queryOptions) (bool, error) {
	c, err := a.conn(o.alias)
	if err != nil {
		return false, err
	}
	kind, err := clients.Get(c)
	if err != nil {
		return false, err
	}
	if err := kind.Check(o.text, o.write); err != nil {
		return false, err
	}
	if err := sinks.CheckFolder(o.out); err != nil {
		return false, err
	}
	tunnel, started, err := a.prepare(o.alias, c, kind)
	if err != nil {
		return false, err
	}
	if tunnel == tunnels.Opened || tunnel == tunnels.Foreign {
		fmt.Fprintf(a.stderr, "%s %s\n", o.alias, tunnelMessages[tunnel])
	}
	if started {
		fmt.Fprintf(a.stderr, "%s 세션 시작\n", o.alias)
	}
	meta := sinks.Meta{
		Version: version.Version, Alias: o.alias, Client: c.Client, User: c.User,
		Query: o.text, Limit: o.limit, Write: o.write, Follow: o.follow, Started: time.Now(),
	}
	sink, err := sinks.Get(o.out, o.format, meta, a.stdout, a.stderr)
	if err != nil {
		return false, err
	}
	// SIGTERM도 Ctrl-C처럼 마무리
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	req := session.Request{
		Version: version.Version, Alias: o.alias, Query: o.text, Write: o.write,
		Limit: o.limit, Follow: o.follow, Interval: o.interval, Key: o.key,
	}
	end, runErr := session.Query(ctx, req, writer(sink))
	// 이전 버전 세션은 아무것도 보내지 않고 거부하므로 다시 띄운 세션에 한 번 더 보냄
	if errors.Is(runErr, session.ErrOutdated) {
		if runErr = a.restart(o.alias); runErr == nil {
			end, runErr = session.Query(ctx, req, writer(sink))
		}
	}
	interrupted := ctx.Err() != nil
	switch {
	case interrupted && o.follow:
		runErr = nil
	case interrupted:
		runErr = errors.New("중단됨")
	}
	if end.Reconnected {
		fmt.Fprintf(a.stderr, "%s 연결이 끊겨 다시 맺음\n세션 변수와 임시 테이블 초기화\n", o.alias)
	}
	if err := sink.Close(runErr); err != nil {
		return interrupted, err
	}
	if interrupted {
		return true, nil
	}
	return false, runErr
}

// 다른 버전의 세션을 멈추고 이 바이너리로 다시 띄움
func (a *app) restart(alias string) error {
	if _, err := session.Stop(alias); err != nil {
		return err
	}
	if _, err := session.Ensure(alias); err != nil {
		return err
	}
	fmt.Fprintf(a.stderr, "%s 세션을 새 버전으로 다시 시작\n", alias)
	return nil
}

// stdout JSON lines는 세션이 보낸 줄을 그대로 쓰고 폴더 출력만 레코드로 해석
func writer(sink sinks.Sink) func(line []byte) error {
	if raw, ok := sink.(sinks.RawWriter); ok {
		return raw.WriteRaw
	}
	return func(line []byte) error {
		r, err := record.FromJSON(line)
		if err != nil {
			return err
		}
		return sink.Write(r)
	}
}
