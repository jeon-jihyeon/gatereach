// Package cli는 모든 패키지를 아는 유일한 진입점
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/sinks"
	"github.com/jeon-jihyeon/gatereach/internal/tunnels"
	"github.com/jeon-jihyeon/gatereach/internal/version"
)

const (
	exitError = 1
	exitUsage = 2
	// 중단으로 끝난 단건 조회
	exitInterrupted = 130
)

type app struct {
	cfg    *config.Config
	stdout io.Writer
	stderr io.Writer
}

type command struct {
	usage  string
	help   string
	run    func(a *app, args []string) error
	hidden bool
}

func commands() map[string]command {
	return map[string]command{
		"ls":    {usage: "ls", help: "연결 목록과 터널과 세션 상태", run: (*app).ls},
		"open":  {usage: "open <alias>", help: "터널 열기와 세션 시작", run: (*app).open},
		"close": {usage: "close <alias|all>", help: "세션 종료와 gatereach가 연 터널 닫기", run: (*app).close},
		"q": {
			usage: "q <alias> <query> [options]  또는  q <alias> --file <path> [options]",
			help:  "조회 결과를 레코드마다 JSON 한 줄로 출력하거나 --out 폴더에 저장\n닫힌 터널과 세션은 자동으로 열고 유지",
			run:   (*app).query,
		},
		"discover": {usage: "discover " + strings.Join(tunnels.Names(), "|"), help: "터널로 접근 가능한 대상 목록", run: (*app).discover},
		"add":      {usage: "add <alias> --client " + strings.Join(clients.Names(), "|") + " [options]", help: "설정 파일에 연결 추가", run: (*app).add},
		"serve":    {usage: "serve <alias>", run: (*app).serve, hidden: true},
	}
}

// 명령이 돌려준 오류의 종료 코드
type exitCode struct {
	code int
	msg  string
}

func (e exitCode) Error() string { return e.msg }

func Run(args []string, stdout, stderr io.Writer) int {
	cmds := commands()
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		usage(stderr, cmds)
		return exitUsage
	}
	if args[0] == "--version" || args[0] == "version" {
		fmt.Fprintln(stdout, version.Version)
		return 0
	}
	cmd, ok := cmds[args[0]]
	if !ok {
		fmt.Fprintf(stderr, "알 수 없는 명령: %s\n", args[0])
		usage(stderr, cmds)
		return exitUsage
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}
	a := &app{cfg: cfg, stdout: stdout, stderr: stderr}
	err = cmd.run(a, args[1:])
	var code exitCode
	switch {
	case err == nil:
		return 0
	case errors.As(err, &code):
		if code.msg != "" {
			fmt.Fprintln(stderr, code.msg)
		}
		return code.code
	case errors.Is(err, flag.ErrHelp):
		return exitUsage
	}
	fmt.Fprintln(stderr, err)
	return exitError
}

func usage(w io.Writer, cmds map[string]command) {
	fmt.Fprintln(w, "gatereach: 터널과 자격 증명 뒤의 데이터 소스를 별칭으로 조회")
	names := make([]string, 0, len(cmds))
	for n, c := range cmds {
		if !c.hidden {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(w, "  gatereach %s\n      %s\n", cmds[n].usage, strings.ReplaceAll(cmds[n].help, "\n", "\n      "))
	}
}

func usageError(name string) error {
	return fmt.Errorf("사용법: gatereach %s", commands()[name].usage)
}

// 플래그와 위치 인자를 섞어 쓸 수 있게 반복 해석
// `--` 뒤는 모두 위치 인자라 -로 시작하는 쿼리도 받음
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if consumed := len(args) - len(rest); consumed > 0 && args[consumed-1] == "--" {
			return append(pos, rest...), nil
		}
		if len(rest) == 0 {
			break
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
	return pos, nil
}

func newFlags(name string, w io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("gatereach "+name, flag.ContinueOnError)
	fs.SetOutput(w)
	return fs
}

func (a *app) conn(alias string) (config.Conn, error) {
	c, ok := a.cfg.Conns[alias]
	if !ok {
		known := strings.Join(a.cfg.Aliases, " ")
		if known == "" {
			known = "없음"
		}
		return config.Conn{}, fmt.Errorf("알 수 없는 별칭: %s\n가능: %s", alias, known)
	}
	return c, nil
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func formatHelp() string {
	return "--out 데이터 파일 형식: " + strings.Join(sinks.FormatNames(), " 또는 ")
}
