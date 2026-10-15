package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/session"
	"github.com/jeon-jihyeon/gatereach/internal/tunnels"
	"github.com/jeon-jihyeon/gatereach/internal/version"
)

var tunnelMessages = map[tunnels.Result]string{
	tunnels.Already: "이미 열림",
	tunnels.Foreign: "다른 프로세스가 연 포트 사용",
	tunnels.Opened:  "열림",
}

func (a *app) ls([]string) error {
	for _, alias := range a.cfg.Aliases {
		c := a.cfg.Conns[alias]
		state := "-"
		if open, ok := tunnels.IsOpen(c); ok {
			state = "closed"
			if open {
				state = "open"
			}
		}
		where := c.Profile
		if c.Port != 0 {
			where = fmt.Sprintf("%s:%d", c.Host, c.Port)
		}
		tunnel := "-"
		if c.Tunnel != nil {
			tunnel = c.Tunnel.Type
		}
		live := "-"
		if session.Alive(alias) {
			live = "live"
		}
		fmt.Fprintf(a.stdout, "%-24s %-8s %-22s %-10s %-5s %-6s %s\n", alias, or(c.Client, "?"), or(where, "-"), or(c.User, "-"), tunnel, state, live)
	}
	return nil
}

// 터널과 세션 준비
// 터널 결과와 세션을 새로 시작했는지 반환
// 세션이 살아 있고 포트가 열려 있으면 프로세스 목록 확인을 건너뜀
func (a *app) prepare(alias string, c config.Conn, kind clients.Kind) (tunnels.Result, bool, error) {
	var tunnel tunnels.Result
	if kind.Networked {
		open, _ := tunnels.IsOpen(c)
		if !open || !session.Alive(alias) {
			var err error
			if tunnel, err = tunnels.Open(a.cfg, alias, c); err != nil {
				return "", false, err
			}
		}
	}
	started, err := session.Ensure(alias)
	return tunnel, started, err
}

func (a *app) open(args []string) error {
	if len(args) != 1 {
		return usageError("open")
	}
	alias := args[0]
	c, err := a.conn(alias)
	if err != nil {
		return err
	}
	kind, err := clients.Get(c)
	if err != nil {
		return err
	}
	tunnel, started, err := a.prepare(alias, c, kind)
	if err != nil {
		return err
	}
	if tunnel != "" && c.Tunnel != nil {
		fmt.Fprintf(a.stdout, "%s 터널 %s %s:%d\n", alias, tunnelMessages[tunnel], c.Host, c.Port)
	}
	state := "이미 실행 중"
	if started {
		state = "시작"
	}
	fmt.Fprintf(a.stdout, "%s 세션 %s\n", alias, state)
	return nil
}

// 한 별칭이 실패해도 나머지는 계속 닫고 오류를 모아 반환
func (a *app) close(args []string) error {
	if len(args) != 1 {
		return usageError("close")
	}
	all := args[0] == "all"
	targets := a.cfg.Aliases
	if !all {
		if _, err := a.conn(args[0]); err != nil {
			return err
		}
		targets = []string{args[0]}
	}
	var errs []error
	for _, alias := range targets {
		stopped, err := session.Stop(alias)
		if err != nil {
			errs = append(errs, err)
		}
		r, err := tunnels.Close(a.cfg, a.cfg.Conns[alias])
		if err != nil {
			errs = append(errs, fmt.Errorf("%s 터널 닫기 실패: %w", alias, err))
			continue
		}
		switch {
		case r == tunnels.Closed || stopped:
			fmt.Fprintf(a.stdout, "%s 닫힘\n", alias)
		case r == tunnels.Foreign:
			fmt.Fprintf(a.stdout, "%s gatereach가 연 터널이 아니라 유지\n", alias)
		case !all:
			fmt.Fprintf(a.stdout, "%s 이미 닫힘\n", alias)
		}
	}
	return errors.Join(errs...)
}

func (a *app) discover(args []string) error {
	if len(args) != 1 {
		return usageError("discover")
	}
	newKind, ok := tunnels.Kinds[args[0]]
	if !ok {
		return fmt.Errorf("지원하지 않는 터널: %s", args[0])
	}
	known := map[string]bool{}
	for _, c := range a.cfg.Conns {
		if c.Tunnel != nil {
			known[c.Tunnel.Target] = true
		}
	}
	targets, err := newKind(a.cfg.Tool(args[0], args[0])).Discover()
	if err != nil {
		return err
	}
	for _, t := range targets {
		mark := "new"
		if known[t.Target] {
			mark = "added"
		}
		fmt.Fprintf(a.stdout, "%-6s %-28s %-6s %-16s %s\n", mark, t.Name, or(t.Client, "-"), strings.Join(t.Users, ","), t.Target)
	}
	return nil
}

func (a *app) serve(args []string) error {
	if len(args) != 1 {
		return usageError("serve")
	}
	c, err := a.conn(args[0])
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	return session.Serve(ctx, args[0], version.Version, c)
}
