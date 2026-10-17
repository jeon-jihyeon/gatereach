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
	tunnels.Already: "already open",
	tunnels.Foreign: "port held by another process",
	tunnels.Opened:  "opened",
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
		fmt.Fprintf(a.stdout, "%-24s %-8s %-22s %-10s %-5s %-6s %s\n",
			clean(alias), clean(or(c.Client, "?")), clean(or(where, "-")), clean(or(c.User, "-")), clean(tunnel), state, live)
	}
	return nil
}

// Returns the tunnel result and whether the session was just started
// A live session with an open port skips the process list check
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
	name := clean(alias)
	if tunnel != "" && c.Tunnel != nil {
		fmt.Fprintf(a.stdout, "%s tunnel %s %s\n", name, tunnelMessages[tunnel], clean(fmt.Sprintf("%s:%d", c.Host, c.Port)))
	}
	state := "already running"
	if started {
		state = "started"
	}
	fmt.Fprintf(a.stdout, "%s session %s\n", name, state)
	return nil
}

// A failing alias doesn't stop the rest from closing
// The errors come back joined
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
			errs = append(errs, fmt.Errorf("%s couldn't close the tunnel: %w", alias, err))
			continue
		}
		// close all prints aliases straight from conns.toml
		name := clean(alias)
		switch {
		case r == tunnels.Closed || stopped:
			fmt.Fprintf(a.stdout, "%s closed\n", name)
		case r == tunnels.Foreign:
			fmt.Fprintf(a.stdout, "%s kept open since gatereach didn't open the tunnel\n", name)
		case !all:
			fmt.Fprintf(a.stdout, "%s already closed\n", name)
		}
	}
	return errors.Join(errs...)
}

func (a *app) discover(args []string) error {
	if len(args) != 1 {
		return usageError("discover")
	}
	kind, err := tunnels.Of(a.cfg, args[0])
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, c := range a.cfg.Conns {
		if c.Tunnel != nil {
			known[c.Tunnel.Target] = true
		}
	}
	targets, err := kind.Discover()
	if err != nil {
		return err
	}
	for _, t := range targets {
		mark := "new"
		if known[t.Target] {
			mark = "added"
		}
		fmt.Fprintf(a.stdout, "%-6s %-28s %-6s %-16s %s\n",
			mark, clean(t.Name), clean(or(t.Client, "-")), clean(strings.Join(t.Users, ",")), clean(t.Target))
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
