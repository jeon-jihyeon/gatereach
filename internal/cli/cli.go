// Package cli is the only entry point that knows every package
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
	"github.com/jeon-jihyeon/gatereach/internal/tunnels"
	"github.com/jeon-jihyeon/gatereach/internal/version"
)

const (
	exitError = 1
	exitUsage = 2
	// A single read stopped by SIGINT, SIGTERM or SIGHUP
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
		"ls":    {usage: "ls", help: "list connections with their tunnel and session state", run: (*app).ls},
		"open":  {usage: "open <alias>", help: "open the tunnel and start the session", run: (*app).open},
		"close": {usage: "close <alias|all>", help: "stop the session and close tunnels gatereach opened", run: (*app).close},
		"q": {
			usage: "q <alias> <query> [options]  or  q <alias> --file <path> [options]",
			help:  "print one JSON line per record or save to the --out folder\nopens closed tunnels and sessions and keeps them up",
			run:   (*app).query,
		},
		"discover": {usage: "discover " + strings.Join(tunnels.Names(), "|"), help: "list targets the tunnel can reach", run: (*app).discover},
		"add":      {usage: "add <alias> --client " + strings.Join(clients.Names(), "|") + " [options]", help: "add a connection to the config file", run: (*app).add},
		"serve":    {usage: "serve <alias>", run: (*app).serve, hidden: true},
	}
}

// An error that sets the exit code of a command
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
		fmt.Fprintf(stderr, "unknown command: %s\n", clean(args[0]))
		usage(stderr, cmds)
		return exitUsage
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, clean(err.Error()))
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
			fmt.Fprintln(stderr, clean(code.msg))
		}
		return code.code
	}
	fmt.Fprintln(stderr, clean(err.Error()))
	return exitError
}

func usage(w io.Writer, cmds map[string]command) {
	fmt.Fprintln(w, "gatereach: query data sources behind tunnels and credentials by alias")
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
	return fmt.Errorf("usage: gatereach %s", commands()[name].usage)
}

// Parse in a loop so flags and positional arguments can mix
// Everything after `--` is positional so a query may start with -
// A bad flag exits 2 with nothing more since the flag set already printed the error and usage
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, exitCode{code: exitUsage}
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
			known = "none"
		}
		return config.Conn{}, fmt.Errorf("unknown alias: %s\navailable: %s", alias, known)
	}
	return c, nil
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// Text from a source or a tunnel tool can carry terminal escapes
// that rewrite the screen or the title when printed raw
// 1. C0 controls except newline and tab become \u00XX
// 2. DEL and C1 controls become \u00XX too
// 3. Invalid UTF-8 becomes U+FFFD so a lone C1 byte can't pass either
func clean(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r < 0x20 && r != '\n' && r != '\t') || (r >= 0x7f && r <= 0x9f) {
			fmt.Fprintf(&b, `\u%04X`, r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
