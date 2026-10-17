package cli

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"
	"unicode"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/proc"
	"github.com/jeon-jihyeon/gatereach/internal/secrets"
	"github.com/jeon-jihyeon/gatereach/internal/tunnels"
)

const maxPort = 65535

type addOptions struct {
	alias string
	// Holds no host, port or tunnel
	// since only a networked client keeps them
	conn   config.Conn
	host   string
	port   int
	tunnel string
	target string
	// Flags given on the command line
	// since an unset --host or --port reads the same as its default
	set map[string]bool
}

func (a *app) parseAdd(args []string) (addOptions, error) {
	fs := newFlags("add", a.stderr)
	var o addOptions
	fs.StringVar(&o.conn.Client, "client", "", "source kind: "+strings.Join(clients.Names(), " "))
	fs.StringVar(&o.host, "host", config.DefaultHost, "host for MySQL, Redis and Kafka")
	fs.IntVar(&o.port, "port", 0, "port for MySQL, Redis and Kafka\na tunnel connection gets a stable port when left out")
	fs.StringVar(&o.conn.User, "user", "", "user to connect as")
	fs.StringVar(&o.conn.Database, "database", "", "default database for MySQL and Athena")
	fs.StringVar(&o.conn.Profile, "profile", "", "AWS profile")
	fs.StringVar(&o.conn.Region, "region", "", "AWS region")
	fs.StringVar(&o.conn.Workgroup, "workgroup", "", "Athena workgroup")
	fs.StringVar(&o.conn.OutputLocation, "output-location", "",
		"Athena results location as an s3:// URL\nthe workgroup's when left out")
	fs.StringVar(&o.conn.Catalog, "catalog", "", "Athena data catalog\nAwsDataCatalog when left out")
	fs.StringVar(&o.conn.Secret, "secret", "", "keychain:<item> or env:"+secrets.EnvPrefix+"<name>")
	fs.BoolVar(&o.conn.TLS, "tls", false, "connect over TLS for MySQL, Redis and Kafka")
	fs.StringVar(&o.tunnel, "tunnel", "", "tunnel kind: "+strings.Join(tunnels.Names(), " "))
	fs.StringVar(&o.target, "target", "", "tunnel target name as discover lists it")
	pos, err := parse(fs, args)
	if err != nil {
		return o, err
	}
	if len(pos) != 1 {
		return o, usageError("add")
	}
	o.alias = pos[0]
	if o.alias == "all" {
		return o, errors.New("alias all is reserved for close all")
	}
	o.set = map[string]bool{}
	fs.Visit(func(f *flag.Flag) { o.set[f.Name] = true })
	return o, nil
}

func (a *app) add(args []string) error {
	o, err := a.parseAdd(args)
	if err != nil {
		return err
	}
	// Held from the duplicate checks to the append
	// so two adds at once can't both take the same alias or port or secret
	unlock, err := proc.Lock(config.LockPath())
	if err != nil {
		return err
	}
	defer unlock()
	if a.cfg, err = config.Load(); err != nil {
		return err
	}
	if _, dup := a.cfg.Conns[o.alias]; dup {
		return fmt.Errorf("alias already exists: %s", o.alias)
	}
	client := o.conn.Client
	if client == "" {
		return fmt.Errorf("--client required\navailable: %s", strings.Join(clients.Names(), " "))
	}
	kind, ok := clients.Kinds[client]
	if !ok {
		return fmt.Errorf("unsupported client: %s\navailable: %s", client, strings.Join(clients.Names(), " "))
	}
	if err := o.checkFlags(kind); err != nil {
		return err
	}
	c := o.conn
	if kind.Networked {
		if c.Port, err = a.port(o.host, o.port, o.tunnel, o.target, c.User); err != nil {
			return err
		}
		c.Host = o.host
		if o.tunnel != "" {
			c.Tunnel = &config.Tunnel{Type: o.tunnel, Target: o.target}
		}
	}
	if err := a.checkSecret(c); err != nil {
		return err
	}
	// Check required keys and key combinations before writing
	// so the file never holds a connection every command refuses
	if _, err := clients.Get(c); err != nil {
		return err
	}
	p, err := config.Append(o.alias, c)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "added %s to %s\n", clean(o.alias), p)
	return nil
}

// A setting the connection can't hold is refused instead of dropped
func (o addOptions) checkFlags(kind clients.Kind) error {
	client := o.conn.Client
	if err := secrets.Check(o.conn.Secret); err != nil {
		return err
	}
	// tsh takes these as arguments so a value like --proxy=... would turn into a flag of its own
	for _, f := range []struct{ name, value string }{{"host", o.host}, {"user", o.conn.User}, {"target", o.target}} {
		if !plainArg(f.value) {
			return fmt.Errorf("--%s can't start with - or hold spaces or control characters", f.name)
		}
	}
	if !kind.Networked && (o.set["host"] || o.set["port"]) {
		return fmt.Errorf("%s connects without --host or --port", client)
	}
	// Checked here since an unset port is 0 too
	if o.set["port"] && (o.port < 1 || o.port > maxPort) {
		return errors.New("--port must be between 1 and 65535")
	}
	if o.conn.TLS && !kind.Networked {
		return fmt.Errorf("%s connects without --tls", client)
	}
	if client != "athena" && (o.conn.Workgroup != "" || o.conn.OutputLocation != "" || o.conn.Catalog != "") {
		return fmt.Errorf("%s doesn't use --workgroup, --output-location or --catalog", client)
	}
	if o.set["target"] && o.tunnel == "" {
		return errors.New("--target needs --tunnel")
	}
	if o.tunnel == "" {
		return nil
	}
	if err := tunnels.Check(o.tunnel); err != nil {
		return err
	}
	if !kind.Networked {
		return fmt.Errorf("%s connects without a tunnel", client)
	}
	return nil
}

// A port another alias uses on the same host is refused
// since one alias's queries would go through the other's tunnel
// A tunnel connection without --port gets a fixed port from its target and user
func (a *app) port(host string, port int, tunnel, target, user string) (int, error) {
	used := a.cfg.UsedPorts(host)
	if port != 0 && used[port] {
		return 0, fmt.Errorf("port used by another connection: %d", port)
	}
	switch {
	case tunnel != "":
		if target == "" || user == "" {
			return 0, errors.New("a tunnel connection needs --target and --user")
		}
		if port != 0 {
			return port, nil
		}
		return config.StablePort(target, user, used)
	case port != 0:
		return port, nil
	}
	return 0, errors.New("a direct connection needs --port")
}

func plainArg(v string) bool {
	return !strings.HasPrefix(v, "-") && !strings.ContainsFunc(v, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
}

// Clients whose secret goes out in clear text without TLS
// A Kafka secret always turns on SASL_SSL
var clearSecret = map[string]bool{"mysql": true, "redis": true}

// 1. A secret sent in clear text to a host off this machine needs --tls
// 2. A reference another alias uses is refused unless the connection is the same
// so a new alias can't carry another alias's password to a host of its own
func (a *app) checkSecret(c config.Conn) error {
	if c.Secret == "" {
		return nil
	}
	if clearSecret[c.Client] && c.Tunnel == nil && !c.TLS && !loopback(c.Host) {
		return fmt.Errorf("%s --secret to %s needs --tls\nadd --tls or reach it through --tunnel", c.Client, c.Host)
	}
	for _, other := range a.cfg.Aliases {
		o := a.cfg.Conns[other]
		if o.Secret == c.Secret && !sameEndpoint(o, c) {
			return fmt.Errorf("secret %s already belongs to alias %s\nreuse that alias instead", c.Secret, other)
		}
	}
	return nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func sameEndpoint(a, b config.Conn) bool {
	return a.Client == b.Client && a.Host == b.Host && a.Port == b.Port && a.User == b.User &&
		tunnelTarget(a) == tunnelTarget(b)
}

func tunnelTarget(c config.Conn) string {
	if c.Tunnel == nil {
		return ""
	}
	return c.Tunnel.Target
}
