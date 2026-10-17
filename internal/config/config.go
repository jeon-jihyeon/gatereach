// Package config holds the connections file and the fixed ports
package config

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

const DefaultHost = "127.0.0.1"

// Fixed ports stay below the dynamic ranges that start at 32768 on Linux and 49152 on macOS
const (
	PortBase = 20000
	PortSpan = 12000
)

type Tunnel struct {
	Type   string `toml:"type"`
	Target string `toml:"target"`
}

type Conn struct {
	Client string `toml:"client"`
	Host   string `toml:"host"`
	// 0 means a connection without a port
	Port     int     `toml:"port"`
	User     string  `toml:"user"`
	Database string  `toml:"database"`
	Profile  string  `toml:"profile"`
	Region   string  `toml:"region"`
	Secret   string  `toml:"secret"`
	TLS      bool    `toml:"tls"`
	Tunnel   *Tunnel `toml:"tunnel"`
	// Lets q take --write for this alias
	// Only the user sets it by editing the file since add has no flag for it
	Write bool `toml:"write"`

	Workgroup string `toml:"workgroup"`
	// A URL that starts with `s3://`
	// Empty uses the workgroup's result location
	OutputLocation string `toml:"output_location"`
	// Empty means AwsDataCatalog
	Catalog string `toml:"catalog"`
}

// Settings of a tunnel tool
type Tool struct {
	// Empty means the default name on PATH
	Bin string `toml:"bin"`
	// The tool's home folder such as TELEPORT_HOME
	Home string `toml:"home"`
}

type Config struct {
	Conns   map[string]Conn `toml:"conns"`
	Tunnels map[string]Tool `toml:"tunnels"`
	// Aliases in the order the file lists them
	Aliases []string `toml:"-"`
}

func Path() string {
	if p := os.Getenv("GATEREACH_CONFIG"); p != "" {
		return p
	}
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, "gatereach", "conns.toml")
}

// A missing file gives an empty config
func Load() (*Config, error) {
	cfg := &Config{Conns: map[string]Conn{}, Tunnels: map[string]Tool{}}
	p := Path()
	md, err := toml.DecodeFile(p, cfg)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't read the config file: %s\n%w", p, err)
	}
	for _, k := range md.Keys() {
		if len(k) == 2 && k[0] == "conns" {
			cfg.Aliases = append(cfg.Aliases, k[1])
		}
	}
	for alias, c := range cfg.Conns {
		if c.Port != 0 && c.Host == "" {
			c.Host = DefaultHost
			cfg.Conns[alias] = c
		}
	}
	return cfg, nil
}

// Expands `~` and uses fallback when the binary is empty
func (c *Config) Tool(kind, fallback string) Tool {
	t := c.Tunnels[kind]
	t.Bin = expand(t.Bin)
	if t.Bin == "" {
		t.Bin = fallback
	}
	t.Home = expand(t.Home)
	return t
}

// Ports the connections on host already use
func (c *Config) UsedPorts(host string) map[int]bool {
	used := map[int]bool{}
	for _, o := range c.Conns {
		if o.Port != 0 && o.Host == host {
			used[o.Port] = true
		}
	}
	return used
}

func (c Conn) Has(key string) bool {
	switch key {
	case "client":
		return c.Client != ""
	case "host":
		return c.Host != ""
	case "port":
		return c.Port != 0
	case "user":
		return c.User != ""
	case "database":
		return c.Database != ""
	case "profile":
		return c.Profile != ""
	case "region":
		return c.Region != ""
	case "workgroup":
		return c.Workgroup != ""
	case "output_location":
		return c.OutputLocation != ""
	case "catalog":
		return c.Catalog != ""
	case "secret":
		return c.Secret != ""
	case "tunnel":
		return c.Tunnel != nil
	}
	return false
}

func expand(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, p[1:])
	}
	return p
}

// The same target and user always get the same port
// A port another connection uses moves on to the next free one in the range
func StablePort(target, user string, used map[int]bool) (int, error) {
	start := int(crc32.ChecksumIEEE([]byte(target+":"+user)) % PortSpan)
	for i := range PortSpan {
		port := PortBase + (start+i)%PortSpan
		if !used[port] {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no fixed port left between %d and %d", PortBase, PortBase+PortSpan-1)
}

// The TOML table of one connection
// It's refused unless reading it back gives the same values so the config file can't break
func Render(alias string, c Conn) (string, error) {
	var b strings.Builder
	head := "conns." + quote(alias)
	fmt.Fprintf(&b, "\n[%s]\n", head)
	str := func(k, v string) {
		if v != "" {
			fmt.Fprintf(&b, "%s = %s\n", k, quote(v))
		}
	}
	str("client", c.Client)
	str("host", c.Host)
	if c.Port != 0 {
		fmt.Fprintf(&b, "port = %d\n", c.Port)
	}
	str("user", c.User)
	str("database", c.Database)
	str("profile", c.Profile)
	str("region", c.Region)
	str("workgroup", c.Workgroup)
	str("output_location", c.OutputLocation)
	str("catalog", c.Catalog)
	str("secret", c.Secret)
	if c.TLS {
		b.WriteString("tls = true\n")
	}
	if c.Write {
		b.WriteString("write = true\n")
	}
	if c.Tunnel != nil {
		fmt.Fprintf(&b, "\n[%s.tunnel]\n", head)
		str("type", c.Tunnel.Type)
		str("target", c.Tunnel.Target)
	}
	text := b.String()
	var back Config
	if _, err := toml.Decode(text, &back); err != nil {
		return "", fmt.Errorf("a character the config can't hold\n%w", err)
	}
	got, ok := back.Conns[alias]
	if !ok || !sameConn(got, c) {
		return "", errors.New("a character the config can't hold\ncheck the values for invalid UTF-8")
	}
	return text, nil
}

func sameConn(a, b Conn) bool {
	if (a.Tunnel == nil) != (b.Tunnel == nil) {
		return false
	}
	if a.Tunnel != nil && *a.Tunnel != *b.Tunnel {
		return false
	}
	a.Tunnel, b.Tunnel = nil, nil
	return a == b
}

// TOML basic string
// Control characters and DEL are escaped since TOML refuses them raw
func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\r':
			b.WriteString(`\r`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\u%04X`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// Only appends to the end so existing content and comments survive
func Append(alias string, c Conn) (string, error) {
	text, err := Render(alias, c)
	if err != nil {
		return "", err
	}
	p := Path()
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return "", err
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	_, werr := f.WriteString(text)
	// A failed close can mean the appended text never reached the disk
	if err := errors.Join(werr, f.Close()); err != nil {
		return "", err
	}
	return p, nil
}

// The config folder that logs and sockets sit under
func Dir() string {
	return filepath.Dir(Path())
}

// The lock file beside conns.toml that add holds from its checks to its append
func LockPath() string {
	return Path() + ".lock"
}

// Per alias log file for a child process's stderr and the lock file beside it
func LogPath(alias, suffix string) string {
	return filepath.Join(Dir(), "logs", FileKey(alias)+suffix)
}

// The alias hash keeps aliases whose file names collide in separate files
func FileKey(alias string) string {
	return FileName(alias) + "-" + AliasHash(alias)
}

// 8 hex digits of the alias's crc32
func AliasHash(alias string) string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(alias)))
}

// 1. Keeps ASCII letters and digits, non-ASCII characters and `.` `-` `_`
// 2. Replaces each run of other characters with one `_`
// Different aliases can map to the same name so callers that need uniqueness append the hash
func FileName(alias string) string {
	var b strings.Builder
	prev := false
	for _, r := range alias {
		ok := r == '.' || r == '-' || r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r > 127
		if ok {
			b.WriteRune(r)
			prev = false
			continue
		}
		if !prev {
			b.WriteByte('_')
		}
		prev = true
	}
	return b.String()
}
