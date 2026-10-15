// Package config는 연결 설정 파일과 고정 포트
package config

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

const DefaultHost = "127.0.0.1"

// Linux 32768부터와 macOS 49152부터인 동적 포트 범위 아래 고정 포트
const (
	PortBase = 20000
	PortSpan = 12000
)

type Tunnel struct {
	Type   string `toml:"type"`
	Target string `toml:"target"`
}

// 별칭 하나의 연결
// Port 0은 포트 없는 연결
type Conn struct {
	Client   string  `toml:"client"`
	Host     string  `toml:"host"`
	Port     int     `toml:"port"`
	User     string  `toml:"user"`
	Database string  `toml:"database"`
	Profile  string  `toml:"profile"`
	Region   string  `toml:"region"`
	Secret   string  `toml:"secret"`
	TLS      bool    `toml:"tls"`
	Tunnel   *Tunnel `toml:"tunnel"`
}

// 터널 도구 설정
type Tool struct {
	// 비어 있으면 PATH의 기본 이름
	Bin string `toml:"bin"`
	// TELEPORT_HOME 같은 도구 홈 폴더
	Home string `toml:"home"`
}

type Config struct {
	Conns   map[string]Conn `toml:"conns"`
	Tunnels map[string]Tool `toml:"tunnels"`
	// 파일에 적힌 별칭 순서
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

// 파일이 없으면 빈 설정
func Load() (*Config, error) {
	cfg := &Config{Conns: map[string]Conn{}, Tunnels: map[string]Tool{}}
	p := Path()
	md, err := toml.DecodeFile(p, cfg)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("설정 파일 읽기 실패: %s\n%w", p, err)
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

// 종류별 도구 설정
// `~`를 풀고 실행 파일이 비면 fallback
func (c *Config) Tool(kind, fallback string) Tool {
	t := c.Tunnels[kind]
	t.Bin = expand(t.Bin)
	if t.Bin == "" {
		t.Bin = fallback
	}
	t.Home = expand(t.Home)
	return t
}

// 같은 host에서 다른 연결이 쓰는 포트
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

// 같은 대상과 계정이면 언제나 같은 포트
// 다른 연결이 이미 쓰는 포트면 범위 안에서 다음 빈 포트
func StablePort(target, user string, used map[int]bool) (int, error) {
	start := int(crc32.ChecksumIEEE([]byte(target+":"+user)) % PortSpan)
	for i := range PortSpan {
		port := PortBase + (start+i)%PortSpan
		if !used[port] {
			return port, nil
		}
	}
	return 0, errors.New("고정 포트 범위 소진")
}

// 연결 하나의 TOML 표
// 다시 읽어 같은 값이 나오지 않으면 거부해 설정 파일 손상 차단
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
	str("secret", c.Secret)
	if c.TLS {
		b.WriteString("tls = true\n")
	}
	if c.Tunnel != nil {
		fmt.Fprintf(&b, "\n[%s.tunnel]\n", head)
		str("type", c.Tunnel.Type)
		str("target", c.Tunnel.Target)
	}
	var back Config
	if _, err := toml.Decode(b.String(), &back); err != nil {
		return "", fmt.Errorf("설정 값으로 쓸 수 없는 문자: %w", err)
	}
	got, ok := back.Conns[alias]
	if !ok || !sameConn(got, c) {
		return "", errors.New("설정 값으로 쓸 수 없는 문자")
	}
	return b.String(), nil
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
// 제어 문자는 DEL까지 모두 이스케이프
func quote(s string) string {
	var b bytes.Buffer
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
			b.WriteString(`\u` + fmt.Sprintf("%04X", r))
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// 기존 내용과 주석 보존을 위해 파일 끝에 추가만 함
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
	defer f.Close()
	if _, err := f.WriteString(text); err != nil {
		return "", err
	}
	return p, nil
}

// 로그와 소켓 폴더의 기준인 설정 폴더
func Dir() string {
	return filepath.Dir(Path())
}

// 별칭마다 자식 프로세스 stderr를 받는 로그 파일과 그 옆의 잠금 파일
func LogPath(alias, suffix string) string {
	return filepath.Join(Dir(), "logs", FileKey(alias)+suffix)
}

// 파일 이름이 겹치는 별칭도 서로 다른 파일을 쓰도록 별칭 해시를 붙인 이름
func FileKey(alias string) string {
	return FileName(alias) + "-" + AliasHash(alias)
}

// 별칭의 crc32 16진수 8자리
func AliasHash(alias string) string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(alias)))
}

// 별칭을 파일 이름으로 변환
// 1. 유지: ASCII 영숫자와 비ASCII 문자 그리고 `.` `-` `_`
// 2. 그 밖의 문자가 이어지면 `_` 하나로 치환
// 서로 다른 별칭이 같은 이름이 될 수 있어 유일성이 필요한 곳은 해시를 덧붙임
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
