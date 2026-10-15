package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/tunnels"
)

const maxPort = 65535

func (a *app) add(args []string) error {
	fs := newFlags("add", a.stderr)
	client := fs.String("client", "", "소스 종류: "+strings.Join(clients.Names(), " "))
	host := fs.String("host", config.DefaultHost, "호스트")
	port := fs.Int("port", 0, "포트")
	user := fs.String("user", "", "계정")
	database := fs.String("database", "", "MySQL 기본 데이터베이스")
	profile := fs.String("profile", "", "AWS 프로필")
	region := fs.String("region", "", "AWS 리전")
	secret := fs.String("secret", "", "keychain:항목 또는 env:변수")
	tls := fs.Bool("tls", false, "MySQL과 Redis와 Kafka의 TLS 연결")
	tunnel := fs.String("tunnel", "", "터널 종류: "+strings.Join(tunnels.Names(), " "))
	target := fs.String("target", "", "터널 대상 이름")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageError("add")
	}
	alias := pos[0]
	if _, dup := a.cfg.Conns[alias]; dup {
		return fmt.Errorf("이미 있는 별칭: %s", alias)
	}
	kind, ok := clients.Kinds[*client]
	if !ok {
		return fmt.Errorf("--client 필요: %s", strings.Join(clients.Names(), " "))
	}
	if *tls && !kind.Networked {
		return fmt.Errorf("%s는 --tls 없이 연결", *client)
	}
	if *tunnel != "" {
		if _, ok := tunnels.Kinds[*tunnel]; !ok {
			return fmt.Errorf("지원하지 않는 터널: %s", *tunnel)
		}
		if !kind.Networked {
			return fmt.Errorf("%s는 터널 없이 연결", *client)
		}
	}
	c := config.Conn{Client: *client, User: *user, Database: *database, Profile: *profile, Region: *region, Secret: *secret, TLS: *tls}
	if kind.Networked {
		if c.Port, err = a.port(*host, *port, *tunnel, *target, *user); err != nil {
			return err
		}
		c.Host = *host
		if *tunnel != "" {
			c.Tunnel = &config.Tunnel{Type: *tunnel, Target: *target}
		}
	}
	// 필수 키와 키 조합을 쓰기 전에 확인해 모든 명령이 거부할 연결을 남기지 않음
	if _, err := clients.Get(c); err != nil {
		return err
	}
	p, err := config.Append(alias, c)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "%s 추가: %s\n", alias, p)
	return nil
}

// 같은 host의 포트를 두 별칭이 나누면 한쪽 터널로 다른 쪽 쿼리가 가므로 거부
// 터널 연결은 포트를 비우면 대상과 계정에서 고정 포트를 고름
func (a *app) port(host string, port int, tunnel, target, user string) (int, error) {
	if port < 0 || port > maxPort {
		return 0, errors.New("--port는 1부터 65535까지")
	}
	used := a.cfg.UsedPorts(host)
	if port != 0 && used[port] {
		return 0, fmt.Errorf("다른 연결이 쓰는 포트: %d", port)
	}
	switch {
	case tunnel != "":
		if target == "" || user == "" {
			return 0, errors.New("터널 연결은 --target과 --user 필요")
		}
		if port != 0 {
			return port, nil
		}
		return config.StablePort(target, user, used)
	case port != 0:
		return port, nil
	}
	return 0, errors.New("터널 없는 연결은 --port 필요")
}
