// Package clients는 소스별 드라이버를 Record 스트림 하나로 맞춤
package clients

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -destination=mocks/client.go -package=mocks github.com/jeon-jihyeon/gatereach/internal/clients Client

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// 레코드 하나를 받는 쪽
// 오류를 돌려주면 스트림 중단
type Emit func(record.Record) error

type Query struct {
	Text  string
	Write bool
	Limit int
}

// 연결이 끊겨 아무것도 실행하지 않았다는 뜻
// 세션이 다시 연결한 뒤 한 번 재시도
var ErrDisconnected = errors.New("연결 끊김")

// 소스 하나의 연결
// 세션이 Connect로 연 연결을 닫을 때까지 쥐고 요청마다 재사용
type Client interface {
	// 실패하면 연 자원을 스스로 정리
	Connect(ctx context.Context) error
	// 쿼리 한 번의 결과
	// 최대 Limit 건
	// 끊긴 연결로 아무것도 실행하지 못했으면 ErrDisconnected
	Read(ctx context.Context, q Query, emit Emit) error
	Close() error
}

// 폴링 대신 소스가 직접 새 레코드를 밀어주는 클라이언트
type Follower interface {
	// 처리하지 않는 쿼리면 ok false
	Follow(ctx context.Context, q Query, emit Emit) (ok bool, err error)
}

type Kind struct {
	// host와 port로 붙어 터널이 먼저 열려야 하는 소스
	Networked bool
	Required  []string
	// 쓰기 차단 규칙
	// 연결 상태와 무관해 연결 없이 실행 전에 검사
	Check func(text string, write bool) error
	// 필수 키 밖의 설정 조합 검사
	// 없으면 검사하지 않음
	Validate func(conn config.Conn) error
	// password는 세션이 한 번 해석한 비밀값
	New func(conn config.Conn, password string) Client
}

var Kinds = map[string]Kind{
	"mysql":    {Networked: true, Required: []string{"port", "user"}, Check: mysqlCheck, New: newMySQL},
	"redis":    {Networked: true, Required: []string{"port"}, Check: redisCheck, New: newRedis},
	"dynamodb": {Networked: false, Check: dynamoCheck, New: newDynamoDB},
	"kafka":    {Networked: true, Required: []string{"port"}, Check: kafkaCheck, Validate: kafkaValidate, New: newKafka},
}

func Names() []string {
	out := make([]string, 0, len(Kinds))
	for k := range Kinds {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// 종류 확인과 필수 키 검사
func Get(conn config.Conn) (Kind, error) {
	k, ok := Kinds[conn.Client]
	if !ok {
		return Kind{}, fmt.Errorf("지원하지 않는 client: %s\n가능: %s", conn.Client, strings.Join(Names(), " "))
	}
	var missing []string
	for _, key := range k.Required {
		if !conn.Has(key) {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return Kind{}, fmt.Errorf("%s 연결 설정에 없는 키: %s", conn.Client, strings.Join(missing, ", "))
	}
	if k.Validate != nil {
		if err := k.Validate(conn); err != nil {
			return Kind{}, err
		}
	}
	return k, nil
}

// 폴링 주기와 중복 판별 필드
type Watching struct {
	Interval time.Duration
	// 빈 값이면 레코드 전체
	Key string
}

// 폴링 중복 판별에 기억할 최대 레코드 수
const seenMax = 100_000

// 새 레코드를 계속 내보냄
// Follower가 받는 쿼리면 소스의 방식
// 그 밖에는 폴링
func Watch(ctx context.Context, c Client, q Query, w Watching, emit Emit) error {
	if f, ok := c.(Follower); ok {
		handled, err := f.Follow(ctx, q, emit)
		if handled {
			return err
		}
	}
	return poll(ctx, c, q, w, seenMax, emit)
}

// 같은 쿼리를 주기마다 다시 실행하고 처음 보는 레코드만 내보냄
// 레코드 대신 16바이트 해시만 기억하고 limit을 넘으면 오래된 것부터 잊음
func poll(ctx context.Context, c Client, q Query, w Watching, limit int, emit Emit) error {
	s := newSeen(limit)
	for {
		err := c.Read(ctx, q, func(r record.Record) error {
			if s.has(r.Identity(w.Key)) {
				return nil
			}
			return emit(r)
		})
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.Interval):
		}
	}
}

// 본 레코드의 해시 집합
// 들어온 순서를 기억해 한도를 넘으면 가장 오래된 것부터 제거
type seen struct {
	limit int
	set   map[[16]byte]struct{}
	order [][16]byte
	h     hash.Hash
}

func newSeen(limit int) *seen {
	h, _ := blake2b.New(16, nil)
	return &seen{limit: limit, set: map[[16]byte]struct{}{}, h: h}
}

// 처음 보면 기억하고 false
func (s *seen) has(identity []byte) bool {
	s.h.Reset()
	s.h.Write(identity)
	var id [16]byte
	copy(id[:], s.h.Sum(nil))
	if _, ok := s.set[id]; ok {
		return true
	}
	s.set[id] = struct{}{}
	s.order = append(s.order, id)
	if len(s.order) > s.limit {
		delete(s.set, s.order[0])
		s.order = s.order[1:]
	}
	return false
}
