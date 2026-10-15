// Package sinks는 Record 스트림을 stdout이나 Data Package 폴더로 출력
package sinks

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// Write를 레코드마다 부르고 Close를 끝에 한 번 부름
type Sink interface {
	Write(r record.Record) error
	// 실패로 끝났으면 err에 원인
	// 출력 자체가 실패했으면 오류 반환
	Close(err error) error
}

// datapackage.json과 Arrow 스키마 메타데이터에 같은 규칙으로 기록
type Meta struct {
	Version string
	Alias   string
	Client  string
	User    string
	Query   string
	Limit   int
	Write   bool
	Follow  bool
	Started time.Time
}

// 초 단위와 지역 시간대 오프셋
const startedLayout = "2006-01-02T15:04:05-07:00"

// complete가 nil이면 실행 결과 없이 시작 정보만
func (m Meta) entries(complete *bool, errText string) ordered {
	out := ordered{{"version", m.Version}, {"alias", m.Alias}, {"client", m.Client}}
	if m.User != "" {
		out = append(out, entry{"user", m.User})
	}
	out = append(out, ordered{
		{"query", m.Query}, {"limit", m.Limit}, {"write", m.Write}, {"follow", m.Follow},
		{"started", m.Started.Format(startedLayout)},
	}...)
	if complete != nil {
		out = append(out, entry{"complete", *complete})
	}
	if errText != "" {
		out = append(out, entry{"error", errText})
	}
	return out
}

func FormatNames() []string {
	out := make([]string, 0, len(Formats))
	for k := range Formats {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// out이 비면 stdout JSON lines
// 그 밖에는 Data Package 폴더
func Get(out, format string, meta Meta, stdout, stderr io.Writer) (Sink, error) {
	if out == "" {
		return NewJSONLines(stdout, stderr, meta.Follow), nil
	}
	return NewPackage(out, format, meta, stdout, stderr)
}

// 세션이 보낸 레코드 JSON을 다시 만들지 않고 그대로 쓰는 출력
type RawWriter interface {
	WriteRaw(line []byte) error
}

// 레코드마다 JSON 한 줄
// follow가 아니면 버퍼가 찰 때와 끝날 때만 내보냄
type JSONLines struct {
	w      *bufio.Writer
	errw   io.Writer
	follow bool
	rows   int
}

func NewJSONLines(stdout, stderr io.Writer, follow bool) *JSONLines {
	return &JSONLines{w: bufio.NewWriterSize(stdout, 64*1024), errw: stderr, follow: follow}
}

func (j *JSONLines) Write(r record.Record) error {
	return j.WriteRaw(r.JSON())
}

func (j *JSONLines) WriteRaw(line []byte) error {
	if _, err := j.w.Write(line); err != nil {
		return err
	}
	if err := j.w.WriteByte('\n'); err != nil {
		return err
	}
	j.rows++
	if j.follow {
		return j.w.Flush()
	}
	return nil
}

func (j *JSONLines) Close(err error) error {
	flushErr := j.w.Flush()
	suffix := ""
	if err != nil || flushErr != nil {
		suffix = ", 실패로 중단"
	}
	fmt.Fprintf(j.errw, "(%d rows%s)\n", j.rows, suffix)
	return flushErr
}
