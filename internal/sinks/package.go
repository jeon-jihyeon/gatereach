package sinks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

const Profile = "https://datapackage.org/profiles/2.0/datapackage.json"

const (
	// 묶음 크기
	batchSize = 1000
	// follow 중 레코드가 드물어도 이 시간 안에 파일에 반영
	lingerTime = time.Second
	lingerTick = 200 * time.Millisecond
	// 다른 사용자가 열지 못하게 만드는 결과 폴더 권한
	folderMode = 0o700
)

// Frictionless Data Package v2 폴더
// 1. 레코드를 묶음 단위로 받는 대로 데이터 파일에 이어 씀
// 2. 묶음이 현재 스키마에 맞지 않으면 손실 없이 새 구간 파일을 시작
// 3. 종료 시 모든 구간을 resource로 담은 datapackage.json 작성
// 4. 실패로 끝나면 받은 데까지 저장하고 complete false와 원인 기록
type Package struct {
	folder string
	format format
	meta   Meta
	metaJS []byte
	stdout io.Writer
	stderr io.Writer
	batch  int
	linger time.Duration

	mu       sync.Mutex
	seg      segment
	closed   []segment
	buffer   []record.Record
	buffered time.Time
	// 처음 난 쓰기 오류
	// 이후 Write와 Close가 같은 오류를 돌려줌
	err  error
	stop chan struct{}
	done chan struct{}
}

func NewPackage(folder, format string, meta Meta, stdout, stderr io.Writer) (*Package, error) {
	return newPackage(folder, format, meta, stdout, stderr, batchSize, lingerTime)
}

func newPackage(folder, formatName string, meta Meta, stdout, stderr io.Writer, batch int, linger time.Duration) (*Package, error) {
	f, ok := Formats[formatName]
	if !ok {
		return nil, fmt.Errorf("지원하지 않는 형식: %s\n가능: %s", formatName, strings.Join(FormatNames(), " "))
	}
	if err := CheckFolder(folder); err != nil {
		return nil, err
	}
	// 이미 있던 빈 폴더도 같은 권한으로 맞춤
	if err := os.MkdirAll(folder, folderMode); err != nil {
		return nil, err
	}
	if err := os.Chmod(folder, folderMode); err != nil {
		return nil, err
	}
	metaJS, err := compactJSON(meta.entries(nil, ""))
	if err != nil {
		return nil, err
	}
	p := &Package{
		folder: folder, format: f, meta: meta, metaJS: metaJS,
		stdout: stdout, stderr: stderr, batch: batch, linger: linger,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go p.lingerLoop()
	return p, nil
}

// 결과 폴더는 없거나 비어 있어야 함
// 터널과 세션을 열기 전에 CLI가 먼저 확인
// 빈 값은 stdout 출력이라 통과
func CheckFolder(folder string) error {
	if folder == "" {
		return nil
	}
	entries, err := os.ReadDir(folder)
	if err == nil && len(entries) > 0 {
		return fmt.Errorf("비어 있지 않은 폴더: %s", folder)
	}
	return nil
}

func (p *Package) lingerLoop() {
	defer close(p.done)
	t := time.NewTicker(lingerTick)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.mu.Lock()
			if len(p.buffer) > 0 && time.Since(p.buffered) >= p.linger && p.err == nil {
				p.err = p.flush()
			}
			p.mu.Unlock()
		}
	}
}

func (p *Package) open(fields []Field) error {
	n := len(p.closed) + 1
	name := "result"
	if n > 1 {
		name = fmt.Sprintf("result-%d", n)
	}
	seg, err := p.format.open(name, filepath.Join(p.folder, name+"."+p.format.suffix), fields, p.metaJS)
	if err != nil {
		return err
	}
	p.seg = seg
	return nil
}

func (p *Package) flush() error {
	records := p.buffer
	p.buffer = nil
	if p.seg == nil || !fits(records, p.seg.fields()) {
		var prev []Field
		if p.seg != nil {
			prev = p.seg.fields()
			if err := p.seg.close(); err != nil {
				return err
			}
			p.closed = append(p.closed, p.seg)
			p.seg = nil
		}
		if err := p.open(infer(records, prev)); err != nil {
			return err
		}
	}
	return p.seg.write(records)
}

func (p *Package) Write(r record.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	if len(p.buffer) == 0 {
		p.buffered = time.Now()
	}
	p.buffer = append(p.buffer, r)
	if len(p.buffer) >= p.batch {
		p.err = p.flush()
	}
	return p.err
}

// 실행이 성공해도 쓰기가 실패했으면 그 오류를 돌려줘 호출자가 0이 아닌 코드로 끝냄
func (p *Package) Close(runErr error) error {
	close(p.stop)
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.buffer) > 0 && p.err == nil {
		p.err = p.flush()
	}
	if p.seg == nil && p.err == nil {
		p.err = p.open(nil)
	}
	if p.seg != nil {
		if err := p.seg.close(); err != nil && p.err == nil {
			p.err = err
		}
		p.closed = append(p.closed, p.seg)
		p.seg = nil
	}
	cause := runErr
	if cause == nil {
		cause = p.err
	}
	path, err := p.descriptor(cause)
	if err != nil {
		return err
	}
	if cause != nil {
		fmt.Fprintf(p.stderr, "실패로 중단되어 받은 데까지만 저장: %s\n", path)
		return p.err
	}
	fmt.Fprintln(p.stdout, path)
	return nil
}

var nonSlug = regexp.MustCompile(`[^a-z0-9._-]+`)

func slug(s string) string {
	out := strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(s), "-"), "-")
	if out == "" {
		return "data"
	}
	return out
}

func (p *Package) descriptor(cause error) (string, error) {
	m := p.meta
	complete := cause == nil
	errText := ""
	if cause != nil {
		errText = cause.Error()
	}
	resources := make([]any, len(p.closed))
	for i, s := range p.closed {
		resources[i] = s.resource()
	}
	doc := ordered{
		{"$schema", Profile},
		{"name", slug(m.Alias + "-" + m.Started.Format("2006-01-02T150405"))},
		{"created", m.Started.Format(startedLayout)},
		{"sources", []any{ordered{{"title", m.Alias + " " + m.Client}}}},
		{"gatereach", m.entries(&complete, errText)},
		{"resources", resources},
	}
	b, err := indentJSON(doc)
	if err != nil {
		return "", err
	}
	b = append(b, '\n')
	path := filepath.Join(p.folder, "datapackage.json")
	return path, os.WriteFile(path, b, fileMode)
}

type entry struct {
	k string
	v any
}

// 키 순서를 지키는 JSON 객체
type ordered []entry

func (o ordered) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, e := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(e.k)
		vb, err := compactJSON(e.v)
		if err != nil {
			return nil, err
		}
		b.Write(kb)
		b.WriteByte(':')
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// HTML 문자와 비ASCII를 그대로 둔 JSON
func compactJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}

func indentJSON(v any) ([]byte, error) {
	raw, err := compactJSON(v)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	if err := json.Indent(&b, raw, "", "  "); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
