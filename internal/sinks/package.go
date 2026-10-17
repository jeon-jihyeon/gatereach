package sinks

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

const Profile = "https://datapackage.org/profiles/2.0/datapackage.json"

const (
	batchSize = 1000
	// Sparse follow records still reach the file within this time
	lingerTime = time.Second
	lingerTick = 200 * time.Millisecond
	// Keeps other users out of the results folder
	folderMode = 0o700
)

// A Frictionless Data Package v2 folder
// 1. Appends records to the data file batch by batch as they arrive
// 2. Starts a new segment file without loss when a batch doesn't fit the current schema
// 3. Writes datapackage.json with complete false at the start
// 4. Replaces it with every segment as a resource at the end
// 5. A failed run keeps what arrived and records complete false with the cause
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
	// The first write error
	// Every later Write and Close returns it
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
		return nil, fmt.Errorf("unsupported format: %s\navailable: %s", formatName, strings.Join(FormatNames(), " "))
	}
	if err := CheckFolder(folder); err != nil {
		return nil, err
	}
	if err := makeFolder(folder); err != nil {
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
	// A reader that opens the folder mid-run or after a crash sees a partial package
	if err := p.writeFirstDescriptor(); err != nil {
		return nil, err
	}
	go p.lingerLoop()
	return p, nil
}

// The results folder must be missing or an empty real folder
// The CLI checks it before opening any tunnel or session
// An empty value means stdout and passes
func CheckFolder(folder string) error {
	if folder == "" {
		return nil
	}
	fi, err := os.Lstat(folder)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("couldn't read the folder: %s\n%w", folder, err)
	}
	// A planted link would send the results and their permissions somewhere else
	if fi.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("folder is a symlink: %s\nuse the real path or a new folder", folder)
	}
	// A file in place of the folder fails here before any tunnel opens
	d, err := openEmpty(folder)
	if err != nil {
		return err
	}
	d.Close()
	return nil
}

// 1. Parents may already exist or be links such as the system temp folder
// 2. The folder itself is made here or must be an empty real folder
// 3. The open handle refuses a link swapped in after CheckFolder and fixes the permissions on what it checked
func makeFolder(folder string) error {
	if err := os.MkdirAll(filepath.Dir(folder), folderMode); err != nil {
		return err
	}
	err := os.Mkdir(folder, folderMode)
	if err == nil || !errors.Is(err, fs.ErrExist) {
		return err
	}
	d, err := openEmpty(folder)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Chmod(folderMode)
}

// Opens a real folder without following a link and fails unless it has no entry
// Reading one name is enough so a large folder costs the same as an empty one
func openEmpty(folder string) (*os.File, error) {
	d, err := os.OpenFile(folder, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("couldn't read the folder: %s\n%w", folder, err)
	}
	names, err := d.Readdirnames(1)
	if err != nil && !errors.Is(err, io.EOF) {
		d.Close()
		return nil, fmt.Errorf("couldn't read the folder: %s\n%w", folder, err)
	}
	if len(names) > 0 {
		d.Close()
		return nil, fmt.Errorf("folder not empty: %s", folder)
	}
	return d, nil
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

// A segment that fails to close still lists its file as a resource
func (p *Package) closeSegment() error {
	err := p.seg.close()
	p.closed = append(p.closed, p.seg)
	p.seg = nil
	return err
}

func (p *Package) flush() error {
	records := p.buffer
	defer func() {
		// The next batch reuses the backing array
		clear(records)
		p.buffer = records[:0]
	}()
	if p.seg == nil || !fits(records, p.seg.fields()) {
		var prev []Field
		if p.seg != nil {
			prev = p.seg.fields()
			if err := p.closeSegment(); err != nil {
				return err
			}
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

// A failed write is returned even after a successful run so the caller exits non-zero
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
		if err := p.closeSegment(); err != nil && p.err == nil {
			p.err = err
		}
	}
	cause := runErr
	if cause == nil {
		cause = p.err
	}
	if err := p.descriptor(cause); err != nil {
		return err
	}
	path := filepath.Join(p.folder, descriptorName)
	if cause != nil {
		fmt.Fprintf(p.stderr, "stopped by a failure, saved only what arrived: %s\n", path)
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

const descriptorName = "datapackage.json"

// Error text of the descriptor written at the start
const notFinished = "not finished"

func (p *Package) writeFirstDescriptor() error {
	b, err := p.descriptorJSON(false, notFinished)
	if err != nil {
		return err
	}
	f, err := create(filepath.Join(p.folder, descriptorName))
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// The final descriptor replaces the first one by a rename
// A reader sees either the old file or the new one and never a half-written one
func (p *Package) descriptor(cause error) error {
	errText := ""
	if cause != nil {
		errText = cause.Error()
	}
	b, err := p.descriptorJSON(cause == nil, errText)
	if err != nil {
		return err
	}
	// CreateTemp opens with O_EXCL and 0600
	f, err := os.CreateTemp(p.folder, ".datapackage-*.json")
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), filepath.Join(p.folder, descriptorName))
	}
	if err != nil {
		_ = os.Remove(f.Name())
	}
	return err
}

func (p *Package) descriptorJSON(complete bool, errText string) ([]byte, error) {
	m := p.meta
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
		return nil, err
	}
	return append(b, '\n'), nil
}

type entry struct {
	k string
	v any
}

// A JSON object that keeps its key order
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

// JSON that leaves HTML characters and non-ASCII as is
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
