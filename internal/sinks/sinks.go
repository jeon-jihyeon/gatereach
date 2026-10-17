// Package sinks writes a Record stream to stdout or a Data Package folder
package sinks

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// Write is called once per record and Close once at the end
type Sink interface {
	Write(r record.Record) error
	// err holds the cause when the run failed
	// The returned error means the output itself failed
	Close(err error) error
}

// Recorded the same way in datapackage.json and the Arrow schema metadata
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

// Seconds with the local time zone offset
const startedLayout = "2006-01-02T15:04:05-07:00"

// A nil complete gives only the start details without the run's outcome
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

// An empty out gives stdout JSON lines
// Anything else gives a Data Package folder
func Get(out, format string, meta Meta, stdout, stderr io.Writer) (Sink, error) {
	if out == "" {
		return NewJSONLines(stdout, stderr, meta.Follow), nil
	}
	return NewPackage(out, format, meta, stdout, stderr)
}

// Output that writes the session's record JSON as is instead of building it again
type RawWriter interface {
	WriteRaw(line []byte) error
}

// One JSON line per record
// Without follow it flushes only when the buffer fills and at the end
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
		suffix = ", stopped by a failure"
	}
	unit := "rows"
	if j.rows == 1 {
		unit = "row"
	}
	fmt.Fprintf(j.errw, "(%d %s%s)\n", j.rows, unit, suffix)
	return flushErr
}
