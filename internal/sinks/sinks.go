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
	// stop names what ended a normal run early
	// err holds the cause when the run failed
	// The returned error means the output itself failed
	Close(stop Stop, err error) error
}

// Why a normal end came before the source ran out
// Empty for every other end
type Stop string

const StopBudget Stop = "budget" // The budget ran out

// Recorded the same way in datapackage.json and the Arrow schema metadata
type Meta struct {
	Version string
	Alias   string
	Client  string
	User    string
	Query   string
	Limit   int
	// Budget in the source's unit
	// 0 means none
	Max     float64
	Unit    string
	Write   bool
	Follow  bool
	Started time.Time
}

// Seconds with the local time zone offset
const startedLayout = "2006-01-02T15:04:05-07:00"

// The run's outcome
// The Arrow metadata leaves it out since it goes out before the run ends
type outcome struct {
	complete bool
	errText  string
	stop     Stop
}

// 1. The outcome comes right after version
// 2. The query goes last so its length never pushes the outcome out of the prefix
// A reader that cuts the object at a length still sees whether the run was complete
// A nil outcome gives only the start details
func (m Meta) entries(o *outcome) ordered {
	out := ordered{{"version", m.Version}}
	if o != nil {
		out = append(out, entry{"complete", o.complete})
		if o.errText != "" {
			out = append(out, entry{"error", o.errText})
		}
		if o.stop != "" {
			out = append(out, entry{"stopped", o.stop})
		}
	}
	out = append(out, entry{"alias", m.Alias}, entry{"client", m.Client})
	if m.User != "" {
		out = append(out, entry{"user", m.User})
	}
	out = append(out, entry{"limit", m.Limit})
	if m.Max > 0 {
		out = append(out, entry{"max", m.Max}, entry{"unit", m.Unit})
	}
	return append(out, ordered{
		{"write", m.Write}, {"follow", m.Follow}, {"started", m.Started.Format(startedLayout)}, {"query", m.Query},
	}...)
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

// The CLI prints why a budget stopped the run so stop adds nothing here
func (j *JSONLines) Close(_ Stop, err error) error {
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
