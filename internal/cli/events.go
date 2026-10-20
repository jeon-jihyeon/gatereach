package cli

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/jeon-jihyeon/gatereach/internal/session"
)

type queryBudget struct {
	Max  float64 `json:"max"`
	Unit string  `json:"unit"`
}

type queryReport struct {
	Schema  string       `json:"schema"`
	Type    string       `json:"type"`
	Version string       `json:"version"`
	Alias   string       `json:"alias"`
	Limit   int          `json:"limit"`
	Follow  bool         `json:"follow"`
	Rows    int          `json:"rows"`
	Status  string       `json:"status"`
	Session *session.End `json:"session,omitempty"`
	Budget  *queryBudget `json:"budget,omitempty"`
	Error   string       `json:"error,omitempty"`
}

type connectionInfo struct {
	Alias        string `json:"alias"`
	Client       string `json:"client"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Profile      string `json:"profile"`
	User         string `json:"user"`
	Tunnel       string `json:"tunnel"`
	PortState    string `json:"port_state"`
	SessionAlive bool   `json:"session_alive"`
}

type targetInfo struct {
	Name   string   `json:"name"`
	Client string   `json:"client"`
	Users  []string `json:"users"`
	Target string   `json:"target"`
	Added  bool     `json:"added"`
}

func writeJSON(w io.Writer, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, clean(string(raw)))
	return err
}
