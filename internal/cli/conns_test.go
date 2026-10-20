package cli_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jeon-jihyeon/gatereach/internal/cli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLs(t *testing.T) {
	useConfig(t, "[conns.z]\nclient = \"dynamodb\"\nprofile = \"dev\"\n\n[conns.a]\nclient = \"redis\"\nport = 1\n")
	code, out, _ := run("ls")
	require.Equal(t, 0, code)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, []string{"z", "dynamodb", "dev", "-", "-", "-", "-"}, strings.Fields(lines[0]))
	assert.Equal(t, []string{"a", "redis", "127.0.0.1:1", "-", "-", "closed", "-"}, strings.Fields(lines[1]))
}

func TestLsJSON(t *testing.T) {
	type want struct {
		aliases []string
		states  []string
	}
	tcs := []struct {
		name   string
		config string
		want   want
	}{
		{"empty array", "", want{[]string{}, []string{}}},
		{"control characters retain their values", "[conns.\"a\\u009B\"]\nclient = \"dynamodb\"\n",
			want{[]string{"a\u009b"}, []string{"-"}}},
		{"config order and port state", "[conns.z]\nclient = \"dynamodb\"\nprofile = \"dev\"\n" +
			"[conns.a]\nclient = \"redis\"\nport = 1\n",
			want{[]string{"z", "a"}, []string{"-", "closed"}}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, tc.config)
			code, out, errw := run("ls", "--json")
			require.Equal(t, 0, code, errw)
			var result struct {
				Schema      string `json:"schema"`
				Connections []struct {
					Alias        string `json:"alias"`
					PortState    string `json:"port_state"`
					SessionAlive bool   `json:"session_alive"`
				} `json:"connections"`
			}
			require.NoError(t, json.Unmarshal([]byte(out), &result))
			assert.Equal(t, "gatereach/v1", result.Schema)
			require.NotNil(t, result.Connections)
			aliases, states := []string{}, []string{}
			for _, c := range result.Connections {
				aliases = append(aliases, c.Alias)
				states = append(states, c.PortState)
				assert.False(t, c.SessionAlive)
			}
			assert.Equal(t, tc.want.aliases, aliases)
			assert.Equal(t, tc.want.states, states)
			assert.NotContains(t, out, "secret")
			assert.NotContains(t, out, "\u009b")
		})
	}
}

func TestDiscoverJSON(t *testing.T) {
	tcs := []struct {
		name    string
		targets string
		want    string
	}{
		{"empty array", `[]`, `[]`},
		{"known and new targets", `[
   {"metadata":{"name":"cache"},"spec":{"protocol":"redis"}},
   {"metadata":{"name":"new"},"spec":{"protocol":"mysql"},"users":{"allowed":["viewer"]}}
  ]`, `[
   {"name":"cache","client":"redis","users":[],"target":"cache","added":true},
   {"name":"new","client":"mysql","users":["viewer"],"target":"new","added":false}
  ]`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			bin := filepath.Join(t.TempDir(), "tsh")
			script := "#!/bin/sh\nprintf %s '" + tc.targets + "'\n"
			require.NoError(t, os.WriteFile(bin, []byte(script), 0o700))
			useConfig(t, "[tunnels.tsh]\nbin = "+strconv.Quote(bin)+"\n"+
				"[conns.r]\nclient = \"redis\"\nport = 1\n"+
				"[conns.r.tunnel]\ntype = \"tsh\"\ntarget = \"cache\"\n")
			code, out, errw := run("discover", "tsh", "--json")
			require.Equal(t, 0, code, errw)
			assert.JSONEq(t, `{"schema":"gatereach/v1","targets":`+tc.want+`}`, out)
		})
	}
}

func TestMachineOutputFailure(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "tsh")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\nprintf %s '[]'\n"), 0o700))
	tcs := []struct {
		name string
		args []string
	}{
		{"connections", []string{"ls", "--json"}},
		{"targets", []string{"discover", "tsh", "--json"}},
		{"query end", []string{"q", "missing", "GET k", "--events"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, "[tunnels.tsh]\nbin = "+strconv.Quote(bin)+"\n")
			reader, writer := io.Pipe()
			require.NoError(t, reader.Close())
			defer func() { require.NoError(t, writer.Close()) }()
			var stderr bytes.Buffer
			assert.Equal(t, 1, cli.Run(tc.args, writer, &stderr))
			assert.Contains(t, stderr.String(), io.ErrClosedPipe.Error())
		})
	}
}
