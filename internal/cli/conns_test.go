package cli_test

import (
	"strings"
	"testing"

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
