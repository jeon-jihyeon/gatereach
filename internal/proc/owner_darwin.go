package proc

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// lsof selects with -a so the port and the LISTEN state must both match
// It exits 1 with no output when nothing matches
func listeners(port int) ([]int, error) {
	out, err := exec.Command("lsof", "-nP", "-w", "-a", "-iTCP:"+strconv.Itoa(port), "-sTCP:LISTEN", "-Fu").Output()
	var ee *exec.ExitError
	switch {
	case errors.As(err, &ee) && len(out) == 0 && len(ee.Stderr) == 0:
		return nil, nil
	case errors.As(err, &ee):
		return nil, fmt.Errorf("lsof failed: %s", strings.TrimSpace(string(ee.Stderr)))
	case err != nil:
		return nil, fmt.Errorf("lsof couldn't run: %w", err)
	}
	return parseLsof(out), nil
}
