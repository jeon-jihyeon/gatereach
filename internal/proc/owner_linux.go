package proc

import (
	"errors"
	"os"
)

// The kernel tables list every socket of the network namespace with its owner
// tcp6 is missing when IPv6 is off
func listeners(port int) ([]int, error) {
	v4, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return nil, err
	}
	uids := parseProcNet(v4, port)
	v6, err := os.ReadFile("/proc/net/tcp6")
	if errors.Is(err, os.ErrNotExist) {
		return uids, nil
	}
	if err != nil {
		return nil, err
	}
	return append(uids, parseProcNet(v6, port)...), nil
}
