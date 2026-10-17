package proc

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

// UIDs of the processes listening on a local TCP port
// Swapped in tests to act as another user
var listenerUIDs = listeners

// Refuses a local port unless only the current user listens on it
// Tunnel ports are predictable so another local user could bind one first and catch the password
// 1. Any listener of another UID refuses
// 2. No visible listener refuses too since lsof on macOS shows only the current user's processes
// 3. A failed lookup refuses so an unknown owner never gets the password
func CheckOwner(name string, port int) error {
	uids, err := listenerUIDs(port)
	if err != nil {
		return fmt.Errorf("%s port %d owner couldn't be checked: %w", name, port, err)
	}
	me := os.Getuid()
	if len(uids) == 0 || slices.ContainsFunc(uids, func(uid int) bool { return uid != me }) {
		return fmt.Errorf("%s port %d is held by another user\nclose that listener or change the port", name, port)
	}
	return nil
}

// The `u<uid>` lines of `lsof -F u` output
func parseLsof(out []byte) []int {
	var uids []int
	for line := range strings.Lines(string(out)) {
		v, ok := strings.CutPrefix(strings.TrimSpace(line), "u")
		if !ok {
			continue
		}
		if uid, err := strconv.Atoi(v); err == nil {
			uids = append(uids, uid)
		}
	}
	return uids
}

// The uid column of LISTEN rows for port in a /proc/net/tcp or tcp6 table
// 1. Field 1 is the local address with the port in hex after the colon
// 2. Field 3 is the state where 0A is LISTEN
// 3. Field 7 is the owner's UID
func parseProcNet(table []byte, port int) []int {
	var uids []int
	for line := range strings.Lines(string(table)) {
		f := strings.Fields(line)
		if len(f) < 8 || f[3] != "0A" {
			continue
		}
		_, hex, ok := strings.Cut(f[1], ":")
		p, err := strconv.ParseUint(hex, 16, 16)
		if !ok || err != nil || int(p) != port {
			continue
		}
		if uid, err := strconv.Atoi(f[7]); err == nil {
			uids = append(uids, uid)
		}
	}
	return uids
}
