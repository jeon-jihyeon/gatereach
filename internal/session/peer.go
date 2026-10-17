package session

import "net"

// Runs f on the socket's descriptor and returns the first error
func sockopt(c *net.UnixConn, f func(fd int) error) error {
	raw, err := c.SyscallConn()
	if err != nil {
		return err
	}
	var ferr error
	if err := raw.Control(func(fd uintptr) { ferr = f(int(fd)) }); err != nil {
		return err
	}
	return ferr
}
