package session

import (
	"net"

	"golang.org/x/sys/unix"
)

func peerCred(c *net.UnixConn) (*unix.Ucred, error) {
	var cred *unix.Ucred
	err := sockopt(c, func(fd int) error {
		var err error
		cred, err = unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
		return err
	})
	return cred, err
}

func peerUID(c *net.UnixConn) (int, error) {
	cred, err := peerCred(c)
	if err != nil {
		return -1, err
	}
	return int(cred.Uid), nil
}

func peerPID(c *net.UnixConn) (int, error) {
	cred, err := peerCred(c)
	if err != nil {
		return 0, err
	}
	return int(cred.Pid), nil
}
