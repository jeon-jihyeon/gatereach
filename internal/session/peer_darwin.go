package session

import (
	"net"

	"golang.org/x/sys/unix"
)

func peerUID(c *net.UnixConn) (int, error) {
	uid := -1
	err := sockopt(c, func(fd int) error {
		cred, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if err == nil {
			uid = int(cred.Uid)
		}
		return err
	})
	return uid, err
}

func peerPID(c *net.UnixConn) (int, error) {
	pid := 0
	err := sockopt(c, func(fd int) error {
		var err error
		pid, err = unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
		return err
	})
	return pid, err
}
