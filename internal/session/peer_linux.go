package session

import (
	"net"

	"golang.org/x/sys/unix"
)

func peerCred(c *net.UnixConn) (*unix.Ucred, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return nil, err
	}
	var cred *unix.Ucred
	var gerr error
	err = raw.Control(func(fd uintptr) {
		cred, gerr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil {
		return nil, err
	}
	return cred, gerr
}

// 소켓 반대편 프로세스의 사용자
func peerUID(c *net.UnixConn) (int, error) {
	cred, err := peerCred(c)
	if err != nil {
		return -1, err
	}
	return int(cred.Uid), nil
}

// 소켓 반대편 프로세스 번호
func peerPID(c net.Conn) (int, error) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, nil
	}
	cred, err := peerCred(uc)
	if err != nil {
		return 0, err
	}
	return int(cred.Pid), nil
}
