package session

import (
	"net"

	"golang.org/x/sys/unix"
)

// 소켓 반대편 프로세스의 사용자
func peerUID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return -1, err
	}
	uid := -1
	var gerr error
	err = raw.Control(func(fd uintptr) {
		var cred *unix.Xucred
		cred, gerr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if gerr == nil {
			uid = int(cred.Uid)
		}
	})
	if err != nil {
		return -1, err
	}
	return uid, gerr
}

// 소켓 반대편 프로세스 번호
func peerPID(c net.Conn) (int, error) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, nil
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, err
	}
	pid := 0
	var gerr error
	err = raw.Control(func(fd uintptr) {
		pid, gerr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	})
	if err != nil {
		return 0, err
	}
	return pid, gerr
}
