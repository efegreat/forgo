//go:build linux

package grpc

import (
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// userTimeoutSupported reports whether grpc-go sets TCP_USER_TIMEOUT, which it does on Linux only.
const userTimeoutSupported = true

// setUserTimeout sets TCP_USER_TIMEOUT on c to d, as grpc-go does.
func setUserTimeout(c *net.TCPConn, d time.Duration) error {
	raw, err := c.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := raw.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(d/time.Millisecond))
	}); err != nil {
		return err
	}
	return serr
}
