//go:build !linux

package grpc

import (
	"net"
	"time"
)

// userTimeoutSupported reports whether grpc-go sets TCP_USER_TIMEOUT, which it does on Linux only.
const userTimeoutSupported = false

func setUserTimeout(*net.TCPConn, time.Duration) error { return nil }
