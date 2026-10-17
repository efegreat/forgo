package grpc

import (
	"fmt"
	"math"
	"net"
	"time"

	"github.com/efegreat/forgo/transport"

	"google.golang.org/grpc/keepalive"
)

// defaultKeepaliveTimeout is grpc-go's default keepalive.ServerParameters.Timeout.
const defaultKeepaliveTimeout = 20 * time.Second

// userTimeout returns the TCP_USER_TIMEOUT grpc-go sets for kp on Linux: Timeout, or 20 seconds when it is
// zero, and none when Time is infinite, which turns keepalive pings off.
func userTimeout(kp keepalive.ServerParameters) (time.Duration, bool) {
	if kp.Time == time.Duration(math.MaxInt64) {
		return 0, false
	}
	if kp.Timeout == 0 {
		return defaultKeepaliveTimeout, true
	}
	return kp.Timeout, true
}

// userTimeoutListener sets TCP_USER_TIMEOUT on the TCP connections it accepts. A connection it cannot be set
// on is closed and reported to observer, as grpc-go fails such a connection.
type userTimeoutListener struct {
	net.Listener
	timeout  time.Duration
	observer transport.IConnObserver
}

func (l *userTimeoutListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		tc, ok := c.(*net.TCPConn)
		if !ok {
			return c, nil
		}
		if err := setUserTimeout(tc, l.timeout); err != nil {
			_ = c.Close()
			l.observer.OnAcceptError(fmt.Errorf("grpc: set TCP_USER_TIMEOUT: %w", err))
			continue
		}
		return c, nil
	}
}
