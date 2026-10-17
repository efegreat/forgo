//go:build linux

package grpc

import (
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/efegreat/forgo/transport"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/keepalive"
)

// A connection wrapped for an IConnIOObserver gets the TCP_USER_TIMEOUT grpc-go sets on the ones it is
// handed unwrapped.
func TestUserTimeout(t *testing.T) {
	for _, tc := range []struct {
		name     string
		observer transport.IConnObserver
		kp       keepalive.ServerParameters
		want     int // milliseconds, 0 when unset
	}{
		{name: "default", observer: &ioObserver{}, want: 20000},
		{name: "timeout", observer: &ioObserver{}, kp: keepalive.ServerParameters{Timeout: 5 * time.Second}, want: 5000},
		{name: "pings off", observer: &ioObserver{}, kp: keepalive.ServerParameters{Time: time.Duration(math.MaxInt64)}},
		// grpc-go sets it on unwrapped connections itself.
		{name: "not wrapped", observer: acceptObserver{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Keepalive: tc.kp, Observer: tc.observer, Services: []ServiceConfig{testServiceConfig()}}
			s := newServer(t, cfg, io.Discard)
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			l := s.listener(ln)
			defer l.Close()

			dialed := make(chan net.Conn, 1)
			go func() {
				c, err := net.Dial("tcp", ln.Addr().String())
				if err != nil {
					t.Error(err)
				}
				dialed <- c
			}()
			c, err := l.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if client := <-dialed; client != nil {
				defer client.Close()
			}

			if n, ok := c.(interface{ NetConn() net.Conn }); ok {
				c = n.NetConn()
			}
			raw, err := c.(*net.TCPConn).SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			var got int
			var gerr error
			if err := raw.Control(func(fd uintptr) {
				got, gerr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
			}); err != nil {
				t.Fatal(err)
			}
			if gerr != nil {
				t.Fatal(gerr)
			}
			if got != tc.want {
				t.Fatalf("TCP_USER_TIMEOUT %dms, want %dms", got, tc.want)
			}
		})
	}
}
