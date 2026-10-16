package http

import (
	"net/http"
	"net/netip"

	"github.com/efegreat/forgo/ratelimit"
	"github.com/efegreat/forgo/transport"
)

const forwardedForHeader = "X-Forwarded-For"

// ClientIP returns the address of the client that sent r.
//
// It is the peer address, unless the peer is one of trustedProxies. Then X-Forwarded-For is read
// from right to left and the first address that is not a trusted proxy is the client. Addresses
// left of it were written by the client and are ignored, so the header cannot be spoofed past the
// proxies you trust. With no trusted proxies the header is never read.
//
// The zero netip.Addr is returned when the peer address is not an IP address, as with Unix sockets.
func ClientIP(r *http.Request, trustedProxies []netip.Prefix) netip.Addr {
	return transport.ClientIP(transport.ParseIP(r.RemoteAddr), func() []string {
		return r.Header.Values(forwardedForHeader)
	}, trustedProxies)
}

// clientKey returns the rate limit key of the client that sent r, as ratelimit.ClientKey does, or its
// peer address when that is not an IP address.
func clientKey(r *http.Request, trustedProxies []netip.Prefix) string {
	if k := ratelimit.ClientKey(ClientIP(r, trustedProxies)); k != "" {
		return k
	}
	return r.RemoteAddr
}
