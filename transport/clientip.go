package transport

import (
	"net/netip"
	"strings"
)

// ClientIP returns the address of the client of a request received from peer.
//
// It is peer, unless peer is one of trustedProxies. Then the X-Forwarded-For values that forwardedFor
// returns are read from right to left, and the first address that is not a trusted proxy is the client.
// Addresses left of it were written by the client and are ignored, so the header cannot be spoofed past
// the proxies you trust. forwardedFor is only called behind a trusted proxy, so with no trusted proxies
// the header is never read.
//
// The zero netip.Addr is returned when peer is, as for a Unix socket.
func ClientIP(peer netip.Addr, forwardedFor func() []string, trustedProxies []netip.Prefix) netip.Addr {
	ip := peer
	if !ip.IsValid() || !trusted(ip, trustedProxies) {
		return ip
	}

	values := forwardedFor()
	for i := len(values) - 1; i >= 0; i-- {
		hops := values[i]
		for hops != "" {
			hop := hops
			if j := strings.LastIndexByte(hops, ','); j >= 0 {
				hop, hops = hops[j+1:], hops[:j]
			} else {
				hops = ""
			}
			addr := ParseIP(strings.TrimSpace(hop))
			if !addr.IsValid() {
				// The chain cannot be followed further: the last trusted hop is all that is known.
				return ip
			}
			if !trusted(addr, trustedProxies) {
				return addr
			}
			ip = addr
		}
	}
	return ip
}

// ParseIP parses an IP address with or without a port, such as the RemoteAddr of an http.Request.
// IPv4-mapped IPv6 addresses are unmapped. It returns the zero netip.Addr when s is not an IP address.
func ParseIP(s string) netip.Addr {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap()
	}
	if addr, err := netip.ParseAddr(s); err == nil {
		return addr.Unmap()
	}
	return netip.Addr{}
}

func trusted(ip netip.Addr, proxies []netip.Prefix) bool {
	for _, p := range proxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}
