package http

import (
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestClientIP(t *testing.T) {
	proxies := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}

	for _, tc := range []struct {
		name    string
		remote  string
		xff     []string
		trusted []netip.Prefix
		want    string
	}{
		{"peer v4", "203.0.113.7:5000", nil, nil, "203.0.113.7"},
		{"peer v6", "[2001:db8::1]:5000", nil, nil, "2001:db8::1"},
		{"peer v4-mapped", "[::ffff:203.0.113.7]:5000", nil, nil, "203.0.113.7"},
		{"peer without port", "203.0.113.7", nil, nil, "203.0.113.7"},
		{"unix socket", "@", nil, nil, "invalid IP"},
		{"header ignored without trusted proxies", "203.0.113.7:5000", []string{"198.51.100.1"}, nil, "203.0.113.7"},
		{"header ignored from untrusted peer", "203.0.113.7:5000", []string{"198.51.100.1"}, proxies, "203.0.113.7"},
		{"behind proxy", "10.0.0.2:80", []string{"198.51.100.1"}, proxies, "198.51.100.1"},
		{"spoofed entries left of the client", "10.0.0.2:80", []string{"1.1.1.1, 198.51.100.1"}, proxies, "198.51.100.1"},
		{"proxy chain", "10.0.0.2:80", []string{"198.51.100.1, 10.0.0.9", "10.0.0.3"}, proxies, "198.51.100.1"},
		{"hops with ports", "10.0.0.2:80", []string{"[2001:db8::5]:1234, 10.0.0.9:80"}, proxies, "2001:db8::5"},
		{"all trusted", "10.0.0.2:80", []string{"10.0.0.1"}, proxies, "10.0.0.1"},
		{"malformed hop", "10.0.0.2:80", []string{"garbage, 10.0.0.3"}, proxies, "10.0.0.3"},
		{"empty header", "10.0.0.2:80", nil, proxies, "10.0.0.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tc.remote
			for _, v := range tc.xff {
				r.Header.Add(forwardedForHeader, v)
			}
			if got := ClientIP(r, tc.trusted).String(); got != tc.want {
				t.Fatalf("ClientIP = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestClientKey(t *testing.T) {
	for _, tc := range []struct{ remote, want string }{
		{"203.0.113.7:5000", "203.0.113.7"},
		{"[2001:db8:1:2:3:4:5:6]:5000", "2001:db8:1:2::/64"},
		{"[fe80::1%eth0]:5000", "fe80::/64"},
		{"@", "@"},
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = tc.remote
		if got := clientKey(r, nil); got != tc.want {
			t.Errorf("clientKey(%q) = %q, want %q", tc.remote, got, tc.want)
		}
	}
}
