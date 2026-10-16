package transport

import (
	"net/netip"
	"testing"
)

// The forwarded addresses are read only behind a trusted proxy: other peers could write anything.
func TestClientIPReadsForwardedForBehindTrustedProxiesOnly(t *testing.T) {
	proxies := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	read := false
	forwardedFor := func() []string {
		read = true
		return []string{"198.51.100.1"}
	}
	if got := ClientIP(netip.MustParseAddr("203.0.113.7"), forwardedFor, proxies); got.String() != "203.0.113.7" || read {
		t.Fatalf("ClientIP = %s, read %v", got, read)
	}
	if got := ClientIP(netip.MustParseAddr("10.0.0.2"), forwardedFor, proxies); got.String() != "198.51.100.1" || !read {
		t.Fatalf("ClientIP = %s, read %v", got, read)
	}
	if got := ClientIP(netip.Addr{}, forwardedFor, proxies); got.IsValid() {
		t.Fatalf("ClientIP of no peer = %s", got)
	}
}

func TestParseIP(t *testing.T) {
	for in, want := range map[string]string{
		"203.0.113.7:80":            "203.0.113.7",
		"[2001:db8::1]:443":         "2001:db8::1",
		"[::ffff:203.0.113.7]:5000": "203.0.113.7",
		"2001:db8::1":               "2001:db8::1",
		"@":                         "invalid IP",
		"":                          "invalid IP",
	} {
		if got := ParseIP(in).String(); got != want {
			t.Errorf("ParseIP(%q) = %s, want %s", in, got, want)
		}
	}
}
