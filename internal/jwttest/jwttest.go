// Package jwttest signs JSON Web Tokens for the tests of the servers' JWTAuth middlewares. Only tests
// import it.
package jwttest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// KeySet holds the signing keys of the tests.
type KeySet struct {
	Secret []byte // 64 bytes, for every HMAC algorithm
	RSA    *rsa.PrivateKey
	P256   *ecdsa.PrivateKey
}

// Keys returns the keys of the tests, generated once.
var Keys = sync.OnceValue(func() KeySet {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	p256, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return KeySet{Secret: []byte(strings.Repeat("s", 64)), RSA: rsaKey, P256: p256}
})

// Sign returns the compact JWS of header and claims, signed for header["alg"] with key.
func Sign(t testing.TB, header map[string]any, claims any, key any) string {
	t.Helper()
	b64 := base64.RawURLEncoding
	h, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	p, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	input := b64.EncodeToString(h) + "." + b64.EncodeToString(p)
	alg, _ := header["alg"].(string)
	sig, err := jwt.GetSigningMethod(alg).Sign(input, key)
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + b64.EncodeToString(sig)
}

// Claims returns claims valid now, with extra merged in. A nil value removes a claim.
func Claims(extra map[string]any) map[string]any {
	now := time.Now()
	c := map[string]any{"sub": "user-1", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	for k, v := range extra {
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
	}
	return c
}
