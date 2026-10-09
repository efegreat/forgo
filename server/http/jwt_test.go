package http

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var jwtNow = time.Unix(1_800_000_000, 0)

type jwtTestKeySet struct {
	secret, otherSecret []byte
	rsa                 *rsa.PrivateKey
	p256, p384, p521    *ecdsa.PrivateKey
	ed                  ed25519.PrivateKey
}

var jwtTestKeys = sync.OnceValue(func() jwtTestKeySet {
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	var k jwtTestKeySet
	var err error
	k.secret = []byte(strings.Repeat("s", 64))
	k.otherSecret = []byte(strings.Repeat("o", 64))
	k.rsa, err = rsa.GenerateKey(rand.Reader, 2048)
	must(err)
	k.p256, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(err)
	k.p384, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	must(err)
	k.p521, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	must(err)
	_, k.ed, err = ed25519.GenerateKey(rand.Reader)
	must(err)
	return k
})

// signJWT builds a compact JWS of header and claims, signed for header["alg"] with key.
// Claims given as []byte are used as the raw payload. Unknown algorithms, such as "none",
// get an empty signature.
func signJWT(t testing.TB, header map[string]any, claims any, key any) string {
	t.Helper()
	h, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := claims.([]byte)
	if !ok {
		if p, err = json.Marshal(claims); err != nil {
			t.Fatal(err)
		}
	}
	input := b64.EncodeToString(h) + "." + b64.EncodeToString(p)

	var sig []byte
	alg, _ := header["alg"].(string)
	if a, ok := jwtAlgorithms[alg]; ok {
		switch a.kind {
		case jwtKeyHMAC:
			mac := hmac.New(a.hash.New, key.([]byte))
			mac.Write([]byte(input))
			sig = mac.Sum(nil)
		case jwtKeyRSA:
			sig, err = rsa.SignPKCS1v15(rand.Reader, key.(*rsa.PrivateKey), a.hash, a.digest(input))
		case jwtKeyRSAPSS:
			sig, err = rsa.SignPSS(rand.Reader, key.(*rsa.PrivateKey), a.hash, a.digest(input), &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
		case jwtKeyECDSA:
			priv := key.(*ecdsa.PrivateKey)
			var r, s *big.Int
			r, s, err = ecdsa.Sign(rand.Reader, priv, a.digest(input))
			size := (priv.Curve.Params().BitSize + 7) / 8
			sig = make([]byte, 2*size)
			r.FillBytes(sig[:size])
			s.FillBytes(sig[size:])
		case jwtKeyEd25519:
			sig = ed25519.Sign(key.(ed25519.PrivateKey), []byte(input))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return input + "." + b64.EncodeToString(sig)
}

func hs256(t testing.TB, claims map[string]any) string {
	return signJWT(t, map[string]any{"alg": "HS256", "typ": "JWT"}, claims, jwtTestKeys().secret)
}

// validClaims returns claims that pass validation at jwtNow, with extra merged in. A nil value removes a claim.
func validClaims(extra map[string]any) map[string]any {
	c := map[string]any{"sub": "user-1", "iat": jwtNow.Unix(), "exp": jwtNow.Add(time.Hour).Unix()}
	for k, v := range extra {
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
	}
	return c
}

func testVerifier(keys IJWTKeySet) *jwtVerifier {
	return &jwtVerifier{keys: keys, now: func() time.Time { return jwtNow }}
}

func TestJWTAlgorithms(t *testing.T) {
	k := jwtTestKeys()
	for _, tc := range []struct {
		alg          string
		sign, verify any
	}{
		{"HS256", k.secret, k.secret},
		{"HS384", k.secret, k.secret},
		{"HS512", k.secret, k.secret},
		{"RS256", k.rsa, &k.rsa.PublicKey},
		{"RS384", k.rsa, &k.rsa.PublicKey},
		{"RS512", k.rsa, &k.rsa.PublicKey},
		{"PS256", k.rsa, &k.rsa.PublicKey},
		{"PS384", k.rsa, &k.rsa.PublicKey},
		{"PS512", k.rsa, &k.rsa.PublicKey},
		{"ES256", k.p256, &k.p256.PublicKey},
		{"ES384", k.p384, &k.p384.PublicKey},
		{"ES512", k.p521, &k.p521.PublicKey},
		{"EdDSA", k.ed, k.ed.Public()},
		{"Ed25519", k.ed, k.ed.Public()},
	} {
		t.Run(tc.alg, func(t *testing.T) {
			keys := JWTKeys{{Key: tc.verify}}
			if err := keys.validate(); err != nil {
				t.Fatal(err)
			}
			token := signJWT(t, map[string]any{"alg": tc.alg}, validClaims(nil), tc.sign)
			claims, err := testVerifier(keys).verify(context.Background(), token)
			if err != nil {
				t.Fatal(err)
			}
			if claims.Subject != "user-1" || !claims.ExpiresAt.Equal(jwtNow.Add(time.Hour)) || !claims.IssuedAt.Equal(jwtNow) {
				t.Fatalf("claims %+v", claims)
			}
		})
	}
}

func TestJWTRejects(t *testing.T) {
	k := jwtTestKeys()
	secretKeys := JWTKeys{{Key: k.secret}}
	rsaKeys := JWTKeys{{Key: &k.rsa.PublicKey}}
	valid := hs256(t, validClaims(nil))
	h64, rest, _ := strings.Cut(valid, ".")
	p64, s64, _ := strings.Cut(rest, ".")
	rsaDER, err := x509.MarshalPKIXPublicKey(&k.rsa.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	es256 := signJWT(t, map[string]any{"alg": "ES256"}, validClaims(nil), k.p256)

	for _, tc := range []struct {
		name    string
		token   string
		keys    JWTKeys
		modify  func(*jwtVerifier)
		wantErr error
	}{
		{name: "valid", token: valid, keys: secretKeys},
		{name: "alg none", token: signJWT(t, map[string]any{"alg": "none"}, validClaims(nil), nil), keys: secretKeys, wantErr: errJWTAlgorithm},
		{name: "unknown alg", token: signJWT(t, map[string]any{"alg": "HS1"}, validClaims(nil), nil), keys: secretKeys, wantErr: errJWTAlgorithm},
		{
			name:    "RSA public key used as HMAC secret",
			token:   signJWT(t, map[string]any{"alg": "HS256"}, validClaims(nil), rsaDER),
			keys:    rsaKeys,
			wantErr: errJWTNoKey,
		},
		{name: "crit header", token: signJWT(t, map[string]any{"alg": "HS256", "crit": []string{"exp"}}, validClaims(nil), k.secret), keys: secretKeys, wantErr: errJWTCritical},
		{name: "tampered payload", token: h64 + "." + b64.EncodeToString([]byte(`{"sub":"admin","exp":1900000000}`)) + "." + s64, keys: secretKeys, wantErr: errJWTSignature},
		{name: "wrong secret", token: signJWT(t, map[string]any{"alg": "HS256"}, validClaims(nil), k.otherSecret), keys: secretKeys, wantErr: errJWTSignature},
		{name: "two segments", token: h64 + "." + p64, keys: secretKeys, wantErr: errJWTMalformed},
		{name: "four segments", token: valid + ".x", keys: secretKeys, wantErr: errJWTMalformed},
		{name: "padded header", token: h64 + "=." + p64 + "." + s64, keys: secretKeys, wantErr: errJWTMalformed},
		{name: "bad signature encoding", token: h64 + "." + p64 + ".!!", keys: secretKeys, wantErr: errJWTMalformed},
		{name: "header not JSON", token: b64.EncodeToString([]byte("nope")) + "." + p64 + "." + s64, keys: secretKeys, wantErr: errJWTMalformed},
		{name: "payload not JSON", token: signJWT(t, map[string]any{"alg": "HS256"}, []byte("nope"), k.secret), keys: secretKeys, wantErr: errJWTMalformed},
		{name: "exp is a string", token: hs256(t, validClaims(map[string]any{"exp": "tomorrow"})), keys: secretKeys, wantErr: errJWTMalformed},
		{name: "negative exp", token: hs256(t, validClaims(map[string]any{"exp": -1})), keys: secretKeys, wantErr: errJWTMalformed},
		{name: "no exp", token: hs256(t, validClaims(map[string]any{"exp": nil})), keys: secretKeys, wantErr: errJWTNoExpiry},
		{name: "expired", token: hs256(t, validClaims(map[string]any{"exp": jwtNow.Add(-time.Second).Unix()})), keys: secretKeys, wantErr: errJWTExpired},
		{name: "expires now", token: hs256(t, validClaims(map[string]any{"exp": jwtNow.Unix()})), keys: secretKeys, wantErr: errJWTExpired},
		{
			name:   "expired within leeway",
			token:  hs256(t, validClaims(map[string]any{"exp": jwtNow.Add(-10 * time.Second).Unix()})),
			keys:   secretKeys,
			modify: func(v *jwtVerifier) { v.leeway = 30 * time.Second },
		},
		{name: "not valid yet", token: hs256(t, validClaims(map[string]any{"nbf": jwtNow.Add(time.Minute).Unix()})), keys: secretKeys, wantErr: errJWTNotYetValid},
		{
			name:   "not valid yet within leeway",
			token:  hs256(t, validClaims(map[string]any{"nbf": jwtNow.Add(time.Minute).Unix()})),
			keys:   secretKeys,
			modify: func(v *jwtVerifier) { v.leeway = time.Minute },
		},
		{
			name:    "issuer mismatch",
			token:   hs256(t, validClaims(map[string]any{"iss": "https://evil.example"})),
			keys:    secretKeys,
			modify:  func(v *jwtVerifier) { v.issuer = "https://idp.example" },
			wantErr: errJWTIssuer,
		},
		{
			name:   "issuer match",
			token:  hs256(t, validClaims(map[string]any{"iss": "https://idp.example"})),
			keys:   secretKeys,
			modify: func(v *jwtVerifier) { v.issuer = "https://idp.example" },
		},
		{
			name:    "audience mismatch",
			token:   hs256(t, validClaims(map[string]any{"aud": "web"})),
			keys:    secretKeys,
			modify:  func(v *jwtVerifier) { v.audience = []string{"api"} },
			wantErr: errJWTAudience,
		},
		{
			name:    "audience missing",
			token:   valid,
			keys:    secretKeys,
			modify:  func(v *jwtVerifier) { v.audience = []string{"api"} },
			wantErr: errJWTAudience,
		},
		{
			name:   "audience string",
			token:  hs256(t, validClaims(map[string]any{"aud": "api"})),
			keys:   secretKeys,
			modify: func(v *jwtVerifier) { v.audience = []string{"api"} },
		},
		{
			name:   "audience array",
			token:  hs256(t, validClaims(map[string]any{"aud": []string{"web", "api"}})),
			keys:   secretKeys,
			modify: func(v *jwtVerifier) { v.audience = []string{"other", "api"} },
		},
		{name: "audience of wrong type", token: hs256(t, validClaims(map[string]any{"aud": 1})), keys: secretKeys, wantErr: errJWTMalformed},
		{name: "truncated ECDSA signature", token: es256[:len(es256)-2], keys: JWTKeys{{Key: &k.p256.PublicKey}}, wantErr: errJWTSignature},
		{name: "ECDSA key on another curve", token: es256, keys: JWTKeys{{Key: &k.p384.PublicKey}}, wantErr: errJWTNoKey},
		{
			name:    "HMAC key shorter than the hash",
			token:   signJWT(t, map[string]any{"alg": "HS512"}, validClaims(nil), k.secret[:32]),
			keys:    JWTKeys{{Key: k.secret[:32]}},
			wantErr: errJWTKeyType,
		},
		{
			name:    "key pinned to another algorithm",
			token:   signJWT(t, map[string]any{"alg": "HS384"}, validClaims(nil), k.secret),
			keys:    JWTKeys{{Algorithm: "HS256", Key: k.secret}},
			wantErr: errJWTNoKey,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := testVerifier(tc.keys)
			if tc.modify != nil {
				tc.modify(v)
			}
			_, err := v.verify(context.Background(), tc.token)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// customKeySet returns keys regardless of the token, like a careless IJWTKeySet implementation.
type customKeySet JWTKeys

func (c customKeySet) Keys(context.Context, string, string) ([]JWTKey, error) { return c, nil }

func TestJWTVerifierRechecksPinnedAlgorithm(t *testing.T) {
	k := jwtTestKeys()
	token := signJWT(t, map[string]any{"alg": "HS512"}, validClaims(nil), k.secret)
	_, err := testVerifier(customKeySet{{Algorithm: "HS256", Key: k.secret}}).verify(context.Background(), token)
	if !errors.Is(err, errJWTNoKey) {
		t.Fatalf("err %v", err)
	}
}

func TestJWTKeySelection(t *testing.T) {
	k := jwtTestKeys()
	third := []byte(strings.Repeat("t", 32))
	keys := JWTKeys{{ID: "old", Key: k.otherSecret}, {ID: "new", Key: k.secret}, {Key: third}}

	for _, tc := range []struct {
		name    string
		kid     string
		secret  []byte
		wantErr error
	}{
		{"kid selects its key", "new", k.secret, nil},
		{"kid excludes other keys", "new", k.otherSecret, errJWTSignature},
		{"unknown kid falls back to keys without ID", "unknown", third, nil},
		{"no kid uses keys without ID", "", third, nil},
		{"no kid ignores keys with ID", "", k.secret, errJWTSignature},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := map[string]any{"alg": "HS256"}
			if tc.kid != "" {
				header["kid"] = tc.kid
			}
			_, err := testVerifier(keys).verify(context.Background(), signJWT(t, header, validClaims(nil), tc.secret))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err %v, want %v", err, tc.wantErr)
			}
		})
	}

	// Keys without IDs are all tried, so they can be rotated without kid.
	rotated := JWTKeys{{Key: k.otherSecret}, {Key: k.secret}}
	if _, err := testVerifier(rotated).verify(context.Background(), hs256(t, validClaims(nil))); err != nil {
		t.Fatalf("rotation without kid: %v", err)
	}

	if n := testing.AllocsPerRun(100, func() { _ = keys.match("new", "HS256") }); n != 0 {
		t.Fatalf("single key match allocates %v times", n)
	}
}

func TestJWTKeyValidation(t *testing.T) {
	k := jwtTestKeys()
	small, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	p224, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		keys  JWTKeys
		valid bool
	}{
		{"valid set", JWTKeys{{Key: k.secret}, {ID: "r", Algorithm: "PS256", Key: &k.rsa.PublicKey}, {Key: &k.p521.PublicKey}, {Key: k.ed.Public()}}, true},
		{"empty", JWTKeys{}, false},
		{"short HMAC secret", JWTKeys{{Key: make([]byte, 16)}}, false},
		{"HMAC secret too short for pinned HS512", JWTKeys{{Algorithm: "HS512", Key: make([]byte, 32)}}, false},
		{"short RSA key", JWTKeys{{Key: &small.PublicKey}}, false},
		{"RSA private key", JWTKeys{{Key: k.rsa}}, false},
		{"nil RSA key", JWTKeys{{Key: (*rsa.PublicKey)(nil)}}, false},
		{"unsupported curve", JWTKeys{{Key: &p224.PublicKey}}, false},
		{"short Ed25519 key", JWTKeys{{Key: ed25519.PublicKey(make([]byte, 16))}}, false},
		{"string secret", JWTKeys{{Key: strings.Repeat("s", 32)}}, false},
		{"algorithm does not fit the key", JWTKeys{{Algorithm: "RS256", Key: k.secret}}, false},
		{"curve does not fit the algorithm", JWTKeys{{Algorithm: "ES256", Key: &k.p384.PublicKey}}, false},
		{"none", JWTKeys{{Algorithm: "none", Key: k.secret}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.keys.validate(); (err == nil) != tc.valid {
				t.Fatalf("validate = %v, want valid %v", err, tc.valid)
			}
		})
	}
}

func TestJWTClaims(t *testing.T) {
	token := hs256(t, validClaims(map[string]any{
		"iss":   "https://idp.example",
		"aud":   []string{"api"},
		"jti":   "token-1",
		"nbf":   float64(jwtNow.Unix()) - 0.5,
		"scope": "read write",
		"roles": []string{"admin"},
	}))
	claims, err := testVerifier(JWTKeys{{Key: jwtTestKeys().secret}}).verify(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Issuer != "https://idp.example" || claims.ID != "token-1" || len(claims.Audience) != 1 || claims.Audience[0] != "api" {
		t.Fatalf("claims %+v", claims)
	}
	if want := jwtNow.Add(-500 * time.Millisecond); !claims.NotBefore.Equal(want) {
		t.Fatalf("nbf %v, want %v", claims.NotBefore, want)
	}

	var custom struct {
		Scope string   `json:"scope"`
		Roles []string `json:"roles"`
	}
	if err := claims.Decode(&custom); err != nil {
		t.Fatal(err)
	}
	if custom.Scope != "read write" || len(custom.Roles) != 1 || custom.Roles[0] != "admin" {
		t.Fatalf("custom claims %+v", custom)
	}
}

func rsaJWK(kid, alg string, pub *rsa.PublicKey) map[string]any {
	return map[string]any{
		"kty": "RSA", "kid": kid, "alg": alg, "use": "sig",
		"n": b64.EncodeToString(pub.N.Bytes()), "e": b64.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}
}

func ecJWK(kid string, pub *ecdsa.PublicKey) map[string]any {
	point, err := pub.Bytes()
	if err != nil {
		panic(err)
	}
	size := (len(point) - 1) / 2
	return map[string]any{
		"kty": "EC", "kid": kid, "crv": pub.Curve.Params().Name,
		"x": b64.EncodeToString(point[1 : 1+size]), "y": b64.EncodeToString(point[1+size:]),
	}
}

func okpJWK(kid string, pub ed25519.PublicKey) map[string]any {
	return map[string]any{"kty": "OKP", "kid": kid, "crv": "Ed25519", "x": b64.EncodeToString(pub)}
}

type jwksServer struct {
	*httptest.Server
	fetches atomic.Int32
	mu      sync.Mutex
	status  int
	body    []byte
	delay   time.Duration
}

func newJWKSServer(t *testing.T, keys ...map[string]any) *jwksServer {
	s := &jwksServer{}
	s.serve(http.StatusOK, keys...)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.fetches.Add(1)
		s.mu.Lock()
		status, body, delay := s.status, s.body, s.delay
		s.mu.Unlock()
		time.Sleep(delay)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *jwksServer) serve(status int, keys ...map[string]any) {
	body, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		panic(err)
	}
	s.serveRaw(status, body)
}

func (s *jwksServer) serveRaw(status int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.body = status, body
}

// testClock is a wall clock tests move by hand.
type testClock struct{ offset atomic.Int64 }

func (c *testClock) now() time.Time          { return jwtNow.Add(time.Duration(c.offset.Load())) }
func (c *testClock) advance(d time.Duration) { c.offset.Add(int64(d)) }

func TestJWKSVerifiesAndCaches(t *testing.T) {
	k := jwtTestKeys()
	srv := newJWKSServer(t,
		rsaJWK("r1", "RS256", &k.rsa.PublicKey),
		ecJWK("e1", &k.p256.PublicKey),
		okpJWK("d1", k.ed.Public().(ed25519.PublicKey)),
		map[string]any{"kty": "oct", "kid": "o1", "k": b64.EncodeToString(k.secret)},
		map[string]any{"kty": "RSA", "kid": "enc", "use": "enc", "n": "AQAB", "e": "AQAB"},
		map[string]any{"kty": "EC", "kid": "k1", "crv": "secp256k1", "x": "AA", "y": "AA"},
	)
	clock := &testClock{}
	v := testVerifier(&JWKS{URL: srv.URL, clock: clock.now})

	for _, tc := range []struct {
		alg, kid string
		key      any
	}{
		{"RS256", "r1", k.rsa},
		{"ES256", "e1", k.p256},
		{"EdDSA", "d1", k.ed},
	} {
		if _, err := v.verify(context.Background(), signJWT(t, map[string]any{"alg": tc.alg, "kid": tc.kid}, validClaims(nil), tc.key)); err != nil {
			t.Fatalf("%s: %v", tc.kid, err)
		}
	}
	if n := srv.fetches.Load(); n != 1 {
		t.Fatalf("fetched %d times, want 1", n)
	}

	// The JWK pins r1 to RS256, and unusable keys were skipped.
	for _, tc := range []struct {
		alg, kid string
		key      any
	}{
		{"PS256", "r1", k.rsa},
		{"HS256", "o1", k.secret},
	} {
		_, err := v.verify(context.Background(), signJWT(t, map[string]any{"alg": tc.alg, "kid": tc.kid}, validClaims(nil), tc.key))
		if !errors.Is(err, errJWTNoKey) {
			t.Fatalf("%s %s: err %v", tc.alg, tc.kid, err)
		}
	}
	if n := len(v.keys.(*JWKS).state.Load().keys); n != 3 {
		t.Fatalf("parsed %d keys, want 3", n)
	}
	if n := srv.fetches.Load(); n != 1 {
		t.Fatalf("fetched %d times within MinRefreshInterval, want 1", n)
	}
}

func TestJWKSRefresh(t *testing.T) {
	k := jwtTestKeys()
	keyA := rsaJWK("a", "RS256", &k.rsa.PublicKey)
	keyB := ecJWK("b", &k.p256.PublicKey)
	srv := newJWKSServer(t, keyA)
	clock := &testClock{}
	v := testVerifier(&JWKS{URL: srv.URL, RefreshInterval: time.Hour, MinRefreshInterval: time.Minute, clock: clock.now})
	tokenA := signJWT(t, map[string]any{"alg": "RS256", "kid": "a"}, validClaims(nil), k.rsa)
	tokenB := signJWT(t, map[string]any{"alg": "ES256", "kid": "b"}, validClaims(nil), k.p256)
	tokenX := signJWT(t, map[string]any{"alg": "ES256", "kid": "x"}, validClaims(nil), k.p256)

	step := func(name, token string, wantErr error, wantFetches int32) {
		t.Helper()
		// The verifier's clock does not move; only the key set's does.
		_, err := v.verify(context.Background(), token)
		if !errors.Is(err, wantErr) {
			t.Fatalf("%s: err %v, want %v", name, err, wantErr)
		}
		if n := srv.fetches.Load(); n != wantFetches {
			t.Fatalf("%s: fetched %d times, want %d", name, n, wantFetches)
		}
	}

	step("first use fetches", tokenA, nil, 1)
	srv.serve(http.StatusOK, keyA, keyB)
	step("unknown key within MinRefreshInterval", tokenB, errJWTNoKey, 1)
	clock.advance(time.Minute)
	step("unknown key refetches", tokenB, nil, 2)
	clock.advance(30 * time.Second)
	step("made-up key within MinRefreshInterval", tokenX, errJWTNoKey, 2)
	clock.advance(2 * time.Hour)
	step("stale keys refetch", tokenA, nil, 3)

	srv.serve(http.StatusInternalServerError)
	clock.advance(2 * time.Hour)
	step("failed refresh keeps the old keys", tokenA, nil, 4)
	step("failed refresh is not retried within MinRefreshInterval", tokenB, nil, 4)
	step("unknown key while the provider fails", tokenX, errJWTKeySet, 4)

	srv.serve(http.StatusOK, keyB)
	clock.advance(time.Minute)
	step("recovered provider replaces the keys", tokenB, nil, 5)
	clock.advance(time.Minute)
	step("removed key is gone", tokenA, errJWTNoKey, 6)
}

func TestJWKSInitialFailure(t *testing.T) {
	k := jwtTestKeys()
	srv := newJWKSServer(t)
	srv.serve(http.StatusServiceUnavailable)
	clock := &testClock{}
	v := testVerifier(&JWKS{URL: srv.URL, clock: clock.now})
	token := signJWT(t, map[string]any{"alg": "ES256", "kid": "e"}, validClaims(nil), k.p256)

	for range 3 {
		if _, err := v.verify(context.Background(), token); !errors.Is(err, errJWTKeySet) {
			t.Fatalf("err %v, want %v", err, errJWTKeySet)
		}
	}
	if n := srv.fetches.Load(); n != 1 {
		t.Fatalf("fetched %d times, want 1", n)
	}

	srv.serve(http.StatusOK, ecJWK("e", &k.p256.PublicKey))
	clock.advance(defaultJWKSMinRefreshInterval)
	if _, err := v.verify(context.Background(), token); err != nil {
		t.Fatal(err)
	}
}

func TestJWKSBadResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"not found", http.StatusNotFound, `{"keys":[]}`},
		{"not JSON", http.StatusOK, `<html>`},
		{"no usable keys", http.StatusOK, `{"keys":[{"kty":"oct","k":"AAAA"}]}`},
		{"too large", http.StatusOK, `{"keys":[],"pad":"` + strings.Repeat("x", jwksMaxBytes) + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newJWKSServer(t)
			srv.serveRaw(tc.status, []byte(tc.body))
			if _, err := (&JWKS{URL: srv.URL}).Keys(context.Background(), "", "RS256"); err == nil {
				t.Fatal("no error")
			}
		})
	}
}

func TestJWKSConcurrentFirstUse(t *testing.T) {
	k := jwtTestKeys()
	srv := newJWKSServer(t, ecJWK("e", &k.p256.PublicKey))
	srv.mu.Lock()
	srv.delay = 20 * time.Millisecond
	srv.mu.Unlock()
	v := testVerifier(&JWKS{URL: srv.URL})
	token := signJWT(t, map[string]any{"alg": "ES256", "kid": "e"}, validClaims(nil), k.p256)

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if _, err := v.verify(context.Background(), token); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := srv.fetches.Load(); n != 1 {
		t.Fatalf("fetched %d times, want 1", n)
	}
}

func TestJWKSFetchOutlivesRequest(t *testing.T) {
	k := jwtTestKeys()
	srv := newJWKSServer(t, ecJWK("e", &k.p256.PublicKey))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	keys, err := (&JWKS{URL: srv.URL}).Keys(ctx, "e", "ES256")
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys %v, err %v", keys, err)
	}
}

func TestJWKSValidate(t *testing.T) {
	for _, u := range []string{"", "jwks.json", "/jwks.json", "ftp://idp.example/jwks", "https://", "https://idp.example/%zz"} {
		if err := (&JWKS{URL: u}).validate(); err == nil {
			t.Errorf("URL %q accepted", u)
		}
	}
	if err := (&JWKS{URL: "https://idp.example/.well-known/jwks.json"}).validate(); err != nil {
		t.Fatal(err)
	}
}

// TestJWKSDoesNotQueueBehindFetch checks that requests that do not need a fetch never wait for
// one in flight: those with made-up key IDs during MinRefreshInterval, and those whose stale
// keys still verify.
func TestJWKSDoesNotQueueBehindFetch(t *testing.T) {
	k := jwtTestKeys()
	srv := newJWKSServer(t, ecJWK("e", &k.p256.PublicKey))
	clock := &testClock{}
	j := &JWKS{URL: srv.URL, clock: clock.now}
	if _, err := j.Keys(context.Background(), "e", "ES256"); err != nil {
		t.Fatal(err)
	}

	j.mu.Lock() // a fetch in flight
	defer j.mu.Unlock()

	lookup := func(name, kid string, wantKeys int) {
		t.Helper()
		done := make(chan []JWTKey, 1)
		go func() {
			keys, _ := j.Keys(context.Background(), kid, "ES256")
			done <- keys
		}()
		select {
		case keys := <-done:
			if len(keys) != wantKeys {
				t.Fatalf("%s: %d keys, want %d", name, len(keys), wantKeys)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: waited for the fetch in flight", name)
		}
	}
	lookup("made-up key ID", "x", 0)
	clock.advance(2 * defaultJWKSRefreshInterval)
	lookup("stale keys", "e", 1)
}
