package jwtauth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minHMACKeyBytes = 32
	minRSAKeyBits   = 2048

	defaultJWKSRefreshInterval    = time.Hour
	defaultJWKSMinRefreshInterval = time.Minute
	jwksFetchTimeout              = 10 * time.Second
	jwksMaxBytes                  = 1 << 20
)

var (
	errAlgorithm = errors.New("jwtauth: unsupported algorithm")
	errCritical  = errors.New("jwtauth: unsupported critical header")
	errKeyID     = errors.New("jwtauth: kid header is not a string")
	errNoKey     = errors.New("jwtauth: no key for the token")
	errKeySet    = errors.New("jwtauth: key set unavailable")
)

var b64 = base64.RawURLEncoding.Strict()

type keyKind uint8

const (
	keyHMAC keyKind = iota + 1
	keyRSA
	keyECDSA
	keyEd25519
)

type algorithm struct {
	kind  keyKind
	hash  crypto.Hash // HMAC keys must be at least as long as its output
	curve string      // ECDSA curve name
}

// algorithms are the accepted JWS algorithms, all verified by golang-jwt. "none" is deliberately absent.
var algorithms = map[string]algorithm{
	"HS256": {kind: keyHMAC, hash: crypto.SHA256},
	"HS384": {kind: keyHMAC, hash: crypto.SHA384},
	"HS512": {kind: keyHMAC, hash: crypto.SHA512},
	"RS256": {kind: keyRSA},
	"RS384": {kind: keyRSA},
	"RS512": {kind: keyRSA},
	"PS256": {kind: keyRSA},
	"PS384": {kind: keyRSA},
	"PS512": {kind: keyRSA},
	"ES256": {kind: keyECDSA, curve: "P-256"},
	"ES384": {kind: keyECDSA, curve: "P-384"},
	"ES512": {kind: keyECDSA, curve: "P-521"},
	"EdDSA": {kind: keyEd25519},
}

// fits reports whether key has the type, curve and strength the algorithm needs. golang-jwt checks
// key types too; binding algorithms to key types here as well keeps unfit keys from ever reaching
// it, so an RSA public key can never be tried as an HMAC secret, and weak keys are never used.
func (a algorithm) fits(key any) bool {
	switch a.kind {
	case keyHMAC:
		k, ok := key.([]byte)
		return ok && len(k) >= a.hash.Size() // RFC 7518, section 3.2
	case keyRSA:
		k, ok := key.(*rsa.PublicKey)
		return ok && k != nil && k.N != nil && k.N.BitLen() >= minRSAKeyBits
	case keyECDSA:
		k, ok := key.(*ecdsa.PublicKey)
		return ok && k != nil && k.Curve != nil && k.Curve.Params().Name == a.curve
	case keyEd25519:
		k, ok := key.(ed25519.PublicKey)
		return ok && len(k) == ed25519.PublicKeySize
	}
	return false
}

// Key is a key that verifies token signatures.
type Key struct {
	// ID is matched against the kid header of tokens. A key without an ID is a candidate
	// for every token whose kid names no key.
	ID string
	// Algorithm pins the key to one algorithm, such as "RS256". When empty, the key verifies
	// every algorithm its type supports.
	Algorithm string
	// Key is the verification key:
	//   - []byte, an HMAC secret of at least 32 bytes, for HS256, HS384 and HS512
	//     (HS384 and HS512 need at least 48 and 64 bytes)
	//   - *rsa.PublicKey of at least 2048 bits, for RS256, RS384, RS512, PS256, PS384 and PS512
	//   - *ecdsa.PublicKey on P-256, P-384 or P-521, for ES256, ES384 and ES512 respectively
	//   - ed25519.PublicKey, for EdDSA
	Key any
}

// fits reports whether k may verify a token signed with alg.
func (k Key) fits(alg string) bool {
	a, ok := algorithms[alg]
	return ok && (k.Algorithm == "" || k.Algorithm == alg) && a.fits(k.Key)
}

func (k Key) validate() error {
	switch key := k.Key.(type) {
	case []byte:
		if len(key) < minHMACKeyBytes {
			return fmt.Errorf("HMAC key must be at least %d bytes", minHMACKeyBytes)
		}
	case *rsa.PublicKey:
		if key == nil || key.N == nil || key.N.BitLen() < minRSAKeyBits {
			return fmt.Errorf("RSA key must be at least %d bits", minRSAKeyBits)
		}
	case *ecdsa.PublicKey:
		if key == nil || key.Curve == nil {
			return errors.New("ECDSA key is nil")
		}
		switch key.Curve.Params().Name {
		case "P-256", "P-384", "P-521":
		default:
			return errors.New("ECDSA key must be on P-256, P-384 or P-521")
		}
	case ed25519.PublicKey:
		if len(key) != ed25519.PublicKeySize {
			return fmt.Errorf("Ed25519 key must be %d bytes", ed25519.PublicKeySize)
		}
	default:
		return fmt.Errorf("unsupported key type %T", k.Key)
	}

	if k.Algorithm != "" {
		a, ok := algorithms[k.Algorithm]
		if !ok {
			return fmt.Errorf("%w %q", errAlgorithm, k.Algorithm)
		}
		if !a.fits(k.Key) {
			if a.kind == keyHMAC {
				return fmt.Errorf("%s needs an HMAC key of at least %d bytes", k.Algorithm, a.hash.Size())
			}
			return fmt.Errorf("%T cannot verify %s", k.Key, k.Algorithm)
		}
	}
	return nil
}

// IKeySet provides the keys that may verify a token.
type IKeySet interface {
	// Keys returns the candidate keys for a token with the given kid and alg headers.
	// An error means the keys could not be looked up, not that the token is invalid.
	Keys(ctx context.Context, kid, alg string) ([]Key, error)
}

// Keys is an IKeySet of keys known up front.
type Keys []Key

// Keys returns the keys with ID kid that fit alg, or, when no key has that ID, the keys without an ID that fit
// alg. It never fails.
func (ks Keys) Keys(_ context.Context, kid, alg string) ([]Key, error) {
	return ks.match(kid, alg), nil
}

func (ks Keys) validate() error {
	if len(ks) == 0 {
		return errors.New("no keys")
	}
	for i, k := range ks {
		if err := k.validate(); err != nil {
			return fmt.Errorf("key %d (%q): %w", i, k.ID, err)
		}
	}
	return nil
}

// match returns the keys that fit alg among the keys with ID kid, or, when no key has
// that ID, among the keys without an ID.
func (ks Keys) match(kid, alg string) []Key {
	if kid != "" && slices.ContainsFunc(ks, func(k Key) bool { return k.ID == kid }) {
		return ks.filter(kid, alg)
	}
	return ks.filter("", alg)
}

func (ks Keys) filter(id, alg string) []Key {
	var out []Key
	first := -1
	for i, k := range ks {
		if k.ID != id || !k.fits(alg) {
			continue
		}
		if first < 0 {
			first = i
			continue
		}
		if out == nil {
			out = append(out, ks[first])
		}
		out = append(out, k)
	}
	if out == nil && first >= 0 {
		// The common single match shares the backing array instead of allocating.
		return ks[first : first+1 : first+1]
	}
	return out
}

// JWKS is an IKeySet that fetches a JSON Web Key Set from URL, typically the jwks_uri
// of an identity provider.
//
// Keys are fetched on first use and cached. They are fetched again once RefreshInterval has
// passed, and when a token names a key the set does not have, which is how key rotations
// are picked up. Fetches happen at most once per MinRefreshInterval, so tokens with made-up
// key IDs cannot flood the provider. While fetches fail, the keys fetched last stay in use.
//
// Only signing keys are used: RSA, EC (P-256, P-384, P-521) and OKP (Ed25519) keys whose
// use is empty or "sig". A JWKS keeps state, so use it by pointer.
type JWKS struct {
	// URL of the key set. Required.
	URL string
	// Client fetches the key set. Default: http.DefaultClient. Each fetch is limited to 10 seconds.
	Client *http.Client
	// RefreshInterval is how long fetched keys are used before they are fetched again. Default: 1 hour.
	RefreshInterval time.Duration
	// MinRefreshInterval is the minimum time between two fetches. Default: 1 minute.
	MinRefreshInterval time.Duration

	mu    sync.Mutex // serializes fetches
	state atomic.Pointer[jwksState]
	clock func() time.Time // replaced in tests
}

type jwksState struct {
	keys      Keys
	fetched   time.Time // last successful fetch
	attempted time.Time // last fetch, successful or not
	err       error     // error of the last fetch
}

func (j *JWKS) validate() error {
	u, err := url.Parse(j.URL)
	if err != nil {
		return fmt.Errorf("jwtauth: jwks: invalid URL: %w", err)
	}
	if (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("jwtauth: jwks: URL must be an absolute http(s) URL, got %q", j.URL)
	}
	return nil
}

// Keys returns the keys of the set that may verify a token with the given kid and alg headers, chosen like
// Keys.Keys. It fetches the set as JWKS describes: on first use, once the keys are older than
// RefreshInterval, and when no key matches, at most once per MinRefreshInterval. It fails when no key
// matches and the last fetch failed.
func (j *JWKS) Keys(ctx context.Context, kid, alg string) ([]Key, error) {
	if st := j.state.Load(); st != nil {
		keys := st.keys.match(kid, alg)
		now := j.now()
		switch {
		case len(keys) > 0 && now.Sub(st.fetched) < j.refreshInterval():
			return keys, nil
		case len(keys) > 0:
			// The keys are stale but still verify: one request refreshes them, the others keep using them.
			if !j.mu.TryLock() {
				return keys, nil
			}
		case now.Sub(st.attempted) < j.minRefreshInterval():
			return nil, st.err
		default:
			j.mu.Lock()
		}
	} else {
		j.mu.Lock()
	}
	defer j.mu.Unlock()

	return j.refresh(ctx, kid, alg)
}

// refresh fetches the key set, unless another request did so while this one waited. j.mu must be held.
func (j *JWKS) refresh(ctx context.Context, kid, alg string) ([]Key, error) {
	now := j.now()
	prev := j.state.Load()
	if prev != nil && now.Sub(prev.attempted) < j.minRefreshInterval() {
		if keys := prev.keys.match(kid, alg); len(keys) > 0 {
			return keys, nil
		}
		return nil, prev.err
	}

	keys, err := j.fetch(ctx)
	next := &jwksState{attempted: now, err: err}
	if err == nil {
		next.keys, next.fetched = keys, now
	} else if prev != nil {
		next.keys, next.fetched = prev.keys, prev.fetched
	}
	j.state.Store(next)

	if matched := next.keys.match(kid, alg); len(matched) > 0 {
		return matched, nil
	}
	return nil, err
}

func (j *JWKS) fetch(ctx context.Context) (Keys, error) {
	// A fetch serves every request waiting for it, so the request that started it must not cancel it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jwksFetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("jwtauth: jwks: %w", err)
	}
	req.Header.Set("Accept", "application/jwk-set+json, application/json")

	client := j.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jwtauth: jwks: fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwtauth: jwks: fetch: unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, jwksMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("jwtauth: jwks: read: %w", err)
	}
	if len(body) > jwksMaxBytes {
		return nil, fmt.Errorf("jwtauth: jwks: key set larger than %d bytes", jwksMaxBytes)
	}
	return parseJWKS(body)
}

func (j *JWKS) now() time.Time {
	if j.clock != nil {
		return j.clock()
	}
	return time.Now()
}

func (j *JWKS) refreshInterval() time.Duration {
	if j.RefreshInterval > 0 {
		return j.RefreshInterval
	}
	return defaultJWKSRefreshInterval
}

func (j *JWKS) minRefreshInterval() time.Duration {
	if j.MinRefreshInterval > 0 {
		return j.MinRefreshInterval
	}
	return defaultJWKSMinRefreshInterval
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// parseJWKS returns the usable signing keys of a JWK Set. Keys it cannot use are skipped,
// so one unsupported key does not take the whole set down.
func parseJWKS(data []byte) (Keys, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("jwtauth: jwks: decode: %w", err)
	}

	keys := make(Keys, 0, len(set.Keys))
	for _, k := range set.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		key, err := k.publicKey()
		if err != nil {
			continue
		}
		jk := Key{ID: k.Kid, Algorithm: k.Alg, Key: key}
		if jk.validate() != nil {
			continue
		}
		keys = append(keys, jk)
	}
	if len(keys) == 0 {
		return nil, errors.New("jwtauth: jwks: no usable signing keys")
	}
	return keys, nil
}

func (k jwk) publicKey() (any, error) {
	switch k.Kty {
	case "RSA":
		n, err := b64.DecodeString(k.N)
		if err != nil {
			return nil, err
		}
		e, err := b64.DecodeString(k.E)
		if err != nil {
			return nil, err
		}
		if len(e) == 0 || len(e) > 4 {
			return nil, errors.New("invalid RSA exponent")
		}
		exp := 0
		for _, b := range e {
			exp = exp<<8 | int(b)
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exp}, nil

	case "EC":
		var curve elliptic.Curve
		switch k.Crv {
		case "P-256":
			curve = elliptic.P256()
		case "P-384":
			curve = elliptic.P384()
		case "P-521":
			curve = elliptic.P521()
		default:
			return nil, fmt.Errorf("unsupported curve %q", k.Crv)
		}
		size := (curve.Params().BitSize + 7) / 8
		x, err := b64.DecodeString(k.X)
		if err != nil {
			return nil, err
		}
		y, err := b64.DecodeString(k.Y)
		if err != nil {
			return nil, err
		}
		if len(x) != size || len(y) != size {
			return nil, errors.New("invalid EC coordinates")
		}
		point := make([]byte, 0, 1+2*size)
		point = append(append(append(point, 4), x...), y...)
		return ecdsa.ParseUncompressedPublicKey(curve, point)

	case "OKP":
		if k.Crv != "Ed25519" {
			return nil, fmt.Errorf("unsupported curve %q", k.Crv)
		}
		x, err := b64.DecodeString(k.X)
		if err != nil {
			return nil, err
		}
		if len(x) != ed25519.PublicKeySize {
			return nil, errors.New("invalid Ed25519 key")
		}
		return ed25519.PublicKey(x), nil
	}
	return nil, fmt.Errorf("unsupported key type %q", k.Kty)
}
