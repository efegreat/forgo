package http

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rsa"
	_ "crypto/sha256" // registers SHA-256 for crypto.Hash
	_ "crypto/sha512" // registers SHA-384 and SHA-512 for crypto.Hash
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
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

	// maxNumericDate is 9999-12-31T23:59:59Z. Later dates are rejected rather than overflowing time.Time.
	maxNumericDate = 253402300799
)

var (
	errJWTMalformed   = errors.New("jwt: malformed token")
	errJWTAlgorithm   = errors.New("jwt: unsupported algorithm")
	errJWTCritical    = errors.New("jwt: unsupported critical header")
	errJWTNoKey       = errors.New("jwt: no key for the token")
	errJWTSignature   = errors.New("jwt: invalid signature")
	errJWTKeySet      = errors.New("jwt: key set unavailable")
	errJWTNoExpiry    = errors.New("jwt: token has no expiry")
	errJWTExpired     = errors.New("jwt: token expired")
	errJWTNotYetValid = errors.New("jwt: token not valid yet")
	errJWTIssuer      = errors.New("jwt: issuer mismatch")
	errJWTAudience    = errors.New("jwt: audience mismatch")
	errJWTKeyType     = errors.New("jwt: key does not fit the algorithm")
)

var b64 = base64.RawURLEncoding.Strict()

type jwtKeyKind uint8

const (
	jwtKeyHMAC jwtKeyKind = iota + 1
	jwtKeyRSA
	jwtKeyRSAPSS
	jwtKeyECDSA
	jwtKeyEd25519
)

type jwtAlgorithm struct {
	kind  jwtKeyKind
	hash  crypto.Hash // unused by Ed25519, which signs the input itself
	curve string      // ECDSA curve name
}

// jwtAlgorithms are the supported JWS algorithms. "none" is deliberately absent.
var jwtAlgorithms = map[string]jwtAlgorithm{
	"HS256":   {kind: jwtKeyHMAC, hash: crypto.SHA256},
	"HS384":   {kind: jwtKeyHMAC, hash: crypto.SHA384},
	"HS512":   {kind: jwtKeyHMAC, hash: crypto.SHA512},
	"RS256":   {kind: jwtKeyRSA, hash: crypto.SHA256},
	"RS384":   {kind: jwtKeyRSA, hash: crypto.SHA384},
	"RS512":   {kind: jwtKeyRSA, hash: crypto.SHA512},
	"PS256":   {kind: jwtKeyRSAPSS, hash: crypto.SHA256},
	"PS384":   {kind: jwtKeyRSAPSS, hash: crypto.SHA384},
	"PS512":   {kind: jwtKeyRSAPSS, hash: crypto.SHA512},
	"ES256":   {kind: jwtKeyECDSA, hash: crypto.SHA256, curve: "P-256"},
	"ES384":   {kind: jwtKeyECDSA, hash: crypto.SHA384, curve: "P-384"},
	"ES512":   {kind: jwtKeyECDSA, hash: crypto.SHA512, curve: "P-521"},
	"EdDSA":   {kind: jwtKeyEd25519},
	"Ed25519": {kind: jwtKeyEd25519},
}

// fits reports whether key has the type, and for ECDSA the curve, the algorithm needs.
// Binding algorithms to key types is what defeats algorithm confusion: an RSA public key
// can never be used as an HMAC secret.
func (a jwtAlgorithm) fits(key any) bool {
	switch a.kind {
	case jwtKeyHMAC:
		_, ok := key.([]byte)
		return ok
	case jwtKeyRSA, jwtKeyRSAPSS:
		k, ok := key.(*rsa.PublicKey)
		return ok && k != nil
	case jwtKeyECDSA:
		k, ok := key.(*ecdsa.PublicKey)
		return ok && k != nil && k.Curve != nil && k.Curve.Params().Name == a.curve
	case jwtKeyEd25519:
		_, ok := key.(ed25519.PublicKey)
		return ok
	}
	return false
}

func (a jwtAlgorithm) verify(key any, input string, sig []byte) error {
	if !a.fits(key) {
		return errJWTKeyType
	}

	var ok bool
	switch a.kind {
	case jwtKeyHMAC:
		secret := key.([]byte)
		if len(secret) < a.hash.Size() {
			return fmt.Errorf("%w: HMAC key shorter than the hash", errJWTKeyType)
		}
		mac := hmac.New(a.hash.New, secret)
		_, _ = io.WriteString(mac, input)
		ok = hmac.Equal(mac.Sum(nil), sig)
	case jwtKeyRSA:
		pub := key.(*rsa.PublicKey)
		if pub.N == nil || pub.N.BitLen() < minRSAKeyBits {
			return fmt.Errorf("%w: RSA key shorter than %d bits", errJWTKeyType, minRSAKeyBits)
		}
		ok = rsa.VerifyPKCS1v15(pub, a.hash, a.digest(input), sig) == nil
	case jwtKeyRSAPSS:
		pub := key.(*rsa.PublicKey)
		if pub.N == nil || pub.N.BitLen() < minRSAKeyBits {
			return fmt.Errorf("%w: RSA key shorter than %d bits", errJWTKeyType, minRSAKeyBits)
		}
		ok = rsa.VerifyPSS(pub, a.hash, a.digest(input), sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto}) == nil
	case jwtKeyECDSA:
		// JWS encodes ECDSA signatures as fixed-size R || S, not ASN.1.
		pub := key.(*ecdsa.PublicKey)
		size := (pub.Curve.Params().BitSize + 7) / 8
		if len(sig) != 2*size {
			return errJWTSignature
		}
		r := new(big.Int).SetBytes(sig[:size])
		s := new(big.Int).SetBytes(sig[size:])
		ok = ecdsa.Verify(pub, a.digest(input), r, s)
	case jwtKeyEd25519:
		pub := key.(ed25519.PublicKey)
		ok = len(pub) == ed25519.PublicKeySize && ed25519.Verify(pub, []byte(input), sig)
	}

	if !ok {
		return errJWTSignature
	}
	return nil
}

func (a jwtAlgorithm) digest(input string) []byte {
	h := a.hash.New()
	_, _ = io.WriteString(h, input)
	return h.Sum(nil)
}

// JWTKey is a key that verifies token signatures.
type JWTKey struct {
	// ID is matched against the kid header of tokens. A key without an ID is a candidate
	// for every token whose kid names no key.
	ID string
	// Algorithm pins the key to one algorithm, such as "RS256". When empty, the key verifies
	// every algorithm its type supports.
	Algorithm string
	// Key is the verification key:
	//   - []byte, an HMAC secret of at least 32 bytes, for HS256, HS384 and HS512
	//   - *rsa.PublicKey of at least 2048 bits, for RS256, RS384, RS512, PS256, PS384 and PS512
	//   - *ecdsa.PublicKey on P-256, P-384 or P-521, for ES256, ES384 and ES512 respectively
	//   - ed25519.PublicKey, for EdDSA and Ed25519
	Key any
}

// fits reports whether k may verify a token signed with alg.
func (k JWTKey) fits(alg string) bool {
	a, ok := jwtAlgorithms[alg]
	return ok && (k.Algorithm == "" || k.Algorithm == alg) && a.fits(k.Key)
}

func (k JWTKey) validate() error {
	if k.Algorithm != "" {
		a, ok := jwtAlgorithms[k.Algorithm]
		if !ok {
			return fmt.Errorf("%w %q", errJWTAlgorithm, k.Algorithm)
		}
		if !a.fits(k.Key) {
			return fmt.Errorf("%T cannot verify %s", k.Key, k.Algorithm)
		}
		// RFC 7518 requires HMAC keys at least as long as the hash output.
		if secret, ok := k.Key.([]byte); ok && len(secret) < a.hash.Size() {
			return fmt.Errorf("%s needs an HMAC key of at least %d bytes", k.Algorithm, a.hash.Size())
		}
	}

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
	return nil
}

// IJWTKeySet provides the keys that may verify a token.
type IJWTKeySet interface {
	// Keys returns the candidate keys for a token with the given kid and alg headers.
	// An error means the keys could not be looked up, not that the token is invalid.
	Keys(ctx context.Context, kid, alg string) ([]JWTKey, error)
}

// JWTKeys is an IJWTKeySet of keys known up front.
type JWTKeys []JWTKey

func (ks JWTKeys) Keys(_ context.Context, kid, alg string) ([]JWTKey, error) {
	return ks.match(kid, alg), nil
}

func (ks JWTKeys) validate() error {
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
func (ks JWTKeys) match(kid, alg string) []JWTKey {
	if kid != "" && slices.ContainsFunc(ks, func(k JWTKey) bool { return k.ID == kid }) {
		return ks.filter(kid, alg)
	}
	return ks.filter("", alg)
}

func (ks JWTKeys) filter(id, alg string) []JWTKey {
	var out []JWTKey
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

// JWKS is an IJWTKeySet that fetches a JSON Web Key Set from URL, typically the jwks_uri
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
	keys      JWTKeys
	fetched   time.Time // last successful fetch
	attempted time.Time // last fetch, successful or not
	err       error     // error of the last fetch
}

func (j *JWKS) validate() error {
	u, err := url.Parse(j.URL)
	if err != nil {
		return fmt.Errorf("jwks: invalid URL: %w", err)
	}
	if (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("jwks: URL must be an absolute http(s) URL, got %q", j.URL)
	}
	return nil
}

func (j *JWKS) Keys(ctx context.Context, kid, alg string) ([]JWTKey, error) {
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
func (j *JWKS) refresh(ctx context.Context, kid, alg string) ([]JWTKey, error) {
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

func (j *JWKS) fetch(ctx context.Context) (JWTKeys, error) {
	// A fetch serves every request waiting for it, so the request that started it must not cancel it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jwksFetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	req.Header.Set("Accept", "application/jwk-set+json, application/json")

	client := j.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jwks: fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks: fetch: unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, jwksMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("jwks: read: %w", err)
	}
	if len(body) > jwksMaxBytes {
		return nil, fmt.Errorf("jwks: key set larger than %d bytes", jwksMaxBytes)
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
func parseJWKS(data []byte) (JWTKeys, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("jwks: decode: %w", err)
	}

	keys := make(JWTKeys, 0, len(set.Keys))
	for _, k := range set.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		key, err := k.publicKey()
		if err != nil {
			continue
		}
		jk := JWTKey{ID: k.Kid, Algorithm: k.Alg, Key: key}
		if jk.validate() != nil {
			continue
		}
		keys = append(keys, jk)
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks: no usable signing keys")
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

// JWTClaims are the claims of a verified token. Time claims the token does not have are zero.
type JWTClaims struct {
	Issuer    string
	Subject   string
	Audience  []string
	ExpiresAt time.Time
	NotBefore time.Time
	IssuedAt  time.Time
	ID        string

	payload []byte
}

// Decode unmarshals the token payload into v, for claims beyond the registered ones.
func (c *JWTClaims) Decode(v any) error {
	return json.Unmarshal(c.payload, v)
}

type jwtHeader struct {
	Alg  string          `json:"alg"`
	Kid  string          `json:"kid"`
	Crit json.RawMessage `json:"crit"`
}

type jwtPayload struct {
	Iss string          `json:"iss"`
	Sub string          `json:"sub"`
	Aud jwtAudience     `json:"aud"`
	Exp *jwtNumericDate `json:"exp"`
	Nbf *jwtNumericDate `json:"nbf"`
	Iat *jwtNumericDate `json:"iat"`
	Jti string          `json:"jti"`
}

// jwtAudience is a single string or an array of strings.
type jwtAudience []string

func (a *jwtAudience) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*a = jwtAudience{s}
		return nil
	}
	return json.Unmarshal(b, (*[]string)(a))
}

// jwtNumericDate is a number of seconds since the Unix epoch, possibly fractional.
type jwtNumericDate struct{ time.Time }

func (d *jwtNumericDate) UnmarshalJSON(b []byte) error {
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil || f < 0 || f > maxNumericDate {
		return errJWTMalformed
	}
	sec, frac := math.Modf(f)
	d.Time = time.Unix(int64(sec), int64(frac*1e9))
	return nil
}

func (d *jwtNumericDate) value() time.Time {
	if d == nil {
		return time.Time{}
	}
	return d.Time
}

// jwtVerifier verifies compact JWS tokens and validates their registered claims.
type jwtVerifier struct {
	keys     IJWTKeySet
	issuer   string
	audience []string
	leeway   time.Duration
	now      func() time.Time
}

func (v *jwtVerifier) verify(ctx context.Context, token string) (*JWTClaims, error) {
	h64, rest, ok := strings.Cut(token, ".")
	if !ok {
		return nil, errJWTMalformed
	}
	p64, s64, ok := strings.Cut(rest, ".")
	if !ok || strings.IndexByte(s64, '.') >= 0 {
		return nil, errJWTMalformed
	}

	var header jwtHeader
	if err := decodeSegment(h64, &header); err != nil {
		return nil, err
	}
	// Extensions listed in crit must be understood, and none are.
	if len(header.Crit) > 0 {
		return nil, errJWTCritical
	}
	alg, ok := jwtAlgorithms[header.Alg]
	if !ok {
		return nil, fmt.Errorf("%w %q", errJWTAlgorithm, header.Alg)
	}
	sig, err := b64.DecodeString(s64)
	if err != nil {
		return nil, errJWTMalformed
	}

	keys, err := v.keys.Keys(ctx, header.Kid, header.Alg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errJWTKeySet, err)
	}
	input := token[:len(h64)+1+len(p64)]
	err = errJWTNoKey
	for _, k := range keys {
		// Re-checked here because IJWTKeySet implementations may return keys that do not fit.
		if k.Algorithm != "" && k.Algorithm != header.Alg {
			continue
		}
		if err = alg.verify(k.Key, input, sig); err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	payload, err := b64.DecodeString(p64)
	if err != nil {
		return nil, errJWTMalformed
	}
	var p jwtPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("%w: %w", errJWTMalformed, err)
	}
	claims := &JWTClaims{
		Issuer:    p.Iss,
		Subject:   p.Sub,
		Audience:  p.Aud,
		ExpiresAt: p.Exp.value(),
		NotBefore: p.Nbf.value(),
		IssuedAt:  p.Iat.value(),
		ID:        p.Jti,
		payload:   payload,
	}
	if err := v.validate(claims); err != nil {
		return nil, err
	}
	return claims, nil
}

func (v *jwtVerifier) validate(c *JWTClaims) error {
	now := v.now()
	if c.ExpiresAt.IsZero() {
		return errJWTNoExpiry
	}
	if !now.Before(c.ExpiresAt.Add(v.leeway)) {
		return errJWTExpired
	}
	if !c.NotBefore.IsZero() && now.Add(v.leeway).Before(c.NotBefore) {
		return errJWTNotYetValid
	}
	if v.issuer != "" && c.Issuer != v.issuer {
		return errJWTIssuer
	}
	if len(v.audience) > 0 && !slices.ContainsFunc(c.Audience, func(a string) bool { return slices.Contains(v.audience, a) }) {
		return errJWTAudience
	}
	return nil
}

func decodeSegment(seg string, v any) error {
	data, err := b64.DecodeString(seg)
	if err != nil {
		return errJWTMalformed
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%w: %w", errJWTMalformed, err)
	}
	return nil
}
