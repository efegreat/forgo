// Package jwtauth authenticates operations with JSON Web Tokens, whatever their transport. It looks keys up
// (Keys known up front, or the JWKS of an identity provider), verifies tokens with golang-jwt, and stores
// their claims in the operation's context, where ClaimsFromCtx finds them.
//
// The servers wrap it in middlewares that take the token from their own input: http.JWTAuth from the
// Authorization header or a cookie, grpc.JWTAuth from the authorization metadata.
package jwtauth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/efegreat/forgo/operation"

	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrUnauthenticated rejects operations that carry no token.
	ErrUnauthenticated = operation.Error{
		Code:     "unauthenticated",
		Status:   operation.StatusUnauthenticated,
		Message:  "authentication required",
		Severity: operation.SeverityLight,
	}
	// ErrInvalidToken rejects operations whose token cannot be verified or whose claims are not accepted.
	ErrInvalidToken = operation.Error{
		Code:     "invalid_token",
		Status:   operation.StatusUnauthenticated,
		Message:  "invalid token",
		Severity: operation.SeverityLight,
	}
	// ErrTokenExpired rejects operations whose token is otherwise valid but expired, so clients know to
	// refresh it.
	ErrTokenExpired = operation.Error{
		Code:     "token_expired",
		Status:   operation.StatusUnauthenticated,
		Message:  "token expired",
		Severity: operation.SeverityLight,
	}
	// ErrAuthUnavailable fails operations when the keys to verify their token cannot be looked up.
	ErrAuthUnavailable = operation.Error{
		Code:     "auth_unavailable",
		Status:   operation.StatusUnavailable,
		Message:  "authentication unavailable",
		Severity: operation.SeverityCritical,
	}
)

// Config configures an Authenticator. The servers' JWTAuth configs hold the same fields.
type Config struct {
	// Keys verify token signatures. Required: Keys for keys known up front, a *JWKS for an identity
	// provider's key set URL, or any IKeySet.
	Keys IKeySet
	// Issuer, when set, must equal the iss claim.
	Issuer string
	// Audience, when set, must contain one of the aud claim values.
	Audience []string
	// Leeway tolerates clock skew between the issuer and the server when checking exp and nbf.
	Leeway time.Duration
	// Optional lets operations without a token through, without claims. A token that is sent must still
	// be valid.
	Optional bool
}

// Authenticator verifies the tokens of operations. It is safe for concurrent use, and built once.
type Authenticator struct {
	keys     IKeySet
	parser   *jwt.Parser // safe for concurrent use
	optional bool
}

// New validates c and returns its Authenticator.
//
// Tokens are parsed and verified by github.com/golang-jwt/jwt/v5, with keys looked up in c.Keys. A token
// must use one of HS256, HS384, HS512, RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384, ES512 or
// EdDSA. "none" is never accepted, and every algorithm only accepts keys of its own type and strength. A
// token must have an exp claim; exp, nbf, iss and aud are validated. Tokens with crit headers are rejected.
func New(c Config) (*Authenticator, error) {
	return newAuthenticator(c, time.Now)
}

// newAuthenticator returns the Authenticator of c, which checks tokens against the clock now.
func newAuthenticator(c Config, now func() time.Time) (*Authenticator, error) {
	if c.Keys == nil {
		return nil, errors.New("jwtauth: Keys is required")
	}
	if v, ok := c.Keys.(interface{ validate() error }); ok {
		if err := v.validate(); err != nil {
			return nil, fmt.Errorf("jwtauth: %w", err)
		}
	}
	if c.Leeway < 0 {
		return nil, errors.New("jwtauth: Leeway cannot be negative")
	}
	return build(c, now), nil
}

// build returns the Authenticator of c, valid or not.
func build(c Config, now func() time.Time) *Authenticator {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(slices.Sorted(maps.Keys(algorithms))),
		jwt.WithExpirationRequired(),
		jwt.WithStrictDecoding(),
		jwt.WithLeeway(c.Leeway),
		jwt.WithTimeFunc(now),
	}
	if c.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(c.Issuer))
	}
	if len(c.Audience) > 0 {
		opts = append(opts, jwt.WithAudience(slices.Clone(c.Audience)...)) // any one of them
	}
	return &Authenticator{keys: c.Keys, parser: jwt.NewParser(opts...), optional: c.Optional}
}

// Authenticate verifies token, which a transport took from the operation's input, and stores its claims
// in ctx for ClaimsFromCtx. An empty token is no token: it passes when the Config is Optional. Otherwise
// it fails with ErrUnauthenticated, ErrInvalidToken, ErrTokenExpired or ErrAuthUnavailable, which wrap
// the reason.
func (a *Authenticator) Authenticate(ctx *operation.Context, token string) error {
	if token == "" {
		if a.optional {
			return nil
		}
		return ErrUnauthenticated
	}
	claims, err := a.verify(ctx, token)
	switch {
	case errors.Is(err, errKeySet):
		return ErrAuthUnavailable.Wrap(err)
	case errors.Is(err, jwt.ErrTokenExpired):
		return ErrTokenExpired.Wrap(err)
	case err != nil:
		return ErrInvalidToken.Wrap(err)
	}
	ctx.Context = context.WithValue(ctx.Context, claimsKey{}, claims)
	return nil
}

// verify returns the claims of token if its signature and claims are valid. Errors wrap the golang-jwt
// sentinels, such as jwt.ErrTokenExpired, or errKeySet when keys cannot be looked up.
func (a *Authenticator) verify(ctx context.Context, token string) (*Claims, error) {
	var p payload
	if _, err := a.parser.ParseWithClaims(token, &p, func(t *jwt.Token) (any, error) {
		return a.key(ctx, t)
	}); err != nil {
		return nil, err
	}
	return p.claims(), nil
}

// key returns the key, or the jwt.VerificationKeySet of keys, that may verify t.
func (a *Authenticator) key(ctx context.Context, t *jwt.Token) (any, error) {
	// Extensions listed in crit must be understood, and none are. golang-jwt does not check crit.
	if _, ok := t.Header["crit"]; ok {
		return nil, errCritical
	}
	kid, ok := t.Header["kid"].(string)
	if !ok && t.Header["kid"] != nil {
		return nil, errKeyID
	}

	alg := t.Method.Alg()
	keys, err := a.keys.Keys(ctx, kid, alg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errKeySet, err)
	}

	// Re-checked because IKeySet implementations may return keys that do not fit.
	var set jwt.VerificationKeySet
	var only any
	for _, k := range keys {
		if !k.fits(alg) {
			continue
		}
		if only == nil {
			only = k.Key
			continue
		}
		if set.Keys == nil {
			set.Keys = append(set.Keys, only)
		}
		set.Keys = append(set.Keys, k.Key)
	}
	switch {
	case set.Keys != nil:
		return set, nil
	case only != nil:
		return only, nil
	}
	return nil, errNoKey
}

// BearerToken returns the token of an Authorization value of the Bearer scheme, such as "Bearer abc", or
// "" for any other value.
func BearerToken(authorization string) string {
	scheme, token, ok := strings.Cut(authorization, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

// Claims are the claims of a verified token. Time claims the token does not have are zero.
type Claims struct {
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
func (c *Claims) Decode(v any) error {
	return json.Unmarshal(c.payload, v)
}

type claimsKey struct{}

// ClaimsFromCtx returns the claims an Authenticator verified for the operation ctx belongs to, on any
// transport. It reports false when the operation has no verified token.
func ClaimsFromCtx(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(*Claims)
	return c, ok
}

// payload is what golang-jwt decodes and validates the payload into. It keeps the raw payload for
// Claims.Decode, so custom claims never need a second base64 decode.
type payload struct {
	jwt.RegisteredClaims
	raw []byte
}

func (p *payload) UnmarshalJSON(b []byte) error {
	p.raw = bytes.Clone(b)
	return json.Unmarshal(b, &p.RegisteredClaims)
}

func (p *payload) claims() *Claims {
	return &Claims{
		Issuer:    p.Issuer,
		Subject:   p.Subject,
		Audience:  p.Audience,
		ExpiresAt: numericTime(p.ExpiresAt),
		NotBefore: numericTime(p.NotBefore),
		IssuedAt:  numericTime(p.IssuedAt),
		ID:        p.ID,
		payload:   p.raw,
	}
}

func numericTime(d *jwt.NumericDate) time.Time {
	if d == nil {
		return time.Time{}
	}
	return d.Time
}
