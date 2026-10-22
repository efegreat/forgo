package jwtauth_test

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/efegreat/forgo/jwtauth"
	"github.com/efegreat/forgo/operation"
)

func ExampleAuthenticator() {
	secret := []byte("an HMAC secret of at least 32 bytes")
	auth, err := jwtauth.New(jwtauth.Config{
		Keys:   jwtauth.Keys{{Algorithm: "HS256", Key: secret}},
		Issuer: "https://auth.example.com",
	})
	if err != nil {
		panic(err)
	}

	// A token as an identity provider would issue it.
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss":    "https://auth.example.com",
		"sub":    "user-42",
		"exp":    time.Now().Add(time.Hour).Unix(),
		"tenant": "acme",
	}).SignedString(secret)
	if err != nil {
		panic(err)
	}

	// The servers' JWTAuth middlewares do this for every request, with the token of its input.
	op := &operation.Context{Context: context.Background()}
	if err := auth.Authenticate(op, token); err != nil {
		panic(err)
	}

	// Handlers find the claims in the context they receive, on any transport.
	claims, ok := jwtauth.ClaimsFromCtx(op)
	var custom struct {
		Tenant string `json:"tenant"`
	}
	if err := claims.Decode(&custom); err != nil {
		panic(err)
	}
	fmt.Println(ok, claims.Subject, custom.Tenant)
	// Output: true user-42 acme
}
