package http_test

import (
	"context"
	"fmt"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/http"
)

type CreateUserReq struct {
	Email string `json:"email" validate:"required,email"`
}

type User struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

type UserService struct{}

func NewUserService() *UserService { return &UserService{} }

func (s *UserService) Create(ctx context.Context, req CreateUserReq) (User, error) {
	return User{ID: "42", Email: req.Email}, nil
}

func ExampleConfig() {
	conf := http.Config{
		Addr: ":8080",
		ServerConfig: forgo.ServerConfig{
			// Server middlewares also run for requests that match no route, such as CORS preflights.
			Middlewares: []forgo.MiddlewareFactory{
				http.CORS(http.CORSConfig{AllowOrigins: []string{"https://app.example.com"}}),
			},
		},
		Routes: []http.RouteConfig{
			{
				Name:    "health",
				Method:  "GET",
				Path:    "/health",
				Handler: http.EmptyHandler(),
				Args:    map[any]any{http.ArgKeyRecorderDisabled: true}, // no log line per probe
			},
			{
				// The body is bound into CreateUserReq and validated before Create runs, and the
				// result is answered in the JSON envelope: {"reqId":"…","status":200,"result":{…}}.
				Name:    "createUser",
				Method:  "POST",
				Path:    "/users",
				Version: "v1",
				Handler: http.HandlerFor((*UserService).Create),
			},
		},
	}

	app := forgo.NewApp(
		forgo.WithName("users-api"),
		forgo.WithProviders(NewUserService),
		forgo.WithServers(conf),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

// ErrUnknownKey answers a request without a known API key.
var ErrUnknownKey = operation.Error{Code: "unknown_api_key", Status: operation.StatusUnauthenticated, Message: "unknown API key"}

type KeyStore struct{ keys map[string]bool }

// NewAPIKeyAuth rejects requests whose X-API-Key header holds no known key.
func NewAPIKeyAuth(s *KeyStore) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if !s.keys[ctx.State.(*http.State).Request.Header.Get("X-API-Key")] {
				return ErrUnknownKey
			}
			return next(ctx)
		}
	}
}

func ExampleWithContract() {
	// Declared once, next to the middleware, and attached to the server or to routes. The openapi command then
	// documents an apiKey security scheme for the header, and the 401 response, on every route it runs on.
	apiKeyAuth := http.WithContract(http.MiddlewareContract{
		Auth:       &http.AuthContract{Credentials: []http.Credential{{Kind: http.CredentialHeader, Name: "X-API-Key"}}},
		Rejections: []http.Rejection{{Err: ErrUnknownKey}},
	}, forgo.MiddlewareAs(NewAPIKeyAuth))

	// The contract is read without constructing the middleware, so the KeyStore is never resolved.
	c, _ := http.MiddlewareContractOf(apiKeyAuth)
	fmt.Println(c.Auth.Credentials[0].Name, c.Rejections[0].Err)
	// Output: X-API-Key unknown API key
}
