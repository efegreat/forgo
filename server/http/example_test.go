package http_test

import (
	"context"
	"os"

	"github.com/efegreat/forgo"
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
				forgo.MiddlewareAs(http.CORS(http.CORSConfig{AllowOrigins: []string{"https://app.example.com"}})),
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
