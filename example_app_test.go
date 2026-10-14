package forgo_test

import (
	"context"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/http"
)

type GetUserReq struct {
	ID string `uri:"id" validate:"required"`
}

type User struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

var ErrUserNotFound = operation.Error{
	Code:     "user_not_found",
	Status:   operation.StatusNotFound,
	Message:  "user not found",
	Severity: operation.SeverityLight,
}

type UserService struct {
	users map[string]User
}

func NewUserService() *UserService {
	return &UserService{users: map[string]User{"42": {ID: "42", Email: "ada@example.com"}}}
}

func (s *UserService) Get(ctx context.Context, req GetUserReq) (User, error) {
	u, ok := s.users[req.ID]
	if !ok {
		return User{}, ErrUserNotFound
	}
	return u, nil
}

func ExampleNewApp() {
	app := forgo.NewApp(
		forgo.WithName("users-api"),
		forgo.WithProviders(NewUserService),
		forgo.WithServers(http.Config{
			Addr: ":8080",
			Routes: []http.RouteConfig{
				{
					Name:    "getUser",
					Method:  "GET",
					Path:    "/users/{id}",
					Version: "v1",
					Handler: http.HandlerFor((*UserService).Get),
				},
			},
		}),
	)

	// Run serves GET /v1/users/{id} until SIGINT or SIGTERM. It logs every failure and returns
	// them, so main picks the exit code.
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}
