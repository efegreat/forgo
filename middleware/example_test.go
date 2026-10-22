package middleware_test

import (
	"context"
	"fmt"

	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
)

// roleKey holds the caller's role, which an authentication middleware outside the Guard stores.
type roleKey struct{}

func ExampleGuard() {
	// Only admins run the operation. Others fail with middleware.ErrRejected, which HTTP answers 403.
	adminsOnly := middleware.Guard(func(ctx *operation.Context) (bool, error) {
		role, _ := ctx.Value(roleKey{}).(string)
		return role == "admin", nil
	}, nil)

	handler := operation.Chain(func(ctx *operation.Context) error {
		fmt.Println("running", ctx.Name)
		return nil
	}, adminsOnly)

	for _, role := range []string{"admin", "viewer"} {
		ctx := context.WithValue(context.Background(), roleKey{}, role)
		err := handler(&operation.Context{Context: ctx, Name: "deleteUser"})
		fmt.Println(role, err)
	}
	// Output:
	// running deleteUser
	// admin <nil>
	// viewer operation not allowed
}
