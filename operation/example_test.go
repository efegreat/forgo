package operation_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/efegreat/forgo/operation"
)

var ErrUserNotFound = operation.Error{
	Code:     "user_not_found",
	Status:   operation.StatusNotFound,
	Message:  "user %s not found",
	Severity: operation.SeverityLight,
}

func ExampleError() {
	err := fmt.Errorf("get user: %w", ErrUserNotFound.Messagef("42").Wrap(sql.ErrNoRows))

	// Transports send the Code and the Message, and map the Status to their own codes.
	var opErr operation.Error
	if errors.As(err, &opErr) {
		fmt.Println(opErr.Code, opErr.Status == operation.StatusNotFound, opErr.Message)
	}

	// errors.Is matches by Code, so the formatted and wrapped copy is still ErrUserNotFound.
	fmt.Println(errors.Is(err, ErrUserNotFound))

	// Logs keep the private cause.
	fmt.Println(err)
	// Output:
	// user_not_found true user 42 not found
	// true
	// get user: user 42 not found: sql: no rows in result set
}

func ExampleChain() {
	trace := func(name string) operation.Middleware {
		return func(next operation.Handler) operation.Handler {
			return func(ctx *operation.Context) error {
				fmt.Println("enter", name)
				defer fmt.Println("leave", name)
				return next(ctx)
			}
		}
	}

	handler := operation.Chain(func(ctx *operation.Context) error {
		fmt.Println("handle", ctx.Name)
		return nil
	}, trace("outer"), trace("inner"))

	_ = handler(&operation.Context{Context: context.Background(), Name: "getUser"})
	// Output:
	// enter outer
	// enter inner
	// handle getUser
	// leave inner
	// leave outer
}
