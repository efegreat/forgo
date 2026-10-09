package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"
)

func TestLockerWithoutLocker(t *testing.T) {
	ran := false
	next := func(*operation.Context) error { ran = true; return nil }
	ctx := &operation.Context{Context: context.Background(), Args: types.NewMap[any, any]()}

	disabled := Locker(nil, func(*operation.Context) operation.Lock { return operation.Lock{Key: "k"} }, nil)(next)
	if err := disabled(ctx); err != nil || !ran {
		t.Fatalf("disabled lock: err %v, ran %v", err, ran)
	}

	ran = false
	enabled := Locker(nil, func(*operation.Context) operation.Lock { return operation.Lock{Key: "k", Ttl: time.Second} }, nil)(next)
	if err := enabled(ctx); !errors.Is(err, ErrNoLocker) || ran {
		t.Fatalf("enabled lock: err %v, ran %v", err, ran)
	}
}
