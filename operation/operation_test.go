package operation

import (
	"context"
	"testing"
	"time"

	"github.com/efegreat/forgo/safemap"
)

type (
	benchArgKey    struct{}
	benchParentKey struct{}
	testKey        string
)

var sinkValue any

func TestContextValue(t *testing.T) {
	parent := context.WithValue(context.WithValue(context.Background(), testKey("shadowed"), "parent"), testKey("parent"), "parent")

	for _, tc := range []struct {
		name string
		args *safemap.Map[any, any]
		key  any
		want any
	}{
		{"args first", safemap.NewMapWithData(map[any]any{testKey("shadowed"): "args"}), testKey("shadowed"), "args"},
		{"falls through to the parent", safemap.NewMapWithData(map[any]any{testKey("args"): "args"}), testKey("parent"), "parent"},
		{"missing", safemap.NewMap[any, any](), testKey("missing"), nil},
		{"nil args", nil, testKey("parent"), "parent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &Context{Context: parent, Args: tc.args}
			if got := ctx.Value(tc.key); got != tc.want {
				t.Fatalf("Value(%v) = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

func TestFromCtx(t *testing.T) {
	op := &Context{Context: context.Background(), Name: "outer"}
	inner := &Context{Context: context.WithValue(op, testKey("k"), "v"), Name: "inner"}
	timeout, cancel := context.WithTimeout(op, time.Minute)
	defer cancel()

	for _, tc := range []struct {
		name string
		ctx  context.Context
		want *Context
	}{
		{"operation", op, op},
		{"derived", timeout, op},
		{"without cancel", context.WithoutCancel(timeout), op},
		{"innermost", context.WithValue(inner, testKey("other"), "v"), inner},
		{"no operation", context.Background(), nil},
		{"nil", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FromCtx(tc.ctx)
			if got != tc.want || ok != (tc.want != nil) {
				t.Fatalf("FromCtx = %v, %v, want %v", got, ok, tc.want)
			}
		})
	}
}

func TestChain(t *testing.T) {
	var order []string
	mw := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx *Context) error {
				order = append(order, name)
				return next(ctx)
			}
		}
	}
	handler := func(*Context) error {
		order = append(order, "handler")
		return nil
	}

	if err := Chain(handler, mw("a"), mw("b"))(&Context{}); err != nil {
		t.Fatal(err)
	}
	if got := len(order); got != 3 || order[0] != "a" || order[1] != "b" || order[2] != "handler" {
		t.Fatalf("ran %v, want [a b handler]", order)
	}

	order = nil
	if err := Chain(handler)(&Context{}); err != nil || len(order) != 1 {
		t.Fatalf("ran %v (err %v), want only the handler", order, err)
	}
}

func TestLockEnabled(t *testing.T) {
	for _, tc := range []struct {
		lock Lock
		want bool
	}{
		{Lock{Key: "job", TTL: time.Minute}, true},
		{Lock{Key: "job"}, false},
		{Lock{Key: "job", TTL: -time.Second}, false},
		{Lock{TTL: time.Minute}, false},
		{Lock{}, false},
	} {
		if got := tc.lock.Enabled(); got != tc.want {
			t.Errorf("%+v.Enabled() = %v, want %v", tc.lock, got, tc.want)
		}
	}
}

// BenchmarkContextValue looks up a key that lives in the parent context, so every lookup passes through Args first.
func BenchmarkContextValue(b *testing.B) {
	parent := context.WithValue(context.Background(), benchParentKey{}, "logger")

	cases := []struct {
		name string
		args map[any]any
	}{
		{"args_empty", nil},
		{"args_set", map[any]any{benchArgKey{}: "tenant"}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			ctx := &Context{Context: parent, Args: safemap.NewMapWithData(c.args)}
			b.ReportAllocs()
			for b.Loop() {
				sinkValue = ctx.Value(benchParentKey{})
			}
		})
	}
}

// BenchmarkFromCtx finds the operation in the context a transport passes and in one derived from it.
func BenchmarkFromCtx(b *testing.B) {
	op := &Context{Context: context.Background(), Args: safemap.NewMap[any, any]()}
	derived, cancel := context.WithCancel(op)
	defer cancel()

	for _, c := range []struct {
		name string
		ctx  context.Context
	}{
		{"operation", op},
		{"derived", derived},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkValue, _ = FromCtx(c.ctx)
			}
		})
	}
}
