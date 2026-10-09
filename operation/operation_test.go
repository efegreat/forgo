package operation

import (
	"context"
	"testing"

	"github.com/efegreat/forgo/types"
)

type (
	benchArgKey    struct{}
	benchParentKey struct{}
)

var sinkValue any

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
			ctx := &Context{Context: parent, Args: types.NewMapWithData(c.args)}
			b.ReportAllocs()
			for b.Loop() {
				sinkValue = ctx.Value(benchParentKey{})
			}
		})
	}
}
