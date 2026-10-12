package middleware

import (
	"bytes"
	"testing"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

func TestLogger(t *testing.T) {
	for _, tc := range []struct {
		name  string
		adder logger.FieldAdder
		want  map[string]any
	}{
		{"nil adder", nil, map[string]any{"op_type": "test", "op_name": "op", "op_id": "id-1"}},
		{
			"adder",
			func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context {
				return zCtx.Str("tenant", "acme").Str("name_again", ctx.Name)
			},
			map[string]any{"op_type": "test", "op_name": "op", "op_id": "id-1", "tenant": "acme", "name_again": "op"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			ctx := newTestCtx(&out)
			ctx.ID = "id-1"
			base := ctx.Context

			err := Logger(tc.adder)(func(ctx *operation.Context) error {
				logger.FromCtx(ctx).Info().Msg("inside")
				return nil
			})(ctx)
			if err != nil {
				t.Fatal(err)
			}

			lines := logLines(t, &out)
			if len(lines) != 1 {
				t.Fatalf("logged %d lines, want 1: %s", len(lines), out.Bytes())
			}
			for k, v := range tc.want {
				if lines[0][k] != v {
					t.Errorf("%s = %v, want %v", k, lines[0][k], v)
				}
			}
			if ctx.Context == base {
				t.Error("ctx.Context not replaced, later middlewares would not see the derived logger")
			}
		})
	}
}

// TestLoggerKeepsBase checks that the derived logger does not change the logger it derives from,
// which the context shares with every other operation.
func TestLoggerKeepsBase(t *testing.T) {
	var out bytes.Buffer
	ctx := newTestCtx(&out)
	base := logger.FromCtx(ctx)

	_ = Logger(nil)(func(*operation.Context) error { return nil })(ctx)
	base.Info().Msg("base")

	if line := logLines(t, &out)[0]; line["op_name"] != nil {
		t.Fatalf("base logger gained operation fields: %v", line)
	}
}
