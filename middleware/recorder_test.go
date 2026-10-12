package middleware

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

// TestRecorderSkipsDisabledEvents checks that the field functions run only for events that are
// logged, since they can be costly.
func TestRecorderSkipsDisabledEvents(t *testing.T) {
	light := operation.Error{Code: "light", Severity: operation.SeverityLight}
	critical := errors.New("critical")

	for _, tc := range []struct {
		name       string
		level      zerolog.Level
		err        error
		wantBefore bool
		wantAfter  bool
	}{
		{"info, success", zerolog.InfoLevel, nil, true, true},
		{"warn, success", zerolog.WarnLevel, nil, false, false},
		{"warn, light error", zerolog.WarnLevel, light, false, true},
		{"error, light error", zerolog.ErrorLevel, light, false, false},
		{"error, critical error", zerolog.ErrorLevel, critical, false, true},
		{"disabled, critical error", zerolog.Disabled, critical, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			ctx := &operation.Context{
				Context: logger.NewCtx(context.Background(), zerolog.New(&out).Level(tc.level)),
				Args:    types.NewMap[any, any](),
			}

			var before, after bool
			handler := Recorder(nil,
				func(_ *operation.Context, e *zerolog.Event) { before = true; e.Bool("before", true) },
				func(_ *operation.Context, e *zerolog.Event) { after = true; e.Bool("after", true) },
			)(func(*operation.Context) error { return tc.err })

			if err := handler(ctx); !errors.Is(err, tc.err) {
				t.Fatalf("err %v, want %v", err, tc.err)
			}
			if before != tc.wantBefore || after != tc.wantAfter {
				t.Fatalf("before ran %v, after ran %v, want %v and %v", before, after, tc.wantBefore, tc.wantAfter)
			}
			if lines := bytes.Count(out.Bytes(), []byte("\n")); lines != btoi(tc.wantBefore)+btoi(tc.wantAfter) {
				t.Fatalf("logged %d lines, want one per event with fields: %s", lines, out.Bytes())
			}
		})
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
