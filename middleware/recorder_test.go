package middleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/safemap"

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
				Args:    safemap.NewMap[any, any](),
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

func TestRecorder(t *testing.T) {
	cause := errors.New("card declined")
	payment := operation.Error{Code: "pay", Message: "payment failed", Severity: operation.SeverityModerate}.Wrap(cause)

	for _, tc := range []struct {
		name      string
		err       error
		wantLevel string
		wantCode  string
		wantSev   float64
		wantErr   string
	}{
		{"success", nil, "info", "", 0, ""},
		{"plain error", errors.New("db down"), "error", "internal", 2, "db down"},
		{"light error", operation.Error{Code: "user_not_found", Severity: operation.SeverityLight, Message: "user not found"}.Wrap(cause), "warn", "user_not_found", 0, "user not found: card declined"},
		{"error with cause", payment, "error", "pay", 1, "payment failed: card declined"},
		{"error without cause", operation.Error{Code: "user_not_found", Message: "user not found"}, "warn", "user_not_found", 0, "user not found"},
		{"wrapped error", fmt.Errorf("charge order 42: %w", payment), "error", "pay", 1, "charge order 42: payment failed: card declined"},
		{"nested errors", operation.Error{Code: "order", Message: "order failed", Severity: operation.SeverityCritical}.Wrap(fmt.Errorf("charge: %w", payment)), "error", "order", 2, "order failed: charge: payment failed: card declined"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := Recorder(nil, nil, nil)(func(*operation.Context) error { return tc.err })(newTestCtx(&out))
			if err != tc.err {
				t.Fatalf("err %v, want %v returned unchanged", err, tc.err)
			}

			lines := logLines(t, &out)
			if len(lines) != 2 {
				t.Fatalf("logged %d lines, want 2: %s", len(lines), out.Bytes())
			}
			if lines[0]["message"] != "operation started" || lines[0]["level"] != "info" {
				t.Fatalf("first line %v, want operation started at info", lines[0])
			}
			end := lines[1]
			if _, ok := end["elapsed"].(float64); !ok {
				t.Fatalf("end line %v has no elapsed", end)
			}
			if end["level"] != tc.wantLevel {
				t.Fatalf("level %v, want %s", end["level"], tc.wantLevel)
			}
			if tc.err == nil {
				if end["message"] != "operation completed" || end["error"] != nil || end[logger.ErrorCodeLogKey] != nil {
					t.Fatalf("end line %v, want operation completed without error fields", end)
				}
				return
			}
			if end["message"] != "operation failed" || end[logger.ErrorCodeLogKey] != tc.wantCode || end[logger.ErrorSeverityLogKey] != tc.wantSev || end["error"] != tc.wantErr {
				t.Fatalf("end line %v, want operation failed with code %q, severity %v, error %q", end, tc.wantCode, tc.wantSev, tc.wantErr)
			}
		})
	}
}

func TestRecorderFields(t *testing.T) {
	var out bytes.Buffer
	handler := Recorder(nil,
		func(ctx *operation.Context, e *zerolog.Event) { e.Str("before", ctx.Name) },
		func(ctx *operation.Context, e *zerolog.Event) { e.Str("after", ctx.Name) },
	)(func(*operation.Context) error { return nil })
	if err := handler(newTestCtx(&out)); err != nil {
		t.Fatal(err)
	}

	lines := logLines(t, &out)
	if lines[0]["before"] != "op" || lines[0]["after"] != nil || lines[1]["after"] != "op" || lines[1]["before"] != nil {
		t.Fatalf("before and after fields on the wrong lines: %v", lines)
	}
}

func TestRecorderDisabled(t *testing.T) {
	var out bytes.Buffer
	runs := 0
	handler := Recorder(func(ctx *operation.Context) bool { return ctx.Name == "op" }, nil, nil)(ok(&runs))
	if err := handler(newTestCtx(&out)); err != nil || runs != 1 {
		t.Fatalf("err %v, runs %d, want the operation run once", err, runs)
	}
	if out.Len() != 0 {
		t.Fatalf("logged %s, want nothing for a disabled operation", out.Bytes())
	}
}

// TestRecorderLogsPanicStack checks the base chains of cron and the CLI, which recover with a nil
// hook: a panic whose value is an operation.Error keeps its code, and its stack is logged.
func TestRecorderLogsPanicStack(t *testing.T) {
	var out bytes.Buffer
	notFound := operation.Error{Code: "user_not_found", Message: "user not found"}
	handler := operation.Chain(func(*operation.Context) error { panic(notFound) }, Recorder(nil, nil, nil), Recovery(nil))
	if err := handler(newTestCtx(&out)); !errors.Is(err, notFound) {
		t.Fatalf("err %v, want the panicked error", err)
	}

	end := logLines(t, &out)[1]
	logged, _ := end["error"].(string)
	if end[logger.ErrorCodeLogKey] != "user_not_found" || !strings.HasPrefix(logged, "panic: user not found") || !strings.Contains(logged, "TestRecorderLogsPanicStack") {
		t.Fatalf("end line %v, want code user_not_found and the panic with its stack", end)
	}
}
