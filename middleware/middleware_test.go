package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/safemap"

	"github.com/rs/zerolog"
)

// newTestCtx returns an operation context whose logger writes JSON lines to out.
func newTestCtx(out *bytes.Buffer) *operation.Context {
	return &operation.Context{
		Context: logger.NewCtx(context.Background(), zerolog.New(out)),
		Name:    "op",
		Type:    "test",
		Args:    safemap.NewMap[any, any](),
	}
}

// logLines decodes the JSON lines in out.
func logLines(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for line := range bytes.Lines(out.Bytes()) {
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		lines = append(lines, m)
	}
	return lines
}

// ok is a terminal handler that counts its runs.
func ok(runs *int) operation.Handler {
	return func(*operation.Context) error {
		*runs++
		return nil
	}
}
