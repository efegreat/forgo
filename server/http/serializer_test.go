package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"
)

// writeOnlySerializer returns write failures only, so its Error returns nil once the response is written.
type writeOnlySerializer struct{ w http.ResponseWriter }

func (s writeOnlySerializer) Success(any) error {
	s.w.WriteHeader(http.StatusOK)
	return nil
}

func (s writeOnlySerializer) Error(err error) error {
	s.w.WriteHeader(http.StatusTeapot)
	_, werr := s.w.Write([]byte(err.Error()))
	return werr
}

// TestSerializerErrorReturningNil checks that a failed operation is logged as failed, with the code of the
// error it answered, when the serializer's Error returns nil.
func TestSerializerErrorReturningNil(t *testing.T) {
	routes := append(testRoutes(),
		RouteConfig{Name: "failing", Method: "GET", Path: "/failing", Handler: HandlerAs[struct{}, struct{}](
			func() Handler[struct{}, struct{}] {
				return func(context.Context, struct{}) (struct{}, error) {
					return struct{}{}, operation.Error{Code: "boom", Status: operation.StatusInvalidArgument, Message: "boom"}
				}
			})},
		panicRoute("panicking", func() { panic("boom") }),
	)
	for _, tc := range []struct {
		name, method, path, body, code string
	}{
		{"handler error", "GET", "/failing", "", "boom"},
		{"binding error", "POST", "/v1/items", "{}", "validation"},
		{"middleware rejection", "GET", "/v1/denied", "", "denied"},
		{"not found", "GET", "/missing", "", "nf"},
		{"panic", "GET", "/panicking", "", "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLoggedServer(t, Config{
				Routes:     routes,
				Serializer: func(_ string, w http.ResponseWriter) transport.ISerializer { return writeOnlySerializer{w: w} },
			})
			w := s.serve(httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			if w.Code != http.StatusTeapot {
				t.Fatalf("status %d, want the serializer's error response", w.Code)
			}

			lines := s.logLines(t)
			if _, ok := lines["operation completed"]; ok {
				t.Fatalf("failed operation logged as completed:\n%s", &s.logs)
			}
			failed, ok := lines["operation failed"]
			if !ok || failed[logger.ErrorCodeLogKey] != tc.code {
				t.Fatalf("want an operation failed line with code %q:\n%s", tc.code, &s.logs)
			}
		})
	}
}
