package http

import (
	"context"
	"errors"
	"math"
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
		{"not found", "GET", "/missing", "", "route_not_found"},
		{"panic", "GET", "/panicking", "", "internal"},
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

// failingWriter is a ResponseWriter whose body writes fail.
type failingWriter struct{ *httptest.ResponseRecorder }

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestDefaultSerializer(t *testing.T) {
	t.Run("error without an operation.Error", func(t *testing.T) {
		w := httptest.NewRecorder()
		if err := newDefaultSerializer("id", w).Error(errors.New("boom")); err != nil {
			t.Fatal(err)
		}
		if w.Code != 500 || w.Body.String() != `{"reqId":"id","status":500,"errCode":"internal","errMsg":"an error occurred"}` {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
	})
	t.Run("result that cannot be encoded", func(t *testing.T) {
		w := httptest.NewRecorder()
		err := newDefaultSerializer("id", w).Success(math.NaN())
		if err == nil || !strings.HasPrefix(err.Error(), "http: encode response: ") {
			t.Fatalf("Success: %v", err)
		}
		if w.Code != 500 || w.Header().Get(contentTypeHeader) != contentTypeJSON || !strings.Contains(w.Body.String(), `"errCode":"internal"`) {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
	})
	t.Run("called twice", func(t *testing.T) {
		w := httptest.NewRecorder()
		s := newDefaultSerializer("id", w)
		if err := s.Success(nil); err != nil {
			t.Fatal(err)
		}
		if err := s.Error(errDeclined); !errors.Is(err, ErrAlreadyWritten) {
			t.Fatalf("second call: %v", err)
		}
		if w.Code != 200 || strings.Contains(w.Body.String(), "errCode") {
			t.Fatalf("second call wrote: %d %s", w.Code, w.Body)
		}
	})
	t.Run("write failure", func(t *testing.T) {
		err := newDefaultSerializer("id", failingWriter{httptest.NewRecorder()}).Error(errDeclined.Wrap(errors.New("issuer said no")))
		if !errors.Is(err, errDeclined) || err.Error() != "card declined: issuer said no; http: write response: broken pipe" {
			t.Fatalf("Error: %v", err)
		}
		if err := newDefaultSerializer("id", failingWriter{httptest.NewRecorder()}).Success(nil); err == nil {
			t.Fatal("Success hid the write failure")
		}
	})
}

// TestUnencodableResult checks that the client gets the envelope of a server error, and the log the reason.
func TestUnencodableResult(t *testing.T) {
	nan := HandlerAs[struct{}, float64](func() Handler[struct{}, float64] {
		return func(context.Context, struct{}) (float64, error) { return math.NaN(), nil }
	})
	s := newLoggedServer(t, Config{Routes: []RouteConfig{{Name: "nan", Method: "GET", Path: "/nan", Handler: nan}}})
	if w := s.serve(httptest.NewRequest("GET", "/nan", nil)); w.Code != 500 || !strings.Contains(w.Body.String(), `"errCode":"internal"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if logged, _ := s.logLines(t)["operation failed"]["error"].(string); !strings.Contains(logged, "unsupported value: NaN") {
		t.Fatalf("logged %q", logged)
	}
}

func TestStatus(t *testing.T) {
	for st, want := range map[operation.Status]int{
		operation.StatusUnknown:            500,
		operation.StatusInvalidArgument:    400,
		operation.StatusUnauthenticated:    401,
		operation.StatusPermissionDenied:   403,
		operation.StatusNotFound:           404,
		operation.StatusConflict:           409,
		operation.StatusAborted:            409,
		operation.StatusPreconditionFailed: 412,
		operation.StatusTooLarge:           413,
		operation.StatusRateLimited:        429,
		operation.StatusCancelled:          408,
		operation.StatusTimeout:            408,
		operation.StatusUnavailable:        503,
		operation.StatusInternal:           500,
		operation.StatusUnimplemented:      501,
		operation.Status(200):              500,
	} {
		if got := status(st); got != want {
			t.Errorf("status(%d) = %d, want %d", st, got, want)
		}
	}
}
