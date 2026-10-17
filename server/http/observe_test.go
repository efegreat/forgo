package http

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/observertest"
	"github.com/efegreat/forgo/operation"
)

// observed is what an observer reads of an HTTP operation's State when End is called.
type observed struct {
	status  int
	written bool
	pattern string
}

func newHTTPObserver() *observertest.Observer {
	return &observertest.Observer{State: func(st any) any {
		s := st.(*State)
		return observed{status: s.Writer.StatusCode(), written: s.Writer.Written(), pattern: s.Request.Pattern}
	}}
}

// TestOperationObserver checks that the observer sees every operation once, with the response the client got
// already written, and that the status of the observed error is the one the client got.
func TestOperationObserver(t *testing.T) {
	errSlow := errors.New("slow")
	cfg := Config{
		Routes: append(testRoutes(),
			panicRoute("panicking", func() { panic("boom") }),
			failingRoute("/failing", errors.New("boom")),
			failingRoute("/slow", errSlow),
			RouteConfig{Name: "quiet", Method: "GET", Path: "/quiet", Handler: EmptyHandler(),
				Args: map[any]any{ArgKeyRecorderDisabled: true}},
		),
		// The observed error is the mapped one.
		ErrorMapper: operation.NewErrorMapper(MapBody, MapBinding, MapValidation, func(err error) (operation.Error, bool) {
			if errors.Is(err, errSlow) {
				return operation.Error{Code: "slow", Status: operation.StatusTimeout, Message: "slow"}, true
			}
			return operation.Error{}, false
		}),
	}
	obs := newHTTPObserver()
	s := newTestServer(t, cfg, obs.Provider())

	for i, tc := range []struct {
		method, path, body string
		name, pattern      string
		wantStatus         int
		wantCode           string
	}{
		{"GET", "/health", "", "health", "GET /health", 200, ""},
		{"POST", "/v1/items", `{"name":"a"}`, "items", "POST /v1/items", 200, ""},
		{"POST", "/v1/items", `{}`, "items", "POST /v1/items", 400, "validation"},
		{"GET", "/v1/denied", "", "denied", "GET /v1/denied", 403, "denied"},
		{"GET", "/panicking", "", "panicking", "GET /panicking", 500, "x"},
		{"GET", "/failing", "", "failing", "GET /failing", 500, "x"},
		{"GET", "/slow", "", "slow", "GET /slow", 408, "slow"},
		{"GET", "/quiet", "", "quiet", "GET /quiet", 200, ""},
		{"GET", "/nowhere", "", "not_found", "/", 404, errNotFound.Code},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w, _ := serve(t, s, tc.method, tc.path, tc.body)
			if w.Code != tc.wantStatus {
				t.Fatalf("answered %d, want %d", w.Code, tc.wantStatus)
			}
			e := obs.Last(t, i+1)
			if e.Type != OperationTypeHTTPHandler || e.Name != tc.name {
				t.Fatalf("observed %s %q, want %q", e.Type, e.Name, tc.name)
			}
			if got := e.State.(observed); got != (observed{status: w.Code, written: true, pattern: tc.pattern}) {
				t.Fatalf("observed %+v, want status %d and pattern %q", got, w.Code, tc.pattern)
			}
			if e.Error.Code != tc.wantCode {
				t.Fatalf("observed error code %q, want %q", e.Error.Code, tc.wantCode)
			}
			// The labels taken from the error are those of the response.
			if e.Err != nil && status(e.Error.Status) != w.Code {
				t.Fatalf("observed status %s, which is %d, but the client got %d", e.Error.Status, status(e.Error.Status), w.Code)
			}
		})
	}
	if obs.Starts() != len(obs.Ends()) {
		t.Fatalf("%d starts for %d ends", obs.Starts(), len(obs.Ends()))
	}
}

// TestOperationObserverHijacked checks that an operation that hijacked its connection is observed as written,
// with no status.
func TestOperationObserverHijacked(t *testing.T) {
	obs := newHTTPObserver()
	s := newTestServer(t, Config{Routes: hijackRoutes(func(*State, net.Conn) {})}, obs.Provider())
	ts := httptest.NewServer(s.s.Handler)
	defer ts.Close()

	conn := dialHijack(t, ts.Listener.Addr().String())
	_, _ = io.Copy(io.Discard, conn) // until the handler closes it

	e := obs.Last(t, 1)
	if e.Name != "hijack" || e.Err != nil {
		t.Fatalf("observed %q with %v", e.Name, e.Err)
	}
	if got := e.State.(observed); got.status != 0 || !got.written {
		t.Fatalf("observed %+v, want status 0 and written", got)
	}
}

func TestOperationObserverWithoutBaseMiddlewares(t *testing.T) {
	obs := newHTTPObserver()
	cfg := Config{Routes: testRoutes()}
	cfg.DisableBaseMiddlewares = true
	s := newTestServer(t, cfg, obs.Provider())
	serve(t, s, "GET", "/health", "")
	if obs.Starts() != 0 || len(obs.Ends()) != 0 {
		t.Fatal("observed an operation without base middlewares")
	}
}

func TestOperationObserverAmbiguous(t *testing.T) {
	a, b := &observertest.Observer{}, &observertest.Observer{}
	err := newServerErr(t, Config{Routes: testRoutes()}, a.Provider(), func() *observertest.Observer { return b })
	if !errors.Is(err, forgo.ErrAmbiguousDependency) {
		t.Fatalf("New = %v, want ErrAmbiguousDependency", err)
	}
}

// TestOperationObserverConcurrent checks under the race detector that the observer is told about concurrent
// requests.
func TestOperationObserverConcurrent(t *testing.T) {
	obs := newHTTPObserver()
	s := newTestServer(t, Config{Routes: testRoutes()}, obs.Provider())
	const n = 20
	done := make(chan struct{})
	for range n {
		go func() {
			defer func() { done <- struct{}{} }()
			w := httptest.NewRecorder()
			s.s.Handler.ServeHTTP(w, httptest.NewRequest("POST", "/v1/items", strings.NewReader(`{"name":"a"}`)))
		}()
	}
	for range n {
		<-done
	}
	for _, e := range obs.Wait(t, n) {
		if !e.Started || e.State.(observed).status != http.StatusOK {
			t.Fatalf("observed %+v", e)
		}
	}
}

// TestOperationObserverAllocations checks that an observer adds no allocation to a request, answered or failed.
func TestOperationObserverAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	allocs := func(path string, providers ...any) float64 {
		h := newTestServer(t, Config{Routes: testRoutes()}, providers...).s.Handler
		r := httptest.NewRequestWithContext(benchCtx(), "GET", path, nil)
		return testing.AllocsPerRun(100, func() { h.ServeHTTP(httptest.NewRecorder(), r) })
	}
	for _, path := range []string{"/health", "/v1/denied"} {
		without := allocs(path)
		if with := allocs(path, func() operation.IObserver { return nopObserver{} }); with != without {
			t.Errorf("GET %s: %v allocations with an observer, %v without", path, with, without)
		}
	}
}

type nopObserver struct{}

func (nopObserver) Start(*operation.Context)                      {}
func (nopObserver) End(*operation.Context, operation.Observation) {}
