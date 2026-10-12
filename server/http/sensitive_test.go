package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/redact"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

func TestMaskQuery(t *testing.T) {
	fields := redact.NewFields("token", "password")
	for _, tc := range []struct {
		query, want string // want is empty when nothing is masked
	}{
		{"", ""},
		{"page=1&sort=asc", ""},
		{"token=abc", "token=[*]"},
		{"page=1&token=abc&sort=asc", "page=1&token=[*]&sort=asc"},
		{"TOKEN=abc", "TOKEN=[*]"},
		{"pass%77ord=abc", "pass%77ord=[*]"}, // names are unescaped before matching
		{"to+ken=abc", ""},                   // "+" is a space
		{"token", ""},                        // no value
		{"token=", "token=[*]"},
		{"token=a&token=b", "token=[*]&token=[*]"},
		{"token=a=b&x=1", "token=[*]&x=1"},
		{"page=1;token=abc", "page=1;token=[*]"}, // legacy separator
		{"&&token=abc&", "&&token=[*]&"},
		{"%zz=1&token=abc", "%zz=1&token=[*]"},
	} {
		got, masked := maskQuery(nil, tc.query, fields)
		if masked != (tc.want != "") || string(got) != tc.want {
			t.Errorf("maskQuery(%q) = %q, %v; want %q", tc.query, got, masked, tc.want)
		}
	}
}

func TestMaskPath(t *testing.T) {
	fields := redact.NewFields("token", "id")
	for _, tc := range []struct {
		pattern, target, want string // want is empty when nothing is masked
	}{
		{"GET /users/{id}", "/users/42", "/users/[*]"},
		{"GET /users/{ID}", "/users/42", "/users/[*]"},
		{"GET /users/{name}", "/users/ada", ""},
		{"GET /health", "/health", ""},
		{"", "/users/42", ""},
		{"/reset/{token}/confirm", "/reset/ab%2Fcd/confirm", "/reset/[*]/confirm"}, // an escaped slash stays in its segment
		{"/files/{token...}", "/files/a/b/c", "/files/[*]"},
		{"/t/{token}/{rest...}", "/t/x/a%20b/c", "/t/[*]/a b/c"},
		{"/u/{name}/t/{token}", "/u/caf%C3%A9/t/x", "/u/café/t/[*]"},
		{"example.com/t/{token}", "/t/x", "/t/[*]"},
		{"/t/{token}/{$}", "/t/x/", "/t/[*]/"},
		{"/t/{token}/", "/t/x/more/here", "/t/[*]/more/here"},
	} {
		r := httptest.NewRequest("GET", tc.target, nil)
		r.Pattern = tc.pattern
		got, masked := maskPath(nil, r, fields)
		if masked != (tc.want != "") || string(got) != tc.want {
			t.Errorf("maskPath(%q, %q) = %q, %v; want %q", tc.pattern, tc.target, got, masked, tc.want)
		}
	}
}

type signupReq struct {
	Token    string `uri:"token"`
	Name     string `json:"name"`
	Password string `json:"password"`
}

type signupResp struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

func newSignupHandler() Handler[signupReq, signupResp] {
	return func(_ context.Context, req signupReq) (signupResp, error) {
		return signupResp{Name: req.Name, Password: req.Password}, nil
	}
}

func newSessionMiddleware() operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			ctx.State.(*State).Writer.Header().Set("X-Session", "h-secret")
			return next(ctx)
		}
	}
}

// loggedServer serves requests on a context whose logger writes to logs, as BaseContext does for a
// running server.
type loggedServer struct {
	*Server
	ctx  context.Context
	logs bytes.Buffer
}

// newLoggedServer builds and initializes a server like newTestServer does.
func newLoggedServer(t *testing.T, cfg Config) *loggedServer {
	t.Helper()
	boot := forgo.NewBootstrap(nil, nil)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })

	s, err := cfg.New(r)
	if err != nil {
		t.Fatal(err)
	}
	ls := &loggedServer{Server: s.(*Server)}
	ls.ctx = logger.NewCtx(context.Background(), zerolog.New(&ls.logs))
	if err := s.Init(ls.ctx); err != nil {
		t.Fatal(err)
	}
	return ls
}

func (s *loggedServer) serve(r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.s.Handler.ServeHTTP(w, r.WithContext(s.ctx))
	return w
}

// logLines decodes the operation log lines, keyed by message.
func (s *loggedServer) logLines(t *testing.T) map[string]map[string]any {
	t.Helper()
	lines := map[string]map[string]any{}
	for line := range bytes.Lines(s.logs.Bytes()) {
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			t.Fatalf("invalid log line %s: %v", line, err)
		}
		lines[fields["message"].(string)] = fields
	}
	return lines
}

func TestSensitiveFieldsAreMaskedInLogs(t *testing.T) {
	s := newLoggedServer(t, Config{
		SensitiveFields: []string{"token", "password", "X-Session"},
		Routes: []RouteConfig{{
			Name: "signup", Method: "POST", Path: "/users/{token}", Version: "v1",
			Handler:     HandlerAs[signupReq, signupResp](newSignupHandler),
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(newSessionMiddleware)},
		}},
	})

	// Indented, as clients send it: logLines fails unless the body is logged on one line.
	body := "{\n  \"name\": \"ada\",\n  \"password\": \"b-secret\",\n  \"profile\": {\"TOKEN\": \"n-secret\"}\n}\n"
	r := httptest.NewRequest("POST", "/v1/users/u-secret%2Fx?Token=q-secret&page=2&pass%77ord=p-secret", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer a-secret")
	r.Header.Set("X-Session", "h-secret")
	r.Header.Set("Accept", "application/json")
	w := s.serve(r)

	if w.Code != 200 || !strings.Contains(w.Body.String(), "b-secret") {
		t.Fatalf("masking changed the response: %d %s", w.Code, w.Body)
	}
	for _, secret := range []string{"u-secret", "q-secret", "p-secret", "a-secret", "h-secret", "b-secret", "n-secret"} {
		if strings.Contains(s.logs.String(), secret) {
			t.Errorf("logs leak %s:\n%s", secret, &s.logs)
		}
	}

	lines := s.logLines(t)
	started, completed := lines["operation started"], lines["operation completed"]
	for _, tc := range []struct {
		name      string
		got, want any
	}{
		{"path", started["path"], "/v1/users/[*]"},
		{"query", started["query"], "Token=[*]&page=2&pass%77ord=[*]"},
		{"authorization", started["req_headers"].(map[string]any)["Authorization"], []any{"[*]"}},
		{"request header", started["req_headers"].(map[string]any)["X-Session"], []any{"[*]"}},
		{"accept", started["req_headers"].(map[string]any)["Accept"], []any{"application/json"}},
		{"request body", started["req_body"], map[string]any{"name": "ada", "password": "[*]", "profile": map[string]any{"TOKEN": "[*]"}}},
		{"response header", completed["resp_headers"].(map[string]any)["X-Session"], []any{"[*]"}},
		{"response body", completed["resp_body"].(map[string]any)["result"], map[string]any{"name": "ada", "password": "[*]"}},
	} {
		if !jsonEqual(tc.got, tc.want) {
			t.Errorf("%s logged as %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}

func TestLogsWithoutSensitiveFields(t *testing.T) {
	s := newLoggedServer(t, Config{Routes: []RouteConfig{{
		Name: "signup", Method: "POST", Path: "/users/{token}",
		Handler: HandlerAs[signupReq, signupResp](newSignupHandler),
	}}})

	r := httptest.NewRequest("POST", "/users/u-1?token=q-1", strings.NewReader("{\n  \"password\": \"b-1\"\n}\n"))
	r.Header.Set("Authorization", "Bearer a-secret")
	s.serve(r)

	started := s.logLines(t)["operation started"]
	if started["path"] != "/users/u-1" || started["query"] != "token=q-1" ||
		!jsonEqual(started["req_body"], map[string]any{"password": "b-1"}) {
		t.Errorf("logged %v, want everything as sent", started)
	}
	if got := started["req_headers"].(map[string]any)["Authorization"]; !jsonEqual(got, []any{"[*]"}) {
		t.Errorf("Authorization logged as %v, credential headers are always masked", got)
	}
}

type bindReq struct {
	Page   int  `query:"page"`
	Pin    int  `query:"pin"`
	Key    int  `header:"X-Api-Key"`
	Amount int8 `json:"amount"`
	Secret int8 `json:"secret"`
}

func newBindHandler() Handler[bindReq, itemResp] {
	return func(context.Context, bindReq) (itemResp, error) { return itemResp{}, nil }
}

// TestBindingErrorsAreMasked checks that the cause the Recorder logs for a binding error never
// quotes the input of a sensitive field, and that the response is the same as without masking.
func TestBindingErrorsAreMasked(t *testing.T) {
	cfg := Config{
		SensitiveFields: []string{"pin", "secret"},
		Routes: []RouteConfig{{
			Name: "bind", Method: "POST", Path: "/bind", Handler: HandlerAs[bindReq, itemResp](newBindHandler),
		}},
	}
	for _, tc := range []struct {
		name, query, header, body string
		code, logged              string
	}{
		{"sensitive query", "pin=12ab", "", "", "binding", `strconv.ParseInt: parsing "[*]": invalid syntax`},
		{"query", "page=12ab", "", "", "binding", `strconv.ParseInt: parsing "12ab": invalid syntax`},
		{"credential header", "", "k3y", "", "binding", `strconv.ParseInt: parsing "[*]": invalid syntax`},
		{"sensitive body field", "", "", `{"secret":1000}`, "json_type", "json: cannot unmarshal number into Go struct field bindReq.secret of type int8"},
		{"body field", "", "", `{"amount":1000}`, "json_type", "json: cannot unmarshal number 1000 into Go struct field bindReq.amount of type int8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLoggedServer(t, cfg)
			r := httptest.NewRequest("POST", "/bind?"+tc.query, strings.NewReader(tc.body))
			if tc.header != "" {
				r.Header.Set("X-Api-Key", tc.header)
			}
			w := s.serve(r)

			var envelope map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil || w.Code != 400 || envelope["errCode"] != tc.code {
				t.Fatalf("response %d %s, want 400 with errCode %s", w.Code, w.Body, tc.code)
			}
			if got := s.logLines(t)["operation failed"]["error"]; got != tc.logged {
				t.Fatalf("logged error %q, want %q", got, tc.logged)
			}
		})
	}
}

func jsonEqual(got, want any) bool {
	g, _ := json.Marshal(got)
	w, _ := json.Marshal(want)
	return bytes.Equal(g, w)
}

// BenchmarkRequestLogFieldsSensitive measures the request log fields with sensitive fields to mask
// in every part of the request, against none.
func BenchmarkRequestLogFieldsSensitive(b *testing.B) {
	body := []byte(`{"name":"widget","password":"hunter2","owner":{"email":"ada@example.com","token":"t"}}`)
	r := httptest.NewRequest("POST", "/v1/items/42?page=1&token=abc", bytes.NewReader(body))
	r.Pattern = "POST /v1/items/{id}"
	r.Header = benchHeaders()
	opCtx := &operation.Context{Context: context.Background(), State: &State{Request: r}}
	lg := zerolog.New(io.Discard)

	for _, bc := range []struct {
		name      string
		sensitive []string
	}{
		{"none", nil},
		{"masked", []string{"password", "token", "id"}},
	} {
		fields := RequestLogFields(RequestRecorderBodyLimit, RequestRecorderHeaderLimit, bc.sensitive)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				r.Body = io.NopCloser(bytes.NewReader(body))
				e := lg.Info()
				fields(opCtx, e)
				e.Msg("operation started")
			}
		})
	}
}
