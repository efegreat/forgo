package http

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	"github.com/go-playground/validator/v10"
)

type tenantParams struct {
	TenantID string `header:"X-Tenant-ID" validate:"required,uuid"`
}

type tenantStore struct{ prefix string }

// newTenantMiddleware echoes the tenant header, read straight from the request, as X-Seen-Tenant.
func newTenantMiddleware(s *tenantStore) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			st.Writer.Header().Set("X-Seen-Tenant", s.prefix+st.Request.Header.Get("X-Tenant-ID"))
			return next(ctx)
		}
	}
}

// TestMiddlewareAsServes checks that on a server MiddlewareAs is forgo.MiddlewareAs: the constructor's
// dependencies are resolved, and Params is never bound or validated.
func TestMiddlewareAsServes(t *testing.T) {
	route := RouteConfig{Name: "health", Method: "GET", Path: "/health", Handler: EmptyHandler(),
		Middlewares: []forgo.MiddlewareFactory{MiddlewareAs[tenantParams](newTenantMiddleware)}}
	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		s := newTestServer(t, Config{Routes: []RouteConfig{route}, ServerConfig: forgo.ServerConfig{DisableBaseMiddlewares: !base}},
			func() *tenantStore { return &tenantStore{prefix: "t-"} })
		for header, want := range map[string]string{"": "t-", "not-a-uuid": "t-not-a-uuid"} {
			r := httptest.NewRequest("GET", "/health", nil)
			if header != "" {
				r.Header.Set("X-Tenant-ID", header)
			}
			w, _ := do(t, s, r)
			if w.Code != 200 || w.Header().Get("X-Seen-Tenant") != want {
				t.Fatalf("header %q: %d, X-Seen-Tenant %q", header, w.Code, w.Header().Get("X-Seen-Tenant"))
			}
		}
	})

	if err := newServerErr(t, Config{Routes: []RouteConfig{route}}); !errors.Is(err, forgo.ErrMissingDependency) {
		t.Fatalf("missing constructor dependency: %v", err)
	}
}

func TestMiddlewareContractOf(t *testing.T) {
	constructed := 0
	construct := func() operation.Middleware {
		constructed++
		return passThrough
	}
	for _, tc := range []struct {
		name    string
		factory forgo.MiddlewareFactory
		want    reflect.Type
	}{
		{"MiddlewareAs with dependencies", MiddlewareAs[tenantParams](newTenantMiddleware), reflect.TypeFor[tenantParams]()},
		{"MiddlewareAs without dependencies", MiddlewareAs[*tenantParams](construct), reflect.TypeFor[*tenantParams]()},
		{"WithContract", WithContract(MiddlewareContract{Params: reflect.TypeFor[tenantParams]()}, forgo.MiddlewareAs(construct)),
			reflect.TypeFor[tenantParams]()},
		{"WithContract over MiddlewareAs", WithContract(MiddlewareContract{Params: reflect.TypeFor[*tenantParams]()},
			MiddlewareAs[tenantParams](construct)), reflect.TypeFor[*tenantParams]()},
		{"forgo.MiddlewareAs with dependencies", forgo.MiddlewareAs(newTenantMiddleware), nil},
		{"forgo.MiddlewareAs without dependencies", forgo.MiddlewareAs(construct), nil},
		{"failing factory", func(forgo.Resolver) (operation.Middleware, error) { return nil, errors.New("boom") }, nil},
		{"nil factory", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, ok := MiddlewareContractOf(tc.factory)
			if ok != (tc.want != nil) || c.Params != tc.want {
				t.Fatalf("got %v, %v; want %v", c.Params, ok, tc.want)
			}
		})
	}
	// Only the plain factory constructs its middleware; MiddlewareAs and WithContract report their contracts
	// instead.
	if constructed != 1 {
		t.Fatalf("constructor called %d times, want 1", constructed)
	}
}

// TestWithContract checks that WithContract reports its whole contract, and that on a server its factory
// creates the middleware.
func TestWithContract(t *testing.T) {
	contract := MiddlewareContract{
		Auth:       &AuthContract{Credentials: []Credential{{Kind: CredentialHeader, Name: "X-API-Key"}}},
		Headers:    []ResponseHeader{{Name: "X-Seen-Tenant", Type: reflect.TypeFor[string]()}},
		Rejections: []Rejection{{Err: operation.Error{Code: "forbidden", Status: operation.StatusPermissionDenied}}},
	}
	factory := WithContract(contract, forgo.MiddlewareAs(newTenantMiddleware))
	if c, ok := MiddlewareContractOf(factory); !ok || !reflect.DeepEqual(c, contract) {
		t.Fatalf("contract %+v, %v", c, ok)
	}

	route := RouteConfig{Name: "health", Method: "GET", Path: "/health", Handler: EmptyHandler(),
		Middlewares: []forgo.MiddlewareFactory{factory}}
	s := newTestServer(t, Config{Routes: []RouteConfig{route}}, func() *tenantStore { return &tenantStore{prefix: "t-"} })
	if w, _ := do(t, s, httptest.NewRequest("GET", "/health", nil)); w.Code != 200 || w.Header().Get("X-Seen-Tenant") != "t-" {
		t.Fatalf("%d, X-Seen-Tenant %q", w.Code, w.Header().Get("X-Seen-Tenant"))
	}

	route.Middlewares = []forgo.MiddlewareFactory{WithContract(contract, nil)}
	if err := newServerErr(t, Config{Routes: []RouteConfig{route}}); err == nil || !strings.Contains(err.Error(), "nil factory") {
		t.Fatalf("nil factory: %v", err)
	}
}

// TestNewRejectsInvalidRoutes checks that New fails, naming the route, instead of dereferencing a missing
// handler or registering a route that never matches.
func TestNewRejectsInvalidRoutes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		routes []RouteConfig
		want   string
	}{
		{"no handler", []RouteConfig{{Name: "a", Method: "GET", Path: "/a"}},
			`http: route "a" (GET /a): no Handler`},
		{"lower-case method", []RouteConfig{{Name: "a", Method: "get", Path: "/a", Handler: EmptyHandler()}},
			`http: route "a" (get /a): method "get" never matches GET requests`},
		{"unresolvable handler", []RouteConfig{{Name: "items", Method: "GET", Path: "/items", Handler: HandlerFor((*itemService).Get)}},
			`http: route "items" (GET /items): missing dependency *http.itemService`},
		{"unresolvable middleware", []RouteConfig{{Name: "m", Method: "GET", Path: "/m", Handler: EmptyHandler(),
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(newTenantMiddleware)}}},
			`http: route "m" (GET /m): bootstrap: constructor `},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := newServerErr(t, Config{Routes: tc.routes})
			if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
				t.Fatalf("New: %v, want %q", err, tc.want)
			}
		})
	}
}

// TestNewRejectsInvalidValidators checks that New fails on a validator that validator refuses to register, which
// would otherwise fail every request that uses its tag.
func TestNewRejectsInvalidValidators(t *testing.T) {
	valid := func(validator.FieldLevel) bool { return true }
	for name, validators := range map[string]map[string]validator.Func{
		"empty name":   {"": valid},
		"nil function": {"slug": nil},
	} {
		t.Run(name, func(t *testing.T) {
			err := newServerErr(t, Config{Validators: validators, Routes: testRoutes()})
			if err == nil || !strings.HasPrefix(err.Error(), "http: validator ") {
				t.Fatalf("New: %v", err)
			}
		})
	}
}

type itemService struct{ prefix string }

func (s *itemService) Get(_ context.Context, req itemReq) (itemResp, error) {
	return itemResp{Name: s.prefix + req.Name}, nil
}

type itemGetter interface {
	Get(context.Context, itemReq) (itemResp, error)
}

// TestHandlerFor checks that HandlerFor resolves the receiver from the bootstrap, also as an interface the
// provided service implements.
func TestHandlerFor(t *testing.T) {
	for name, handler := range map[string]HandlerFactory{
		"method of a pointer":    HandlerFor((*itemService).Get),
		"method of an interface": HandlerFor(itemGetter.Get),
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t, Config{Routes: []RouteConfig{{Name: "items", Method: "POST", Path: "/items", Handler: handler}}},
				func() *itemService { return &itemService{prefix: "svc-"} })
			w, envelope := serve(t, s, "POST", "/items", `{"name":"pen"}`)
			if result, _ := envelope["result"].(map[string]any); w.Code != 200 || result["name"] != "svc-pen" {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			if c := handler.Contract(); c.Request != reflect.TypeFor[itemReq]() || c.Response != reflect.TypeFor[itemResp]() {
				t.Fatalf("contract %+v", c)
			}
		})
	}
}
