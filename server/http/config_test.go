package http

import (
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
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
		{"forgo.MiddlewareAs with dependencies", forgo.MiddlewareAs(newTenantMiddleware), nil},
		{"forgo.MiddlewareAs without dependencies", forgo.MiddlewareAs(construct), nil},
		{"panicking factory", func(forgo.Resolver) (operation.Middleware, error) { panic("boom") }, nil},
		{"nil factory", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, ok := MiddlewareContractOf(tc.factory)
			if ok != (tc.want != nil) || c.Params != tc.want {
				t.Fatalf("got %v, %v; want %v", c.Params, ok, tc.want)
			}
		})
	}
	// Only the plain factory constructs its middleware; MiddlewareAs reports its contract instead.
	if constructed != 1 {
		t.Fatalf("constructor called %d times, want 1", constructed)
	}
}
