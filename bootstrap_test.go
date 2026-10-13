package forgo

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/efegreat/forgo/operation"
)

// closeLog records the names of closed resources, in order.
type closeLog struct {
	mu    sync.Mutex
	names []string
}

func (l *closeLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.names = append(l.names, name)
}

func (l *closeLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.names...)
}

type testResource struct {
	name     string
	value    any
	closeErr error
	panicOn  string // "Name", "Value" or "Close"
	log      *closeLog
}

func (r *testResource) Name() string {
	if r.panicOn == "Name" {
		panic("name failed")
	}
	return r.name
}

func (r *testResource) Value() any {
	if r.panicOn == "Value" {
		panic("value failed")
	}
	return r.value
}

func (r *testResource) Close() error {
	if r.panicOn == "Close" {
		panic("close failed")
	}
	if r.log != nil {
		r.log.add(r.name)
	}
	return r.closeErr
}

// resourceFunc is a ResourceFactory.
type resourceFunc func() (Resource, error)

func (f resourceFunc) New() (Resource, error) { return f() }

func resourceOf(r *testResource) ResourceFactory {
	return resourceFunc(func() (Resource, error) { return r, nil })
}

type testDB struct{ name string }

type repository interface{ Find() string }

type sqlRepository struct{ db *testDB }

func (r *sqlRepository) Find() string { return r.db.name }

type memRepository struct{}

func (memRepository) Find() string { return "memory" }

type service struct{ repo repository }

func newRepository(db *testDB) *sqlRepository { return &sqlRepository{db: db} }
func newService(r repository) *service        { return &service{repo: r} }

func build(t *testing.T, resources []ResourceFactory, providers []any) Resolver {
	t.Helper()
	b := NewBootstrap(resources, providers)
	r, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return r
}

func mustResolve[T any](t *testing.T, r Resolver) T {
	t.Helper()
	v, err := r.Resolve[T]()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	return v
}

func TestBootstrapResolvesInDeclarationOrder(t *testing.T) {
	calls := 0
	countedService := func(r repository) *service {
		calls++
		return newService(r)
	}
	db := &testDB{name: "main"}
	r := build(t,
		[]ResourceFactory{resourceOf(&testResource{name: "main", value: db})},
		[]any{newRepository, countedService},
	)

	svc := mustResolve[*service](t, r)
	if got := svc.repo.Find(); got != "main" {
		t.Fatalf("repository reads %q, want main", got)
	}
	if again := mustResolve[*service](t, r); again != svc {
		t.Fatal("providers must be singletons")
	}
	if calls != 1 {
		t.Fatalf("provider called %d times, want 1", calls)
	}
	if got := mustResolve[*testDB](t, r); got != db {
		t.Fatal("resource value not resolved by its dynamic type")
	}
}

func TestBootstrapProviderOrder(t *testing.T) {
	_, err := NewBootstrap(
		[]ResourceFactory{resourceOf(&testResource{name: "main", value: &testDB{}})},
		[]any{newService, newRepository},
	).Build()
	if !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("got %v, want ErrMissingDependency", err)
	}
	want := "bootstrap: provider 0 (github.com/efegreat/forgo.newService): argument 0 (forgo.repository): missing dependency forgo.repository"
	if err.Error() != want {
		t.Fatalf("error:\n got %s\nwant %s", err, want)
	}
}

func TestBootstrapAssignability(t *testing.T) {
	t.Run("ambiguous interface", func(t *testing.T) {
		r := build(t,
			[]ResourceFactory{resourceOf(&testResource{name: "main", value: &testDB{}})},
			[]any{newRepository, func() memRepository { return memRepository{} }},
		)
		_, err := r.Resolve[repository]()
		if !errors.Is(err, ErrAmbiguousDependency) {
			t.Fatalf("got %v, want ErrAmbiguousDependency", err)
		}
		for _, source := range []string{"provider 0 (github.com/efegreat/forgo.newRepository) (*forgo.sqlRepository)", "(forgo.memRepository)"} {
			if !strings.Contains(err.Error(), source) {
				t.Fatalf("error %q does not name %q", err, source)
			}
		}
		// concrete types stay unambiguous
		mustResolve[*sqlRepository](t, r)
		mustResolve[memRepository](t, r)
	})

	t.Run("provider binds under its declared type", func(t *testing.T) {
		r := build(t, nil, []any{func() repository { return memRepository{} }})
		mustResolve[repository](t, r)
		if _, err := r.Resolve[memRepository](); !errors.Is(err, ErrMissingDependency) {
			t.Fatalf("got %v, want the concrete type hidden", err)
		}
	})

	t.Run("resource binds under its dynamic type", func(t *testing.T) {
		r := build(t, []ResourceFactory{resourceOf(&testResource{name: "repo", value: memRepository{}})}, nil)
		mustResolve[repository](t, r)
		mustResolve[memRepository](t, r)
	})

	t.Run("unnamed result for a named type", func(t *testing.T) {
		type double func(int) int
		r := build(t, nil, []any{func() func(int) int { return func(i int) int { return 2 * i } }})
		if got := mustResolve[double](t, r)(2); got != 4 {
			t.Fatalf("got %d, want 4", got)
		}
	})
}

func TestBootstrapProviderValidation(t *testing.T) {
	var nilFunc func() *service
	tests := []struct {
		name     string
		provider any
		want     string
	}{
		{"nil", nil, "bootstrap: provider 0: nil function"},
		{"typed nil", nilFunc, "bootstrap: provider 0: nil function"},
		{"not a function", "service", "bootstrap: provider 0: string is not a function"},
		{"variadic", func(...int) *service { return nil }, "cannot be variadic"},
		{"no result", func() {}, "must return T or (T, error)"},
		{"three results", func() (int, int, error) { return 0, 0, nil }, "must return T or (T, error)"},
		{"second result not error", func() (int, int) { return 0, 0 }, "second result must be error"},
		{"provides error", func() error { return nil }, "cannot provide error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opened := false
			factory := resourceFunc(func() (Resource, error) {
				opened = true
				return &testResource{name: "main", value: &testDB{}}, nil
			})
			_, err := NewBootstrap([]ResourceFactory{factory}, []any{tt.provider}).Build()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want %q", err, tt.want)
			}
			if opened {
				t.Fatal("resources opened before providers were validated")
			}
		})
	}

	t.Run("duplicate result type", func(t *testing.T) {
		_, err := NewBootstrap(nil, []any{newService, func() *service { return nil }}).Build()
		if err == nil || !strings.Contains(err.Error(), "bootstrap: duplicate providers for *forgo.service") {
			t.Fatalf("got %v", err)
		}
	})
}

func TestBootstrapProviderFailures(t *testing.T) {
	errDial := errors.New("dial failed")
	tests := []struct {
		name     string
		provider any
		want     string
		is       error
	}{
		{"error", func() (*service, error) { return nil, errDial }, "dial failed", errDial},
		{"nil pointer", func() *service { return nil }, "returned nil *forgo.service", nil},
		{"nil interface", func() repository { return nil }, "returned nil forgo.repository", nil},
		{"typed nil in interface", func() repository { return (*sqlRepository)(nil) }, "returned nil forgo.repository", nil},
		{"nil map", func() map[string]int { return nil }, "returned nil map[string]int", nil},
		{"panic", func() *service { panic("boom") }, "panicked: boom", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &closeLog{}
			b := NewBootstrap(
				[]ResourceFactory{
					resourceOf(&testResource{name: "first", value: &testDB{}, log: log}),
					resourceOf(&testResource{name: "second", value: 1, log: log}),
				},
				[]any{tt.provider},
			)
			_, err := b.Build()
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.HasPrefix(err.Error(), "bootstrap: provider 0 (") {
				t.Fatalf("got %v, want %q", err, tt.want)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("%v does not wrap %v", err, tt.is)
			}
			if got := log.get(); strings.Join(got, ",") != "second,first" {
				t.Fatalf("rolled back %v, want [second first]", got)
			}
			if _, err := b.Build(); err == nil || err.Error() != "bootstrap: already closed" {
				t.Fatalf("build after rollback: %v", err)
			}
		})
	}

	t.Run("panic keeps the stack", func(t *testing.T) {
		_, err := NewBootstrap(nil, []any{func() *service { panic("boom") }}).Build()
		if err == nil || !strings.Contains(err.Error(), "runtime/debug.Stack") {
			t.Fatalf("got %v, want the stack", err)
		}
	})
}

func TestBootstrapResourceFailures(t *testing.T) {
	errDial := errors.New("dial failed")
	tests := []struct {
		name    string
		factory func(log *closeLog) ResourceFactory
		want    string
		closed  string // resources closed, in order, after the failure
	}{
		{
			name:    "nil factory",
			factory: func(*closeLog) ResourceFactory { return nil },
			want:    "bootstrap: resource factory 1 is nil",
			closed:  "first",
		},
		{
			name:    "typed nil factory",
			factory: func(*closeLog) ResourceFactory { return (*resourceFunc)(nil) },
			want:    "bootstrap: resource factory 1 is nil",
			closed:  "first",
		},
		{
			name: "error",
			factory: func(*closeLog) ResourceFactory {
				return resourceFunc(func() (Resource, error) { return nil, errDial })
			},
			want:   "bootstrap: resource factory 1: dial failed",
			closed: "first",
		},
		{
			name: "error with a resource",
			factory: func(log *closeLog) ResourceFactory {
				return resourceFunc(func() (Resource, error) {
					return &testResource{name: "half", log: log}, errDial
				})
			},
			want:   "bootstrap: resource factory 1: dial failed",
			closed: "half,first",
		},
		{
			name: "nil resource",
			factory: func(*closeLog) ResourceFactory {
				return resourceFunc(func() (Resource, error) { return nil, nil })
			},
			want:   "bootstrap: resource factory 1 returned nil",
			closed: "first",
		},
		{
			name: "typed nil resource",
			factory: func(*closeLog) ResourceFactory {
				return resourceFunc(func() (Resource, error) { return (*testResource)(nil), nil })
			},
			want:   "bootstrap: resource factory 1 returned nil",
			closed: "first",
		},
		{
			name: "empty name",
			factory: func(log *closeLog) ResourceFactory {
				return resourceOf(&testResource{name: " ", value: 1, log: log})
			},
			want:   "bootstrap: resource factory 1 returned an empty name",
			closed: " ,first",
		},
		{
			name: "duplicate name",
			factory: func(log *closeLog) ResourceFactory {
				return resourceOf(&testResource{name: "first", value: 1, log: log})
			},
			want:   `bootstrap: duplicate resource name "first"`,
			closed: "first,first",
		},
		{
			name: "nil value",
			factory: func(log *closeLog) ResourceFactory {
				return resourceOf(&testResource{name: "second", log: log})
			},
			want:   `bootstrap: resource "second" returned nil Value`,
			closed: "second,first",
		},
		{
			name: "typed nil value",
			factory: func(log *closeLog) ResourceFactory {
				return resourceOf(&testResource{name: "second", value: (*testDB)(nil), log: log})
			},
			want:   `bootstrap: resource "second" returned typed nil *forgo.testDB`,
			closed: "second,first",
		},
		{
			name: "factory panics",
			factory: func(*closeLog) ResourceFactory {
				return resourceFunc(func() (Resource, error) { panic("boom") })
			},
			want:   "bootstrap: resource factory 1 panicked: boom",
			closed: "first",
		},
		{
			name: "Name panics",
			factory: func(log *closeLog) ResourceFactory {
				return resourceOf(&testResource{name: "second", value: 1, panicOn: "Name", log: log})
			},
			want:   "bootstrap: resource factory 1 panicked: name failed",
			closed: "second,first",
		},
		{
			name: "Value panics",
			factory: func(log *closeLog) ResourceFactory {
				return resourceOf(&testResource{name: "second", panicOn: "Value", log: log})
			},
			want:   "bootstrap: resource factory 1 panicked: value failed",
			closed: "second,first",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &closeLog{}
			b := NewBootstrap([]ResourceFactory{
				resourceOf(&testResource{name: "first", value: &testDB{}, log: log}),
				tt.factory(log),
			}, nil)
			_, err := b.Build()
			if err == nil || !strings.HasPrefix(err.Error(), tt.want) {
				t.Fatalf("got %v, want %q", err, tt.want)
			}
			if got := strings.Join(log.get(), ","); got != tt.closed {
				t.Fatalf("closed %q, want %q", got, tt.closed)
			}
		})
	}
}

func TestBootstrapRollbackCloseErrors(t *testing.T) {
	errClose := errors.New("close failed")
	_, err := NewBootstrap([]ResourceFactory{
		resourceOf(&testResource{name: "first", value: 1, closeErr: errClose}),
		resourceOf(&testResource{name: "first", value: 2, closeErr: errClose}),
	}, nil).Build()
	want := `bootstrap: duplicate resource name "first"` + "\n" + `close resource "first": close failed` + "\n" +
		`bootstrap rollback: close resource "first": close failed`
	if err == nil || err.Error() != want || !errors.Is(err, errClose) {
		t.Fatalf("got %v, want %q", err, want)
	}
}

func TestBootstrapClose(t *testing.T) {
	log := &closeLog{}
	errClose := errors.New("close failed")
	b := NewBootstrap([]ResourceFactory{
		resourceOf(&testResource{name: "a", value: 1, log: log}),
		resourceOf(&testResource{name: "b", value: "b", closeErr: errClose, log: log}),
		resourceOf(&testResource{name: "c", value: 1.5, panicOn: "Close"}),
		resourceOf(&testResource{name: "d", value: true, log: log}),
	}, nil)
	if _, err := b.Build(); err != nil {
		t.Fatal(err)
	}

	err := b.Close()
	if !errors.Is(err, errClose) || !strings.Contains(err.Error(), `close resource "b": close failed`) {
		t.Fatalf("got %v, want the close error of b", err)
	}
	if !strings.Contains(err.Error(), "close resource *forgo.testResource: panicked: close failed") {
		t.Fatalf("got %v, want the panic of c", err)
	}
	if got := strings.Join(log.get(), ","); got != "d,b,a" {
		t.Fatalf("closed %q, want d,b,a", got)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if got := len(log.get()); got != 3 {
		t.Fatalf("second close closed resources again: %d", got)
	}
}

type reportDeps struct {
	Primary *testDB `boot:"primary"`
	Replica *testDB `boot:"replica"`
	Repo    repository
	Skipped *testDB `boot:"-"`
	cache   *testDB
}

type report struct{ deps reportDeps }

func TestBootstrapDependencyStructs(t *testing.T) {
	primary, replica := &testDB{name: "primary"}, &testDB{name: "replica"}
	resources := []ResourceFactory{
		resourceOf(&testResource{name: "primary", value: primary}),
		resourceOf(&testResource{name: "replica", value: replica}),
	}

	t.Run("value and pointer", func(t *testing.T) {
		r := build(t, resources, []any{
			func() repository { return memRepository{} },
			func(d reportDeps) *report { return &report{deps: d} },
			func(d *reportDeps) report { return report{deps: *d} },
		})
		for _, rep := range []report{*mustResolve[*report](t, r), mustResolve[report](t, r)} {
			d := rep.deps
			if d.Primary != primary || d.Replica != replica || d.Repo == nil || d.Skipped != nil || d.cache != nil {
				t.Fatalf("deps %+v", d)
			}
		}
	})

	t.Run("plain *testDB is ambiguous", func(t *testing.T) {
		r := build(t, resources, nil)
		if _, err := r.Resolve[*testDB](); !errors.Is(err, ErrAmbiguousDependency) {
			t.Fatalf("got %v", err)
		}
	})

	tests := []struct {
		name     string
		provider any
		want     string
		is       error
	}{
		{
			name: "unknown resource",
			provider: func(struct {
				DB *testDB `boot:"missing"`
			}) int {
				return 0
			},
			want: `resource "missing" not found`,
			is:   ErrResourceNotFound,
		},
		{
			name: "wrong type",
			provider: func(struct {
				DB *sqlRepository `boot:"primary"`
			}) int {
				return 0
			},
			want: `resource "primary" provides *forgo.testDB, expected *forgo.sqlRepository`,
			is:   ErrResourceNotFound,
		},
		{
			name: "unexported tagged field",
			provider: func(struct {
				db *testDB `boot:"primary"`
			}) int {
				return 0
			},
			want: "dependency field struct { db *forgo.testDB \"boot:\\\"primary\\\"\" }.db is unexported",
		},
		{
			name: "empty tag",
			provider: func(struct {
				DB *testDB `boot:""`
			}) int {
				return 0
			},
			want: "has an empty boot tag",
		},
		{
			name: "untagged field resolved by type",
			provider: func(struct {
				DB   *testDB `boot:"primary"`
				Repo repository
			}) int {
				return 0
			},
			want: "Repo: missing dependency forgo.repository",
			is:   ErrMissingDependency,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewBootstrap(resources, []any{tt.provider}).Build()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want %q", err, tt.want)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("%v does not wrap %v", err, tt.is)
			}
		})
	}
}

func TestResolverResolveResource(t *testing.T) {
	db := &testDB{name: "primary"}
	r := build(t, []ResourceFactory{resourceOf(&testResource{name: "primary", value: db})}, nil)

	got, err := r.ResolveResource[*testDB]("primary")
	if err != nil || got != db {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := r.ResolveResource[*testDB]("replica"); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("missing: got %v", err)
	}
	if _, err := r.ResolveResource[string]("primary"); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("wrong type: got %v", err)
	}
}

func TestResolverCall(t *testing.T) {
	r := build(t,
		[]ResourceFactory{resourceOf(&testResource{name: "main", value: &testDB{name: "main"}})},
		[]any{newRepository},
	)

	t.Run("resolves arguments and does not register", func(t *testing.T) {
		svc, err := r.Call[*service](newService)
		if err != nil || svc.repo.Find() != "main" {
			t.Fatalf("got %v, %v", svc, err)
		}
		if _, err := r.Resolve[*service](); !errors.Is(err, ErrMissingDependency) {
			t.Fatalf("Call registered its result: %v", err)
		}
	})

	t.Run("result assignable to T", func(t *testing.T) {
		repo, err := r.Call[repository](newRepository)
		if err != nil || repo.Find() != "main" {
			t.Fatalf("got %v, %v", repo, err)
		}
		mw, err := r.Call[operation.Middleware](func() func(operation.Handler) operation.Handler {
			return func(next operation.Handler) operation.Handler { return next }
		})
		if err != nil || mw == nil {
			t.Fatalf("got %v, %v", mw, err)
		}
	})

	errBuild := errors.New("build failed")
	tests := []struct {
		name        string
		constructor any
		want        string
		is          error
	}{
		{"nil", nil, "bootstrap: constructor: nil function", nil},
		{"not a function", 42, "bootstrap: constructor: int is not a function", nil},
		{"wrong result", func() int { return 1 }, "bootstrap: constructor: github.com/efegreat/forgo.TestResolverCall.func3 returns int, expected *forgo.service", nil},
		{"error", func() (*service, error) { return nil, errBuild }, "bootstrap: constructor github.com/efegreat/forgo.TestResolverCall.func4: build failed", errBuild},
		{"nil result", func() *service { return nil }, "returned nil *forgo.service", nil},
		{"panic", func() *service { panic("boom") }, "panicked: boom", nil},
		{"missing argument", func(*memRepository) *service { return nil }, "argument 0 (*forgo.memRepository): missing dependency *forgo.memRepository", ErrMissingDependency},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := r.Call[*service](tt.constructor)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want %q", err, tt.want)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("%v does not wrap %v", err, tt.is)
			}
		})
	}
}

func TestBootstrapLifecycle(t *testing.T) {
	t.Run("zero resolver", func(t *testing.T) {
		if _, err := (Resolver{}).Resolve[int](); err == nil || err.Error() != "bootstrap: invalid resolver" {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("nil bootstrap", func(t *testing.T) {
		var b *Bootstrap
		if _, err := b.Build(); err == nil {
			t.Fatal("Build of a nil bootstrap succeeded")
		}
		if err := b.Seal(); err == nil {
			t.Fatal("Seal of a nil bootstrap succeeded")
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close of a nil bootstrap: %v", err)
		}
	})

	t.Run("seal", func(t *testing.T) {
		b := NewBootstrap(nil, []any{func() int { return 1 }})
		if err := b.Seal(); err == nil || err.Error() != "bootstrap: cannot seal before build" {
			t.Fatalf("seal before build: %v", err)
		}
		r, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Build(); err == nil || err.Error() != "bootstrap: already built" {
			t.Fatalf("second build: %v", err)
		}
		if err := b.Seal(); err != nil {
			t.Fatal(err)
		}
		sealed := "bootstrap: resolver is sealed"
		if _, err := r.Resolve[int](); err == nil || err.Error() != sealed {
			t.Fatalf("Resolve: %v", err)
		}
		if _, err := r.ResolveResource[int]("x"); err == nil || err.Error() != sealed {
			t.Fatalf("ResolveResource: %v", err)
		}
		if _, err := r.Call[int](func() int { return 1 }); err == nil || err.Error() != sealed {
			t.Fatalf("Call: %v", err)
		}
		if _, err := MiddlewareAs(func() operation.Middleware { return nil })(r); err == nil || err.Error() != sealed {
			t.Fatalf("MiddlewareAs: %v", err)
		}
	})

	t.Run("close", func(t *testing.T) {
		b := NewBootstrap(nil, nil)
		r, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Resolve[int](); err == nil || err.Error() != "bootstrap: closed" {
			t.Fatalf("Resolve after close: %v", err)
		}
		if err := b.Seal(); err == nil || err.Error() != "bootstrap: already closed" {
			t.Fatalf("Seal after close: %v", err)
		}
	})

	t.Run("not built", func(t *testing.T) {
		r := Resolver{bootstrap: NewBootstrap(nil, nil)}
		if _, err := r.Resolve[int](); err == nil || err.Error() != "bootstrap: not built" {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("lists are copied", func(t *testing.T) {
		providers := []any{func() int { return 1 }}
		b := NewBootstrap(nil, providers)
		providers[0] = "not a provider"
		if _, err := b.Build(); err != nil {
			t.Fatal(err)
		}
	})
}

// newTagMiddleware returns a middleware constructor that records tag and the name of its database.
func newTagMiddleware(tag string, tags *[]string) func(*testDB) operation.Middleware {
	return func(db *testDB) operation.Middleware {
		return func(next operation.Handler) operation.Handler {
			return func(ctx *operation.Context) error {
				*tags = append(*tags, tag+":"+db.name)
				return next(ctx)
			}
		}
	}
}

func TestResolveMiddlewares(t *testing.T) {
	r := build(t, []ResourceFactory{resourceOf(&testResource{name: "main", value: &testDB{name: "main"}})}, nil)

	var tags []string
	middlewares, err := ResolveMiddlewares(r, []MiddlewareFactory{
		MiddlewareAs(newTagMiddleware("a", &tags)),
		MiddlewareAs(newTagMiddleware("b", &tags)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := &operation.Context{Context: t.Context()}
	if err := operation.Chain(func(*operation.Context) error { return nil }, middlewares...)(ctx); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(tags, ","); got != "a:main,b:main" {
		t.Fatalf("ran %q, want a:main,b:main", got)
	}

	errFactory := errors.New("factory failed")
	tests := []struct {
		name      string
		factories []MiddlewareFactory
		want      string
	}{
		{"nil factory", []MiddlewareFactory{MiddlewareAs(newTagMiddleware("a", &tags)), nil}, "bootstrap: middleware 1: nil factory"},
		{"nil middleware", []MiddlewareFactory{func(Resolver) (operation.Middleware, error) { return nil, nil }}, "bootstrap: middleware 0: factory returned nil"},
		{"factory error", []MiddlewareFactory{func(Resolver) (operation.Middleware, error) { return nil, errFactory }}, "factory failed"},
		{"constructor error", []MiddlewareFactory{MiddlewareAs(func(*memRepository) operation.Middleware { return nil })}, "missing dependency *forgo.memRepository"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveMiddlewares(r, tt.factories)
			if err == nil || !strings.Contains(err.Error(), tt.want) || got != nil {
				t.Fatalf("got %v, %v, want %q", got, err, tt.want)
			}
		})
	}
}
