package forgo

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/efegreat/forgo/operation"
)

var (
	// ErrMissingDependency is returned when no resource or provider is assignable to a requested type.
	// Servers resolve optional dependencies, such as operation.ILocker, and treat it as their absence.
	ErrMissingDependency = errors.New("missing dependency")
	// ErrAmbiguousDependency is returned when more than one resource or provider is assignable to a
	// requested type. There is no best match: narrow the type, or select a resource by name.
	ErrAmbiguousDependency = errors.New("ambiguous dependency")
	// ErrResourceNotFound is returned when no resource has the requested name, or when it has the name but
	// its value is not assignable to the requested type.
	ErrResourceNotFound = errors.New("resource not found")
)

// Resource is a lifecycle-managed infrastructure dependency, such as a database connection pool.
//
// Name must be non-empty and unique among the resources of a Bootstrap: `boot:"name"` tags and
// Resolver.ResolveResource select resources by it. Value returns the dependency providers receive, and
// its dynamic type is the type they ask for: an SQL resource would normally return a *sql.DB. Close is
// called once, on shutdown, in reverse creation order.
type Resource interface {
	Name() string
	Value() any
	Close() error
}

// ResourceFactory creates and fully initializes a Resource, usually as a config struct.
//
// Initialization checks, such as pinging a database, belong in New: when New returns without error, the
// Resource is expected to be ready for use. A Resource New returns along with an error is closed.
type ResourceFactory interface {
	New() (Resource, error)
}

var errorType = reflect.TypeFor[error]()

type binding struct {
	typ    reflect.Type
	value  reflect.Value
	source string
}

type resourceBinding struct {
	resource Resource
	binding  binding
}

// Resolver resolves dependencies while an application is constructed, such as in ServerFactory.New.
//
// It is valid after Bootstrap.Build and until Bootstrap.Seal, so it cannot become a runtime service
// locator. Like Bootstrap, it is not safe for concurrent use.
type Resolver struct {
	bootstrap *Bootstrap
}

// Bootstrap constructs application dependencies. App and cli.CLI drive it, and tests may use it directly.
//
// Initialization order:
//
//	Resource factories
//	      ↓
//	Resources
//	      ↓
//	Providers (in declaration order)
//	      ↓
//	Server factories use Resolver
//	      ↓
//	Seal()
//	      ↓
//	Serve()
//
// Bootstrap deliberately does not reorder providers: they must be declared from innermost to outermost.
// It is not safe for concurrent use.
type Bootstrap struct {
	resourceFactories []ResourceFactory
	providers         []any

	resources map[string]resourceBinding
	bindings  []binding

	// Resources in creation order. Cleanup happens in reverse order.
	resourceOrder []Resource

	built  bool
	sealed bool
	closed bool
}

// NewBootstrap creates a Bootstrap. It copies both lists.
//
// Providers must be ordered from innermost to outermost:
//
//	NewRepository,
//	NewManager,
//	NewService,
//
// rather than:
//
//	NewService,
//	NewRepository,
func NewBootstrap(resources []ResourceFactory, providers []any) *Bootstrap {
	return &Bootstrap{
		resourceFactories: append([]ResourceFactory(nil), resources...),
		providers:         append([]any(nil), providers...),
		resources:         make(map[string]resourceBinding, len(resources)),
	}
}

// Build validates the providers, creates the resources, then invokes the providers.
//
// If any step fails, the resources created so far are closed in reverse creation order before Build
// returns, and the Bootstrap is closed.
func (b *Bootstrap) Build() (Resolver, error) {
	if b == nil {
		return Resolver{}, errors.New("bootstrap: nil bootstrap")
	}
	if b.built {
		return Resolver{}, errors.New("bootstrap: already built")
	}
	if b.closed {
		return Resolver{}, errors.New("bootstrap: already closed")
	}

	// Validate provider signatures before opening resources.
	if err := b.validateProviders(); err != nil {
		return Resolver{}, err
	}
	if err := b.buildResources(); err != nil {
		return Resolver{}, b.rollback(err)
	}
	if err := b.buildProviders(); err != nil {
		return Resolver{}, b.rollback(err)
	}

	b.built = true
	return Resolver{bootstrap: b}, nil
}

// Seal disables dependency resolution.
//
// Call it after all server factories have constructed their handlers, jobs and consumers, and before the
// servers begin serving work. This prevents Bootstrap from becoming a runtime service locator.
func (b *Bootstrap) Seal() error {
	if b == nil {
		return errors.New("bootstrap: nil bootstrap")
	}
	if !b.built {
		return errors.New("bootstrap: cannot seal before build")
	}
	if b.closed {
		return errors.New("bootstrap: already closed")
	}
	b.sealed = true
	return nil
}

// Close closes all resources in reverse creation order, and returns their errors joined. It is
// idempotent.
func (b *Bootstrap) Close() error {
	if b == nil || b.closed {
		return nil
	}
	b.closed = true
	b.sealed = true
	return b.closeResources()
}

// buildResources creates all resources before providers are constructed.
func (b *Bootstrap) buildResources() error {
	for i, factory := range b.resourceFactories {
		if isNilAny(factory) {
			return fmt.Errorf("bootstrap: resource factory %d is nil", i)
		}
		if err := b.buildResource(i, factory); err != nil {
			return err
		}
	}
	return nil
}

// buildResource creates and binds the resource of factory i. A resource it rejects is closed.
func (b *Bootstrap) buildResource(i int, factory ResourceFactory) (err error) {
	var resource Resource
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("bootstrap: resource factory %d panicked: %v\n%s", i, recovered, debug.Stack())
			if !isNilAny(resource) {
				err = closeCreatedResource(resource, err)
			}
		}
	}()

	resource, err = factory.New()
	if err != nil {
		err = fmt.Errorf("bootstrap: resource factory %d: %w", i, err)
		// Be defensive if a factory returns both a resource and an error.
		if !isNilAny(resource) {
			return closeCreatedResource(resource, err)
		}
		return err
	}
	if isNilAny(resource) {
		return fmt.Errorf("bootstrap: resource factory %d returned nil", i)
	}

	name := strings.TrimSpace(resource.Name())
	if name == "" {
		return closeCreatedResource(resource, fmt.Errorf("bootstrap: resource factory %d returned an empty name", i))
	}
	if _, exists := b.resources[name]; exists {
		return closeCreatedResource(resource, fmt.Errorf("bootstrap: duplicate resource name %q", name))
	}

	raw := resource.Value()
	if raw == nil {
		return closeCreatedResource(resource, fmt.Errorf("bootstrap: resource %q returned nil Value", name))
	}
	value := reflect.ValueOf(raw)
	if isNilValue(value) {
		return closeCreatedResource(resource, fmt.Errorf("bootstrap: resource %q returned typed nil %s", name, value.Type()))
	}

	bind := binding{typ: value.Type(), value: value, source: fmt.Sprintf("resource %q", name)}
	b.resources[name] = resourceBinding{resource: resource, binding: bind}
	b.bindings = append(b.bindings, bind)
	b.resourceOrder = append(b.resourceOrder, resource)
	return nil
}

// validateProviders validates provider definitions before resources are opened.
//
// Supported provider signatures:
//
//	func(...) T
//	func(...) (T, error)
//
// Providers may not be variadic and may not provide the same exact output type more than once.
func (b *Bootstrap) validateProviders() error {
	outputs := make(map[reflect.Type]string, len(b.providers))
	for i, provider := range b.providers {
		fn, err := inspectFunc(provider, nil)
		if err != nil {
			return fmt.Errorf("bootstrap: provider %d: %w", i, err)
		}
		out := fn.Type().Out(0)
		if out == errorType {
			return fmt.Errorf("bootstrap: provider %d (%s): cannot provide error", i, functionName(fn))
		}
		if previous, exists := outputs[out]; exists {
			return fmt.Errorf("bootstrap: duplicate providers for %s: %s and %s", out, previous, functionName(fn))
		}
		outputs[out] = functionName(fn)
	}
	return nil
}

// buildProviders invokes providers sequentially.
//
// A provider may only depend on:
//
//   - resources
//   - outputs of providers declared before it
//
// Bootstrap intentionally performs no topological sorting.
func (b *Bootstrap) buildProviders() error {
	for i, provider := range b.providers {
		fn := reflect.ValueOf(provider)
		out := fn.Type().Out(0)
		name := functionName(fn)

		result, err := b.call(fn)
		if err != nil {
			return fmt.Errorf("bootstrap: provider %d (%s): %w", i, name, err)
		}
		b.bindings = append(b.bindings, binding{typ: out, value: result, source: fmt.Sprintf("provider %d (%s)", i, name)})
	}
	return nil
}

// resolveArgument resolves an ordinary dependency, or constructs a dependency struct containing
// `boot:"resourceName"` tags.
func (b *Bootstrap) resolveArgument(typ reflect.Type) (reflect.Value, error) {
	structType, pointer, ok := bootStructType(typ)
	if !ok {
		return b.resolveType(typ)
	}

	value, err := b.resolveBootStruct(structType)
	if err != nil {
		return reflect.Value{}, err
	}
	if pointer {
		ptr := reflect.New(structType)
		ptr.Elem().Set(value)
		return ptr, nil
	}
	return value, nil
}

// resolveBootStruct fills a dependency struct.
//
// Example:
//
//	type UserRepositoryDependencies struct {
//	    DBOne *sql.DB `boot:"mysql"`
//	    DBTwo *sql.DB `boot:"anotherMysql"`
//	}
//
// Tagged fields resolve only against named resources. Untagged exported fields are resolved by type.
func (b *Bootstrap) resolveBootStruct(typ reflect.Type) (reflect.Value, error) {
	result := reflect.New(typ).Elem()
	for i := range typ.NumField() {
		field := typ.Field(i)
		tag, tagged := field.Tag.Lookup("boot")

		if !field.IsExported() {
			if tagged {
				return reflect.Value{}, fmt.Errorf("dependency field %s.%s is unexported", typ, field.Name)
			}
			continue
		}

		var value reflect.Value
		var err error
		switch {
		case tagged && tag == "":
			return reflect.Value{}, fmt.Errorf("dependency field %s.%s has an empty boot tag", typ, field.Name)
		case tag == "-":
			continue
		case tagged:
			value, err = b.resolveNamedResource(tag, field.Type)
		default:
			value, err = b.resolveType(field.Type)
		}
		if err != nil {
			return reflect.Value{}, fmt.Errorf("%s.%s: %w", typ, field.Name, err)
		}
		result.Field(i).Set(value)
	}
	return result, nil
}

// resolveNamedResource resolves a Resource.Value by its Resource.Name.
func (b *Bootstrap) resolveNamedResource(name string, typ reflect.Type) (reflect.Value, error) {
	resource, ok := b.resources[name]
	if !ok {
		return reflect.Value{}, fmt.Errorf("%w: resource %q not found", ErrResourceNotFound, name)
	}
	if !resource.binding.typ.AssignableTo(typ) {
		return reflect.Value{}, fmt.Errorf("%w: resource %q provides %s, expected %s",
			ErrResourceNotFound, name, resource.binding.typ, typ)
	}
	return resource.binding.value, nil
}

// resolveType resolves a dependency according to the strict rule:
//
//	0 matches  -> error
//	1 match    -> inject it
//	2+ matches -> ambiguity error
//
// AssignableTo also handles concrete implementations of requested interfaces.
func (b *Bootstrap) resolveType(typ reflect.Type) (reflect.Value, error) {
	var matches []binding
	for _, candidate := range b.bindings {
		if candidate.typ.AssignableTo(typ) {
			matches = append(matches, candidate)
		}
	}

	switch len(matches) {
	case 0:
		return reflect.Value{}, fmt.Errorf("%w %s", ErrMissingDependency, typ)
	case 1:
		return matches[0].value, nil
	default:
		sources := make([]string, len(matches))
		for i, match := range matches {
			sources[i] = fmt.Sprintf("%s (%s)", match.source, match.typ)
		}
		return reflect.Value{}, fmt.Errorf("%w %s: %s", ErrAmbiguousDependency, typ, strings.Join(sources, ", "))
	}
}

// Resolve returns the single resource or provider value assignable to T.
//
// It is intended for composition roots such as ServerFactory.New:
//
//	service, err := r.Resolve[*user.Service]()
//
// Resolve cannot be used after Bootstrap.Seal.
func (r Resolver) Resolve[T any]() (T, error) {
	var zero T
	b, err := r.activeBootstrap()
	if err != nil {
		return zero, err
	}
	value, err := b.resolveType(reflect.TypeFor[T]())
	if err != nil {
		return zero, err
	}
	return valueAs[T](value), nil
}

// ResolveResource returns the value of the resource named name, as T.
//
// Providers receive resources by type, or by name through dependency structs, so this is rarely needed.
// It is useful when a server factory itself needs a specific resource.
func (r Resolver) ResolveResource[T any](name string) (T, error) {
	var zero T
	b, err := r.activeBootstrap()
	if err != nil {
		return zero, err
	}
	value, err := b.resolveNamedResource(name, reflect.TypeFor[T]())
	if err != nil {
		return zero, err
	}
	return valueAs[T](value), nil
}

// Call invokes a bootstrap-time constructor and returns its result, which must be assignable to T.
//
// Every argument is resolved with the same rules as provider dependencies:
//
//   - exact/concrete dependencies
//   - interface implementations
//   - named resources through dependency structs using `boot:"name"`
//   - missing dependency detection
//   - ambiguous dependency detection
//
// Supported signatures:
//
//	func(...) T
//	func(...) (T, error)
//
// Call does NOT register the result in Bootstrap, so nothing can depend on it. It constructs entry points
// such as:
//
//   - HTTP handlers
//   - middlewares
//   - cron jobs
//   - message handlers
//   - commands
//
// Call cannot be used after Bootstrap.Seal.
func (r Resolver) Call[T any](constructor any) (T, error) {
	var zero T
	b, err := r.activeBootstrap()
	if err != nil {
		return zero, err
	}

	fn, err := inspectFunc(constructor, reflect.TypeFor[T]())
	if err != nil {
		return zero, fmt.Errorf("bootstrap: constructor: %w", err)
	}
	value, err := b.call(fn)
	if err != nil {
		return zero, fmt.Errorf("bootstrap: constructor %s: %w", functionName(fn), err)
	}
	return valueAs[T](value), nil
}

// valueAs returns value, whose type is assignable to T, as a T. Unlike a type assertion, it accepts a
// value of an unnamed type for a named T, such as a func(int) int for a type Fn func(int) int.
func valueAs[T any](value reflect.Value) T {
	var result T
	reflect.ValueOf(&result).Elem().Set(value)
	return result
}

func (r Resolver) activeBootstrap() (*Bootstrap, error) {
	switch {
	case r.bootstrap == nil:
		return nil, errors.New("bootstrap: invalid resolver")
	case !r.bootstrap.built:
		return nil, errors.New("bootstrap: not built")
	case r.bootstrap.closed:
		return nil, errors.New("bootstrap: closed")
	case r.bootstrap.sealed:
		return nil, errors.New("bootstrap: resolver is sealed")
	}
	return r.bootstrap, nil
}

// call resolves the arguments of fn, a function inspectFunc accepted, invokes it and returns its result,
// which is not nil.
func (b *Bootstrap) call(fn reflect.Value) (reflect.Value, error) {
	fnType := fn.Type()
	args := make([]reflect.Value, fnType.NumIn())
	for i := range args {
		value, err := b.resolveArgument(fnType.In(i))
		if err != nil {
			return reflect.Value{}, fmt.Errorf("argument %d (%s): %w", i, fnType.In(i), err)
		}
		args[i] = value
	}

	results, err := invoke(fn, args)
	if err != nil {
		return reflect.Value{}, err
	}
	if len(results) == 2 && !results[1].IsNil() {
		return reflect.Value{}, results[1].Interface().(error)
	}

	if isNilValue(results[0]) {
		return reflect.Value{}, fmt.Errorf("returned nil %s", fnType.Out(0))
	}
	return results[0], nil
}

// rollback closes the resources created before Build failed, and closes the Bootstrap.
func (b *Bootstrap) rollback(cause error) error {
	cleanupErr := b.closeResources()
	b.closed = true
	b.sealed = true
	if cleanupErr == nil {
		return cause
	}
	return errors.Join(cause, fmt.Errorf("bootstrap rollback: %w", cleanupErr))
}

// inspectFunc checks that f is a non-nil, non-variadic func(...) T or func(...) (T, error), and, unless
// expected is nil, that T is assignable to expected.
func inspectFunc(f any, expected reflect.Type) (reflect.Value, error) {
	if f == nil {
		return reflect.Value{}, errors.New("nil function")
	}
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func {
		return reflect.Value{}, fmt.Errorf("%T is not a function", f)
	}
	if fn.IsNil() {
		return reflect.Value{}, errors.New("nil function")
	}

	fnType := fn.Type()
	name := functionName(fn)
	if fnType.IsVariadic() {
		return reflect.Value{}, fmt.Errorf("%s cannot be variadic", name)
	}
	switch {
	case fnType.NumOut() == 2 && fnType.Out(1) != errorType:
		return reflect.Value{}, fmt.Errorf("%s second result must be error", name)
	case fnType.NumOut() != 1 && fnType.NumOut() != 2:
		return reflect.Value{}, fmt.Errorf("%s must return T or (T, error)", name)
	}
	if expected != nil && !fnType.Out(0).AssignableTo(expected) {
		return reflect.Value{}, fmt.Errorf("%s returns %s, expected %s", name, fnType.Out(0), expected)
	}
	return fn, nil
}

// invoke calls fn, turning a panic into an error with the stack.
func invoke(fn reflect.Value, args []reflect.Value) (results []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panicked: %v\n%s", recovered, debug.Stack())
		}
	}()
	return fn.Call(args), nil
}

// closeResources closes resources in reverse creation order.
func (b *Bootstrap) closeResources() error {
	var errs []error
	for i := len(b.resourceOrder) - 1; i >= 0; i-- {
		if err := closeResource(b.resourceOrder[i]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// closeResource closes resource, turning a panic into an error, so the resources after it are still
// closed.
func closeResource(resource Resource) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("close resource %T: panicked: %v\n%s", resource, recovered, debug.Stack())
		}
	}()
	if err := resource.Close(); err != nil {
		return fmt.Errorf("close resource %q: %w", resource.Name(), err)
	}
	return nil
}

// closeCreatedResource closes a resource that was created but rejected before Bootstrap managed it.
func closeCreatedResource(resource Resource, cause error) error {
	if err := closeResource(resource); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// bootStructType reports whether typ is a dependency struct: a struct, or a pointer to a struct, with at
// least one boot tag.
func bootStructType(typ reflect.Type) (structType reflect.Type, pointer bool, ok bool) {
	switch typ.Kind() {
	case reflect.Struct:
		if hasBootTag(typ) {
			return typ, false, true
		}
	case reflect.Pointer:
		if elem := typ.Elem(); elem.Kind() == reflect.Struct && hasBootTag(elem) {
			return elem, true, true
		}
	}
	return nil, false, false
}

func hasBootTag(typ reflect.Type) bool {
	for i := range typ.NumField() {
		if _, ok := typ.Field(i).Tag.Lookup("boot"); ok {
			return true
		}
	}
	return false
}

func functionName(fn reflect.Value) string {
	if f := runtime.FuncForPC(fn.Pointer()); f != nil {
		return f.Name()
	}
	return "<anonymous>"
}

func isNilAny(value any) bool {
	return value == nil || isNilValue(reflect.ValueOf(value))
}

// isNilValue also catches interfaces containing typed nils.
func isNilValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Interface:
		return value.IsNil() || isNilValue(value.Elem())
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// MiddlewareFactory constructs a middleware from the bootstrap, in ServerFactory.New. Server configs hold
// lists of them, such as ServerConfig.Middlewares.
type MiddlewareFactory func(Resolver) (operation.Middleware, error)

// MiddlewareAs returns a MiddlewareFactory that calls constructor through Resolver.Call: its arguments are
// resolved from the bootstrap, and its result must be an operation.Middleware.
//
//	forgo.MiddlewareAs(NewAuthMiddleware) // func NewAuthMiddleware(s *auth.Service) operation.Middleware
func MiddlewareAs(constructor any) MiddlewareFactory {
	return func(r Resolver) (operation.Middleware, error) {
		return r.Call[operation.Middleware](constructor)
	}
}

// ResolveMiddlewares calls every factory, in order. It fails on a nil factory and on a factory that returns
// a nil middleware, so neither reaches a chain.
func ResolveMiddlewares(r Resolver, factories []MiddlewareFactory) ([]operation.Middleware, error) {
	middlewares := make([]operation.Middleware, len(factories))
	for i, factory := range factories {
		if factory == nil {
			return nil, fmt.Errorf("bootstrap: middleware %d: nil factory", i)
		}
		m, err := factory(r)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, fmt.Errorf("bootstrap: middleware %d: factory returned nil", i)
		}
		middlewares[i] = m
	}
	return middlewares, nil
}
