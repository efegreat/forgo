package forgo

import (
	"errors"
	"fmt"
	"forgo/operation"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
)

// Resource is a lifecycle-managed infrastructure dependency.
//
// Value returns the actual dependency exposed to providers.
// For example, an SqlDB resource would normally return *sql.DB.
type Resource interface {
	Name() string
	Value() any
	Close() error
}

// ResourceFactory creates and fully initializes a Resource.
//
// Any initialization checks such as database pinging should happen inside New.
// If New returns successfully, the Resource is expected to be ready for use.
type ResourceFactory interface {
	New() (Resource, error)
}

var errorType = reflect.TypeOf((*error)(nil)).Elem()

type binding struct {
	typ    reflect.Type
	value  reflect.Value
	source string
}

type resourceBinding struct {
	resource Resource
	binding  binding
}

// Resolver is a read-only bootstrap-time dependency resolver.
//
// It is valid after Bootstrap.Build and until Bootstrap.Seal.
// It should only be used while constructing/initing application/server objects.
type Resolver struct {
	bootstrap *Bootstrap
}

// Bootstrap constructs application dependencies.
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
// Bootstrap deliberately does not attempt to reorder providers.
// Providers must be declared from innermost to outermost.
type Bootstrap struct {
	resourceFactories []ResourceFactory
	providers         []any

	resources map[string]resourceBinding
	bindings  []binding

	// Resources in creation order.
	// Cleanup happens in reverse order.
	resourceOrder []Resource

	built  bool
	sealed bool
	closed bool
}

// NewBootstrap creates a Bootstrap.
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
func NewBootstrap(
	resources []ResourceFactory,
	providers []any,
) *Bootstrap {
	return &Bootstrap{
		resourceFactories: append(
			[]ResourceFactory(nil),
			resources...,
		),
		providers: append(
			[]any(nil),
			providers...,
		),
		resources: make(
			map[string]resourceBinding,
			len(resources),
		),
	}
}

// Build initializes resources and invokes providers.
//
// If any step fails, already-created resources are closed in reverse
// initialization order before Build returns.
func (b *Bootstrap) Build() (Resolver, error) {
	if b == nil {
		return Resolver{}, errors.New(
			"bootstrap: nil bootstrap",
		)
	}

	if b.built {
		return Resolver{}, errors.New(
			"bootstrap: already built",
		)
	}

	if b.closed {
		return Resolver{}, errors.New(
			"bootstrap: already closed",
		)
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

	return Resolver{
		bootstrap: b,
	}, nil
}

// Seal disables dependency resolution.
//
// It should be called after all server factories have finished constructing
// their handlers/jobs/consumers, but before the servers begin serving work.
//
// This prevents Bootstrap from accidentally becoming a runtime service locator.
func (b *Bootstrap) Seal() error {
	if b == nil {
		return errors.New(
			"bootstrap: nil bootstrap",
		)
	}

	if !b.built {
		return errors.New(
			"bootstrap: cannot seal before build",
		)
	}

	if b.closed {
		return errors.New(
			"bootstrap: already closed",
		)
	}

	b.sealed = true

	return nil
}

// Close closes all initialized resources in reverse initialization order.
//
// Close is idempotent.
func (b *Bootstrap) Close() error {
	if b == nil || b.closed {
		return nil
	}

	b.closed = true
	b.sealed = true

	return b.closeResources()
}

// buildResources initializes all resources before providers are constructed.
func (b *Bootstrap) buildResources() error {
	for i, factory := range b.resourceFactories {
		if isNilAny(factory) {
			return fmt.Errorf(
				"bootstrap: resource factory %d is nil",
				i,
			)
		}

		resource, err := factory.New()
		if err != nil {
			cause := fmt.Errorf(
				"bootstrap: resource factory %d: %w",
				i,
				err,
			)

			// Be defensive if a factory returns both a resource and an error.
			if !isNilAny(resource) {
				return closeCreatedResource(
					resource,
					cause,
				)
			}

			return cause
		}

		if isNilAny(resource) {
			return fmt.Errorf(
				"bootstrap: resource factory %d returned nil",
				i,
			)
		}

		name := strings.TrimSpace(resource.Name())

		if name == "" {
			return closeCreatedResource(
				resource,
				fmt.Errorf(
					"bootstrap: resource factory %d returned an empty name",
					i,
				),
			)
		}

		if _, exists := b.resources[name]; exists {
			return closeCreatedResource(
				resource,
				fmt.Errorf(
					"bootstrap: duplicate resource name %q",
					name,
				),
			)
		}

		raw := resource.Value()

		if raw == nil {
			return closeCreatedResource(
				resource,
				fmt.Errorf(
					"bootstrap: resource %q returned nil Value",
					name,
				),
			)
		}

		value := reflect.ValueOf(raw)

		if isNilValue(value) {
			return closeCreatedResource(
				resource,
				fmt.Errorf(
					"bootstrap: resource %q returned typed nil %s",
					name,
					value.Type(),
				),
			)
		}

		bind := binding{
			typ:    value.Type(),
			value:  value,
			source: fmt.Sprintf("resource %q", name),
		}

		b.resources[name] = resourceBinding{
			resource: resource,
			binding:  bind,
		}

		b.bindings = append(
			b.bindings,
			bind,
		)

		b.resourceOrder = append(
			b.resourceOrder,
			resource,
		)
	}

	return nil
}

// validateProviders validates provider definitions before resources are opened.
//
// Supported provider signatures:
//
//	func(...) T
//	func(...) (T, error)
//
// Providers may not be variadic and may not provide the same exact output type
// more than once.
func (b *Bootstrap) validateProviders() error {
	outputs := make(
		map[reflect.Type]string,
		len(b.providers),
	)

	for i, provider := range b.providers {
		if provider == nil {
			return fmt.Errorf(
				"bootstrap: provider %d is nil",
				i,
			)
		}

		fn := reflect.ValueOf(provider)

		if fn.Kind() != reflect.Func {
			return fmt.Errorf(
				"bootstrap: provider %d must be a function, got %T",
				i,
				provider,
			)
		}

		if fn.IsNil() {
			return fmt.Errorf(
				"bootstrap: provider %d is nil",
				i,
			)
		}

		typ := fn.Type()
		name := functionName(fn)

		if typ.IsVariadic() {
			return fmt.Errorf(
				"bootstrap: provider %s cannot be variadic",
				name,
			)
		}

		switch typ.NumOut() {
		case 1:
			// func(...) T

		case 2:
			// func(...) (T, error)

			if typ.Out(1) != errorType {
				return fmt.Errorf(
					"bootstrap: provider %s second result must be error",
					name,
				)
			}

		default:
			return fmt.Errorf(
				"bootstrap: provider %s must return T or (T, error)",
				name,
			)
		}

		out := typ.Out(0)

		if out == errorType {
			return fmt.Errorf(
				"bootstrap: provider %s cannot provide error",
				name,
			)
		}

		if previous, exists := outputs[out]; exists {
			return fmt.Errorf(
				"bootstrap: duplicate providers for %s: %s and %s",
				out,
				previous,
				name,
			)
		}

		outputs[out] = name
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
		fnType := fn.Type()
		name := functionName(fn)

		result, err := b.call(
			provider,
			fnType.Out(0),
		)
		if err != nil {
			return fmt.Errorf(
				"bootstrap: provider %s[%d]: %w",
				name,
				i,
				err,
			)
		}

		b.bindings = append(
			b.bindings,
			binding{
				typ:   fnType.Out(0),
				value: result,
				source: fmt.Sprintf(
					"provider %s[%d]",
					name,
					i,
				),
			},
		)
	}

	return nil
}

// resolveProviderArgument resolves an ordinary dependency or constructs
// a dependency struct containing `boot:"resourceName"` tags.
func (b *Bootstrap) resolveProviderArgument(
	typ reflect.Type,
) (reflect.Value, error) {
	structType, pointer, ok := bootStructType(typ)

	if !ok {
		return b.resolveType(typ)
	}

	value, err := b.resolveBootStruct(
		structType,
	)
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
// Tagged fields resolve only against named resources.
//
// Untagged exported fields are resolved normally by type.
func (b *Bootstrap) resolveBootStruct(
	typ reflect.Type,
) (reflect.Value, error) {
	result := reflect.New(typ).Elem()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		tag, tagged := field.Tag.Lookup("boot")

		// PkgPath is non-empty for unexported fields.
		if field.PkgPath != "" {
			if tagged {
				return reflect.Value{}, fmt.Errorf(
					"dependency field %s.%s is unexported",
					typ,
					field.Name,
				)
			}

			continue
		}

		if tagged {
			if tag == "" {
				return reflect.Value{}, fmt.Errorf(
					"dependency field %s.%s has an empty boot tag",
					typ,
					field.Name,
				)
			}

			if tag == "-" {
				continue
			}

			value, err := b.resolveNamedResource(
				tag,
				field.Type,
			)
			if err != nil {
				return reflect.Value{}, fmt.Errorf(
					"%s.%s: %w",
					typ,
					field.Name,
					err,
				)
			}

			result.Field(i).Set(value)

			continue
		}

		value, err := b.resolveType(
			field.Type,
		)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"%s.%s: %w",
				typ,
				field.Name,
				err,
			)
		}

		result.Field(i).Set(value)
	}

	return result, nil
}

// resolveNamedResource resolves a Resource.Value by its Resource.Name.
func (b *Bootstrap) resolveNamedResource(
	name string,
	typ reflect.Type,
) (reflect.Value, error) {
	resource, ok := b.resources[name]

	if !ok {
		return reflect.Value{}, fmt.Errorf(
			"resource %q not found",
			name,
		)
	}

	if !resource.binding.typ.AssignableTo(typ) {
		return reflect.Value{}, fmt.Errorf(
			"resource %q provides %s, expected %s",
			name,
			resource.binding.typ,
			typ,
		)
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
func (b *Bootstrap) resolveType(
	typ reflect.Type,
) (reflect.Value, error) {
	matches := make(
		[]binding,
		0,
		1,
	)

	for _, candidate := range b.bindings {
		if candidate.typ.AssignableTo(typ) {
			matches = append(
				matches,
				candidate,
			)
		}
	}

	switch len(matches) {
	case 0:
		return reflect.Value{}, fmt.Errorf(
			"missing dependency %s",
			typ,
		)

	case 1:
		return matches[0].value, nil

	default:
		sources := make(
			[]string,
			0,
			len(matches),
		)

		for _, match := range matches {
			sources = append(
				sources,
				fmt.Sprintf(
					"%s (%s)",
					match.source,
					match.typ,
				),
			)
		}

		return reflect.Value{}, fmt.Errorf(
			"ambiguous dependency %s: %s",
			typ,
			strings.Join(sources, ", "),
		)
	}
}

// Resolve returns a uniquely matching provider/resource value.
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

	typ := reflect.TypeOf((*T)(nil)).Elem()

	value, err := b.resolveType(typ)
	if err != nil {
		return zero, err
	}

	result, ok := value.Interface().(T)
	if !ok {
		return zero, fmt.Errorf(
			"bootstrap: resolved %s as incompatible type %s",
			typ,
			value.Type(),
		)
	}

	return result, nil
}

// ResolveResource resolves a specifically named infrastructure resource.
//
// Normally this is unnecessary because provider constructors should receive
// resources automatically. It is useful when a server factory itself needs a
// specifically named resource.
func (r Resolver) ResolveResource[T any](name string) (T, error) {
	var zero T

	b, err := r.activeBootstrap()
	if err != nil {
		return zero, err
	}

	typ := reflect.TypeOf((*T)(nil)).Elem()

	value, err := b.resolveNamedResource(name, typ)
	if err != nil {
		return zero, err
	}

	result, ok := value.Interface().(T)
	if !ok {
		return zero, fmt.Errorf(
			"bootstrap: resource %q resolved as incompatible type %s",
			name,
			value.Type(),
		)
	}

	return result, nil
}

// Call invokes a bootstrap-time constructor/factory.
//
// Every function argument is resolved using the same rules as provider
// dependencies:
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
// Call does NOT register the returned value in Bootstrap.
// It is intended for constructing final/bootstrap-local objects such as:
//
//   - HTTP handlers
//   - middlewares
//   - cron handlers
//   - consumer handlers
//   - command handlers
//
// Call is only available while the Resolver is active. Once Bootstrap is
// sealed, calling it returns an error.
func (r Resolver) Call[T any](constructor any) (T, error) {
	var zero T

	b, err := r.activeBootstrap()
	if err != nil {
		return zero, err
	}

	expected := reflect.TypeFor[T]()

	value, err := b.call(
		constructor,
		expected,
	)
	if err != nil {
		return zero, err
	}

	result, ok := value.Interface().(T)
	if !ok {
		return zero, fmt.Errorf(
			"bootstrap: internal error: resolved %s cannot be asserted as %s",
			value.Type(),
			expected,
		)
	}

	return result, nil
}

func (r Resolver) activeBootstrap() (*Bootstrap, error) {
	if r.bootstrap == nil {
		return nil, errors.New(
			"bootstrap: invalid resolver",
		)
	}

	if !r.bootstrap.built {
		return nil, errors.New(
			"bootstrap: not built",
		)
	}

	if r.bootstrap.closed {
		return nil, errors.New(
			"bootstrap: closed",
		)
	}

	if r.bootstrap.sealed {
		return nil, errors.New(
			"bootstrap: resolver is sealed",
		)
	}

	return r.bootstrap, nil
}

// call resolves the arguments of constructor, invokes it, validates its
// result and returns the constructed value.
//
// expected is the type that the caller expects the constructor to produce.
func (b *Bootstrap) call(
	constructor any,
	expected reflect.Type,
) (reflect.Value, error) {
	fn, fnType, name, err := inspectConstructor(
		constructor,
		expected,
	)
	if err != nil {
		return reflect.Value{}, err
	}

	args := make(
		[]reflect.Value,
		fnType.NumIn(),
	)

	for i := 0; i < fnType.NumIn(); i++ {
		dependencyType := fnType.In(i)

		value, err := b.resolveProviderArgument(
			dependencyType,
		)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"bootstrap: constructor %s argument %d (%s): %w",
				name,
				i,
				dependencyType,
				err,
			)
		}

		args[i] = value
	}

	results, err := invokeConstructor(
		fn,
		args,
		name,
	)
	if err != nil {
		return reflect.Value{}, err
	}

	if len(results) == 2 && !results[1].IsNil() {
		return reflect.Value{}, fmt.Errorf(
			"bootstrap: constructor %s: %w",
			name,
			results[1].Interface().(error),
		)
	}

	result := results[0]

	if isNilValue(result) {
		return reflect.Value{}, fmt.Errorf(
			"bootstrap: constructor %s returned nil %s",
			name,
			fnType.Out(0),
		)
	}

	// The constructor result may be assignable to expected without
	// having exactly the same type.
	//
	// Example:
	//
	//	func(...) func(context.Context, Req) (Resp, error)
	//
	// can be assigned to:
	//
	//	type ServiceHandler[Req, Resp any]
	//	    func(context.Context, Req) (Resp, error)
	//
	// Normalize the reflected value to the expected type so callers can
	// safely retrieve it as T.
	if result.Type() != expected {
		normalized := reflect.New(expected).Elem()
		normalized.Set(result)
		result = normalized
	}

	return result, nil
}

// rollback closes resources that were successfully initialized before
// bootstrap failed.
func (b *Bootstrap) rollback(
	cause error,
) error {
	cleanupErr := b.closeResources()

	b.closed = true
	b.sealed = true

	if cleanupErr == nil {
		return cause
	}

	return errors.Join(
		cause,
		fmt.Errorf(
			"bootstrap rollback: %w",
			cleanupErr,
		),
	)
}

func inspectConstructor(
	constructor any,
	expected reflect.Type,
) (
	reflect.Value,
	reflect.Type,
	string,
	error,
) {
	if constructor == nil {
		return reflect.Value{},
			nil,
			"",
			fmt.Errorf(
				"bootstrap: constructor is nil",
			)
	}

	fn := reflect.ValueOf(constructor)

	if fn.Kind() != reflect.Func {
		return reflect.Value{},
			nil,
			"",
			fmt.Errorf(
				"bootstrap: constructor must be a function, got %T",
				constructor,
			)
	}

	if fn.IsNil() {
		return reflect.Value{},
			nil,
			"",
			fmt.Errorf(
				"bootstrap: constructor is nil",
			)
	}

	fnType := fn.Type()
	name := functionName(fn)

	if fnType.IsVariadic() {
		return reflect.Value{},
			nil,
			"",
			fmt.Errorf(
				"bootstrap: constructor %s cannot be variadic",
				name,
			)
	}

	switch fnType.NumOut() {
	case 1:
		// func(...) T

	case 2:
		// func(...) (T, error)

		if fnType.Out(1) != errorType {
			return reflect.Value{},
				nil,
				"",
				fmt.Errorf(
					"bootstrap: constructor %s second result must be error",
					name,
				)
		}

	default:
		return reflect.Value{},
			nil,
			"",
			fmt.Errorf(
				"bootstrap: constructor %s must return T or (T, error)",
				name,
			)
	}

	produced := fnType.Out(0)

	if !produced.AssignableTo(expected) {
		return reflect.Value{},
			nil,
			"",
			fmt.Errorf(
				"bootstrap: constructor %s returns %s, expected %s",
				name,
				produced,
				expected,
			)
	}

	return fn, fnType, name, nil
}

func invokeConstructor(
	fn reflect.Value,
	args []reflect.Value,
	name string,
) (
	results []reflect.Value,
	err error,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"bootstrap: constructor %s panicked: %v\n%s",
				name,
				recovered,
				debug.Stack(),
			)
		}
	}()

	results = fn.Call(args)

	return results, nil
}

// closeResources closes resources in reverse initialization order.
func (b *Bootstrap) closeResources() error {
	var errs []error

	for i := len(b.resourceOrder) - 1; i >= 0; i-- {
		resource := b.resourceOrder[i]

		if err := resource.Close(); err != nil {
			errs = append(
				errs,
				fmt.Errorf(
					"close resource %q: %w",
					resource.Name(),
					err,
				),
			)
		}
	}

	return errors.Join(errs...)
}

// bootStructType determines whether typ is a dependency struct.
//
// Both:
//
//	Dependencies
//
// and:
//
//	*Dependencies
//
// are supported.
func bootStructType(
	typ reflect.Type,
) (
	structType reflect.Type,
	pointer bool,
	ok bool,
) {
	switch typ.Kind() {
	case reflect.Struct:
		if hasBootTag(typ) {
			return typ, false, true
		}

	case reflect.Pointer:
		elem := typ.Elem()

		if elem.Kind() == reflect.Struct &&
			hasBootTag(elem) {

			return elem, true, true
		}
	}

	return nil, false, false
}

func hasBootTag(
	typ reflect.Type,
) bool {
	for i := 0; i < typ.NumField(); i++ {
		if _, ok := typ.Field(i).Tag.Lookup("boot"); ok {
			return true
		}
	}

	return false
}

func functionName(
	fn reflect.Value,
) string {
	if fn.Kind() != reflect.Func ||
		fn.IsNil() {

		return "<invalid>"
	}

	f := runtime.FuncForPC(
		fn.Pointer(),
	)
	if f == nil {
		return "<anonymous>"
	}

	return f.Name()
}

// closeCreatedResource handles resources that were constructed successfully
// but rejected before they were added to Bootstrap's managed resource list.
func closeCreatedResource(
	resource Resource,
	cause error,
) error {
	if err := resource.Close(); err != nil {
		return errors.Join(
			cause,
			fmt.Errorf(
				"close resource %q: %w",
				resource.Name(),
				err,
			),
		)
	}

	return cause
}

func isNilAny(
	value any,
) bool {
	if value == nil {
		return true
	}

	return isNilValue(
		reflect.ValueOf(value),
	)
}

// isNilValue also catches interfaces containing typed nils.
func isNilValue(
	value reflect.Value,
) bool {
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return true
		}

		return isNilValue(
			value.Elem(),
		)

	case reflect.Chan,
		reflect.Func,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:

		return value.IsNil()

	default:
		return false
	}
}

type MiddlewareFactory func(
	Resolver,
) (operation.Middleware, error)

func MiddlewareAs(
	constructor any,
) MiddlewareFactory {
	return func(
		r Resolver,
	) (operation.Middleware, error) {
		return r.Call[operation.Middleware](
			constructor,
		)
	}
}

func ResolveMiddlewares(r Resolver, factories []MiddlewareFactory) ([]operation.Middleware, error) {
	middlewares := make([]operation.Middleware, len(factories))
	for i, fc := range factories {
		m, err := fc(r)
		if err != nil {
			return nil, err
		}
		middlewares[i] = m
	}
	return middlewares, nil
}
