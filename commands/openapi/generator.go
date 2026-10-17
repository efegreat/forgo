package openapi

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/efegreat/forgo"
	fhttp "github.com/efegreat/forgo/server/http"
)

const (
	openAPIVersion     = "3.1.0"
	defaultTitle       = "API Doc"
	defaultDescription = "HTTP API Doc"
	defaultVersion     = "1.0.0"
)

// operationMethods are the methods an OpenAPI 3.1 path item can describe.
var operationMethods = []string{
	http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete,
	http.MethodOptions, http.MethodHead, http.MethodPatch, http.MethodTrace,
}

// Generator generates the OpenAPI documents of an HTTP server configuration. Create it with New.
type Generator struct {
	config  fhttp.Config
	info    Info
	schemas map[reflect.Type]Schema
	hooks   []func(version string, doc *Document) error
}

// Option configures a Generator.
type Option func(*Generator)

// WithInfo sets the title, description and version of the documents. An empty Title is "API Doc" and an empty
// Version is "1.0.0". Only the document of unversioned routes uses Version: the document of versioned routes
// takes their RouteConfig.Version. Without WithInfo, the description is "HTTP API Doc".
func WithInfo(info Info) Option { return func(g *Generator) { g.info = info } }

// WithSchema documents T as schema wherever T appears: in request bodies, in responses and as a parameter. It
// is meant for types whose JSON shape cannot be inferred, such as types with their own json.Marshaler, and for
// giving a type a format or enum values. A field's validate tag still adds its constraints. *T stays nullable in
// bodies unless it has a WithSchema of its own. A later WithSchema for the same type replaces an earlier one.
func WithSchema[T any](schema Schema) Option {
	return func(g *Generator) {
		if g.schemas == nil {
			g.schemas = make(map[reflect.Type]Schema)
		}
		g.schemas[reflect.TypeFor[T]()] = maps.Clone(schema)
	}
}

// WithDocument adds a hook that runs on every document after it is generated, with the RouteConfig.Version of
// its routes (empty for unversioned routes). It adds what routes do not declare, such as Servers, Security and
// Components.SecuritySchemes, or Tags and Summary on operations found with Document.Operation. Hooks run in the
// order they are added, and an error fails generation.
func WithDocument(hook func(version string, doc *Document) error) Option {
	return func(g *Generator) { g.hooks = append(g.hooks, hook) }
}

// New returns a generator for config. It sanitizes config, so the server's default serializer is documented
// unless config sets its own.
func New(config fhttp.Config, options ...Option) *Generator {
	g := &Generator{config: config.Sanitize(), info: Info{Title: defaultTitle, Description: defaultDescription}}
	for _, option := range options {
		option(g)
	}
	if strings.TrimSpace(g.info.Title) == "" {
		g.info.Title = defaultTitle
	}
	if g.info.Version == "" {
		g.info.Version = defaultVersion
	}
	return g
}

// GenerateAll returns a separate document for every exact RouteConfig.Version.
// The empty key identifies unversioned routes. Components, operation names,
// and path validation are scoped to each individual document. Routes with
// DisableAPIDoc are left out, so a version whose routes all have it gets no
// document.
func (g *Generator) GenerateAll(ctx context.Context) (map[string]*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	groups := make(map[string][]fhttp.RouteConfig)
	for _, route := range documented(g.config.Routes) {
		groups[route.Version] = append(groups[route.Version], route)
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("openapi: no routes configured")
	}
	versions := make([]string, 0, len(groups))
	for version := range groups {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	docs := make(map[string]*Document, len(groups))
	for _, version := range versions {
		child := *g
		child.config.Routes = groups[version]
		doc, err := child.Generate(ctx)
		if err != nil {
			return nil, fmt.Errorf("openapi: version %q: %w", version, err)
		}
		docs[version] = doc
	}
	return docs, nil
}

// Generate returns the document of a configuration whose documented routes all have the same version. Use
// GenerateAll for several versions. Routes with DisableAPIDoc are left out.
//
// It reflects the request and response types of the routes, probes the configured serializer, and reads the
// Params of server and route middlewares created by http.MiddlewareAs through http.MiddlewareContractOf, which
// never resolves the application's dependencies. The WithDocument hooks run last.
func (g *Generator) Generate(ctx context.Context) (*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g.config.Serializer == nil {
		return nil, errors.New("openapi: generator has no serializer; create it with New")
	}
	routes := documented(g.config.Routes)
	version := ""
	if len(routes) > 0 {
		version = routes[0].Version
	}
	for _, route := range routes {
		if route.Version != version {
			return nil, fmt.Errorf("openapi: multiple route versions configured; use GenerateAll")
		}
	}
	info := g.info
	if version != "" {
		info.Version = version
	}
	doc := &Document{OpenAPI: openAPIVersion, Info: info, Paths: make(map[string]map[string]*Operation)}
	b := newSchemaBuilder(g.schemas)
	schemes := make(map[string]SecurityScheme)
	serverMiddlewares := middlewareContracts(g.config.Middlewares)
	names := make(map[string]bool)
	templates := make(map[string]string)
	for _, route := range routes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		label := routeLabel(route)
		method, err := operationMethod(route.Method)
		if err != nil {
			return nil, fmt.Errorf("openapi: route %s: %w", label, err)
		}
		// FullPath is the exact path the HTTP server registers the route on.
		path, vars, err := pathTemplate(route.Method, route.FullPath())
		if err != nil {
			return nil, fmt.Errorf("openapi: route %s: %w", label, err)
		}
		shape := path
		for name := range vars {
			shape = strings.ReplaceAll(shape, "{"+name+"}", "{}")
		}
		if other, ok := templates[shape]; ok && other != path {
			return nil, fmt.Errorf("openapi: equivalent templated paths %q and %q", other, path)
		}
		templates[shape] = path
		if doc.Paths[path] == nil {
			doc.Paths[path] = make(map[string]*Operation)
		}
		if doc.Paths[path][method] != nil {
			return nil, fmt.Errorf("openapi: duplicate operation %s %s", method, path)
		}
		if route.Name != "" {
			if names[route.Name] {
				return nil, fmt.Errorf("openapi: duplicate route name %q", route.Name)
			}
			names[route.Name] = true
		}
		op, err := g.operation(ctx, b, route, method, vars, serverMiddlewares, schemes)
		if err != nil {
			return nil, fmt.Errorf("openapi: route %s: %w", label, err)
		}
		doc.Paths[path][method] = op
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.finish(doc)
	if len(schemes) > 0 {
		doc.Components.SecuritySchemes = schemes
	}
	for _, hook := range g.hooks {
		if err := hook(version, doc); err != nil {
			return nil, fmt.Errorf("openapi: document hook: %w", err)
		}
	}
	return doc, nil
}

// operation documents one route, whose path has the wildcards vars and whose server middlewares have the
// contracts serverMiddlewares. The security schemes of its middlewares are added to schemes.
func (g *Generator) operation(ctx context.Context, b *schemaBuilder, route fhttp.RouteConfig, method string,
	vars map[string]bool, serverMiddlewares []fhttp.MiddlewareContract, schemes map[string]SecurityScheme) (*Operation, error) {
	contract := route.Handler.Contract()
	covered := make(map[string]bool)
	params, body, err := inspectRequest(b, contract.Request, vars, covered)
	if err != nil {
		return nil, err
	}
	middlewares := slices.Concat(serverMiddlewares, middlewareContracts(route.Middlewares))
	for _, mc := range middlewares {
		if mc.Params == nil {
			continue
		}
		more, err := inspectMiddleware(b, mc.Params, vars, covered)
		if err == nil {
			params, err = mergeParams(params, more)
		}
		if err != nil {
			return nil, fmt.Errorf("middleware params %s: %w", mc.Params, err)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		if !covered[name] {
			return nil, fmt.Errorf("path parameter %q has no matching uri tag", name)
		}
	}
	params = withRequestID(params)
	sortParams(params)
	security, err := securityOf(middlewares, schemes)
	if err != nil {
		return nil, err
	}
	var result Schema
	if contract.Response != nil {
		result, err = b.schema(contract.Response, false)
		if err != nil {
			return nil, fmt.Errorf("response: %w", err)
		}
	}
	success, status, err := inspectSuccess(g.config.Serializer, result, method == "head")
	if err != nil {
		return nil, fmt.Errorf("success serializer: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	failure, err := inspectError(g.config.Serializer, method == "head")
	if err != nil {
		return nil, fmt.Errorf("error serializer: %w", err)
	}
	responses := map[string]Response{strconv.Itoa(status): success, "default": failure}
	headers, err := g.rejections(middlewares, responses, method == "head")
	if err != nil {
		return nil, err
	}
	addHeaders(success, headers)
	addHeaders(failure, headers)
	return &Operation{OperationID: route.Name, Description: route.Description, Parameters: params, RequestBody: body,
		Responses: responses, Security: security}, nil
}

// requestIDDescription documents the request header the server takes the operation ID from.
const requestIDDescription = "Identifies the request in the server's logs. A missing ID, or one longer than 128 " +
	"bytes or with characters other than visible ASCII, is replaced with a new one."

// withRequestID documents the X-Request-ID header the server reads on every request, unless params declare it.
func withRequestID(params []Parameter) []Parameter {
	name := http.CanonicalHeaderKey(fhttp.ReqIDHeader)
	i := slices.IndexFunc(params, func(p Parameter) bool { return p.In == "header" && p.Name == name })
	if i < 0 {
		return append(params, Parameter{Name: name, In: "header", Description: requestIDDescription,
			Schema: Schema{"type": "string"}})
	}
	if params[i].Description == "" {
		params[i].Description = requestIDDescription
	}
	return params
}

// rejections adds to responses the responses the middlewares of contracts, in chain order, answer themselves,
// each with the headers of the middlewares that ran before it, and returns the headers of them all, which every
// other response carries.
func (g *Generator) rejections(contracts []fhttp.MiddlewareContract, responses map[string]Response, head bool) ([]fhttp.ResponseHeader, error) {
	var headers []fhttp.ResponseHeader
	for _, mc := range contracts {
		headers = append(headers, mc.Headers...)
		for _, rejection := range mc.Rejections {
			resp, status, err := inspectRejection(g.config.Serializer, g.config.ErrorMapper, rejection.Err, head)
			if err != nil {
				return nil, fmt.Errorf("middleware rejection %q: %w", rejection.Err, err)
			}
			key := strconv.Itoa(status)
			if earlier, ok := responses[key]; ok {
				resp = earlier
			}
			addHeaders(resp, headers)
			addHeaders(resp, rejection.Headers)
			responses[key] = resp
		}
	}
	return headers, nil
}

// addHeaders documents headers on resp under their own spelling, over the headers the serializer was found to
// write, whose names are canonical.
func addHeaders(resp Response, headers []fhttp.ResponseHeader) {
	for _, h := range headers {
		schema := Schema{"type": "string"}
		if h.Type != nil {
			if s, ok := primitive(h.Type); ok {
				schema = s
			}
		}
		maps.DeleteFunc(resp.Headers, func(name string, _ Header) bool { return strings.EqualFold(name, h.Name) })
		resp.Headers[h.Name] = Header{Description: h.Description, Schema: schema}
	}
}

// bearerScheme names the security scheme of bearer tokens.
const bearerScheme = "bearerAuth"

// securityOf returns the security requirements of the middlewares of contracts that authenticate, and nil when
// none does, and adds their schemes to schemes. A request satisfies every such middleware, each with any one of
// the tokens it accepts.
func securityOf(contracts []fhttp.MiddlewareContract, schemes map[string]SecurityScheme) ([]SecurityRequirement, error) {
	var reqs []SecurityRequirement
	for _, mc := range contracts {
		if mc.Auth == nil {
			continue
		}
		alternatives, err := authAlternatives(mc.Auth, schemes)
		if err != nil {
			return nil, err
		}
		if reqs == nil {
			reqs = alternatives
			continue
		}
		var both []SecurityRequirement
		for _, r := range reqs {
			for _, alt := range alternatives {
				merged := maps.Clone(r)
				maps.Copy(merged, alt)
				if !slices.ContainsFunc(both, func(s SecurityRequirement) bool { return maps.EqualFunc(s, merged, slices.Equal) }) {
					both = append(both, merged)
				}
			}
		}
		reqs = both
	}
	return reqs, nil
}

// authAlternatives returns the security requirements of a, any one of which authenticates a request, and adds
// their schemes to schemes: a bearer token, else a token in a cookie, whose scheme is named after the cookie, else
// nothing when a is optional.
func authAlternatives(a *fhttp.AuthContract, schemes map[string]SecurityScheme) ([]SecurityRequirement, error) {
	type named struct {
		name   string
		scheme SecurityScheme
	}
	accepted := []named{{bearerScheme, SecurityScheme{Type: "http", Scheme: "bearer", BearerFormat: a.BearerFormat}}}
	if a.Cookie != "" {
		accepted = append(accepted, named{componentName(a.Cookie), SecurityScheme{Type: "apiKey", In: "cookie", Name: a.Cookie}})
	}
	var alternatives []SecurityRequirement
	for _, n := range accepted {
		if s, ok := schemes[n.name]; ok && s != n.scheme {
			return nil, fmt.Errorf("security scheme %q has two definitions, %+v and %+v", n.name, s, n.scheme)
		}
		schemes[n.name] = n.scheme
		alternatives = append(alternatives, SecurityRequirement{n.name: {}})
	}
	if a.Optional {
		alternatives = append(alternatives, SecurityRequirement{})
	}
	return alternatives, nil
}

// componentName returns s with the characters a component name cannot have replaced by "_".
func componentName(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9', r == '.', r == '-', r == '_':
			return r
		}
		return '_'
	}, s)
}

// documented returns the routes without DisableAPIDoc.
func documented(routes []fhttp.RouteConfig) []fhttp.RouteConfig {
	return slices.DeleteFunc(slices.Clone(routes), func(r fhttp.RouteConfig) bool { return r.DisableAPIDoc })
}

// middlewareContracts returns the contracts of the factories created by http.MiddlewareAs, in chain order.
func middlewareContracts(factories []forgo.MiddlewareFactory) []fhttp.MiddlewareContract {
	var contracts []fhttp.MiddlewareContract
	for _, f := range factories {
		if c, ok := fhttp.MiddlewareContractOf(f); ok {
			contracts = append(contracts, c)
		}
	}
	return contracts
}

// routeLabel names a route in errors by its method and full path, and by its name when it has one.
func routeLabel(route fhttp.RouteConfig) string {
	label := strings.TrimSpace(route.Method + " " + route.FullPath())
	if route.Name != "" {
		return fmt.Sprintf("%q (%s)", route.Name, label)
	}
	return label
}

// operationMethod returns the OpenAPI operation key of a route method. Methods are case-sensitive in ServeMux
// patterns, so only the upper-case spelling matches requests.
func operationMethod(method string) (string, error) {
	if slices.Contains(operationMethods, method) {
		return strings.ToLower(method), nil
	}
	if method == "" {
		return "", errors.New("an empty Method matches every method, which an OpenAPI operation cannot describe: set Method, or DisableAPIDoc")
	}
	if upper := strings.ToUpper(method); slices.Contains(operationMethods, upper) {
		return "", fmt.Errorf("method %q never matches %s requests, because ServeMux methods are case-sensitive: use %q", method, upper, upper)
	}
	return "", fmt.Errorf("OpenAPI 3.1 cannot describe method %q: set DisableAPIDoc", method)
}

// pathTemplate converts the path of a ServeMux pattern into an OpenAPI path template. It returns the template
// and the wildcards of the path, each mapped to whether it matches the rest of the path. The pattern is first
// registered on a throwaway ServeMux, so a path the server cannot register fails with ServeMux's own error.
//
// OpenAPI has no remainder wildcards and no end-of-path anchor: {name...} is documented as {name}, and {$} is
// dropped, so /users/{$} is documented as /users/.
func pathTemplate(method, path string) (template string, vars map[string]bool, err error) {
	func() {
		defer func() {
			if v := recover(); v != nil {
				err = fmt.Errorf("invalid ServeMux pattern: %v", v)
			}
		}()
		http.NewServeMux().HandleFunc(method+" "+path, func(http.ResponseWriter, *http.Request) {})
	}()
	if err != nil {
		return "", nil, err
	}
	// ServeMux accepts a wildcard only as a whole segment, with a unique name, and {name...} and {$} only last.
	vars = make(map[string]bool)
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if !strings.HasPrefix(segment, "{") {
			continue
		}
		name := strings.TrimSuffix(segment[1:len(segment)-1], "...")
		if name == "$" {
			segments[i] = ""
			continue
		}
		vars[name] = strings.HasSuffix(segment, "...}")
		segments[i] = "{" + name + "}"
	}
	return strings.Join(segments, "/"), vars, nil
}
