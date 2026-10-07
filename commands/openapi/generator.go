package openapi

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	fhttp "forgo/server/http"
)

type Generator struct {
	config fhttp.Config
	info   Info
}

type Option func(*Generator)

// WithInfo sets document metadata without adding OpenAPI fields to HTTP config.
func WithInfo(info Info) Option { return func(g *Generator) { g.info = info } }

func New(config fhttp.Config, options ...Option) *Generator {
	g := &Generator{config: config, info: Info{Title: "API Doc", Description: "HTTP API Doc"}}
	for _, option := range options {
		option(g)
	}
	return g
}

// GenerateAll returns a separate document for every exact RouteConfig.Version.
// The empty key identifies unversioned routes. Components, operation names,
// and path validation are scoped to each individual document.
func (g *Generator) GenerateAll(ctx context.Context) (map[string]*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	groups := make(map[string][]fhttp.RouteConfig)
	for _, route := range g.config.Routes {
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

// Generate generates a document for a configuration with one route version.
// For configurations containing several versions, use GenerateAll instead.
// It inspects types and calls only the configured serializer. The
// serializer factory must be explicitly supplied: ForGo's unexported default
// factory cannot be recovered from a nil Config.Serializer without a server.
func (g *Generator) Generate(ctx context.Context) (*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(g.info.Title) == "" {
		return nil, fmt.Errorf("openapi: title must be nonempty")
	}
	if g.config.Serializer == nil {
		return nil, fmt.Errorf("openapi: Config.Serializer is nil; supply the HTTP serializer factory explicitly")
	}
	version := ""
	if len(g.config.Routes) > 0 {
		version = g.config.Routes[0].Version
	}
	for _, route := range g.config.Routes {
		if route.Version != version {
			return nil, fmt.Errorf("openapi: multiple route versions configured; use GenerateAll")
		}
	}
	doc := &Document{OpenAPI: "3.1.0", Info: DocumentInfo{Info: g.info, Version: version},
		Paths: make(map[string]map[string]*Operation), Components: Components{Schemas: make(map[string]Schema)}}
	b := newSchemaBuilder(doc.Components.Schemas)
	names := make(map[string]bool)
	templates := make(map[string]string)
	for _, route := range g.config.Routes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		method := strings.ToLower(route.Method)
		switch method {
		case "get", "put", "post", "delete", "options", "head", "patch", "trace":
		default:
			return nil, fmt.Errorf("openapi: route %q: unsupported HTTP method %q", route.Name, route.Method)
		}
		path, vars, err := normalizePath(route.Path)
		if err != nil {
			return nil, fmt.Errorf("openapi: route %q: %w", route.Name, err)
		}
		if version != "" {
			path, vars, err = normalizePath("/" + version + route.Path)
			if err != nil {
				return nil, fmt.Errorf("openapi: route %q: %w", route.Name, err)
			}
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
		contract := route.Handler.Contract()
		params, body, err := inspectRequest(b, contract.Request, vars)
		if err != nil {
			return nil, fmt.Errorf("openapi: route %q: %w", route.Name, err)
		}
		var result Schema
		if contract.Response != nil {
			result, err = b.schema(contract.Response, false)
			if err != nil {
				return nil, fmt.Errorf("openapi: route %q response: %w", route.Name, err)
			}
		}
		success, status, err := inspectSuccess(g.config.Serializer, result, method == "head")
		if err != nil {
			return nil, fmt.Errorf("openapi: route %q success serializer: %w", route.Name, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		failure, err := inspectError(g.config.Serializer, method == "head")
		if err != nil {
			return nil, fmt.Errorf("openapi: route %q error serializer: %w", route.Name, err)
		}
		doc.Paths[path][method] = &Operation{OperationID: route.Name, Description: route.Description,
			Parameters: params, RequestBody: body, Responses: map[string]Response{strconv.Itoa(status): success, "default": failure}}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return doc, nil
}

// normalizePath accepts mux-style {name} and {name:regexp} placeholders.
// Nested braces in regular expressions (for example [0-9]{2}) are preserved
// while finding the closing placeholder, then the regexp is removed for OAS.
func normalizePath(path string) (string, map[string]bool, error) {
	if !strings.HasPrefix(path, "/") {
		return "", nil, fmt.Errorf("path %q must start with /", path)
	}
	vars := make(map[string]bool)
	var out strings.Builder
	for i := 0; i < len(path); {
		if path[i] == '}' {
			return "", nil, fmt.Errorf("unmatched } in path %q", path)
		}
		if path[i] != '{' {
			out.WriteByte(path[i])
			i++
			continue
		}
		start, depth := i+1, 1
		i++
		for i < len(path) && depth > 0 {
			if path[i] == '\\' && i+1 < len(path) {
				i += 2
				continue
			}
			if path[i] == '{' {
				depth++
			}
			if path[i] == '}' {
				depth--
			}
			if depth > 0 {
				i++
			}
		}
		if depth != 0 {
			return "", nil, fmt.Errorf("unclosed placeholder in path %q", path)
		}
		name := strings.SplitN(path[start:i], ":", 2)[0]
		if name == "" || strings.ContainsAny(name, "{} /\t\r\n") {
			return "", nil, fmt.Errorf("invalid path parameter %q", name)
		}
		if vars[name] {
			return "", nil, fmt.Errorf("duplicate path parameter %q", name)
		}
		vars[name] = true
		out.WriteString("{" + name + "}")
		i++
	}
	return out.String(), vars, nil
}
