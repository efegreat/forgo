// Package openapi generates OpenAPI 3.1 documents from forgo HTTP configuration. It reads the routes, their
// request and response types, the contracts of middlewares (what http.JWTAuth, http.RateLimit and
// http.RateLimitWith read and answer, the Params of http.MiddlewareAs, and the contracts of http.WithContract) and
// what the configured serializer writes. It never constructs handlers or middlewares, binds a request or starts a
// server.
package openapi

// Schema is a JSON Schema (draft 2020-12) object, the schema dialect of OpenAPI 3.1.
type Schema map[string]any

// Document is an OpenAPI 3.1 document. The generator fills OpenAPI, Info, Paths, Components.Schemas, and
// Components.SecuritySchemes from the credentials of the routes' middlewares. Servers, Security and Tags are left
// for a WithDocument hook, because routes do not declare them.
type Document struct {
	OpenAPI    string                           `json:"openapi"`
	Info       Info                             `json:"info"`
	Servers    []Server                         `json:"servers,omitempty"`
	Paths      map[string]map[string]*Operation `json:"paths"`
	Components Components                       `json:"components"`
	Security   []SecurityRequirement            `json:"security,omitempty"`
	Tags       []Tag                            `json:"tags,omitempty"`
}

// Operation returns the operation whose operationId is id, which is the RouteConfig.Name of its route, or nil.
func (d *Document) Operation(id string) *Operation {
	for _, methods := range d.Paths {
		for _, op := range methods {
			if op.OperationID == id {
				return op
			}
		}
	}
	return nil
}

// Info is the info object of a document. Version is the version of the document of unversioned routes. The
// document of versioned routes takes their RouteConfig.Version instead.
type Info struct {
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	Version     string `json:"version"`
}

// Server is a server the API is served from.
type Server struct {
	URL         string                    `json:"url"`
	Description string                    `json:"description,omitempty"`
	Variables   map[string]ServerVariable `json:"variables,omitempty"`
}

// ServerVariable is a variable of a Server URL template, such as {region} in https://{region}.example.com.
type ServerVariable struct {
	Enum        []string `json:"enum,omitempty"`
	Default     string   `json:"default"`
	Description string   `json:"description,omitempty"`
}

// Components holds the schemas the document references, and its security schemes.
type Components struct {
	Schemas         map[string]Schema         `json:"schemas"`
	SecuritySchemes map[string]SecurityScheme `json:"securitySchemes,omitempty"`
}

// SecurityRequirement maps the names of security schemes in Components.SecuritySchemes to the scopes a request
// needs. Schemes without scopes, such as a bearer token, map to an empty list.
type SecurityRequirement map[string][]string

// SecurityScheme describes how requests authenticate: Type is "apiKey", "http", "mutualTLS", "oauth2" or
// "openIdConnect". A bearer token is {Type: "http", Scheme: "bearer", BearerFormat: "JWT"}.
type SecurityScheme struct {
	Type             string      `json:"type"`
	Description      string      `json:"description,omitempty"`
	Name             string      `json:"name,omitempty"`
	In               string      `json:"in,omitempty"`
	Scheme           string      `json:"scheme,omitempty"`
	BearerFormat     string      `json:"bearerFormat,omitempty"`
	Flows            *OAuthFlows `json:"flows,omitempty"`
	OpenIDConnectURL string      `json:"openIdConnectUrl,omitempty"`
}

// OAuthFlows lists the OAuth 2 flows of an "oauth2" SecurityScheme.
type OAuthFlows struct {
	Implicit          *OAuthFlow `json:"implicit,omitempty"`
	Password          *OAuthFlow `json:"password,omitempty"`
	ClientCredentials *OAuthFlow `json:"clientCredentials,omitempty"`
	AuthorizationCode *OAuthFlow `json:"authorizationCode,omitempty"`
}

// OAuthFlow is one OAuth 2 flow. Scopes maps scope names to their descriptions.
type OAuthFlow struct {
	AuthorizationURL string            `json:"authorizationUrl,omitempty"`
	TokenURL         string            `json:"tokenUrl,omitempty"`
	RefreshURL       string            `json:"refreshUrl,omitempty"`
	Scopes           map[string]string `json:"scopes"`
}

// Tag groups operations, such as in Swagger UI. Operations name their tags in Operation.Tags.
type Tag struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Operation is one route. The generator fills OperationID from RouteConfig.Name, Description from
// RouteConfig.Description, Parameters, RequestBody, Responses, and Security on the routes a middleware with
// credentials in its contract authenticates.
type Operation struct {
	Tags        []string            `json:"tags,omitempty"`
	Summary     string              `json:"summary,omitempty"`
	OperationID string              `json:"operationId,omitempty"`
	Description string              `json:"description,omitempty"`
	Parameters  []Parameter         `json:"parameters,omitempty"`
	RequestBody *RequestBody        `json:"requestBody,omitempty"`
	Responses   map[string]Response `json:"responses"`
	Deprecated  bool                `json:"deprecated,omitempty"`
	// Security replaces the document's Security for this operation. A nil list keeps it, and an empty list
	// documents an operation without security, such as a health check.
	Security []SecurityRequirement `json:"security,omitzero"`
}

// Parameter is a path, header or query parameter.
type Parameter struct {
	Name        string `json:"name"`
	In          string `json:"in"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Style       string `json:"style,omitempty"`
	Explode     *bool  `json:"explode,omitempty"`
	Schema      Schema `json:"schema"`
}

// RequestBody is the body of a request, keyed by media type.
type RequestBody struct {
	Required bool                 `json:"required,omitempty"`
	Content  map[string]MediaType `json:"content"`
}

// MediaType is the schema of a body in one media type.
type MediaType struct {
	Schema Schema `json:"schema"`
}

// Response is one response of an operation.
type Response struct {
	Description string               `json:"description"`
	Headers     map[string]Header    `json:"headers,omitempty"`
	Content     map[string]MediaType `json:"content,omitempty"`
}

// Header is a response header.
type Header struct {
	Description string `json:"description,omitempty"`
	Schema      Schema `json:"schema"`
}
