// Package openapi generates OpenAPI 3.1 documents from ForGo HTTP configuration.
// It never constructs handlers, invokes a binder, or starts a server.
package openapi

type Schema map[string]any

type Document struct {
	OpenAPI    string                           `json:"openapi"`
	Info       DocumentInfo                     `json:"info"`
	Paths      map[string]map[string]*Operation `json:"paths"`
	Components Components                       `json:"components"`
}

type Info struct {
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
}

// DocumentInfo contains generated metadata. Version always comes from the
// routes, rather than the caller-supplied Info option.
type DocumentInfo struct {
	Info
	Version string `json:"version"`
}

type Components struct {
	Schemas map[string]Schema `json:"schemas"`
}

type Operation struct {
	OperationID string              `json:"operationId,omitempty"`
	Description string              `json:"description,omitempty"`
	Parameters  []Parameter         `json:"parameters,omitempty"`
	RequestBody *RequestBody        `json:"requestBody,omitempty"`
	Responses   map[string]Response `json:"responses"`
}

type Parameter struct {
	Name     string `json:"name"`
	In       string `json:"in"`
	Required bool   `json:"required,omitempty"`
	Style    string `json:"style,omitempty"`
	Explode  *bool  `json:"explode,omitempty"`
	Schema   Schema `json:"schema"`
}

type RequestBody struct {
	Required bool                 `json:"required,omitempty"`
	Content  map[string]MediaType `json:"content"`
}

type MediaType struct {
	Schema Schema `json:"schema"`
}

type Response struct {
	Description string               `json:"description"`
	Headers     map[string]Header    `json:"headers,omitempty"`
	Content     map[string]MediaType `json:"content,omitempty"`
}

type Header struct {
	Schema Schema `json:"schema"`
}
