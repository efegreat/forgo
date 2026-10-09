package openapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"

	"github.com/efegreat/forgo/operation"
	fhttp "github.com/efegreat/forgo/server/http"
)

const markerKey = "__forgo_openapi_result_54e9453c__"
const probeID = "FORGO_OPENAPI_REQUEST_ID"

type resultMarker struct{}

func (resultMarker) MarshalJSON() ([]byte, error) { return []byte(`{"` + markerKey + `":true}`), nil }

func probe(factory fhttp.SerializerFactory, value any, failure error) (r *http.Response, body []byte, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("inspection panicked: %v", v)
		}
	}()
	w := httptest.NewRecorder()
	s := factory(probeID, w)
	if s == nil {
		return nil, nil, fmt.Errorf("factory returned nil serializer")
	}
	if failure == nil {
		err = s.Success(value)
	} else {
		err = s.Error(failure)
		// Some ForGo serializers return the input domain error after a successful
		// write. Accept only that error; wrapped write errors remain failures.
		if err != nil && sameProbeError(err, failure) {
			err = nil
		}
	}
	if err != nil {
		return nil, nil, err
	}
	r = w.Result()
	defer r.Body.Close()
	body, err = io.ReadAll(r.Body)
	return r, body, err
}

func sameProbeError(got, input error) bool {
	return reflect.DeepEqual(got, input)
}

func inspectSuccess(factory fhttp.SerializerFactory, result Schema, head bool) (Response, int, error) {
	var value any // nil matches EmptyHandler's omitted result contract
	if result != nil {
		value = resultMarker{}
	}
	r, body, err := probe(factory, value, nil)
	if err != nil {
		return Response{}, 0, err
	}
	if r.StatusCode < 200 || r.StatusCode > 299 {
		return Response{}, 0, fmt.Errorf("success probe wrote status %d", r.StatusCode)
	}
	resp, count, err := describeResponse(r, body, result, "Successful response", head)
	if err != nil {
		return Response{}, 0, err
	}
	if result != nil && count == 0 && !head && r.StatusCode != http.StatusNoContent && r.StatusCode != http.StatusResetContent {
		return Response{}, 0, fmt.Errorf("serializer did not preserve the result marker; endpoint response cannot be located")
	}
	return resp, r.StatusCode, nil
}

func inspectError(factory fhttp.SerializerFactory, head bool) (Response, error) {
	errValue := operation.Error{Status: operation.StatusInvalidArgument, Code: "FORGO_OPENAPI_ERROR_CODE", Message: "FORGO_OPENAPI_ERROR_MESSAGE"}
	r, body, err := probe(factory, nil, errValue)
	if err != nil {
		return Response{}, err
	}
	resp, _, err := describeResponse(r, body, nil, "Error response", head)
	return resp, err
}

func describeResponse(r *http.Response, body []byte, result Schema, description string, head bool) (Response, int, error) {
	resp := Response{Description: description, Headers: make(map[string]Header)}
	for name := range r.Header {
		// HTTP metadata and dynamic lengths are not response contract headers.
		if strings.EqualFold(name, "Content-Type") || strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Date") {
			continue
		}
		resp.Headers[name] = Header{Schema: Schema{"type": "string"}}
	}
	if head || r.StatusCode == 204 || r.StatusCode == 205 || len(body) == 0 {
		return resp, 0, nil
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return Response{}, 0, fmt.Errorf("missing or invalid Content-Type: %w", err)
	}
	var schema Schema
	count := 0
	if media == "application/json" || strings.HasSuffix(media, "+json") {
		d := json.NewDecoder(bytes.NewReader(body))
		d.UseNumber()
		var data any
		if err := d.Decode(&data); err != nil {
			return Response{}, 0, fmt.Errorf("invalid JSON response: %w", err)
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			return Response{}, 0, fmt.Errorf("serializer wrote multiple or invalid JSON documents")
		}
		schema, count = observedSchema(data, result)
	} else if strings.HasPrefix(media, "text/") && result == nil {
		schema = Schema{"type": "string"}
	} else {
		return Response{}, 0, fmt.Errorf("unsupported serializer media type %q", media)
	}
	resp.Content = map[string]MediaType{media: {Schema: schema}}
	return resp, count, nil
}

// observedSchema describes only envelope structure. The marker is replaced by
// the reflected response schema; omitted response-model fields are never lost.
func observedSchema(value any, result Schema) (Schema, int) {
	switch v := value.(type) {
	case map[string]any:
		if flag, ok := v[markerKey].(bool); ok && flag && len(v) == 1 && result != nil {
			return result, 1
		}
		props := make(map[string]Schema)
		count := 0
		for key, value := range v {
			s, n := observedSchema(value, result)
			props[key] = s
			count += n
		}
		return Schema{"type": "object", "properties": props}, count
	case []any:
		var items []Schema
		count := 0
		seen := make(map[string]bool)
		for _, value := range v {
			s, n := observedSchema(value, result)
			count += n
			encoded, _ := json.Marshal(s)
			if !seen[string(encoded)] {
				seen[string(encoded)] = true
				items = append(items, s)
			}
		}
		sort.Slice(items, func(i, j int) bool {
			a, _ := json.Marshal(items[i])
			b, _ := json.Marshal(items[j])
			return string(a) < string(b)
		})
		item := Schema{}
		if len(items) == 1 {
			item = items[0]
		} else if len(items) > 1 {
			item["anyOf"] = items
		}
		return Schema{"type": "array", "items": item}, count
	case string:
		return Schema{"type": "string"}, 0
	case bool:
		return Schema{"type": "boolean"}, 0
	case json.Number:
		typ := "number"
		if !strings.ContainsAny(string(v), ".eE") {
			typ = "integer"
		}
		return Schema{"type": typ}, 0
	case nil:
		return Schema{}, 0 // one null sample does not reveal a field's type
	default:
		return Schema{}, 0
	}
}
