package openapi

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func testDocument() *Document {
	return &Document{
		OpenAPI: openAPIVersion,
		Info:    Info{Title: `Shop: "API" # 1`, Description: "line one\nline two\t ü 💡 <&>", Version: ""},
		Servers: []Server{{URL: "https://{region}.example.com", Variables: map[string]ServerVariable{
			"region": {Enum: []string{"eu", "us"}, Default: "eu"},
		}}},
		Paths: map[string]map[string]*Operation{"/health": {"get": {
			OperationID: "health",
			Responses:   map[string]Response{"200": {Description: "- not a list"}},
			Security:    []SecurityRequirement{},
		}}},
		Components: Components{Schemas: map[string]Schema{
			"Answer": {"enum": []any{"yes", "no", "null", "~", "", " padded "}, "maximum": json.Number("1e3")},
			"Empty":  {},
		}},
		Security: []SecurityRequirement{{"bearer": {}}},
	}
}

// parseYAML reads the YAML subset Marshal writes: two-space block indentation, JSON-quoted keys, and JSON,
// {} or [] scalars.
func parseYAML(t *testing.T, data string) any {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
	i := 0
	scalar := func(s string) any {
		switch s {
		case "{}":
			return map[string]any{}
		case "[]":
			return []any{}
		}
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		var v any
		if err := d.Decode(&v); err != nil || d.More() {
			t.Fatalf("line %d: invalid scalar %q: %v", i, s, err)
		}
		return v
	}
	var parse func(depth int) any
	parse = func(depth int) any {
		indent := strings.Repeat("  ", depth)
		at := func(prefix string) bool {
			return i < len(lines) && strings.HasPrefix(lines[i], indent+prefix)
		}
		if at("-") {
			list := []any{}
			for at("-") {
				rest := strings.TrimPrefix(lines[i], indent+"-")
				i++
				if rest == "" {
					list = append(list, parse(depth+1))
				} else {
					list = append(list, scalar(strings.TrimPrefix(rest, " ")))
				}
			}
			return list
		}
		m := map[string]any{}
		for at(`"`) {
			line := strings.TrimPrefix(lines[i], indent)
			d := json.NewDecoder(strings.NewReader(line))
			var key string
			if err := d.Decode(&key); err != nil {
				t.Fatalf("line %d: invalid key: %v", i, err)
			}
			rest := line[d.InputOffset():]
			i++
			if rest == ":" {
				m[key] = parse(depth + 1)
			} else {
				m[key] = scalar(strings.TrimPrefix(rest, ": "))
			}
		}
		return m
	}
	v := parse(0)
	if i != len(lines) {
		t.Fatalf("unparsed line %d: %q", i, lines[i])
	}
	return v
}

func TestMarshalYAML(t *testing.T) {
	doc := testDocument()
	data, err := Marshal(doc, "yaml")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Marshal(doc, "JSON")
	if err != nil {
		t.Fatal(err)
	}
	d := json.NewDecoder(strings.NewReader(string(encoded)))
	d.UseNumber()
	var want any
	if err := d.Decode(&want); err != nil {
		t.Fatal(err)
	}
	if got := parseYAML(t, string(data)); !reflect.DeepEqual(got, want) {
		t.Fatalf("YAML differs from JSON:\n%s", data)
	}
	// Words YAML 1.1 reads as booleans or null stay strings.
	for _, quoted := range []string{`- "yes"`, `- "no"`, `- "null"`, `- "~"`, `- ""`, `"security": []`} {
		if !strings.Contains(string(data), quoted) {
			t.Fatalf("missing %s in\n%s", quoted, data)
		}
	}
	if _, err := Marshal(doc, "xml"); err == nil || err.Error() != `openapi: unsupported format "xml"` {
		t.Fatalf("unsupported format: %v", err)
	}
	if _, err := Marshal(nil, "json"); err == nil {
		t.Fatal("nil document accepted")
	}
	doc.Components.Schemas["Bad"] = Schema{"value": func() {}}
	if _, err := Marshal(doc, "json"); err == nil || !strings.HasPrefix(err.Error(), "openapi: encode document:") {
		t.Fatalf("encoding error: %v", err)
	}
}

func tempFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".openapi-*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func TestWriteFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "openapi.json")
	if err := WriteFile(ctx, path, "json", testDocument()); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("new file: %v %v", info.Mode(), err)
	}

	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	doc := testDocument()
	doc.Info.Title = "changed"
	if err := WriteFile(ctx, path, "yaml", doc); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), `"title": "changed"`) {
		t.Fatalf("replaced file: %s %v", data, err)
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0600 {
		t.Fatalf("permissions not kept: %v", info.Mode())
	}

	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(ctx, link, "json", doc); err == nil || !strings.Contains(err.Error(), "is not a regular file") {
		t.Fatalf("symlink: %v", err)
	}
	missing := filepath.Join(dir, "missing", "openapi.json")
	if err := WriteFile(ctx, missing, "json", doc); err == nil || !strings.HasPrefix(err.Error(), "openapi: write "+missing+":") {
		t.Fatalf("missing directory: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	other := filepath.Join(dir, "other.json")
	if err := WriteFile(cancelled, other, "json", doc); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Fatalf("cancelled write created the file: %v", err)
	}
	if leftovers := tempFiles(t, dir); len(leftovers) != 0 {
		t.Fatalf("temporary files left: %v", leftovers)
	}
}

func TestWriteFilesEncodesFirst(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "openapi.json")
	bad := testDocument()
	bad.Components.Schemas["Bad"] = Schema{"value": func() {}}
	err := WriteFiles(context.Background(), base, "json", map[string]*Document{"": testDocument(), "v2": bad})
	if err == nil || !strings.HasPrefix(err.Error(), `openapi: version "v2": openapi: encode document:`) {
		t.Fatalf("encoding error: %v", err)
	}
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatalf("a document was written before the encoding error: %v", err)
	}
	if err := WriteFiles(context.Background(), base, "json", nil); err == nil {
		t.Fatal("no documents accepted")
	}
}
