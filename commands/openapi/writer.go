package openapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Marshal produces stable JSON or ordinary block YAML using only stdlib.
func Marshal(doc *Document, format string) ([]byte, error) {
	if doc == nil {
		return nil, fmt.Errorf("openapi: document is nil")
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(format) {
	case "json":
		return append(data, '\n'), nil
	case "yaml":
		var value any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		var out strings.Builder
		writeYAML(&out, value, 0)
		return []byte(out.String()), nil
	default:
		return nil, fmt.Errorf("openapi: unsupported format %q", format)
	}
}

// VersionOutputPath inserts a filename-safe version before the base extension.
// Empty versions keep the base filename. Unsafe UTF-8 bytes are percent-encoded
// so distinct version strings produce distinct filenames without subdirectories.
func VersionOutputPath(base, version string) string {
	if version == "" {
		return base
	}
	var token strings.Builder
	for i := 0; i < len(version); i++ {
		c := version[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._-", rune(c)) {
			token.WriteByte(c)
		} else {
			fmt.Fprintf(&token, "%%%02X", c)
		}
	}
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext) + "-" + token.String() + ext
}

// WriteFiles writes one document per route version, in deterministic order.
// All documents are marshaled before writing; each replacement is atomic.
func WriteFiles(ctx context.Context, base, format string, docs map[string]*Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(docs) == 0 {
		return fmt.Errorf("openapi: no documents to write")
	}
	versions := make([]string, 0, len(docs))
	for version := range docs {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	for _, version := range versions {
		if _, err := Marshal(docs[version], format); err != nil {
			return fmt.Errorf("openapi: version %q: %w", version, err)
		}
	}
	for _, version := range versions {
		if err := WriteFile(ctx, VersionOutputPath(base, version), format, docs[version]); err != nil {
			return fmt.Errorf("openapi: version %q: %w", version, err)
		}
	}
	return nil
}

// WriteFile uses a sibling temporary file and rename, so a generation/write
// failure never truncates the previously committed document. Existing parent
// directories are required. Existing file permissions are preserved.
func WriteFile(ctx context.Context, path, format string, doc *Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := Marshal(doc, format)
	if err != nil {
		return err
	}
	mode := os.FileMode(0644)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("openapi: output %q is not a regular file", path)
		}
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".openapi-*")
	if err != nil {
		return fmt.Errorf("openapi: create output: %w", err)
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("openapi: replace output: %w", err)
	}
	return nil
}

func yamlScalar(v any) (string, bool) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			return "{}", true
		}
	case []any:
		if len(x) == 0 {
			return "[]", true
		}
	default:
		data, _ := json.Marshal(v)
		return string(data), true
	}
	return "", false
}

func writeYAML(out *strings.Builder, v any, depth int) {
	indent := strings.Repeat("  ", depth)
	if scalar, ok := yamlScalar(v); ok {
		out.WriteString(indent + scalar + "\n")
		return
	}
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for key := range x {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			quoted, _ := json.Marshal(key)
			out.WriteString(indent + string(quoted) + ":")
			if scalar, ok := yamlScalar(x[key]); ok {
				out.WriteString(" " + scalar + "\n")
			} else {
				out.WriteByte('\n')
				writeYAML(out, x[key], depth+1)
			}
		}
	case []any:
		for _, value := range x {
			out.WriteString(indent + "-")
			if scalar, ok := yamlScalar(value); ok {
				out.WriteString(" " + scalar + "\n")
			} else {
				out.WriteByte('\n')
				writeYAML(out, value, depth+1)
			}
		}
	}
}
