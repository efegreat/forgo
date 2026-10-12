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

// Marshal encodes doc as indented JSON or as block YAML, in format "json" or "yaml". Object keys are sorted, so
// the same document always encodes to the same bytes.
func Marshal(doc *Document, format string) ([]byte, error) {
	if doc == nil {
		return nil, fmt.Errorf("openapi: document is nil")
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("openapi: encode document: %w", err)
	}
	switch strings.ToLower(format) {
	case "json":
		return append(data, '\n'), nil
	case "yaml":
		var value any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("openapi: encode document: %w", err)
		}
		var out strings.Builder
		writeYAML(&out, value, 0)
		return []byte(out.String()), nil
	default:
		return nil, fmt.Errorf("openapi: unsupported format %q", format)
	}
}

// VersionOutputPath returns the file WriteFiles writes the document of version to: base with "-<version>"
// before its extension, or base itself for the empty version. Bytes other than letters, digits, '.', '_' and
// '-' are percent-encoded, so distinct versions get distinct files and never a subdirectory.
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

// WriteFiles writes the documents of GenerateAll, each to VersionOutputPath(base, version), in version order.
// It encodes every document before writing any, so an encoding error writes nothing. Each file is replaced
// atomically, but the files are not replaced together: when writing one fails, the files written before it
// stay replaced.
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

// WriteFile writes doc to path in format "json" or "yaml". It writes a temporary file next to path and renames
// it over path, so a failure never leaves a truncated document behind. The parent directory must exist, and an
// existing path must be a regular file, not a symlink. An existing file keeps its permissions; a new one gets
// 0644.
func WriteFile(ctx context.Context, path, format string, doc *Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := Marshal(doc, format)
	if err != nil {
		return err
	}
	fail := func(err error) error { return fmt.Errorf("openapi: write %s: %w", path, err) }
	mode := os.FileMode(0644)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("openapi: output %q is not a regular file", path)
		}
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".openapi-*")
	if err != nil {
		return fail(err)
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return fail(err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fail(err)
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
