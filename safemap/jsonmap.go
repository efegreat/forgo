package safemap

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
)

// JSONMap is a concurrency-safe map that encodes as a JSON object. It implements json.Marshaler and
// json.Unmarshaler, and driver.Valuer and sql.Scanner to be stored in a JSON column as is.
//
// Decoding keeps the numbers of interface values, such as the values of a JSONMap[any] and anything
// nested in them, as json.Number instead of float64, so that large integers such as uint64 keep their
// precision. GetTyped converts them to numeric types.
//
// A JSONMap without a map encodes as null and stores as SQL NULL. That is the zero JSONMap and one
// that decoded a JSON null or scanned NULL. NewJSONMap, NewJSONMapWithData and the first write give it
// a map, so that it encodes as an object, {} when it is empty.
//
// Hold a JSONMap by pointer. encoding/json calls the methods of a *JSONMap only on addressable values,
// and encodes a JSONMap copied by value as {}. On a nil *JSONMap, MarshalJSON and Value encode null and
// NULL, while the methods it promotes from Map panic, since reaching the embedded Map dereferences it.
type JSONMap[V any] struct {
	Map[string, V]
}

// NewJSONMap creates an initialized empty JSONMap.
func NewJSONMap[V any]() *JSONMap[V] {
	return &JSONMap[V]{
		Map: Map[string, V]{
			data: make(map[string]V),
		},
	}
}

// NewJSONMapWithData creates a JSONMap initialized with the given data.
//
// The provided map is shallow-cloned so subsequent mutations to the original
// map do not bypass JSONMap's synchronization.
func NewJSONMapWithData[V any](data map[string]V) *JSONMap[V] {
	if data == nil {
		return NewJSONMap[V]()
	}

	m := &JSONMap[V]{}
	m.setData(maps.Clone(data), false)
	return m
}

// MarshalJSON implements json.Marshaler. It encodes the entries as a JSON object with sorted keys, and
// a JSONMap without a map as null.
func (m *JSONMap[V]) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return json.Marshal(m.data)
}

// UnmarshalJSON implements json.Unmarshaler. It replaces the entries with those of a JSON object, and
// leaves the JSONMap without a map for null, as encoding/json does with a Go map. On error, the
// JSONMap is unchanged.
func (m *JSONMap[V]) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("json map: UnmarshalJSON called on nil receiver")
	}

	decoded, err := decodeJSONMap[V](bytes.NewReader(data))
	if err != nil {
		return err
	}

	m.replace(decoded)
	return nil
}

// Value implements driver.Valuer.
//
// This allows JSONMap to be passed directly to database/sql:
//
//	_, err := db.ExecContext(
//		ctx,
//		"INSERT INTO example (metadata) VALUES (?)",
//		metadata,
//	)
//
// The object is a string, which every driver sends as text. As a []byte, MySQL drivers that
// interpolate parameters would send it as a binary string, which JSON columns reject. A nil JSONMap or
// one without a map is SQL NULL.
func (m *JSONMap[V]) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.data == nil {
		return nil, nil
	}

	data, err := json.Marshal(m.data)
	if err != nil {
		return nil, fmt.Errorf("json map: marshal: %w", err)
	}

	return string(data), nil
}

// Scan implements sql.Scanner. It replaces the entries with those of the JSON object in a []byte or a
// string, and leaves the JSONMap without a map for NULL or a JSON null. Anything but white space
// after the object is an error. On error, the JSONMap is unchanged.
func (m *JSONMap[V]) Scan(src any) error {
	if m == nil {
		return errors.New("json map: Scan called on nil receiver")
	}

	var r io.Reader
	switch value := src.(type) {
	case nil:
		m.replace(nil)
		return nil

	case []byte:
		r = bytes.NewReader(value)

	case string:
		r = strings.NewReader(value)

	default:
		return fmt.Errorf("json map: unsupported Scan source type %T", src)
	}

	decoded, err := decodeJSONMap[V](r)
	if err != nil {
		return err
	}

	m.replace(decoded)
	return nil
}

// replace makes data the content of m, which owns it from then on.
func (m *JSONMap[V]) replace(data map[string]V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.setData(data, false)
}

// decodeJSONMap decodes one JSON object or null from r, followed by white space only. It keeps the
// numbers of interface values as json.Number.
func decodeJSONMap[V any](r io.Reader) (map[string]V, error) {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()

	var decoded map[string]V
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("json map: decode: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("json map: decode: data after the JSON object")
	}

	return decoded, nil
}
